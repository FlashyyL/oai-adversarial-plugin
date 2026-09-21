package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPendingRenewalPersistsAndDoesNotRepeatAfterRestart(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	t.Setenv("LKS_TZ_STATE_FILE", filepath.Join(t.TempDir(), "state.json"))
	now := time.Now().UTC()
	key := accountHealthKey("pending", "astra")
	accountRouter.health[key] = accountModelHealth{AuthID: "pending", Model: "astra", State: "probe_pending", ProbeReason: "probe_model_mismatch", ObservedAt: now}
	if err := savePersistedState(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(stateFilePath())
	if err != nil {
		t.Fatal(err)
	}
	var saved persistedState
	if err = json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	accountRouter.health = map[string]accountModelHealth{}
	applyPersistedState(persistedState{AccountHealth: saved.AccountHealth})
	if !accountRouter.needsRenewal("pending", "astra", now) {
		t.Fatal("restart discarded pending renewal")
	}
	if !accountRouter.claimRenewal("pending", "astra", "legacy-pending", now) {
		t.Fatal("claim failed")
	}
	if err = savePersistedState(); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(stateFilePath())
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	accountRouter.health = map[string]accountModelHealth{}
	applyPersistedState(persistedState{AccountHealth: saved.AccountHealth})
	if accountRouter.needsRenewal("pending", "astra", now.Add(time.Hour)) {
		t.Fatal("restart repeated an attempted generation")
	}
	if accountRouter.health[key].ProbeReason != "probe_model_mismatch" {
		t.Fatal("lost probe diagnostics")
	}
}

func TestExpiryScanDoesNotNeedTrafficOrPrefetch(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	now := time.Now().UTC()
	for _, id := range []string{"a", "b", "disabled"} {
		setHealthyAccountState(accountRouter, id, "astra", strings.Repeat("a", 332), now.Add(-2*time.Hour))
	}
	old := hostAuthListFunc
	t.Cleanup(func() { hostAuthListFunc = old })
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{{ID: "a", AuthIndex: "a", Provider: "codex"}, {ID: "b", AuthIndex: "b", Provider: "codex"}, {ID: "disabled", AuthIndex: "d", Provider: "codex", Disabled: true}}, nil
	}
	e := &probeEngine{cfg: probeConfigState{Config: probeConfig{Enabled: true, AccountMode: "highest-priority", Models: []string{"gpt-6-astra"}, Prefetch: 0}}, queueActive: true}
	// A normal model probe must not swallow targeted renewals.
	e.queue = []probeTask{{Model: "gpt-6-astra"}}
	e.expiryScan(now)
	if len(e.queue) != 3 || e.queue[1].TargetAuthID != "a" || e.queue[2].TargetAuthID != "b" {
		t.Fatalf("incorrect FIFO targets: %+v", e.queue)
	}
	e.expiryScan(now.Add(time.Minute))
	if len(e.queue) != 3 {
		t.Fatal("scan duplicated pending tasks")
	}
	e.queue = nil
	e.activeTask = &probeTask{Model: "gpt-6-astra", TargetAuthID: "a"}
	e.probing = map[string]bool{"gpt-6-astra": true}
	e.expiryScan(now)
	if len(e.queue) != 1 || e.queue[0].TargetAuthID != "b" {
		t.Fatal("active A must not swallow B or duplicate A")
	}
	e.activeTask = nil
	e.queue = nil
	e.halted = true
	e.expiryScan(now)
	if len(e.queue) != 0 {
		t.Fatal("stop-all ignored")
	}
	e.halted = false
	e.paused = map[string]bool{"gpt-6-astra": true}
	e.expiryScan(now)
	if len(e.queue) != 0 {
		t.Fatal("pause ignored")
	}
	e.paused = nil
	e.cfg.Config.Enabled = false
	e.expiryScan(now)
	if len(e.queue) != 0 {
		t.Fatal("probe disable ignored")
	}
}

func TestQueuedRenewalSkipsSupersededLease(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	now := time.Now().UTC()
	setHealthyAccountState(accountRouter, "a", "astra", strings.Repeat("a", 332), now.Add(-2*time.Hour))
	candidates := accountRouter.renewalCandidates(now)
	oldGeneration := renewalGeneration(candidates[0])
	setHealthyAccountState(accountRouter, "a", "astra", strings.Repeat("b", 332), now)
	if accountRouter.claimRenewal("a", "astra", oldGeneration, now) {
		t.Fatal("renewed lease consumed queued attempt")
	}
	if accountRouter.claimRenewal("a", "astra", oldGeneration, now.Add(2*time.Hour)) {
		t.Fatal("old queued generation applied to newer lease")
	}
	candidates = accountRouter.renewalCandidates(now.Add(2 * time.Hour))
	if len(candidates) != 1 || !accountRouter.claimRenewal("a", "astra", renewalGeneration(candidates[0]), now.Add(2*time.Hour)) {
		t.Fatal("new lease cannot renew independently")
	}
}

func TestRenewalPersistsBeforeOneNetworkAttempt(t *testing.T) {
	for _, broken := range []bool{false, true} {
		t.Run(map[bool]string{false: "once", true: "disk-failure"}[broken], func(t *testing.T) {
			setupSessionGuardTest(t, sessionGuardModeObserve)
			e := newPrefetchTestEngine(t)
			now := time.Now().UTC()
			setHealthyAccountState(accountRouter, "a", "astra", strings.Repeat("a", 332), now.Add(-2*time.Hour))
			oldList, oldGet := hostAuthListFunc, hostAuthGetFunc
			t.Cleanup(func() { hostAuthListFunc, hostAuthGetFunc = oldList, oldGet })
			hostAuthListFunc = func() ([]hostAuthEntry, error) {
				return []hostAuthEntry{{ID: "a", AuthIndex: "a", Provider: "codex"}}, nil
			}
			hostAuthGetFunc = func(string) (json.RawMessage, error) {
				return json.RawMessage(`{"access_token":"synthetic-test"}`), nil
			}
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				raw, err := os.ReadFile(stateFilePath())
				if err != nil {
					t.Error(err)
				}
				var state persistedState
				_ = json.Unmarshal(raw, &state)
				found := false
				for _, entry := range state.AccountHealth {
					if entry.AuthID == "a" && entry.RenewalAttemptedFor != "" {
						found = true
					}
				}
				if !found {
					t.Error("network request preceded durable claim")
				}
				w.WriteHeader(503)
			}))
			defer server.Close()
			e.cfg.Config.AccountMode = "highest-priority"
			e.cfg.Config.UpstreamURL = server.URL
			e.cfg.Config.Proxies = []string{"direct"}
			e.cfg.Config.Prefetch = 0
			e.cfg.Config.MaxAttemptsPerRound = 99
			if broken {
				blocker := filepath.Join(t.TempDir(), "file")
				if err := os.WriteFile(blocker, []byte("x"), 0600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("LKS_TZ_STATE_FILE", filepath.Join(blocker, "state.json"))
			}
			e.expiryScan(now)
			waitPrefetchIdle(t, e)
			expected := int32(1)
			if broken {
				expected = 0
			}
			if calls.Load() != expected {
				t.Fatalf("network calls=%d expected=%d", calls.Load(), expected)
			}
			e.expiryScan(now.Add(5 * time.Minute))
			waitPrefetchIdle(t, e)
			if calls.Load() != expected {
				t.Fatal("failed renewal repeated automatically")
			}
		})
	}
}
