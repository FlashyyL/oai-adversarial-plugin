package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func concurrencyTestManager(t *testing.T) *concurrencyManager {
	t.Helper()
	t.Setenv("LKS_TZ_STATE_FILE", filepath.Join(t.TempDir(), "state.json"))
	m := newConcurrencyManager()
	m.loaded = true
	return m
}

func concurrencyTestPick(m *concurrencyManager, id string, candidates ...string) schedulerPickRequest {
	m.mu.Lock()
	m.requests[id] = &concurrencyRequest{ID: id, Token: id, Started: time.Now().UTC(), Status: "selecting"}
	m.tokens[id] = id
	m.mu.Unlock()
	req := schedulerPickRequest{Provider: "codex", Model: "gpt-6-astra"}
	req.Options.Headers = http.Header{concurrencyHeader: {id}}
	for _, auth := range candidates {
		req.Candidates = append(req.Candidates, schedulerAuthCandidate{ID: auth, Provider: "codex"})
	}
	return req
}

func TestAccountConcurrencyAtomicCapAndFailover(t *testing.T) {
	m := concurrencyTestManager(t)
	m.limits[accountScope("A")] = 1
	m.limits[accountScope("B")] = 1
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := concurrencyTestPick(m, fmt.Sprint(i), "A", "B")
			m.pick(req, schedulerPickResponse{AuthID: "A", Handled: true})
		}(i)
	}
	wg.Wait()
	if m.used[accountScope("A")] != 1 || m.used[accountScope("B")] != 1 {
		t.Fatalf("oversubscribed: %v", m.used)
	}
	reserved, rejected, rerouted := 0, 0, false
	for _, r := range m.requests {
		if r.Status == "reserved" {
			reserved++
			rerouted = rerouted || r.Rerouted
		} else if r.Status == "capacity_rejected" {
			rejected++
		}
	}
	if reserved != 2 || rejected != 62 || !rerouted {
		t.Fatalf("reservation counts %d/%d rerouted=%v", reserved, rejected, rerouted)
	}
	for id := range m.requests {
		m.finish(requestCompletion{RequestID: id, Outcome: "canceled"})
		m.finish(requestCompletion{RequestID: id, Outcome: "canceled"})
	}
	if len(m.requests) != 0 || len(m.tokens) != 0 || m.used[accountScope("A")] != 0 || m.used[accountScope("B")] != 0 {
		t.Fatal("leaked or double-released slots")
	}
}

func TestAccountConcurrencyRetryTransferAndLimitReduction(t *testing.T) {
	m := concurrencyTestManager(t)
	m.limits[accountScope("A")] = 1
	m.limits[accountScope("B")] = 1
	req := concurrencyTestPick(m, "retry", "A")
	if m.pick(req, schedulerPickResponse{}).AuthID != "A" {
		t.Fatal("first selection")
	}
	if err := m.admit("retry", "A", "gpt-5.6-luna", "business"); err != nil {
		t.Fatal(err)
	}
	if err := m.admit("retry", "A", "gpt-5.6-luna", "business"); err != nil || m.used[accountScope("A")] != 1 {
		t.Fatal("same attempt counted twice")
	}
	req.Candidates = []schedulerAuthCandidate{{ID: "B", Provider: "codex"}}
	m.pick(req, schedulerPickResponse{})
	if m.used[accountScope("A")] != 0 || m.used[accountScope("B")] != 1 || !m.requests["retry"].Rerouted {
		t.Fatal("retry transfer")
	}
	m.finish(requestCompletion{RequestID: "retry", Outcome: "failed", CompletedAt: time.Now().Add(-time.Hour)})
	if m.used[accountScope("B")] != 1 {
		t.Fatal("old completion released new attempt")
	}
	m.finish(requestCompletion{RequestID: "retry", Outcome: "succeeded", Metadata: map[string]any{"selected_auth_id": "A"}})
	if m.used[accountScope("B")] != 1 {
		t.Fatal("wrong account completion released new attempt")
	}
	m.limits[accountScope("B")] = 0
	if err := m.admit("second", "B", "gpt-6-sol", "business"); err != nil {
		t.Fatal(err)
	}
	if err := m.save(accountScope("B"), 1); err != nil {
		t.Fatal(err)
	}
	if m.used[accountScope("B")] != 2 {
		t.Fatal("limit change terminated streams")
	}
	if err := m.admit("third", "B", "gpt-6-sol", "business"); err == nil {
		t.Fatal("new request ignored reduced limit")
	}
	m.finish(requestCompletion{RequestID: "retry", Outcome: "succeeded"})
	m.finish(requestCompletion{RequestID: "second", Outcome: "failed"})
	if err := m.admit("third", "B", "gpt-6-sol", "business"); err != nil {
		t.Fatal(err)
	}
}

func TestAccountConcurrencyPersistenceAndInvalidStorage(t *testing.T) {
	m := concurrencyTestManager(t)
	key := accountScope("native")
	if err := m.save(key, 8); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{-1, maxAccountConcurrency + 1} {
		if err := m.save(key, limit); err == nil {
			t.Fatal("invalid limit accepted")
		}
	}
	fresh := newConcurrencyManager()
	fresh.mu.Lock()
	fresh.loadLocked()
	fresh.mu.Unlock()
	if fresh.limits[key] != 8 || len(fresh.requests) != 0 {
		t.Fatal("settings/active restart semantics")
	}
	if err := os.WriteFile(concurrencySettingsPath(), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	broken := newConcurrencyManager()
	if err := broken.admit("request", "native", "model", "business"); err == nil {
		t.Fatal("corrupt settings bypassed")
	}
	if err := broken.save(key, 1); err == nil {
		t.Fatal("corrupt settings overwritten")
	}
	raw, _ := os.ReadFile(concurrencySettingsPath())
	if string(raw) != "broken" {
		t.Fatal("original lost")
	}
	if err := os.Remove(concurrencySettingsPath()); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(concurrencySettingsPath(), 0700); err != nil {
		t.Fatal(err)
	}
	if err := m.save(key, 2); err == nil || m.limits[key] != 8 {
		t.Fatal("failed write changed live limit")
	}
}

func TestAccountSettingsAtomicSaveAndPartialCompatibility(t *testing.T) {
	m := concurrencyTestManager(t)
	key := accountScope("A")
	limit, threshold := 4, 90.5
	if err := m.saveSettings(key, &limit, &threshold); err != nil {
		t.Fatal(err)
	}
	var saved concurrencySettings
	raw, err := os.ReadFile(concurrencySettingsPath())
	if err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(raw, &saved) != nil || saved.Limits[key] != 4 || saved.WeeklyThresholds[key] != 90.5 {
		t.Fatal("joint settings not persisted")
	}
	if err := m.save(key, 6); err != nil || m.weeklyThresholds[key] != 90.5 {
		t.Fatal("legacy limit save changed threshold")
	}
	if err := m.saveWeeklyThreshold(key, 75); err != nil || m.limits[key] != 6 {
		t.Fatal("legacy threshold save changed limit")
	}
	before, err := os.ReadFile(concurrencySettingsPath())
	if err != nil {
		t.Fatal(err)
	}
	invalid := 101.0
	if err := m.saveSettings(key, &limit, &invalid); err == nil {
		t.Fatal("invalid threshold accepted")
	}
	after, _ := os.ReadFile(concurrencySettingsPath())
	if string(after) != string(before) || m.limits[key] != 6 || m.weeklyThresholds[key] != 75 {
		t.Fatal("invalid request partially saved")
	}
	if err := os.Remove(concurrencySettingsPath()); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(concurrencySettingsPath(), 0700); err != nil {
		t.Fatal(err)
	}
	if err := m.saveSettings(key, &limit, &threshold); err == nil || m.limits[key] != 6 || m.weeklyThresholds[key] != 75 {
		t.Fatal("write failure published partial settings")
	}
}

func TestAccountSettingsManagementJointValidation(t *testing.T) {
	m := concurrencyTestManager(t)
	oldManager, oldList := accountConcurrency, hostAuthListFunc
	accountConcurrency = m
	t.Cleanup(func() { accountConcurrency, hostAuthListFunc = oldManager, oldList })
	hostAuthListFunc = func() ([]hostAuthEntry, error) { return []hostAuthEntry{{ID: "A", Provider: "codex"}}, nil }
	key := accountScope("A")
	body, _ := json.Marshal(map[string]any{"account": key, "limit": 3, "weekly_threshold": 85.5})
	if response := concurrencyManagement(body, true); response.StatusCode != 200 || m.limits[key] != 3 || m.weeklyThresholds[key] != 85.5 {
		t.Fatal("management joint save failed")
	}
	for _, payload := range []map[string]any{
		{"account": key, "limit": 2, "weekly_threshold": 101},
		{"account": key, "limit": -1, "weekly_threshold": 80},
		{"account": key, "limit": 1.5, "weekly_threshold": 80},
		{"account": key},
	} {
		body, _ := json.Marshal(payload)
		if response := concurrencyManagement(body, true); response.StatusCode != 400 || m.limits[key] != 3 || m.weeklyThresholds[key] != 85.5 {
			t.Fatal("invalid joint API request changed state")
		}
	}
}

func TestAccountConcurrencyPinnedWaitAndCancellation(t *testing.T) {
	m := concurrencyTestManager(t)
	m.limits[accountScope("A")] = 1
	if err := m.admit("business", "A", "gpt-6-astra", "business"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.internal(ctx, "evaluation", "A", "gpt-6-astra", "evaluation", true) }()
	deadline := time.Now().Add(time.Second)
	for {
		state, _, _ := m.state("evaluation")
		if state == "waiting" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no capacity wait")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("canceled waiter admitted")
	}
	if m.used[accountScope("A")] != 1 {
		t.Fatal("waiter consumed/released owner's slot")
	}
	if err := m.internal(context.Background(), "probe-busy", "A", "model", "probe", false); err == nil {
		t.Fatal("probe bypassed limit")
	}
	if _, ok := m.requests["probe-busy"]; ok {
		t.Fatal("busy probe retained a phantom request")
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { done <- m.internal(ctx, "evaluation2", "A", "gpt-6-astra", "evaluation", true) }()
	m.finish(requestCompletion{RequestID: "business", Outcome: "failed"})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if m.used[accountScope("A")] != 1 {
		t.Fatal("waiter didn't claim freed slot")
	}
}

func TestAccountConcurrencyHeaderBridgeAndNativeGuard(t *testing.T) {
	m := concurrencyTestManager(t)
	history.mu.Lock()
	previousHistory := history.records
	history.records = nil
	history.mu.Unlock()
	t.Cleanup(func() { history.mu.Lock(); history.records = previousHistory; history.mu.Unlock() })
	previous, previousList := accountConcurrency, hostAuthListFunc
	accountConcurrency = m
	t.Cleanup(func() { accountConcurrency, hostAuthListFunc = previous, previousList })
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{{ID: "A", Provider: "codex"}, {ID: "B", Provider: "codex"}, {ID: "virtual", Provider: "oai-basispoints"}}, nil
	}
	m.limits[accountScope("A")] = 1
	m.limits[accountScope("B")] = 1
	before := func(id string) interceptResponse {
		raw, _ := json.Marshal(interceptRequest{RequestID: id, RequestedModel: "gpt-6-astra", Headers: http.Header{concurrencyHeader: {"client-forged"}}})
		r, err := concurrencyBefore(raw)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	b := before("one")
	if b.Headers.Get(concurrencyHeader) == "client-forged" || b.Headers.Get(concurrencyHeader) == "" {
		t.Fatal("client input trusted")
	}
	req := schedulerPickRequest{Provider: "codex", Candidates: []schedulerAuthCandidate{{ID: "A", Provider: "codex"}, {ID: "B", Provider: "codex"}}}
	req.Options.Headers = b.Headers
	m.pick(req, schedulerPickResponse{AuthID: "A", Handled: true})
	guard := concurrencyAfter(interceptRequest{RequestID: "one", ToFormat: "codex", Metadata: map[string]any{"selected_auth_id": "A"}})
	if guard.Terminate || m.used[accountScope("A")] != 1 {
		t.Fatal("reserved admission")
	}
	stripped := stripConcurrencyHeader(b.Headers, guard)
	if stripped.Headers.Get(concurrencyHeader) != "" || len(stripped.ClearHeaders) != 1 {
		t.Fatal("internal header leaked")
	}
	req.Options.Headers = before("two").Headers
	if picked := m.pick(req, schedulerPickResponse{AuthID: "A", Handled: true}); picked.AuthID != "B" {
		t.Fatalf("not rerouted: %+v", picked)
	}
	guard = concurrencyAfter(interceptRequest{RequestID: "two", ToFormat: "codex", Metadata: map[string]any{"selected_auth_id": "B"}})
	if guard.Terminate {
		t.Fatal("B unavailable")
	}
	req.Options.Headers = before("three").Headers
	picked := m.pick(req, schedulerPickResponse{})
	guard = concurrencyAfter(interceptRequest{RequestID: "three", ToFormat: "codex", Metadata: map[string]any{"selected_auth_id": picked.AuthID}})
	if !guard.Terminate || guard.StatusCode != 429 || guard.ResponseHeaders.Get("Retry-After") == "" {
		t.Fatal("full account allowed")
	}
	if guard = concurrencyAfter(interceptRequest{RequestID: "virtual", ToFormat: "codex", Metadata: map[string]any{"selected_auth_id": "virtual"}}); guard.Terminate {
		t.Fatal("virtual route affected")
	}
	if guard = concurrencyAfter(interceptRequest{RequestID: "untracked", ToFormat: "codex"}); !guard.Terminate || guard.StatusCode != 503 {
		t.Fatal("missing selected identity bypassed configured limits")
	}
	noToken := req
	noToken.Options.Headers = nil
	if got := m.pick(noToken, schedulerPickResponse{}); got.Handled {
		t.Fatal("unbound auth lookup reserved")
	}
}

func TestAccountConcurrencyIndependentOfAuditRetention(t *testing.T) {
	m := concurrencyTestManager(t)
	m.limits[accountScope("A")] = 1
	m.admit("long-stream", "A", "model", "business")
	for i := 0; i < 250; i++ {
		id := fmt.Sprint(i)
		m.admit(id, "B", "model", "business")
		m.finish(requestCompletion{RequestID: id, Outcome: "succeeded"})
	}
	if m.used[accountScope("A")] != 1 || len(m.recent) != 50 {
		t.Fatal("history pruning released stream")
	}
	if err := m.admit("new", "A", "model", "business"); err == nil {
		t.Fatal("long stream no longer limited")
	}
	m.finish(requestCompletion{RequestID: "long-stream", Outcome: "canceled"})
	if err := m.admit("new", "A", "model", "business"); err != nil {
		t.Fatal(err)
	}
}

func TestAccountConcurrencyWaitingEvaluationRevalidatesIdentity(t *testing.T) {
	previous := accountConcurrency
	m := newConcurrencyManager()
	m.loaded = true
	accountConcurrency = m
	t.Cleanup(func() { accountConcurrency = previous })
	e, f := newEvalFixture(t)
	accounts, err := e.evalAccounts()
	if err != nil || len(accounts) == 0 {
		t.Fatal("fixture inventory")
	}
	auth := accounts[0]
	m.limits[accountScope(auth.ID)] = 1
	if err := m.admit("owner", auth.ID, "gpt-6-astra", "business"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := executeEvalModel(ctx, auth, "gpt-6-astra", "synthetic", "none", &evalRequestAudit{ID: "waiting-identity"})
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		state, _, _ := m.state("waiting-identity")
		if state == "waiting" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not waiting")
		}
		time.Sleep(time.Millisecond)
	}
	f.mu.Lock()
	for i := range f.Entries {
		if f.Entries[i].ID == auth.ID {
			f.Entries[i].Disabled = true
		}
	}
	f.mu.Unlock()
	m.finish(requestCompletion{RequestID: "owner", Outcome: "succeeded"})
	if err := <-done; err == nil || err.Error() != "account_unavailable_or_changed" {
		t.Fatalf("identity not rechecked: %v", err)
	}
	f.mu.Lock()
	calls := len(f.Calls)
	f.mu.Unlock()
	if calls != 0 || m.used[accountScope(auth.ID)] != 0 {
		t.Fatal("disabled auth dispatched or leaked slot")
	}
}
