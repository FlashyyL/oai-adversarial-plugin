package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"os"
	"testing"
	"time"
)

func quotaTestSnapshot(id string, used float64) weeklyQuotaSnapshot {
	now := time.Now().UTC()
	return weeklyQuotaSnapshot{Scope: credentialScope(id, "synthetic-token", "synthetic-workspace"), UsedPercent: used, ObservedAt: now, CheckedAt: now, ResetAt: now.Add(time.Hour)}
}

func TestWeeklyQuotaUnlimitedConcurrencyStillFiltersAndRecovers(t *testing.T) {
	m := concurrencyTestManager(t)
	a, b := accountScope("A"), accountScope("B")
	m.weeklyThresholds[a], m.weeklyThresholds[b] = 90, 90
	m.weeklyUsage[a], m.weeklyUsage[b] = quotaTestSnapshot("A", 90), quotaTestSnapshot("B", 89.9)
	req := concurrencyTestPick(m, "one", "A", "B")
	if pick := m.pick(req, schedulerPickResponse{AuthID: "A", Handled: true}); pick.AuthID != "B" {
		t.Fatal("threshold did not reroute with concurrency=0")
	}
	if err := m.admit("one", "B", "model", "business"); err != nil {
		t.Fatal(err)
	}
	m.weeklyUsage[b] = quotaTestSnapshot("B", 95)
	if m.used[b] != 1 {
		t.Fatal("threshold interrupted an in-flight request")
	}
	if err := m.admit("two", "B", "model", "business"); err == nil || err.Error() != "account_weekly_quota_exceeded" {
		t.Fatal("new request bypassed quota")
	}
	req = concurrencyTestPick(m, "all-full", "A", "B")
	m.pick(req, schedulerPickResponse{})
	if m.requests["all-full"].Status != "quota_rejected" || m.used[a] != 0 || m.used[b] != 1 {
		t.Fatal("all paused pool admitted a request")
	}
	q := quotaTestSnapshot("A", 90)
	q.ResetAt = time.Now().Add(-time.Second)
	m.weeklyUsage[a] = q
	if m.weeklyGateLocked(a, time.Now()) != "account_weekly_quota_unavailable" {
		t.Fatal("old reset invented fresh quota")
	}
	m.observeWeekly(a, quotaTestSnapshot("A", 0))
	req = concurrencyTestPick(m, "reset", "A", "B")
	if m.pick(req, schedulerPickResponse{}).AuthID != "A" || m.requests["reset"].Status != "reserved" {
		t.Fatal("verified quota reset did not recover")
	}
	m.finish(requestCompletion{RequestID: "one", Outcome: "succeeded"})
	if m.used[b] != 0 {
		t.Fatal("completion after quota pause leaked concurrency")
	}
}

func TestWeeklyQuotaPersistenceOldSettingsAndWriteFailure(t *testing.T) {
	m := concurrencyTestManager(t)
	key := accountScope("A")
	if err := m.save(key, 7); err != nil {
		t.Fatal(err)
	}
	if err := m.saveWeeklyThreshold(key, 90.5); err != nil {
		t.Fatal(err)
	}
	m.observeWeekly(key, quotaTestSnapshot("A", 92))
	m.stopWeekly()
	fresh := newConcurrencyManager()
	fresh.mu.Lock()
	fresh.loadLocked()
	fresh.mu.Unlock()
	if fresh.limits[key] != 7 || fresh.weeklyThresholds[key] != 90.5 || fresh.weeklyGateLocked(key, time.Now()) != "account_weekly_quota_exceeded" || len(fresh.requests) != 0 {
		t.Fatal("restart lost limits/quota or resurrected activities")
	}
	for _, threshold := range []float64{-1, 100.1, math.NaN(), math.Inf(1)} {
		if m.saveWeeklyThreshold(key, threshold) == nil {
			t.Fatal("invalid threshold accepted")
		}
	}
	if err := m.saveWeeklyThreshold(key, 0); err != nil {
		t.Fatal(err)
	}
	if m.weeklyGateLocked(key, time.Now()) != "" || m.limits[key] != 7 {
		t.Fatal("disabling quota changed concurrency")
	}
	old, _ := json.Marshal(concurrencySettings{Version: 1, Limits: map[string]int{key: 4}})
	if err := os.WriteFile(concurrencySettingsPath(), old, 0600); err != nil {
		t.Fatal(err)
	}
	legacy := newConcurrencyManager()
	legacy.loadLocked()
	if legacy.loadError != "" || legacy.limits[key] != 4 || legacy.weeklyGateLocked(key, time.Now()) != "" {
		t.Fatal("legacy settings not compatible")
	}
	if err := os.Remove(concurrencySettingsPath()); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(concurrencySettingsPath(), 0700); err != nil {
		t.Fatal(err)
	}
	if err := fresh.saveWeeklyThreshold(key, 60); err == nil || fresh.weeklyThresholds[key] != 90.5 {
		t.Fatal("write failure published a new threshold")
	}
}

func TestWeeklyQuotaWindowsAndMissingZero(t *testing.T) {
	now := time.Now().UTC()
	for _, weekly := range []string{"primary", "secondary"} {
		h := http.Header{}
		h.Set("x-codex-"+weekly+"-window-minutes", "10080")
		h.Set("x-codex-"+weekly+"-used-percent", "0")
		h.Set("x-codex-"+weekly+"-reset-after-seconds", "3600")
		q, ok := weeklyQuotaFromHeaders(h, now.Add(-time.Second), now)
		if !ok || q.UsedPercent != 0 || !q.ResetAt.Equal(now.Add(time.Hour)) {
			t.Fatal("weekly header/real zero not recognized")
		}
		h.Del("x-codex-" + weekly + "-used-percent")
		if _, ok := weeklyQuotaFromHeaders(h, now, now); ok {
			t.Fatal("missing usage became zero")
		}
	}
	for _, raw := range []string{
		`{"rate_limit":{"primary_window":{"used_percent":99,"limit_window_seconds":18000},"secondary_window":{"used_percent":0,"limit_window_seconds":604800,"reset_after_seconds":3600}}}`,
		`{"rate_limit":{"primary_window":{"used_percent":0,"limit_window_seconds":604800,"reset_after_seconds":3600},"secondary_window":null}}`,
	} {
		q, err := weeklyQuotaFromJSON([]byte(raw), now, now)
		if err != nil || q.UsedPercent != 0 {
			t.Fatal("7d JSON window misidentified")
		}
	}
	for _, raw := range []string{`{}`, `{"rate_limit":{"primary_window":{"limit_window_seconds":604800}}}`, `{"rate_limit":{"primary_window":{"used_percent":null,"limit_window_seconds":604800}}}`, `{"rate_limit":{"primary_window":{"used_percent":-1,"limit_window_seconds":604800}}}`, `{"rate_limit":{"primary_window":{"used_percent":0,"limit_window_seconds":18000}}}`} {
		if _, err := weeklyQuotaFromJSON([]byte(raw), now, now); err == nil {
			t.Fatal("missing/invalid/nonweekly quota fabricated")
		}
	}
}

func TestWeeklyQuotaLateSnapshotsIdentityAndPinnedRequests(t *testing.T) {
	m := concurrencyTestManager(t)
	key := accountScope("A")
	m.weeklyThresholds[key] = 80
	fresh := quotaTestSnapshot("A", 95)
	m.observeWeekly(key, fresh)
	late := fresh
	late.UsedPercent = 0
	late.ObservedAt = fresh.ObservedAt.Add(-time.Minute)
	m.observeWeekly(key, late)
	changed := quotaTestSnapshot("replaced-account", 0)
	m.observeWeekly(key, changed)
	if m.weeklyUsage[key].UsedPercent != 95 {
		t.Fatal("late/different credential cleared threshold")
	}
	if err := m.internal(context.Background(), "eval", "A", "model", "evaluation", true); err == nil || err.Error() != "account_weekly_quota_exceeded" {
		t.Fatal("fixed-account evaluation bypassed quota or switched account")
	}
	if _, exists := m.requests["eval"]; exists {
		t.Fatal("quota rejection invented activity")
	}
	m.checkWeeklyIdentity(key, changed.Scope)
	if m.weeklyGateLocked(key, time.Now()) != "account_weekly_quota_unavailable" {
		t.Fatal("replaced credential borrowed old observation")
	}
	m.observeWeekly(key, changed)
	if m.weeklyGateLocked(key, time.Now()) != "" {
		t.Fatal("new verified identity failed to become available")
	}
}

func TestWeeklyQuotaRefreshDoesNotEnableDisabledAccountsOrForgeReset(t *testing.T) {
	m := concurrencyTestManager(t)
	oldList, oldGet, oldFetch := hostAuthListFunc, hostAuthGetFunc, weeklyQuotaFetch
	t.Cleanup(func() { hostAuthListFunc, hostAuthGetFunc, weeklyQuotaFetch = oldList, oldGet, oldFetch })
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{{ID: "A", AuthIndex: "A-index", Provider: "codex"}, {ID: "B", AuthIndex: "B-index", Provider: "codex", Disabled: true}}, nil
	}
	hostAuthGetFunc = func(string) (json.RawMessage, error) {
		return json.RawMessage(`{"access_token":"synthetic-token","account_id":"synthetic-workspace"}`), nil
	}
	m.weeklyThresholds[accountScope("A")], m.weeklyThresholds[accountScope("B")] = 80, 80
	m.observeWeekly(accountScope("A"), quotaTestSnapshot("A", 90))
	calls := 0
	weeklyQuotaFetch = func(ctx context.Context, entry hostAuthEntry) (weeklyQuotaSnapshot, error) {
		calls++
		if entry.ID != "A" {
			t.Error("disabled account queried")
		}
		return weeklyQuotaSnapshot{}, errors.New("weekly_quota_query_failed")
	}
	m.refreshWeekly(context.Background())
	if calls != 1 || m.weeklyGateLocked(accountScope("A"), time.Now()) != "account_weekly_quota_exceeded" {
		t.Fatal("query failure cleared real threshold evidence")
	}
	delete(m.weeklyNext, accountScope("A"))
	weeklyQuotaFetch = func(ctx context.Context, entry hostAuthEntry) (weeklyQuotaSnapshot, error) {
		return quotaTestSnapshot("A", 0), nil
	}
	m.refreshWeekly(context.Background())
	if m.weeklyGateLocked(accountScope("A"), time.Now()) != "" || m.weeklyGateLocked(accountScope("B"), time.Now()) == "" {
		t.Fatal("reset failed or disabled account acquired quota")
	}
}

func TestWeeklyQuotaSnapshotWriteFailureDoesNotBlockUnrestrictedAccounts(t *testing.T) {
	m := concurrencyTestManager(t)
	key := accountScope("A")
	m.weeklyThresholds[key] = 90
	m.observeWeekly(key, quotaTestSnapshot("A", 95))
	if err := os.Mkdir(concurrencySettingsPath(), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m.refreshWeekly(ctx)
	if m.loadError != "" || m.weeklyStorageError == "" || !m.weeklyDirty {
		t.Fatal("quota snapshot failure corrupted the saved controls")
	}
	if err := m.admit("unrestricted", "B", "model", "business"); err != nil {
		t.Fatal("unrelated unlimited account blocked", err)
	}
	if m.weeklyGateLocked(key, time.Now()) != "account_weekly_quota_exceeded" {
		t.Fatal("storage failure discarded live evidence")
	}
}
