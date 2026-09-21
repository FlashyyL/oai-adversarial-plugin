package main

import (
	"testing"
	"time"
)

func TestUsageProxyFailureDoesNotCoolAccount(t *testing.T) {
	m := newRoutingTestManager(true)
	now := time.Now().UTC()
	r := routingUsage("review-account", 0, 0, true)
	r.Failure.Body = "proxyconnect tcp: dial tcp: connection refused"
	m.observe(r, now)
	m.observe(r, now.Add(time.Second))
	e := m.health[accountHealthKey("review-account", "astra")]
	if e.State != "" {
		t.Fatalf("proxy-only failures cooled the account: state=%s reason=%s", e.State, e.LastReason)
	}
}

func TestHealthyEvidenceExpires(t *testing.T) {
	m := newRoutingTestManager(true)
	now := time.Now().UTC()
	m.observe(routingUsage("review-account", 332, 0, false), now)
	if p := m.pick(routingRequest("review-account", "unknown"), now.Add(2*time.Hour)); p.Handled {
		t.Fatal("two-hour-old health evidence still overrides native scheduling")
	}
}

func TestSuccessfulRequestResetsConsecutiveFailures(t *testing.T) {
	m := newRoutingTestManager(true)
	now := time.Now().UTC()
	m.observe(routingUsage("review-account", 0, 502, true), now)
	m.observe(routingUsage("review-account", 0, 0, false), now.Add(time.Second))
	m.observe(routingUsage("review-account", 0, 502, true), now.Add(2*time.Second))
	e := m.health[accountHealthKey("review-account", "astra")]
	if e.ConsecutiveFailures != 1 || e.State != "" {
		t.Fatalf("nonconsecutive failures cooled account: failures=%d", e.ConsecutiveFailures)
	}
}

func TestAccountWatchRespectsZeroPrefetch(t *testing.T) {
	old := hostAuthListFunc
	t.Cleanup(func() { hostAuthListFunc = old })
	selected := "a"
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{{AuthIndex: selected, Provider: "codex"}}, nil
	}
	e := &probeEngine{
		cfg:         probeConfigState{Config: probeConfig{Enabled: true, AccountMode: "highest-priority", Prefetch: 0, Models: []string{"gpt-6-astra"}}},
		queueActive: true, probing: map[string]bool{}, paused: map[string]bool{},
	}
	e.authSelectionScan()
	selected = "b"
	e.authSelectionScan()
	if len(e.queue) != 0 {
		t.Fatalf("account watcher queued %d probes with automatic prefetch disabled", len(e.queue))
	}
}

func TestUsageTransportAndRateLimitDoNotPenalizeAccount(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
	}{
		{0, "timeout"}, {407, "authentication required"}, {502, "proxyconnect tcp: connection refused"},
		{502, "socks connect failed"}, {429, "rate limited"},
	} {
		m := newRoutingTestManager(true)
		now := time.Now().UTC()
		m.observe(routingUsage("test-account", 332, 0, false), now)
		r := routingUsage("test-account", 356, tc.status, true)
		r.Failure.Body = tc.body
		m.observe(r, now.Add(time.Second))
		m.observe(r, now.Add(2*time.Second))
		e := m.health[accountHealthKey("test-account", "astra")]
		if tc.status == 429 {
			if e.State != "rate_limited" || e.ConsecutiveFailures != 0 || !e.HealthyAt.Equal(now) || !e.CooldownUntil.After(now) {
				t.Fatal("429 must temporarily leave candidates without degradation or TTL renewal")
			}
			continue
		}
		if e.State != "healthy" || e.ConsecutiveFailures != 0 || !e.CooldownUntil.IsZero() || !e.HealthyAt.Equal(now) {
			t.Fatalf("transport/rate limit polluted health for status %d", tc.status)
		}
	}
}

func TestSuccessWithoutStateDoesNotRefreshHealthyEvidence(t *testing.T) {
	m := newRoutingTestManager(true)
	now := time.Now().UTC()
	m.observe(routingUsage("test-account", 332, 0, false), now)
	m.observe(routingUsage("test-account", 0, 0, false), now.Add(59*time.Minute))
	if p := m.pick(routingRequest("test-account", "unknown"), now.Add(time.Hour)); p.Handled {
		t.Fatal("state-free success extended healthy evidence")
	}
	m.observe(routingUsage("test-account", 332, 0, false), now.Add(time.Hour))
	if p := m.pick(routingRequest("test-account", "unknown"), now.Add(time.Hour+time.Second)); !p.Handled {
		t.Fatal("fresh state did not restore healthy evidence")
	}
}

func TestAccountWatchRechecksStopAfterHostCallback(t *testing.T) {
	old := hostAuthListFunc
	t.Cleanup(func() { hostAuthListFunc = old })
	e := &probeEngine{
		cfg:         probeConfigState{Config: probeConfig{Enabled: true, AccountMode: "highest-priority", Prefetch: time.Minute, Models: []string{"gpt-6-astra"}}},
		queueActive: true, probing: map[string]bool{}, paused: map[string]bool{}, autoAuthSeen: true, lastAutoAuth: "a",
	}
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		e.mu.Lock()
		e.halted = true
		e.mu.Unlock()
		return []hostAuthEntry{{AuthIndex: "b", Provider: "codex"}}, nil
	}
	e.authSelectionScan()
	if len(e.queue) != 0 {
		t.Fatal("host callback racing stop queued a forced probe")
	}
}

func TestAccountWatchRespectsSleepBeforeAndAfterHostCallback(t *testing.T) {
	old := hostAuthListFunc
	t.Cleanup(func() { hostAuthListFunc = old })
	for _, duringCallback := range []bool{false, true} {
		e := &probeEngine{
			cfg:         probeConfigState{Config: probeConfig{Enabled: true, AccountMode: "highest-priority", Prefetch: time.Minute, Models: []string{"gpt-6-astra"}}},
			queueActive: true, probing: map[string]bool{}, paused: map[string]bool{}, autoAuthSeen: true, lastAutoAuth: "a",
		}
		calls := 0
		hostAuthListFunc = func() ([]hostAuthEntry, error) {
			calls++
			e.mu.Lock()
			e.cfg.Config.SleepHours = sleepingHoursNow()
			e.mu.Unlock()
			return []hostAuthEntry{{AuthIndex: "b", Provider: "codex"}}, nil
		}
		if !duringCallback {
			e.cfg.Config.SleepHours = sleepingHoursNow()
		}
		e.authSelectionScan()
		if len(e.queue) != 0 || e.lastAutoAuth != "a" || (!duringCallback && calls != 0) {
			t.Fatal("sleep was bypassed by the account watcher")
		}
	}
}
