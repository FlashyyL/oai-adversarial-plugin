package main

import (
	"strings"
	"testing"
	"time"
)

func TestAccountExpiryRespectsUpstreamSleepWindow(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	now := time.Now().UTC()
	setHealthyAccountState(accountRouter, "sleeping-owner", "astra", strings.Repeat("a", 332), now.Add(-2*time.Hour))
	old := hostAuthListFunc
	t.Cleanup(func() { hostAuthListFunc = old })
	calls := 0
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		calls++
		return []hostAuthEntry{{ID: "sleeping-owner", AuthIndex: "one", Provider: "codex"}}, nil
	}
	e := &probeEngine{cfg: probeConfigState{Config: probeConfig{Enabled: true, AccountMode: "highest-priority", Models: []string{"gpt-6-astra"}, SleepHours: sleepingHoursNow()}}, queueActive: true}
	e.expiryScan(now)
	if e.renewAccount("sleeping-owner", "gpt-6-astra", now) || calls != 0 || len(e.queue) != 0 {
		t.Fatal("account renewal bypassed upstream sleep")
	}
	if !accountRouter.needsRenewal("sleeping-owner", "astra", now) {
		t.Fatal("sleep consumed renewal attempt")
	}
	e.cfg.Config.SleepHours = probeSleepHours{}
	e.expiryScan(now)
	if calls != 1 || len(e.queue) != 1 || e.queue[0].TargetAuthID != "sleeping-owner" {
		t.Fatal("wake failed to queue owner renewal")
	}
}

func TestAccountLeaseFollowsConfiguredLengthPolicy(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	old := append([]int(nil), acceptedStateLengths()...)
	t.Cleanup(func() { stateLengthPolicy.Store(old) })
	now := time.Now().UTC()
	stateLengthPolicy.Store([]int{308})
	if !accountRouter.observeConfirmedState("owner", "gpt-6-astra", "gpt-6-astra", strings.Repeat("x", 308), now) {
		t.Fatal("configured account length not accepted")
	}
	stateLengthPolicy.Store([]int{332})
	if value, _ := accountRouter.accountTurnState("owner", "astra", now); value != "" {
		t.Fatal("disallowed account lease still injected")
	}
	if entry := accountRouter.health[accountHealthKey("owner", "astra")]; entry.State != "expired" || entry.LastReason != "account_state_policy_changed" {
		t.Fatal("policy change not reflected in routing")
	}
}

func TestExplicitQuotaEvidencePrecedesTransportFallback(t *testing.T) {
	m := newRoutingTestManager(true)
	r := routingUsage("owner", 356, 0, true)
	r.Failure.Body = `{"error":{"code":"usage_limit_reached"}}`
	m.observe(r, time.Now().UTC())
	if entry := m.health[accountHealthKey("owner", "astra")]; entry.State != "quota_exhausted" || entry.LastReason != "quota_exhausted" {
		t.Fatal("explicit quota evidence was hidden by missing HTTP status")
	}
}
