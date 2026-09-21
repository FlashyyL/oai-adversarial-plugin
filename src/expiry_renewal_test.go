package main

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestExpiredLeaseIsNotDegradedAndCanRecover(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	now := time.Now().UTC()
	setHealthyAccountState(accountRouter, "owner", "astra", strings.Repeat("a", 332), now.Add(-2*time.Hour))
	if !accountRouter.needsRenewal("owner", "astra", now) {
		t.Fatal("expired lease must request renewal")
	}
	if reason, _ := accountRouter.accountDegradedReason("owner", "astra", now); reason != "" {
		t.Fatal("expiry rejected business traffic")
	}
	entry := accountRouter.health[accountHealthKey("owner", "astra")]
	if entry.State != "expired" || entry.TurnStateValue != "" || !entry.CooldownUntil.IsZero() {
		t.Fatal("expiry must clear only the lease")
	}
	accountRouter.observeProbe("owner", "gpt-6-astra", probeRecord{Success: true, StatusCode: 200, StateLength: 332, ObservedModel: "gpt-6-astra"}, strings.Repeat("b", 332), now)
	if accountRouter.needsRenewal("owner", "astra", now) {
		t.Fatal("successful reprobe did not recover lease")
	}
}

func TestProbeFailureDoesNotReplaceLeaseOrBusinessVerdict(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	now := time.Now().UTC()
	setHealthyAccountState(accountRouter, "owner", "astra", strings.Repeat("a", 332), now)
	failure := probeRecord{StatusCode: 200, StateLength: 356, ObservedModel: "gpt-5.6-luna"}
	accountRouter.observeProbe("owner", "gpt-6-astra", failure, "", now)
	if entry := accountRouter.health[accountHealthKey("owner", "astra")]; entry.State != "healthy" || entry.TurnStateValue == "" {
		t.Fatal("preparation failure erased active lease")
	}
	if reason, _ := accountRouter.accountDegradedReason("owner", "astra", now.Add(2*time.Hour)); reason != "" {
		t.Fatal("probe failure became business rejection at expiry")
	}
	accountRouter.observe(routingUsage("owner", 356, 0, false), now)
	accountRouter.observeProbe("owner", "gpt-6-astra", failure, "", now)
	if reason, _ := accountRouter.accountDegradedReason("owner", "astra", now); reason == "" {
		t.Fatal("real business anomaly was cleared")
	}
	accountRouter.observeProbe("auth", "gpt-6-astra", probeRecord{StatusCode: http.StatusUnauthorized}, "", now)
	if reason, _ := accountRouter.accountDegradedReason("auth", "astra", now); reason == "" {
		t.Fatal("401 must remain an authentication failure")
	}
}

func TestOldProbeOnlyCooldownMigratesToPending(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	now := time.Now().UTC()
	accountRouter.health[accountHealthKey("owner", "astra")] = accountModelHealth{AuthID: "owner", Model: "astra", State: "degraded", LastReason: "probe_model_mismatch", CooldownUntil: now.Add(time.Hour)}
	if reason, _ := accountRouter.accountDegradedReason("owner", "astra", now); reason != "" {
		t.Fatal("legacy probe-only evidence still blocks")
	}
	if !accountRouter.needsRenewal("owner", "astra", now) {
		t.Fatal("legacy state did not become renewable")
	}
}

func TestAccountRejectionRespectsGlobalSwitch(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	original := probeTrack.rejectDegradedEnabled()
	t.Cleanup(func() { probeTrack.setRejectDegraded(original) })
	accountRouter.observe(routingUsage("owner", 356, 0, false), time.Now().UTC())
	probeTrack.setRejectDegraded(false)
	if degradedRejectMessageForAccount("owner", "gpt-6-astra") != "" {
		t.Fatal("disabled rejection still blocks accounts")
	}
	probeTrack.setRejectDegraded(true)
	if degradedRejectMessageForAccount("owner", "gpt-6-astra") == "" {
		t.Fatal("enabled rejection failed to protect business anomaly")
	}
}

func TestRenewalUsesFIFOAndIsBounded(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	e := &probeEngine{cfg: probeConfigState{Config: probeConfig{Enabled: true, AccountMode: "highest-priority"}}, queueActive: true}
	now := time.Now().UTC()
	for _, id := range []string{"owner", "other"} {
		setHealthyAccountState(accountRouter, id, "astra", strings.Repeat("a", 332), now.Add(-2*time.Hour))
	}
	if !e.renewAccount("owner", "gpt-6-astra", now) {
		t.Fatal("renewal not queued")
	}
	if len(e.queue) != 1 || e.queue[0].TargetAuthID != "owner" || e.queue[0].Force {
		t.Fatal("renewal must target its owner without overriding stops")
	}
	if !e.renewAccount("other", "gpt-6-astra", now) || len(e.queue) != 2 || e.queue[1].TargetAuthID != "other" {
		t.Fatal("different account must get its own FIFO task")
	}
	e.queue = nil
	if e.renewAccount("owner", "gpt-6-astra", now.Add(time.Second)) {
		t.Fatal("renewal retry was not throttled")
	}
	e.halted = true
	if e.renewAccount("owner", "gpt-6-astra", now.Add(2*time.Minute)) {
		t.Fatal("renewal ignored stop-all")
	}
	e.halted = false
	e.paused = map[string]bool{"gpt-6-astra": true}
	if e.renewAccount("owner", "gpt-6-astra", now.Add(2*time.Minute)) {
		t.Fatal("renewal ignored model pause")
	}
}

func TestExpiredClientStateIsRemovedWithoutRejecting(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	removeAuditRecord(t, "expired-client")
	out := provenanceInterceptRequest(t, "expired-client", "owner", "session", timedRoutingState(time.Now().Add(-2*time.Hour), 332))
	if out.Terminate || !responseClearsHeader(out.ClearHeaders, turnStateHeader) || out.Headers.Get(turnStateHeader) != "" {
		t.Fatal("expired client state was rejected or forwarded")
	}
}
