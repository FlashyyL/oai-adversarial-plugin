package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func timedRoutingState(generated time.Time, encodedLength int) string {
	rawLength := encodedLength * 3 / 4
	raw := make([]byte, rawLength)
	raw[0] = 0x80
	binary.BigEndian.PutUint64(raw[1:9], uint64(generated.Unix()))
	return base64.URLEncoding.EncodeToString(raw)
}

func newRoutingTestManager(enabled bool) *accountRoutingManager {
	cfg := defaultAccountRoutingConfig()
	cfg.Enabled = enabled
	return &accountRoutingManager{
		config: accountRoutingConfigState{Config: cfg},
		health: map[string]accountModelHealth{},
	}
}

func routingUsage(authID string, stateLength, status int, failed bool) usageRecord {
	headers := http.Header{}
	if stateLength > 0 {
		headers.Set(turnStateHeader, strings.Repeat("s", stateLength))
	}
	return usageRecord{
		Provider: "codex", Model: "gpt-6-astra", AuthID: authID,
		Failed: failed, Failure: usageFailure{StatusCode: status}, ResponseHeaders: headers,
	}
}

func routingRequest(ids ...string) schedulerPickRequest {
	candidates := make([]schedulerAuthCandidate, 0, len(ids))
	for _, id := range ids {
		candidates = append(candidates, schedulerAuthCandidate{ID: id, Provider: "codex", Priority: 10, Status: "active"})
	}
	return schedulerPickRequest{Provider: "codex", Model: "gpt-6-astra", Candidates: candidates}
}

func TestAccountRoutingUsageAndSchedulerAvoidDegraded(t *testing.T) {
	m := newRoutingTestManager(true)
	now := time.Date(2026, 9, 20, 6, 0, 0, 0, time.UTC)
	m.observe(routingUsage("healthy", 332, 0, false), now)
	m.observe(routingUsage("degraded", 356, 0, false), now)
	got := m.pick(routingRequest("degraded", "healthy"), now.Add(time.Minute))
	if !got.Handled || got.AuthID != "healthy" {
		t.Fatalf("pick = %+v", got)
	}
	entry := m.health[accountHealthKey("degraded", "astra")]
	if entry.State != "degraded" || entry.LastStateLength != 356 {
		t.Fatalf("degraded entry = %+v", entry)
	}
}

func TestAccountRoutingKeepsBuiltinForAllUnknownOrNoHealthy(t *testing.T) {
	m := newRoutingTestManager(true)
	now := time.Now().UTC()
	if got := m.pick(routingRequest("a", "b"), now); got.Handled {
		t.Fatalf("all unknown should preserve builtin: %+v", got)
	}
	m.observe(routingUsage("a", 356, 0, false), now)
	m.observe(routingUsage("b", 356, 0, false), now)
	if got := m.pick(routingRequest("a", "b"), now.Add(time.Minute)); got.Handled {
		t.Fatalf("all cooling should fail open to builtin: %+v", got)
	}
}

func TestAccountRoutingChoosesUnknownInsteadOfCoolingCandidate(t *testing.T) {
	m := newRoutingTestManager(true)
	now := time.Now().UTC()
	m.observe(routingUsage("bad", 312, 0, false), now)
	got := m.pick(routingRequest("bad", "new"), now.Add(time.Minute))
	if !got.Handled || got.AuthID != "new" {
		t.Fatalf("pick = %+v", got)
	}
}

func TestAccountRoutingAcceptsEmptyCandidateProvider(t *testing.T) {
	m := newRoutingTestManager(true)
	now := time.Now().UTC()
	m.observe(routingUsage("healthy", 332, 0, false), now)
	req := routingRequest("healthy", "other")
	for i := range req.Candidates {
		req.Candidates[i].Provider = ""
	}
	got := m.pick(req, now.Add(time.Minute))
	if !got.Handled || got.AuthID != "healthy" {
		t.Fatalf("pick = %+v; diagnostic = %+v", got, m.lastScheduler)
	}
	if m.schedulerCalls != 1 || m.lastScheduler.EligibleCount != 2 || m.lastScheduler.Selected != publicAccountID("healthy") {
		t.Fatalf("diagnostic = %+v", m.lastScheduler)
	}
}

func TestAccountRoutingDiagnosticsExplainNoCandidates(t *testing.T) {
	m := newRoutingTestManager(true)
	req := routingRequest("foreign")
	req.Candidates[0].Provider = "claude"
	if got := m.pick(req, time.Now().UTC()); got.Handled {
		t.Fatalf("foreign candidate should not be handled: %+v", got)
	}
	if m.schedulerCalls != 1 || m.lastScheduler.Outcome != "no_eligible_candidates" || m.lastScheduler.CandidateCount != 1 {
		t.Fatalf("diagnostic = %+v", m.lastScheduler)
	}
}

func TestAccountRouting401AndTransientThreshold(t *testing.T) {
	m := newRoutingTestManager(true)
	now := time.Now().UTC()
	m.observe(routingUsage("revoked", 0, http.StatusUnauthorized, true), now)
	if got := m.health[accountHealthKey("revoked", "astra")]; got.State != "auth_error" {
		t.Fatalf("revoked = %+v", got)
	}
	m.observe(routingUsage("flaky", 0, http.StatusBadGateway, true), now)
	if got := m.health[accountHealthKey("flaky", "astra")]; got.State == "transient_failure" {
		t.Fatalf("first transient failure must not cool down: %+v", got)
	}
	m.observe(routingUsage("flaky", 0, http.StatusBadGateway, true), now.Add(time.Second))
	if got := m.health[accountHealthKey("flaky", "astra")]; got.State != "transient_failure" {
		t.Fatalf("second transient failure = %+v", got)
	}
}

func TestAccountRoutingRateLimitCoolsDownWithoutDegradation(t *testing.T) {
	m := newRoutingTestManager(true)
	m.observe(routingUsage("limited", 0, http.StatusTooManyRequests, true), time.Now().UTC())
	got := m.health[accountHealthKey("limited", "astra")]
	if got.State != "rate_limited" || got.CooldownUntil.IsZero() || got.LastReason != "rate_limited" {
		t.Fatalf("rate limited = %+v", got)
	}
}

func TestAccountRoutingConsumesProbeEvidence(t *testing.T) {
	m := newRoutingTestManager(true)
	now := time.Now().UTC()
	m.observeProbe("healthy", "gpt-6-astra", probeRecord{
		Success: true, StatusCode: http.StatusOK, StateLength: 332, ObservedModel: "gpt-6-astra",
	}, strings.Repeat("h", 332), now)
	m.observeProbe("degraded", "gpt-6-astra", probeRecord{
		StatusCode: http.StatusOK, StateLength: 356, ObservedModel: "gpt-5.6-luna",
	}, strings.Repeat("d", 356), now)
	got := m.pick(routingRequest("degraded", "healthy"), now.Add(time.Minute))
	if !got.Handled || got.AuthID != "healthy" {
		t.Fatalf("pick = %+v", got)
	}
	if entry := m.health[accountHealthKey("degraded", "astra")]; entry.State != "probe_pending" || entry.ProbeReason != "probe_state_length_356" {
		t.Fatalf("degraded probe entry = %+v", entry)
	}
}

func TestAccountRoutingProbeIgnoresTransportFailure(t *testing.T) {
	m := newRoutingTestManager(true)
	m.observeProbe("candidate", "gpt-6-astra", probeRecord{Error: "proxy timeout"}, "", time.Now().UTC())
	if len(m.health) != 0 {
		t.Fatalf("transport failure must not change account health: %+v", m.health)
	}
}

func TestAccountRoutingNoStateKeepsProbeEvidence(t *testing.T) {
	m := newRoutingTestManager(true)
	now := time.Now().UTC()
	m.observeProbe("healthy", "gpt-6-astra", probeRecord{
		Success: true, StatusCode: http.StatusOK, StateLength: 332, ObservedModel: "gpt-6-astra",
	}, strings.Repeat("h", 332), now)
	m.observe(routingUsage("healthy", 0, 0, false), now.Add(time.Minute))
	entry := m.health[accountHealthKey("healthy", "astra")]
	if entry.State != "healthy" || entry.LastStateLength != 332 || entry.LastReason != "probe_healthy" {
		t.Fatalf("no-state usage erased probe evidence: %+v", entry)
	}
}

func TestConfigureAccountRoutingAliases(t *testing.T) {
	original := accountRouter
	accountRouter = newRoutingTestManager(false)
	t.Cleanup(func() { accountRouter = original })
	if err := configureAccountRouting([]byte("experimental-account-routing: true\naccount-degraded-cooldown-minutes: 90\naccount-failure-cooldown-minutes: 7\naccount-failure-threshold: 3\n")); err != nil {
		t.Fatal(err)
	}
	summary := accountRoutingSummary()
	if summary["enabled"] != true || summary["failure_threshold"] != 3 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestAccountRoutingRPCShapes(t *testing.T) {
	original := accountRouter
	accountRouter = newRoutingTestManager(true)
	t.Cleanup(func() { accountRouter = original })
	accountRouter.observeConfirmedState("healthy", "gpt-6-astra", "gpt-6-astra", strings.Repeat("h", 332), time.Now().UTC())
	raw, _ := json.Marshal(routingRequest("healthy", "other"))
	result, err := pickAccountForRequest(raw)
	if err != nil || !result.Handled || result.AuthID != "healthy" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	usageRaw, _ := json.Marshal(routingUsage("other", 356, 0, false))
	if err := observeAccountUsage(usageRaw); err != nil {
		t.Fatal(err)
	}
}

func TestAccountRoutingTTLExpiresOnlyOwningAccount(t *testing.T) {
	m := newRoutingTestManager(true)
	now := time.Now().UTC()
	m.health[accountHealthKey("short", "astra")] = accountModelHealth{
		AuthID: "short", Account: publicAccountID("short"), Model: "astra", State: "healthy",
		TurnStateValue: strings.Repeat("a", 332), HealthyUntil: now.Add(time.Minute), ObservedAt: now,
	}
	m.health[accountHealthKey("long", "astra")] = accountModelHealth{
		AuthID: "long", Account: publicAccountID("long"), Model: "astra", State: "healthy",
		TurnStateValue: strings.Repeat("b", 332), HealthyUntil: now.Add(10 * time.Minute), ObservedAt: now,
	}

	got := m.pick(routingRequest("short", "long"), now.Add(2*time.Minute))
	if !got.Handled || got.AuthID != "long" {
		t.Fatalf("pick after short TTL expired = %+v", got)
	}
	short := m.health[accountHealthKey("short", "astra")]
	long := m.health[accountHealthKey("long", "astra")]
	if short.State != "expired" || short.TurnStateValue != "" {
		t.Fatalf("short account was not independently expired: %+v", short)
	}
	if long.State != "healthy" || long.TurnStateValue == "" {
		t.Fatalf("long account was changed by another account expiry: %+v", long)
	}
}

func TestAccountScopedTurnStateNeverBorrowsAnotherAccount(t *testing.T) {
	original := accountRouter
	accountRouter = newRoutingTestManager(true)
	t.Cleanup(func() { accountRouter = original })
	now := time.Now().UTC()
	stateA := strings.Repeat("a", 332)
	stateB := strings.Repeat("b", 332)
	accountRouter.health[accountHealthKey("account-a", "astra")] = accountModelHealth{
		AuthID: "account-a", Model: "astra", State: "healthy", TurnStateValue: stateA,
		HealthyUntil: now.Add(10 * time.Minute), ObservedAt: now,
	}
	accountRouter.health[accountHealthKey("account-b", "astra")] = accountModelHealth{
		AuthID: "account-b", Model: "astra", State: "healthy", TurnStateValue: stateB,
		HealthyUntil: now.Add(20 * time.Minute), ObservedAt: now,
	}

	headers, status := applyTurnStateOverrideForAccount("account-a", "gpt-6-astra", "", nil)
	if status != "applied-account" || headers.Get(turnStateHeader) != stateA {
		t.Fatalf("account A override = %q %q", status, headers.Get(turnStateHeader))
	}
	headers, status = applyTurnStateOverrideForAccount("account-b", "gpt-6-astra", "", nil)
	if status != "applied-account" || headers.Get(turnStateHeader) != stateB {
		t.Fatalf("account B override = %q %q", status, headers.Get(turnStateHeader))
	}
	headers, status = applyTurnStateOverrideForAccount("account-c", "gpt-6-astra", "", nil)
	if headers != nil || status != "account-state-missing" {
		t.Fatalf("missing account borrowed a state: headers=%v status=%q", headers, status)
	}
	repaired := repairTurnStateHeaderForAccount("account-a", "gpt-6-astra", "", "gpt-5.6-luna", strings.Repeat("x", 356))
	if repaired.Get(turnStateHeader) != stateA {
		t.Fatalf("account A response repair borrowed wrong state: %q", repaired.Get(turnStateHeader))
	}
	if repaired = repairTurnStateHeaderForAccount("account-c", "gpt-6-astra", "", "gpt-5.6-luna", strings.Repeat("x", 356)); repaired != nil {
		t.Fatalf("missing account response repair borrowed a state: %v", repaired)
	}

	summaryJSON, err := json.Marshal(accountRoutingSummary())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(summaryJSON), stateA) || strings.Contains(string(summaryJSON), stateB) {
		t.Fatalf("account state leaked through management summary")
	}
}

func TestSelectedAuthMetadata(t *testing.T) {
	if got := selectedAuthID(map[string]any{"selected_auth_id": " auth-real "}); got != "auth-real" {
		t.Fatalf("selected auth = %q", got)
	}
	if got := selectedAuthID(map[string]any{"selected_auth_id": 7}); got != "" {
		t.Fatalf("non-string selected auth = %q", got)
	}
}

func TestAccountRoutingAdoptsTimestampedRequestState(t *testing.T) {
	m := newRoutingTestManager(true)
	now := time.Now().UTC().Truncate(time.Second)
	generated := now.Add(-5 * time.Minute)
	state := timedRoutingState(generated, 332)
	if len(state) != 332 {
		t.Fatalf("state length = %d", len(state))
	}
	m.observeRequestState("request-owner", "gpt-6-astra", state, now)
	entry := m.health[accountHealthKey("request-owner", "astra")]
	if entry.State != "healthy" || entry.TurnStateValue != state || !entry.HealthyUntil.After(now) {
		t.Fatalf("request state was not adopted: %+v", entry)
	}

	expired := timedRoutingState(now.Add(-2*time.Hour), 332)
	m.observeRequestState("expired-owner", "gpt-6-astra", expired, now)
	if _, ok := m.health[accountHealthKey("expired-owner", "astra")]; ok {
		t.Fatalf("expired request state must not create a healthy lease")
	}
}

func TestHealthyRequestStateClearsAccountCooldownBeforeRejection(t *testing.T) {
	original := accountRouter
	originalSessions := turnStateSessions
	accountRouter = newRoutingTestManager(true)
	turnStateSessions = newTurnStateSessionManager()
	t.Cleanup(func() {
		accountRouter = original
		turnStateSessions = originalSessions
	})
	now := time.Now().UTC().Truncate(time.Second)
	accountRouter.health[accountHealthKey("recovering", "astra")] = accountModelHealth{
		AuthID: "recovering", Model: "astra", State: "degraded", LastStateLength: 356,
		LastReason: "state_length_356", ObservedAt: now.Add(-time.Minute), CooldownUntil: now.Add(time.Hour),
	}
	state := timedRoutingState(now.Add(-time.Minute), 332)
	headers := http.Header{turnStateHeader: []string{state}, "Session-Id": []string{"account-recovery-session"}}
	sessionKey, _ := turnStateSessionKey(headers, []byte(`{"input":"hello"}`), nil)
	turnStateSessions.mu.Lock()
	turnStateSessions.rememberOriginLocked(sessionKey, turnStateSessionOrigin{
		AuthID: "recovering", Model: "astra", Identity: turnStateIdentity(state),
		Fingerprint: turnStateFingerprint(state), StateLength: len(state),
		Source: "test-confirmed", ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
	})
	turnStateSessions.mu.Unlock()
	t.Cleanup(func() {
		history.mu.Lock()
		filtered := history.records[:0]
		for _, record := range history.records {
			if record.RequestID != "account-recovery" {
				filtered = append(filtered, record)
			}
		}
		history.records = filtered
		history.mu.Unlock()
	})
	raw, _ := json.Marshal(interceptRequest{
		RequestID: "account-recovery", ToFormat: "codex", SourceFormat: "openai-response",
		Model: "gpt-6-astra", Headers: headers,
		Body: []byte(`{"input":"hello"}`), Metadata: map[string]any{"selected_auth_id": "recovering"},
	})
	response, err := intercept(raw)
	if err != nil || response.Terminate {
		t.Fatalf("healthy account-owned request state was rejected: response=%+v err=%v", response, err)
	}
	entry := accountRouter.health[accountHealthKey("recovering", "astra")]
	if entry.State != "healthy" || entry.TurnStateValue != state || !entry.CooldownUntil.IsZero() {
		t.Fatalf("account cooldown was not cleared before rejection: %+v", entry)
	}
}
