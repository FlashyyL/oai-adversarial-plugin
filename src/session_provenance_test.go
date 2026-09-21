package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func setupSessionGuardTest(t *testing.T, mode string) {
	t.Helper()
	originalRouter := accountRouter
	originalSessions := turnStateSessions
	originalOverride := currentTurnStateOverride()
	originalProbe := probeTrack
	probeTrack = &probeEngine{cfg: probeConfigState{Config: probeConfig{TTL: time.Hour}}, rejectDegraded: true}
	accountRouter = newRoutingTestManager(true)
	turnStateSessions = newTurnStateSessionManager()
	turnStateOverride.Store(&turnStateOverrideState{Config: turnStateOverrideConfig{
		Enabled: true, Models: []string{"gpt-6-astra"},
		SessionGuardMode: mode, SessionProvenanceTTLMinutes: 60,
	}})
	t.Cleanup(func() {
		probeTrack = originalProbe
		accountRouter = originalRouter
		turnStateSessions = originalSessions
		if originalOverride != nil {
			turnStateOverride.Store(originalOverride)
		} else {
			turnStateOverride.Store(&turnStateOverrideState{})
		}
	})
}

func removeAuditRecord(t *testing.T, requestID string) {
	t.Helper()
	t.Cleanup(func() {
		history.mu.Lock()
		filtered := history.records[:0]
		for _, record := range history.records {
			if record.RequestID != requestID {
				filtered = append(filtered, record)
			}
		}
		history.records = filtered
		history.mu.Unlock()
	})
}

func findAuditRecord(t *testing.T, requestID string) auditRecord {
	t.Helper()
	history.mu.Lock()
	defer history.mu.Unlock()
	for _, record := range history.records {
		if record.RequestID == requestID {
			return record
		}
	}
	t.Fatalf("audit record %q not found", requestID)
	return auditRecord{}
}

func setHealthyAccountState(manager *accountRoutingManager, authID, model, value string, now time.Time) {
	model = routingModelKey(model)
	manager.health[accountHealthKey(authID, model)] = accountModelHealth{
		AuthID: authID, Account: publicAccountID(authID), Model: model,
		State: "healthy", TurnStateValue: value, HealthyUntil: now.Add(time.Hour),
		LastStateLength: len(value), LastReason: "test", ObservedAt: now,
	}
}

func provenanceInterceptRequest(t *testing.T, requestID, authID, sessionID, state string) interceptResponse {
	t.Helper()
	raw, err := json.Marshal(interceptRequest{
		RequestID: requestID, ToFormat: "codex", SourceFormat: "openai-response",
		Model: "gpt-6-astra", Headers: http.Header{
			turnStateHeader: []string{state}, "Session-Id": []string{sessionID},
		},
		Body: []byte(`{"input":"hello"}`), Metadata: map[string]any{"selected_auth_id": authID},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := intercept(raw)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func TestTurnStateSessionKeyIsHashedAndPrincipalScoped(t *testing.T) {
	headers := http.Header{"Session-Id": []string{"raw-session-secret"}, "X-Codex-Installation-Id": []string{"install-a"}}
	first, source := turnStateSessionKey(headers, nil, map[string]any{"client_principal_id": "principal-a"})
	second, _ := turnStateSessionKey(headers, nil, map[string]any{"client_principal_id": "principal-a"})
	other, _ := turnStateSessionKey(headers, nil, map[string]any{"client_principal_id": "principal-b"})
	if first == "" || first != second || first == other || source != "header:session-id" {
		t.Fatalf("unexpected session keys: first=%q second=%q other=%q source=%q", first, second, other, source)
	}
	if strings.Contains(first, "raw-session-secret") || strings.Contains(first, "principal-a") {
		t.Fatal("session key leaked raw input")
	}
}

func TestSessionGuardObserveDoesNotAdoptUnknownState(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	requestID := "session-unknown-observe"
	removeAuditRecord(t, requestID)
	unknown := strings.Repeat("u", 332)
	response := provenanceInterceptRequest(t, requestID, "account-b", "session-b", unknown)
	if response.Terminate || len(response.ClearHeaders) != 0 {
		t.Fatalf("observe mode changed request: %+v", response)
	}
	if _, ok := accountRouter.health[accountHealthKey("account-b", "astra")]; ok {
		t.Fatal("unknown client state seeded the account pool")
	}
	record := findAuditRecord(t, requestID)
	if record.TurnStateProvenance != "unknown" || record.TurnStateFingerprint != turnStateFingerprint(unknown) {
		t.Fatalf("unexpected provenance audit: %+v", record)
	}
}

func TestUsageCallbackCannotAdoptUnconfirmedOrForeignState(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	now := time.Now().UTC()
	state := strings.Repeat("u", 332)
	record := usageRecord{
		Provider: "codex", AuthID: "account-b", Model: "gpt-6-astra",
		ResponseHeaders: http.Header{turnStateHeader: []string{state}},
	}
	accountRouter.observe(record, now)
	if entry := accountRouter.health[accountHealthKey("account-b", "astra")]; entry.State == "healthy" || entry.TurnStateValue != "" {
		t.Fatal("usage adopted an unconfirmed response state")
	}
	setHealthyAccountState(accountRouter, "account-a", "astra", state, now)
	accountRouter.observe(record, now)
	if entry := accountRouter.health[accountHealthKey("account-b", "astra")]; entry.State == "healthy" || entry.TurnStateValue != "" {
		t.Fatal("usage rebound another account's state")
	}
	record.AuthID = "account-a"
	accountRouter.observe(record, now)
	if entry := accountRouter.health[accountHealthKey("account-a", "astra")]; entry.State != "healthy" || entry.TurnStateValue != state {
		t.Fatal("usage failed to retain the confirmed owner's state")
	}
}

func TestUnknownRequestStateCannotClearDegradedAccount(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	originalReject := probeTrack.rejectDegradedEnabled()
	probeTrack.setRejectDegraded(true)
	t.Cleanup(func() { probeTrack.setRejectDegraded(originalReject) })
	now := time.Now().UTC()
	accountRouter.health[accountHealthKey("account-b", "astra")] = accountModelHealth{
		AuthID: "account-b", Model: "astra", State: "degraded", LastStateLength: 356,
		LastReason: "state_length_356", ObservedAt: now.Add(-time.Minute), CooldownUntil: now.Add(time.Hour),
	}
	requestID := "session-unknown-degraded"
	removeAuditRecord(t, requestID)
	unknown := timedRoutingState(now.Add(-time.Minute), 332)
	response := provenanceInterceptRequest(t, requestID, "account-b", "session-b", unknown)
	if !response.Terminate || response.StatusCode != http.StatusForbidden {
		t.Fatalf("unconfirmed state cleared degraded account: %+v", response)
	}
	entry := accountRouter.health[accountHealthKey("account-b", "astra")]
	if entry.State != "degraded" || entry.TurnStateValue != "" {
		t.Fatalf("unconfirmed state polluted account health: %+v", entry)
	}
}

func TestSessionGuardEnforceReplacesKnownForeignState(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeEnforce)
	now := time.Now().UTC()
	foreign := strings.Repeat("a", 332)
	current := strings.Repeat("b", 332)
	setHealthyAccountState(accountRouter, "account-a", "astra", foreign, now)
	setHealthyAccountState(accountRouter, "account-b", "astra", current, now)
	requestID := "session-foreign-replaced"
	removeAuditRecord(t, requestID)

	response := provenanceInterceptRequest(t, requestID, "account-b", "session-b", foreign)
	if response.Terminate || response.Headers.Get(turnStateHeader) != current || len(response.ClearHeaders) != 0 {
		t.Fatalf("foreign state was not replaced by current account: %+v", response)
	}
	record := findAuditRecord(t, requestID)
	if record.TurnStateProvenance != "foreign-account" || record.TurnStateOverride != "session-foreign-replaced" || record.TurnStateOwner != publicAccountID("account-a") {
		t.Fatalf("unexpected enforcement audit: %+v", record)
	}
}

func TestSessionGuardEnforceStripsForeignStateWithoutReplacement(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeEnforce)
	foreign := strings.Repeat("a", 332)
	setHealthyAccountState(accountRouter, "account-a", "astra", foreign, time.Now().UTC())
	requestID := "session-foreign-stripped"
	removeAuditRecord(t, requestID)

	response := provenanceInterceptRequest(t, requestID, "account-b", "session-b", foreign)
	if response.Terminate || response.Headers.Get(turnStateHeader) != "" || len(response.ClearHeaders) != 1 || response.ClearHeaders[0] != turnStateHeader {
		t.Fatalf("foreign state was not stripped: %+v", response)
	}
	if record := findAuditRecord(t, requestID); record.TurnStateOverride != "session-foreign-stripped" {
		t.Fatalf("unexpected strip audit: %+v", record)
	}
}

func TestConfirmedResponseBindsStateAcrossSessions(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeEnforce)
	m := turnStateSessions
	now := time.Now().UTC()
	state := strings.Repeat("s", 332)
	headers := http.Header{"Session-Id": []string{"minting-session"}}
	m.stageResponse("mismatch", headers, nil, nil, "account-a", "gpt-6-astra", state, state, now)
	m.confirmResponse("mismatch", "gpt-5.6-luna", now)
	if _, ok := accountRouter.health[accountHealthKey("account-a", "astra")]; ok {
		t.Fatal("model-mismatched response was trusted")
	}

	m.stageResponse("confirmed", headers, nil, nil, "account-a", "gpt-6-astra", state, state, now)
	m.confirmResponse("confirmed", "gpt-6-astra", now)
	entry, ok := accountRouter.health[accountHealthKey("account-a", "astra")]
	if !ok || entry.TurnStateValue != state || entry.LastReason != "healthy_response_state" {
		t.Fatalf("confirmed state not stored: %+v", entry)
	}
	decision := m.inspect(http.Header{"Session-Id": []string{"different-session"}}, nil, nil, "account-b", "gpt-6-astra", state, now.Add(time.Minute))
	if decision.Kind != "foreign-account" || decision.OwnerAuthID != "account-a" {
		t.Fatalf("confirmed state ownership not retained across sessions: %+v", decision)
	}
}

func TestHTTPResponseHookConfirmsAccountOwnership(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeEnforce)
	state := strings.Repeat("h", 332)
	raw, err := json.Marshal(responseInterceptRequest{
		RequestID: "http-confirmed", Model: "gpt-6-astra", StatusCode: http.StatusOK,
		RequestHeaders:  http.Header{"Session-Id": []string{"http-session"}},
		ResponseHeaders: http.Header{turnStateHeader: []string{state}},
		RequestBody:     []byte(`{"input":"hello"}`),
		Body:            []byte(`{"object":"response","model":"gpt-6-astra"}`),
		Metadata:        map[string]any{"selected_auth_id": "account-http"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = interceptNonStreamingResponse(raw); err != nil {
		t.Fatal(err)
	}
	entry, ok := accountRouter.health[accountHealthKey("account-http", "astra")]
	if !ok || entry.TurnStateValue != state {
		t.Fatalf("HTTP response did not confirm ownership: %+v", entry)
	}
	decision := turnStateSessions.inspect(http.Header{"Session-Id": []string{"http-other"}}, nil, nil, "account-other", "gpt-6-astra", state, time.Now().UTC())
	if decision.Kind != "foreign-account" || decision.OwnerAuthID != "account-http" {
		t.Fatalf("HTTP confirmed owner not resolved: %+v", decision)
	}
}

func TestResponseUsesTrustedRequestBindingWhenHostMetadataIsMissing(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeEnforce)
	now := time.Now().UTC()
	turnStateSessions.bindRequest("bound-request", "account-bound", "gpt-6-astra", now)
	if got := turnStateSessions.responseAuth("bound-request", "gpt-5.6-sol", nil, now); got != "" {
		t.Fatal("request binding crossed model families")
	}
	if got := turnStateSessions.responseAuth("bound-request", "gpt-6-astra", nil, now.Add(requestBindingTTL)); got != "" {
		t.Fatal("expired request binding was reused")
	}
	if got := turnStateSessions.responseAuth("bound-request", "gpt-6-astra", map[string]any{"selected_auth_id": "explicit-owner"}, now); got != "explicit-owner" {
		t.Fatal("response's explicit owner must take precedence")
	}
	state := strings.Repeat("r", 332)
	raw, _ := json.Marshal(responseInterceptRequest{
		RequestID: "bound-request", Model: "gpt-6-astra", StatusCode: http.StatusOK,
		ResponseHeaders: http.Header{turnStateHeader: []string{state}},
		Body:            []byte(`{"object":"response","model":"gpt-6-astra"}`),
	})
	if _, err := interceptNonStreamingResponse(raw); err != nil {
		t.Fatal(err)
	}
	if entry := accountRouter.health[accountHealthKey("account-bound", "astra")]; entry.State != "healthy" || entry.TurnStateValue != state {
		t.Fatal("response did not use its trusted post-selection request binding")
	}
}

func TestHTTPResponseWithoutSessionStillConfirmsStateOwner(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeEnforce)
	state := strings.Repeat("n", 332)
	raw, err := json.Marshal(responseInterceptRequest{
		RequestID: "http-confirmed-no-session", Model: "gpt-6-astra", StatusCode: http.StatusOK,
		ResponseHeaders: http.Header{turnStateHeader: []string{state}},
		RequestBody:     []byte(`{"input":"hello"}`),
		Body:            []byte(`{"object":"response","model":"gpt-6-astra"}`),
		Metadata:        map[string]any{"selected_auth_id": "account-http"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = interceptNonStreamingResponse(raw); err != nil {
		t.Fatal(err)
	}
	decision := turnStateSessions.inspect(nil, nil, nil, "account-other", "gpt-6-astra", state, time.Now().UTC())
	if decision.Kind != "foreign-account" || decision.OwnerAuthID != "account-http" {
		t.Fatalf("response without session did not retain state owner: %+v", decision)
	}
	if summary := turnStateSessions.summary(); summary["tracked_sessions"] != 0 || summary["tracked_states"] != 1 {
		t.Fatalf("unexpected session/state counts: %+v", summary)
	}
}

func TestObserveModeDoesNotRebindForeignEchoedResponseState(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	state := strings.Repeat("f", 332)
	setHealthyAccountState(accountRouter, "account-a", "astra", state, time.Now().UTC())
	raw, err := json.Marshal(responseInterceptRequest{
		RequestID: "http-foreign-echo", Model: "gpt-6-astra", StatusCode: http.StatusOK,
		RequestHeaders:  http.Header{turnStateHeader: []string{state}, "Session-Id": []string{"borrowed-session"}},
		ResponseHeaders: http.Header{turnStateHeader: []string{state}},
		RequestBody:     []byte(`{"input":"hello"}`),
		Body:            []byte(`{"object":"response","model":"gpt-6-astra"}`),
		Metadata:        map[string]any{"selected_auth_id": "account-b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = interceptNonStreamingResponse(raw); err != nil {
		t.Fatal(err)
	}
	if entry, ok := accountRouter.health[accountHealthKey("account-b", "astra")]; ok && entry.TurnStateValue == state {
		t.Fatalf("foreign echoed state was rebound to current account: %+v", entry)
	}
	decision := turnStateSessions.inspect(nil, nil, nil, "account-b", "gpt-6-astra", state, time.Now().UTC())
	if decision.Kind != "foreign-account" || decision.OwnerAuthID != "account-a" {
		t.Fatalf("foreign owner changed after echo: %+v", decision)
	}
}

func TestEnforceModeReplacesForeignResponseState(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeEnforce)
	foreign := strings.Repeat("a", 332)
	current := strings.Repeat("b", 332)
	now := time.Now().UTC()
	setHealthyAccountState(accountRouter, "account-a", "astra", foreign, now)
	setHealthyAccountState(accountRouter, "account-b", "astra", current, now)
	raw, _ := json.Marshal(responseInterceptRequest{
		RequestID: "http-response-replace", Model: "gpt-6-astra", StatusCode: http.StatusOK,
		RequestHeaders:  http.Header{"Session-Id": []string{"response-session"}},
		ResponseHeaders: http.Header{turnStateHeader: []string{foreign}},
		RequestBody:     []byte(`{"input":"hello"}`),
		Body:            []byte(`{"object":"response","model":"gpt-6-astra"}`),
		Metadata:        map[string]any{"selected_auth_id": "account-b"},
	})
	out, err := interceptNonStreamingResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if out.Headers.Get(turnStateHeader) != current || len(out.ClearHeaders) != 0 {
		t.Fatalf("foreign response was not replaced: %+v", out)
	}
	if entry := accountRouter.health[accountHealthKey("account-b", "astra")]; entry.TurnStateValue != current {
		t.Fatalf("foreign upstream state polluted current account: %+v", entry)
	}
}

func TestEnforceModeStripsForeignResponseWithoutReplacement(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeEnforce)
	foreign := strings.Repeat("a", 332)
	setHealthyAccountState(accountRouter, "account-a", "astra", foreign, time.Now().UTC())
	raw, _ := json.Marshal(responseInterceptRequest{
		RequestID: "http-response-strip", Model: "gpt-6-astra", StatusCode: http.StatusOK,
		RequestHeaders:  http.Header{"Session-Id": []string{"response-session"}},
		ResponseHeaders: http.Header{turnStateHeader: []string{foreign}},
		RequestBody:     []byte(`{"input":"hello"}`),
		Body:            []byte(`{"object":"response","model":"gpt-6-astra"}`),
		Metadata:        map[string]any{"selected_auth_id": "account-b"},
	})
	out, err := interceptNonStreamingResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if out.Headers.Get(turnStateHeader) != "" || !responseClearsHeader(out.ClearHeaders, turnStateHeader) {
		t.Fatalf("foreign response was not stripped: %+v", out)
	}
}

func TestSSEHeaderWaitsForModelBeforeConfirmingOwnership(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeEnforce)
	state := strings.Repeat("e", 332)
	base := streamChunkInterceptRequest{
		RequestID: "sse-confirmed", Model: "gpt-6-astra",
		RequestHeaders: http.Header{"Session-Id": []string{"sse-session"}},
		RequestBody:    []byte(`{"input":"hello"}`),
		Metadata:       map[string]any{"selected_auth_id": "account-sse"},
	}
	header := base
	header.ChunkIndex = streamChunkHeaderInitIndex
	header.ResponseHeaders = http.Header{turnStateHeader: []string{state}}
	raw, _ := json.Marshal(header)
	if _, err := interceptStreamChunk(raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := accountRouter.health[accountHealthKey("account-sse", "astra")]; ok {
		t.Fatal("SSE state was trusted before model evidence arrived")
	}
	event := base
	event.ChunkIndex = 0
	event.Body = []byte("data: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n")
	raw, _ = json.Marshal(event)
	if _, err := interceptStreamChunk(raw); err != nil {
		t.Fatal(err)
	}
	entry, ok := accountRouter.health[accountHealthKey("account-sse", "astra")]
	if !ok || entry.TurnStateValue != state {
		t.Fatalf("SSE response did not confirm ownership: %+v", entry)
	}
}

func TestSessionProvenanceExpiresWithoutRawStateInSummary(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	m := turnStateSessions
	now := time.Now().UTC()
	state := strings.Repeat("z", 332)
	headers := http.Header{"Session-Id": []string{"expiring-session"}}
	key, _ := turnStateSessionKey(headers, nil, nil)
	m.mu.Lock()
	m.rememberOriginLocked(key, turnStateSessionOrigin{
		AuthID: "account-a", Model: "astra", Identity: turnStateIdentity(state), Fingerprint: turnStateFingerprint(state),
		StateLength: len(state), ObservedAt: now.Add(-2 * time.Hour), ExpiresAt: now.Add(-time.Minute),
	})
	m.mu.Unlock()
	decision := m.inspect(headers, nil, nil, "account-b", "gpt-6-astra", state, now)
	if decision.Kind != "unknown" {
		t.Fatalf("expired provenance still trusted: %+v", decision)
	}
	encoded, err := json.Marshal(m.summary())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), state) || strings.Contains(string(encoded), "expiring-session") {
		t.Fatal("summary leaked raw state or session id")
	}
}
