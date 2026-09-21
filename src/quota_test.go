package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestQuotaTakesPrecedenceOverStateAndRoutesHealthyAlternative(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	now := time.Now().UTC()
	own := timedRoutingState(now.Add(-time.Minute), 332)
	setHealthyAccountState(accountRouter, "limited", "astra", own, now)
	setHealthyAccountState(accountRouter, "healthy", "astra", strings.Repeat("h", 332), now)
	row := routingUsage("limited", 356, 429, true)
	row.Failure.Body = `{"error":{"type":"usage_limit_reached","resets_in_seconds":120}}`
	accountRouter.observe(row, now)
	entry := accountRouter.health[accountHealthKey("limited", "astra")]
	if entry.State != "quota_exhausted" || entry.TurnStateValue != own || entry.LastStateLength != 332 {
		t.Fatal("429 anomaly poisoned lease or classification")
	}
	if !entry.CooldownUntil.Equal(now.Add(120 * time.Second)) {
		t.Fatal("quota reset deadline ignored")
	}
	picked := accountRouter.pick(routingRequest("limited", "healthy"), now)
	if !picked.Handled || picked.AuthID != "healthy" {
		t.Fatal("did not select healthy alternate")
	}
	accountRouter.observeRequestState("limited", "gpt-6-astra", own, now)
	accountRouter.observeProbe("limited", "gpt-6-astra", probeRecord{Success: true, StatusCode: 200, StateLength: 332, ObservedModel: "gpt-6-astra"}, own, now)
	accountRouter.observeConfirmedState("limited", "gpt-6-astra", "gpt-6-astra", own, now)
	if accountRouter.health[accountHealthKey("limited", "astra")].State != "quota_exhausted" {
		t.Fatal("client replay cleared quota cooldown")
	}
	if accountRouter.needsRenewal("limited", "astra", now) {
		t.Fatal("quota triggered recovery probe")
	}
	if reason, _ := accountRouter.accountDegradedReason("limited", "astra", now); reason != "" {
		t.Fatal("quota was called degradation")
	}
	accountRouter.pick(routingRequest("limited"), now.Add(121*time.Second))
	if accountRouter.health[accountHealthKey("limited", "astra")].State != "healthy" {
		t.Fatal("quota reset requires manual probing")
	}
}

func TestQuotaRejectionUses429AndCorrectAudit(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	removeAuditRecord(t, "quota-rejection")
	now := time.Now().UTC()
	accountRouter.observeQuota("limited", "astra", "quota_exhausted", now.Add(time.Minute), now)
	out := provenanceInterceptRequest(t, "quota-rejection", "limited", "session", "")
	if !out.Terminate || out.StatusCode != 429 || out.ResponseHeaders.Get("Retry-After") == "" {
		t.Fatal("wrong quota response")
	}
	row := findAuditRecord(t, "quota-rejection")
	if row.DegradedRejected || row.RejectionKind != "quota_exhausted" || strings.Contains(string(out.ResponseBody), "degraded_model_rejected") {
		t.Fatal("quota displayed as degradation")
	}
}

func TestQuotaHTTPAndSSEErrorsDoNotSeedDegradation(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	raw, _ := json.Marshal(responseInterceptRequest{RequestID: "quota-http", Model: "gpt-6-astra", StatusCode: 429, Metadata: map[string]any{"selected_auth_id": "http-owner"}, ResponseHeaders: http.Header{turnStateHeader: []string{strings.Repeat("a", 356)}}, Body: []byte(`{"error":{"code":"insufficient_quota"}}`)})
	if _, err := interceptNonStreamingResponse(raw); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(streamChunkInterceptRequest{RequestID: "quota-sse", Model: "gpt-6-astra", Metadata: map[string]any{"selected_auth_id": "sse-owner"}, Body: []byte("data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"usage_limit_reached\"}}}\n\n")})
	if _, err := interceptStreamChunk(raw); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []string{"http-owner", "sse-owner"} {
		if accountRouter.health[accountHealthKey(owner, "astra")].State != "quota_exhausted" {
			t.Fatal("error hook did not classify quota")
		}
	}
	if quotaFailure(200, `{"output_text":"usage_limit_reached"}`) != "" {
		t.Fatal("ordinary model output classified as error")
	}
}

func TestQuotaRetryHeadersAndAuthErrors(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	if got := quotaRetryAt("rate_limited", http.Header{"Retry-After": []string{"90"}}, "", now); !got.Equal(now.Add(90 * time.Second)) {
		t.Fatal("retry-after seconds ignored")
	}
	if got := quotaRetryAt("rate_limited", http.Header{"Retry-After": []string{now.Add(2 * time.Minute).Format(http.TimeFormat)}}, "", now); !got.Equal(now.Add(2 * time.Minute)) {
		t.Fatal("retry-after date ignored")
	}
	m := newRoutingTestManager(true)
	m.observe(routingUsage("auth", 356, 401, true), now)
	if m.health[accountHealthKey("auth", "astra")].State != "auth_error" {
		t.Fatal("401 was misclassified by state length")
	}
	m.observeProbe("probe", "gpt-6-astra", probeRecord{StatusCode: 429, StateLength: 356, ReasonCode: "quota_exhausted", RetryAt: now.Add(time.Minute)}, "", now)
	if m.health[accountHealthKey("probe", "astra")].State != "quota_exhausted" {
		t.Fatal("probe misclassified quota")
	}
}

func TestOldAmbiguous429DoesNotKeepHardDegradationLock(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	now := time.Now().UTC()
	applyPersistedState(persistedState{StateVersion: 2, AccountHealth: []accountModelHealth{
		{AuthID: "ambiguous", Model: "astra", State: "degraded", LastReason: "state_length_356", LastStatusCode: 429, CooldownUntil: now.Add(time.Hour)},
		{AuthID: "real", Model: "astra", State: "degraded", LastReason: "state_length_356", LastStatusCode: 200, CooldownUntil: now.Add(time.Hour)},
	}})
	if accountRouter.health[accountHealthKey("ambiguous", "astra")].State != "probe_pending" {
		t.Fatal("legacy ambiguous quota lock retained")
	}
	if accountRouter.health[accountHealthKey("real", "astra")].State != "degraded" {
		t.Fatal("genuine business evidence erased")
	}
}
