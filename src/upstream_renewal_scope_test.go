package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAllAccountsRenewalStoresCredentialBoundTicket(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	e := accountTestEngine(t)
	now := time.Now().UTC()
	setHealthyAccountState(accountRouter, "fixture-A", "astra", strings.Repeat("x", 332), now.Add(-2*time.Hour))
	token := synthStateToken(now)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer synthetic-index-A" {
			t.Error("renewal selected another credential")
		}
		w.Header().Set(turnStateHeader, token)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n"))
	}))
	defer server.Close()
	e.cfg.Config.UpstreamURL = server.URL
	e.cfg.Config.Proxies = []string{"direct"}
	e.cfg.Config.Prefetch = 0
	e.expiryScan(now)
	waitPrefetchIdle(t, e)
	if calls != 1 {
		t.Fatalf("renewal requests=%d", calls)
	}
	if e.activeValueFor(accountTarget("fixture-A")) != token {
		t.Fatal("renewed ticket missing from credential-bound slot")
	}
	if e.activeValueFor(accountTarget("fixture-B")) != "" || e.activeValueFor("gpt-6-astra") != "" {
		t.Fatal("renewal contaminated another slot")
	}
	if got := scopedRequest(t, "fixture-A", ""); got.Headers.Get(turnStateHeader) != token {
		t.Fatal("business cannot use renewed ticket")
	}
}

func TestScopedRenewalFailureDoesNotBecomeBusinessRejection(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	e := accountTestEngine(t)
	e.setRejectDegraded(true)
	now := time.Now().UTC()
	setHealthyAccountState(accountRouter, "fixture-A", "astra", strings.Repeat("x", 332), now.Add(-2*time.Hour))
	target := accountTarget("fixture-A")
	e.failures[target] = probeFailure{LastError: "model mismatch: requested gpt-6-astra got gpt-5.6-luna"}
	if reason := e.degradedRejectReason(target); reason != "" {
		t.Fatal("renewal failure blocked expired account")
	}
	e.noteBusinessDegradation(target, "model mismatch")
	if reason := e.degradedRejectReason(target); reason == "" {
		t.Fatal("real business evidence was ignored")
	}
}
