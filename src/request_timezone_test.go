package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestEvaluationTimezoneRequiredAndAudited(t *testing.T) {
	for _, prompt := range []string{"synthetic candy prompt", "synthetic <environment_context><timezone>UTC</timezone></environment_context>"} {
		t.Run(prompt, func(t *testing.T) {
			e, f := newEvalFixture(t)
			previous := currentTimezone()
			configuredTimezone.Store("America/Los_Angeles")
			t.Cleanup(func() { configuredTimezone.Store(previous) })
			accounts, _ := e.evalAccounts()
			auth := accounts[0]
			audit := &evalRequestAudit{ID: "eval-tz-fixture", Scope: auth.Scope, AuthID: auth.ID, Kind: "candy", Model: "gpt-6-astra"}
			out, err := executeEvalModel(context.Background(), auth, "gpt-6-astra", prompt, "medium", audit)
			if err != nil {
				t.Fatal(err)
			}
			audit.finish(out, "")
			f.mu.Lock()
			body := f.Calls[0].Body
			f.mu.Unlock()
			encoded, _ := json.Marshal(body)
			if !strings.Contains(string(encoded), "America/Los_Angeles") || strings.Contains(body["input"].(string), "<timezone>UTC") {
				t.Fatal("evaluation bypassed timezone normalization")
			}
			row := evaluationRows()[0]
			want := "inserted"
			if strings.Contains(prompt, "UTC") {
				want = "replaced"
			}
			if row.Target != "America/Los_Angeles" || row.Action != want {
				t.Fatalf("audit mismatch: %s/%s", row.Target, row.Action)
			}
		})
	}
}

func TestProbeTimezoneRequired(t *testing.T) {
	e := newPrefetchTestEngine(t)
	previous := currentTimezone()
	configuredTimezone.Store("Asia/Tokyo")
	t.Cleanup(func() { configuredTimezone.Store(previous) })
	var received []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received, _ = io.ReadAll(r.Body); w.WriteHeader(503) }))
	defer server.Close()
	cfg := e.cfg.Config
	cfg.UpstreamURL = server.URL
	cred := filepath.Join(t.TempDir(), "synthetic-auth.json")
	if err := os.WriteFile(cred, []byte(`{"access_token":"synthetic-token"}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg.CredFile = cred
	_, _ = e.probeOnce("gpt-6-astra", "direct", cfg)
	if !strings.Contains(string(received), "Asia/Tokyo") {
		t.Fatal("health probe bypassed timezone normalization")
	}
}

func TestAllEvaluationKindsAndUncheckedModelsNormalizeTimezone(t *testing.T) {
	for _, kind := range []string{"candy", "modeltrace"} {
		for _, model := range []string{"gpt-6-astra", "gpt-5.6-luna"} {
			t.Run(kind+"/"+model, func(t *testing.T) {
				e, f := newEvalFixture(t)
				e.halted = true
				e.cfg.Config.Enabled = false
				e.paused[model] = true
				previous := currentTimezone()
				configuredTimezone.Store("Asia/Tokyo")
				t.Cleanup(func() { configuredTimezone.Store(previous) })
				if kind == "modeltrace" {
					f.Answer = func(evalMockCall) (string, error) { return strings.Repeat("42,", 320), nil }
				}
				if evalTestRun(t, e, kind, model, "none", 1, 1).StatusCode != 202 {
					t.Fatal("launch failed")
				}
				waitPrefetchIdle(t, e)
				f.mu.Lock()
				calls := append([]evalMockCall(nil), f.Calls...)
				f.mu.Unlock()
				for _, call := range calls {
					if !strings.Contains(call.Body["instructions"].(string), "<timezone>Asia/Tokyo</timezone>") || call.Body["tools"] != nil {
						t.Fatal("evaluation timezone exemption or sampling parameters changed")
					}
				}
				for _, row := range evaluationRows() {
					if row.Target != "Asia/Tokyo" || row.Action != "inserted" || row.TurnStateInjectedLength != 0 || row.ModelMismatch {
						t.Fatal("audit wrong or health semantics introduced")
					}
				}
				if !e.halted || !e.paused[model] || e.probesTotal != 0 {
					t.Fatal("normalization changed participation")
				}
			})
		}
	}
}

func TestEvaluationTimezoneCapturedPerRequestAndNextSampleUsesNewTarget(t *testing.T) {
	e, f := newEvalFixture(t)
	previous := currentTimezone()
	configuredTimezone.Store("America/Los_Angeles")
	t.Cleanup(func() { configuredTimezone.Store(previous) })
	count := 0
	f.Answer = func(evalMockCall) (string, error) {
		count++
		if count == 1 {
			configuredTimezone.Store("Asia/Tokyo")
		}
		return "21", nil
	}
	if evalTestRun(t, e, "candy", "gpt-6-astra", "none", 2, 1).StatusCode != 202 {
		t.Fatal("launch failed")
	}
	waitPrefetchIdle(t, e)
	want := []string{"America/Los_Angeles", "Asia/Tokyo"}
	rows := evaluationRows()
	var actual []string
	for _, row := range rows {
		actual = append(actual, row.Target)
	}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("audit lost request snapshot: %v", actual)
	}
	e.mu.Lock()
	samples := append([]evalSample(nil), e.evaluations.Results[0].Samples...)
	e.mu.Unlock()
	for i, sample := range samples {
		if sample.Timezone != want[i] {
			t.Fatal("detail does not match sent request")
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, call := range f.Calls {
		if !strings.Contains(call.Body["instructions"].(string), timezoneTag(want[i])) {
			t.Fatal("body and audit used different timezone snapshots")
		}
	}
}
