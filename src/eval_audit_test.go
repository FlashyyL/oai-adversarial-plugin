package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func evaluationRows() []auditRecord {
	history.mu.Lock()
	defer history.mu.Unlock()
	return append([]auditRecord(nil), history.records...)
}

func TestEvaluationRequestsEnterAuditWithoutLearning(t *testing.T) {
	e, _ := newEvalFixture(t)
	e.halted = true
	e.values["keep"] = stateEntry{Value: "HEALTHY_TICKET_CANARY"}
	if evalTestRun(t, e, "candy", "gpt-6-astra", "medium", 2, 2).StatusCode != 202 {
		t.Fatal("launch failed")
	}
	waitPrefetchIdle(t, e)
	rows := evaluationRows()
	if len(rows) != 4 || history.total != 4 || history.inserted != 4 || history.replaced != 0 {
		t.Fatal("requests not recorded once per sample")
	}
	ids := map[string]bool{}
	for _, row := range rows {
		if row.RequestKind != "evaluation" || row.EvaluationKind != "candy" || row.RequestOutcome != "succeeded" || row.EvaluationID == "" || row.TimingSource != "evaluation-callback" || ids[row.RequestID] {
			t.Fatal("missing audit association")
		}
		ids[row.RequestID] = true
		if row.RequestLatencyMS == nil || *row.RequestLatencyMS < 0 || row.FirstOutputLatencyMS != nil || (*row.RequestLatencyMS > 0 && requestTPS(row) == nil) || row.ResponseTokensOutput == nil || *row.ResponseTokensOutput != 5 {
			t.Fatal("missing metrics or fabricated first output")
		}
		if row.TurnStateValue != "" || row.TurnStateInjectedLength != 0 || row.TurnStateOriginalLength != nil || row.ModelMismatch || row.ModelChecked {
			t.Fatal("audit introduced health semantics")
		}
	}
	e.mu.Lock()
	results := append([]evalResult(nil), e.evaluations.Results...)
	e.mu.Unlock()
	for _, r := range results {
		for _, sample := range r.Samples {
			if !ids[sample.RequestID] {
				t.Fatal("detail request ID does not match audit")
			}
		}
	}
	if !e.halted || e.probesTotal != 0 || e.values["keep"].Value != "HEALTHY_TICKET_CANARY" {
		t.Fatal("audit altered health state")
	}
	state := collectState()
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var loaded persistedState
	if json.Unmarshal(raw, &loaded) != nil || len(loaded.Records) != 4 || loaded.Records[0].AccountEmail == "" || loaded.Records[0].EvaluationID == "" || loaded.Records[0].RequestLatencyMS == nil {
		t.Fatal("audit metadata not persistent")
	}
	for _, r := range loaded.Records {
		encoded, _ := json.Marshal(r)
		if bytes.Contains(encoded, []byte("PRIVATE_A_CANARY")) || bytes.Contains(encoded, []byte("HEALTHY_TICKET_CANARY")) || bytes.Contains(encoded, []byte("最少取出")) {
			t.Fatal("audit retained credentials, tickets or answer text")
		}
	}
}

func TestEvaluationRequestFailuresRemainUngradedAndVisible(t *testing.T) {
	e, f := newEvalFixture(t)
	f.Answer = func(evalMockCall) (string, error) { return "", errors.New("PRIVATE_UPSTREAM_BODY") }
	if evalTestRun(t, e, "candy", "gpt-6-astra", "none", 1, 1).StatusCode != 202 {
		t.Fatal("launch failed")
	}
	waitPrefetchIdle(t, e)
	rows := evaluationRows()
	if len(rows) != 1 || rows[0].RequestOutcome != "failed" || rows[0].EvaluationError != "host_execute_failed" || requestTPS(rows[0]) != nil || rows[0].ResponseTokensInput != nil {
		t.Fatal("request error not safely recorded")
	}
	if e.evaluations.Results[0].Status != "failed" || e.evaluations.Results[0].Graded != 0 {
		t.Fatal("all-error batch shown as partially valid")
	}
	if safeEvalError("PRIVATE_UPSTREAM_BODY") != "evaluation_failed" {
		t.Fatal("raw error escaped whitelist")
	}
	legacy := evalResult{Status: "partial", Samples: []evalSample{{Status: "error", Error: "invalid_response_event"}}}
	if evalSummaryResult(legacy).Status != "failed" || legacy.Status != "partial" {
		t.Fatal("legacy presentation wrong or record rewritten")
	}
}

func TestEvaluationCancelledBeforeDispatchDoesNotInventRequest(t *testing.T) {
	_, _ = newEvalFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	audit := &evalRequestAudit{ID: "not-sent", Scope: "synthetic", Model: "gpt-6-astra"}
	out, err := executeEvalModel(ctx, hostAuthEntry{ID: "synthetic"}, "gpt-6-astra", "prompt", "none", audit)
	if err == nil {
		t.Fatal("cancel ignored")
	}
	audit.finish(out, err.Error())
	if len(evaluationRows()) != 0 {
		t.Fatal("recorded a request that was never dispatched")
	}
}

func TestEvaluationAuditCompletionIsIdempotentAndDoesNotResurrect(t *testing.T) {
	_, _ = newEvalFixture(t)
	a := &evalRequestAudit{ID: "eval-fixture", Scope: "synthetic", Kind: "candy", Model: "gpt-6-astra"}
	start := time.Now()
	a.start(start)
	out := evalModelOutput{StartedAt: start, CompletedAt: start.Add(time.Second), FirstOutputAt: start.Add(200 * time.Millisecond)}
	a.finish(out, "cancelled")
	before := evaluationRows()
	a.finish(out, "")
	if !reflect.DeepEqual(before, evaluationRows()) || before[0].RequestOutcome != "canceled" || requestTPS(before[0]) != nil {
		t.Fatal("duplicate completion changed outcome")
	}
	history.mu.Lock()
	history.records = nil
	history.mu.Unlock()
	a.finish(out, "")
	if len(evaluationRows()) != 0 {
		t.Fatal("late completion revived evicted record")
	}
}

func TestEvaluationRequestFirstOutputTimingAcrossFragments(t *testing.T) {
	e, _ := newEvalFixture(t)
	previous := evalHostCall
	evalHostCall = func(method string, payload any) (json.RawMessage, error) {
		raw, err := previous(method, payload)
		if method != "host.model.stream_read" || err != nil {
			return raw, err
		}
		var chunk struct {
			Payload []byte
			Done    bool
		}
		if json.Unmarshal(raw, &chunk) != nil {
			t.Fatal("fixture decode")
		}
		chunk.Payload = append([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\n"), chunk.Payload...)
		return json.Marshal(map[string]any{"payload": chunk.Payload, "done": chunk.Done})
	}
	if evalTestRun(t, e, "candy", "gpt-6-astra", "none", 1, 1).StatusCode != 202 {
		t.Fatal("launch failed")
	}
	waitPrefetchIdle(t, e)
	row := evaluationRows()[0]
	if row.FirstOutputLatencyMS == nil || *row.FirstOutputLatencyMS < 0 || *row.FirstOutputLatencyMS > *row.RequestLatencyMS || strings.Contains(row.UpstreamModel, "PRIVATE") {
		t.Fatal("first output evidence lost")
	}
}
