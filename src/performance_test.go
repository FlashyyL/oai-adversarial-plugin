package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func perfPointer[T any](value T) *T { return &value }

func perfRecord(id, scope, auth string, at time.Time) auditRecord {
	return auditRecord{RequestID: id, TraceID: "trace-" + id, Model: "gpt-6-astra", RequestedModel: "gpt-6-astra", AccountScope: scope, AuthBinding: accountScope(auth), performanceAttemptAt: at, Time: at.UTC().Format(time.RFC3339Nano), conversion: conversion{Original: []string{}, Paths: []string{}}}
}

func perfCompletion(r auditRecord, start, end time.Time, stream bool) requestCompletion {
	return requestCompletion{RequestID: r.RequestID, TraceID: r.TraceID, RequestedModel: r.RequestedModel, StartedAt: start, CompletedAt: end, Stream: stream, Outcome: "succeeded"}
}

func perfUsagePayload(output, reasoning int) []byte {
	return []byte(fmt.Sprintf(`{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":100,"output_tokens":%d,"output_tokens_details":{"reasoning_tokens":%d},"total_tokens":%d}}}`, output, reasoning, 100+output))
}

func recordPerfUsage(s *auditState, r auditRecord, payload []byte, stream bool) {
	tokens := probeUsageTokens(payload)
	s.observeResponseTokens(r.RequestID, r.AccountScope, tokens, completeTokenPayload(payload, stream))
}

func TestPerformanceFirstOutputDoesNotCountMetadataOrReasoningTwice(t *testing.T) {
	start := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	r := perfRecord("timing", "scope-a", "auth-a", start.Add(time.Millisecond))
	s := auditState{records: []auditRecord{r}}
	for _, payload := range []string{`{"type":"response.created"}`, `{"type":"response.output_item.added"}`, `{"type":"response.output_text.delta","delta":""}`, `{"choices":[{"delta":{"role":"assistant"}}]}`} {
		s.observeFirstOutput(r.RequestID, r.AccountScope, nil, []byte(payload), start.Add(time.Second))
	}
	if !s.records[0].firstOutputAt.IsZero() {
		t.Fatal("metadata counted as output")
	}
	s.observeFirstOutput(r.RequestID, "wrong-scope", nil, []byte(`{"type":"response.output_text.delta","delta":"hello"}`), start.Add(time.Second))
	s.observeFirstOutput(r.RequestID, r.AccountScope, map[string]any{"selected_auth_id": "other-auth"}, []byte(`{"type":"response.output_text.delta","delta":"hello"}`), start.Add(time.Second))
	s.observeFirstOutput(r.RequestID, r.AccountScope, nil, []byte(`{"type":"response.reasoning_summary_text.delta","delta":"PRIVATE_TEXT_NOT_SAVED"}`), start.Add(63460*time.Millisecond))
	s.observeFirstOutput(r.RequestID, r.AccountScope, nil, []byte(`{"type":"response.output_text.delta","delta":"answer"}`), start.Add(70*time.Second))
	recordPerfUsage(&s, r, perfUsagePayload(1420, 1034), true)
	s.observeCompletion(perfCompletion(r, start, start.Add(75120*time.Millisecond), true))
	got := s.records[0]
	if got.RequestLatencyMS == nil || *got.RequestLatencyMS != 75120 || got.FirstOutputLatencyMS == nil || *got.FirstOutputLatencyMS != 63460 {
		t.Fatal("first/total timing changed")
	}
	tps := requestTPS(got)
	if tps == nil || fmt.Sprintf("%.1f", *tps) != "18.9" {
		t.Fatal("TPS must include reasoning once, divided by total latency")
	}
	raw, _ := json.Marshal(got)
	if bytes.Contains(raw, []byte("PRIVATE_TEXT")) || bytes.Contains(raw, []byte("performanceAttemptAt")) || bytes.Contains(raw, []byte("firstOutputAt")) {
		t.Fatal("in-flight/text data persisted")
	}
}

func TestPerformanceOutputEventShapes(t *testing.T) {
	for payload, want := range map[string]bool{
		`{"type":"response.output_text.delta","delta":"text"}`:                                 true,
		`{"type":"response.reasoning_text.delta","delta":"reason"}`:                            true,
		`{"type":"response.function_call_arguments.delta","delta":"{\"x\":1}"}`:                true,
		`{"type":"response.refusal.delta","delta":"no"}`:                                       true,
		`data: {"choices":[{"delta":{"content":"hello"}}]}`:                                    true,
		`{"choices":[{"delta":{"reasoning_content":"reason"}}]}`:                               true,
		`{"choices":[{"delta":{"tool_calls":[{"function":{"arguments":"{}"}}]}}]}`:             true,
		`{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"reason"}}`: true,
		`{"type":"response.completed","response":{"output":[{"text":"not a first delta"}]}}`:   false,
		`{"type":"response.output_text.delta","delta":null}`:                                   false,
		`{"type":"response.output_text.delta","delta":42}`:                                     false,
		`[DONE]`: false, "not-json": false,
	} {
		if got := hasResponseOutput([]byte(payload)); got != want {
			t.Errorf("output %q: got %v want %v", payload, got, want)
		}
	}
	if hasResponseOutput([]byte(strings.Repeat("x", maxModelProbeSize+1))) {
		t.Fatal("large payload parsed")
	}
}

func TestPerformanceCompletenessAndFailureGuards(t *testing.T) {
	r := perfRecord("guard", "scope", "auth", time.Now())
	r.ResponseTokensOutput = perfPointer(100)
	r.ResponseTokensReasoning = perfPointer(20)
	r.RequestLatencyMS = perfPointer(1000.0)
	r.RequestOutcome = "succeeded"
	if requestTPS(r) != nil {
		t.Fatal("partial tokens counted")
	}
	r.ResponseTokensComplete = true
	for _, outcome := range []string{"", "failed", "rejected", "canceled"} {
		r.RequestOutcome = outcome
		if requestTPS(r) != nil {
			t.Fatal("failed/incomplete request counted")
		}
	}
	r.RequestOutcome = "succeeded"
	for _, latency := range []float64{0, -1, math.Inf(1), math.NaN()} {
		r.RequestLatencyMS = &latency
		if requestTPS(r) != nil {
			t.Fatal("invalid latency counted")
		}
	}
	r.RequestLatencyMS = perfPointer(1000.0)
	r.ResponseTokensOutput = perfPointer(0)
	r.ResponseTokensReasoning = nil
	if got := requestTPS(r); got == nil || *got != 0 {
		t.Fatal("measured zero output must stay zero")
	}
	r.ResponseTokensOutput = perfPointer(-1)
	if requestTPS(r) != nil {
		t.Fatal("negative output counted")
	}
	for _, tokens := range []map[string]int{{"input": 1}, {"output": -1}, {"output": 2, "reasoning": 3}, {"output": 2, "input": -1}} {
		if validPerformanceTokens(tokens) {
			t.Fatal("invalid token detail accepted")
		}
	}
	if completeTokenPayload([]byte(`{"usage":{"output_tokens":5},"status":"incomplete"}`), false) || completeTokenPayload([]byte(`{"type":"response.in_progress","usage":{"output_tokens":5}}`), true) {
		t.Fatal("partial response counted as final")
	}
}

func TestPerformanceRetryClearsPriorTimingAndTokens(t *testing.T) {
	start := time.Now().Add(-10 * time.Second)
	r := perfRecord("retry", "scope-a", "auth-a", start)
	s := auditState{records: []auditRecord{r}}
	recordPerfUsage(&s, r, perfUsagePayload(100, 50), true)
	s.observeFirstOutput(r.RequestID, r.AccountScope, nil, []byte(`{"type":"response.output_text.delta","delta":"old"}`), start.Add(time.Second))
	s.observeCompletion(perfCompletion(r, start, start.Add(2*time.Second), true))
	for _, scope := range []string{"scope-a", "scope-b"} {
		at := start.Add(4 * time.Second)
		auth := "auth-a"
		if scope != "scope-a" {
			auth = "auth-b"
		}
		retry := perfRecord("retry", scope, auth, at)
		s.record(retry)
		got := s.records[0]
		if got.RequestLatencyMS != nil || got.FirstOutputLatencyMS != nil || got.RequestOutcome != "" || got.ResponseTokensOutput != nil || got.ResponseTokensReasoning != nil || got.ResponseTokensComplete || !got.firstOutputAt.IsZero() {
			t.Fatal("retry inherited earlier performance")
		}
		s.observeCompletion(perfCompletion(r, start, start.Add(2*time.Second), true))
		if s.records[0].RequestOutcome != "" {
			t.Fatal("late completion attached to newer attempt")
		}
	}
}

func TestPerformanceCompletionMissingInvalidDuplicateAndEvicted(t *testing.T) {
	start := time.Now()
	r := perfRecord("id", "scope", "auth", start)
	for _, mode := range []string{"times-missing", "negative", "start-after-attempt", "wrong-trace", "wrong-auth", "unknown", "old-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			s := auditState{records: []auditRecord{r}}
			req := perfCompletion(r, start, start.Add(time.Second), true)
			switch mode {
			case "times-missing":
				req.StartedAt = time.Time{}
				req.CompletedAt = time.Time{}
			case "negative":
				req.CompletedAt = start.Add(-time.Second)
			case "start-after-attempt":
				req.StartedAt = start.Add(time.Millisecond)
			case "wrong-trace":
				req.TraceID = "other"
			case "wrong-auth":
				req.Metadata = map[string]any{"selected_auth_id": "other"}
			case "unknown":
				req.RequestID = "other"
			case "old-snapshot":
				s.records[0].performanceAttemptAt = time.Time{}
			}
			s.observeCompletion(req)
			if s.records[0].RequestLatencyMS != nil || s.records[0].FirstOutputLatencyMS != nil {
				t.Fatal("invented invalid/missing duration")
			}
		})
	}
	s := auditState{records: []auditRecord{r}}
	req := perfCompletion(r, start, start.Add(500*time.Microsecond), false)
	s.observeCompletion(req)
	req.CompletedAt = start.Add(time.Second)
	s.observeCompletion(req)
	if *s.records[0].RequestLatencyMS != .5 {
		t.Fatal("duplicate extended duration or sub-ms precision lost")
	}
	for i := 0; i < historyLimit; i++ {
		s.record(perfRecord(fmt.Sprint(i), "scope", "auth", start))
	}
	s.observeCompletion(req)
	if len(s.records) != historyLimit {
		t.Fatal("late completion resurrected evicted record")
	}
}

func TestPerformanceCompletedUsageMayArriveAfterCompletion(t *testing.T) {
	start := time.Now()
	r := perfRecord("after", "scope", "auth", start)
	s := auditState{records: []auditRecord{r}}
	s.observeCompletion(perfCompletion(r, start, start.Add(time.Second), false))
	if requestTPS(s.records[0]) != nil {
		t.Fatal("missing usage must stay unknown")
	}
	recordPerfUsage(&s, r, perfUsagePayload(100, 50), true)
	if tps := requestTPS(s.records[0]); tps == nil || *tps != 100 {
		t.Fatal("late complete usage lost")
	}
	s.observeResponseTokens(r.RequestID, r.AccountScope, map[string]int{"output": 50}, false)
	if requestTPS(s.records[0]) != nil {
		t.Fatal("later partial usage retained complete flag")
	}
}

func TestPerformanceCoalescedEventsUseFinalUsageAndActualOutput(t *testing.T) {
	partial := `data: {"type":"response.in_progress","usage":{"output_tokens":2}}` + "\n\n"
	metadata := strings.Repeat("data: {\"type\":\"response.created\"}\n\n", 6)
	delta := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"actual output\"}\n\n"
	payload := []byte(partial + metadata + delta + "data: " + string(perfUsagePayload(100, 20)) + "\n\n")
	if !hasResponseOutput(payload) {
		t.Fatal("coalesced output beyond first events missed")
	}
	tokens, complete := performanceUsageTokens(payload, true)
	if !complete || tokens["output"] != 100 || tokens["reasoning"] != 20 {
		t.Fatal("partial tokens labelled as completed usage")
	}
	if validPerformanceTokens(map[string]int{"input": 100, "output": 100, "total": 101}) {
		t.Fatal("inconsistent total accepted")
	}
}

func TestPerformanceConcurrentRequestsDoNotJoinByAccountOrTrace(t *testing.T) {
	start := time.Now()
	s := auditState{}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		s.record(perfRecord(fmt.Sprint(i), "shared-scope", "auth", start))
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := perfRecord(fmt.Sprint(i), "shared-scope", "auth", start)
			r.TraceID = ""
			recordPerfUsage(&s, r, perfUsagePayload(i, 0), true)
			s.observeCompletion(perfCompletion(r, start, start.Add(time.Second), false))
		}(i)
	}
	wg.Wait()
	for i, r := range s.records {
		if r.ResponseTokensOutput == nil || *r.ResponseTokensOutput != i || requestTPS(r) == nil || *requestTPS(r) != float64(i) {
			t.Fatal("concurrent same-model/account requests crossed")
		}
	}
}

func TestPerformanceLifecycleStreamHTTPWebSocketAndPersist(t *testing.T) {
	e := accountTestEngine(t)
	history.mu.Lock()
	history.records = nil
	history.mu.Unlock()
	e.rejectDegraded = false
	registration, err := handleMethod("plugin.register", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(registration)
	if !bytes.Contains(encoded, []byte(`"request_lifecycle_plugin":true`)) {
		t.Fatal("completion capability absent")
	}
	e.rejectDegraded = false
	for _, mode := range []string{"stream", "http", "websocket"} {
		t.Run(mode, func(t *testing.T) {
			scopedRequest(t, "fixture-A", "")
			id := "account-request"
			history.mu.Lock()
			history.records[0].performanceAttemptAt = time.Now().Add(-time.Second)
			r := history.records[0]
			history.mu.Unlock()
			if mode == "stream" {
				for _, index := range []int{-1, 0, 1} {
					body := []byte(`{"type":"response.created"}`)
					if index == 1 {
						body = []byte(`{"type":"response.output_text.delta","delta":"hello"}`)
					}
					raw, _ := json.Marshal(streamChunkInterceptRequest{RequestID: id, ChunkIndex: index, Body: body})
					if _, err := handleMethod("response.intercept_stream_chunk", raw); err != nil {
						t.Fatal(err)
					}
				}
				raw, _ := json.Marshal(streamChunkInterceptRequest{RequestID: id, ChunkIndex: 2, Body: perfUsagePayload(100, 20)})
				_, err = handleMethod("response.intercept_stream_chunk", raw)
			}
			if mode == "http" {
				raw, _ := json.Marshal(responseInterceptRequest{RequestID: id, StatusCode: 200, Body: []byte(`{"object":"response","status":"completed","usage":{"input_tokens":100,"output_tokens":100,"output_tokens_details":{"reasoning_tokens":20}}}`)})
				_, err = handleMethod("response.intercept_after", raw)
			}
			if mode == "websocket" {
				for _, payload := range [][]byte{[]byte(`{"type":"response.reasoning_text.delta","delta":"reason"}`), perfUsagePayload(100, 20)} {
					raw, _ := json.Marshal(webSocketResponseEvent{RequestID: id, AuthID: "fixture-A", Payload: payload})
					_, err = handleMethod("websocket.response_event", raw)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			req := perfCompletion(r, r.performanceAttemptAt.Add(-time.Millisecond), time.Now().Add(time.Millisecond), mode != "http")
			raw, _ := json.Marshal(req)
			if _, err = handleMethod("request.complete", raw); err != nil {
				t.Fatal(err)
			}
			got := history.snapshot()["records"].([]auditRecord)[0]
			if got.OutputTPS == nil || got.RequestLatencyMS == nil || !got.ResponseTokensComplete {
				t.Fatalf("missing lifecycle performance: %s", mode)
			}
			if (got.FirstOutputLatencyMS != nil) != (mode != "http") {
				t.Fatal("nonstream first output fabricated or stream missing")
			}
			persistRaw, _ := json.Marshal(collectState())
			var persisted persistedState
			if json.Unmarshal(persistRaw, &persisted) != nil {
				t.Fatal("snapshot decode failed")
			}
			if persisted.Records[0].RequestLatencyMS == nil || !persisted.Records[0].performanceAttemptAt.IsZero() {
				t.Fatal("durations not persisted or live evidence restored")
			}
			if err := os.WriteFile(stateFilePath(), persistRaw, 0600); err != nil {
				t.Fatal(err)
			}
			history.mu.Lock()
			history.records = nil
			history.mu.Unlock()
			loadPersistedState()
			restored := history.snapshot()["records"].([]auditRecord)[0]
			if restored.OutputTPS == nil || *restored.OutputTPS != *got.OutputTPS {
				t.Fatal("completed TPS did not survive restore")
			}
		})
	}
	if _, err := handleMethod("request.complete", []byte(`invalid`)); err == nil {
		t.Fatal("invalid completion accepted")
	}
	if _, err := handleMethod("request.complete", []byte(`{"RequestID":"unknown","Outcome":"failed","Error":"PRIVATE_ERROR_DO_NOT_SAVE"}`)); err != nil {
		t.Fatal(err)
	}
}
