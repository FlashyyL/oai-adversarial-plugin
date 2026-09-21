package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestProbeUpstreamModel covers the transport shapes that carry the model the
// upstream actually served: non-streaming bodies, SSE events and websocket
// frames.
func TestProbeUpstreamModel(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		model   string
		ok      bool
	}{
		{"responses non-streaming", `{"id":"resp_1","object":"response","model":"gpt-6-luna","output":[]}`, "gpt-6-luna", true},
		{"chat completion", `{"id":"chatcmpl_1","object":"chat.completion","model":"gpt-6-astra","choices":[]}`, "gpt-6-astra", true},
		{"sse response.created", "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_2\",\"model\":\"gpt-6-luna\"}}\n\n", "gpt-6-luna", true},
		{"sse bare line", "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n", "gpt-6-astra", true},
		{"websocket frame", `{"type":"response.created","response":{"model":"gpt-6-luna","id":"resp_3"}}`, "gpt-6-luna", true},
		{"anthropic message_start", `{"type":"message_start","message":{"id":"msg_1","model":"claude-sonnet-4"}}`, "claude-sonnet-4", true},
		{"no model key", `{"type":"response.in_progress"}`, "", false},
		{"model key without identity", `{"type":"ping","model":"gpt-6-luna"}`, "gpt-6-luna", true},
		{"empty", ``, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model, ok := probeUpstreamModel([]byte(tc.payload))
			if ok != tc.ok || model != tc.model {
				t.Fatalf("probeUpstreamModel(%q) = %q, %v; want %q, %v", tc.payload, model, ok, tc.model, tc.ok)
			}
		})
	}
}

func TestProbeUpstreamModelRejectsOversizePayload(t *testing.T) {
	oversize := append(bytes.Repeat([]byte(" "), maxModelProbeSize+1), []byte(`"model":"x"`)...)
	if _, ok := probeUpstreamModel(oversize); ok {
		t.Fatal("oversize payload must be ignored")
	}
}

// TestObserveModelAndTurnState drives the audit state directly: a request is
// recorded first (like the request interceptor does), then the response side
// attaches the observed model and turn-state.
func TestObserveModelAndTurnState(t *testing.T) {
	var state auditState
	state.record(auditRecord{RequestID: "req-1", Model: "gpt-5.6-sol", conversion: conversion{Target: targetTimezone, Action: "inserted"}})
	state.record(auditRecord{RequestID: "req-2", Model: "gpt-6-astra", conversion: conversion{Target: targetTimezone, Action: "unchanged"}})

	state.observeModel("req-1", "", "gpt-6-luna")
	state.observeTurnState("req-1", "sample-state-0001...long", "stream")
	state.observeModel("req-2", "", "gpt-6-astra")

	snapshot := state.snapshot()
	records := snapshot["records"].([]auditRecord)
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	first := records[0] // newest first: req-2, req-1
	if first.ModelMismatch || !first.ModelChecked || first.UpstreamModel != "gpt-6-astra" {
		t.Fatalf("req-2 must be consistent: %+v", first)
	}
	second := records[1]
	if !second.ModelChecked || !second.ModelMismatch || second.UpstreamModel != "gpt-6-luna" {
		t.Fatalf("req-1 must be flagged as mismatched: %+v", second)
	}
	if second.TurnStateLength != len("sample-state-0001...long") || second.TurnStateSource != "stream" {
		t.Fatalf("turn-state observation missing: %+v", second)
	}
	if second.TurnStateValue != "sample-state-0001...long" || second.TurnStateTruncated {
		t.Fatalf("turn-state full value missing: %+v", second)
	}
	if snapshot["mismatches"].(int) != 1 {
		t.Fatalf("mismatch counter wrong: %v", snapshot["mismatches"])
	}
}

func TestTurnStateValueIsBounded(t *testing.T) {
	var state auditState
	state.record(auditRecord{RequestID: "big", conversion: conversion{Target: targetTimezone, Action: "unchanged"}})
	long := strings.Repeat("A", turnStateValueLimit+100)
	state.observeTurnState("big", long, "stream")
	record := state.snapshot()["records"].([]auditRecord)[0]
	if record.TurnStateLength != len(long) || len(record.TurnStateValue) != turnStateValueLimit || !record.TurnStateTruncated {
		t.Fatalf("bounded value wrong: len=%d truncated=%v", len(record.TurnStateValue), record.TurnStateTruncated)
	}
}

func TestObserveIgnoresUnknownRequests(t *testing.T) {
	var state auditState
	state.observeModel("missing", "", "gpt-6-luna")
	state.observeTurnState("missing", "value", "response")
	if snapshot := state.snapshot(); len(snapshot["records"].([]auditRecord)) != 0 {
		t.Fatal("observers must not create records")
	}
}

// TestInterceptRequestRecordsTurnState verifies the request-side header view is
// captured when the client provides X-Codex-Turn-State.
func TestInterceptRequestRecordsTurnState(t *testing.T) {
	history = auditState{}
	raw, _ := json.Marshal(interceptRequest{
		RequestID: "req-ts", ToFormat: "codex", Model: "gpt-5.6-luna",
		Headers: http.Header{turnStateHeader: {"sample-state-0001"}},
		Body:    []byte(`{"input":"hello"}`),
	})
	if _, err := intercept(raw); err != nil {
		t.Fatal(err)
	}
	records := history.snapshot()["records"].([]auditRecord)
	if len(records) != 1 || records[0].TurnStateLength != len("sample-state-0001") || records[0].TurnStateSource != "request" {
		t.Fatalf("request-side turn-state not recorded: %+v", records)
	}
}

// TestResponseInterceptorsRecordObservations feeds synthetic wire payloads for
// the three response-side hooks and checks the attached observations.
func TestResponseInterceptorsRecordObservations(t *testing.T) {
	history = auditState{}
	history.record(auditRecord{RequestID: "req-ns", Model: "gpt-5.6-sol", conversion: conversion{Target: targetTimezone, Action: "replaced"}})
	history.record(auditRecord{RequestID: "req-stream", Model: "gpt-5.6-sol", conversion: conversion{Target: targetTimezone, Action: "inserted"}})

	nonStream, _ := json.Marshal(responseInterceptRequest{
		RequestID:       "req-ns",
		Body:            []byte(`{"id":"resp_1","object":"response","model":"gpt-6-luna"}`),
		ResponseHeaders: http.Header{turnStateHeader: {"0123456789"}},
	})
	out, err := interceptNonStreamingResponse(nonStream)
	if err != nil || out.Headers != nil || out.Body != nil {
		t.Fatalf("observation must not modify the response: %+v %v", out, err)
	}

	headerInit, _ := json.Marshal(streamChunkInterceptRequest{RequestID: "req-stream", ChunkIndex: streamChunkHeaderInitIndex})
	if _, err := interceptStreamChunk(headerInit); err != nil {
		t.Fatal(err)
	}
	chunk, _ := json.Marshal(streamChunkInterceptRequest{
		RequestID: "req-stream", ChunkIndex: 0,
		Body: []byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n"),
	})
	if _, err := interceptStreamChunk(chunk); err != nil {
		t.Fatal(err)
	}

	event, _ := json.Marshal(webSocketResponseEvent{RequestID: "req-ns", Payload: []byte(`{"type":"response.completed","response":{"model":"gpt-6-luna"}}`)})
	if _, err := observeWebSocketEvent(event); err != nil {
		t.Fatal(err)
	}

	snapshot := history.snapshot()
	records := snapshot["records"].([]auditRecord)
	byID := map[string]auditRecord{}
	for _, record := range records {
		byID[record.RequestID] = record
	}
	if record := byID["req-ns"]; !record.ModelMismatch || record.UpstreamModel != "gpt-6-luna" || record.TurnStateLength != 10 {
		t.Fatalf("non-streaming observation wrong: %+v", record)
	}
	if record := byID["req-stream"]; !record.ModelMismatch || record.UpstreamModel != "gpt-6-astra" || record.ModelChecked != true {
		t.Fatalf("stream observation wrong: %+v", record)
	}
	if snapshot["mismatches"].(int) != 2 {
		t.Fatalf("mismatch counter wrong: %v", snapshot["mismatches"])
	}
}

// TestProbeModelConsistent drives the model-consistency check used to accept
// or reject a captured state.
func TestProbeModelConsistent(t *testing.T) {
	cases := []struct {
		requested string
		observed  string
		ok        bool
	}{
		{"gpt-6-astra", "gpt-6-astra", true},
		{"gpt-6-astra", "GPT-6-ASTRA", true},
		{"gpt-6-astra", "gpt-6-astra-preview", true},
		{"gpt-6-astra", "gpt-5.6-luna", false},
		{"gpt-5.6-sol", "gpt-5.6-sol", true},
		{"gpt-5.6-sol", "gpt-6-astra", false},
		{"", "gpt-6-astra", false},
		{"gpt-6-astra", "", false},
		{"gpt-6-astra", " gpt-6-astra ", true},
	}
	for _, tc := range cases {
		if got := probeModelConsistent(tc.requested, tc.observed); got != tc.ok {
			t.Errorf("probeModelConsistent(%q, %q) = %v; want %v", tc.requested, tc.observed, got, tc.ok)
		}
	}
}

// TestProbeRecordCarriesObservedModel verifies the audit record shape used by
// the dashboard for the consistency display.
func TestProbeRecordCarriesObservedModel(t *testing.T) {
	record := probeRecord{Model: "gpt-6-astra", ObservedModel: "gpt-6-astra", Success: true, StateLength: 332}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"observed_model":"gpt-6-astra"`, `"state_length":332`} {
		if !bytes.Contains(encoded, []byte(key)) {
			t.Fatalf("record missing %s: %s", key, encoded)
		}
	}
}

// TestProbeFailureShape verifies the failure annotation payload structure used
// by the dashboard when retries are exhausted.
func TestProbeFailureShape(t *testing.T) {
	failure := probeFailure{Model: "gpt-6-astra", Attempts: 30, Rounds: 10,
		LastError: "state length 312 != 332 (suspected degraded)", LastLength: 312,
		FailedAt: "2026-09-18T02:20:00Z", CooldownUntil: "2026-09-18T02:40:00Z"}
	encoded, err := json.Marshal(failure)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"attempts":30`, `"rounds":10`, `"last_length":312`, `"last_error"`, `"cooldown_until":"2026-09-18T02:40:00Z"`} {
		if !bytes.Contains(encoded, []byte(key)) {
			t.Fatalf("failure payload missing %s: %s", key, encoded)
		}
	}
	// Success clears the annotation (engine semantics contract).
	engine := &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{}}
	engine.failures["gpt-6-astra"] = failure
	delete(engine.failures, "gpt-6-astra")
	if _, ok := engine.failures["gpt-6-astra"]; ok {
		t.Fatal("failure must be removable on success")
	}
}

// TestProbePauseAndResume verifies the dashboard pause/resume controls: a
// paused model is excluded from the automatic queue while its value and
// annotation are preserved; resume clears the annotation and re-enters it.
func TestProbePauseAndResume(t *testing.T) {
	state := &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	state.cfg.Config = parseProbeConfig(probeConfigYAML{})
	if state.probeSuppressed("gpt-6-astra") {
		t.Fatal("fresh model must not be suppressed")
	}
	state.failures["gpt-6-astra"] = probeFailure{Model: "gpt-6-astra", Attempts: 30, Rounds: 10,
		LastError: "state length 312 != 332 (suspected degraded)"}
	state.setModelPaused("gpt-6-astra", true)
	if !state.probeSuppressed("gpt-6-astra") {
		t.Fatal("paused model must be suppressed")
	}
	if _, ok := state.failures["gpt-6-astra"]; !ok {
		t.Fatal("pause must keep the annotation")
	}
	if models := state.pausedModels(); len(models) != 1 || models[0] != "gpt-6-astra" {
		t.Fatalf("paused list wrong: %v", models)
	}
	state.setModelPaused("gpt-6-astra", false)
	if _, ok := state.failures["gpt-6-astra"]; ok {
		t.Fatal("resume must clear the stale annotation")
	}
	// Resume queues a probe through the unified executor; the model is held
	// while that task runs, then re-enters normal scheduling. Wait for the
	// queue to drain (the probe itself fails fast on the default cred path).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !state.probeSuppressed("gpt-6-astra") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if state.probeSuppressed("gpt-6-astra") {
		t.Fatal("resumed model must re-enter normal scheduling")
	}
}

// TestDegradedRejectDecision drives the degraded-model rejection switch:
// only length anomalies and model mismatches count as degradation, and only
// while the switch is on. In-round suspicions crossing suspect-threshold are
// rejection-eligible before the round completes.
func TestDegradedRejectDecision(t *testing.T) {
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		suspects: map[string]probeSuspicion{}, paused: map[string]bool{}, probing: map[string]bool{}}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{})
	if message := degradedRejectMessage("gpt-6-astra"); message != "" {
		t.Fatalf("switch off must allow everything: %q", message)
	}
	probeTrack.failures["gpt-6-astra"] = probeFailure{LastError: "state length 312 != 332 (suspected degraded)"}
	if message := degradedRejectMessage("gpt-6-astra"); message != "" {
		t.Fatalf("switch off must allow even degraded: %q", message)
	}
	probeTrack.setRejectDegraded(true)
	if message := degradedRejectMessage("gpt-6-astra"); !strings.Contains(message, "风控降智") {
		t.Fatalf("length anomaly must be rejected with a Chinese message: %q", message)
	}
	if message := degradedRejectMessage("gpt-6-luna"); message != "" {
		t.Fatalf("unrelated model must pass: %q", message)
	}
	probeTrack.failures["gpt-5.6-sol"] = probeFailure{LastError: "model mismatch: requested gpt-5.6-sol got gpt-6-astra"}
	if message := degradedRejectMessage("gpt-5.6-sol"); !strings.Contains(message, "模型不一致") {
		t.Fatalf("model mismatch must be rejected: %q", message)
	}
	probeTrack.failures["other-model"] = probeFailure{LastError: "status 429: rate limit exceeded"}
	if message := degradedRejectMessage("other-model"); message != "" {
		t.Fatalf("rate limit must not count as degradation: %q", message)
	}
	// Early suspicion: below the threshold passes, at the threshold rejects.
	delete(probeTrack.failures, "gpt-6-astra")
	probeTrack.suspects["gpt-6-astra"] = probeSuspicion{Model: "gpt-6-astra", Failures: 2,
		LastError: "state length 312 != 332 (suspected degraded)"}
	if message := degradedRejectMessage("gpt-6-astra"); message != "" {
		t.Fatalf("below threshold must pass: %q", message)
	}
	probeTrack.suspects["gpt-6-astra"] = probeSuspicion{Model: "gpt-6-astra", Failures: 3,
		LastError: "state length 312 != 332 (suspected degraded)"}
	if message := degradedRejectMessage("gpt-6-astra"); !strings.Contains(message, "尚未达到正式判定") {
		t.Fatalf("threshold-crossing suspicion must be rejected: %q", message)
	}
}

// TestSuspectTracking drives the in-round suspicion counter: only degradation
// evidence counts, the count survives below-threshold failures, success
// clears it, and an exhausted round promotes it to the full annotation.
func TestSuspectTracking(t *testing.T) {
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		suspects: map[string]probeSuspicion{}, paused: map[string]bool{}, probing: map[string]bool{}}
	cfg := parseProbeConfig(probeConfigYAML{})
	probeTrack.noteProbeFailure("gpt-6-astra", probeRecord{Error: "status 429: rate limit exceeded"}, cfg)
	if len(probeTrack.suspects) != 0 {
		t.Fatal("rate limit must not start a suspicion")
	}
	probeTrack.noteProbeFailure("gpt-6-astra", probeRecord{Error: "state length 312 != 332 (suspected degraded)"}, cfg)
	if probeTrack.suspects["gpt-6-astra"].Failures != 1 {
		t.Fatalf("first failure must count: %+v", probeTrack.suspects)
	}
	probeTrack.noteProbeFailure("gpt-6-astra", probeRecord{Error: "model mismatch: requested x got y"}, cfg)
	if probeTrack.suspects["gpt-6-astra"].Failures != 2 {
		t.Fatalf("count must continue below the threshold: %+v", probeTrack.suspects)
	}
	probeTrack.noteProbeFailure("gpt-6-astra", probeRecord{Error: "state length 312 != 332 (suspected degraded)"}, cfg)
	if probeTrack.suspects["gpt-6-astra"].Failures != 3 {
		t.Fatalf("third failure must reach the threshold: %+v", probeTrack.suspects)
	}
	probeTrack.clearSuspect("gpt-6-astra")
	if _, ok := probeTrack.suspects["gpt-6-astra"]; ok {
		t.Fatal("success must clear the suspicion")
	}
}

// TestProbeControlEndpoint drives the POST control payloads the dashboard
// sends: pause, resume and the reject switch.
func TestProbeControlEndpoint(t *testing.T) {
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	response, err := probeControl([]byte(`{"model":"gpt-5.6-sol","action":"pause"}`))
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("pause failed: %+v %v", response, err)
	}
	if !probeTrack.paused["gpt-5.6-sol"] {
		t.Fatal("pause must mark the model")
	}
	response, err = probeControl([]byte(`{"model":"gpt-5.6-sol","action":"resume"}`))
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("resume failed: %+v %v", response, err)
	}
	if probeTrack.paused["gpt-5.6-sol"] {
		t.Fatal("resume must clear the mark")
	}
	response, err = probeControl([]byte(`{"action":"reject-degraded","enabled":true}`))
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("reject-degraded failed: %+v %v", response, err)
	}
	if !probeTrack.rejectDegradedEnabled() {
		t.Fatal("switch must be on")
	}
	response, _ = probeControl([]byte(`{"action":"bogus"}`))
	if response.StatusCode != 400 {
		t.Fatalf("unknown action must be rejected: %+v", response)
	}
	response, _ = probeControl([]byte(`{"action":"pause"}`))
	if response.StatusCode != 400 {
		t.Fatalf("pause without a model must be rejected: %+v", response)
	}
}

// TestInterceptDegradedRejection covers the full rejection path: with the
// switch on and a degradation-annotated failure present, intercept() must
// terminate the request with a 403 and a Chinese JSON message, and record
// the rejection without attempting timezone normalization. The rejection
// record must carry empty arrays (never nil) for original/paths so that the
// dashboard JSON never contains null for them.
func TestInterceptDegradedRejection(t *testing.T) {
	installFixtureHostAuth(t)
	history = auditState{}
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	probeTrack.failures[scopedTarget(credentialScope("fixture-account", "synthetic-fixture-token", ""), "gpt-6-astra")] = probeFailure{Model: "gpt-6-astra", Attempts: 30, Rounds: 10,
		LastError: "state length 312 != 332 (suspected degraded)"}
	probeTrack.setRejectDegraded(true)
	raw, _ := json.Marshal(interceptRequest{Metadata: map[string]any{"selected_auth_id": "fixture-account", "selected_auth_index": "fixture-index"},
		Headers: http.Header{"Authorization": {"Bearer synthetic-fixture-token"}}, RequestID: "req-reject", ToFormat: "codex", Model: "gpt-6-astra",
		Body: []byte(`{"input":"hello"}`),
	})
	resp, err := intercept(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Terminate || resp.StatusCode != 403 {
		t.Fatalf("degraded model must be terminated with 403: %+v", resp)
	}
	if !strings.Contains(string(resp.ResponseBody), "风控降智") {
		t.Fatalf("rejection body must be Chinese: %s", resp.ResponseBody)
	}
	record := history.snapshot()["records"].([]auditRecord)[0]
	if !record.DegradedRejected {
		t.Fatalf("rejection must be recorded: %+v", record)
	}
	if record.Original == nil || record.Paths == nil {
		t.Fatalf("rejection record must keep empty arrays, not nil: %+v", record)
	}
	encoded, _ := json.Marshal(record)
	if bytes.Contains(encoded, []byte(`"original":null`)) || bytes.Contains(encoded, []byte(`"paths":null`)) {
		t.Fatalf("rejection record must not marshal null arrays: %s", encoded)
	}
	// With the switch off the same request passes through to normalization.
	probeTrack.setRejectDegraded(false)
	history = auditState{}
	resp, err = intercept(raw)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Terminate {
		t.Fatalf("switch off must not terminate: %+v", resp)
	}
}

// TestPersistenceRoundTrip saves a snapshot and restores it into a fresh
// engine + audit state, covering values, failures, suspicions, paused models,
// the rejection switch, counters, probe history and audit records.
func TestPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LKS_TZ_STATE_FILE", filepath.Join(dir, "state.json"))

	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		suspects: map[string]probeSuspicion{}, paused: map[string]bool{}, probing: map[string]bool{}}
	cfg := parseProbeConfig(probeConfigYAML{})
	probeTrack.cfg.Config = cfg
	probeTrack.setRejectDegraded(false)
	probeTrack.setModelPaused("gpt-5.6-terra", true)
	probeTrack.storeValue("gpt-6-astra", "sample-state-0001", "probe", "direct", cfg)
	probeTrack.noteError("state length 312 != 332 (suspected degraded)")
	probeTrack.noteProbeFailure("gpt-6-astra", probeRecord{Error: "state length 312 != 332 (suspected degraded)"}, cfg)
	probeTrack.failures["gpt-5.6-sol"] = probeFailure{Model: "gpt-5.6-sol", Attempts: 30, Rounds: 10,
		LastError: "state length 312 != 332 (suspected degraded)", FailedAt: "2026-09-18T02:20:00Z"}
	probeTrack.appendRecord(probeRecord{Time: "2026-09-18T02:20:00Z", Model: "gpt-6-astra", Success: true})
	history = auditState{}
	history.record(auditRecord{RequestID: "persist-1", Model: "gpt-6-astra",
		conversion: conversion{Target: targetTimezone, Action: "inserted"},
		Time:       "2026-09-18T02:21:00Z"})
	savePersistedState()

	// Simulate a fresh plugin instance: wipe everything.
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		suspects: map[string]probeSuspicion{}, paused: map[string]bool{}, probing: map[string]bool{},
		rejectDegraded: true}
	probeTrack.cfg.Config = cfg
	history = auditState{}
	loadPersistedState()

	if probeTrack.rejectDegradedEnabled() {
		t.Fatal("switch must restore the saved off state")
	}
	if !probeTrack.paused["gpt-5.6-terra"] {
		t.Fatal("paused model must be restored")
	}
	if entry, ok := probeTrack.values["gpt-6-astra"]; !ok || entry.Value != "sample-state-0001" {
		t.Fatalf("value must be restored: %+v", entry)
	}
	if _, ok := probeTrack.failures["gpt-5.6-sol"]; !ok {
		t.Fatal("failure annotation must be restored")
	}
	if suspicion, ok := probeTrack.suspects["gpt-6-astra"]; !ok || suspicion.Failures != 1 {
		t.Fatalf("suspicion must be restored: %+v", suspicion)
	}
	if probeTrack.probesTotal != 1 {
		t.Fatalf("probe counters must be restored: %d", probeTrack.probesTotal)
	}
	if len(probeTrack.history) != 1 {
		t.Fatalf("probe history must be restored: %d", len(probeTrack.history))
	}
	snapshot := history.snapshot()
	if snapshot["total"] != uint64(1) {
		t.Fatalf("audit total must be restored: %v", snapshot["total"])
	}
	records := snapshot["records"].([]auditRecord)
	if len(records) != 1 || records[0].RequestID != "persist-1" {
		t.Fatalf("audit records must be restored: %+v", records)
	}
}

// TestBusinessObservation drives the event-driven strategy: one unhealthy
// business observation marks the model degraded immediately; a healthy one
// stores the value with source "business" and clears the mark.
func TestBusinessObservation(t *testing.T) {
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		suspects: map[string]probeSuspicion{}, business: map[string]businessDegradation{},
		lastAttempt: map[string]time.Time{},
		paused:      map[string]bool{}, probing: map[string]bool{}}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{})
	probeTrack.cfg.Config.Enabled = false // observations must never start upstream traffic in tests

	// One length-anomaly observation marks degraded immediately.
	observeBusinessState("gpt-6-astra", "short-312", "gpt-6-astra")
	if _, ok := probeTrack.business["gpt-6-astra"]; !ok {
		t.Fatal("single unhealthy observation must mark the model")
	}
	probeTrack.setRejectDegraded(true)
	if message := degradedRejectMessage("gpt-6-astra"); !strings.Contains(message, "业务流量确认") {
		t.Fatalf("business mark must feed the rejection switch: %q", message)
	}

	// One model-mismatch observation (state empty) marks too.
	observeBusinessState("gpt-5.6-sol", "", "gpt-5.6-luna")
	if _, ok := probeTrack.business["gpt-5.6-sol"]; !ok {
		t.Fatal("model mismatch must mark the model")
	}

	// A healthy observation stores the value and clears the mark.
	healthy := synthBusinessState(len("gpt-6-astra"))
	observeBusinessState("gpt-6-astra", healthy, "gpt-6-astra")
	if _, ok := probeTrack.business["gpt-6-astra"]; ok {
		t.Fatal("healthy observation must clear the mark")
	}
	if entry, ok := probeTrack.values["gpt-6-astra"]; !ok || entry.Source != "business" || entry.Value != healthy {
		t.Fatalf("healthy observation must store the value: %+v", entry)
	}

	// Empty state with no mismatch is ignored.
	observeBusinessState("gpt-5.6-terra", "", "")
	if len(probeTrack.business) != 1 {
		t.Fatalf("no-signal observation must not mark: %+v", probeTrack.business)
	}
}

// synthBusinessState builds a deterministic value of the required length so
// the healthy business-observation path can be exercised without real tokens.
func synthBusinessState(seed int) string {
	length := probeRequiredStateLength
	var builder strings.Builder
	for i := 0; i < length; i++ {
		builder.WriteByte(byte('A' + (seed+i)%26))
	}
	return builder.String()
}

// TestManualRoundControl verifies the manual probing model: rounds only run
// when started explicitly, a disabled track notes the refusal, and stop is
// idempotent. The single configured model has no credential file, so the
// round fails fast without network I/O.
func TestManualRoundControl(t *testing.T) {
	enabled := true
	one := 1
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		lastAttempt: map[string]time.Time{},
		paused:      map[string]bool{}, probing: map[string]bool{}}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{
		Enabled:          &enabled,
		Models:           []string{"gpt-6-astra"},
		IntervalSeconds:  &one,
		AttemptsPerHop:   &one,
		MaxAttemptsRound: &one,
	})
	probeTrack.cfg.Config.AccountMode = ""

	waitFinished := func() {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			probeTrack.mu.Lock()
			running := probeTrack.running
			probeTrack.mu.Unlock()
			if !running {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("round must finish")
	}

	// Disabled track: start records the refusal and finishes immediately.
	probeTrack.cfg.Config.Enabled = false
	probeTrack.start()
	waitFinished()
	probeTrack.mu.Lock()
	note := probeTrack.runNote
	probeTrack.mu.Unlock()
	if !strings.Contains(note, "未启用") {
		t.Fatalf("disabled track must note the refusal: %q", note)
	}

	// Enabled track: one round with a credential-less model fails fast and
	// records its finish time.
	probeTrack.cfg.Config.Enabled = true
	probeTrack.start()
	waitFinished()
	probeTrack.mu.Lock()
	finished := probeTrack.runFinishedAt
	probeTrack.mu.Unlock()
	if finished == "" {
		t.Fatal("round must record its finish time")
	}

	// stop must be safe even when nothing is running.
	probeTrack.stop()
	probeTrack.stop()
	probeTrack.mu.Lock()
	running := probeTrack.running
	probeTrack.mu.Unlock()
	if running {
		t.Fatal("stop must leave the engine idle")
	}
}

// TestSingleModelProbe covers the per-row "probe now" control: it refuses to
// start while the track is disabled, never double-starts a model that is
// already probing, and may run independently of the sequential round.
func TestSingleModelProbe(t *testing.T) {
	enabled := false
	one := 1
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		lastAttempt: map[string]time.Time{},
		paused:      map[string]bool{}, probing: map[string]bool{}}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{
		Enabled:          &enabled,
		Models:           []string{"gpt-6-astra"},
		IntervalSeconds:  &one,
		AttemptsPerHop:   &one,
		MaxAttemptsRound: &one,
	})

	// Disabled track: the request is a no-op.
	probeTrack.probeModelAsync("gpt-6-astra")
	if probeTrack.probing["gpt-6-astra"] {
		t.Fatal("disabled track must not start a probe")
	}

	// An already-probing model is never started twice.
	probeTrack.cfg.Config.Enabled = true
	probeTrack.probing["gpt-6-astra"] = true
	probeTrack.probeModelAsync("gpt-6-astra")
	if !probeTrack.probing["gpt-6-astra"] {
		t.Fatal("existing in-flight probe state lost")
	}
	delete(probeTrack.probing, "gpt-6-astra")

	// The async entry launches exactly one round (credential missing fails
	// fast and records an annotation, exercising the whole path).
	probeTrack.probeModelAsync("gpt-6-astra")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		probeTrack.mu.Lock()
		busy := probeTrack.probing["gpt-6-astra"]
		probeTrack.mu.Unlock()
		if !busy {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	probeTrack.mu.Lock()
	busy := probeTrack.probing["gpt-6-astra"]
	probeTrack.mu.Unlock()
	if busy {
		t.Fatal("single-model round must finish")
	}
}

// TestSeedBaselinesFromAudit restores missing baseline values from the
// deployment seeds file and the newest healthy (332-byte, decodable) audit
// records - the "last recorded 332-byte value as the initial baseline" rule.
func TestSeedBaselinesFromAudit(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LKS_TZ_STATE_FILE", filepath.Join(dir, "state.json"))
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{})
	old := synthStateToken(time.Now().Add(-30 * time.Minute))
	newer := synthStateToken(time.Now().Add(-10 * time.Minute))
	fileSeed := synthStateToken(time.Now().Add(-20 * time.Minute))
	seeds, _ := json.Marshal(map[string]string{
		"gpt-5.6-sol":   fileSeed,
		"gpt-5.6-terra": strings.Repeat("A", 312), // degraded: rejected
	})
	if err := os.WriteFile(filepath.Join(dir, "seeds.json"), seeds, 0o600); err != nil {
		t.Fatal(err)
	}
	history = auditState{}
	history.record(auditRecord{RequestID: "seed-1", Model: "gpt-6-astra",
		TurnStateLength: len(old), TurnStateValue: old})
	history.record(auditRecord{RequestID: "seed-2", Model: "gpt-6-astra",
		TurnStateLength: len(newer), TurnStateValue: newer})
	probeTrack.seedBaselinesFromAudit()
	entry, ok := probeTrack.values["gpt-6-astra"]
	if !ok || entry.Value != newer || entry.Source != "seed" || entry.ValueLength != probeRequiredStateLength {
		t.Fatalf("seed must adopt the newest healthy audit value: %+v", entry)
	}
	if entry, ok := probeTrack.values["gpt-5.6-sol"]; !ok || entry.Value != fileSeed || entry.Source != "seed" {
		t.Fatalf("seeds file value must be adopted: %+v", entry)
	}
	if _, ok := probeTrack.values["gpt-5.6-terra"]; ok {
		t.Fatal("a degraded (312-byte) seed must never be drafted")
	}
	// Existing entries are never overwritten by the seed pass.
	probeTrack.values["gpt-5.6-luna"] = stateEntry{Model: "gpt-5.6-luna", Value: "busy", Valid: true}
	probeTrack.seedBaselinesFromAudit()
	if got := probeTrack.values["gpt-5.6-luna"].Value; got != "busy" {
		t.Fatalf("seed must not touch existing entries: %q", got)
	}
}

// TestRepairTurnStateHeader drives the "discard and backfill" rule: an
// unhealthy upstream value is replaced by the model's healthy baseline on the
// response path, while a healthy value passes through untouched.
func TestRepairTurnStateHeader(t *testing.T) {
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{})
	healthy := synthStateToken(time.Now().Add(-5 * time.Minute))
	probeTrack.values["gpt-5.6-sol"] = stateEntry{Model: "gpt-5.6-sol", Value: healthy,
		ValueLength: len(healthy), Valid: true}

	// Length anomaly: replaced with the healthy baseline.
	headers := repairTurnStateHeader("gpt-5.6-sol", "", "gpt-5.6-sol", strings.Repeat("A", 312))
	if headers == nil || headers.Get(turnStateHeader) != healthy {
		t.Fatalf("degraded state must be backfilled with the baseline: %+v", headers)
	}
	// Model mismatch: replaced too.
	headers = repairTurnStateHeader("gpt-5.6-sol", "", "gpt-6-astra", strings.Repeat("A", 332))
	if headers == nil || headers.Get(turnStateHeader) != healthy {
		t.Fatalf("model mismatch must be backfilled: %+v", headers)
	}
	// Healthy and consistent: untouched.
	if headers := repairTurnStateHeader("gpt-5.6-sol", "", "gpt-5.6-sol", healthy); headers != nil {
		t.Fatalf("healthy state must pass through: %+v", headers)
	}
	// No state at all: untouched.
	if headers := repairTurnStateHeader("gpt-5.6-sol", "", "gpt-5.6-sol", ""); headers != nil {
		t.Fatalf("absent state must pass through: %+v", headers)
	}
	// No baseline for the model: untouched (nothing to backfill with).
	if headers := repairTurnStateHeader("gpt-6-astra", "", "gpt-6-astra", strings.Repeat("B", 312)); headers != nil {
		t.Fatalf("missing baseline must pass through: %+v", headers)
	}
}

// TestParseTurnStateTimestamp decodes the Fernet timestamp embedded in a
// synthetic token with the production layout; malformed inputs are rejected.
func TestParseTurnStateTimestamp(t *testing.T) {
	// Deterministic synthetic sample with the same Fernet layout as production.
	token := synthStateToken(time.Unix(1789695384, 0))
	ts, ok := parseTurnStateTimestamp(token)
	if !ok {
		t.Fatal("valid token must decode")
	}
	if ts.Unix() < 1789690000 || ts.Unix() > 1789700000 {
		t.Fatalf("unexpected timestamp: %v (%d)", ts, ts.Unix())
	}
	for _, bad := range []string{"", "AAAA", token[:10], "not-base64!!"} {
		if _, ok := parseTurnStateTimestamp(bad); ok {
			t.Fatalf("malformed token must be rejected: %q", bad)
		}
	}
}

// TestProbeConfigDefaultsAndOverrides drives probe config parsing.
func TestProbeConfigDefaultsAndOverrides(t *testing.T) {
	enabled := true
	scan := 15
	window := 4
	attempts := 2
	cooldown := 3
	block := probeConfigYAML{
		Enabled:         &enabled,
		Models:          []string{" gpt-6-astra ", ""},
		TTLMinutes:      nil,
		ScanSeconds:     &scan,
		WindowMinutes:   &window,
		AttemptsPerHop:  &attempts,
		CooldownMinutes: &cooldown,
		Proxies:         []string{"direct", "socks5://a:b@192.0.2.2:1080"},
	}
	cfg := parseProbeConfig(block)
	if !cfg.Enabled || cfg.ScanInterval != 15*time.Second || cfg.Window != 4*time.Minute || cfg.AttemptsPerHop != 2 || cfg.Cooldown != 3*time.Minute {
		t.Fatalf("overrides wrong: %+v", cfg)
	}
	if cfg.TTL != 55*time.Minute || cfg.ProbeInterval != 5*time.Second || cfg.Timeout != 60*time.Second {
		t.Fatalf("defaults wrong: %+v", cfg)
	}
	if len(cfg.Proxies) != 2 || cfg.Proxies[0] != "direct" {
		t.Fatalf("proxies wrong: %+v", cfg.Proxies)
	}
	// Empty config falls back to the default model priority list.
	empty := parseProbeConfig(probeConfigYAML{})
	if len(empty.Models) == 0 || empty.Models[0] != "gpt-6-astra" {
		t.Fatalf("default models wrong: %+v", empty.Models)
	}
	if empty.Enabled {
		t.Fatal("probe must default to disabled")
	}
}

// TestActiveValueLifecycle exercises the baseline serving rules without
// network: missing entries serve nothing, fresh decodable values serve, and
// values that provably exceed the TTL stop serving.
func TestActiveValueLifecycle(t *testing.T) {
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	cfg := parseProbeConfig(probeConfigYAML{})
	probeTrack.cfg.Config = cfg
	if value := probeTrack.activeValueFor("gpt-6-astra"); value != "" {
		t.Fatalf("missing entry must serve nothing: %q", value)
	}
	fresh := synthStateToken(time.Now())
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra", Value: fresh, ValueLength: len(fresh), Valid: true}
	if value := probeTrack.activeValueFor("gpt-6-astra"); value != fresh {
		t.Fatalf("fresh value must serve: %q", value)
	}
	expired := synthStateToken(time.Now().Add(-2 * cfg.TTL))
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra", Value: expired, ValueLength: len(expired), Valid: true}
	if value := probeTrack.activeValueFor("gpt-6-astra"); value != "" {
		t.Fatalf("expired value must stop serving: %q", value)
	}
}

// synthStateToken builds a Fernet-layout token (byte-exact 249-byte payload,
// base64url 332 characters) whose embedded timestamp is the given instant, so
// the validity logic can be exercised without real tokens.
func synthStateToken(t time.Time) string {
	raw := make([]byte, 249) // [1B version][8B ts][16B IV][192B cipher][32B HMAC]
	raw[0] = 0x80
	binary.BigEndian.PutUint64(raw[1:9], uint64(t.Unix()))
	for i := 9; i < len(raw); i++ {
		raw[i] = byte('A' + i%26)
	}
	return base64.URLEncoding.EncodeToString(raw)
}

// TestSmoothHandoff drives the two-slot behavior: a fresh capture parks as
// candidate while the active token still lives; an expired active promotes a
// parked candidate seamlessly; a capture with no valid active replaces it.
func TestSmoothHandoff(t *testing.T) {
	enabled := true
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		candidates: map[string]stateEntry{}, prefetchGate: map[string]time.Time{},
		lastAttempt: map[string]time.Time{},
		paused:      map[string]bool{}, probing: map[string]bool{}}
	cfg := parseProbeConfig(probeConfigYAML{Enabled: &enabled, Models: []string{"gpt-6-astra"}})
	probeTrack.cfg.Config = cfg
	if cfg.Prefetch != 3*time.Minute {
		t.Fatalf("default prefetch wrong: %v", cfg.Prefetch)
	}

	oldToken := synthStateToken(time.Now().Add(-1 * time.Minute))
	freshToken := synthStateToken(time.Now())
	probeTrack.storeValue("gpt-6-astra", oldToken, "seed", "", cfg)
	if got := probeTrack.activeValueFor("gpt-6-astra"); got != oldToken {
		t.Fatalf("first capture must become active: %q", got)
	}

	// Fresh capture while active lives: parks in candidate; serving unaffected.
	probeTrack.storeValue("gpt-6-astra", freshToken, "probe", "direct", cfg)
	if got := probeTrack.activeValueFor("gpt-6-astra"); got != oldToken {
		t.Fatalf("active must keep serving while valid: %q", got)
	}
	if candidate, ok := probeTrack.candidates["gpt-6-astra"]; !ok || candidate.Value != freshToken {
		t.Fatalf("fresh capture must park as candidate: %+v", candidate)
	}

	// Simulate active expiry: next lookup promotes the candidate seamlessly.
	probeTrack.mu.Lock()
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra",
		Value: synthStateToken(time.Now().Add(-2 * cfg.TTL)), Valid: true}
	probeTrack.mu.Unlock()
	if got := probeTrack.activeValueFor("gpt-6-astra"); got != freshToken {
		t.Fatalf("expired active must promote candidate: %q", got)
	}
	if _, ok := probeTrack.candidates["gpt-6-astra"]; ok {
		t.Fatal("promoted candidate must leave the slot")
	}

	// No valid active: a capture becomes active directly.
	probeTrack.mu.Lock()
	delete(probeTrack.values, "gpt-6-astra")
	probeTrack.mu.Unlock()
	newer := synthStateToken(time.Now())
	probeTrack.storeValue("gpt-6-astra", newer, "probe", "", cfg)
	if got := probeTrack.activeValueFor("gpt-6-astra"); got != newer {
		t.Fatalf("capture must activate when no valid active: %q", got)
	}
}

// TestSettledBaselineGate verifies the "one healthy capture is enough"
// round gate: a comfortably-valid baseline is settled (skip), while a
// missing, near-expiry, expired, or non-decodable value is not, and the gate
// disables itself when the hand-off window is turned off (fully manual).
func TestSettledBaselineGate(t *testing.T) {
	enabled := true
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	cfg := parseProbeConfig(probeConfigYAML{Enabled: &enabled, Models: []string{"gpt-6-astra"}})
	probeTrack.cfg.Config = cfg
	now := time.Now().UTC()
	if cfg.Prefetch != 3*time.Minute {
		t.Fatalf("default hand-off window wrong: %v", cfg.Prefetch)
	}
	// No baseline: the round must probe.
	if probeTrack.settledBaseline("gpt-6-astra", cfg, now) {
		t.Fatal("missing baseline must not be settled")
	}
	// Fresh baseline (remaining far beyond the window): nothing to do.
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra",
		Value: synthStateToken(now.Add(-10 * time.Minute)), Valid: true}
	if !probeTrack.settledBaseline("gpt-6-astra", cfg, now) {
		t.Fatal("fresh baseline must be settled")
	}
	// Near expiry (remaining <= window): hand-off territory, keep probing.
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra",
		Value: synthStateToken(now.Add(-(cfg.TTL - 2*time.Minute))), Valid: true}
	if probeTrack.settledBaseline("gpt-6-astra", cfg, now) {
		t.Fatal("near-expiry baseline must not be settled")
	}
	// Expired: probe.
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra",
		Value: synthStateToken(now.Add(-2 * cfg.TTL)), Valid: true}
	if probeTrack.settledBaseline("gpt-6-astra", cfg, now) {
		t.Fatal("expired baseline must not be settled")
	}
	// Value without a decodable timestamp cannot prove validity: probe.
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra", Value: "not-a-fernet-token", Valid: true}
	if probeTrack.settledBaseline("gpt-6-astra", cfg, now) {
		t.Fatal("undecodable baseline must not be settled")
	}
	// Disabled window (fully manual): the gate stays open.
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra",
		Value: synthStateToken(now.Add(-time.Minute)), Valid: true}
	manual := cfg
	manual.Prefetch = 0
	if probeTrack.settledBaseline("gpt-6-astra", manual, now) {
		t.Fatal("disabled hand-off window must keep the gate open")
	}
}

// TestPrefetchScanTriggersOnlyNearExpiry verifies the watcher's trigger: a
// model outside the window is untouched, a near-expiry model gets one
// throttled attempt, and a parked candidate suppresses further attempts.
func TestPrefetchScanTriggersOnlyNearExpiry(t *testing.T) {
	enabled := true
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		candidates: map[string]stateEntry{}, prefetchGate: map[string]time.Time{},
		lastAttempt: map[string]time.Time{},
		paused:      map[string]bool{}, probing: map[string]bool{}}
	cfg := parseProbeConfig(probeConfigYAML{Enabled: &enabled,
		Models: []string{"gpt-6-astra", "gpt-5.6-sol", "gpt-5.6-luna"}})
	cfg.ProbeInterval = time.Millisecond
	cfg.AccountMode = ""
	probeTrack.cfg.Config = cfg

	fresh := synthStateToken(time.Now().Add(-10 * time.Minute))
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra", Value: fresh, Valid: true}
	// sol: near expiry (remaining <= prefetch window) but feature disabled.
	nearly := synthStateToken(time.Now().Add(-(cfg.TTL - 2*time.Minute)))
	probeTrack.values["gpt-5.6-sol"] = stateEntry{Model: "gpt-5.6-sol", Value: nearly, Valid: true}

	// Disabled prefetch: scan must not touch anything.
	probeTrack.cfg.Config.Prefetch = 0
	probeTrack.prefetchScan()
	if !probeTrack.prefetchGate["gpt-5.6-sol"].IsZero() {
		t.Fatal("disabled prefetch must not gate")
	}

	// Enable and scan: only the near-expiry model gets an attempt.
	probeTrack.cfg.Config.Prefetch = cfg.Prefetch
	probeTrack.prefetchScan()
	waitPrefetchIdle(t, probeTrack)
	if probeTrack.prefetchGate["gpt-6-astra"] != (time.Time{}) {
		t.Fatal("fresh model must not be probed")
	}
	if probeTrack.prefetchGate["gpt-5.6-sol"].IsZero() {
		t.Fatal("near-expiry model must be probed")
	}
	// Second scan within the retry window is throttled (gate unchanged).
	gate := probeTrack.prefetchGate["gpt-5.6-sol"]
	probeTrack.prefetchScan()
	waitPrefetchIdle(t, probeTrack)
	if !probeTrack.prefetchGate["gpt-5.6-sol"].Equal(gate) {
		t.Fatal("retry window must throttle repeats")
	}
	// Parked candidate suppresses further attempts even near expiry.
	candidate := synthStateToken(time.Now())
	waitPrefetchIdle(t, probeTrack)
	probeTrack.mu.Lock()
	probeTrack.candidates["gpt-5.6-sol"] = stateEntry{Model: "gpt-5.6-sol", Value: candidate, Valid: true}
	probeTrack.prefetchGate["gpt-5.6-sol"] = time.Time{}
	probeTrack.mu.Unlock()
	probeTrack.prefetchScan()
	waitPrefetchIdle(t, probeTrack)
	if !probeTrack.prefetchGate["gpt-5.6-sol"].IsZero() {
		t.Fatal("a parked candidate must suppress prefetch")
	}
	t.Cleanup(func() {
		probeTrack.stop()
		waitPrefetchIdle(t, probeTrack)
	})
}

// TestPoolNeverBenched verifies rotating pools (one endpoint presenting many
// source addresses behind it) never leave the rotation, no matter how long
// the failing streak grows: every attempt draws a fresh address, so a streak
// cannot condemn the endpoint - it only reflects the current upstream state.
// Failures keep accumulating for the dashboard, a stale cool-down recorded by
// an older policy is cleared on the next observation, and a healthy capture
// still clears the counters.
func TestPoolNeverBenched(t *testing.T) {
	enabled := true
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{Enabled: &enabled, Models: []string{"gpt-6-astra"}})
	cfg := probeTrack.cfg.Config
	if cfg.ExitPoolFailThreshold != 10 {
		t.Fatalf("pool threshold default wrong: %d", cfg.ExitPoolFailThreshold)
	}
	poolSpec := "socks5h://192.0.2.1:1080"
	probeTrack.cfg.Config.ProxyPools = map[string]bool{poolSpec: true}
	probeTrack.cfg.Config.ProxyLabels = map[string]string{poolSpec: "IPv6 池"}
	pool := []string{"direct", poolSpec}
	now := time.Now().UTC()
	// A dozen failures: the counters grow, the rotation is untouched (the
	// plain-exit threshold would have benched a fixed endpoint at three).
	for i := 0; i < 12; i++ {
		probeTrack.noteExitOutcome(poolSpec, false, "state length 312 != 332 (suspected degraded)")
	}
	if got := probeTrack.availableProxies(pool, now); len(got) != 2 {
		t.Fatalf("a rotating pool must never leave the rotation: %v", got)
	}
	penalty := probeTrack.exitPenalties[poolSpec]
	if penalty.Failures != 12 || penalty.Until != "" {
		t.Fatalf("pool counters must keep accumulating without a cool-down: %+v", penalty)
	}
	// A stale cool-down left behind by an older policy is cleared on the next
	// observation of the rotating pool.
	probeTrack.mu.Lock()
	probeTrack.exitPenalties[poolSpec] = exitPenalty{Proxy: poolSpec, Failures: 12,
		Until: now.Add(time.Hour).Format(time.RFC3339Nano)}
	probeTrack.mu.Unlock()
	probeTrack.noteExitOutcome(poolSpec, false, "boom")
	if got := probeTrack.availableProxies(pool, now); len(got) != 2 {
		t.Fatalf("a stale pool cool-down must be cleared on observation: %v", got)
	}
	if cleared := probeTrack.exitPenalties[poolSpec]; cleared.Until != "" {
		t.Fatalf("stale pool cool-down must be reset: %+v", cleared)
	}
	// A healthy capture still clears the counters entirely.
	probeTrack.noteExitOutcome(poolSpec, true, "")
	if _, ok := probeTrack.exitPenalties[poolSpec]; ok {
		t.Fatal("healthy capture must clear the pool counters")
	}
}

// TestPauseAbortsInFlightRound verifies the v1.5.20 fix: pausing a model
// from the dashboard stops its already-running round at the next attempt
// boundary without writing a failure annotation (a pause is a deliberate
// operator command, not a probe outcome).
func TestPauseAbortsInFlightRound(t *testing.T) {
	enabled := true
	interval := 2
	probeTrack = &probeEngine{
		values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		candidates: map[string]stateEntry{}, prefetchGate: map[string]time.Time{},
		lastAttempt: map[string]time.Time{},
		paused:      map[string]bool{}, probing: map[string]bool{},
	}
	cfg := parseProbeConfig(probeConfigYAML{
		Enabled:         &enabled,
		Models:          []string{"gpt-6-astra"},
		IntervalSeconds: &interval,
	})
	cfg.CredFile = "/nonexistent/cred.json" // fails fast; exercises the real loop
	probeTrack.cfg.Config = cfg

	done := make(chan struct{})
	go func() {
		probeTrack.probeModel("gpt-6-astra", cfg, make(chan struct{}))
		close(done)
	}()
	// Wait for the first attempt to land in the audit journal.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		probeTrack.mu.Lock()
		records := len(probeTrack.history)
		probeTrack.mu.Unlock()
		if records >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	probeTrack.setModelPaused("gpt-6-astra", true)
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("pausing must abort the in-flight round")
	}
	probeTrack.mu.Lock()
	total := probeTrack.probesTotal
	_, annotated := probeTrack.failures["gpt-6-astra"]
	probeTrack.mu.Unlock()
	// The aborted round must not keep probing after the pause; a bug would
	// fire the next attempt after the 2s interval.
	time.Sleep(3 * time.Second)
	probeTrack.mu.Lock()
	after := probeTrack.probesTotal
	probeTrack.mu.Unlock()
	if after != total {
		t.Fatalf("aborted round must not keep probing: %d -> %d", total, after)
	}
	if annotated {
		t.Fatal("an operator pause must not write a failure annotation")
	}
}

// TestStopAbortsAllInFlight verifies the v1.5.21 global brake: the dashboard
// "stop" control terminates every in-flight probe (sequential or per-model
// asynchronous) at the next attempt boundary, clears the in-round suspicion
// counters, and leaves the engine idle without failure annotations.
func TestStopAbortsAllInFlight(t *testing.T) {
	enabled := true
	interval := 2
	probeTrack = &probeEngine{
		values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		candidates: map[string]stateEntry{}, prefetchGate: map[string]time.Time{},
		lastAttempt: map[string]time.Time{},
		paused:      map[string]bool{}, probing: map[string]bool{},
	}
	cfg := parseProbeConfig(probeConfigYAML{
		Enabled:         &enabled,
		Models:          []string{"gpt-6-astra", "gpt-5.6-sol"},
		IntervalSeconds: &interval,
	})
	cfg.CredFile = "/nonexistent/cred.json" // fails fast; exercises the real loop
	probeTrack.cfg.Config = cfg

	done := make(chan struct{})
	for _, model := range []string{"gpt-6-astra", "gpt-5.6-sol"} {
		go func(name string) {
			probeTrack.probeModel(name, cfg, make(chan struct{}))
			done <- struct{}{}
		}(model)
	}
	// Wait for both rounds to land their first attempts.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		probeTrack.mu.Lock()
		records := len(probeTrack.history)
		probeTrack.mu.Unlock()
		if records >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	probeTrack.stop()
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Fatal("the global stop must abort every in-flight round")
		}
	}
	probeTrack.mu.Lock()
	total := probeTrack.probesTotal
	annotationA := probeTrack.failures["gpt-6-astra"]
	annotationB := probeTrack.failures["gpt-5.6-sol"]
	probing := len(probeTrack.probing)
	suspects := len(probeTrack.suspects)
	runNote := probeTrack.runNote
	probeTrack.mu.Unlock()
	time.Sleep(3 * time.Second)
	probeTrack.mu.Lock()
	after := probeTrack.probesTotal
	probeTrack.mu.Unlock()
	if after != total {
		t.Fatalf("stopped rounds must not keep probing: %d -> %d", total, after)
	}
	if annotationA.Attempts != 0 || annotationB.Attempts != 0 {
		t.Fatalf("a global stop must not write failure annotations: %+v / %+v", annotationA, annotationB)
	}
	if probing != 0 {
		t.Fatalf("no model may remain marked as probing: %d", probing)
	}
	if suspects != 0 {
		t.Fatalf("in-round suspicion counters must be cleared: %d", suspects)
	}
	if !strings.Contains(runNote, "已停止所有探测") {
		t.Fatalf("the stop must be reported on the dashboard: %q", runNote)
	}
}

// TestWatcherSilentAfterStop verifies the v1.5.22 halt semantics: after a
// global stop the automatic hand-off watcher stays silent (no prefetch gate
// is armed for an expiring baseline), while an explicit request re-ignites
// the engine.
func TestWatcherSilentAfterStop(t *testing.T) {
	enabled := true
	newPrefetchTestEngine(t) // Stop and drain this engine before the next test replaces the global.
	cfg := parseProbeConfig(probeConfigYAML{Enabled: &enabled, Models: []string{"gpt-6-astra"}})
	cfg.CredFile = "/nonexistent/cred.json"
	cfg.ProbeInterval = time.Millisecond
	cfg.AccountMode = ""
	probeTrack.cfg.Config = cfg
	// A baseline about to expire: the watcher would normally probe it.
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra",
		Value: synthStateToken(time.Now().Add(-(cfg.TTL - 2*time.Minute))), Valid: true}

	probeTrack.stop()
	probeTrack.mu.Lock()
	halted := probeTrack.halted
	probeTrack.mu.Unlock()
	if !halted {
		t.Fatal("the global stop must halt the engine")
	}
	probeTrack.prefetchScan()
	if !probeTrack.prefetchGate["gpt-6-astra"].IsZero() {
		t.Fatal("a halted engine must not arm the hand-off watcher")
	}

	// An explicit one-off request runs while halted but keeps the halt; the
	// only re-ignition path is the explicit "start round".
	if !halted {
		t.Fatal("the engine must stay halted")
	}
	probeTrack.start()
	probeTrack.mu.Lock()
	reignited := !probeTrack.halted
	probeTrack.mu.Unlock()
	if !reignited {
		t.Fatal("start-round must re-ignite the engine")
	}
	waitPrefetchIdle(t, probeTrack)
}

// TestOneOffProbeDoesNotReignite verifies the v1.5.24 semantics: a one-off
// "probe now" for a single model runs even while the engine is halted, but
// it never re-ignites the engine (the halt and the silent watcher stay);
// resuming a paused model is the deliberate re-ignition path.
func TestOneOffProbeDoesNotReignite(t *testing.T) {
	enabled := true
	probeTrack = &probeEngine{
		values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		candidates: map[string]stateEntry{}, prefetchGate: map[string]time.Time{},
		lastAttempt: map[string]time.Time{},
		paused:      map[string]bool{}, probing: map[string]bool{},
	}
	cfg := parseProbeConfig(probeConfigYAML{Enabled: &enabled, Models: []string{"gpt-6-astra", "gpt-5.6-sol"}})
	cfg.CredFile = "/nonexistent/cred.json"
	// Fast pacing: the resume task must not wait behind the production
	// default interval while the 5s deadline runs out.
	cfg.ProbeInterval = time.Millisecond
	probeTrack.cfg.Config = cfg

	probeTrack.stop() // halted
	probeTrack.probeModelAsync("gpt-6-astra")
	probeTrack.mu.Lock()
	stillHalted := probeTrack.halted
	probeTrack.mu.Unlock()
	if !stillHalted {
		t.Fatal("a one-off probe must not re-ignite the halted engine")
	}
	// The one-off probe itself still runs (and fails fast on the missing
	// credential file). Wait for its record to land - checking the busy flag
	// can race with an unscheduled goroutine.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		probeTrack.mu.Lock()
		records := len(probeTrack.history)
		probeTrack.mu.Unlock()
		if records > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	probeTrack.mu.Lock()
	records := len(probeTrack.history)
	probeTrack.mu.Unlock()
	if records == 0 {
		t.Fatal("the one-off probe must actually run")
	}
	// The watcher stays suppressed while halted.
	probeTrack.prefetchScan()
	if !probeTrack.prefetchGate["gpt-5.6-sol"].IsZero() {
		t.Fatal("the halted engine must keep the watcher silent")
	}
	// Resuming a paused model while halted probes that model once (so the
	// action is visible) but never re-ignites the engine.
	probeTrack.mu.Lock()
	baseRecords := len(probeTrack.history)
	probeTrack.mu.Unlock()
	probeTrack.setModelPaused("gpt-5.6-sol", true)
	probeTrack.setModelPaused("gpt-5.6-sol", false)
	probeTrack.mu.Lock()
	stillHalted = probeTrack.halted
	probeTrack.mu.Unlock()
	if !stillHalted {
		t.Fatal("resuming while halted must not re-ignite the engine")
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		probeTrack.mu.Lock()
		records = len(probeTrack.history)
		probeTrack.mu.Unlock()
		if records > baseRecords {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if records <= baseRecords {
		t.Fatal("resuming must run one visible probe for that model")
	}
	probeTrack.mu.Lock()
	stillHalted = probeTrack.halted
	probeTrack.mu.Unlock()
	if !stillHalted {
		t.Fatal("the resume probe must keep the engine halted")
	}
}

// TestResetExit verifies the manual reset control: one egress's cool-down or
// scheduled rest is cleared immediately, a full reset clears everything,
// and a no-op reset leaves other entries untouched.
func TestResetExit(t *testing.T) {
	enabled := true
	probeTrack = &probeEngine{
		values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		candidates: map[string]stateEntry{}, prefetchGate: map[string]time.Time{},
		lastAttempt: map[string]time.Time{},
		paused:      map[string]bool{}, probing: map[string]bool{},
	}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{Enabled: &enabled, Models: []string{"gpt-6-astra"}})
	cfg := probeTrack.cfg.Config
	pool := []string{"direct", "socks5://a:b@192.0.2.2:1080"}
	now := time.Now().UTC()
	for _, spec := range pool {
		for i := 0; i < 3; i++ {
			probeTrack.noteExitOutcome(spec, false, "boom")
		}
	}
	for _, spec := range pool {
		penalty, ok := probeTrack.exitPenalties[spec]
		if !ok || penalty.Until == "" {
			t.Fatalf("exit must be cooling after the streak: %v", spec)
		}
	}
	if removed := probeTrack.resetExit(pool[0]); removed != 1 {
		t.Fatalf("single reset must remove one entry: %d", removed)
	}
	if _, ok := probeTrack.exitPenalties[pool[0]]; ok {
		t.Fatal("the reset entry must be gone")
	}
	if _, ok := probeTrack.exitPenalties[pool[1]]; !ok {
		t.Fatal("other entries must stay untouched")
	}
	// The reset exit is usable again even while the other one cools.
	available := probeTrack.availableProxies(pool, now)
	found := false
	for _, spec := range available {
		if spec == pool[0] {
			found = true
		}
	}
	if !found {
		t.Fatalf("reset exit must return to rotation: %v", available)
	}
	if removed := probeTrack.resetExit(pool[0]); removed != 0 {
		t.Fatalf("an already clean exit reports zero: %d", removed)
	}
	if removed := probeTrack.resetExit(""); removed != 1 {
		t.Fatalf("full reset must clear the rest: %d", removed)
	}
	if len(probeTrack.exitPenalties) != 0 {
		t.Fatal("full reset must empty the table")
	}
	if got := probeTrack.availableProxies(pool, now); len(got) != len(pool) {
		t.Fatalf("everything must be usable after a full reset: %v", got)
	}
	_ = cfg
}

// TestExitEnableDisable verifies the manual per-egress switch: a disabled
// egress is never used by rotation (even when everything else is cooling and
// even via the emergency/last-resort paths), and enabling it brings it back.
func TestExitEnableDisable(t *testing.T) {
	enabled := true
	probeTrack = &probeEngine{
		values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		candidates: map[string]stateEntry{}, prefetchGate: map[string]time.Time{},
		lastAttempt: map[string]time.Time{},
		paused:      map[string]bool{}, probing: map[string]bool{},
	}
	cfg := parseProbeConfig(probeConfigYAML{Enabled: &enabled, Models: []string{"gpt-6-astra"}})
	cfg.Proxies = []string{"direct", "socks5://a:b@192.0.2.2:1080"}
	probeTrack.cfg.Config = cfg
	pool := cfg.Proxies
	now := time.Now().UTC()

	if !probeTrack.setExitEnabled(pool[0], false) {
		t.Fatal("disabling a configured exit must succeed")
	}
	if probeTrack.setExitEnabled("socks5://not-configured:1", false) {
		t.Fatal("unknown exits must be rejected")
	}
	got := probeTrack.availableProxies(pool, now)
	for _, spec := range got {
		if spec == pool[0] {
			t.Fatal("a disabled exit must not appear in rotation")
		}
	}

	// Everything else cooling + emergency release must still skip it.
	for i := 0; i < 3; i++ {
		probeTrack.noteExitOutcome(pool[1], false, "boom")
	}
	got = probeTrack.availableProxies(pool, now)
	for _, spec := range got {
		if spec == pool[0] {
			t.Fatal("the last-resort fallback must skip disabled exits")
		}
	}

	// Enabling brings it back.
	if !probeTrack.setExitEnabled(pool[0], true) {
		t.Fatal("enabling must succeed for configured exits")
	}
	got = probeTrack.availableProxies(pool, now)
	found := false
	for _, spec := range got {
		if spec == pool[0] {
			found = true
		}
	}
	if !found {
		t.Fatalf("an enabled exit must return to rotation: %v", got)
	}
}

// TestPoolBudgetShare verifies the v1.5.19 adaptive per-round budget: plain
// egresses contribute attempts-per-proxy tries each, rotating pools
// contribute their own pool-attempts budget (default 100), an explicit
// max-attempts-per-round still overrides everything, and pool-attempts: 0
// disables the pool contribution.
func TestPoolBudgetShare(t *testing.T) {
	enabled := true
	cfg := parseProbeConfig(probeConfigYAML{Enabled: &enabled, Models: []string{"gpt-6-astra"}})
	if cfg.PoolAttempts != 100 {
		t.Fatalf("pool budget default wrong: %d", cfg.PoolAttempts)
	}
	pool := "socks5h://192.0.2.1:1080"
	cfg.ProxyPools = map[string]bool{pool: true}
	proxies := []string{"direct", "socks5://a:b@192.0.2.2:1080", pool}
	if got := effectiveMaxAttempts(cfg, proxies); got != 3+3+100 {
		t.Fatalf("auto cap must weight pools separately: %d", got)
	}
	// All plain exits: legacy auto behaviour stays (count x per-hop).
	plain := []string{"direct", "socks5://a:b@192.0.2.2:1080"}
	if got := effectiveMaxAttempts(cfg, plain); got != 6 {
		t.Fatalf("plain-only auto cap wrong: %d", got)
	}
	// Explicit override wins.
	cfg.MaxAttemptsPerRound = 7
	if got := effectiveMaxAttempts(cfg, proxies); got != 7 {
		t.Fatalf("explicit cap must override: %d", got)
	}
	cfg.MaxAttemptsPerRound = 0
	// Custom pool-attempts is honoured.
	cfg.PoolAttempts = 40
	if got := effectiveMaxAttempts(cfg, proxies); got != 3+3+40 {
		t.Fatalf("custom pool budget wrong: %d", got)
	}
}

// TestPickRotationBudgets drives the round-robin with per-egress budgets: the
// walk rotates while plain shares last, then skips exhausted egresses so a
// large pool keeps drawing its remaining budget, and the round closes only
// once every usable egress is spent.
func TestPickRotationBudgets(t *testing.T) {
	direct := "direct"
	pool := "socks5h://192.0.2.1:1080"
	rotation := []string{direct, pool}
	budgets := map[string]int{direct: 2, pool: 4}
	cursor := 0
	var sequence []string
	for {
		spec, next, ok := pickRotation(rotation, budgets, cursor)
		if !ok {
			break
		}
		cursor = next
		budgets[spec]--
		sequence = append(sequence, spec)
	}
	if len(sequence) != 6 {
		t.Fatalf("every share must be spent exactly once: %v", sequence)
	}
	// The first three picks rotate through both egresses; the tail is the
	// pool's remaining budget only.
	for i, spec := range sequence[:3] {
		if i%2 == 0 && spec != direct {
			t.Fatalf("round-robin phase must alternate: %v", sequence)
		}
		if i%2 == 1 && spec != pool {
			t.Fatalf("round-robin phase must alternate: %v", sequence)
		}
	}
	for _, spec := range sequence[3:] {
		if spec != pool {
			t.Fatalf("tail must be the pool's leftover budget: %v", sequence)
		}
	}
	// A fully spent set reports the round as over.
	if _, _, ok := pickRotation(rotation, map[string]int{direct: 0, pool: 0}, 0); ok {
		t.Fatal("a spent rotation must end the round")
	}
}

func TestProbeSummaryShape(t *testing.T) {
	summary := probeSummary()
	for _, key := range []string{"enabled", "models", "proxies", "proxies_state", "pool_total", "pool_active",
		"exit_fail_threshold", "exit_pool_fail_threshold", "exit_cooldown_minutes", "values", "history", "ttl_minutes", "window_minutes", "running", "seeded", "pool_attempts"} {
		if _, ok := summary[key]; !ok {
			t.Fatalf("probe summary missing %s: %+v", key, summary)
		}
	}
}

// TestExitCircuitBreaker drives the egress pool cool-down: consecutive failures
// remove an exit from rotation once the threshold is reached, an expiry
// releases it with a fresh window, successes clear it, and the emergency
// release keeps at least exit-min-active usable.
func TestExitCircuitBreaker(t *testing.T) {
	enabled := true
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	probeTrack.cfg.Config = parseProbeConfig(probeConfigYAML{Enabled: &enabled, Models: []string{"gpt-6-astra"}})
	cfg := probeTrack.cfg.Config
	if cfg.ExitCooldown != 180*time.Minute || cfg.ExitFailThreshold != 3 || cfg.ExitMinActive != 1 {
		t.Fatalf("exit defaults wrong: cooldown=%v threshold=%d min=%d", cfg.ExitCooldown, cfg.ExitFailThreshold, cfg.ExitMinActive)
	}
	if cfg.ExitSuccessCooldown != 30*time.Minute {
		t.Fatalf("exit success rest default wrong: %v", cfg.ExitSuccessCooldown)
	}
	pool := []string{"direct", "socks5h://[2001:db8::1]:1080", "socks5://a:b@192.0.2.2:1080"}
	now := time.Now().UTC()
	if got := probeTrack.availableProxies(pool, now); len(got) != 3 {
		t.Fatalf("all exits must start usable: %v", got)
	}
	// Two failures below threshold: still usable.
	probeTrack.noteExitOutcome(pool[1], false, "state length 312 != 332 (suspected degraded)")
	probeTrack.noteExitOutcome(pool[1], false, "state length 312 != 332 (suspected degraded)")
	if got := probeTrack.availableProxies(pool, now); len(got) != 3 {
		t.Fatalf("below-threshold exit must stay: %v", got)
	}
	// Third failure: removed from rotation.
	probeTrack.noteExitOutcome(pool[1], false, "state length 312 != 332 (suspected degraded)")
	got := probeTrack.availableProxies(pool, now)
	if len(got) != 2 {
		t.Fatalf("cooled-down exit must be skipped: %v", got)
	}
	for _, spec := range got {
		if spec == pool[1] {
			t.Fatal("penalized exit must not be in rotation")
		}
	}
	penalty := probeTrack.exitPenalties[pool[1]]
	if penalty.Failures != 3 || penalty.Until == "" {
		t.Fatalf("penalty shape wrong: %+v", penalty)
	}
	until, err := time.Parse(time.RFC3339Nano, penalty.Until)
	if err != nil || until.Sub(now) < 170*time.Minute {
		t.Fatalf("cool-down window wrong: %v (%v)", penalty.Until, err)
	}
	// Expiry releases it with a fresh window.
	probeTrack.mu.Lock()
	probeTrack.exitPenalties[pool[1]] = exitPenalty{Proxy: pool[1], Failures: 5,
		Until: now.Add(-time.Minute).Format(time.RFC3339Nano)}
	probeTrack.mu.Unlock()
	if got := probeTrack.availableProxies(pool, time.Now().UTC()); len(got) != 3 {
		t.Fatalf("expired cool-down must be released: %v", got)
	}
	probeTrack.mu.Lock()
	released := probeTrack.exitPenalties[pool[1]]
	probeTrack.mu.Unlock()
	if released.Failures != 0 || released.Until != "" {
		t.Fatalf("release must reset the failure window: %+v", released)
	}
	// A success schedules a short rest (default exit-success-cooldown-minutes
	// = 30): the rotation spreads next attempts across the other exits
	// instead of hammering the same address.
	probeTrack.noteExitOutcome(pool[1], false, "boom")
	probeTrack.noteExitOutcome(pool[1], false, "boom")
	probeTrack.noteExitOutcome(pool[1], false, "boom")
	probeTrack.noteExitOutcome(pool[1], true, "")
	rest, ok := probeTrack.exitPenalties[pool[1]]
	if !ok || !rest.Success || rest.Until == "" || rest.Failures != 0 {
		t.Fatalf("healthy capture must schedule a rest: %+v", rest)
	}
	if got := probeTrack.availableProxies(pool, time.Now().UTC()); len(got) != 2 {
		t.Fatalf("a resting exit must leave the rotation: %v", got)
	}
	// The rest expires on the clock and the exit returns automatically.
	probeTrack.mu.Lock()
	probeTrack.exitPenalties[pool[1]] = exitPenalty{Proxy: pool[1], Success: true,
		Until: time.Now().Add(-time.Minute).Format(time.RFC3339Nano)}
	probeTrack.mu.Unlock()
	if got := probeTrack.availableProxies(pool, time.Now().UTC()); len(got) != 3 {
		t.Fatalf("an expired rest must be released: %v", got)
	}
	probeTrack.mu.Lock()
	releasedRest := probeTrack.exitPenalties[pool[1]]
	probeTrack.mu.Unlock()
	if releasedRest.Success || releasedRest.Until != "" {
		t.Fatalf("release must clear the rest flags: %+v", releasedRest)
	}
	// exit-success-cooldown-minutes: 0 restores the legacy behaviour where
	// success clears the entry immediately.
	probeTrack.cfg.Config.ExitSuccessCooldown = 0
	probeTrack.noteExitOutcome(pool[1], true, "")
	if _, ok := probeTrack.exitPenalties[pool[1]]; ok {
		t.Fatal("legacy mode: healthy capture must clear the entry")
	}
	probeTrack.cfg.Config.ExitSuccessCooldown = 30 * time.Minute
	// Emergency release: with every exit cooled down, the minimum active set
	// is freed so a round can still run.
	for _, spec := range pool {
		for i := 0; i < 3; i++ {
			probeTrack.noteExitOutcome(spec, false, "failed")
		}
	}
	got = probeTrack.availableProxies(pool, time.Now().UTC())
	if len(got) < 1 {
		t.Fatalf("emergency release must free at least the minimum active set: %v", got)
	}
}

// TestValidityDisplay drives the token-derived validity window exposed to the
// dashboard: issued_at/expires_at/remaining_seconds/expired are computed from
// the embedded timestamp plus the configured validity duration (no capture-time
// arithmetic).
func TestValidityDisplay(t *testing.T) {
	enabled := true
	probeTrack = &probeEngine{values: map[string]stateEntry{}, failures: map[string]probeFailure{},
		paused: map[string]bool{}, probing: map[string]bool{}}
	cfg := parseProbeConfig(probeConfigYAML{Enabled: &enabled, TTLMinutes: nil})
	cfg.AccountMode = ""
	probeTrack.cfg.Config = cfg
	if cfg.TTL != 55*time.Minute {
		t.Fatalf("default validity wrong: %v", cfg.TTL)
	}
	issued := time.Now().Add(-20 * time.Minute).Truncate(time.Second)
	fresh := synthStateToken(issued)
	if len(fresh) != probeRequiredStateLength {
		t.Fatalf("synthetic token must be 332 characters: %d", len(fresh))
	}
	probeTrack.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra", Value: fresh,
		ValueLength: len(fresh), Source: "probe", Valid: true}
	expiredTs := time.Now().Add(-2 * cfg.TTL).Truncate(time.Second)
	stale := synthStateToken(expiredTs)
	probeTrack.values["gpt-5.6-sol"] = stateEntry{Model: "gpt-5.6-sol", Value: stale,
		ValueLength: len(stale), Source: "seed", Valid: true}

	summary := probeSummary()
	byModel := map[string]map[string]any{}
	for _, item := range summary["values"].([]map[string]any) {
		byModel[item["model"].(string)] = item
	}
	freshItem := byModel["gpt-6-astra"]
	if freshItem == nil {
		t.Fatalf("missing astra entry: %+v", summary["values"])
	}
	if got := freshItem["issued_at"]; got != issued.UTC().Format(time.RFC3339) {
		t.Fatalf("issued_at must come from the token: %v vs %v", got, issued.UTC().Format(time.RFC3339))
	}
	if got := freshItem["expires_at"]; got != issued.Add(cfg.TTL).UTC().Format(time.RFC3339) {
		t.Fatalf("expires_at wrong: %v", got)
	}
	if freshItem["expired"] != false {
		t.Fatalf("fresh token must not be expired: %+v", freshItem)
	}
	remaining, ok := freshItem["remaining_seconds"].(int64)
	if !ok || remaining < int64((cfg.TTL-21*time.Minute).Seconds()) || remaining > int64((cfg.TTL-19*time.Minute).Seconds()) {
		t.Fatalf("remaining_seconds out of range: %v (%T)", freshItem["remaining_seconds"], freshItem["remaining_seconds"])
	}
	staleItem := byModel["gpt-5.6-sol"]
	if staleItem == nil || staleItem["expired"] != true {
		t.Fatalf("stale token must be flagged expired: %+v", staleItem)
	}
	if remaining, ok := staleItem["remaining_seconds"].(int64); !ok || remaining >= 0 {
		t.Fatalf("expired token must report a negative remaining: %v", staleItem["remaining_seconds"])
	}
}

// TestTurnStateOverrideConfigParsing drives the config parser with realistic
// lifecycle payloads and checks activation, trimming and validation errors.
func TestTurnStateOverrideConfigParsing(t *testing.T) {
	turnStateOverride = atomic.Value{}
	t.Cleanup(func() { turnStateOverride = atomic.Value{} })

	valid := []byte(`enabled: true
priority: 100
store:
  version: 1.3.0
turn-state-override:
  enabled: true
  models: ["gpt-6-astra", " gpt-6-luna "]
  value: "SAMPLE-1"
  force: true
`)
	if err := configureTurnStateOverride(valid); err != nil {
		t.Fatal(err)
	}
	summary := turnStateOverrideSummary()
	if summary["enabled"] != true || summary["force"] != true || summary["value_length"] != 8 {
		t.Fatalf("summary wrong: %+v", summary)
	}
	guard := summary["session_guard"].(map[string]any)
	if guard["mode"] != sessionGuardModeObserve || guard["ttl_minutes"] != 60 {
		t.Fatalf("session guard defaults wrong: %+v", guard)
	}
	models := summary["models"].([]string)
	if len(models) != 2 || models[0] != "gpt-6-astra" || models[1] != "gpt-6-luna" {
		t.Fatalf("models not trimmed: %+v", models)
	}

	if err := configureTurnStateOverride([]byte(`turn-state-override:
  enabled: true
  models: []
  value: x
`)); err == nil {
		t.Fatal("empty models must be rejected")
	}
	if summary := turnStateOverrideSummary(); summary["error"] == "" {
		t.Fatalf("config error must be surfaced: %+v", summary)
	}
	if err := configureTurnStateOverride([]byte(`turn-state-override:
  session-guard-mode: invalid
`)); err == nil {
		t.Fatal("invalid session guard mode must be rejected")
	}
	if err := configureTurnStateOverride([]byte(`turn-state-override:
  session-provenance-ttl-minutes: 1441
`)); err == nil {
		t.Fatal("invalid session provenance TTL must be rejected")
	}

	if err := configureTurnStateOverride([]byte(`turn-state-override:
  enabled: true
  models: ["gpt-6-astra"]
  value: ""
`)); err != nil {
		t.Fatalf("passive business mode must allow empty static value: %v", err)
	}
}

// TestTimezoneConfigParsing checks the optional top-level timezone override:
// valid IANA names activate, invalid names reject the config, and omission
// restores the default.
func TestTimezoneConfigParsing(t *testing.T) {
	t.Cleanup(func() { configuredTimezone.Store(targetTimezone) })

	if err := configureTurnStateOverride([]byte(`timezone: "Asia/Singapore"`)); err != nil {
		t.Fatal(err)
	}
	if zone := currentTimezone(); zone != "Asia/Singapore" {
		t.Fatalf("configured zone must activate, got %q", zone)
	}

	if err := configureTurnStateOverride([]byte(`timezone: "Not/AZone"`)); err == nil {
		t.Fatal("invalid zone must be rejected")
	}
	if summary := turnStateOverrideSummary(); summary["error"] == "" {
		t.Fatalf("invalid zone must surface an error: %+v", summary)
	}

	if err := configureTurnStateOverride([]byte(`enabled: true`)); err != nil {
		t.Fatal(err)
	}
	if zone := currentTimezone(); zone != targetTimezone {
		t.Fatalf("omitted zone must restore the default, got %q", zone)
	}
}

// TestTurnStateOverrideApply drives the rewrite decision matrix.
func TestTurnStateOverrideApply(t *testing.T) {
	turnStateOverride = atomic.Value{}
	t.Cleanup(func() { turnStateOverride = atomic.Value{} })
	// Isolate from other tests: the shared baseline table must be empty here.
	probeTrack.values = map[string]stateEntry{}

	config := `turn-state-override:
  enabled: true
  models: ["gpt-6-astra"]
  value: "REWRITTEN-STATE"
`
	if err := configureTurnStateOverride([]byte(config)); err != nil {
		t.Fatal(err)
	}

	headers, status := applyTurnStateOverride("gpt-6-astra", "", nil)
	if status != "applied-config" || headers.Get(turnStateHeader) != "REWRITTEN-STATE" {
		t.Fatalf("astra must be rewritten: %v %+v", status, headers)
	}
	headers, status = applyTurnStateOverride("GPT-6-ASTRA-preview", "", nil)
	if status != "applied-config" || headers.Get(turnStateHeader) != "REWRITTEN-STATE" {
		t.Fatalf("case/prefix match must rewrite: %v %+v", status, headers)
	}
	if _, status = applyTurnStateOverride("gpt-6-luna", "", nil); status != "" {
		t.Fatalf("unmatched model must not rewrite: %v", status)
	}
	if headers, status = applyTurnStateOverride("", "gpt-6-astra", nil); status != "applied-config" {
		t.Fatalf("requested model fallback must rewrite: %v %+v", status, headers)
	}

	// Fill mode keeps a healthy-length existing client value.
	healthyClient := synthBusinessState(3)
	clientHeaders := http.Header{turnStateHeader: {healthyClient}}
	if headers, status = applyTurnStateOverride("gpt-6-astra", "", clientHeaders); status != "skipped-existing" || headers != nil {
		t.Fatalf("fill mode must skip a healthy existing value: %v %+v", status, headers)
	}

	// An unhealthy-length client value is replaced even in fill mode.
	degradedHeaders := http.Header{turnStateHeader: {strings.Repeat("A", 312)}}
	if headers, status = applyTurnStateOverride("gpt-6-astra", "", degradedHeaders); status != "applied-config" || headers.Get(turnStateHeader) != "REWRITTEN-STATE" {
		t.Fatalf("degraded existing value must be replaced in fill mode: %v %+v", status, headers)
	}

	// Force mode replaces it.
	if err := configureTurnStateOverride([]byte(config + "  force: true\n")); err != nil {
		t.Fatal(err)
	}
	if headers, status = applyTurnStateOverride("gpt-6-astra", "", clientHeaders); status != "applied-config" || headers.Get(turnStateHeader) != "REWRITTEN-STATE" {
		t.Fatalf("force mode must replace: %v %+v", status, headers)
	}

	// Disabled config means no action at all.
	if err := configureTurnStateOverride([]byte(`turn-state-override:
  enabled: false
  models: ["gpt-6-astra"]
  value: "x"
`)); err != nil {
		t.Fatal(err)
	}
	if _, status = applyTurnStateOverride("gpt-6-astra", "", nil); status != "" {
		t.Fatalf("disabled config must not act: %v", status)
	}
}

// TestInterceptRequestAppliesRewrite checks the request interceptor returns the
// header replacement and records the override status alongside the timezone
// normalization.
func TestInterceptRequestAppliesRewrite(t *testing.T) {
	installFixtureHostAuth(t)
	history = auditState{}
	turnStateOverride = atomic.Value{}
	t.Cleanup(func() { turnStateOverride = atomic.Value{} })
	probeTrack.values = map[string]stateEntry{}
	if err := configureTurnStateOverride([]byte(`turn-state-override:
  enabled: true
  models: ["gpt-6-astra"]
  value: "REWRITTEN-STATE"
  force: true
`)); err != nil {
		t.Fatal(err)
	}
	value := synthStateToken(time.Now())
	probeTrack.storeValue(scopedTarget(credentialScope("fixture-account", "synthetic-fixture-token", ""), "gpt-6-astra"), value, "probe", "", currentProbeConfig().Config)
	raw, _ := json.Marshal(interceptRequest{Metadata: map[string]any{"selected_auth_id": "fixture-account", "selected_auth_index": "fixture-index"},
		RequestID: "req-rw", ToFormat: "codex", Model: "gpt-6-astra",
		Headers: http.Header{turnStateHeader: {"CLIENT-STATE"}, "Authorization": {"Bearer synthetic-fixture-token"}},
		Body:    []byte(`{"input":"hello"}`),
	})
	resp, err := intercept(raw)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Headers == nil || resp.Headers.Get(turnStateHeader) != value {
		t.Fatalf("interceptor must return the header rewrite: %+v", resp)
	}
	record := history.snapshot()["records"].([]auditRecord)[0]
	if record.TurnStateOverride != "applied" {
		t.Fatalf("record must show applied-config: %+v", record)
	}
	// The observed request-side value stays the client's original view.
	if record.TurnStateValue != "CLIENT-STATE" || record.TurnStateSource != "request" {
		t.Fatalf("observation must keep the original view: %+v", record)
	}
}

// TestLifecycleConfigExtraction feeds a base64 config_yaml lifecycle payload as
// the host sends it.
func TestLifecycleConfigExtraction(t *testing.T) {
	turnStateOverride = atomic.Value{}
	t.Cleanup(func() { turnStateOverride = atomic.Value{} })
	payload, _ := json.Marshal(map[string]any{
		"config_yaml":    []byte("turn-state-override:\n  enabled: true\n  models: [\"gpt-6-astra\"]\n  value: \"X\"\n"),
		"schema_version": 6,
	})
	configureTurnStateOverrideFromLifecycle(payload)
	if summary := turnStateOverrideSummary(); summary["enabled"] != true {
		t.Fatalf("lifecycle extraction failed: %+v", summary)
	}
	// Malformed payloads keep the previous state.
	configureTurnStateOverrideFromLifecycle([]byte("not json"))
	if summary := turnStateOverrideSummary(); summary["enabled"] != true {
		t.Fatalf("malformed payload must keep state: %+v", summary)
	}
}

// TestRegistrationDeclaresResponseCapabilities ensures the new hooks are
// advertised to the host, otherwise CPA never calls them.
func TestRegistrationDeclaresResponseCapabilities(t *testing.T) {
	t.Setenv("LKS_MIRROR_URL", "") // Registration tests must never publish to an operator's configured receiver.
	result, err := handleMethod("plugin.register", nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(result)
	for _, key := range []string{
		`"response_interceptor":true`,
		`"response_stream_interceptor":true`,
		`"websocket_response_observer":true`,
		`"schema_version":6`,
	} {
		if !bytes.Contains(encoded, []byte(key)) {
			t.Errorf("registration missing %s in %s", key, encoded)
		}
	}
}

// TestResponseHookMethodsAreRouted verifies the exact method names the host
// dispatches match what handleMethod accepts.
func TestResponseHookMethodsAreRouted(t *testing.T) {
	for _, method := range []string{"response.intercept_after", "response.intercept_stream_chunk", "websocket.response_event"} {
		if _, err := handleMethod(method, []byte(`{"RequestID":"x"}`)); err != nil {
			t.Errorf("handleMethod(%s) failed: %v", method, err)
		}
	}
}

// TestAuditUpsertPreservesObservations checks a repeated request record keeps
// the already observed upstream model and turn-state.
func TestAuditUpsertPreservesObservations(t *testing.T) {
	var state auditState
	state.record(auditRecord{RequestID: "retry-1", Model: "gpt-5.6-sol", conversion: conversion{Target: targetTimezone, Action: "inserted"}})
	state.observeModel("retry-1", "", "gpt-6-luna")
	state.observeTurnState("retry-1", "abcdef", "stream")
	// A retry re-records the request level; observed fields must survive.
	state.record(auditRecord{RequestID: "retry-1", Model: "gpt-5.6-sol", conversion: conversion{Target: targetTimezone, Action: "unchanged"}})
	records := state.snapshot()["records"].([]auditRecord)
	if len(records) != 1 {
		t.Fatalf("retry must update in place: %d records", len(records))
	}
	record := records[0]
	if record.UpstreamModel != "gpt-6-luna" || !record.ModelMismatch || record.TurnStateLength != 6 {
		t.Fatalf("observations lost on upsert: %+v", record)
	}
}

// TestTurnStateInjectedLengthRecorded verifies the byte length written into
// the header is recorded independently of the original request ticket and
// remains available even before any response has been observed.
func TestTurnStateInjectedLengthRecorded(t *testing.T) {
	installFixtureHostAuth(t)
	history = auditState{}
	turnStateOverride = atomic.Value{}
	t.Cleanup(func() { turnStateOverride = atomic.Value{} })
	probeTrack.values = map[string]stateEntry{}
	if err := configureTurnStateOverride([]byte(`turn-state-override:
  enabled: true
  models: ["gpt-6-astra"]
  value: "REWRITTEN-STATE"
  force: true
`)); err != nil {
		t.Fatal(err)
	}
	value := synthStateToken(time.Now())
	probeTrack.storeValue(scopedTarget(credentialScope("fixture-account", "synthetic-fixture-token", ""), "gpt-6-astra"), value, "probe", "", currentProbeConfig().Config)
	raw, _ := json.Marshal(interceptRequest{Metadata: map[string]any{"selected_auth_id": "fixture-account", "selected_auth_index": "fixture-index"},
		Headers: http.Header{"Authorization": {"Bearer synthetic-fixture-token"}, turnStateHeader: {"old"}}, RequestID: "req-inj", ToFormat: "codex", Model: "gpt-6-astra",
		Body: []byte(`{"input":"hello"}`),
	})
	if _, err := intercept(raw); err != nil {
		t.Fatal(err)
	}
	record := history.snapshot()["records"].([]auditRecord)[0]
	if record.TurnStateLength != 3 {
		t.Fatal("original request ticket length was not retained")
	}
	if record.TurnStateOverride != "applied" || record.TurnStateInjectedLength != len(value) {
		t.Fatalf("injected length must be recorded: %+v", record)
	}
	// A retry keeps the recorded injection length.
	history.record(auditRecord{AccountScope: credentialScope("fixture-account", "synthetic-fixture-token", ""), AuthBinding: accountScope("fixture-account"), RequestID: "req-inj", Model: "gpt-6-astra", conversion: conversion{Target: targetTimezone, Action: "unchanged"}})
	after := history.snapshot()["records"].([]auditRecord)[0]
	if after.TurnStateInjectedLength != len(value) {
		t.Fatalf("injected length lost on retry: %+v", after)
	}
}

func newPrefetchTestEngine(t *testing.T) *probeEngine {
	t.Helper()
	enabled := true
	e := &probeEngine{
		values: map[string]stateEntry{}, candidates: map[string]stateEntry{},
		failures: map[string]probeFailure{}, suspects: map[string]probeSuspicion{},
		business: map[string]businessDegradation{}, paused: map[string]bool{},
		probing: map[string]bool{}, prefetchGate: map[string]time.Time{}, rejectDegraded: true,
	}
	e.cfg.Config = parseProbeConfig(probeConfigYAML{Enabled: &enabled, Models: []string{"gpt-6-astra"}})
	e.cfg.Config.AccountMode = "" // Exercise the internal single-target engine; account binding has dedicated tests.
	e.cfg.Config.CredFile = filepath.Join(t.TempDir(), "missing-auth.json")
	e.cfg.Config.ProbeInterval = time.Millisecond
	e.cfg.Config.MaxAttemptsPerRound = 1
	previous := probeTrack
	probeTrack = e
	t.Setenv("LKS_TZ_STATE_FILE", filepath.Join(t.TempDir(), "state.json"))
	t.Cleanup(func() {
		e.stopPrefetchWatcher()
		e.stop()
		waitPrefetchIdle(t, e)
		probeTrack = previous
	})
	return e
}

func waitPrefetchIdle(t *testing.T, e *probeEngine) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		e.mu.Lock()
		busy := e.queueActive || len(e.probing) > 0
		e.mu.Unlock()
		if !busy {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("probe worker did not drain")
}

func TestPrefetchStopModes(t *testing.T) {
	e := newPrefetchTestEngine(t)
	cfg := e.cfg.Config
	e.storeValue("gpt-6-astra", synthStateToken(time.Now().Add(-cfg.TTL+2*time.Minute)), "seed", "", cfg)
	response, _ := probeControl([]byte(`{"action":"stop-current"}`))
	if response.StatusCode != 200 || e.halted {
		t.Fatal("stopping the current batch must preserve automatic prefetch")
	}
	e.prefetchScan()
	waitPrefetchIdle(t, e)
	if e.probesTotal != 1 {
		t.Fatal("the next scan must still probe the expiring baseline")
	}
	for _, action := range []string{"stop-all", "stop-round"} {
		body, _ := json.Marshal(map[string]string{"action": action})
		response, _ = probeControl(body)
		e.prefetchScan()
		if response.StatusCode != 200 || !e.halted || e.probesTotal != 1 || e.queueActive {
			t.Fatal("full stop and its legacy alias must suppress the watcher")
		}
	}
	e.stopCurrent()
	if !e.halted {
		t.Fatal("stop-current must not undo a prior full stop")
	}
}

func TestPriorityModelCutsAheadOfQueuedProbe(t *testing.T) {
	e := newPrefetchTestEngine(t)
	e.cfg.Config.Models = []string{"gpt-6-astra", "gpt-5.6-sol"}
	e.queueActive = true // keep this unit test from starting a worker
	defer func() { e.queueActive = false }()
	e.queue = []probeTask{{Model: "gpt-5.6-sol", Force: false}}
	if !e.enqueueTask("gpt-6-astra", false) {
		t.Fatal("priority model should be accepted into the waiting queue")
	}
	if len(e.queue) != 2 || e.queue[0].Model != "gpt-6-astra" || e.queue[1].Model != "gpt-5.6-sol" {
		t.Fatalf("priority model did not cut ahead: %+v", e.queue)
	}
	// Re-enqueueing the same model cannot duplicate pending work.
	e.queue = []probeTask{{Model: "gpt-6-astra", Force: false}}
	if e.enqueueTask("gpt-6-astra", false) {
		t.Fatal("duplicate model should not be queued twice")
	}
	if len(e.queue) != 1 {
		t.Fatal("the same model must not be queued twice")
	}
	for _, model := range []string{"gpt-5.6-sol-alt-1", "gpt-5.6-sol-alt-2"} {
		if !e.enqueueTask(model, true) {
			t.Fatal("unconfigured explicit model rejected")
		}
	}
	if e.queue[1].Model != "gpt-5.6-sol-alt-1" || e.queue[2].Model != "gpt-5.6-sol-alt-2" {
		t.Fatal("equal-priority models must retain FIFO order")
	}
	e.queueActive = false
}

func TestPrefetchStoppingDrainsRequest(t *testing.T) {
	e := newPrefetchTestEngine(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(entered) })
		<-release
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(upstream.Close)
	t.Cleanup(func() { close(release) })
	e.cfg.Config.UpstreamURL = upstream.URL
	e.cfg.Config.MaxAttemptsPerRound = 3
	if err := os.WriteFile(e.cfg.Config.CredFile, []byte(`{"access_token":"mock-only"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	e.probeModelAsync("gpt-6-astra")
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("mock request did not start")
	}
	e.stopCurrent()
	summary := probeSummary()
	if summary["stopping"] != true || summary["running"] != true || summary["prefetch_enabled"] != true {
		t.Fatal("an outstanding request must display stopping with prefetch still armed")
	}
	if e.enqueueTask("gpt-5.6-sol", true) || e.start() {
		t.Fatal("new requests must not overtake the draining worker")
	}
	// Release via the cleanup channel without closing it twice.
	release <- struct{}{}
	waitPrefetchIdle(t, e)
	if e.probesTotal != 1 || e.stopping || e.running || len(e.failures) != 0 {
		t.Fatal("stopping must drain the current request without retries or failure annotation")
	}
}

func TestPrefetchCancelledBeforeProbe(t *testing.T) {
	e := newPrefetchTestEngine(t)
	dequeuedBrake := e.abortSignal()
	e.stopCurrent()
	e.probeModel("gpt-6-astra", e.cfg.Config, dequeuedBrake)
	if e.probesTotal != 0 {
		t.Fatal("a stop after dequeue must still cancel before the first attempt")
	}
}

func TestPrefetchPauseRemovesQueuedModel(t *testing.T) {
	e := newPrefetchTestEngine(t)
	e.queue = []probeTask{{Model: "gpt-6-astra"}, {Model: "gpt-5.6-sol"}}
	e.setModelPaused("gpt-6-astra", true)
	if len(e.queue) != 1 || e.queue[0].Model != "gpt-5.6-sol" {
		t.Fatal("pausing must remove that model from the queue immediately")
	}
	if e.enqueueTask("gpt-6-astra", true) {
		t.Fatal("one-off refresh must respect explicit model pause")
	}
}

func TestPrefetchCapturePreservesNewerState(t *testing.T) {
	e := newPrefetchTestEngine(t)
	cfg, now := e.cfg.Config, time.Now()
	active := synthStateToken(now.Add(-10 * time.Minute))
	successor := synthStateToken(now.Add(-time.Minute))
	e.storeValue("gpt-6-astra", active, "seed", "", cfg)
	e.storeValue("gpt-6-astra", active, "business", "", cfg)
	if len(e.candidates) != 0 {
		t.Fatal("an echoed active must not masquerade as a ready successor")
	}
	e.candidates["gpt-6-astra"] = e.values["gpt-6-astra"]
	probeSummary()
	if len(e.candidates) != 0 {
		t.Fatal("an echoed successor restored from an older snapshot must be discarded")
	}
	e.storeValue("gpt-6-astra", successor, "probe", "direct", cfg)
	for _, value := range []string{active, synthStateToken(now.Add(-5 * time.Minute)), synthStateToken(now.Add(-2 * cfg.TTL))} {
		e.storeValue("gpt-6-astra", value, "business", "", cfg)
		if e.values["gpt-6-astra"].Value != active || e.candidates["gpt-6-astra"].Value != successor {
			t.Fatal("repeated, older and expired captures must preserve both healthy slots")
		}
	}
}

func TestPrefetchRejectsExpiredCapture(t *testing.T) {
	e := newPrefetchTestEngine(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(turnStateHeader, synthStateToken(time.Now().Add(-2*e.cfg.Config.TTL)))
		_, _ = w.Write([]byte("data: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n"))
	}))
	defer upstream.Close()
	e.cfg.Config.UpstreamURL = upstream.URL
	if err := os.WriteFile(e.cfg.Config.CredFile, []byte(`{"access_token":"mock-only"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	record, value := e.probeOnce("gpt-6-astra", "direct", e.cfg.Config)
	if value != "" || record.Success || !strings.Contains(record.Error, "expired") {
		t.Fatalf("expired capture check: success=%v returned=%v error=%q", record.Success, value != "", record.Error)
	}
}

func TestPrefetchCandidateSuppressesQueueAndPromotes(t *testing.T) {
	e := newPrefetchTestEngine(t)
	cfg := e.cfg.Config
	active := synthStateToken(time.Now().Add(-cfg.TTL + 2*time.Minute))
	successor := synthStateToken(time.Now())
	e.storeValue("gpt-6-astra", active, "seed", "", cfg)
	e.storeValue("gpt-6-astra", successor, "probe", "direct", cfg)
	if !e.start() || e.queueActive || e.probesTotal != 0 {
		t.Fatal("a ready successor must suppress manual round duplication")
	}
	e.queue = []probeTask{{Model: "gpt-6-astra"}}
	e.queueLoop()
	if e.probesTotal != 0 {
		t.Fatal("a successor acquired while queued must suppress the stale task at dequeue")
	}
	e.values["gpt-6-astra"] = stateEntry{Model: "gpt-6-astra", Value: synthStateToken(time.Now().Add(-2 * cfg.TTL)), Valid: true}
	e.stop()
	probeSummary()
	if e.values["gpt-6-astra"].Value != successor || len(e.candidates) != 0 || !e.halted {
		t.Fatal("summary must reflect due hand-off even while probing is halted")
	}
}

func TestPrefetchRestartKeepsBaselineAndMode(t *testing.T) {
	for _, halted := range []bool{false, true} {
		t.Run(map[bool]string{false: "armed", true: "halted"}[halted], func(t *testing.T) {
			e := newPrefetchTestEngine(t)
			cfg := e.cfg.Config
			active := synthStateToken(time.Now().Add(-10 * time.Minute))
			successor := synthStateToken(time.Now())
			e.storeValue("gpt-6-astra", active, "seed", "", cfg)
			e.storeValue("gpt-6-astra", successor, "probe", "direct", cfg)
			e.halted = halted
			e.shutdown()
			if e.enqueueTask("gpt-6-astra", true) || e.start() {
				t.Fatal("shutdown must block new work without persisting an operator halt")
			}
			savePersistedState()
			e.values, e.candidates = map[string]stateEntry{}, map[string]stateEntry{}
			e.shuttingDown = false // the next process starts with a fresh lifecycle
			e.halted = !halted
			loadPersistedState()
			e.prefetchScan()
			if e.halted != halted || e.activeValueFor("gpt-6-astra") != active || e.candidates["gpt-6-astra"].Value != successor || e.queueActive {
				t.Fatal("restart must restore both slots and mode without probing a healthy baseline")
			}
			applyPersistedState(persistedState{})
			if e.halted != halted {
				t.Fatal("legacy snapshots without halted must not overwrite an explicit mode")
			}
		})
	}
}

func TestPrefetchFailureDoesNotRejectHealthyBusiness(t *testing.T) {
	installFixtureHostAuth(t)
	e := newPrefetchTestEngine(t)
	cfg := e.cfg.Config
	turnStateOverride.Store(&turnStateOverrideState{Config: turnStateOverrideConfig{Enabled: true, Force: true, Models: cfg.Models}})
	t.Cleanup(func() { turnStateOverride = atomic.Value{} })
	active := synthStateToken(time.Now().Add(-cfg.TTL + 3*time.Minute))
	successor := synthStateToken(time.Now())
	e.storeValue(scopedTarget(credentialScope("fixture-account", "synthetic-fixture-token", ""), "gpt-6-astra"), active, "seed", "", cfg)
	assertBusiness := func(want string, blocked bool) {
		t.Helper()
		raw, _ := json.Marshal(interceptRequest{Metadata: map[string]any{"selected_auth_id": "fixture-account", "selected_auth_index": "fixture-index"}, Headers: http.Header{"Authorization": {"Bearer synthetic-fixture-token"}, turnStateHeader: {"old"}}, RequestID: "prefetch-business", ToFormat: "codex", Model: "gpt-6-astra", Body: []byte(`{"input":"hello"}`)})
		response, err := intercept(raw)
		if err != nil || response.Terminate != blocked {
			t.Fatalf("business rejection mismatch: blocked=%v response=%+v err=%v", blocked, response, err)
		}
		if !blocked && response.Headers.Get(turnStateHeader) != want {
			t.Fatal("business must retain the expected healthy state")
		}
	}
	for i := 0; i < cfg.SuspectThreshold; i++ {
		e.noteProbeFailure(scopedTarget(credentialScope("fixture-account", "synthetic-fixture-token", ""), "gpt-6-astra"), probeRecord{Error: "model mismatch: requested gpt-6-astra got other"}, cfg)
		assertBusiness(active, false)
	}
	e.failures[scopedTarget(credentialScope("fixture-account", "synthetic-fixture-token", ""), "gpt-6-astra")] = probeFailure{LastError: "model mismatch: requested gpt-6-astra got other"}
	assertBusiness(active, false)
	e.storeValue(scopedTarget(credentialScope("fixture-account", "synthetic-fixture-token", ""), "gpt-6-astra"), successor, "probe", "direct", cfg)
	assertBusiness(active, false)
	// Keep the old probe annotations to ensure rejection checks promote before
	// deciding, rather than relying on a probe-success side effect.
	e.values[scopedTarget(credentialScope("fixture-account", "synthetic-fixture-token", ""), "gpt-6-astra")] = stateEntry{Model: "gpt-6-astra", Value: synthStateToken(time.Now().Add(-2 * cfg.TTL)), Valid: true}
	assertBusiness(successor, false)
	if len(e.candidates) != 0 {
		t.Fatal("business must atomically consume the successor")
	}
	if reason := degradedRejectMessage("gpt-6-astra-preview", "gpt-6-astra"); reason != "" {
		t.Fatal("the requested-model baseline must also protect a routed alias")
	}
	e.values[scopedTarget(credentialScope("fixture-account", "synthetic-fixture-token", ""), "gpt-6-astra")] = stateEntry{Value: synthStateToken(time.Now().Add(-2 * cfg.TTL)), Valid: true}
	assertBusiness("", true)
	e.storeValue(scopedTarget(credentialScope("fixture-account", "synthetic-fixture-token", ""), "gpt-6-astra"), successor, "probe", "direct", cfg)
	e.noteBusinessDegradation(scopedTarget(credentialScope("fixture-account", "synthetic-fixture-token", ""), "gpt-6-astra"), "业务模型不一致")
	assertBusiness("", true)
}

func TestUncheckedModelPolicy(t *testing.T) {
	for _, tc := range []struct {
		model, requested string
		enabled          bool
	}{
		{"gpt-5.6-luna", "", false}, {"gpt-5.6-terra", "", false},
		{" GPT-6-LUNA-preview ", "", false}, {"gpt-6-terra-20260919", "", false},
		{"luna", "", false}, {"terra", "", false},
		{"gpt-6-astra", "gpt-5.6-luna", false},
		{"gpt-5.6-luna", "gpt-6-astra", true},
		{"gpt-5.6-sol", "", true}, {"gpt-6-astra-luna", "", true},
		{"gpt-5.6-lunafoo", "", true}, {"other-luna", "", true},
	} {
		if got := degradationDetectionEnabled(tc.model, tc.requested); got != tc.enabled {
			t.Errorf("policy(%q, %q)=%v; want %v", tc.model, tc.requested, got, tc.enabled)
		}
	}
}

func TestUncheckedRequestsIgnoreOldStateAndErrors(t *testing.T) {
	e := newPrefetchTestEngine(t)
	turnStateOverride.Store(&turnStateOverrideState{Config: turnStateOverrideConfig{
		Enabled: true, Force: true, Models: []string{"gpt-5.6-luna", "gpt-5.6-terra"}, Value: "STATIC-FALLBACK",
	}})
	t.Cleanup(func() { turnStateOverride = atomic.Value{} })
	for _, model := range []string{"gpt-5.6-luna", "gpt-5.6-terra"} {
		e.values[model] = stateEntry{Model: model, Value: synthStateToken(time.Now()), Valid: true}
		e.failures[model] = probeFailure{Model: model, LastError: "model mismatch"}
		e.suspects[model] = probeSuspicion{Model: model, Failures: 100, LastError: "state length 312 != 332"}
		e.business[model] = businessDegradation{Model: model, Reason: "历史模型不一致"}
		for _, length := range []int{0, 1, 332, 312} {
			headers := http.Header{turnStateHeader: {strings.Repeat("x", length)}}
			raw, _ := json.Marshal(interceptRequest{RequestID: "unchecked-" + model,
				ToFormat: "codex", Model: model, Headers: headers, Body: []byte(`{"input":"hello"}`)})
			response, err := intercept(raw)
			if err != nil || response.Terminate || response.Headers.Get(turnStateHeader) != "" {
				t.Fatalf("unchecked request must pass without cached/static state injection: model=%s length=%d err=%v", model, length, err)
			}
			if headers.Get(turnStateHeader) != strings.Repeat("x", length) {
				t.Fatal("the client's state must remain untouched")
			}
		}
		if e.degradedRejectReason(model) != "" {
			t.Fatal("restored failure, suspicion and business marks must not reject an exempt model")
		}
	}
}

func TestUncheckedResponseHooks(t *testing.T) {
	e := newPrefetchTestEngine(t)
	history = auditState{}
	for _, model := range []string{"gpt-5.6-luna", "gpt-5.6-terra"} {
		for _, state := range []string{"", "short", strings.Repeat("x", 332), strings.Repeat("x", 312)} {
			id := "unchecked-response-" + model
			history.record(auditRecord{RequestID: id, Model: model})
			body := []byte(`{"type":"response.created","response":{"model":"gpt-6-astra"}}`)
			nonStream, _ := json.Marshal(responseInterceptRequest{RequestID: id, Model: model,
				Body: body, ResponseHeaders: http.Header{turnStateHeader: {state}}})
			out, err := interceptNonStreamingResponse(nonStream)
			if err != nil || out.Headers != nil || out.Body != nil {
				t.Fatal("unchecked non-streaming responses must pass unchanged")
			}
			chunk, _ := json.Marshal(streamChunkInterceptRequest{RequestID: id, Model: model, ChunkIndex: 0,
				Body: body, ResponseHeaders: http.Header{turnStateHeader: {state}}})
			out, err = interceptStreamChunk(chunk)
			if err != nil || out.Headers != nil || out.Body != nil {
				t.Fatal("unchecked streaming responses must pass unchanged")
			}
			event, _ := json.Marshal(webSocketResponseEvent{RequestID: id, Model: model, Payload: body})
			if _, err := observeWebSocketEvent(event); err != nil {
				t.Fatal(err)
			}
			record := history.snapshot()["records"].([]auditRecord)[0]
			if !record.DetectionExempt || record.ModelChecked || record.ModelMismatch || record.UpstreamModel != "gpt-6-astra" {
				t.Fatal("observations must stay available without evaluating model consistency")
			}
			if out := repairTurnStateHeader(model, "", "gpt-6-astra", state); out != nil {
				t.Fatal("unchecked responses must not be backfilled")
			}
		}
	}
	if len(e.business) != 0 || len(e.suspects) != 0 || len(e.values) != 0 {
		t.Fatal("unchecked observations must not create degradation marks or healthy baselines")
	}
}

func TestUncheckedProbeEntrypointsAndSummary(t *testing.T) {
	e := newPrefetchTestEngine(t)
	e.cfg.Config.Models = []string{"gpt-5.6-luna", "gpt-5.6-terra"}
	for _, model := range e.cfg.Config.Models {
		for _, action := range []string{"pause", "resume", "probe-model"} {
			body, _ := json.Marshal(map[string]string{"model": model, "action": action})
			response, _ := probeControl(body)
			var result map[string]any
			if err := json.Unmarshal(response.Body, &result); err != nil || response.StatusCode != 200 || result["skipped"] != true {
				t.Fatal("unchecked model controls must explicitly acknowledge a no-op")
			}
		}
		e.values[model] = stateEntry{Model: model, Value: synthStateToken(time.Now().Add(-2 * e.cfg.Config.TTL)), Valid: true}
		e.failures[model] = probeFailure{Model: model, LastError: "model mismatch"}
		e.suspects[model] = probeSuspicion{Model: model, Failures: 100, LastError: "state length 312 != 332"}
		e.business[model] = businessDegradation{Model: model, Reason: "旧标记"}
		if e.probeModelAsync(model) || e.enqueueTask(model, false) {
			t.Fatal("unchecked models must not enter the probe queue")
		}
		e.probeModel(model, e.cfg.Config, make(chan struct{}))
		e.noteProbeFailure(model, probeRecord{Error: "model mismatch"}, e.cfg.Config)
		if e.suspects[model].Failures != 100 {
			t.Fatal("unchecked models must not accumulate suspicion")
		}
	}
	e.start()
	e.prefetchScan()
	if e.queueActive || len(e.queue) != 0 || e.probesTotal != 0 || len(e.prefetchGate) != 0 {
		t.Fatal("manual rounds and automatic prefetch must skip unchecked models")
	}
	summary := probeSummary()
	if summary["prefetch_enabled"] != false || len(summary["detection_models"].([]string)) != 0 {
		t.Fatal("all-unchecked configuration must not advertise prefetch")
	}
	for _, value := range summary["values"].([]map[string]any) {
		if value["detection_enabled"] != false || value["value_length"] != nil {
			t.Fatal("unchecked rows must expose their policy instead of stale baseline status")
		}
	}
	if len(summary["failures"].([]probeFailure)) != 0 || len(summary["suspects"].([]probeSuspicion)) != 0 || len(summary["business"].([]businessDegradation)) != 0 {
		t.Fatal("historical marks must not appear as current unchecked-model status")
	}
}

func TestUncheckedRoutingKeepsAstraDetection(t *testing.T) {
	e := newPrefetchTestEngine(t)
	observeBusinessStateForRequest("gpt-6-astra", "", "", "gpt-5.6-luna")
	if degradedRejectMessage("gpt-6-astra") == "" {
		t.Fatal("astra routed to luna must still be detected")
	}
	if degradedRejectMessage("gpt-6-astra", "gpt-5.6-luna") != "" {
		t.Fatal("an explicit user request for luna must remain exempt after routing")
	}
	delete(e.business, "gpt-6-astra")
	observeBusinessStateForRequest("gpt-5.6-luna", "gpt-6-astra", "", "gpt-5.6-luna")
	if degradedRejectMessage("gpt-5.6-luna", "gpt-6-astra") == "" {
		t.Fatal("routing a checked request to an unchecked model must not exempt the original request")
	}
	delete(e.business, "gpt-6-astra")
	observeBusinessStateForRequest("gpt-6-astra", "gpt-5.6-terra", "short", "gpt-5.6-sol")
	if len(e.business) != 0 {
		t.Fatal("explicit terra requests must not create marks on a routed model")
	}
}

func TestUncheckedLegacyAuditSnapshot(t *testing.T) {
	var audit auditState
	audit.records = []auditRecord{{Model: "gpt-5.6-luna", UpstreamModel: "gpt-6-astra", ModelChecked: true, ModelMismatch: true}}
	snapshot := audit.snapshot()
	record := snapshot["records"].([]auditRecord)[0]
	if !record.DetectionExempt || record.ModelChecked || record.ModelMismatch || snapshot["mismatches"].(int) != 0 {
		t.Fatal("restored audit records must use the current unchecked policy")
	}
	if !audit.records[0].ModelMismatch || record.UpstreamModel != "gpt-6-astra" {
		t.Fatal("historical observations must not be destroyed")
	}
}

func TestRuntimePrefetchPersistsWithoutStarting(t *testing.T) {
	e := newPrefetchTestEngine(t)
	e.halted = true
	e.paused["gpt-6-astra"] = true
	token := synthStateToken(time.Now().Add(-time.Minute))
	e.storeValue("gpt-6-astra", token, "business", "", e.cfg.Config)
	response, _ := probeControl([]byte(`{"action":"prefetch-window","minutes":10}`))
	if response.StatusCode != 200 || e.cfg.Config.Prefetch != 10*time.Minute || !e.halted || !e.paused["gpt-6-astra"] || e.queueActive {
		t.Fatal("saving prefetch must preserve modes and remain idle")
	}
	if e.values["gpt-6-astra"].Value != token {
		t.Fatal("saving prefetch changed the active token")
	}
	base := e.baseCfg.Config
	e.settings = runtimeSettings{}
	loadRuntimeSettings()
	restored, err := applyRuntimeSettings(base, e.settings)
	if err != nil || restored.Prefetch != 10*time.Minute {
		t.Fatal("confirmed prefetch did not survive a reload")
	}
	base.TTL = 5 * time.Minute
	restored, err = applyRuntimeSettings(base, e.settings)
	if err != nil || restored.Prefetch != 4*time.Minute {
		t.Fatal("a reduced TTL must bound the saved window")
	}
	for _, raw := range []string{`{"action":"prefetch-window"}`, `{"action":"prefetch-window","minutes":-1}`, `{"action":"prefetch-window","minutes":55}`, `{"action":"prefetch-window","minutes":1.5}`, `{"action":"prefetch-window","minutes":"10"}`} {
		response, _ = probeControl([]byte(raw))
		if response.StatusCode != 400 || e.cfg.Config.Prefetch != 10*time.Minute {
			t.Fatal("invalid prefetch changed the confirmed setting")
		}
	}
	if err := e.setPrefetchMinutes(0); err != nil || e.cfg.Config.Prefetch != 0 || !e.halted {
		t.Fatal("zero must disable prefetch without changing the stop mode")
	}
}

func TestRuntimePrefetchNewWindowTriggersScan(t *testing.T) {
	e := newPrefetchTestEngine(t)
	cfg := e.cfg.Config
	e.storeValue("gpt-6-astra", synthStateToken(time.Now().Add(-cfg.TTL+6*time.Minute)), "seed", "", cfg)
	e.prefetchScan()
	if e.queueActive {
		t.Fatal("six minutes must be outside the old three-minute window")
	}
	if err := e.setPrefetchMinutes(10); err != nil {
		t.Fatal(err)
	}
	e.prefetchScan()
	waitPrefetchIdle(t, e)
	if e.probesTotal != 1 {
		t.Fatal("the next scan did not use the new window")
	}
}

func TestProbeHistoryExitLabelsBeforeRedaction(t *testing.T) {
	e := newPrefetchTestEngine(t)
	first := "socks5://first:test-secret-a@192.0.2.1:1080"
	second := "socks5://second:test-secret-b@192.0.2.1:1080"
	e.cfg.Config.Proxies = []string{first, second, "direct"}
	e.cfg.Config.ProxyLabels = map[string]string{first: "Named A", second: "Named B", "direct": "Local exit"}
	e.history = []probeRecord{{Proxy: first}, {Proxy: second}, {Proxy: "direct"}, {Proxy: "http://192.0.2.2:8080"}}
	history := probeSummary()["history"].([]probeRecord)
	for i, want := range []string{"", "Local exit", "Named B", "Named A"} {
		if history[i].ProxyLabel != want {
			t.Fatalf("history[%d] label = %q, want %q", i, history[i].ProxyLabel, want)
		}
	}
	if history[2].Proxy != history[3].Proxy {
		t.Fatal("fixture must have identical public URLs but distinct labels")
	}
	e.cfg.Config.ProxyLabels[first] = "Renamed A"
	if got := probeSummary()["history"].([]probeRecord)[3].ProxyLabel; got != "Renamed A" {
		t.Fatalf("renamed label = %q", got)
	}
	delete(e.cfg.Config.ProxyLabels, second)
	if got := probeSummary()["history"].([]probeRecord)[2].ProxyLabel; got != "" {
		t.Fatal("removed names must fall back to the public URL")
	}
	encoded, err := json.Marshal(history)
	if err != nil || bytes.Contains(encoded, []byte("test-secret")) {
		t.Fatal("history must serialize without proxy authentication")
	}
	if e.history[0].ProxyLabel != "" || e.history[0].Proxy != first {
		t.Fatal("summary must not mutate stored probe records")
	}
}

func TestRuntimeExitEditingAndRedaction(t *testing.T) {
	e := newPrefetchTestEngine(t)
	old := "socks5://proxy-user:proxy-password-canary@192.0.2.1:1080"
	e.cfg.Config.Proxies = []string{"direct", old}
	e.cfg.Config.MaxAttemptsPerRound = 0
	e.halted = true
	e.disabledExits = map[string]bool{old: true}
	e.exitPenalties = map[string]exitPenalty{old: {Proxy: old, Failures: 2, Until: time.Now().Add(time.Hour).Format(time.RFC3339Nano)}}
	id := defaultExitID(old)
	if err := e.saveExit(exitEdit{ID: id, URL: "socks5://192.0.2.2:1080", Label: "备用出口", Attempts: 10, Multiplier: 0.5}); err != nil {
		t.Fatal(err)
	}
	updated := e.resolveExit(id, "")
	if !strings.Contains(updated, "proxy-password-canary") || !e.disabledExits[updated] || e.exitPenalties[updated].Failures != 2 {
		t.Fatal("editing must retain authentication, disablement and penalties")
	}
	if err := e.saveExit(exitEdit{URL: "http://192.0.2.3:8080", Label: "聚合池", Pool: true, Attempts: 10, Multiplier: 2.5}); err != nil {
		t.Fatal(err)
	}
	if effectiveMaxAttempts(e.cfg.Config, e.cfg.Config.Proxies) != 33 || !e.halted || e.running {
		t.Fatal("budgets must multiply without re-arming the engine")
	}
	capped := e.cfg.Config
	capped.MaxAttemptsPerRound = 7
	if effectiveMaxAttempts(capped, capped.Proxies) != 7 {
		t.Fatal("explicit round limit must retain precedence")
	}
	e.lastActivity = "gpt-6-astra via " + old
	e.lastError = "connection failed: " + old
	e.history = []probeRecord{{Proxy: old, Error: old}}
	e.failures["gpt-6-astra"] = probeFailure{Model: "gpt-6-astra", LastError: old}
	encoded, _ := json.Marshal(probeSummary())
	if bytes.Contains(encoded, []byte("proxy-password-canary")) || bytes.Contains(encoded, []byte("proxy-user")) {
		t.Fatal("management summary leaked proxy authentication")
	}
	response, _ := probeControl([]byte(`{"action":"exit-enabled","id":"` + id + `","enabled":true}`))
	if response.StatusCode != 200 || e.disabledExits[updated] || !e.halted {
		t.Fatal("opaque ID control failed or changed the global stop mode")
	}
	if err := e.saveExit(exitEdit{ID: id, URL: publicProxyURL(updated), ClearAuth: true, Multiplier: 1}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(e.resolveExit(id, ""), "@") {
		t.Fatal("explicit clear-auth did not remove credentials")
	}
	base := e.baseCfg.Config
	e.settings = runtimeSettings{}
	loadRuntimeSettings()
	restored, err := applyRuntimeSettings(base, e.settings)
	if err != nil || len(restored.Proxies) != 3 || exitID(restored, restored.Proxies[1]) != id {
		t.Fatal("managed exits did not survive reload with stable IDs")
	}
}

func TestRuntimeExitValidationAndAtomicSave(t *testing.T) {
	e := newPrefetchTestEngine(t)
	for _, edit := range []exitEdit{
		{URL: "ftp://192.0.2.1:1080", Multiplier: 1},
		{URL: "socks5://user:secret@192.0.2.1:99999", Multiplier: 1},
		{URL: "http://192.0.2.1:8080/?secret=canary", Multiplier: 1},
		{URL: "http://192.0.2.1:8080", Multiplier: 0},
		{URL: "http://192.0.2.1:8080", Multiplier: 101},
		{URL: "http://192.0.2.1:8080", Multiplier: 100, Attempts: maxExitAttempts},
		{URL: "http://192.0.2.1:8080", Multiplier: 1, Attempts: -1},
		{URL: "direct", Pool: true, Multiplier: 1},
		{URL: "direct", Multiplier: 1}, // duplicate
		{ID: "missing", URL: "http://192.0.2.1:8080", Multiplier: 1},
	} {
		if err := e.saveExit(edit); err == nil {
			t.Fatal("invalid proxy edit unexpectedly succeeded")
		}
		if len(e.cfg.Config.Proxies) != 1 || len(e.settings.Exits) != 0 {
			t.Fatal("rejected edit modified the pool")
		}
	}
	blocker := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(blocker, []byte("test"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LKS_TZ_STATE_FILE", filepath.Join(blocker, "state.json"))
	if err := e.setPrefetchMinutes(10); err == nil || e.cfg.Config.Prefetch != 3*time.Minute {
		t.Fatal("failed disk write must not apply an in-memory prefetch change")
	}
	if err := e.saveExit(exitEdit{URL: "http://192.0.2.1:8080", Multiplier: 1}); err == nil || len(e.cfg.Config.Proxies) != 1 {
		t.Fatal("failed disk write must not publish an exit")
	}
}

func TestRuntimePoolEditPreservesCountersAndDisabledExit(t *testing.T) {
	e := newPrefetchTestEngine(t)
	spec := "http://192.0.2.1:8080"
	e.cfg.Config.Proxies = []string{spec}
	e.cfg.Config.ProxyPools[spec] = true
	e.exitPenalties = map[string]exitPenalty{spec: {Proxy: spec, Failures: 12}}
	e.disabledExits = map[string]bool{spec: true}
	if err := e.setPrefetchMinutes(10); err != nil || e.exitPenalties[spec].Failures != 12 {
		t.Fatal("saving a window must not clear pool counters")
	}
	e.exitPenalties[spec] = exitPenalty{Proxy: spec, Failures: 12, Until: time.Now().Add(time.Hour).Format(time.RFC3339Nano)}
	if got := e.availableProxies([]string{spec}, time.Now()); len(got) != 0 {
		t.Fatal("emergency release must never resurrect a disabled exit")
	}
}

func TestRuntimeExitHotReloadDuringAttempt(t *testing.T) {
	for _, add := range []bool{false, true} {
		name := "edit"
		if add {
			name = "add"
		}
		t.Run(name, func(t *testing.T) {
			e := newPrefetchTestEngine(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var enterOnce, releaseOnce sync.Once
			var oldCalls, newCalls atomic.Int32
			oldProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				oldCalls.Add(1)
				enterOnce.Do(func() { close(entered) })
				<-release
				w.WriteHeader(http.StatusTooManyRequests)
			}))
			newProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				newCalls.Add(1)
				w.WriteHeader(http.StatusTooManyRequests)
			}))
			t.Cleanup(oldProxy.Close)
			t.Cleanup(newProxy.Close)
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			e.cfg.Config.Proxies = []string{oldProxy.URL}
			e.cfg.Config.AttemptsPerHop = 1
			e.cfg.Config.MaxAttemptsPerRound = 0
			e.cfg.Config.UpstreamURL = "http://mock-upstream.invalid/responses"
			if err := os.WriteFile(e.cfg.Config.CredFile, []byte(`{"access_token":"mock-only"}`), 0o600); err != nil {
				t.Fatal(err)
			}
			e.probeModelAsync("gpt-6-astra")
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("mock proxy did not receive the first attempt")
			}
			edit := exitEdit{URL: newProxy.URL, Attempts: 2, Multiplier: 1}
			if !add {
				edit.ID = defaultExitID(oldProxy.URL)
			}
			if err := e.saveExit(edit); err != nil {
				t.Fatal(err)
			}
			releaseOnce.Do(func() { close(release) })
			waitPrefetchIdle(t, e)
			expectedNew := int32(1) // one of the edited exit's two attempts was spent
			if add {
				expectedNew = 2
			}
			if oldCalls.Load() != 1 || newCalls.Load() != expectedNew {
				t.Fatalf("hot reload repeated old requests or reset the budget: old=%d new=%d", oldCalls.Load(), newCalls.Load())
			}
		})
	}
}

func TestRuntimeCorruptSettingsAreNotOverwritten(t *testing.T) {
	e := newPrefetchTestEngine(t)
	raw := []byte(`{"version":999,"secret":"private-canary"}`)
	if err := os.WriteFile(runtimeSettingsPath(), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	loadRuntimeSettings()
	if e.settingsError == "" || e.setPrefetchMinutes(10) == nil {
		t.Fatal("invalid settings must be reported and preserved")
	}
	after, err := os.ReadFile(runtimeSettingsPath())
	if err != nil || !bytes.Equal(raw, after) || strings.Contains(e.settingsError, "private-canary") {
		t.Fatal("corrupt settings were overwritten or exposed")
	}
}

func TestRuntimeRestartAndReconfigureKeepTickets(t *testing.T) {
	for _, halted := range []bool{false, true} {
		name := "prefetch-enabled"
		if halted {
			name = "halted"
		}
		t.Run(name, func(t *testing.T) {
			e := newPrefetchTestEngine(t)
			e.halted = halted
			e.paused["gpt-5.6-sol"] = true
			e.disabledExits = map[string]bool{"direct": true}
			active := synthStateToken(time.Now().Add(-20 * time.Minute))
			candidate := synthStateToken(time.Now().Add(-8 * time.Minute))
			e.storeValue("gpt-6-astra", active, "seed", "", e.cfg.Config)
			e.storeValue("gpt-6-astra", candidate, "probe", "direct", e.cfg.Config)
			if err := e.setPrefetchMinutes(10); err != nil {
				t.Fatal(err)
			}
			if err := e.saveExit(exitEdit{ID: defaultExitID("direct"), URL: "direct", Label: "直连", Multiplier: 2}); err != nil {
				t.Fatal(err)
			}
			e.shutdown()
			savePersistedState()
			restarted := &probeEngine{}
			probeTrack = restarted
			t.Cleanup(func() { restarted.stopPrefetchWatcher(); probeTrack = e })
			loadPersistedState()
			loadRuntimeSettings()
			enabled, configuredWindow := true, 3
			if err := configureProbeTrack(probeConfigYAML{Enabled: &enabled, PrefetchMinutes: &configuredWindow}); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if restarted.values["gpt-6-astra"].Value != active || restarted.candidates["gpt-6-astra"].Value != candidate ||
					restarted.halted != halted || !restarted.paused["gpt-5.6-sol"] || !restarted.disabledExits["direct"] {
					t.Fatal("restart or reconfigure lost tickets or user mode")
				}
				if restarted.cfg.Config.Prefetch != 10*time.Minute || exitBudget(restarted.cfg.Config, "direct") != 6 {
					t.Fatal("restart or reconfigure lost confirmed runtime settings")
				}
				restarted.prefetchScan()
				if restarted.probesTotal != 0 || restarted.queueActive {
					t.Fatal("restored healthy tickets must not trigger startup probing")
				}
				// A subsequent in-process configuration reload has the same rule.
				if err := configureProbeTrack(probeConfigYAML{Enabled: &enabled, PrefetchMinutes: &configuredWindow}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRuntimeIntervalPersistsWithoutStarting(t *testing.T) {
	e := newPrefetchTestEngine(t)
	e.halted = true
	e.paused["gpt-6-astra"] = true
	if err := e.setProbeIntervalSeconds(25); err != nil || e.cfg.Config.ProbeInterval != 25*time.Second {
		t.Fatal("saving the interval must apply the new value")
	}
	if !e.halted || !e.paused["gpt-6-astra"] || e.queueActive || e.probesTotal != 0 {
		t.Fatal("saving the interval must preserve modes and remain idle")
	}
	base := e.baseCfg.Config
	e.settings = runtimeSettings{}
	loadRuntimeSettings()
	restored, err := applyRuntimeSettings(base, e.settings)
	if err != nil || restored.ProbeInterval != 25*time.Second || e.settings.IntervalSeconds == nil || *e.settings.IntervalSeconds != 25 {
		t.Fatal("confirmed interval did not survive a reload")
	}
	for _, raw := range []string{
		`{"action":"probe-interval"}`,
		`{"action":"probe-interval","seconds":0}`,
		`{"action":"probe-interval","seconds":3601}`,
		`{"action":"probe-interval","seconds":1.5}`,
		`{"action":"probe-interval","seconds":"25"}`,
	} {
		response, _ := probeControl([]byte(raw))
		if response.StatusCode != 400 || e.cfg.Config.ProbeInterval != 25*time.Second {
			t.Fatalf("invalid interval %s changed the confirmed setting", raw)
		}
	}
	for _, seconds := range []int{1, 3600} {
		response, _ := probeControl([]byte(`{"action":"probe-interval","seconds":` + strconv.Itoa(seconds) + `}`))
		if response.StatusCode != 200 || e.cfg.Config.ProbeInterval != time.Duration(seconds)*time.Second {
			t.Fatalf("boundary interval %d must be accepted", seconds)
		}
	}
	if !e.halted || e.queueActive || e.probesTotal != 0 {
		t.Fatal("boundary saves must not re-arm the stopped engine")
	}
}

func TestRuntimeIntervalAppliesToNextWait(t *testing.T) {
	e := newPrefetchTestEngine(t)
	base := e.cfg
	e.baseCfg = &base
	e.cfg.Config.ProbeInterval = 3 * time.Second
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		e.waitProbeInterval(3*time.Second, nil, nil)
		done <- time.Since(start)
	}()
	time.Sleep(150 * time.Millisecond)
	if err := e.setProbeIntervalSeconds(1); err != nil {
		t.Fatal(err)
	}
	elapsed := <-done
	if elapsed < 2800*time.Millisecond || elapsed > 6*time.Second {
		t.Fatalf("an in-flight wait must keep its original duration, got %v", elapsed)
	}
	go func() {
		start := time.Now()
		// A stale argument must not win: the snapshot takes the saved value.
		e.waitProbeInterval(3*time.Second, nil, nil)
		done <- time.Since(start)
	}()
	elapsed = <-done
	if elapsed < 800*time.Millisecond || elapsed > 2400*time.Millisecond {
		t.Fatalf("the next wait must use the saved interval, got %v", elapsed)
	}
	stopped := make(chan struct{})
	close(stopped)
	start := time.Now()
	if e.waitProbeInterval(time.Second, stopped, nil) {
		t.Fatal("a closed stop channel must report cancellation")
	}
	if time.Since(start) > 900*time.Millisecond {
		t.Fatal("a closed stop channel must end the wait immediately")
	}
}

func installFixtureHostAuth(t *testing.T) {
	t.Helper()
	list, get := hostAuthListFunc, hostAuthGetFunc
	t.Cleanup(func() { hostAuthListFunc, hostAuthGetFunc = list, get })
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{{ID: "fixture-account", AuthIndex: "fixture-index", Provider: "codex"}}, nil
	}
	hostAuthGetFunc = func(index string) (json.RawMessage, error) {
		return json.Marshal(map[string]string{"access_token": "synthetic-fixture-token"})
	}
}
