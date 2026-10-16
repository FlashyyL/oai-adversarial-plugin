package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type evalMockCall struct {
	Auth  string
	Model string
	Body  map[string]any
}
type evalMockHost struct {
	mu           sync.Mutex
	Entries      []hostAuthEntry
	Tokens       map[string]string
	Calls        []evalMockCall
	Streams      map[string][]byte
	Active, Peak int
	Answer       func(evalMockCall) (string, error)
}

func evalMockSSE(answer, model string) []byte {
	raw, _ := json.Marshal(map[string]any{"type": "response.completed", "response": map[string]any{
		"status": "completed", "model": model, "output": []any{map[string]any{"type": "message", "content": []any{map[string]any{"type": "output_text", "text": answer}}}},
		"usage": map[string]any{"input_tokens": 12, "output_tokens": 5, "output_tokens_details": map[string]any{"reasoning_tokens": 2}},
	}})
	return []byte("event: response.completed\ndata: " + string(raw) + "\n\n")
}

func newEvalFixture(t *testing.T) (*probeEngine, *evalMockHost) {
	t.Helper()
	e := newPrefetchTestEngine(t)
	history.mu.Lock()
	previousRecords, previousTotal, previousInserted, previousReplaced := history.records, history.total, history.inserted, history.replaced
	history.records, history.total, history.inserted, history.replaced = nil, 0, 0, 0
	history.mu.Unlock()
	f := &evalMockHost{Entries: []hostAuthEntry{{ID: "native-a", AuthIndex: "a", Provider: "codex", Email: "a@example.invalid"}, {ID: "native-b", AuthIndex: "b", Provider: "codex", Email: "b@example.invalid"}}, Tokens: map[string]string{"a": "PRIVATE_A_CANARY", "b": "PRIVATE_B_CANARY"}, Streams: map[string][]byte{}}
	f.Answer = func(evalMockCall) (string, error) {
		return "最少取出 21 个糖果，9 颗圆形和 12 颗星形。", nil
	}
	previousList, previousGet, previousCall := hostAuthListFunc, hostAuthGetFunc, evalHostCall
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		return append([]hostAuthEntry(nil), f.Entries...), nil
	}
	hostAuthGetFunc = func(index string) (json.RawMessage, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		return json.Marshal(map[string]any{"access_token": f.Tokens[index], "account_id": "synthetic-workspace"})
	}
	evalHostCall = func(method string, payload any) (json.RawMessage, error) {
		switch method {
		case "host.model.execute_stream":
			p := payload.(map[string]any)
			var body map[string]any
			_ = json.Unmarshal(p["body"].([]byte), &body)
			if p["forced_provider"] != "codex" || p["entry_protocol"] != "openai-response" {
				t.Error("request did not pin native route")
			}
			if body["tools"] != nil || body["temperature"] != nil || p["headers"] != nil {
				t.Error("test contaminated sampling conditions")
			}
			call := evalMockCall{Auth: p["auth_id"].(string), Model: p["model"].(string), Body: body}
			f.mu.Lock()
			f.Calls = append(f.Calls, call)
			id := fmt.Sprint(len(f.Calls))
			f.Active++
			f.Peak = max(f.Peak, f.Active)
			answerFn := f.Answer
			f.mu.Unlock()
			answer, err := answerFn(call)
			f.mu.Lock()
			defer f.mu.Unlock()
			if err != nil {
				f.Active--
				return nil, err
			}
			f.Streams[id] = evalMockSSE(answer, call.Model)
			return json.Marshal(map[string]string{"stream_id": id})
		case "host.model.stream_read":
			id := payload.(map[string]string)["stream_id"]
			f.mu.Lock()
			defer f.mu.Unlock()
			return json.Marshal(map[string]any{"payload": f.Streams[id], "done": true})
		case "host.model.stream_close":
			id := payload.(map[string]string)["stream_id"]
			f.mu.Lock()
			defer f.mu.Unlock()
			if _, ok := f.Streams[id]; ok {
				delete(f.Streams, id)
				f.Active--
			}
			return json.RawMessage(`{}`), nil
		default:
			return nil, errors.New("unexpected callback")
		}
	}
	t.Cleanup(func() {
		e.stop()
		waitPrefetchIdle(t, e)
		e.queueWG.Wait()
		hostAuthListFunc, hostAuthGetFunc, evalHostCall = previousList, previousGet, previousCall
		history.mu.Lock()
		history.records, history.total, history.inserted, history.replaced = previousRecords, previousTotal, previousInserted, previousReplaced
		history.mu.Unlock()
	})
	return e, f
}

func evalTestRun(t *testing.T, e *probeEngine, kind, model, effort string, runs, accounts int) managementResponse {
	t.Helper()
	inventory, err := e.evalAccounts()
	if err != nil {
		t.Fatal(err)
	}
	var scopes []string
	for _, entry := range inventory[:accounts] {
		scopes = append(scopes, entry.Scope)
	}
	raw, _ := json.Marshal(map[string]any{"kind": kind, "model": model, "effort": effort, "runs": runs, "accounts": scopes, "confirmed": true})
	return e.evalRunResponse(raw)
}

func TestEvaluationCandySerialAndIndependentPersistence(t *testing.T) {
	e, f := newEvalFixture(t)
	e.halted = true
	e.values["healthy"] = stateEntry{Value: "HEALTHY_TICKET_CANARY"}
	e.candidates["healthy"] = stateEntry{Value: "CANDIDATE_CANARY"}
	e.business["old"] = businessDegradation{Reason: "keep"}
	e.disabledExits = map[string]bool{"keep": true}
	response := evalTestRun(t, e, "candy", "gpt-6-astra", "high", 2, 2)
	if response.StatusCode != 202 {
		t.Fatalf("launch: %d %s", response.StatusCode, response.Body)
	}
	waitPrefetchIdle(t, e)
	f.mu.Lock()
	calls, peak, active := append([]evalMockCall(nil), f.Calls...), f.Peak, f.Active
	f.mu.Unlock()
	if len(calls) != 4 || peak != 1 || active != 0 {
		t.Fatalf("budget/concurrency %d/%d/%d", len(calls), peak, active)
	}
	if !e.halted || e.probesTotal != 0 || e.probesOK != 0 || e.values["healthy"].Value != "HEALTHY_TICKET_CANARY" || e.candidates["healthy"].Value != "CANDIDATE_CANARY" || len(e.business) != 1 || !e.disabledExits["keep"] {
		t.Fatal("evaluation mutated health state")
	}
	e.mu.Lock()
	results := append([]evalResult(nil), e.evalStateLocked().Results...)
	e.mu.Unlock()
	if len(results) != 2 || results[0].Correct != 2 || results[0].Graded != 2 || results[0].Samples[0].Usage.Input == nil {
		t.Fatal("missing grade/usage")
	}
	if calls[0].Auth == calls[1].Auth {
		t.Fatal("same-priority jobs did not preserve FIFO per attempt")
	}
	for _, call := range calls {
		if call.Body["reasoning"].(map[string]any)["effort"] != "high" {
			t.Fatal("effort not sent")
		}
	}
	path := filepath.Join(filepath.Dir(stateFilePath()), "eval-results.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"PRIVATE_A_CANARY", "PRIVATE_B_CANARY", "HEALTHY_TICKET_CANARY", "CANDIDATE_CANARY", "a@example.invalid"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatal("private non-evaluation data persisted")
		}
	}
	restarted := &probeEngine{}
	restarted.mu.Lock()
	got := append([]evalResult(nil), restarted.evalStateLocked().Results...)
	restarted.mu.Unlock()
	if !reflect.DeepEqual(results, got) {
		t.Fatal("independent history did not survive reload")
	}
}

func TestEvaluationModelTracePartialAndNoRetries(t *testing.T) {
	e, f := newEvalFixture(t)
	count := 0
	f.Answer = func(call evalMockCall) (string, error) {
		count++
		if call.Body["reasoning"] != nil {
			t.Error("ModelTrace effort was changed")
		}
		if count == 1 {
			return "refused", nil
		}
		return strings.Repeat("42,", 320), nil
	}
	if evalTestRun(t, e, "modeltrace", "gpt-6-astra", "none", 1, 1).StatusCode != 202 {
		t.Fatal("launch failed")
	}
	waitPrefetchIdle(t, e)
	e.mu.Lock()
	r := e.evalStateLocked().Results[0]
	e.mu.Unlock()
	if count != 3 || r.Status != "partial" || r.Attribution == nil || r.Attribution.Used != 2 || r.Total != 3 || r.Graded != 0 {
		t.Fatal("partial attribution or fixed budget lost")
	}
}

func TestEvaluationValidationSleepAndGlobalStop(t *testing.T) {
	e, f := newEvalFixture(t)
	for _, body := range []string{`{}`, `{"confirmed":true,"kind":"candy","model":"bad\nmodel","accounts":["x"]}`, `{"confirmed":true,"kind":"modeltrace","model":"gpt-6-astra","effort":"high","accounts":["x"]}`} {
		if e.evalRunResponse([]byte(body)).StatusCode != 400 {
			t.Fatal("invalid request accepted")
		}
	}
	e.paused["gpt-6-astra"] = true
	hour := time.Now().In(probeSleepZone).Hour()
	e.cfg.Config.SleepHours = probeSleepHours{Start: hour, End: (hour + 1) % 24}
	if evalTestRun(t, e, "candy", "gpt-6-astra", "none", 1, 1).StatusCode != 202 {
		t.Fatal("sleep task rejected")
	}
	time.Sleep(15 * time.Millisecond)
	f.mu.Lock()
	calls := len(f.Calls)
	f.mu.Unlock()
	if calls != 0 {
		t.Fatal("sleep bypassed")
	}
	e.stop()
	waitPrefetchIdle(t, e)
	e.mu.Lock()
	r := e.evalStateLocked().Results[0]
	e.mu.Unlock()
	if !e.halted || r.Status != "cancelled" || r.Done != 0 {
		t.Fatal("global stop lost")
	}
}

func TestEvaluationHealthPauseDoesNotBlockManualTests(t *testing.T) {
	for _, kind := range []string{"candy", "modeltrace"} {
		for _, scoped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/scoped=%t", kind, scoped), func(t *testing.T) {
				e, f := newEvalFixture(t)
				accounts, _ := e.evalAccounts()
				target := "gpt-6-astra"
				if scoped {
					target = scopedTarget(accounts[0].Scope, target)
				}
				e.paused[target] = true
				e.halted = true
				e.cfg.Config.Enabled = false
				effort := "medium"
				if kind == "modeltrace" {
					effort = "none"
					f.Answer = func(evalMockCall) (string, error) { return strings.Repeat("42,", 320), nil }
				}
				if response := evalTestRun(t, e, kind, "gpt-6-astra", effort, 1, 1); response.StatusCode != 202 {
					t.Fatalf("paused manual test rejected: %d", response.StatusCode)
				}
				waitPrefetchIdle(t, e)
				e.mu.Lock()
				r := e.evaluations.Results[0]
				unchanged := e.halted && e.paused[target] && len(e.paused) == 1 && e.probesTotal == 0 && len(e.values) == 0
				e.mu.Unlock()
				want := 1
				if kind == "modeltrace" {
					want = 3
				}
				if r.Done != want || r.Status != "completed" || !unchanged {
					t.Fatalf("manual result/mode: %s/%d, unchanged=%t", r.Status, r.Done, unchanged)
				}
			})
		}
	}
}

func TestEvaluationHealthPauseKeepsRunningQueuedAndRemainingSamples(t *testing.T) {
	e, f := newEvalFixture(t)
	e.halted = true
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.Answer = func(evalMockCall) (string, error) {
		once.Do(func() { close(entered); <-release })
		return "答案是 21", nil
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	if evalTestRun(t, e, "candy", "gpt-6-astra", "medium", 2, 2).StatusCode != 202 {
		t.Fatal("launch failed")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	if !e.setModelPaused("gpt-6-astra", true) || e.enqueueTask("gpt-6-astra", true) {
		t.Fatal("health pause gate lost")
	}
	e.mu.Lock()
	kept := len(e.evaluations.Jobs) == 2 && len(e.queue) == 1 && e.queue[0].Evaluation != nil
	e.mu.Unlock()
	if !kept {
		t.Fatal("health pause cancelled running or queued ability work")
	}
	close(release)
	waitPrefetchIdle(t, e)
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.halted || !e.paused["gpt-6-astra"] || e.probesTotal != 0 || len(e.evaluations.Results) != 2 {
		t.Fatal("evaluation changed health participation")
	}
	for _, r := range e.evaluations.Results {
		if r.Status != "completed" || r.Correct != 2 || r.Done != 2 {
			t.Fatal("pause affected a sample or account")
		}
	}
}

func TestEvaluationHealthPauseDuringSleepSurvivesWake(t *testing.T) {
	e, f := newEvalFixture(t)
	e.halted = true
	hour := time.Now().In(probeSleepZone).Hour()
	e.cfg.Config.SleepHours = probeSleepHours{Start: hour, End: (hour + 1) % 24}
	if evalTestRun(t, e, "candy", "gpt-6-astra", "none", 1, 1).StatusCode != 202 {
		t.Fatal("launch failed")
	}
	e.setModelPaused("gpt-6-astra", true)
	time.Sleep(15 * time.Millisecond)
	f.mu.Lock()
	calls := len(f.Calls)
	f.mu.Unlock()
	e.mu.Lock()
	kept := len(e.evaluations.Jobs) == 1 && len(e.evaluations.Results) == 0
	e.cfg.Config.SleepHours = probeSleepHours{}
	e.mu.Unlock()
	if calls != 0 || !kept {
		t.Fatal("sleep bypassed or health pause cancelled waiting evaluation")
	}
	waitPrefetchIdle(t, e)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.evaluations.Results[0].Correct != 1 || !e.paused["gpt-6-astra"] || !e.halted {
		t.Fatal("wake lost evaluation or changed health mode")
	}
}

func TestEvaluationPausedModelRetainsSharedQueuePriority(t *testing.T) {
	e := &probeEngine{cfg: probeConfigState{Config: probeConfig{Models: []string{"gpt-6-astra", "gpt-5.6-sol"}}},
		paused: map[string]bool{"gpt-6-astra": true}, halted: true, queueActive: true,
		queue: []probeTask{{Model: "gpt-6-astra", Force: true, Evaluation: &evalJob{}}}}
	e.mu.Lock()
	yielded := e.yieldProbeTaskLocked(probeTask{Model: "gpt-5.6-sol", Force: true}, probeProgress{}, make(chan struct{}), make(chan struct{}))
	e.mu.Unlock()
	if !yielded || len(e.queue) != 2 || e.queue[0].Evaluation == nil || e.queue[1].Model != "gpt-5.6-sol" {
		t.Fatal("health pause prevented higher-priority manual work")
	}
}

func TestEvaluationCancellationKeepsOtherAccountAndHalt(t *testing.T) {
	e, f := newEvalFixture(t)
	e.halted = true
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.Answer = func(evalMockCall) (string, error) { once.Do(func() { close(entered); <-release }); return "21", nil }
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	if evalTestRun(t, e, "candy", "gpt-6-astra", "none", 3, 2).StatusCode != 202 {
		t.Fatal("launch failed")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	e.mu.Lock()
	var active *evalJob
	for _, job := range e.evaluations.Jobs {
		if job.Executing {
			active = job
		}
	}
	e.mu.Unlock()
	raw, _ := json.Marshal(map[string]string{"id": active.Record.ID})
	if e.evalCancelResponse(raw).StatusCode != 200 {
		t.Fatal("cancel failed")
	}
	close(release)
	waitPrefetchIdle(t, e)
	e.mu.Lock()
	results := append([]evalResult(nil), e.evaluations.Results...)
	e.mu.Unlock()
	if len(results) != 2 || results[0].Status != "cancelled" || results[1].Done != 3 || !e.halted {
		t.Fatal("cancel affected other job/mode")
	}
	f.mu.Lock()
	calls := len(f.Calls)
	f.mu.Unlock()
	if calls != 4 {
		t.Fatal("cancel sent extra attempts")
	}
}

func TestEvaluationLateReplacementCannotInheritResults(t *testing.T) {
	e, f := newEvalFixture(t)
	f.Answer = func(call evalMockCall) (string, error) {
		f.mu.Lock()
		for _, entry := range f.Entries {
			if entry.ID == call.Auth {
				f.Tokens[entry.AuthIndex] = "REPLACED_CREDENTIAL"
			}
		}
		f.mu.Unlock()
		return "21", nil
	}
	if evalTestRun(t, e, "candy", "gpt-6-astra", "none", 2, 1).StatusCode != 202 {
		t.Fatal("launch failed")
	}
	waitPrefetchIdle(t, e)
	e.mu.Lock()
	r := e.evaluations.Results[0]
	e.mu.Unlock()
	if r.Status != "failed" || r.Error != "account_unavailable_or_changed" || len(r.Samples) != 0 {
		t.Fatal("late answer attributed after replacement")
	}
	var state struct {
		Results []evalResult `json:"results"`
	}
	_ = json.Unmarshal(e.evalStateResponse().Body, &state)
	if len(state.Results) != 0 {
		t.Fatal("replacement inherited old result")
	}
}

func TestEvaluationSharesHealthWorkerPriorityAndInterval(t *testing.T) {
	e, f := newEvalFixture(t)
	e.cfg.Config.Models = []string{"gpt-6-astra", "gpt-5.6-sol"}
	e.cfg.Config.ProbeInterval = 15 * time.Millisecond
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var order []string
	var mu sync.Mutex
	f.Answer = func(evalMockCall) (string, error) {
		mu.Lock()
		order = append(order, "eval")
		mu.Unlock()
		once.Do(func() { close(entered); <-release })
		return "21", nil
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.Active++
		f.Peak = max(f.Peak, f.Active)
		f.mu.Unlock()
		mu.Lock()
		order = append(order, "health")
		mu.Unlock()
		w.WriteHeader(503)
		f.mu.Lock()
		f.Active--
		f.mu.Unlock()
	}))
	defer server.Close()
	e.cfg.Config.Proxies = []string{server.URL}
	e.cfg.Config.UpstreamURL = "http://mock-eval.invalid/responses"
	e.cfg.Config.AttemptsPerHop = 1
	if err := os.WriteFile(e.cfg.Config.CredFile, []byte(`{"access_token":"synthetic-health"}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	if evalTestRun(t, e, "candy", "gpt-5.6-sol", "none", 2, 1).StatusCode != 202 {
		t.Fatal("launch failed")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("eval did not start")
	}
	if !e.enqueueTask("gpt-6-astra", true) {
		t.Fatal("health task rejected")
	}
	close(release)
	waitPrefetchIdle(t, e)
	if !reflect.DeepEqual(order, []string{"eval", "health", "eval"}) {
		t.Fatalf("priority lost: %v", order)
	}
	f.mu.Lock()
	peak := f.Peak
	f.mu.Unlock()
	if peak != 1 {
		t.Fatal("health and evaluation overlapped")
	}
}

func TestEvaluationCorruptStoreFailsClosedAndHistoryBounded(t *testing.T) {
	e, _ := newEvalFixture(t)
	path := filepath.Join(filepath.Dir(stateFilePath()), "eval-results.json")
	if err := os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if evalTestRun(t, e, "candy", "gpt-6-astra", "none", 1, 1).StatusCode != 503 {
		t.Fatal("corrupt storage accepted new work")
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != "corrupt" {
		t.Fatal("corrupt history overwritten")
	}
	e.evaluations = &evalState{Loaded: true, Jobs: map[string]*evalJob{}}
	for i := 0; i < 28; i++ {
		e.evaluations.Results = append(e.evaluations.Results, evalResult{ID: fmt.Sprint(i), Scope: "synthetic-scope", Kind: "candy", Model: "gpt-6-astra"})
	}
	e.saveEvalLocked()
	if len(e.evaluations.Results) != 20 || e.evaluations.Results[0].ID != "8" {
		t.Fatal("history cap lost")
	}
}

func TestEvaluationOldCandyRevisionIsPreservedWithoutRegrading(t *testing.T) {
	e, _ := newEvalFixture(t)
	accounts, _ := e.evalAccounts()
	old := evalResult{ID: "legacy-candy", Scope: accounts[0].Scope, Kind: "candy", Model: "gpt-6-astra",
		Revision: "candy-shapes-v2-final-29", Status: "completed", Done: 1, Total: 1, Correct: 1, Graded: 1,
		Samples: []evalSample{{Text: "FINAL_ANSWER=29", Status: "correct"}}}
	e.mu.Lock()
	e.evalStateLocked().Results = []evalResult{old}
	e.saveEvalLocked()
	e.evaluations = nil
	e.mu.Unlock()
	if evalTestRun(t, e, "candy", "gpt-6-astra", "medium", 1, 1).StatusCode != 202 {
		t.Fatal("launch failed")
	}
	waitPrefetchIdle(t, e)
	e.mu.Lock()
	defer e.mu.Unlock()
	results := e.evaluations.Results
	if len(results) != 2 || !reflect.DeepEqual(results[0], old) || results[1].Revision != "candy-shapes-v3-original-21" || results[1].Correct != 1 {
		t.Fatal("legacy history was rewritten or new result used the old benchmark")
	}
}

func TestEvaluationErrorsDoNotLeakAndUnknownTokensStayUnknown(t *testing.T) {
	e, f := newEvalFixture(t)
	f.Answer = func(evalMockCall) (string, error) { return "", errors.New("PRIVATE_UPSTREAM_BODY PRIVATE_A_CANARY") }
	if evalTestRun(t, e, "candy", "gpt-6-astra", "none", 1, 1).StatusCode != 202 {
		t.Fatal("launch failed")
	}
	waitPrefetchIdle(t, e)
	r := e.evaluations.Results[0]
	if r.Graded != 0 || r.Samples[0].Error != "host_execute_failed" || r.Samples[0].Usage.Input != nil {
		t.Fatal("error counted as grade or token usage invented")
	}
	raw, _ := json.Marshal(r)
	if bytes.Contains(raw, []byte("PRIVATE_")) {
		t.Fatal("raw upstream error leaked")
	}
}

func TestEvaluationStreamLimitsCompletionAndCancellation(t *testing.T) {
	if _, err := parseEvalResponse([]byte("data: {\"type\":\"response.created\"}\n\n")); err == nil {
		t.Fatal("incomplete stream accepted")
	}
	if _, err := parseEvalResponse(evalMockSSE(strings.Repeat("x", evalTextLimit+1), "model")); err == nil {
		t.Fatal("oversized answer accepted")
	}
	previous := evalHostCall
	defer func() { evalHostCall = previous }()
	blocked, closed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	evalHostCall = func(method string, _ any) (json.RawMessage, error) {
		switch method {
		case "host.model.execute_stream":
			return json.RawMessage(`{"stream_id":"cancel-test"}`), nil
		case "host.model.stream_read":
			close(blocked)
			<-closed
			return nil, errors.New("closed")
		case "host.model.stream_close":
			once.Do(func() { close(closed) })
			return json.RawMessage(`{}`), nil
		}
		return nil, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = executeEvalModel(ctx, hostAuthEntry{ID: "synthetic"}, "model", "prompt", "none", nil)
	}()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("read not entered")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancel did not close stream")
	}
}

func TestEvaluationHostEventFieldBoundary(t *testing.T) {
	previous := evalHostCall
	t.Cleanup(func() { evalHostCall = previous })
	chunks := [][]byte{[]byte(`data: {"type":"response.created"}`), evalMockSSE("21", "gpt-6-astra")}
	next := 0
	evalHostCall = func(method string, _ any) (json.RawMessage, error) {
		switch method {
		case "host.model.execute_stream":
			return json.RawMessage(`{"stream_id":"event-boundary"}`), nil
		case "host.model.stream_read":
			payload := chunks[next]
			next++
			return json.Marshal(map[string]any{"payload": payload, "done": next == len(chunks)})
		case "host.model.stream_close":
			return json.RawMessage(`{}`), nil
		}
		return nil, errors.New("unexpected callback")
	}
	out, err := executeEvalModel(context.Background(), hostAuthEntry{ID: "synthetic"}, "gpt-6-astra", "synthetic prompt", "medium", nil)
	if err != nil || out.Text != "21" || out.Usage.Output == nil {
		t.Fatalf("host event boundary lost: %v", err)
	}
}

func TestEvaluationStopDuringInventoryCannotLaunchLater(t *testing.T) {
	e, f := newEvalFixture(t)
	accounts, _ := e.evalAccounts()
	raw, _ := json.Marshal(map[string]any{"accounts": []string{accounts[0].Scope}, "kind": "candy", "model": "gpt-6-astra", "confirmed": true})
	previous := hostAuthListFunc
	entered, release, response := make(chan struct{}), make(chan struct{}), make(chan managementResponse, 1)
	hostAuthListFunc = func() ([]hostAuthEntry, error) { close(entered); <-release; return previous() }
	go func() { response <- e.evalRunResponse(raw) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("inventory not entered")
	}
	e.stopCurrent()
	close(release)
	select {
	case got := <-response:
		if got.StatusCode != 409 {
			t.Fatal("stale inventory launched a task after stop")
		}
	case <-time.After(time.Second):
		t.Fatal("launch did not return")
	}
	hostAuthListFunc = previous
	f.mu.Lock()
	calls := len(f.Calls)
	f.mu.Unlock()
	if calls != 0 || e.halted {
		t.Fatal("stop-current lost its mode or sent a request")
	}
}

func TestEvaluationUncheckedModelIsManualOnlyAndRoutesKeepMethods(t *testing.T) {
	e, _ := newEvalFixture(t)
	if evalTestRun(t, e, "candy", "gpt-5.6-luna", "none", 1, 1).StatusCode != 202 {
		t.Fatal("explicit ability test rejected")
	}
	waitPrefetchIdle(t, e)
	if degradationDetectionEnabled("gpt-5.6-luna", "") || e.probesTotal != 0 || len(e.values) != 0 {
		t.Fatal("ability test enabled ticket detection")
	}
	for _, tc := range []struct{ path, method string }{{"eval", "POST"}, {"eval-run", "GET"}, {"eval-cancel", "GET"}, {"eval-detail", "GET"}} {
		raw, _ := json.Marshal(managementRequest{Path: "/v0/management/" + pluginID + "/" + tc.path, Method: tc.method})
		response, err := management(raw)
		if err != nil || response.StatusCode != 405 {
			t.Fatal("evaluation route method gate lost")
		}
	}
	registration, _ := handleMethod("management.register", nil)
	raw, _ := json.Marshal(registration)
	for _, route := range []string{"/eval", "/eval-run", "/eval-cancel", "/eval-detail"} {
		if !bytes.Contains(raw, []byte("/"+pluginID+route)) {
			t.Fatal("evaluation route missing")
		}
	}
}

func TestEvaluationShutdownDrainsCallbacksBeforeReturning(t *testing.T) {
	e, f := newEvalFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	f.Answer = func(evalMockCall) (string, error) { close(entered); <-release; return "21", nil }
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	if evalTestRun(t, e, "candy", "gpt-6-astra", "none", 1, 1).StatusCode != 202 {
		t.Fatal("launch failed")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request not entered")
	}
	done := make(chan struct{})
	go func() { e.shutdown(); close(done) }()
	select {
	case <-done:
		t.Fatal("shutdown released a running callback")
	case <-time.After(15 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not drain")
	}
	if e.evaluations.Results[0].Status != "cancelled" || e.halted {
		t.Fatal("shutdown lost cancellation or changed user mode")
	}
	if evalTestRun(t, e, "candy", "gpt-6-astra", "none", 1, 1).StatusCode != 409 {
		t.Fatal("shutdown accepted new work")
	}
}
