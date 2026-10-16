package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestCPAIntegration(t *testing.T) {
	binary := os.Getenv("CPA_INTEGRATION_BINARY")
	plugin := os.Getenv("CPA_INTEGRATION_PLUGIN")
	if binary == "" || plugin == "" {
		t.Skip("set CPA_INTEGRATION_BINARY and CPA_INTEGRATION_PLUGIN for the isolated CPA test")
	}
	type captured struct {
		transport string
		body      []byte
		headers   http.Header
	}
	captures := make(chan captured, 32)
	var sequence atomic.Int32
	var outputModel atomic.Value
	outputModel.Store("gpt-5.5")
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			for {
				_, body, err := conn.ReadMessage()
				if err != nil {
					return
				}
				captures <- captured{"websocket", body, r.Header.Clone()}
				for _, event := range mockResponseEvents(sequence.Add(1), currentMockModel(&outputModel)) {
					if err := conn.WriteJSON(event); err != nil {
						return
					}
				}
			}
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read failed", http.StatusBadRequest)
			return
		}
		captures <- captured{"http", body, r.Header.Clone()}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, event := range mockResponseEvents(sequence.Add(1), currentMockModel(&outputModel)) {
			encoded, _ := json.Marshal(event)
			fmt.Fprintf(w, "data: %s\n\n", encoded)
		}
	}))
	defer upstream.Close()

	dir := t.TempDir()
	pluginDir := filepath.Join(dir, "plugins", "linux", "amd64")
	if err := os.MkdirAll(pluginDir, 0700); err != nil {
		t.Fatal(err)
	}
	pluginBytes, err := os.ReadFile(plugin)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, pluginID+".so"), pluginBytes, 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	configPath := filepath.Join(dir, "config.yaml")
	config := fmt.Sprintf(`host: "127.0.0.1"
port: %d
auth-dir: %q
api-keys: ["integration-client-key"]
request-retry: 0
remote-management:
  allow-remote: false
  secret-key: "integration-management-key"
  disable-control-panel: true
plugins:
  enabled: true
  dir: %q
  configs:
    oai-adversarial-plugin:
      enabled: true
      priority: 100
      turn-state-override:
        enabled: true
        models: ["gpt-5.5", "test-codex"]
        value: "INTEGRATION-REWRITTEN-STATE"
        force: true
codex-api-key:
  - api-key: "integration-upstream-key"
    base-url: %q
    websockets: true
    models:
      - name: "gpt-5.5"
        alias: "test-codex"
`, port, filepath.Join(dir, "auths"), filepath.Join(dir, "plugins"), upstream.URL)
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "cpa.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "--config", configPath, "--local-model")
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		command.Process.Kill()
		command.Wait()
		logFile.Close()
		if t.Failed() {
			contents, _ := os.ReadFile(logPath)
			t.Logf("CPA output:\n%s", contents)
		}
	}()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := &http.Client{Timeout: 12 * time.Second}
	request := func(t *testing.T, method, path, key string, body []byte) (int, []byte) {
		t.Helper()
		req, _ := http.NewRequest(method, base+path, bytes.NewReader(body))
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, data
	}
	ready := false
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		req, _ := http.NewRequest("GET", base+"/v1/models", nil)
		req.Header.Set("Authorization", "Bearer integration-client-key")
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		t.Fatal("CPA did not become ready")
	}
	assertCapture := func(t *testing.T, transport, original string) {
		t.Helper()
		select {
		case captured := <-captures:
			if captured.transport != transport {
				t.Fatalf("expected %s upstream, got %s", transport, captured.transport)
			}
			if transport == "http" {
				if got := captured.headers.Get(turnStateHeader); got != "INTEGRATION-REWRITTEN-STATE" {
					t.Errorf("turn-state rewrite missing on upstream request: %q", got)
				}
			} else {
				// WS handshake headers depend on how the executor merges
				// interceptor headers; record the observed value for diagnosis.
				t.Logf("websocket upstream handshake turn-state: %q", captured.headers.Get(turnStateHeader))
			}
			_, info, err := normalizeRequest(captured.body, "codex")
			if err != nil || info.Action != "unchanged" || len(info.Original) == 0 {
				t.Fatalf("wire payload did not contain normalized timezone: %+v %v, body %s", info, err, captured.body)
			}
			for _, zone := range info.Original {
				if zone != targetTimezone {
					t.Errorf("unconverted timezone on wire: %s", zone)
				}
			}
		case <-time.After(3 * time.Second):
			t.Fatal("no upstream request captured")
		}
		status, data := request(t, "GET", "/v0/management/timezone-override/requests", "integration-management-key", nil)
		var snapshot struct {
			Records []auditRecord `json:"records"`
		}
		if status != 200 || json.Unmarshal(data, &snapshot) != nil || len(snapshot.Records) == 0 {
			t.Fatalf("audit endpoint failed: %d %s", status, data)
		}
		last := snapshot.Records[0]
		if strings.Join(last.Original, ",") != original || last.Target != targetTimezone {
			t.Fatalf("original/target display data incorrect: %+v", last)
		}
		if last.TurnStateOverride != "applied" && last.TurnStateOverride != "applied-config" {
			t.Errorf("override status must be recorded: %+v", last)
		}
	}

	for _, tc := range []struct{ name, path, body, original string }{
		{"responses replace", "/v1/responses", `{"model":"test-codex","input":[{"role":"user","content":"<environment_context><timezone>Asia/Shanghai</timezone></environment_context>"}]}`, "Asia/Shanghai"},
		{"responses insert", "/v1/responses", `{"model":"test-codex","input":"hello"}`, ""},
		{"responses stream", "/v1/responses", `{"model":"test-codex","input":"hello","stream":true}`, ""},
		{"chat insert", "/v1/chat/completions", `{"model":"test-codex","messages":[{"role":"user","content":"hello"}]}`, ""},
		{"chat stream replace", "/v1/chat/completions", `{"model":"test-codex","messages":[{"role":"user","content":"<environment_context><timezone>Europe/London</timezone></environment_context>"}],"stream":true}`, "Europe/London"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := request(t, "POST", tc.path, "integration-client-key", []byte(tc.body))
			if status != 200 || !bytes.Contains(body, []byte("OK")) {
				t.Fatalf("proxy response failed: %d %s", status, body)
			}
			assertCapture(t, "http", tc.original)
			var input struct {
				Stream bool `json:"stream"`
			}
			if err := json.Unmarshal([]byte(tc.body), &input); err != nil {
				t.Fatal(err)
			}
			// Completion callbacks are asynchronous. Verify host timestamps
			// and response tokens on the same execution after its body ends.
			observed := false
			for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
				status, data := request(t, "GET", "/v0/management/"+pluginID+"/requests", "integration-management-key", nil)
				var snapshot struct {
					Records []auditRecord `json:"records"`
				}
				if status == 200 && json.Unmarshal(data, &snapshot) == nil && len(snapshot.Records) > 0 {
					r := snapshot.Records[0]
					if r.RequestOutcome == "succeeded" && r.RequestLatencyMS != nil && *r.RequestLatencyMS > 0 && r.OutputTPS != nil {
						if input.Stream && r.FirstOutputLatencyMS == nil {
							t.Fatal("stream first output missing")
						}
						if !input.Stream && r.FirstOutputLatencyMS != nil {
							t.Fatal("nonstream first output fabricated")
						}
						observed = true
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !observed {
				t.Fatal("host completion/token performance unavailable")
			}
		})
	}

	t.Run("websocket insert and replace on reused connection", func(t *testing.T) {
		wsURL := strings.Replace(base, "http://", "ws://", 1) + "/v1/responses"
		conn, response, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": {"Bearer integration-client-key"}})
		if err != nil {
			t.Fatalf("websocket upgrade failed: %v %+v", err, response)
		}
		defer conn.Close()
		for _, original := range []string{"", "Asia/Shanghai"} {
			text := "hello"
			if original != "" {
				text = "<environment_context><timezone>" + original + "</timezone></environment_context>"
			}
			frame := map[string]any{"type": "response.create", "model": "test-codex", "input": []any{map[string]any{"role": "user", "content": text}}}
			if err := conn.WriteJSON(frame); err != nil {
				t.Fatal(err)
			}
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			for {
				_, raw, err := conn.ReadMessage()
				if err != nil {
					t.Fatal(err)
				}
				var event map[string]any
				json.Unmarshal(raw, &event)
				if event["type"] == "error" {
					t.Fatalf("websocket error: %s", raw)
				}
				if event["type"] == "response.completed" {
					break
				}
			}
			assertCapture(t, "websocket", original)
		}
	})
	t.Run("model mismatch and turn state observations", func(t *testing.T) {
		outputModel.Store("gpt-6-luna")
		defer outputModel.Store("gpt-5.5")
		const turnState = "sample-state-0001-partial-state"
		req, _ := http.NewRequest("POST", base+"/v1/responses", bytes.NewReader([]byte(`{"model":"test-codex","input":"hello","stream":true}`)))
		req.Header.Set("Authorization", "Bearer integration-client-key")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Codex-Turn-State", turnState)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(resp.Body); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("proxy response failed: %d", resp.StatusCode)
		}
		// Stream finalization and the observation callback are asynchronous;
		// poll until the observation lands or the deadline expires.
		var last auditRecord
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
			status, data := request(t, "GET", "/v0/management/timezone-override/requests", "integration-management-key", nil)
			var snapshot struct {
				Records []auditRecord `json:"records"`
			}
			if status == 200 && json.Unmarshal(data, &snapshot) == nil && len(snapshot.Records) > 0 {
				last = snapshot.Records[0]
				if last.ModelChecked && last.TurnStateLength > 0 {
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !last.ModelChecked || last.UpstreamModel != "gpt-6-luna" {
			t.Fatalf("upstream model observation missing: %+v", last)
		}
		if !last.ModelMismatch {
			t.Fatalf("model mismatch must be flagged: %+v", last)
		}
		if last.TurnStateLength != len(turnState) {
			t.Fatalf("turn state length wrong: %d != %d (%+v)", last.TurnStateLength, len(turnState), last)
		}
		if last.TurnStateValue != turnState {
			t.Fatalf("turn state full value missing: %+v", last)
		}
	})
	t.Run("turn state rewrite replaces client value on wire", func(t *testing.T) {
		req, _ := http.NewRequest("POST", base+"/v1/chat/completions", bytes.NewReader([]byte(`{"model":"test-codex","messages":[{"role":"user","content":"hello"}]}`)))
		req.Header.Set("Authorization", "Bearer integration-client-key")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Codex-Turn-State", "CLIENT-ORIGINAL")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || !bytes.Contains(body, []byte("OK")) {
			t.Fatalf("proxy response failed: %d %s", resp.StatusCode, body)
		}
		select {
		case captured := <-captures:
			if got := captured.headers.Get(turnStateHeader); got != "INTEGRATION-REWRITTEN-STATE" {
				t.Fatalf("upstream must receive the rewritten value, got %q", got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("no upstream request captured")
		}
		status, data := request(t, "GET", "/v0/management/timezone-override/requests", "integration-management-key", nil)
		var snapshot struct {
			Records []auditRecord `json:"records"`
		}
		if status != 200 || json.Unmarshal(data, &snapshot) != nil || len(snapshot.Records) == 0 {
			t.Fatalf("audit endpoint failed: %d %s", status, data)
		}
		last := snapshot.Records[0]
		if last.TurnStateOverride != "applied" && last.TurnStateOverride != "applied-config" {
			t.Fatalf("record must show applied: %+v", last)
		}
		if last.TurnStateValue != "CLIENT-ORIGINAL" {
			t.Fatalf("record must keep the client-side original view: %+v", last)
		}
	})
	t.Run("dashboard authentication", func(t *testing.T) {
		status, data := request(t, "GET", "/v0/resource/plugins/oai-adversarial-plugin/status", "", nil)
		if status != 200 || !bytes.Contains(data, []byte("原时区")) {
			t.Fatalf("dashboard resource unavailable: %d", status)
		}
		for _, id := range []string{pluginID, legacyPluginID} {
			status, _ = request(t, "GET", "/v0/management/"+id+"/requests", "", nil)
			if status != 401 && status != 403 {
				t.Errorf("audit records exposed without management authentication (%s): %d", id, status)
			}
			status, data = request(t, "GET", "/v0/management/"+id+"/requests", "integration-management-key", nil)
			var identity struct {
				Plugin string `json:"plugin"`
			}
			if status != 200 || json.Unmarshal(data, &identity) != nil || identity.Plugin != pluginID {
				t.Fatalf("renamed API identity unavailable (%s): %d", id, status)
			}
		}
		status, data = request(t, "GET", "/v0/management/oai-adversarial-plugin/requests", "integration-management-key", nil)
		var snapshot struct {
			Override map[string]any `json:"turn_state_override"`
		}
		if status != 200 || json.Unmarshal(data, &snapshot) != nil {
			t.Fatalf("override summary unavailable: %d %s", status, data)
		}
		if snapshot.Override["enabled"] != true {
			t.Fatalf("rewrite config not active: %+v", snapshot.Override)
		}
		if message, _ := snapshot.Override["error"].(string); message != "" {
			t.Fatalf("rewrite config error present: %+v", snapshot.Override)
		}
		if snapshot.Override["value_length"] != float64(len("INTEGRATION-REWRITTEN-STATE")) {
			t.Fatalf("rewrite value length wrong: %+v", snapshot.Override)
		}
	})
}

func currentMockModel(v *atomic.Value) string {
	if model, ok := v.Load().(string); ok {
		return model
	}
	return "gpt-5.5"
}

func mockResponseEvents(sequence int32, model string) []map[string]any {
	id := fmt.Sprintf("resp_timezone_test_%d", sequence)
	message := map[string]any{"id": "msg_timezone", "type": "message", "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": "OK", "annotations": []any{}}}}
	return []map[string]any{
		{"type": "response.created", "response": map[string]any{"id": id, "object": "response", "model": model, "status": "in_progress", "output": []any{}}},
		{"type": "response.output_item.added", "output_index": 0, "item": map[string]any{"id": "msg_timezone", "type": "message", "role": "assistant", "status": "in_progress", "content": []any{}}},
		{"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "item_id": "msg_timezone", "delta": "OK"},
		{"type": "response.output_text.done", "output_index": 0, "content_index": 0, "item_id": "msg_timezone", "text": "OK"},
		{"type": "response.output_item.done", "output_index": 0, "item": message},
		{"type": "response.completed", "response": map[string]any{"id": id, "object": "response", "model": model, "status": "completed", "output": []any{message}, "usage": map[string]int{"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}}},
	}
}
