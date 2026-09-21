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
    timezone-override:
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
	command.Env = append(os.Environ(), "LKS_TZ_STATE_FILE="+filepath.Join(dir, "state.json"))
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
	// This suite intentionally changes the mock model to test observation.
	// Disable rejection through its supported runtime control, not YAML.
	status, _ := request(t, "POST", "/v0/management/timezone-override/probe-control", "integration-management-key", []byte(`{"action":"reject-degraded","enabled":false}`))
	if status != http.StatusOK {
		t.Fatalf("failed to disable rejection for observation suite: %d", status)
	}
	assertCapture := func(t *testing.T, transport, original string) {
		t.Helper()
		select {
		case captured := <-captures:
			if captured.transport != transport {
				t.Fatalf("expected %s upstream, got %s", transport, captured.transport)
			}
			if transport == "http" {
				if got := captured.headers.Get(turnStateHeader); got != "" {
					t.Errorf("unbound API-key fixture received a global ticket: length=%d", len(got))
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
		if last.TurnStateOverride != "account-unavailable" {
			t.Errorf("unconfirmed identity must fail closed: status=%s", last.TurnStateOverride)
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
	t.Run("unbound identity strips client value without static fallback", func(t *testing.T) {
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
			if got := captured.headers.Get(turnStateHeader); got != "" {
				t.Fatalf("unbound identity must not inject a ticket: length=%d", len(got))
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
		if last.TurnStateOverride != "account-unavailable" {
			t.Fatalf("record must explain missing account identity: %s", last.TurnStateOverride)
		}
		if last.TurnStateValue != "CLIENT-ORIGINAL" {
			t.Fatalf("record must keep the client-side original view: %+v", last)
		}
	})
	t.Run("dashboard authentication", func(t *testing.T) {
		status, data := request(t, "GET", "/v0/resource/plugins/timezone-override/status", "", nil)
		if status != 200 || !bytes.Contains(data, []byte("原时区")) {
			t.Fatalf("dashboard resource unavailable: %d", status)
		}
		status, _ = request(t, "GET", "/v0/management/timezone-override/requests", "", nil)
		if status != 401 && status != 403 {
			t.Errorf("audit records exposed without management authentication: %d", status)
		}
		status, data = request(t, "GET", "/v0/management/timezone-override/requests", "integration-management-key", nil)
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

// TestCPASessionGuardIntegration runs the production CPA binary with the
// compiled plugin and proves the host actually applies ClearHeaders on both
// sides of an HTTP/SSE exchange. The mock upstream first issues a healthy
// Astra state, then the client tries to reuse it for Sol on the same AuthID.
func TestCPASessionGuardIntegration(t *testing.T) {
	for _, mode := range []string{sessionGuardModeOff, sessionGuardModeEnforce} {
		t.Run(mode, func(t *testing.T) { runCPASessionGuardIntegration(t, mode) })
	}
}

func runCPASessionGuardIntegration(t *testing.T, mode string) {
	binary := os.Getenv("CPA_INTEGRATION_BINARY")
	plugin := os.Getenv("CPA_INTEGRATION_PLUGIN")
	if binary == "" || plugin == "" {
		t.Skip("set CPA_INTEGRATION_BINARY and CPA_INTEGRATION_PLUGIN for the isolated CPA test")
	}
	type captured struct {
		model        string
		state        string
		statePresent bool
	}
	captures := make(chan captured, 4)
	issuedState := strings.Repeat("S", 332)
	var quotaEnabled atomic.Bool
	var exhaustedOwner atomic.Value
	var quotaResponses, healthyResponses atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read failed", http.StatusBadRequest)
			return
		}
		var requestBody struct {
			Model string `json:"model"`
		}
		_ = json.Unmarshal(body, &requestBody)
		if quotaEnabled.Load() {
			authorization := r.Header.Get("Authorization")
			exhaustedOwner.CompareAndSwap(nil, authorization)
			if exhaustedOwner.Load() == authorization {
				quotaResponses.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "120")
				w.Header().Set(turnStateHeader, strings.Repeat("Q", 356))
				w.WriteHeader(http.StatusTooManyRequests)
				fmt.Fprint(w, `{"error":{"type":"usage_limit_reached","message":"synthetic quota exhausted"}}`)
				return
			}
			healthyResponses.Add(1)
		}
		_, statePresent := r.Header[http.CanonicalHeaderKey(turnStateHeader)]
		captures <- captured{model: requestBody.Model, state: r.Header.Get(turnStateHeader), statePresent: statePresent}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set(turnStateHeader, issuedState)
		for _, event := range mockResponseEvents(1, requestBody.Model) {
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
api-keys: ["session-guard-client-key"]
request-retry: 0
passthrough-headers: true
max-retry-credentials: 2
remote-management:
  allow-remote: false
  secret-key: "session-guard-management-key"
  disable-control-panel: true
plugins:
  enabled: true
  dir: %q
  configs:
    timezone-override:
      enabled: true
      priority: 100
      experimental-account-routing: true
      session-guard-mode: %s
      session-provenance-ttl-minutes: 60
      operation-mode: business-only
      override-policy: preserve-healthy-client
      override-models: ["gpt-6-astra", "gpt-5.6-sol"]
codex-api-key:
  - api-key: "session-guard-upstream-key"
    base-url: %q
    models:
      - name: "gpt-6-astra"
        alias: "test-astra"
      - name: "gpt-5.6-sol"
        alias: "test-sol"
      - name: "gpt-6-astra"
        alias: "test-quota"
  - api-key: "session-guard-second-account-key"
    base-url: %q
    models:
      - name: "gpt-6-astra"
        alias: "test-other-astra"
      - name: "gpt-6-astra"
        alias: "test-quota"
`, port, filepath.Join(dir, "auths"), filepath.Join(dir, "plugins"), mode, upstream.URL, upstream.URL)
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "cpa.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "--config", configPath, "--local-model")
	command.Env = append(os.Environ(), "LKS_TZ_STATE_FILE="+filepath.Join(dir, "state.json"))
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
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		req, _ := http.NewRequest("GET", base+"/v1/models", nil)
		req.Header.Set("Authorization", "Bearer session-guard-client-key")
		resp, requestErr := client.Do(req)
		if requestErr == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("CPA did not become ready")
		}
	}

	request := func(model, state string, stream bool) (*http.Response, []byte) {
		t.Helper()
		payload := fmt.Sprintf(`{"model":%q,"input":"hello","stream":%t}`, model, stream)
		req, _ := http.NewRequest("POST", base+"/v1/responses", strings.NewReader(payload))
		req.Header.Set("Authorization", "Bearer session-guard-client-key")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Session-Id", "session-guard-integration")
		if state != "" {
			req.Header.Set(turnStateHeader, state)
		}
		resp, requestErr := client.Do(req)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		return resp, body
	}

	first, firstBody := request("test-astra", "", false)
	if first.StatusCode != http.StatusOK || !bytes.Contains(firstBody, []byte("OK")) || first.Header.Get(turnStateHeader) != issuedState {
		t.Fatalf("Astra state was not issued: status=%d state=%d body=%s", first.StatusCode, len(first.Header.Get(turnStateHeader)), firstBody)
	}
	firstCapture := <-captures
	if firstCapture.state != "" || !strings.Contains(firstCapture.model, "astra") {
		t.Fatalf("unexpected first upstream request: %+v", firstCapture)
	}

	// Response finalization and usage callbacks are asynchronous. Wait until
	// the account-scoped table exposes a healthy Astra lease.
	healthy := false
	var lastSnapshot struct {
		Routing  map[string]any `json:"account_routing"`
		Override map[string]any `json:"turn_state_override"`
		Records  []auditRecord  `json:"records"`
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		req, _ := http.NewRequest("GET", base+"/v0/management/timezone-override/requests", nil)
		req.Header.Set("Authorization", "Bearer session-guard-management-key")
		resp, requestErr := client.Do(req)
		if requestErr != nil {
			continue
		}
		var snapshot struct {
			Routing struct {
				Accounts      []accountModelHealth `json:"accounts"`
				UsageObserved int                  `json:"usage_observed"`
			} `json:"account_routing"`
			Override struct {
				Guard struct {
					Confirmed int `json:"confirmed"`
				} `json:"session_guard"`
			} `json:"turn_state_override"`
		}
		data, _ := io.ReadAll(resp.Body)
		decodeErr := json.Unmarshal(data, &snapshot)
		_ = json.Unmarshal(data, &lastSnapshot)
		resp.Body.Close()
		if decodeErr == nil {
			// API-key fixtures are intentionally absent from CPA's physical
			// auth-file inventory. They must not leak into the account table;
			// verify ownership via its confirmation counter instead.
			healthy = mode == sessionGuardModeOff && snapshot.Routing.UsageObserved > 0 || mode != sessionGuardModeOff && snapshot.Override.Guard.Confirmed > 0
			for _, account := range snapshot.Routing.Accounts {
				if account.Model == "astra" && account.State == "healthy" {
					healthy = true
				}
			}
		}
		if healthy {
			break
		}
	}
	if !healthy {
		t.Logf("synthetic CPA diagnostics: routing=%+v guard=%+v records=%+v", lastSnapshot.Routing, lastSnapshot.Override["session_guard"], lastSnapshot.Records)
		t.Fatal("Astra response did not establish account-scoped ownership")
	}

	for _, target := range []string{"test-sol", "test-other-astra"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", target, stream), func(t *testing.T) {
				second, secondBody := request(target, issuedState, stream)
				if second.StatusCode != http.StatusOK || !bytes.Contains(secondBody, []byte("OK")) {
					t.Fatalf("request failed: status=%d body=%s", second.StatusCode, secondBody)
				}
				secondCapture := <-captures
				if mode == sessionGuardModeEnforce {
					if got := second.Header.Get(turnStateHeader); got != "" {
						t.Errorf("foreign response state was not cleared: %d bytes", len(got))
					}
					if secondCapture.state != "" || secondCapture.statePresent {
						t.Errorf("foreign request state reached upstream: model=%q state=%d present=%t", secondCapture.model, len(secondCapture.state), secondCapture.statePresent)
					}
				} else if secondCapture.state != "" || secondCapture.statePresent || second.Header.Get(turnStateHeader) != issuedState {
					// Disabling the additional session guard must not disable the
					// upstream credential-scoping policy on outgoing requests.
					t.Error("guard off must retain request isolation while leaving response observation unchanged")
				}
			})
		}
	}
	t.Run("quota_automatically_fails_over_without_probe", func(t *testing.T) {
		quotaEnabled.Store(true)
		for i := 0; i < 2; i++ {
			response, body := request("test-quota", "", false)
			if response.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("OK")) {
				t.Fatalf("quota failover failed: status=%d", response.StatusCode)
			}
		}
		if quotaResponses.Load() != 1 || healthyResponses.Load() != 2 {
			t.Fatalf("unexpected calls: limited=%d healthy=%d", quotaResponses.Load(), healthyResponses.Load())
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
