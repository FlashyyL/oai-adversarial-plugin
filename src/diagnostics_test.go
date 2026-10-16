package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDiagnosticsJournalBoundedAndRedacted verifies the opt-in structural
// journal: it is off by default, records header names and digests but never
// header values or bodies, and rotation keeps it bounded.
func TestDiagnosticsJournalBoundedAndRedacted(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LKS_TZ_STATE_FILE", filepath.Join(dir, "state.json"))
	t.Cleanup(func() { setDiagnosticsEnabled(false) })
	setDiagnosticsEnabled(false)
	diagnosticsEvent("request", map[string]any{"k": "must-not-write"})
	if _, err := os.Stat(diagnosticsPath()); !os.IsNotExist(err) {
		t.Fatal("disabled journal must not create a file")
	}

	setDiagnosticsEnabled(true)
	headers := http.Header{}
	headers.Set("Authorization", "Bearer SECRET-VALUE")
	headers.Set("X-Codex-Turn-State", "TICKET-VALUE")
	diagnosticsEvent("request", map[string]any{
		"headers": diagnosticsHeaderNames(headers),
		"digest":  digestShort("TICKET-VALUE"),
		"len":     len("TICKET-VALUE"),
	})
	raw, err := os.ReadFile(diagnosticsPath())
	if err != nil || len(raw) == 0 {
		t.Fatalf("journal not written: %v", err)
	}
	text := string(raw)
	if strings.Contains(text, "SECRET-VALUE") || strings.Contains(text, "TICKET-VALUE") {
		t.Fatalf("journal leaked a header value: %s", text)
	}
	if !strings.Contains(text, "authorization") || !strings.Contains(text, "x-codex-turn-state") {
		t.Fatalf("journal must keep header names: %s", text)
	}

	// Rotation: grow past the cap and confirm both files stay bounded.
	filler := strings.Repeat("x", 60<<10)
	for i := 0; i < 80 && !strings.Contains(text, "diagnostics.jsonl.1"); i++ {
		diagnosticsEvent("bulk", map[string]any{"pad": filler, "i": i})
		raw, _ = os.ReadFile(diagnosticsPath())
		text = string(raw)
		if info, err := os.Stat(diagnosticsPath()); err == nil && info.Size() > diagnosticsMaxBytes {
			t.Fatalf("active journal exceeded cap: %d", info.Size())
		}
		if _, err := os.Stat(filepath.Join(dir, diagnosticsBackup)); err == nil {
			text += "diagnostics.jsonl.1"
		}
	}
	if _, err := os.Stat(filepath.Join(dir, diagnosticsBackup)); err != nil {
		t.Fatal("rotation did not produce a backup file")
	}
	setDiagnosticsEnabled(false)
	before, _ := os.ReadFile(diagnosticsPath())
	diagnosticsEvent("request", map[string]any{"late": true})
	after, _ := os.ReadFile(diagnosticsPath())
	if len(after) != len(before) {
		t.Fatal("journal grew after being disabled")
	}
}

// TestDiagnosticsDigestAndShape covers the redaction helpers.
func TestDiagnosticsDigestAndShape(t *testing.T) {
	if digestShort("") != "" {
		t.Fatal("empty digest must stay empty")
	}
	if digestShort("a") == digestShort("b") || len(digestShort("a")) != 16 {
		t.Fatal("digest must be a stable 16-hex prefix")
	}
	shape := bodyShape([]byte(`{"model":"x","input":[],"stream":true}`))
	keys, ok := shape["keys"].([]string)
	if !ok || len(keys) != 3 || keys[0] != "input" || keys[1] != "model" || keys[2] != "stream" {
		t.Fatalf("shape keys wrong: %#v", shape)
	}
	if int(shape["length"].(int)) != 38 {
		t.Fatalf("shape length wrong: %#v", shape)
	}
	types := diagnosticsEventTypes([]byte("data: {\"type\":\"response.created\"}\ndata: {\"type\":\"x\"}\n"), 4)
	if len(types) != 2 || types[0] != "response.created" {
		t.Fatalf("event types wrong: %#v", types)
	}
	trimmed := diagnosticsErrorKind(`do: Post "https://chatgpt.com/backend-api/codex/responses": EOF`)
	if strings.Contains(trimmed, "chatgpt.com") || !strings.Contains(trimmed, "EOF") {
		t.Fatalf("error kind leaked a host: %q", trimmed)
	}
}

// TestUsageTokensExtractionAndRecord covers numeric token accounting from
// response payloads: extraction from SSE events, non-streaming bodies, and
// persistence onto the matching audit record (numbers only).
func TestUsageTokensExtractionAndRecord(t *testing.T) {
	sse := []byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":120,\"output_tokens\":45,\"total_tokens\":165,\"output_tokens_details\":{\"reasoning_tokens\":30}}}}\n")
	tokens := probeUsageTokens(sse)
	if tokens["input"] != 120 || tokens["output"] != 45 || tokens["total"] != 165 || tokens["reasoning"] != 30 {
		t.Fatalf("sse tokens wrong: %#v", tokens)
	}
	body := []byte(`{"usage":{"input_tokens":9,"output_tokens":1,"total_tokens":10}}`)
	if direct := probeUsageTokens(body); direct["input"] != 9 || direct["output"] != 1 {
		t.Fatalf("body tokens wrong: %#v", direct)
	}
	if probeUsageTokens([]byte(`{"type":"response.created"}`)) != nil {
		t.Fatal("payload without usage must return nil")
	}

	history = auditState{}
	history.records = []auditRecord{{RequestID: "usage-1", Model: "gpt-6-astra"}}
	history.observeResponseTokens("usage-1", "", tokens, false)
	record := history.snapshot()["records"].([]auditRecord)[0]
	if record.ResponseTokensInput == nil || *record.ResponseTokensInput != 120 ||
		record.ResponseTokensReasoning == nil || *record.ResponseTokensReasoning != 30 {
		t.Fatalf("tokens not recorded: %+v", record)
	}
	raw, err := json.Marshal(record)
	if err != nil || !bytes.Contains(raw, []byte(`"response_tokens_reasoning":30`)) {
		t.Fatalf("tokens not serialized: %v %s", err, raw)
	}
}
