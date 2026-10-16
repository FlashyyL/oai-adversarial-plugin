package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func evalOutputFromChunks(t *testing.T, chunks [][]byte) (evalModelOutput, error) {
	t.Helper()
	previous := evalHostCall
	t.Cleanup(func() { evalHostCall = previous })
	next := 0
	evalHostCall = func(method string, _ any) (json.RawMessage, error) {
		switch method {
		case "host.model.execute_stream":
			return json.RawMessage(`{"stream_id":"fragmented-fixture"}`), nil
		case "host.model.stream_read":
			payload := chunks[next]
			next++
			return json.Marshal(map[string]any{"payload": payload, "done": next == len(chunks)})
		case "host.model.stream_close":
			return json.RawMessage(`{}`), nil
		}
		return nil, errors.New("unexpected callback")
	}
	return executeEvalModel(context.Background(), hostAuthEntry{ID: "synthetic"}, "gpt-6-astra", "synthetic prompt", "medium", nil)
}

func TestEvaluationSSEChunkGeometry(t *testing.T) {
	for _, field := range []string{"event: response.completed\n", "id: fixture\n", "retry: 1000\n", ": heartbeat\n"} {
		t.Run(field, func(t *testing.T) {
			out, err := evalOutputFromChunks(t, [][]byte{[]byte(`data: {"type":"response.created"}`), []byte(field), evalMockSSE("21", "gpt-6-astra")})
			if err != nil || out.Text != "21" {
				t.Fatalf("field boundary: %v", err)
			}
		})
	}
	t.Run("byte fragments and UTF-8", func(t *testing.T) {
		raw := evalMockSSE("21颗糖果", "gpt-6-astra")
		var chunks [][]byte
		for i := range raw {
			chunks = append(chunks, raw[i:i+1])
		}
		out, err := evalOutputFromChunks(t, chunks)
		if err != nil || out.Text != "21颗糖果" {
			t.Fatalf("byte fragmentation: %v", err)
		}
	})
	t.Run("literal data inside fragmented JSON string", func(t *testing.T) {
		raw := evalMockSSE("21 data: literal", "gpt-6-astra")
		offset := strings.Index(string(raw), "data: literal")
		out, err := evalOutputFromChunks(t, [][]byte{raw[:offset], raw[offset:]})
		if err != nil || out.Text != "21 data: literal" {
			t.Fatalf("literal field corrupted: %v", err)
		}
	})
	t.Run("empty data and CRLF", func(t *testing.T) {
		out, err := evalOutputFromChunks(t, [][]byte{[]byte("data: \r\n\r\n"), []byte(strings.ReplaceAll(string(evalMockSSE("21", "gpt-6-astra")), "\n", "\r\n"))})
		if err != nil || out.Text != "21" {
			t.Fatalf("empty data: %v", err)
		}
	})
}

func TestEvaluationIgnoresNonMessageItemContent(t *testing.T) {
	aux := []byte("data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"reasoning\",\"content\":{\"metadata\":true}}}\n\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"reasoning\",\"content\":\"not message text\"}}\n\n")
	out, err := parseEvalResponse(append(aux, evalMockSSE("21", "gpt-6-astra")...))
	if err != nil || out.Text != "21" {
		t.Fatalf("irrelevant item decoded as text: %v", err)
	}
}

func TestEvaluationMalformedOrIncompleteStreamFailsClosed(t *testing.T) {
	for _, raw := range [][]byte{
		[]byte("data: {broken}\n\n"),
		[]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\n"),
		[]byte("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"content\":\"PRIVATE_CANARY\"}]}}\n\n"),
	} {
		out, err := parseEvalResponse(raw)
		if err == nil || out.Text != "" || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatal("invalid stream accepted or leaked")
		}
	}
}
