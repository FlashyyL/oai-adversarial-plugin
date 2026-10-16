package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
)

const evalStreamLimit = 1 << 20
const evalTextLimit = 64000

var evalHostCall = callHost

type evalUsage struct {
	Input     *int64 `json:"input_tokens,omitempty"`
	Output    *int64 `json:"output_tokens,omitempty"`
	Reasoning *int64 `json:"reasoning_tokens,omitempty"`
	Total     *int64 `json:"total_tokens,omitempty"`
}

type evalModelOutput struct {
	Text                                  string
	Model                                 string
	Usage                                 evalUsage
	StartedAt, CompletedAt, FirstOutputAt time.Time
}

type evalOutputItem struct {
	Type    string          `json:"type"`
	Content json.RawMessage `json:"content"`
}

type evalResponseBody struct {
	Model  string           `json:"model"`
	Status string           `json:"status"`
	Error  json.RawMessage  `json:"error"`
	Output []evalOutputItem `json:"output"`
	Usage  struct {
		Input   *int64 `json:"input_tokens"`
		Output  *int64 `json:"output_tokens"`
		Total   *int64 `json:"total_tokens"`
		Details struct {
			Reasoning *int64 `json:"reasoning_tokens"`
		} `json:"output_tokens_details"`
	} `json:"usage"`
}

// This goes through CPA's pinned account route. It does not inject a ticket,
// change a health slot, or ask the host to choose another account on failure.
func executeEvalModel(ctx context.Context, auth hostAuthEntry, model, prompt, effort string, audit *evalRequestAudit) (out evalModelOutput, err error) {
	if ctx.Err() != nil {
		return evalModelOutput{}, evalContextError(ctx)
	}
	requestID := ""
	if audit != nil {
		requestID = audit.ID
	} else {
		requestID, _ = reservationToken()
		requestID = "eval-" + requestID
	}
	if err := accountConcurrency.internal(ctx, requestID, auth.ID, model, "evaluation", true); err != nil {
		if ctx.Err() != nil {
			return evalModelOutput{}, evalContextError(ctx)
		}
		return evalModelOutput{}, err
	}
	defer func() {
		outcome := "succeeded"
		if err != nil {
			outcome = "failed"
		}
		if ctx.Err() != nil {
			outcome = "canceled"
		}
		accountConcurrency.finish(requestCompletion{RequestID: requestID, Outcome: outcome})
	}()
	// Capacity waiting can span an auth disable/replacement. Revalidate the
	// original identity immediately before issuing the pinned host execution.
	if auth.Scope != "" {
		current, identityErr := probeTrack.evalAccount(auth.Scope)
		if identityErr != nil || current.ID != auth.ID || current.AuthIndex != auth.AuthIndex {
			return evalModelOutput{}, errors.New("account_unavailable_or_changed")
		}
	}
	if ctx.Err() != nil {
		return evalModelOutput{}, evalContextError(ctx)
	}
	body := map[string]any{"model": model, "stream": true, "store": false, "input": prompt}
	if effort != "" && effort != "none" {
		body["reasoning"] = map[string]string{"effort": effort}
	}
	encoded, _ := json.Marshal(body)
	encoded, timezoneResult, err := normalizeRequest(encoded, "openai-response")
	if err != nil {
		return evalModelOutput{}, errors.New("timezone_normalization_failed")
	}
	started := time.Now()
	var firstOutput time.Time
	defer func() { out.StartedAt, out.CompletedAt, out.FirstOutputAt = started, time.Now(), firstOutput }()
	if audit != nil {
		audit.conversion = timezoneResult
		audit.start(started)
	}
	raw, err := evalHostCall("host.model.execute_stream", map[string]any{
		"entry_protocol": "openai-response", "exit_protocol": "openai-response",
		"model": model, "stream": true, "body": encoded,
		"forced_provider": "codex", "auth_id": auth.ID,
	})
	if err != nil {
		if ctx.Err() != nil {
			return evalModelOutput{}, evalContextError(ctx)
		}
		return evalModelOutput{}, errors.New("host_execute_failed")
	}
	var stream struct {
		ID string `json:"stream_id"`
	}
	if json.Unmarshal(raw, &stream) != nil || stream.ID == "" || len(stream.ID) > 512 {
		return evalModelOutput{}, errors.New("invalid_stream")
	}
	var closeOnce sync.Once
	closeStream := func() {
		closeOnce.Do(func() { _, _ = evalHostCall("host.model.stream_close", map[string]string{"stream_id": stream.ID}) })
	}
	// A blocked stream read can be released by close. Before the host returns
	// an ID there is no cancellation handle; the single worker still waits,
	// so a timed-out initial callback can never overlap a second request.
	finished, watcherDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			closeStream()
		case <-finished:
		}
	}()
	defer func() { close(finished); closeStream(); <-watcherDone }()
	var events []byte
	observed := 0
	for reads := 0; reads < 10000; reads++ {
		if ctx.Err() != nil {
			return evalModelOutput{}, evalContextError(ctx)
		}
		raw, err := evalHostCall("host.model.stream_read", map[string]string{"stream_id": stream.ID})
		if err != nil {
			if ctx.Err() != nil {
				return evalModelOutput{}, evalContextError(ctx)
			}
			return evalModelOutput{}, errors.New("stream_read_failed")
		}
		var chunk struct {
			Payload []byte `json:"payload"`
			Error   string `json:"error"`
			Done    bool   `json:"done"`
		}
		if json.Unmarshal(raw, &chunk) != nil {
			return evalModelOutput{}, errors.New("invalid_stream_chunk")
		}
		if chunk.Error != "" {
			return evalModelOutput{}, errors.New("stream_interrupted")
		}
		if evalNeedsSeparator(events, chunk.Payload) {
			if len(events) == evalStreamLimit {
				return evalModelOutput{}, errors.New("response_too_large")
			}
			events = append(events, '\n')
		}
		if len(chunk.Payload) > evalStreamLimit-len(events) {
			return evalModelOutput{}, errors.New("response_too_large")
		}
		events = append(events, chunk.Payload...)
		if firstOutput.IsZero() {
			// Consume complete lines once. Keep a fragmented JSON line until it
			// is complete; never timestamp headers or response.completed as output.
			for observed < len(events) {
				end := bytes.IndexByte(events[observed:], '\n')
				if end < 0 {
					if hasResponseOutput(events[observed:]) {
						firstOutput = time.Now()
					}
					break
				}
				if hasResponseOutput(events[observed : observed+end]) {
					firstOutput = time.Now()
					break
				}
				observed += end + 1
			}
		}
		if chunk.Done {
			return parseEvalResponse(events)
		}
	}
	return evalModelOutput{}, errors.New("stream_read_limit")
}

func evalContextError(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errors.New("request_timeout")
	}
	return errors.New("cancelled")
}

// Host stream payloads may omit a separator before any SSE field, not just
// data:. Only repair a complete previous line so a literal "data:" inside a
// fragmented JSON string is not mistaken for a new event.
func evalNeedsSeparator(events, payload []byte) bool {
	if len(events) == 0 || len(payload) == 0 || bytes.HasSuffix(events, []byte("\n")) {
		return false
	}
	next := bytes.TrimLeft(payload, " \t")
	field := false
	for _, prefix := range []string{"data:", "event:", "id:", "retry:", ":"} {
		field = field || bytes.HasPrefix(next, []byte(prefix))
	}
	if !field {
		return false
	}
	line := bytes.TrimSpace(events[bytes.LastIndexByte(events, '\n')+1:])
	if data, ok := bytes.CutPrefix(line, []byte("data:")); ok {
		data = bytes.TrimSpace(data)
		return len(data) == 0 || json.Valid(data) || bytes.Equal(data, []byte("[DONE]"))
	}
	return bytes.HasPrefix(line, []byte("event:")) || bytes.HasPrefix(line, []byte("id:")) || bytes.HasPrefix(line, []byte("retry:")) || bytes.HasPrefix(line, []byte(":"))
}

func parseEvalResponse(events []byte) (evalModelOutput, error) {
	var final *evalResponseBody
	var items []evalOutputItem
	var data []string
	decode := func() error {
		if len(data) == 0 {
			return nil
		}
		raw := strings.Join(data, "\n")
		data = nil
		if strings.TrimSpace(raw) == "" || strings.TrimSpace(raw) == "[DONE]" {
			return nil
		}
		var event struct {
			Type     string          `json:"type"`
			Response json.RawMessage `json:"response"`
			Item     json.RawMessage `json:"item"`
		}
		if json.Unmarshal([]byte(raw), &event) != nil {
			return errors.New("invalid_response_event")
		}
		switch event.Type {
		case "response.output_item.done":
			var item evalOutputItem
			if json.Unmarshal(event.Item, &item) != nil {
				return errors.New("invalid_response_item")
			}
			if item.Type == "message" {
				items = append(items, item)
			}
		case "response.completed":
			var value evalResponseBody
			if json.Unmarshal(event.Response, &value) != nil {
				return errors.New("invalid_completed_response")
			}
			final = &value
		case "response.incomplete", "response.failed", "error":
			return errors.New("response_incomplete")
		}
		return nil
	}
	for _, line := range strings.Split(strings.ReplaceAll(string(events), "\r\n", "\n"), "\n") {
		if line == "" {
			if err := decode(); err != nil {
				return evalModelOutput{}, err
			}
			continue
		}
		if strings.HasPrefix(line, "event:") && len(data) > 0 {
			if err := decode(); err != nil {
				return evalModelOutput{}, err
			}
		}
		if value, ok := strings.CutPrefix(line, "data:"); ok {
			// Some host streams omit blank separators between complete JSON
			// lines. Do not join two complete events into invalid JSON.
			if len(data) > 0 && json.Valid([]byte(strings.Join(data, "\n"))) {
				if err := decode(); err != nil {
					return evalModelOutput{}, err
				}
			}
			data = append(data, strings.TrimPrefix(value, " "))
		}
	}
	if err := decode(); err != nil {
		return evalModelOutput{}, err
	}
	if final == nil || (final.Status != "" && final.Status != "completed") || (len(final.Error) > 0 && string(final.Error) != "null") {
		return evalModelOutput{}, errors.New("response_incomplete")
	}
	if len(final.Output) == 0 {
		final.Output = items
	}
	var text strings.Builder
	for _, item := range final.Output {
		if item.Type != "message" {
			continue
		}
		var content []struct{ Type, Text string }
		if json.Unmarshal(item.Content, &content) != nil {
			return evalModelOutput{}, errors.New("invalid_response_item")
		}
		for _, part := range content {
			if part.Type != "output_text" && part.Type != "text" {
				continue
			}
			if len(part.Text) > evalTextLimit-text.Len() {
				return evalModelOutput{}, errors.New("answer_too_large")
			}
			text.WriteString(part.Text)
		}
	}
	if strings.TrimSpace(text.String()) == "" {
		return evalModelOutput{}, errors.New("empty_answer")
	}
	if len(final.Model) > 200 || strings.ContainsAny(final.Model, "\r\n\x00") {
		final.Model = ""
	}
	usage := evalUsage{Input: final.Usage.Input, Output: final.Usage.Output, Reasoning: final.Usage.Details.Reasoning, Total: final.Usage.Total}
	for _, count := range []*int64{usage.Input, usage.Output, usage.Reasoning, usage.Total} {
		if count != nil && *count < 0 {
			return evalModelOutput{}, errors.New("invalid_usage")
		}
	}
	return evalModelOutput{Text: text.String(), Model: final.Model, Usage: usage}, nil
}
