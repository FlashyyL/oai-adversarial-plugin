package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// RequestCompletion is correlated by the host's execution ID, never by a
// usage event's account/model/time heuristic. Raw errors are not retained.
type requestCompletion struct {
	RequestID      string
	TraceID        string
	Model          string
	RequestedModel string
	Stream         bool
	Outcome        string
	StartedAt      time.Time
	CompletedAt    time.Time
	Metadata       map[string]any
}

func completeRequest(raw []byte) error {
	var req requestCompletion
	if err := json.Unmarshal(raw, &req); err != nil {
		return fmt.Errorf("decode request completion: %w", err)
	}
	accountConcurrency.finish(req)
	history.observeCompletion(req)
	return nil
}

func completionOutcome(value string) bool {
	return value == "succeeded" || value == "failed" || value == "rejected" || value == "canceled"
}

func timingAuthMatches(r *auditRecord, metadata map[string]any) bool {
	id, _ := metadata["selected_auth_id"].(string)
	return id == "" || (r.AuthBinding != "" && accountScope(id) == r.AuthBinding)
}

// Called with the audit lock held. A fresh attempt must not reuse earlier
// response counts, completion status or first-output evidence, even on the
// same account. The unexported timestamps are deliberately not persisted.
func (r *auditRecord) resetPerformance(at time.Time) {
	r.performanceAttemptAt = at
	r.firstOutputAt = time.Time{}
	r.RequestLatencyMS = nil
	r.FirstOutputLatencyMS = nil
	r.RequestOutcome = ""
	r.ResponseTokensComplete = false
	r.ResponseTokensInput = nil
	r.ResponseTokensOutput = nil
	r.ResponseTokensReasoning = nil
	r.ResponseTokensTotal = nil
}

func (s *auditState) observeCompletion(req requestCompletion) {
	if req.RequestID == "" || !completionOutcome(req.Outcome) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.records {
		r := &s.records[i]
		if r.RequestID != req.RequestID || r.performanceAttemptAt.IsZero() || r.RequestOutcome != "" || !timingAuthMatches(r, req.Metadata) {
			continue
		}
		if req.TraceID != "" && r.TraceID != "" && req.TraceID != r.TraceID {
			return
		}
		if req.RequestedModel != "" && r.RequestedModel != "" && req.RequestedModel != r.RequestedModel {
			return
		}
		// Host completion times are authoritative. Missing times on older
		// hosts keep durations unknown; never fall back to callback arrival.
		// End-before-attempt rejects a delayed completion after a retry.
		if !req.CompletedAt.IsZero() && req.CompletedAt.Before(r.performanceAttemptAt) {
			return
		}
		r.RequestOutcome = req.Outcome
		if !req.StartedAt.IsZero() && !req.CompletedAt.IsZero() && !req.StartedAt.After(r.performanceAttemptAt) && !req.CompletedAt.Before(req.StartedAt) {
			ms := float64(req.CompletedAt.Sub(req.StartedAt)) / float64(time.Millisecond)
			r.RequestLatencyMS = &ms
			if req.Stream && !r.firstOutputAt.IsZero() && !r.firstOutputAt.Before(req.StartedAt) && !r.firstOutputAt.After(req.CompletedAt) {
				first := float64(r.firstOutputAt.Sub(req.StartedAt)) / float64(time.Millisecond)
				r.FirstOutputLatencyMS = &first
			}
		}
		markStateDirty()
		return
	}
}

// Inspect structured events but keep neither text nor tool arguments. Header
// init, role-only/empty deltas, metadata and completed usage are not output.
func hasResponseOutput(payload []byte) bool {
	if len(payload) == 0 || len(payload) > maxModelProbeSize {
		return false
	}
	for _, candidate := range performanceCandidates(payload) {
		var event struct {
			Type    string          `json:"type"`
			Delta   json.RawMessage `json:"delta"`
			Choices []struct {
				Delta struct {
					Content   string `json:"content"`
					Reasoning string `json:"reasoning_content"`
					Tools     []struct {
						Function struct {
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal(candidate, &event) != nil {
			continue
		}
		switch event.Type {
		case "response.output_text.delta", "response.reasoning_text.delta", "response.reasoning_summary_text.delta", "response.function_call_arguments.delta", "response.refusal.delta":
			var delta string
			if json.Unmarshal(event.Delta, &delta) == nil && delta != "" {
				return true
			}
		case "content_block_delta":
			var delta struct{ Type, Text, Thinking string }
			if json.Unmarshal(event.Delta, &delta) == nil && ((delta.Type == "text_delta" && delta.Text != "") || (delta.Type == "thinking_delta" && delta.Thinking != "")) {
				return true
			}
		}
		for _, choice := range event.Choices {
			if choice.Delta.Content != "" || choice.Delta.Reasoning != "" {
				return true
			}
			for _, tool := range choice.Delta.Tools {
				if tool.Function.Arguments != "" {
					return true
				}
			}
		}
	}
	return false
}

func (s *auditState) observeFirstOutput(requestID, scope string, metadata map[string]any, payload []byte, at time.Time) {
	if requestID == "" || !hasResponseOutput(payload) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.records {
		r := &s.records[i]
		if r.RequestID != requestID || r.AccountScope != scope || !timingAuthMatches(r, metadata) || r.performanceAttemptAt.IsZero() || r.RequestOutcome != "" || !r.firstOutputAt.IsZero() || at.Before(r.performanceAttemptAt) {
			continue
		}
		r.firstOutputAt = at
		return
	}
}

func completeTokenPayload(payload []byte, stream bool) bool {
	if len(payload) == 0 || len(payload) > maxModelProbeSize {
		return false
	}
	for _, candidate := range performanceCandidates(payload) {
		var event struct {
			Type   string          `json:"type"`
			Status string          `json:"status"`
			Usage  json.RawMessage `json:"usage"`
		}
		if json.Unmarshal(candidate, &event) != nil {
			continue
		}
		if event.Type == "response.completed" {
			return true
		}
		if !stream && (event.Type == "" || event.Type == "response") && len(event.Usage) > 0 && (event.Status == "" || event.Status == "completed") {
			return true
		}
	}
	return false
}

func validPerformanceTokens(tokens map[string]int) bool {
	output, ok := tokens["output"]
	if !ok || output < 0 {
		return false
	}
	for _, value := range tokens {
		if value < 0 {
			return false
		}
	}
	if total, ok := tokens["total"]; ok {
		if total < output {
			return false
		}
		if input, ok := tokens["input"]; ok && total-output < input {
			return false
		}
	}
	return tokens["reasoning"] <= output
}

// A host chunk may contain several SSE events. Performance must see output
// beyond the model observer's four-candidate limit and take final usage from
// the same completed event, not from an earlier partial usage in that chunk.
func performanceCandidates(payload []byte) [][]byte {
	trimmed := bytes.TrimSpace(payload)
	if json.Valid(trimmed) {
		return [][]byte{trimmed}
	}
	var candidates [][]byte
	for _, line := range bytes.Split(payload, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if value, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			line = bytes.TrimSpace(value)
		}
		if len(line) > 0 && line[0] == '{' && json.Valid(line) {
			candidates = append(candidates, line)
		}
	}
	return candidates
}

func performanceUsageTokens(payload []byte, stream bool) (map[string]int, bool) {
	tokens := probeUsageTokens(payload)
	complete := false
	if len(payload) > maxModelProbeSize {
		return tokens, complete
	}
	for _, candidate := range performanceCandidates(payload) {
		if !completeTokenPayload(candidate, stream) {
			continue
		}
		finalTokens := probeUsageTokens(candidate)
		if len(finalTokens) > 0 {
			tokens, complete = finalTokens, validPerformanceTokens(finalTokens)
		}
	}
	return tokens, complete
}

func requestTPS(r auditRecord) *float64 {
	if r.RequestOutcome != "succeeded" || !r.ResponseTokensComplete || r.RequestLatencyMS == nil || *r.RequestLatencyMS <= 0 || math.IsNaN(*r.RequestLatencyMS) || math.IsInf(*r.RequestLatencyMS, 0) || r.ResponseTokensOutput == nil || *r.ResponseTokensOutput < 0 {
		return nil
	}
	if r.ResponseTokensReasoning != nil && (*r.ResponseTokensReasoning < 0 || *r.ResponseTokensReasoning > *r.ResponseTokensOutput) {
		return nil
	}
	tokensPerSecond := float64(*r.ResponseTokensOutput) * 1000 / *r.RequestLatencyMS
	if math.IsNaN(tokensPerSecond) || math.IsInf(tokensPerSecond, 0) {
		return nil
	}
	return &tokensPerSecond
}

// Keep persisted outcome fields fixed even if a malformed older snapshot was
// hand edited. No raw failure text can escape into the request performance UI.
func performanceOutcome(value string) string {
	if completionOutcome(value) {
		return value
	}
	return ""
}
