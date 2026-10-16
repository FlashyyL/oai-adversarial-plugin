package main

import (
	"fmt"
	"strings"
	"time"
)

// host.model.execute_stream bypasses the incoming-request observer. Add one
// audit row per dispatched sample, with the actual timezone conversion but
// without ticket injection, learning or a business health verdict.
type evalRequestAudit struct {
	ID, Scope, AuthID, JobID, Kind, Model, Email string
	Sample                                       int
	Started                                      bool
	conversion
}

func (a *evalRequestAudit) start(at time.Time) {
	a.Started = true
	history.record(auditRecord{
		AccountScope: a.Scope, AuthBinding: accountScope(a.AuthID), Account: targetAccount(scopedTarget(a.Scope, a.Model)),
		AccountEmail: a.Email,
		RequestID:    a.ID, Model: a.Model, RequestedModel: a.Model, Time: at.UTC().Format(time.RFC3339Nano),
		RequestKind: "evaluation", EvaluationID: a.JobID, EvaluationKind: a.Kind, EvaluationSample: a.Sample,
		TimingSource: "evaluation-callback", DetectionExempt: true, conversion: a.conversion,
	})
}

func safeEvalError(code string) string {
	switch code {
	case "cancelled", "cancelled_or_timeout", "request_timeout", "timezone_normalization_failed", "host_execute_failed", "invalid_stream", "stream_read_failed", "invalid_stream_chunk",
		"stream_interrupted", "response_too_large", "stream_read_limit", "invalid_response_event", "invalid_response_item",
		"invalid_completed_response", "response_incomplete", "answer_too_large", "empty_answer", "invalid_usage", "account_unavailable_or_changed":
		return code
	}
	return "evaluation_failed"
}

func evalTokenCount(value *int64) *int {
	if value == nil || *value < 0 || *value > int64(int(^uint(0)>>1)) {
		return nil
	}
	count := int(*value)
	return &count
}

// Called outside the engine mutex after stream close and identity checking.
// No response text, prompt, token ticket or raw host error enters this ring.
func (a *evalRequestAudit) finish(out evalModelOutput, errorCode string) {
	if !a.Started {
		return
	}
	history.mu.Lock()
	defer history.mu.Unlock()
	for i := range history.records {
		r := &history.records[i]
		if r.RequestID != a.ID || r.AccountScope != a.Scope {
			continue
		}
		if r.RequestOutcome != "" {
			return
		}
		r.RequestOutcome = "succeeded"
		if errorCode != "" {
			r.EvaluationError = safeEvalError(errorCode)
			r.RequestOutcome = "failed"
			if strings.HasPrefix(r.EvaluationError, "cancelled") {
				r.RequestOutcome = "canceled"
			}
		}
		if !out.StartedAt.IsZero() && !out.CompletedAt.Before(out.StartedAt) {
			ms := float64(out.CompletedAt.Sub(out.StartedAt)) / float64(time.Millisecond)
			r.RequestLatencyMS = &ms
			if !out.FirstOutputAt.IsZero() && !out.FirstOutputAt.Before(out.StartedAt) && !out.FirstOutputAt.After(out.CompletedAt) {
				first := float64(out.FirstOutputAt.Sub(out.StartedAt)) / float64(time.Millisecond)
				r.FirstOutputLatencyMS = &first
			}
		}
		r.UpstreamModel = out.Model
		r.ResponseTokensInput, r.ResponseTokensOutput = evalTokenCount(out.Usage.Input), evalTokenCount(out.Usage.Output)
		r.ResponseTokensReasoning, r.ResponseTokensTotal = evalTokenCount(out.Usage.Reasoning), evalTokenCount(out.Usage.Total)
		tokens := map[string]int{}
		for name, value := range map[string]*int{"input": r.ResponseTokensInput, "output": r.ResponseTokensOutput, "reasoning": r.ResponseTokensReasoning, "total": r.ResponseTokensTotal} {
			if value != nil {
				tokens[name] = *value
			}
		}
		r.ResponseTokensComplete = errorCode == "" && validPerformanceTokens(tokens)
		markStateDirty()
		return
	}
}

func newEvalRequestAudit(job *evalJob, auth hostAuthEntry) *evalRequestAudit {
	sample := job.Record.Done + 1
	return &evalRequestAudit{ID: fmt.Sprintf("eval-%s-%d", job.Record.ID, sample), Scope: job.Record.Scope,
		AuthID: auth.ID, JobID: job.Record.ID, Kind: job.Record.Kind, Model: job.Record.Model, Sample: sample, Email: auth.Email}
}
