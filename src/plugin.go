package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	pluginID                   = "oai-adversarial-plugin"
	legacyPluginID             = "timezone-override"
	pluginVersion              = "1.5.63"
	historyLimit               = 200
	schemaVersion              = 6
	streamChunkHeaderInitIndex = -1
)

//go:embed web/index.html
var dashboard []byte

type interceptRequest struct {
	Metadata       map[string]any
	RequestID      string
	TraceID        string
	SourceFormat   string
	ToFormat       string
	Model          string
	RequestedModel string
	Stream         bool
	Headers        http.Header
	Body           []byte
}

type interceptResponse struct {
	ClearHeaders    []string    `json:"ClearHeaders,omitempty"`
	Body            []byte      `json:"Body,omitempty"`
	Headers         http.Header `json:"Headers,omitempty"`
	Terminate       bool        `json:"Terminate,omitempty"`
	StatusCode      int         `json:"StatusCode,omitempty"`
	ResponseHeaders http.Header `json:"ResponseHeaders,omitempty"`
	ResponseBody    []byte      `json:"ResponseBody,omitempty"`
}

// responseInterceptRequest mirrors the successful non-streaming execution
// response handed to response.intercept_after.
type responseInterceptRequest struct {
	RequestID       string
	SourceFormat    string
	Model           string
	RequestedModel  string
	Stream          bool
	RequestHeaders  http.Header
	ResponseHeaders http.Header
	OriginalRequest []byte
	RequestBody     []byte
	Body            []byte
	StatusCode      int
	Metadata        map[string]any
}

// streamChunkInterceptRequest mirrors one successful stream chunk handed to
// response.intercept_stream_chunk.
type streamChunkInterceptRequest struct {
	RequestID       string
	SourceFormat    string
	Model           string
	RequestedModel  string
	RequestHeaders  http.Header
	ResponseHeaders http.Header
	OriginalRequest []byte
	RequestBody     []byte
	Body            []byte
	HistoryChunks   [][]byte
	ChunkIndex      int
	Metadata        map[string]any
}

// webSocketResponseEvent mirrors one upstream websocket response event handed
// to websocket.response_event.
type webSocketResponseEvent struct {
	RequestID      string
	TraceID        string
	SourceFormat   string
	Model          string
	RequestedModel string
	Provider       string
	AuthID         string
	AuthLabel      string
	AuthType       string
	EventType      string
	Payload        []byte
	Metadata       map[string]any
}

// responseInterceptOutput is the modification envelope for the response-side
// interceptors: mentioned headers are replaced, everything else is preserved.
type responseInterceptOutput struct {
	Headers      http.Header `json:"Headers,omitempty"`
	Body         []byte      `json:"Body,omitempty"`
	ClearHeaders []string    `json:"ClearHeaders,omitempty"`
}

type auditRecord struct {
	AccountScope     string `json:"-"`
	AuthBinding      string `json:"-"`
	Account          string `json:"account,omitempty"`
	AccountEmail     string `json:"account_email,omitempty"`
	RequestKind      string `json:"request_kind,omitempty"`
	EvaluationID     string `json:"evaluation_id,omitempty"`
	EvaluationKind   string `json:"evaluation_kind,omitempty"`
	EvaluationSample int    `json:"evaluation_sample,omitempty"`
	EvaluationError  string `json:"evaluation_error,omitempty"`
	TimingSource     string `json:"timing_source,omitempty"`
	conversion
	RequestID                       string `json:"request_id"`
	TraceID                         string `json:"trace_id,omitempty"`
	Model                           string `json:"model"`
	RequestedModel                  string `json:"requested_model,omitempty"`
	Time                            string `json:"time"`
	UpstreamModel                   string `json:"upstream_model,omitempty"`
	ModelChecked                    bool   `json:"model_checked"`
	ModelMismatch                   bool   `json:"model_mismatch"`
	DetectionExempt                 bool   `json:"detection_exempt,omitempty"`
	TurnStateLength                 int    `json:"turn_state_length"`
	TurnStateSource                 string `json:"turn_state_source,omitempty"`
	TurnStatePreview                string `json:"turn_state_preview,omitempty"`
	TurnStateValue                  string `json:"turn_state_value,omitempty"`
	TurnStateTruncated              bool   `json:"turn_state_truncated,omitempty"`
	TurnStateOverride               string `json:"turn_state_override,omitempty"`
	TurnStateInjectedLength         int    `json:"turn_state_injected_length,omitempty"`
	TurnStateOriginalLength         *int   `json:"turn_state_original_length,omitempty"`
	TurnStateResponseOriginalLength *int   `json:"turn_state_response_original_length,omitempty"`
	TurnStateResponseInjectedLength int    `json:"turn_state_response_injected_length,omitempty"`
	TurnStateResponseStatus         string `json:"turn_state_response_status,omitempty"`
	// Token accounting observed in the response stream (numbers only, no
	// text): used to quantify a suspected degraded tier.
	ResponseTokensInput     *int     `json:"response_tokens_input,omitempty"`
	ResponseTokensOutput    *int     `json:"response_tokens_output,omitempty"`
	ResponseTokensReasoning *int     `json:"response_tokens_reasoning,omitempty"`
	ResponseTokensTotal     *int     `json:"response_tokens_total,omitempty"`
	ResponseTokensComplete  bool     `json:"response_tokens_complete,omitempty"`
	RequestLatencyMS        *float64 `json:"request_latency_ms,omitempty"`
	FirstOutputLatencyMS    *float64 `json:"first_output_latency_ms,omitempty"`
	RequestOutcome          string   `json:"request_outcome,omitempty"`
	ConcurrencyState        string   `json:"concurrency_state,omitempty"`
	ConcurrencyLimit        *int     `json:"concurrency_limit,omitempty"`
	ConcurrencyRerouted     bool     `json:"concurrency_rerouted,omitempty"`
	OutputTPS               *float64 `json:"output_tps,omitempty"`
	// In-flight evidence is never restored from snapshots or exposed by the API.
	responseTicket       string
	responseModel        string
	performanceAttemptAt time.Time
	firstOutputAt        time.Time
	DegradedRejected     bool `json:"degraded_rejected,omitempty"`
}

type auditState struct {
	mu       sync.Mutex
	records  []auditRecord
	total    uint64
	inserted uint64
	replaced uint64
}

var history auditState

type managementRequest struct {
	Method string
	Path   string
	Body   []byte
}

type managementResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

func handleMethod(method string, raw []byte) (any, error) {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		ensurePersistence()
		configureTurnStateOverrideFromLifecycle(raw)
		accountConcurrency.activateWeekly()
		startStateMirror()
		return map[string]any{
			"schema_version": schemaVersion,
			"metadata": map[string]any{
				"Name": "O/对抗插件", "Version": pluginVersion,
				"Author": "FlashyyL / AgentEase", "ConfigFields": visualConfigFields(),
				"GitHubRepository": "https://github.com/FlashyyL/oai-adversarial-plugin",
			},
			"capabilities": map[string]bool{
				"request_interceptor":         true,
				"request_lifecycle_plugin":    true,
				"management_api":              true,
				"response_interceptor":        true,
				"response_stream_interceptor": true,
				"websocket_response_observer": true,
				"scheduler":                   true,
				"usage_plugin":                true,
			},
		}, nil
	case "plugin.quiesce":
		return struct{}{}, nil
	case "request.intercept_before":
		return concurrencyBefore(raw)
	case "request.intercept_after":
		return intercept(raw)
	case "request.complete":
		return struct{}{}, completeRequest(raw)
	case "response.intercept_after":
		return interceptNonStreamingResponse(raw)
	case "response.intercept_stream_chunk":
		return interceptStreamChunk(raw)
	case "websocket.response_event":
		return observeWebSocketEvent(raw)
	case "scheduler.pick":
		return pickAccountForRequest(raw)
	case "usage.handle":
		return struct{}{}, observeAccountUsage(raw)
	case "management.register":
		return map[string]any{
			"routes": []map[string]string{
				{"Method": "GET", "Path": "/" + pluginID + "/requests"},
				{"Method": "GET", "Path": "/" + pluginID + "/account-concurrency"},
				{"Method": "POST", "Path": "/" + pluginID + "/account-concurrency"},
				{"Method": "POST", "Path": "/" + pluginID + "/probe-control"},
				{"Method": "GET", "Path": "/" + pluginID + "/eval"},
				{"Method": "POST", "Path": "/" + pluginID + "/eval-run"},
				{"Method": "POST", "Path": "/" + pluginID + "/eval-cancel"},
				{"Method": "POST", "Path": "/" + pluginID + "/eval-detail"},
				// Existing diagnostic clients can keep using the old API paths.
				{"Method": "GET", "Path": "/" + legacyPluginID + "/requests"},
				{"Method": "POST", "Path": "/" + legacyPluginID + "/probe-control"},
			},
			"resources": []map[string]string{{
				"Path": "/status", "Menu": "O/对抗插件",
				"Description": "查看请求的原时区、替换结果、上游模型一致性及 X-Codex-Turn-State 观测。",
			}},
		}, nil
	case "management.handle":
		return management(raw)
	case "plugin.shutdown":
		stopStateMirror()
		probeTrackShutdown()
		accountConcurrency.stopWeekly()
		closePersistence()
		return struct{}{}, nil
	default:
		return nil, fmt.Errorf("unsupported plugin method: %s", method)
	}
}

func intercept(raw []byte) (response interceptResponse, err error) {
	var req interceptRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return interceptResponse{}, fmt.Errorf("decode interception: %w", err)
	}
	defer func() { response = stripConcurrencyHeader(req.Headers, response) }()
	if guarded := concurrencyAfter(req); guarded.Terminate {
		return guarded, nil
	}
	if req.ToFormat != "codex" {
		return interceptResponse{}, nil
	}
	attemptAt := time.Now()
	scope, accountEmail := selectedAccountInfo(req.Metadata, req.Headers)
	authID, _ := req.Metadata["selected_auth_id"].(string)
	// Degraded-model rejection: when the switch is on and the request targets
	// a model with business degradation evidence, or probe evidence without
	// a usable baseline, terminate with 403. A failing prefetch must not
	// interrupt traffic still protected by the active or successor value.
	if message := scopedRejectMessage(scope, req.Model, req.RequestedModel); message != "" {
		history.record(auditRecord{
			AccountScope: scope, AuthBinding: accountScope(authID),
			AccountEmail: accountEmail,
			Account:      targetAccount(scopedTarget(scope, req.Model)),
			RequestID:    req.RequestID, TraceID: req.TraceID,
			Model: req.Model, RequestedModel: req.RequestedModel,
			Time:                 time.Now().UTC().Format(time.RFC3339Nano),
			DegradedRejected:     true,
			performanceAttemptAt: attemptAt,
			// The request never reaches normalization, so the conversion keeps
			// empty slices (never nil) - a nil slice marshals as JSON null and
			// the dashboard expects arrays.
			conversion: conversion{Target: currentTimezone(), Original: []string{}, Paths: []string{}},
		})
		payload, _ := json.Marshal(map[string]any{"error": map[string]string{
			"type": "degraded_model_rejected", "message": message,
		}})
		return interceptResponse{
			Terminate: true, StatusCode: http.StatusForbidden,
			ResponseHeaders: http.Header{"Content-Type": {"application/json; charset=utf-8"}},
			ResponseBody:    payload,
		}, nil
	}
	body, result, err := normalizeRequest(req.Body, req.SourceFormat)
	if err != nil {
		payload, _ := json.Marshal(map[string]any{"error": map[string]string{
			"type": "timezone_normalization_error", "message": err.Error(),
		}})
		return interceptResponse{
			Terminate: true, StatusCode: http.StatusBadRequest,
			ResponseHeaders: http.Header{"Content-Type": {"application/json"}}, ResponseBody: payload,
		}, nil
	}
	originalLength := len(headerValue(req.Headers, turnStateHeader))
	overrideHeaders, overrideStatus := applyScopedOverride(scope, req.Model, req.RequestedModel, req.Headers)
	injectedLength := 0
	if overrideHeaders != nil {
		injectedLength = len(overrideHeaders.Get(turnStateHeader))
	}
	history.record(auditRecord{
		AccountScope: scope, AuthBinding: accountScope(authID),
		AccountEmail: accountEmail,
		conversion:   result, Account: targetAccount(scopedTarget(scope, req.Model)),
		RequestID: req.RequestID, TraceID: req.TraceID,
		Model: req.Model, RequestedModel: req.RequestedModel,
		Time:              time.Now().UTC().Format(time.RFC3339Nano),
		TurnStateOverride: overrideStatus, TurnStateInjectedLength: injectedLength,
		TurnStateOriginalLength: &originalLength,
		performanceAttemptAt:    attemptAt,
	})
	history.observeTurnState(req.RequestID, headerValue(req.Headers, turnStateHeader), "request")
	diagnosticsEvent("request", map[string]any{
		"id":         digestShort(req.RequestID),
		"model":      req.Model,
		"requested":  req.RequestedModel,
		"format":     req.ToFormat,
		"headers":    diagnosticsHeaderNames(req.Headers),
		"state_in":   headerValue(req.Headers, turnStateHeader) != "",
		"state_orig": originalLength,
		"state_inj":  injectedLength,
		"override":   overrideStatus,
		"body":       bodyShape(req.Body),
	})
	response = interceptResponse{}
	if result.Action != "unchanged" {
		response.Body = body
	}
	response.Headers = overrideHeaders
	if overrideHeaders == nil && overrideStatus != "" && headerValue(req.Headers, turnStateHeader) != "" {
		response.ClearHeaders = []string{turnStateHeader}
		// CPA 7.3.6 merges the plugin chain into a full header map but drops
		// ClearHeaders on return. Keep an explicit empty replacement so its
		// second merge cannot resurrect the client's previous-account ticket.
		response.Headers = http.Header{turnStateHeader: {""}}
	}
	return response, nil
}

func (s *auditState) record(record auditRecord) {
	if state, limit, rerouted := accountConcurrency.state(record.RequestID); state != "" {
		record.ConcurrencyState, record.ConcurrencyLimit, record.ConcurrencyRerouted = state, limit, rerouted
	}
	defer markStateDirty()
	s.mu.Lock()
	defer s.mu.Unlock()
	if record.RequestID != "" {
		for i := range s.records {
			if s.records[i].RequestID != record.RequestID {
				continue
			}
			existing := &s.records[i]
			if record.ConcurrencyState != "" {
				existing.ConcurrencyState, existing.ConcurrencyLimit, existing.ConcurrencyRerouted = record.ConcurrencyState, record.ConcurrencyLimit, record.ConcurrencyRerouted
			}
			if !record.performanceAttemptAt.IsZero() {
				existing.resetPerformance(record.performanceAttemptAt)
			}
			if record.TurnStateOriginalLength != nil {
				existing.TurnStateResponseOriginalLength = nil
				existing.TurnStateResponseInjectedLength = 0
				existing.TurnStateResponseStatus = ""
				existing.responseTicket = ""
				existing.responseModel = ""
			}
			if record.AccountScope != existing.AccountScope || record.AuthBinding != existing.AuthBinding {
				*existing = record // A retry selected another account: do not retain prior account evidence.
				return
			}
			// Observed fields (upstream model, turn-state view, override status)
			// stay untouched unless the retry re-applied them; only the
			// request-level view is refreshed so a retry never resets it.
			if record.TurnStateOverride != "" {
				existing.TurnStateOverride = record.TurnStateOverride
				existing.TurnStateOriginalLength = record.TurnStateOriginalLength
				existing.TurnStateInjectedLength = record.TurnStateInjectedLength
			}
			if record.Action != "" || len(record.Original) > 0 || record.Model != "" {
				existing.conversion = record.conversion
				existing.Model = record.Model
				if record.RequestedModel != "" {
					existing.RequestedModel = record.RequestedModel
				}
				if record.TraceID != "" {
					existing.TraceID = record.TraceID
				}
				existing.Time = record.Time
				if existing.ModelChecked {
					basis := strings.TrimSpace(existing.Model)
					if basis == "" {
						basis = strings.TrimSpace(existing.RequestedModel)
					}
					existing.ModelMismatch = basis != "" && !strings.EqualFold(basis, existing.UpstreamModel)
				}
			}
			return
		}
	}
	s.total++
	if record.Action == "inserted" {
		s.inserted++
	}
	if record.Action == "replaced" {
		s.replaced++
	}
	if len(s.records) == historyLimit {
		copy(s.records, s.records[1:])
		s.records[len(s.records)-1] = record
		return
	}
	s.records = append(s.records, record)
}

func (s *auditState) snapshot() map[string]any {
	probeTrack.syncProbeAccounts()
	probeTrack.mu.Lock()
	emails := auditEmails(probeTrack.accounts)
	probeTrack.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]auditRecord, len(s.records))
	mismatches := 0
	overridden := 0
	for i := range s.records {
		record := s.records[i]
		record.RequestOutcome = performanceOutcome(record.RequestOutcome)
		if record.RequestOutcome != "" && record.ConcurrencyState != "" {
			record.ConcurrencyState = record.RequestOutcome
		}
		if state, limit, rerouted := accountConcurrency.state(record.RequestID); state != "" {
			record.ConcurrencyState, record.ConcurrencyLimit, record.ConcurrencyRerouted = state, limit, rerouted
		} else if record.ConcurrencyState == "running" || record.ConcurrencyState == "reserved" {
			record.ConcurrencyState = "unknown"
		}
		if record.EvaluationError != "" {
			record.EvaluationError = safeEvalError(record.EvaluationError)
		}
		record.OutputTPS = requestTPS(record)
		record.AccountEmail = emails.email(record)
		if s.records[i].AccountEmail == "" && record.AccountEmail != "" {
			s.records[i].AccountEmail = record.AccountEmail
			markStateDirty()
		}
		// Preserve the historic observations, but publish today's policy even
		// for records restored from snapshots predating model exemptions.
		record.DetectionExempt = record.RequestKind == "evaluation" || !degradationDetectionEnabled(record.Model, record.RequestedModel)
		if record.DetectionExempt {
			record.ModelChecked = false
			record.ModelMismatch = false
		}
		// Defensive: legacy records may carry nil slices from snapshots;
		// the dashboard parses these as arrays.
		if record.Original == nil {
			record.Original = []string{}
		}
		if record.Paths == nil {
			record.Paths = []string{}
		}
		records[len(s.records)-1-i] = record
		if record.ModelMismatch && !record.DetectionExempt {
			mismatches++
		}
		if s.records[i].TurnStateOverride == "applied" {
			overridden++
		}
	}
	return map[string]any{
		"plugin": pluginID, "version": pluginVersion, "target": currentTimezone(),
		"limit": historyLimit, "total": s.total, "inserted": s.inserted,
		"replaced": s.replaced, "mismatches": mismatches, "overridden": overridden,
		"turn_state_override": turnStateOverrideSummary(),
		"account_routing":     accountRoutingSummary(),
		"records":             records,
	}
}

func management(raw []byte) (managementResponse, error) {
	var req managementRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return managementResponse{}, fmt.Errorf("decode management request: %w", err)
	}
	switch {
	case strings.HasSuffix(req.Path, "/account-concurrency"):
		if req.Method != http.MethodGet && req.Method != http.MethodPost {
			return managementResponse{StatusCode: http.StatusMethodNotAllowed}, nil
		}
		return concurrencyManagement(req.Body, req.Method == http.MethodPost), nil
	case strings.HasSuffix(req.Path, "/eval"):
		if req.Method != http.MethodGet {
			return managementResponse{StatusCode: http.StatusMethodNotAllowed}, nil
		}
		return probeTrack.evalStateResponse(), nil
	case strings.HasSuffix(req.Path, "/eval-run"):
		if req.Method != http.MethodPost {
			return managementResponse{StatusCode: http.StatusMethodNotAllowed}, nil
		}
		return probeTrack.evalRunResponse(req.Body), nil
	case strings.HasSuffix(req.Path, "/eval-cancel"):
		if req.Method != http.MethodPost {
			return managementResponse{StatusCode: http.StatusMethodNotAllowed}, nil
		}
		return probeTrack.evalCancelResponse(req.Body), nil
	case strings.HasSuffix(req.Path, "/eval-detail"):
		if req.Method != http.MethodPost {
			return managementResponse{StatusCode: http.StatusMethodNotAllowed}, nil
		}
		return probeTrack.evalDetailResponse(req.Body), nil
	case strings.HasSuffix(req.Path, "/status"):
		if req.Method != http.MethodGet {
			return managementResponse{StatusCode: http.StatusMethodNotAllowed}, nil
		}
		return managementResponse{
			StatusCode: http.StatusOK,
			Headers:    http.Header{"Content-Type": {"text/html; charset=utf-8"}, "Cache-Control": {"no-store"}},
			Body:       dashboard,
		}, nil
	case strings.HasSuffix(req.Path, "/requests"):
		if req.Method != http.MethodGet {
			return managementResponse{StatusCode: http.StatusMethodNotAllowed}, nil
		}
		body, err := json.Marshal(history.snapshot())
		return managementResponse{
			StatusCode: http.StatusOK,
			Headers:    http.Header{"Content-Type": {"application/json; charset=utf-8"}, "Cache-Control": {"no-store"}},
			Body:       body,
		}, err
	case strings.HasSuffix(req.Path, "/probe-control"):
		if req.Method != http.MethodPost {
			return managementResponse{StatusCode: http.StatusMethodNotAllowed}, nil
		}
		return probeControl(req.Body)
	default:
		return managementResponse{StatusCode: http.StatusNotFound}, nil
	}
}

// probeControl handles POST /oai-adversarial-plugin/probe-control and its legacy alias:
//
//	{"model": "gpt-5.6-luna", "action": "pause"|"resume"|"probe-model"}
//	{"action": "reject-degraded", "enabled": true|false}
//	{"action": "turn-state-inject", "enabled": true|false}
//	{"action": "diagnostics-journal", "enabled": true|false}
//	{"action": "start-round"}   # start one user-initiated probe round
//	{"action": "stop-current"}  # cancel the batch, preserve prefetch mode
//	{"action": "stop-all"}      # cancel all tasks and halt prefetch
//	{"action": "stop-round"}    # legacy alias of stop-all
//
// Pause removes the model from rounds; resume clears the stale annotation
// and probes that model once in the background; probe-model starts one
// single-model probe through the same FIFO worker. Neither re-arms prefetch.
func probeControl(body []byte) (managementResponse, error) {
	skipped := false
	var req struct {
		Model     string    `json:"model"`
		Target    string    `json:"target"`
		Proxy     string    `json:"proxy"`
		ID        string    `json:"id"`
		Action    string    `json:"action"`
		Enabled   *bool     `json:"enabled"`
		Minutes   *int      `json:"minutes"`
		Seconds   *int      `json:"seconds"`
		Timezone  *string   `json:"timezone"`
		StartHour *int      `json:"start_hour"`
		EndHour   *int      `json:"end_hour"`
		Exit      *exitEdit `json:"exit"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return jsonErrorResponse(http.StatusBadRequest, "无法解析控制请求体："+err.Error()), nil
		}
	}
	switch action := strings.ToLower(strings.TrimSpace(req.Action)); action {
	case "target-timezone":
		if req.Timezone == nil {
			return jsonErrorResponse(http.StatusBadRequest, "缺少 timezone 字段"), nil
		}
		if err := probeTrack.setTargetTimezone(*req.Timezone); err != nil {
			return settingsErrorResponse(err)
		}
	case "sleep-hours":
		if req.StartHour == nil || req.EndHour == nil {
			return jsonErrorResponse(http.StatusBadRequest, "缺少 start_hour 或 end_hour 字段"), nil
		}
		if err := probeTrack.setSleepHours(probeSleepHours{Start: *req.StartHour, End: *req.EndHour}); err != nil {
			return settingsErrorResponse(err)
		}
	case "prefetch-window":
		if req.Minutes == nil {
			return jsonErrorResponse(http.StatusBadRequest, "缺少 minutes 字段"), nil
		}
		if err := probeTrack.setPrefetchMinutes(*req.Minutes); err != nil {
			return settingsErrorResponse(err)
		}
	case "probe-interval":
		if req.Seconds == nil {
			return jsonErrorResponse(http.StatusBadRequest, "缺少 seconds 字段"), nil
		}
		if err := probeTrack.setProbeIntervalSeconds(*req.Seconds); err != nil {
			return settingsErrorResponse(err)
		}
	case "save-exit":
		if req.Exit == nil {
			return jsonErrorResponse(http.StatusBadRequest, "缺少 exit 字段"), nil
		}
		if err := probeTrack.saveExit(*req.Exit); err != nil {
			return settingsErrorResponse(err)
		}
	case "pause", "resume":
		model := strings.TrimSpace(req.Model)
		if model == "" {
			return jsonErrorResponse(http.StatusBadRequest, "缺少 model 字段"), nil
		}
		if !degradationDetectionEnabled(model, "") {
			skipped = true
			break
		}
		targets, err := probeTrack.resolveTargets(model, req.Target)
		if err != nil {
			return jsonErrorResponse(http.StatusConflict, err.Error()), nil
		}
		for _, target := range targets {
			if !probeTrack.setModelPaused(target, action == "pause") {
				return jsonErrorResponse(http.StatusConflict, "正在停止，或该模型已有探测任务，请等待状态刷新"), nil
			}
		}
	case "probe-model":
		model := strings.TrimSpace(req.Model)
		if model == "" {
			return jsonErrorResponse(http.StatusBadRequest, "缺少 model 字段"), nil
		}
		if !degradationDetectionEnabled(model, "") {
			skipped = true
			break
		}
		targets, err := probeTrack.resolveTargets(model, req.Target)
		if err != nil {
			return jsonErrorResponse(http.StatusConflict, err.Error()), nil
		}
		for _, target := range targets {
			if !probeTrack.probeModelAsync(target) {
				return jsonErrorResponse(http.StatusConflict, "探测未就绪，或该模型已暂停/已有任务，请检查当前状态"), nil
			}
		}
	case "reject-degraded":
		if req.Enabled == nil {
			return jsonErrorResponse(http.StatusBadRequest, "缺少 enabled 字段"), nil
		}
		probeTrack.setRejectDegraded(*req.Enabled)
	case "turn-state-inject":
		if req.Enabled == nil {
			return jsonErrorResponse(http.StatusBadRequest, "缺少 enabled 字段"), nil
		}
		probeTrack.setTurnStateInjectEnabled(*req.Enabled)
	case "diagnostics-journal":
		if req.Enabled == nil {
			return jsonErrorResponse(http.StatusBadRequest, "缺少 enabled 字段"), nil
		}
		if err := probeTrack.setDiagnosticsJournal(*req.Enabled); err != nil {
			return settingsErrorResponse(err)
		}
	case "reset-exit":
		// Clear the cool-down / scheduled rest of one egress (or every
		// egress when proxy is empty), returning them to rotation at once.
		proxy := ""
		if req.ID != "" || req.Proxy != "" {
			proxy = probeTrack.resolveExit(req.ID, req.Proxy)
			if proxy == "" {
				return jsonErrorResponse(http.StatusBadRequest, "未知出口，请刷新列表"), nil
			}
		}
		probeTrack.resetExit(proxy)
	case "exit-enabled":
		proxy := probeTrack.resolveExit(req.ID, strings.TrimSpace(req.Proxy))
		if proxy == "" || req.Enabled == nil {
			return jsonErrorResponse(http.StatusBadRequest, "缺少 proxy 或 enabled 字段"), nil
		}
		if !probeTrack.setExitEnabled(proxy, *req.Enabled) {
			return jsonErrorResponse(http.StatusBadRequest, "未知出口，请刷新列表"), nil
		}
	case "start-round":
		if !probeTrack.start() {
			return jsonErrorResponse(http.StatusConflict, "探测未启用、配置错误或正在停止，请检查当前状态"), nil
		}
	case "stop-current":
		probeTrack.stopCurrent()
	case "stop-round", "stop-all":
		probeTrack.stop()
	default:
		return jsonErrorResponse(http.StatusBadRequest, "不支持的操作："+action), nil
	}
	payload, _ := json.Marshal(map[string]any{
		"ok":                        true,
		"skipped":                   skipped,
		"paused":                    probeTrack.pausedModels(),
		"reject_degraded":           probeTrack.rejectDegradedEnabled(),
		"turn_state_inject_enabled": probeTrack.turnStateInjectEnabled(),
		"diagnostics_journal":       probeTrack.diagnosticsJournalEnabled(),
	})
	return managementResponse{
		StatusCode: http.StatusOK,
		Headers:    http.Header{"Content-Type": {"application/json; charset=utf-8"}, "Cache-Control": {"no-store"}},
		Body:       payload,
	}, nil
}

// jsonErrorResponse builds a JSON error payload for the control endpoint.
func jsonErrorResponse(status int, message string) managementResponse {
	payload, _ := json.Marshal(map[string]any{"ok": false, "error": message})
	return managementResponse{
		StatusCode: status,
		Headers:    http.Header{"Content-Type": {"application/json; charset=utf-8"}, "Cache-Control": {"no-store"}},
		Body:       payload,
	}
}
