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
	pluginID                   = "timezone-override"
	pluginVersion              = "1.5.37-agentease.1"
	historyLimit               = 200
	schemaVersion              = 6
	streamChunkHeaderInitIndex = -1
)

//go:embed web/index.html
var dashboard []byte

type interceptRequest struct {
	RequestID      string
	TraceID        string
	SourceFormat   string
	ToFormat       string
	Model          string
	RequestedModel string
	Headers        http.Header
	Body           []byte
}

type interceptResponse struct {
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
	conversion
	RequestID        string `json:"request_id"`
	TraceID          string `json:"trace_id,omitempty"`
	Model            string `json:"model"`
	RequestedModel   string `json:"requested_model,omitempty"`
	Time             string `json:"time"`
	UpstreamModel    string `json:"upstream_model,omitempty"`
	ModelChecked     bool   `json:"model_checked"`
	ModelMismatch    bool   `json:"model_mismatch"`
	DetectionExempt  bool   `json:"detection_exempt,omitempty"`
	TurnStateLength  int    `json:"turn_state_length"`
	TurnStateSource  string `json:"turn_state_source,omitempty"`
	TurnStatePreview string `json:"turn_state_preview,omitempty"`
	TurnStateValue   string `json:"turn_state_value,omitempty"`
	TurnStateTruncated bool `json:"turn_state_truncated,omitempty"`
	TurnStateOverride  string `json:"turn_state_override,omitempty"`
	TurnStateInjectedLength int `json:"turn_state_injected_length,omitempty"`
	DegradedRejected   bool   `json:"degraded_rejected,omitempty"`
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
		return map[string]any{
			"schema_version": schemaVersion,
			"metadata": map[string]any{
				"Name": "O/对抗插件", "Version": pluginVersion,
				"Author": "FlashyyL / AgentEase", "ConfigFields": visualConfigFields(),
				"GitHubRepository": "https://github.com/AgentEase/oai-adversarial-plugin",
			},
			"capabilities": map[string]bool{
				"request_interceptor":         true,
				"management_api":              true,
				"response_interceptor":        true,
				"response_stream_interceptor": true,
				"websocket_response_observer": true,
			},
		}, nil
	case "plugin.quiesce":
		return struct{}{}, nil
	case "request.intercept_before":
		return interceptResponse{}, nil
	case "request.intercept_after":
		return intercept(raw)
	case "response.intercept_after":
		return interceptNonStreamingResponse(raw)
	case "response.intercept_stream_chunk":
		return interceptStreamChunk(raw)
	case "websocket.response_event":
		return observeWebSocketEvent(raw)
	case "management.register":
		return map[string]any{
			"routes": []map[string]string{
				{"Method": "GET", "Path": "/timezone-override/requests"},
				{"Method": "POST", "Path": "/timezone-override/probe-control"},
			},
			"resources": []map[string]string{{
				"Path": "/status", "Menu": "O/对抗插件",
				"Description": "查看请求的原时区、替换结果、上游模型一致性及 X-Codex-Turn-State 观测。",
			}},
		}, nil
	case "management.handle":
		return management(raw)
	case "plugin.shutdown":
		probeTrackShutdown()
		closePersistence()
		return struct{}{}, nil
	default:
		return nil, fmt.Errorf("unsupported plugin method: %s", method)
	}
}

func intercept(raw []byte) (interceptResponse, error) {
	var req interceptRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return interceptResponse{}, fmt.Errorf("decode interception: %w", err)
	}
	if req.ToFormat != "codex" {
		return interceptResponse{}, nil
	}
	// Degraded-model rejection: when the switch is on and the request targets
	// a model with business degradation evidence, or probe evidence without
	// a usable baseline, terminate with 403. A failing prefetch must not
	// interrupt traffic still protected by the active or successor value.
	if message := degradedRejectMessage(req.Model, req.RequestedModel); message != "" {
		history.record(auditRecord{
			RequestID: req.RequestID, TraceID: req.TraceID,
			Model: req.Model, RequestedModel: req.RequestedModel,
			Time:             time.Now().UTC().Format(time.RFC3339Nano),
			DegradedRejected: true,
			// The request never reaches normalization, so the conversion keeps
			// empty slices (never nil) - a nil slice marshals as JSON null and
			// the dashboard expects arrays.
			conversion: conversion{Target: targetTimezone, Original: []string{}, Paths: []string{}},
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
	overrideHeaders, overrideStatus := applyTurnStateOverride(req.Model, req.RequestedModel, req.Headers)
	injectedLength := 0
	if overrideHeaders != nil {
		injectedLength = len(overrideHeaders.Get(turnStateHeader))
	}
	history.record(auditRecord{
		conversion: result, RequestID: req.RequestID, TraceID: req.TraceID,
		Model: req.Model, RequestedModel: req.RequestedModel,
		Time:              time.Now().UTC().Format(time.RFC3339Nano),
		TurnStateOverride: overrideStatus, TurnStateInjectedLength: injectedLength,
	})
	history.observeTurnState(req.RequestID, headerValue(req.Headers, turnStateHeader), "request")
	response := interceptResponse{}
	if result.Action != "unchanged" {
		response.Body = body
	}
	response.Headers = overrideHeaders
	return response, nil
}

func (s *auditState) record(record auditRecord) {
	defer markStateDirty()
	s.mu.Lock()
	defer s.mu.Unlock()
	if record.RequestID != "" {
		for i := range s.records {
			if s.records[i].RequestID != record.RequestID {
				continue
			}
			existing := &s.records[i]
			// Observed fields (upstream model, turn-state view, override status)
			// stay untouched unless the retry re-applied them; only the
			// request-level view is refreshed so a retry never resets it.
			if record.TurnStateOverride != "" {
				existing.TurnStateOverride = record.TurnStateOverride
				if record.TurnStateInjectedLength > 0 {
					existing.TurnStateInjectedLength = record.TurnStateInjectedLength
				}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]auditRecord, len(s.records))
	mismatches := 0
	overridden := 0
	for i := range s.records {
		record := s.records[i]
		// Preserve the historic observations, but publish today's policy even
		// for records restored from snapshots predating model exemptions.
		record.DetectionExempt = !degradationDetectionEnabled(record.Model, record.RequestedModel)
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
		"plugin": pluginID, "version": pluginVersion, "target": targetTimezone,
		"limit": historyLimit, "total": s.total, "inserted": s.inserted,
		"replaced": s.replaced, "mismatches": mismatches, "overridden": overridden,
		"turn_state_override": turnStateOverrideSummary(),
		"records":            records,
	}
}

func management(raw []byte) (managementResponse, error) {
	var req managementRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return managementResponse{}, fmt.Errorf("decode management request: %w", err)
	}
	switch {
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

// probeControl handles POST /timezone-override/probe-control:
//
//	{"model": "gpt-5.6-luna", "action": "pause"|"resume"|"probe-model"}
//	{"action": "reject-degraded", "enabled": true|false}
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
		Model   string `json:"model"`
		Proxy   string `json:"proxy"`
		ID      string `json:"id"`
		Action  string `json:"action"`
		Enabled *bool  `json:"enabled"`
		Minutes *int   `json:"minutes"`
		Seconds *int   `json:"seconds"`
		Exit    *exitEdit `json:"exit"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return jsonErrorResponse(http.StatusBadRequest, "无法解析控制请求体："+err.Error()), nil
		}
	}
	switch action := strings.ToLower(strings.TrimSpace(req.Action)); action {
	case "check-egress":
		result, status := probeTrack.checkEgress(strings.TrimSpace(req.ID), egressCheckURL)
		payload, _ := json.Marshal(map[string]any{"ok": status == http.StatusOK, "egress_check": result})
		return managementResponse{StatusCode: status, Body: payload,
			Headers: http.Header{"Content-Type": {"application/json; charset=utf-8"}, "Cache-Control": {"no-store"}}}, nil
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
		if !probeTrack.setModelPaused(model, action == "pause") {
			return jsonErrorResponse(http.StatusConflict, "正在停止，或该模型已有探测任务，请等待状态刷新"), nil
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
		if !probeTrack.probeModelAsync(model) {
			return jsonErrorResponse(http.StatusConflict, "探测未就绪，或该模型已暂停/已有任务，请检查当前状态"), nil
		}
	case "reject-degraded":
		if req.Enabled == nil {
			return jsonErrorResponse(http.StatusBadRequest, "缺少 enabled 字段"), nil
		}
		probeTrack.setRejectDegraded(*req.Enabled)
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
		"ok":              true,
		"skipped":         skipped,
		"paused":          probeTrack.pausedModels(),
		"reject_degraded": probeTrack.rejectDegradedEnabled(),
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
