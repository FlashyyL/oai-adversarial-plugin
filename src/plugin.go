package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	pluginID                   = "timezone-override"
	pluginVersion              = "1.5.48"
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
	Metadata       map[string]any
}

type interceptResponse struct {
	Body            []byte      `json:"Body,omitempty"`
	Headers         http.Header `json:"Headers,omitempty"`
	ClearHeaders    []string    `json:"ClearHeaders,omitempty"`
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
	AccountScope string `json:"-"`
	AuthBinding  string `json:"-"`
	Account      string `json:"account,omitempty"`
	AccountEmail string `json:"account_email,omitempty"`
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
	TurnStateProvenance             string `json:"turn_state_provenance,omitempty"`
	TurnStateOwner                  string `json:"turn_state_owner,omitempty"`
	TurnStateFingerprint            string `json:"turn_state_fingerprint,omitempty"`
	TurnStateOriginalLength         *int   `json:"turn_state_original_length,omitempty"`
	TurnStateResponseOriginalLength *int   `json:"turn_state_response_original_length,omitempty"`
	TurnStateResponseInjectedLength int    `json:"turn_state_response_injected_length,omitempty"`
	TurnStateResponseStatus         string `json:"turn_state_response_status,omitempty"`
	// In-flight evidence is never restored from snapshots or exposed by the API.
	responseTicket   string
	responseModel    string
	DegradedRejected bool   `json:"degraded_rejected,omitempty"`
	RejectionKind    string `json:"rejection_kind,omitempty"`
	QuotaStatus      string `json:"quota_status,omitempty"`
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
		return interceptResponse{}, nil
	case "request.intercept_after":
		return intercept(raw)
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
		stopStateMirror()
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
	scope := selectedAccount(req.Metadata, req.Headers)
	authID := selectedAuthID(req.Metadata)
	now := time.Now().UTC()
	accountModel := businessModelName(req.Model, req.RequestedModel)
	turnStateSessions.bindRequest(req.RequestID, authID, accountModel, now)
	if accountRouter.needsRenewal(authID, accountModel, now) {
		probeTrack.renewAccount(authID, accountModel, now)
	}
	clientState := headerValue(req.Headers, turnStateHeader)
	provenance := turnStateSessions.inspect(req.Headers, req.Body, req.Metadata, authID, accountModel, clientState, now)
	// A client-carried state may recover an account only when its owner is
	// already known to be the selected AuthID. Unknown states remain pass-through
	// evidence until a successful upstream response confirms their ownership.
	if !sessionGuardEnabled() || provenance.Kind == "same-account" {
		accountRouter.observeRequestState(authID, accountModel, clientState, now)
	}
	// Degraded-model rejection: when the switch is on and the request targets
	// a model with business degradation evidence, or probe evidence without
	// a usable baseline, terminate with 403. A failing prefetch must not
	// interrupt traffic still protected by the active or successor value.
	message := degradedRejectMessageForAccount(authID, req.Model, req.RequestedModel)
	if scope != "" || currentProbeConfig().Config.AccountMode != "" {
		message = scopedRejectMessage(scope, req.Model, req.RequestedModel)
	}
	rejectionKind, rejectionStatus := "degraded_model_rejected", http.StatusForbidden
	responseHeaders := http.Header{"Content-Type": {"application/json; charset=utf-8"}}
	if kind, until := accountRouter.quotaBlock(authID, accountModel, now); kind != "" {
		message = "该账号额度已耗尽或请求受限，等待冷却结束；当前没有选中可用替补账号。"
		rejectionKind, rejectionStatus = kind, http.StatusTooManyRequests
		responseHeaders.Set("Retry-After", strconv.Itoa(max(1, int(until.Sub(now).Seconds()))))
	} else if message != "" {
		accountRouter.mu.Lock()
		entry := accountRouter.health[accountHealthKey(authID, routingModelKey(accountModel))]
		accountRouter.mu.Unlock()
		if entry.State == "auth_error" {
			rejectionKind, rejectionStatus = "account_auth_error", http.StatusUnauthorized
		}
		if entry.State == "transient_failure" {
			rejectionKind, rejectionStatus = "account_temporarily_unavailable", http.StatusServiceUnavailable
		}
	}
	if message != "" {
		history.record(auditRecord{
			AccountScope: scope, AuthBinding: accountScope(authID),
			Account:   targetAccount(scopedTarget(scope, req.Model)),
			RequestID: req.RequestID, TraceID: req.TraceID,
			Model: req.Model, RequestedModel: req.RequestedModel,
			Time:                 time.Now().UTC().Format(time.RFC3339Nano),
			DegradedRejected:     rejectionKind == "degraded_model_rejected",
			RejectionKind:        rejectionKind,
			TurnStateProvenance:  provenance.Kind,
			TurnStateOwner:       publicAccountIDOrEmpty(provenance.OwnerAuthID),
			TurnStateFingerprint: provenance.Fingerprint,
			// The request never reaches normalization, so the conversion keeps
			// empty slices (never nil) - a nil slice marshals as JSON null and
			// the dashboard expects arrays.
			conversion: conversion{Target: currentTimezone(), Original: []string{}, Paths: []string{}},
		})
		payload, _ := json.Marshal(map[string]any{"error": map[string]string{
			"type": rejectionKind, "message": message,
		}})
		return interceptResponse{
			Terminate: true, StatusCode: rejectionStatus,
			ResponseHeaders: responseHeaders,
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
	overrideInput := req.Headers
	foreignState := provenance.Kind == "foreign-account" || provenance.Kind == "foreign-model"
	enforceForeign := foreignState && provenance.Mode == sessionGuardModeEnforce
	expiredClient := false
	if issued, ok := parseTurnStateTimestamp(clientState); ok && degradationDetectionEnabled(req.Model, req.RequestedModel) {
		expiredClient = !issued.Add(currentProbeConfig().Config.TTL).After(now)
	}
	if enforceForeign || expiredClient {
		overrideInput = req.Headers.Clone()
		overrideInput.Del(turnStateHeader)
	}
	overrideHeaders, overrideStatus := applyTurnStateOverrideForAccount(authID, req.Model, req.RequestedModel, overrideInput)
	if scope != "" || currentProbeConfig().Config.AccountMode != "" {
		overrideHeaders, overrideStatus = applyScopedOverride(scope, req.Model, req.RequestedModel, req.Headers)
	}
	clearHeaders := []string(nil)
	if enforceForeign || expiredClient {
		if overrideHeaders != nil && headerValue(overrideHeaders, turnStateHeader) != "" {
			overrideStatus = "session-foreign-replaced"
			if enforceForeign {
				turnStateSessions.noteEnforcement("replaced")
			}
		} else {
			overrideStatus = "session-foreign-stripped"
			clearHeaders = []string{turnStateHeader}
			// CPA 7.3's host can flatten ClearHeaders into a complete header
			// snapshot, which its executor then merges into the original map.
			// An explicit empty value survives that merge and prevents the
			// original client value from being reintroduced. Codex omits it
			// from the actual upstream request.
			overrideHeaders = http.Header{turnStateHeader: []string{""}}
			if enforceForeign {
				turnStateSessions.noteEnforcement("stripped")
			}
		}
		if expiredClient && !enforceForeign {
			if headerValue(overrideHeaders, turnStateHeader) == "" {
				overrideStatus = "expired-state-stripped"
			} else {
				overrideStatus = "expired-state-replaced"
			}
		}
	}
	injectedLength := 0
	if overrideHeaders != nil {
		injectedLength = len(overrideHeaders.Get(turnStateHeader))
	}
	finalState := clientState
	if len(clearHeaders) > 0 {
		finalState = ""
	}
	if value := headerValue(overrideHeaders, turnStateHeader); value != "" {
		finalState = value
	}
	if finalState != "" && (provenance.Kind == "same-account" || accountRouter.stateOwnedBy(authID, accountModel, finalState, now)) {
		turnStateSessions.noteOwned(req.Headers, req.Body, req.Metadata, authID, accountModel, finalState, "request-account-state", now)
	}
	history.record(auditRecord{
		AccountScope: scope, AuthBinding: accountScope(authID),
		conversion: result, Account: targetAccount(scopedTarget(scope, req.Model)),
		RequestID: req.RequestID, TraceID: req.TraceID,
		Model: req.Model, RequestedModel: req.RequestedModel,
		Time:              time.Now().UTC().Format(time.RFC3339Nano),
		TurnStateOverride: overrideStatus, TurnStateInjectedLength: injectedLength,
		TurnStateProvenance:     provenance.Kind,
		TurnStateOwner:          publicAccountIDOrEmpty(provenance.OwnerAuthID),
		TurnStateFingerprint:    provenance.Fingerprint,
		TurnStateOriginalLength: &originalLength,
	})
	history.observeTurnState(req.RequestID, headerValue(req.Headers, turnStateHeader), "request")
	response := interceptResponse{}
	if result.Action != "unchanged" {
		response.Body = body
	}
	response.Headers = overrideHeaders
	response.ClearHeaders = clearHeaders
	if (scope != "" || currentProbeConfig().Config.AccountMode != "") && overrideHeaders == nil && overrideStatus != "" && headerValue(req.Headers, turnStateHeader) != "" {
		response.ClearHeaders = []string{turnStateHeader}
		// CPA 7.3.6 merges the plugin chain into a full header map but drops
		// ClearHeaders on return. Keep an explicit empty replacement so its
		// second merge cannot resurrect the client's previous-account ticket.
		response.Headers = http.Header{turnStateHeader: {""}}
	}
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
			if record.TurnStateProvenance != "" {
				existing.TurnStateProvenance = record.TurnStateProvenance
				existing.TurnStateOwner = record.TurnStateOwner
				existing.TurnStateFingerprint = record.TurnStateFingerprint
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

func (s *auditState) observeSessionGuard(requestID string, decision turnStateProvenanceDecision, override string, injectedLength int) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" || (decision.Kind == "" && override == "") {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.records {
		if s.records[i].RequestID != requestID {
			continue
		}
		if decision.Kind != "" {
			foreign := decision.Kind == "foreign-account" || decision.Kind == "foreign-model"
			if s.records[i].TurnStateProvenance == "" || foreign {
				s.records[i].TurnStateProvenance = decision.Kind
				s.records[i].TurnStateOwner = publicAccountIDOrEmpty(decision.OwnerAuthID)
				s.records[i].TurnStateFingerprint = decision.Fingerprint
			}
		}
		if override != "" {
			s.records[i].TurnStateOverride = override
			s.records[i].TurnStateInjectedLength = injectedLength
		}
		markStateDirty()
		return
	}
}

func (s *auditState) snapshot() map[string]any {
	probeTrack.syncProbeAccounts()
	probeTrack.mu.Lock()
	emails := make(map[string]string, len(probeTrack.accounts))
	for scope, entry := range probeTrack.accounts {
		emails[scope] = entry.Email
	}
	probeTrack.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]auditRecord, len(s.records))
	mismatches := 0
	overridden := 0
	for i := range s.records {
		record := s.records[i]
		record.AccountEmail = emails[record.AccountScope]
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
