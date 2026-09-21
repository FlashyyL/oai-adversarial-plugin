package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
	// Embed the IANA database so timezone validation works in minimal
	// containers that ship no system zoneinfo.
	_ "time/tzdata"

	"gopkg.in/yaml.v3"
)

// This file implements the observation extensions that run alongside the
// timezone normalization:
//
//  1. Upstream model consistency: every recorded request keeps the model CPA
//     executed; the response-side interceptors (non-streaming, streaming and
//     websocket) probe the model that the upstream actually served. The
//     dashboard flags the record when the two disagree (like sub2api shows
//     "upstream response: <model> / model mismatch").
//  2. X-Codex-Turn-State tracking and rewrite: the header value seen on the
//     request, downstream response or stream is recorded as length plus a
//     short preview (bounded full value from v1.2.0), and matching requests
//     receive a configured replacement value before they reach the upstream.
//
// Everything here must stay cheap: stream and websocket paths call into this
// file for every chunk or event of hot traffic.

const (
	turnStateHeader         = "X-Codex-Turn-State"
	turnStatePreviewLength  = 32
	turnStateValueLimit     = 4096
	maxModelProbeSize       = 1 << 20
	maxModelProbeCandidates = 4
)

// turnStateOverrideConfig is the plugin-config block that drives the
// request-side X-Codex-Turn-State rewrite. It is read from the
// plugins.configs.timezone-override subtree (key "turn-state-override"):
//
//	turn-state-override:
//	  enabled: true
//	  models: ["gpt-6-astra"]
//	  value: "gAAAAAB..."
//	  force: true
type turnStateOverrideConfig struct {
	AcceptedStateLengths        []int           `yaml:"accepted-state-lengths"`
	Enabled                     bool            `yaml:"enabled"`
	Models                      []string        `yaml:"models"`
	Value                       string          `yaml:"value"`
	Force                       bool            `yaml:"force"`
	SessionGuardMode            string          `yaml:"session-guard-mode"`
	SessionProvenanceTTLMinutes int             `yaml:"session-provenance-ttl-minutes"`
	Probe                       probeConfigYAML `yaml:"probe"`
}

// turnStateOverrideState is the active rewrite configuration plus the last
// configuration error (shown on the dashboard when the config is invalid).
type turnStateOverrideState struct {
	Config turnStateOverrideConfig
	Error  string
}

var turnStateOverride atomic.Value // *turnStateOverrideState

// currentTurnStateOverride returns the active configuration snapshot or nil.
func currentTurnStateOverride() *turnStateOverrideState {
	if state, ok := turnStateOverride.Load().(*turnStateOverrideState); ok {
		return state
	}
	return nil
}

// configureTurnStateOverride parses the plugin config YAML handed to
// plugin.register / plugin.reconfigure and activates the rewrite rules. The
// config YAML is the full plugins.configs.<id> subtree, so unknown keys
// (enabled, priority, store, ...) are ignored.
func configureTurnStateOverride(configYAML []byte) error {
	state := &turnStateOverrideState{}
	var root struct {
		Timezone          string                  `yaml:"timezone"`
		TurnStateOverride turnStateOverrideConfig `yaml:"turn-state-override"`
	}
	trimmed := bytes.TrimSpace(configYAML)
	if len(trimmed) > 0 {
		normalized, err := normalizePanelConfig(trimmed)
		if err != nil {
			turnStateOverride.Store(&turnStateOverrideState{Error: err.Error()})
			return err
		}
		trimmed = normalized
		if err := yaml.Unmarshal(trimmed, &root); err != nil {
			state = &turnStateOverrideState{Error: fmt.Sprintf("decode turn-state-override config: %v", err)}
			turnStateOverride.Store(state)
			return fmt.Errorf("decode turn-state-override config: %w", err)
		}
	}
	// Optional top-level target timezone; must be a valid IANA zone name.
	zone := strings.TrimSpace(root.Timezone)
	if zone == "" {
		zone = targetTimezone
	} else if _, err := time.LoadLocation(zone); err != nil {
		state = &turnStateOverrideState{Error: fmt.Sprintf("invalid timezone %q: %v", zone, err)}
		turnStateOverride.Store(state)
		return fmt.Errorf("invalid timezone %q: %w", zone, err)
	}
	config := root.TurnStateOverride
	config.Value = strings.TrimSpace(config.Value)
	config.SessionGuardMode = strings.ToLower(strings.TrimSpace(config.SessionGuardMode))
	if config.SessionGuardMode == "" {
		config.SessionGuardMode = sessionGuardModeObserve
	}
	if config.SessionGuardMode != sessionGuardModeOff && config.SessionGuardMode != sessionGuardModeObserve && config.SessionGuardMode != sessionGuardModeEnforce {
		state = &turnStateOverrideState{Config: config, Error: "turn-state-override.session-guard-mode must be off, observe or enforce"}
		turnStateOverride.Store(state)
		return fmt.Errorf("turn-state-override.session-guard-mode must be off, observe or enforce")
	}
	if config.SessionProvenanceTTLMinutes == 0 {
		config.SessionProvenanceTTLMinutes = int(defaultSessionProvenanceTTL / time.Minute)
	}
	if config.SessionProvenanceTTLMinutes < 1 || config.SessionProvenanceTTLMinutes > 1440 {
		state = &turnStateOverrideState{Config: config, Error: "turn-state-override.session-provenance-ttl-minutes must be 1-1440"}
		turnStateOverride.Store(state)
		return fmt.Errorf("turn-state-override.session-provenance-ttl-minutes must be 1-1440")
	}
	models := make([]string, 0, len(config.Models))
	for _, model := range config.Models {
		if model = strings.TrimSpace(model); model != "" {
			models = append(models, model)
		}
	}
	config.Models = models
	if config.Enabled {
		switch {
		case len(config.Models) == 0 && !probeEnabled(config.Probe):
			state = &turnStateOverrideState{Config: config, Error: "turn-state-override.models must not be empty"}
			turnStateOverride.Store(state)
			return fmt.Errorf("turn-state-override.models must not be empty")
		}
	}
	// Empty static value is valid: passive business observations may populate
	// the baseline even with the probe track disabled. Until then, do not inject.
	_ = configureProbeTrack(config.Probe)
	// Publish only after the complete rewrite configuration passed validation.
	probeTrack.mu.Lock()
	if override := probeTrack.settings.Timezone; override != nil && validateTargetTimezone(*override) == nil {
		zone = *override
	}
	configuredTimezone.Store(zone)
	probeTrack.mu.Unlock()
	lengths := config.AcceptedStateLengths
	if lengths == nil {
		lengths = []int{292, 332}
	}
	stateLengthPolicy.Store(append([]int(nil), lengths...))
	state = &turnStateOverrideState{Config: config}
	turnStateOverride.Store(state)
	return nil
}

// probeEnabled reports whether the probe block requests the background track.
func probeEnabled(block probeConfigYAML) bool {
	return block.Enabled != nil && *block.Enabled
}

// turnStateOverrideMatches reports whether the request model (executed or
// requested name) is targeted by the rewrite rules. Matching is a
// case-insensitive prefix comparison so a family entry such as "gpt-6-astra"
// also covers suffixed variants.
func turnStateOverrideMatches(config turnStateOverrideConfig, models ...string) bool {
	for _, candidate := range models {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		lowerCandidate := strings.ToLower(targetModel(candidate))
		for _, target := range config.Models {
			if strings.HasPrefix(lowerCandidate, strings.ToLower(targetModel(target))) {
				return true
			}
		}
	}
	return false
}

// applyTurnStateOverride evaluates the rewrite rules for one request and
// returns the header replacement plus the record status:
//
//	""                not applicable (disabled, no match, or no value)
//	"applied"         header replaced with the model's healthy baseline
//	"applied-config"  static fallback value (no fresh baseline available)
//	"skipped-existing" fill mode found an existing client value
//
// The healthy baseline table is seeded from recorded history and refreshed by
// business observations and manual probe rounds. It wins over the static
// configured value; the baseline keeps serving even while the probe track is
// idle - it is cached protection, not probing.
func applyTurnStateOverride(model, requestedModel string, headers http.Header) (http.Header, string) {
	return applyTurnStateOverrideInternal("", model, requestedModel, headers, false)
}

func applyTurnStateOverrideForAccount(authID, model, requestedModel string, headers http.Header) (http.Header, string) {
	return applyTurnStateOverrideInternal(authID, model, requestedModel, headers, true)
}

func applyTurnStateOverrideInternal(authID, model, requestedModel string, headers http.Header, accountScoped bool) (http.Header, string) {
	if !degradationDetectionEnabled(model, requestedModel) {
		return nil, ""
	}
	state := currentTurnStateOverride()
	force := state != nil && state.Config.Force
	existing := headerValue(headers, turnStateHeader)
	keepExisting := existing != "" && isAcceptedStateLength(len(existing)) && !force

	// Once account routing is enabled, the selected AuthID owns its state and
	// expiry. Never borrow the model-global baseline or static fallback from a
	// different account. A healthy client-provided value may still pass through
	// in fill mode because it already belongs to this request/account.
	accountModel := businessModelName(model, requestedModel)
	now := time.Now().UTC()
	if accountScoped {
		if value, scoped := accountRouter.accountTurnState(authID, accountModel, now); scoped {
			if keepExisting {
				return nil, "skipped-existing"
			}
			if value == "" {
				return nil, "account-state-missing"
			}
			return http.Header{turnStateHeader: []string{value}}, "applied-account"
		}
	}

	if state == nil {
		return nil, ""
	}
	baseline := ""
	matched := false
	for _, candidate := range []string{model, requestedModel} {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" || !degradationDetectionEnabled(candidate, "") {
			continue
		}
		if state.Config.Enabled && turnStateModelMatched(state.Config, candidate) {
			matched = true
		}
		if value := probeTrack.activeValueFor(candidate); value != "" {
			baseline = value
			matched = true
			break
		}
	}
	if !matched {
		return nil, ""
	}
	// An existing client value is kept only when it looks healthy (required
	// length) and force is off; an unhealthy one (for example a 312-byte
	// degraded state) is always replaced, whichever the mode.
	if baseline == "" {
		if scope, _ := splitTarget(model); scope != "" {
			return nil, "account-baseline-unavailable"
		}
		if !state.Config.Enabled || state.Config.Value == "" {
			return nil, ""
		}
		if keepExisting {
			return nil, "skipped-existing"
		}
		return http.Header{turnStateHeader: []string{state.Config.Value}}, "applied-config"
	}
	if keepExisting {
		return nil, "skipped-existing"
	}
	return http.Header{turnStateHeader: []string{baseline}}, "applied"
}

// isHealthyTurnState reports whether an observed turn-state value is healthy:
// exactly the required length and, when the upstream model is known, served by
// a consistent model. An unknown upstream model only length-checks.
func isHealthyTurnState(model, observedModel, state string) bool {
	if !isAcceptedStateLength(len(state)) {
		return false
	}
	if observedModel != "" && !probeModelConsistent(model, observedModel) {
		return false
	}
	return true
}

// repairTurnStateHeader substitutes the model's healthy baseline for an
// unhealthy upstream value on the response path ("丢弃并回灌"): the degraded
// state is discarded and the last known-good state is delivered downstream
// instead, so it is not fed back into the next request. Returns nil when no
// repair applies (healthy value, no state, or no baseline yet), in which case
// the response must pass through untouched.
func repairTurnStateHeader(model, requestedModel, observedModel, state string) http.Header {
	return repairTurnStateHeaderInternal("", model, requestedModel, observedModel, state, false)
}

func repairTurnStateHeaderForAccount(authID, model, requestedModel, observedModel, state string) http.Header {
	return repairTurnStateHeaderInternal(authID, model, requestedModel, observedModel, state, true)
}

func repairTurnStateHeaderInternal(authID, model, requestedModel, observedModel, state string, accountScoped bool) http.Header {
	if !degradationDetectionEnabled(model, requestedModel) {
		return nil
	}
	state = strings.TrimSpace(state)
	if state == "" {
		return nil
	}
	key := businessModelName(model, requestedModel)
	if key == "" {
		return nil
	}
	if isHealthyTurnState(key, observedModel, state) {
		return nil
	}
	baseline := ""
	scoped := false
	if accountScoped {
		baseline, scoped = accountRouter.accountTurnState(authID, key, time.Now().UTC())
	}
	if !accountScoped || !scoped {
		baseline = probeTrack.activeValueFor(key)
	}
	if baseline == "" || baseline == state {
		return nil
	}
	return http.Header{turnStateHeader: []string{baseline}}
}

// turnStateModelMatched reports whether a single candidate model name is
// targeted by the static rewrite list (case-insensitive prefix).
func turnStateModelMatched(config turnStateOverrideConfig, candidate string) bool {
	lower := strings.ToLower(targetModel(candidate))
	for _, target := range config.Models {
		if strings.HasPrefix(lower, strings.ToLower(targetModel(target))) {
			return true
		}
	}
	return false
}

func headerValue(headers http.Header, name string) string {
	if headers == nil {
		return ""
	}
	if values, ok := headers[http.CanonicalHeaderKey(name)]; ok {
		if len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
		return ""
	}
	// JSON-decoded ABI maps are not guaranteed to use Go's canonical casing.
	for key, values := range headers {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}

func previewValue(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}

// interceptNonStreamingResponse observes successful non-streaming execution
// responses before downstream header repair, so injected values are not learned.
func interceptNonStreamingResponse(raw []byte) (responseInterceptOutput, error) {
	var req responseInterceptRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return responseInterceptOutput{}, fmt.Errorf("decode response interception: %w", err)
	}
	state := headerValue(req.ResponseHeaders, turnStateHeader)
	authID := turnStateSessions.responseAuth(req.RequestID, businessModelName(req.Model, req.RequestedModel), req.Metadata, time.Now().UTC())
	if kind := quotaFailure(req.StatusCode, string(req.Body)); kind != "" {
		now := time.Now().UTC()
		accountRouter.observeQuota(authID, businessModelName(req.Model, req.RequestedModel), kind, quotaRetryAt(kind, req.ResponseHeaders, string(req.Body), now), now)
		history.observeQuota(req.RequestID, kind)
		return responseInterceptOutput{}, nil
	}
	upstream := ""
	if model, ok := probeUpstreamModel(req.Body); ok {
		upstream = model
		history.observeModel(req.RequestID, "", model)
	}
	scope := responseMetadataAccount(req.RequestID, req.Metadata)
	original := history.observeResponseBusiness(req.RequestID, scope, state, upstream)
	history.observeTurnState(req.RequestID, original, "response")
	out := responseInterceptOutput{}
	headers := repairTurnStateHeaderForAccount(authID, req.Model, req.RequestedModel, upstream, state)
	if scope != "" || currentProbeConfig().Config.AccountMode != "" {
		headers = repairScopedHeader(scope, req.Model, req.RequestedModel, upstream, state)
	}
	if headers != nil {
		out.Headers = headers
	}
	history.observeResponseTicket(req.RequestID, scope, state, out.Headers)
	guardResponseTurnState(
		req.RequestID, req.RequestHeaders, firstNonEmptyBody(req.RequestBody, req.OriginalRequest), req.Metadata,
		authID, businessModelName(req.Model, req.RequestedModel), state, &out, time.Now().UTC(),
	)
	if req.StatusCode == 0 || (req.StatusCode >= http.StatusOK && req.StatusCode < http.StatusMultipleChoices) {
		downstreamState := state
		if replacement := headerValue(out.Headers, turnStateHeader); replacement != "" {
			downstreamState = replacement
		}
		if responseClearsHeader(out.ClearHeaders, turnStateHeader) {
			downstreamState = ""
		}
		turnStateSessions.stageResponse(
			req.RequestID, req.RequestHeaders, firstNonEmptyBody(req.RequestBody, req.OriginalRequest), req.Metadata,
			authID, businessModelName(req.Model, req.RequestedModel), state, downstreamState, time.Now().UTC(),
		)
		if upstream != "" {
			turnStateSessions.confirmResponse(req.RequestID, upstream, time.Now().UTC())
		}
	}
	return out, nil
}

// interceptStreamChunk observes every successful stream chunk before it is
// delivered downstream. The header-init call (ChunkIndex == -1) carries no
// payload; payload chunks carry one upstream event each. Learning waits for
// a reported model while retaining the original, unrepaired header.
func interceptStreamChunk(raw []byte) (responseInterceptOutput, error) {
	var req streamChunkInterceptRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return responseInterceptOutput{}, fmt.Errorf("decode stream chunk interception: %w", err)
	}
	authID := turnStateSessions.responseAuth(req.RequestID, businessModelName(req.Model, req.RequestedModel), req.Metadata, time.Now().UTC())
	if kind := quotaFailure(0, string(req.Body)); kind != "" {
		now := time.Now().UTC()
		accountRouter.observeQuota(authID, businessModelName(req.Model, req.RequestedModel), kind, quotaRetryAt(kind, req.ResponseHeaders, string(req.Body), now), now)
		history.observeQuota(req.RequestID, kind)
		return responseInterceptOutput{}, nil
	}
	upstream := ""
	if req.ChunkIndex != streamChunkHeaderInitIndex {
		if model, ok := probeUpstreamModel(req.Body); ok {
			upstream = model
			history.observeModel(req.RequestID, "", model)
		}
	}
	state := headerValue(req.ResponseHeaders, turnStateHeader)
	scope := responseMetadataAccount(req.RequestID, req.Metadata)
	original := history.observeResponseBusiness(req.RequestID, scope, state, upstream)
	history.observeTurnState(req.RequestID, original, "stream")
	out := responseInterceptOutput{}
	headers := repairTurnStateHeaderForAccount(authID, req.Model, req.RequestedModel, upstream, state)
	if scope != "" || currentProbeConfig().Config.AccountMode != "" {
		headers = repairScopedHeader(scope, req.Model, req.RequestedModel, upstream, state)
	}
	if headers != nil {
		out.Headers = headers
	}
	history.observeResponseTicket(req.RequestID, scope, state, out.Headers)
	guardResponseTurnState(
		req.RequestID, req.RequestHeaders, firstNonEmptyBody(req.RequestBody, req.OriginalRequest), req.Metadata,
		authID, businessModelName(req.Model, req.RequestedModel), state, &out, time.Now().UTC(),
	)
	if state != "" {
		downstreamState := state
		if replacement := headerValue(out.Headers, turnStateHeader); replacement != "" {
			downstreamState = replacement
		}
		if responseClearsHeader(out.ClearHeaders, turnStateHeader) {
			downstreamState = ""
		}
		turnStateSessions.stageResponse(
			req.RequestID, req.RequestHeaders, firstNonEmptyBody(req.RequestBody, req.OriginalRequest), req.Metadata,
			authID, businessModelName(req.Model, req.RequestedModel), state, downstreamState, time.Now().UTC(),
		)
	}
	if upstream != "" {
		turnStateSessions.confirmResponse(req.RequestID, upstream, time.Now().UTC())
	}
	return out, nil
}

// Serialize learning with request-attempt replacement. The engine never holds
// its mutex while acquiring the audit mutex. Keep the first nonempty header:
// later stream callbacks may contain the plugin's repaired value instead.
func (s *auditState) observeResponseBusiness(requestID, scope, value, upstream string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.records {
		r := &s.records[i]
		if requestID == "" || r.RequestID != requestID || r.AccountScope != scope {
			continue
		}
		changed := r.TurnStateResponseStatus == "" || r.TurnStateResponseStatus == "headers-unavailable"
		if r.responseTicket == "" && value != "" {
			// Lengths above the maximum accepted size need no full-value cache.
			if len(value) > turnStateValueLimit+1 {
				value = value[:turnStateValueLimit+1]
			}
			r.responseTicket = value
			changed = true
		}
		if upstream != "" && upstream != r.responseModel {
			r.responseModel = upstream
			changed = true
		}
		if changed {
			r.TurnStateResponseStatus = observeScopedBusiness(scope, r.Model, r.RequestedModel, r.responseTicket, r.responseModel)
			markStateDirty()
		}
		return r.responseTicket
	}
	return ""
}

// Record the response header decision independently of the request injection.
// Later stream chunks may repeat already-repaired headers; keep the actual
// replacement evidence until the next request attempt resets it.
func (s *auditState) observeResponseTicket(requestID, scope, value string, replacement http.Header) {
	length := len(strings.TrimSpace(value))
	if requestID == "" || length == 0 {
		return
	}
	injected := len(headerValue(replacement, turnStateHeader))
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.records {
		r := &s.records[i]
		if r.RequestID != requestID || r.AccountScope != scope {
			continue
		}
		if r.TurnStateResponseInjectedLength > 0 && injected == 0 {
			return
		}
		r.TurnStateResponseOriginalLength = &length
		r.TurnStateResponseInjectedLength = injected
		markStateDirty()
		return
	}
}

// observeWebSocketEvent observes upstream websocket response events (the Codex
// Desktop /v1/responses path). The observer interface is read-only, so this
// only records the upstream model.
func observeWebSocketEvent(raw []byte) (struct{}, error) {
	var event webSocketResponseEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return struct{}{}, fmt.Errorf("decode websocket event: %w", err)
	}
	scope := responseAccount(event.RequestID, event.AuthID)
	history.noteResponseHeadersUnavailable(event.RequestID, scope)
	if model, ok := probeUpstreamModel(event.Payload); ok {
		history.observeModel(event.RequestID, event.TraceID, model)
		if scope == "" && currentProbeConfig().Config.AccountMode == "" {
			observeBusinessStateForRequest(event.Model, event.RequestedModel, "", model)
		}
		turnStateSessions.confirmResponse(event.RequestID, model, time.Now().UTC())
		observeScopedBusiness(scope, event.Model, event.RequestedModel, "", model)
	}
	return struct{}{}, nil
}

func firstNonEmptyBody(primary, fallback []byte) []byte {
	if len(primary) > 0 {
		return primary
	}
	return fallback
}

func guardResponseTurnState(requestID string, requestHeaders http.Header, requestBody []byte, metadata map[string]any, authID, model, state string, out *responseInterceptOutput, now time.Time) {
	if out == nil || strings.TrimSpace(state) == "" {
		return
	}
	decision := turnStateSessions.inspect(requestHeaders, requestBody, metadata, authID, model, state, now)
	foreign := decision.Kind == "foreign-account" || decision.Kind == "foreign-model"
	status := ""
	injectedLength := 0
	if foreign && decision.Mode == sessionGuardModeEnforce {
		replacement := headerValue(out.Headers, turnStateHeader)
		scoped := responseAccount(requestID, authID) != "" || currentProbeConfig().Config.AccountMode != ""
		if !scoped && (replacement == "" || !accountRouter.stateOwnedBy(authID, model, replacement, now)) {
			if owned, scoped := accountRouter.accountTurnState(authID, model, now); scoped {
				replacement = owned
			}
		}
		if replacement != "" && replacement != state {
			if out.Headers == nil {
				out.Headers = make(http.Header)
			}
			out.Headers.Set(turnStateHeader, replacement)
			status = "session-foreign-replaced"
			injectedLength = len(replacement)
			turnStateSessions.noteEnforcement("replaced")
		} else {
			out.Headers.Del(turnStateHeader)
			out.ClearHeaders = appendUniqueHeader(out.ClearHeaders, turnStateHeader)
			status = "session-foreign-stripped"
			turnStateSessions.noteEnforcement("stripped")
		}
	}
	history.observeSessionGuard(requestID, decision, status, injectedLength)
}

func appendUniqueHeader(headers []string, name string) []string {
	if responseClearsHeader(headers, name) {
		return headers
	}
	return append(headers, name)
}

func responseClearsHeader(headers []string, name string) bool {
	for _, candidate := range headers {
		if strings.EqualFold(strings.TrimSpace(candidate), name) {
			return true
		}
	}
	return false
}

// The current WebSocket event ABI has no response-header field. This is a
// visibility limitation, not proof that the upstream returned no ticket.
func (s *auditState) noteResponseHeadersUnavailable(requestID, scope string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.records {
		r := &s.records[i]
		if requestID != "" && r.RequestID == requestID && r.AccountScope == scope && r.TurnStateResponseStatus == "" {
			r.TurnStateResponseStatus = "headers-unavailable"
			markStateDirty()
			return
		}
	}
}

// Exempt requests are selected by the user's requested model, never by the
// model reported in the response. An astra response routed to luna must still
// be checked. Family boundaries avoid exempting names such as lunafoo.
func degradationDetectionEnabled(model, requestedModel string) bool {
	if strings.TrimSpace(requestedModel) != "" {
		model = requestedModel
	}
	model = strings.ToLower(strings.TrimSpace(targetModel(model)))
	if model == "luna" || model == "terra" {
		return false
	}
	parts := strings.Split(model, "-")
	return !(len(parts) >= 3 && parts[0] == "gpt" && (parts[2] == "luna" || parts[2] == "terra"))
}

func observeBusinessStateForRequest(model, requestedModel, state, upstream string) string {
	if degradationDetectionEnabled(model, requestedModel) {
		return observeBusinessState(businessModelName(model, requestedModel), state, upstream)
	}
	return "exempt"
}

// businessModelName picks the model name used as the business-observation
// key: the executed model first, falling back to the requested one. If a
// checked request was routed to an exempt family, retain the requested key.
func businessModelName(model, requestedModel string) string {
	if !degradationDetectionEnabled(model, "") && degradationDetectionEnabled(model, requestedModel) {
		return strings.TrimSpace(requestedModel)
	}
	if value := strings.TrimSpace(model); value != "" {
		return value
	}
	return strings.TrimSpace(requestedModel)
}

// probeUpstreamModel extracts the model name that the upstream reported, from
// a non-streaming body, an SSE chunk or a websocket frame. It is deliberately
// permissive about the transport shape and conservative about false matches:
// a payload without the "model" key is rejected by a substring prefilter, and
// unknown shapes fall through to the next candidate.
func probeUpstreamModel(payload []byte) (string, bool) {
	if len(payload) == 0 || len(payload) > maxModelProbeSize || !bytes.Contains(payload, []byte(`"model"`)) {
		return "", false
	}
	for _, candidate := range jsonCandidates(payload) {
		var probe struct {
			Type     string `json:"type"`
			Object   string `json:"object"`
			Model    string `json:"model"`
			Response struct {
				Model string `json:"model"`
			} `json:"response"`
			Message struct {
				Model string `json:"model"`
			} `json:"message"`
		}
		if err := json.Unmarshal(candidate, &probe); err != nil {
			continue
		}
		switch {
		case probe.Response.Model != "":
			return probe.Response.Model, true
		case probe.Message.Model != "":
			return probe.Message.Model, true
		case probe.Model != "" && (probe.Type != "" || probe.Object != ""):
			return probe.Model, true
		}
	}
	return "", false
}

// jsonCandidates returns the JSON documents that may appear in a payload:
// the whole body when it starts with '{', plus the first few "data:" / raw
// lines of an SSE stream.
func jsonCandidates(payload []byte) [][]byte {
	trimmed := bytes.TrimSpace(payload)
	candidates := make([][]byte, 0, maxModelProbeCandidates)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		candidates = append(candidates, trimmed)
	}
	for _, line := range bytes.Split(payload, []byte("\n")) {
		if len(candidates) >= maxModelProbeCandidates {
			break
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 || (len(candidates) > 0 && bytes.Equal(line, candidates[0])) {
			continue
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			line = bytes.TrimSpace(line[len("data:"):])
		}
		if len(line) > 0 && line[0] == '{' {
			candidates = append(candidates, line)
		}
	}
	return candidates
}

// observeModel attaches the upstream-reported model to an existing record.
// Requests are recorded by the request interceptor before execution starts,
// so an observer call for an unknown request (for example non-Codex traffic)
// is intentionally ignored rather than creating observer-only records.
func (s *auditState) observeModel(requestID, traceID, upstreamModel string) {
	upstreamModel = strings.TrimSpace(upstreamModel)
	if requestID == "" || upstreamModel == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.records {
		record := &s.records[i]
		if record.RequestID != requestID && (traceID == "" || record.TraceID != traceID) {
			continue
		}
		record.UpstreamModel = upstreamModel
		if !degradationDetectionEnabled(record.Model, record.RequestedModel) {
			record.ModelChecked = false
			record.ModelMismatch = false
			return
		}
		record.ModelChecked = true
		basis := businessModelName(record.Model, record.RequestedModel)
		record.ModelMismatch = basis != "" && !strings.EqualFold(basis, upstreamModel)
		return
	}
}

// observeTurnState records the size, the full value (bounded by
// turnStateValueLimit) and a short preview of the X-Codex-Turn-State header
// seen at the request, response or stream stage.
func (s *auditState) observeTurnState(requestID, value, source string) {
	value = strings.TrimSpace(value)
	if requestID == "" || value == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.records {
		if s.records[i].RequestID != requestID {
			continue
		}
		record := &s.records[i]
		record.TurnStateLength = len(value)
		record.TurnStateSource = source
		record.TurnStatePreview = previewValue(value, turnStatePreviewLength)
		if len(value) <= turnStateValueLimit {
			record.TurnStateValue = value
			record.TurnStateTruncated = false
		} else {
			record.TurnStateValue = value[:turnStateValueLimit]
			record.TurnStateTruncated = true
		}
		return
	}
}

// configureTurnStateOverrideFromLifecycle extracts the plugin config YAML from
// a plugin.register / plugin.reconfigure lifecycle payload (the request is
// JSON with a base64 config_yaml field) and activates the rewrite rules.
// Failures are recorded on the dashboard state instead of failing
// registration, so a bad rewrite config can never take the plugin down.
func configureTurnStateOverrideFromLifecycle(raw []byte) {
	var request struct {
		ConfigYAML []byte `json:"config_yaml"`
	}
	if err := json.Unmarshal(raw, &request); err != nil || len(request.ConfigYAML) == 0 {
		// Malformed lifecycle payloads keep the previous configuration.
		return
	}
	_ = configureTurnStateOverride(request.ConfigYAML)
	_ = configureAccountRouting(request.ConfigYAML)
}

// turnStateOverrideSummary describes the active rewrite configuration for the
// management API. The full rewrite value is never exposed; only its length and
// a short preview are reported.
func turnStateOverrideSummary() map[string]any {
	state := currentTurnStateOverride()
	if state == nil {
		return map[string]any{"enabled": false}
	}
	preview := ""
	if state.Config.Value != "" {
		preview = previewValue(state.Config.Value, turnStatePreviewLength)
	}
	models := state.Config.Models
	if models == nil {
		models = []string{}
	}
	return map[string]any{
		"enabled":       state.Config.Enabled,
		"models":        models,
		"force":         state.Config.Force,
		"value_length":  len(state.Config.Value),
		"value_preview": preview,
		"error":         state.Error,
		"session_guard": turnStateSessions.summary(),
		"probe":         probeSummary(),
	}
}
