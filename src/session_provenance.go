package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	sessionGuardModeOff     = "off"
	sessionGuardModeObserve = "observe"
	sessionGuardModeEnforce = "enforce"

	defaultSessionProvenanceTTL = 60 * time.Minute
	responseCandidateTTL        = 2 * time.Minute
	requestBindingTTL           = 15 * time.Minute
	maxSessionProvenanceEntries = 4096
)

// turnStateSessionOrigin records only ownership metadata. The state itself is
// already held by the account-scoped health table and is deliberately not
// duplicated here.
type turnStateSessionOrigin struct {
	AuthID      string
	Model       string
	Identity    string
	Fingerprint string
	StateLength int
	Source      string
	ObservedAt  time.Time
	ExpiresAt   time.Time
}

type turnStateResponseCandidate struct {
	RequestID          string
	SessionKey         string
	SessionSource      string
	AuthID             string
	Model              string
	UpstreamOwnerAuth  string
	UpstreamOwnerModel string
	UpstreamState      string
	DownstreamState    string
	StagedAt           time.Time
}

type turnStateProvenanceDecision struct {
	Kind          string
	Mode          string
	SessionKey    string
	SessionSource string
	Fingerprint   string
	OwnerAuthID   string
	OwnerModel    string
}

type turnStateSessionManager struct {
	mu sync.Mutex

	origins  map[string]turnStateSessionOrigin
	states   map[string]map[string]turnStateSessionOrigin
	pending  map[string]turnStateResponseCandidate
	requests map[string]turnStateRequestBinding

	inspected       uint64
	sameAccount     uint64
	foreignObserved uint64
	foreignStripped uint64
	foreignReplaced uint64
	unknown         uint64
	confirmed       uint64
	lastDecision    turnStateSessionEvent
}

type turnStateRequestBinding struct {
	AuthID     string
	Model      string
	ObservedAt time.Time
}

type turnStateSessionEvent struct {
	Kind        string    `json:"kind,omitempty"`
	Account     string    `json:"account,omitempty"`
	Owner       string    `json:"owner,omitempty"`
	Model       string    `json:"model,omitempty"`
	Fingerprint string    `json:"fingerprint,omitempty"`
	ObservedAt  time.Time `json:"observed_at,omitempty"`
}

var turnStateSessions = newTurnStateSessionManager()

func newTurnStateSessionManager() *turnStateSessionManager {
	return &turnStateSessionManager{
		origins:  make(map[string]turnStateSessionOrigin),
		states:   make(map[string]map[string]turnStateSessionOrigin),
		pending:  make(map[string]turnStateResponseCandidate),
		requests: make(map[string]turnStateRequestBinding),
	}
}

// CPA's post-selection request hook has the selected AuthID, but some host
// versions pass an earlier metadata snapshot to response hooks. Bind that
// trusted selection to the host request ID; never derive it from client headers.
func (m *turnStateSessionManager) bindRequest(requestID, authID, model string, now time.Time) {
	requestID, authID, model = strings.TrimSpace(requestID), strings.TrimSpace(authID), routingModelKey(model)
	if !sessionGuardEnabled() || requestID == "" || authID == "" || model == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupLocked(now)
	if len(m.requests) >= maxSessionProvenanceEntries {
		oldestKey := ""
		var oldest time.Time
		for key, binding := range m.requests {
			if oldestKey == "" || binding.ObservedAt.Before(oldest) {
				oldestKey, oldest = key, binding.ObservedAt
			}
		}
		delete(m.requests, oldestKey)
	}
	m.requests[requestID] = turnStateRequestBinding{AuthID: authID, Model: model, ObservedAt: now}
}

func (m *turnStateSessionManager) responseAuth(requestID, model string, metadata map[string]any, now time.Time) string {
	if authID := selectedAuthID(metadata); authID != "" {
		return authID
	}
	if !sessionGuardEnabled() {
		return ""
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	binding, ok := m.requests[strings.TrimSpace(requestID)]
	if !ok || now.Sub(binding.ObservedAt) >= requestBindingTTL || binding.Model != routingModelKey(model) {
		return ""
	}
	return binding.AuthID
}

func sessionGuardSettings() (string, time.Duration) {
	mode := sessionGuardModeObserve
	ttl := defaultSessionProvenanceTTL
	if state := currentTurnStateOverride(); state != nil {
		if configured := strings.ToLower(strings.TrimSpace(state.Config.SessionGuardMode)); configured != "" {
			mode = configured
		}
		if state.Config.SessionProvenanceTTLMinutes > 0 {
			ttl = time.Duration(state.Config.SessionProvenanceTTLMinutes) * time.Minute
		}
	}
	return mode, ttl
}

func sessionGuardEnabled() bool {
	mode, _ := sessionGuardSettings()
	if mode == sessionGuardModeOff {
		return false
	}
	accountRouter.mu.Lock()
	enabled := accountRouter.config.Config.Enabled && accountRouter.config.Error == ""
	accountRouter.mu.Unlock()
	return enabled
}

func (m *turnStateSessionManager) inspect(headers http.Header, body []byte, metadata map[string]any, authID, model, state string, now time.Time) turnStateProvenanceDecision {
	mode, _ := sessionGuardSettings()
	decision := turnStateProvenanceDecision{Mode: mode}
	state = strings.TrimSpace(state)
	model = routingModelKey(model)
	authID = strings.TrimSpace(authID)
	if !sessionGuardEnabled() || authID == "" || model == "" || state == "" {
		return decision
	}
	decision.Fingerprint = turnStateFingerprint(state)
	decision.SessionKey, decision.SessionSource = turnStateSessionKey(headers, body, metadata)
	decision.OwnerAuthID, decision.OwnerModel, _ = m.knownOwner(decision.SessionKey, state, now)

	switch {
	case decision.OwnerAuthID == "":
		decision.Kind = "unknown"
	case decision.OwnerAuthID != authID:
		decision.Kind = "foreign-account"
	case routingModelKey(decision.OwnerModel) != model:
		decision.Kind = "foreign-model"
	default:
		decision.Kind = "same-account"
	}
	m.recordInspection(decision, authID, model, now)
	return decision
}

func (m *turnStateSessionManager) recordInspection(decision turnStateProvenanceDecision, authID, model string, now time.Time) {
	if decision.Kind == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inspected++
	switch decision.Kind {
	case "same-account":
		m.sameAccount++
	case "foreign-account", "foreign-model":
		m.foreignObserved++
	case "unknown":
		m.unknown++
	}
	m.lastDecision = turnStateSessionEvent{
		Kind: decision.Kind, Account: publicAccountID(authID),
		Owner: publicAccountIDOrEmpty(decision.OwnerAuthID), Model: model,
		Fingerprint: decision.Fingerprint, ObservedAt: now,
	}
}

func (m *turnStateSessionManager) noteEnforcement(kind string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch kind {
	case "replaced":
		m.foreignReplaced++
	case "stripped":
		m.foreignStripped++
	}
}

func (m *turnStateSessionManager) noteOwned(headers http.Header, body []byte, metadata map[string]any, authID, model, state, source string, now time.Time) {
	if !sessionGuardEnabled() || strings.TrimSpace(authID) == "" || routingModelKey(model) == "" || !isAcceptedStateLength(len(strings.TrimSpace(state))) {
		return
	}
	key, sessionSource := turnStateSessionKey(headers, body, metadata)
	_, ttl := sessionGuardSettings()
	origin := turnStateSessionOrigin{
		AuthID: strings.TrimSpace(authID), Model: routingModelKey(model),
		Identity: turnStateIdentity(state), Fingerprint: turnStateFingerprint(state), StateLength: len(strings.TrimSpace(state)),
		Source: strings.TrimSpace(source), ObservedAt: now, ExpiresAt: now.Add(ttl),
	}
	if origin.Source == "" {
		origin.Source = sessionSource
	}
	m.mu.Lock()
	m.cleanupLocked(now)
	m.rememberOriginLocked(key, origin)
	m.confirmed++
	m.mu.Unlock()
}

// stageResponse delays ownership confirmation until a stream event reports the
// actual upstream model. This mirrors Sub2's "commit only after output" rule
// as closely as the CPA plugin ABI permits.
func (m *turnStateSessionManager) stageResponse(requestID string, requestHeaders http.Header, requestBody []byte, metadata map[string]any, authID, model, upstreamState, downstreamState string, now time.Time) {
	requestID = strings.TrimSpace(requestID)
	authID = strings.TrimSpace(authID)
	model = routingModelKey(model)
	if !sessionGuardEnabled() || requestID == "" || authID == "" || model == "" || strings.TrimSpace(downstreamState) == "" {
		return
	}
	key, source := turnStateSessionKey(requestHeaders, requestBody, metadata)
	upstreamOwnerAuth, upstreamOwnerModel, _ := m.knownOwner(key, upstreamState, now)
	candidate := turnStateResponseCandidate{
		RequestID: requestID, SessionKey: key, SessionSource: source,
		AuthID: authID, Model: model, UpstreamOwnerAuth: upstreamOwnerAuth,
		UpstreamOwnerModel: upstreamOwnerModel, UpstreamState: strings.TrimSpace(upstreamState),
		DownstreamState: strings.TrimSpace(downstreamState), StagedAt: now,
	}
	m.mu.Lock()
	m.cleanupLocked(now)
	m.pending[requestID] = candidate
	m.mu.Unlock()
}

func (m *turnStateSessionManager) confirmResponse(requestID, observedModel string, now time.Time) {
	requestID = strings.TrimSpace(requestID)
	observedModel = strings.TrimSpace(observedModel)
	if requestID == "" || observedModel == "" {
		return
	}
	m.mu.Lock()
	m.cleanupLocked(now)
	candidate, ok := m.pending[requestID]
	if ok {
		delete(m.pending, requestID)
	}
	m.mu.Unlock()
	if !ok || routingModelKey(observedModel) != candidate.Model {
		return
	}
	foreignUpstream := candidate.UpstreamOwnerAuth != "" &&
		(candidate.UpstreamOwnerAuth != candidate.AuthID || routingModelKey(candidate.UpstreamOwnerModel) != candidate.Model)
	upstreamHealthy := false
	if !foreignUpstream {
		upstreamHealthy = accountRouter.observeConfirmedState(candidate.AuthID, candidate.Model, observedModel, candidate.UpstreamState, now)
	}
	downstreamOwned := upstreamHealthy && candidate.DownstreamState == candidate.UpstreamState
	if !downstreamOwned {
		downstreamOwned = accountRouter.stateOwnedBy(candidate.AuthID, candidate.Model, candidate.DownstreamState, now)
	}
	if !downstreamOwned {
		return
	}
	_, ttl := sessionGuardSettings()
	origin := turnStateSessionOrigin{
		AuthID: candidate.AuthID, Model: candidate.Model,
		Identity: turnStateIdentity(candidate.DownstreamState), Fingerprint: turnStateFingerprint(candidate.DownstreamState), StateLength: len(candidate.DownstreamState),
		Source: "upstream-confirmed", ObservedAt: now, ExpiresAt: now.Add(ttl),
	}
	m.mu.Lock()
	m.cleanupLocked(now)
	m.rememberOriginLocked(candidate.SessionKey, origin)
	m.confirmed++
	m.mu.Unlock()
}

func (m *turnStateSessionManager) knownOwner(sessionKey, state string, now time.Time) (string, string, bool) {
	state = strings.TrimSpace(state)
	if state == "" {
		return "", "", false
	}
	identity := turnStateIdentity(state)
	m.mu.Lock()
	m.cleanupLocked(now)
	if sessionKey != "" {
		if origin, ok := m.origins[sessionKey]; ok && origin.Identity == identity {
			m.mu.Unlock()
			return origin.AuthID, origin.Model, true
		}
	}
	if owners := m.states[identity]; len(owners) == 1 {
		for _, origin := range owners {
			m.mu.Unlock()
			return origin.AuthID, origin.Model, true
		}
	}
	m.mu.Unlock()
	return accountRouter.knownStateOwner(state, now)
}

func (m *turnStateSessionManager) rememberOriginLocked(sessionKey string, origin turnStateSessionOrigin) {
	if sessionKey != "" {
		if len(m.origins) >= maxSessionProvenanceEntries {
			m.evictOldestOriginLocked()
		}
		m.origins[sessionKey] = origin
	}
	if m.states == nil {
		m.states = make(map[string]map[string]turnStateSessionOrigin)
	}
	owners := m.states[origin.Identity]
	if owners == nil {
		owners = make(map[string]turnStateSessionOrigin)
		m.states[origin.Identity] = owners
	}
	owners[accountHealthKey(origin.AuthID, origin.Model)] = origin
	if len(m.states) > maxSessionProvenanceEntries {
		m.evictOldestStateLocked()
	}
}

func (m *turnStateSessionManager) cleanupLocked(now time.Time) {
	for key, binding := range m.requests {
		if now.Sub(binding.ObservedAt) >= requestBindingTTL {
			delete(m.requests, key)
		}
	}
	for key, origin := range m.origins {
		if !origin.ExpiresAt.IsZero() && !now.Before(origin.ExpiresAt) {
			delete(m.origins, key)
		}
	}
	for identity, owners := range m.states {
		for ownerKey, origin := range owners {
			if !origin.ExpiresAt.IsZero() && !now.Before(origin.ExpiresAt) {
				delete(owners, ownerKey)
			}
		}
		if len(owners) == 0 {
			delete(m.states, identity)
		}
	}
	for requestID, candidate := range m.pending {
		if now.Sub(candidate.StagedAt) >= responseCandidateTTL {
			delete(m.pending, requestID)
		}
	}
}

func (m *turnStateSessionManager) evictOldestStateLocked() {
	oldestIdentity := ""
	var oldest time.Time
	for identity, owners := range m.states {
		for _, origin := range owners {
			if oldestIdentity == "" || origin.ObservedAt.Before(oldest) {
				oldestIdentity = identity
				oldest = origin.ObservedAt
			}
		}
	}
	if oldestIdentity != "" {
		delete(m.states, oldestIdentity)
	}
}

func (m *turnStateSessionManager) evictOldestOriginLocked() {
	oldestKey := ""
	var oldest time.Time
	for key, origin := range m.origins {
		if oldestKey == "" || origin.ObservedAt.Before(oldest) {
			oldestKey = key
			oldest = origin.ObservedAt
		}
	}
	if oldestKey != "" {
		delete(m.origins, oldestKey)
	}
}

func (m *turnStateSessionManager) summary() map[string]any {
	mode, ttl := sessionGuardSettings()
	now := time.Now().UTC()
	m.mu.Lock()
	m.cleanupLocked(now)
	defer m.mu.Unlock()
	return map[string]any{
		"enabled": sessionGuardEnabled(), "mode": mode,
		"ttl_minutes": int(ttl / time.Minute), "tracked_sessions": len(m.origins),
		"tracked_states":    len(m.states),
		"tracked_requests":  len(m.requests),
		"pending_responses": len(m.pending), "inspected": m.inspected,
		"same_account": m.sameAccount, "foreign_observed": m.foreignObserved,
		"foreign_stripped": m.foreignStripped, "foreign_replaced": m.foreignReplaced,
		"unknown": m.unknown, "confirmed": m.confirmed, "last_decision": m.lastDecision,
	}
}

func turnStateFingerprint(value string) string {
	return turnStateIdentity(value)[:12]
}

func turnStateIdentity(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:])
}

func turnStateSessionKey(headers http.Header, body []byte, metadata map[string]any) (string, string) {
	principal := sessionMetadataPrincipal(metadata)
	installation := firstHeaderValue(headers, "x-codex-installation-id")
	window := firstHeaderValue(headers, "x-codex-window-id")

	for _, name := range []string{
		"session-id", "session_id", "conversation_id", "conversation-id",
		"x-codex-session-id", "x-session-id", "thread-id", "thread_id", "x-codex-thread-id",
	} {
		if value := firstHeaderValue(headers, name); value != "" {
			return hashSessionSeed(principal, installation, window, name, value), "header:" + strings.ToLower(name)
		}
	}

	var payload struct {
		PromptCacheKey string `json:"prompt_cache_key"`
		SessionID      string `json:"session_id"`
		ConversationID string `json:"conversation_id"`
		ThreadID       string `json:"thread_id"`
		ClientMetadata struct {
			SessionID string `json:"session_id"`
			ThreadID  string `json:"thread_id"`
		} `json:"client_metadata"`
	}
	if len(body) > 0 && len(body) <= maxModelProbeSize && json.Unmarshal(body, &payload) == nil {
		for _, item := range []struct{ name, value string }{
			{"client_metadata.session_id", payload.ClientMetadata.SessionID},
			{"client_metadata.thread_id", payload.ClientMetadata.ThreadID},
			{"session_id", payload.SessionID}, {"conversation_id", payload.ConversationID},
			{"thread_id", payload.ThreadID}, {"prompt_cache_key", payload.PromptCacheKey},
		} {
			if value := strings.TrimSpace(item.value); value != "" {
				return hashSessionSeed(principal, installation, window, item.name, value), "body:" + item.name
			}
		}
	}
	return "", ""
}

func sessionMetadataPrincipal(metadata map[string]any) string {
	for _, key := range []string{"client_principal_id", "frontend_auth_principal", "api_key_id", "tenant_id"} {
		value, ok := metadata[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case string:
			if typed = strings.TrimSpace(typed); typed != "" {
				return key + ":" + typed
			}
		case float64:
			return key + ":" + strconv.FormatFloat(typed, 'f', -1, 64)
		case json.Number:
			return key + ":" + typed.String()
		}
	}
	return ""
}

func hashSessionSeed(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte(strings.TrimSpace(part)))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func firstHeaderValue(headers http.Header, name string) string {
	if headers == nil {
		return ""
	}
	return strings.TrimSpace(headers.Get(name))
}

func publicAccountIDOrEmpty(authID string) string {
	if strings.TrimSpace(authID) == "" {
		return ""
	}
	return publicAccountID(authID)
}
