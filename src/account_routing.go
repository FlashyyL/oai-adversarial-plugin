package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// accountRouting is deliberately evidence-driven. The usage callback records
// the selected AuthID after a request completes; the scheduler callback then
// receives only the host's current highest-priority candidate tier and may
// avoid credentials with fresh degradation evidence. It never reads tokens or
// reaches across priority tiers.
type accountRoutingConfig struct {
	Enabled          bool
	FailureThreshold int
}

type accountRoutingConfigState struct {
	Config accountRoutingConfig
	Error  string
}

type accountModelHealth struct {
	AuthID              string    `json:"auth_id,omitempty"`
	Account             string    `json:"account"`
	Model               string    `json:"model"`
	State               string    `json:"state"`
	LastStateLength     int       `json:"last_state_length,omitempty"`
	ConsecutiveFailures int       `json:"consecutive_failures,omitempty"`
	LastStatusCode      int       `json:"last_status_code,omitempty"`
	LastReason          string    `json:"last_reason,omitempty"`
	ObservedAt          time.Time `json:"observed_at"`
	HealthyAt           time.Time `json:"healthy_at,omitempty"`
}

const accountHealthMaxAge = time.Hour

func (e accountModelHealth) healthyAt(now time.Time) bool {
	return e.State == "healthy" && !e.HealthyAt.IsZero() &&
		!now.Before(e.HealthyAt) && now.Sub(e.HealthyAt) < accountHealthMaxAge
}

type schedulerCandidateDiagnostic struct {
	Account  string `json:"account"`
	Provider string `json:"provider,omitempty"`
	Status   string `json:"status,omitempty"`
	Health   string `json:"health"`
}

type schedulerRoutingDiagnostic struct {
	ObservedAt     time.Time                      `json:"observed_at"`
	Provider       string                         `json:"provider,omitempty"`
	Providers      []string                       `json:"providers,omitempty"`
	Model          string                         `json:"model"`
	CandidateCount int                            `json:"candidate_count"`
	EligibleCount  int                            `json:"eligible_count"`
	Outcome        string                         `json:"outcome"`
	Selected       string                         `json:"selected,omitempty"`
	Candidates     []schedulerCandidateDiagnostic `json:"candidates,omitempty"`
}

type accountRoutingManager struct {
	mu             sync.Mutex
	config         accountRoutingConfigState
	health         map[string]accountModelHealth
	roundRobin     uint64
	usageObserved  uint64
	schedulerCalls uint64
	routingHandled uint64
	lastScheduler  schedulerRoutingDiagnostic
}

var accountRouter = &accountRoutingManager{
	config: accountRoutingConfigState{Config: defaultAccountRoutingConfig()},
	health: map[string]accountModelHealth{},
}

func defaultAccountRoutingConfig() accountRoutingConfig {
	return accountRoutingConfig{
		FailureThreshold: 2,
	}
}

func configureAccountRouting(configYAML []byte) error {
	cfg := defaultAccountRoutingConfig()
	trimmed := bytes.TrimSpace(configYAML)
	if len(trimmed) == 0 {
		accountRouter.setConfig(accountRoutingConfigState{Config: cfg})
		return nil
	}
	normalized, err := normalizePanelConfig(trimmed)
	if err != nil {
		accountRouter.setConfig(accountRoutingConfigState{Config: cfg, Error: err.Error()})
		return err
	}
	var root struct {
		ExperimentalEnabled *bool `yaml:"experimental-account-routing"`
		FailureThreshold    *int  `yaml:"account-failure-threshold"`
		AccountRouting      struct {
			Enabled          *bool `yaml:"enabled"`
			FailureThreshold *int  `yaml:"failure-threshold"`
		} `yaml:"account-routing"`
	}
	if err := yaml.Unmarshal(normalized, &root); err != nil {
		state := accountRoutingConfigState{Config: cfg, Error: fmt.Sprintf("decode account routing config: %v", err)}
		accountRouter.setConfig(state)
		return fmt.Errorf("decode account routing config: %w", err)
	}
	if root.AccountRouting.Enabled != nil {
		cfg.Enabled = *root.AccountRouting.Enabled
	}
	if root.ExperimentalEnabled != nil {
		cfg.Enabled = *root.ExperimentalEnabled
	}
	threshold := root.AccountRouting.FailureThreshold
	if root.FailureThreshold != nil {
		threshold = root.FailureThreshold
	}
	if threshold != nil {
		if *threshold < 1 || *threshold > 10 {
			return accountRouter.configError(cfg, "account failure threshold must be 1-10")
		}
		cfg.FailureThreshold = *threshold
	}
	accountRouter.setConfig(accountRoutingConfigState{Config: cfg})
	return nil
}

func (m *accountRoutingManager) setConfig(state accountRoutingConfigState) {
	m.mu.Lock()
	m.config = state
	if m.health == nil {
		m.health = map[string]accountModelHealth{}
	}
	m.mu.Unlock()
}

func (m *accountRoutingManager) configError(cfg accountRoutingConfig, message string) error {
	m.setConfig(accountRoutingConfigState{Config: cfg, Error: message})
	return fmt.Errorf("%s", message)
}

type schedulerPickRequest struct {
	Provider   string
	Providers  []string
	Model      string
	Stream     bool
	Candidates []schedulerAuthCandidate
	Options    struct {
		Headers  http.Header
		Metadata map[string]any
	}
}

type schedulerAuthCandidate struct {
	ID       string
	Provider string
	Priority int
	Status   string
}

type schedulerPickResponse struct {
	AuthID          string `json:"AuthID,omitempty"`
	DelegateBuiltin string `json:"DelegateBuiltin,omitempty"`
	Handled         bool   `json:"Handled"`
}

func pickAccountForRequest(raw []byte) (schedulerPickResponse, error) {
	var req schedulerPickRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return schedulerPickResponse{}, fmt.Errorf("decode scheduler request: %w", err)
	}
	return accountConcurrency.pick(req, accountRouter.pick(req, time.Now().UTC())), nil
}

func (m *accountRoutingManager) pick(req schedulerPickRequest, now time.Time) schedulerPickResponse {
	model := routingModelKey(req.Model)
	if model == "" || !schedulerTargetsCodex(req) {
		return schedulerPickResponse{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.config.Config.Enabled || m.config.Error != "" {
		return schedulerPickResponse{}
	}
	m.schedulerCalls++
	diagnostic := schedulerRoutingDiagnostic{
		ObservedAt:     now,
		Provider:       req.Provider,
		Providers:      append([]string(nil), req.Providers...),
		Model:          req.Model,
		CandidateCount: len(req.Candidates),
		Outcome:        "delegated",
	}
	all := make([]schedulerAuthCandidate, 0, len(req.Candidates))
	healthy := make([]schedulerAuthCandidate, 0, len(req.Candidates))
	for _, candidate := range req.Candidates {
		// Some CPA releases leave the per-candidate provider empty after the
		// request provider has already selected the Codex scheduler. Accept that
		// shape, but still reject an explicitly different provider.
		if strings.TrimSpace(candidate.ID) == "" ||
			(strings.TrimSpace(candidate.Provider) != "" && !providerLooksCodex(candidate.Provider)) {
			continue
		}
		all = append(all, candidate)
		entry, ok := m.health[accountHealthKey(candidate.ID, model)]
		candidateDiagnostic := schedulerCandidateDiagnostic{
			Account: publicAccountID(candidate.ID), Provider: candidate.Provider,
			Status: candidate.Status, Health: "unknown",
		}
		freshHealthy := entry.healthyAt(now) && isAcceptedStateLength(entry.LastStateLength)
		if ok && entry.State != "" && (entry.State != "healthy" || freshHealthy) {
			candidateDiagnostic.Health = entry.State
		}
		diagnostic.Candidates = append(diagnostic.Candidates, candidateDiagnostic)
		if ok && freshHealthy {
			healthy = append(healthy, candidate)
		}
	}
	diagnostic.EligibleCount = len(all)
	if len(all) == 0 {
		diagnostic.Outcome = "no_eligible_candidates"
		m.lastScheduler = diagnostic
		return schedulerPickResponse{}
	}
	if len(healthy) == 0 {
		diagnostic.Outcome = "all_unknown"
		m.lastScheduler = diagnostic
		return schedulerPickResponse{}
	}
	pool := healthy
	sort.Slice(pool, func(i, j int) bool { return pool[i].ID < pool[j].ID })
	selected := pool[int(m.roundRobin%uint64(len(pool)))]
	m.roundRobin++
	m.routingHandled++
	diagnostic.Outcome = "selected"
	diagnostic.Selected = publicAccountID(selected.ID)
	m.lastScheduler = diagnostic
	return schedulerPickResponse{AuthID: selected.ID, Handled: true}
}

func schedulerTargetsCodex(req schedulerPickRequest) bool {
	if providerLooksCodex(req.Provider) {
		return true
	}
	for _, provider := range req.Providers {
		if providerLooksCodex(provider) {
			return true
		}
	}
	return false
}

func providerLooksCodex(provider string) bool {
	provider = strings.ToLower(strings.TrimSpace(provider))
	return strings.Contains(provider, "codex") || strings.Contains(provider, "openai") || strings.Contains(provider, "chatgpt")
}

type usageRecord struct {
	ResponseModel   string
	Alias           string
	Provider        string
	Model           string
	AuthID          string
	AuthIndex       string
	AuthType        string
	Failed          bool
	Failure         usageFailure
	ResponseHeaders http.Header
}

type usageFailure struct {
	StatusCode int
	Body       string
}

func observeAccountUsage(raw []byte) error {
	var record usageRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return fmt.Errorf("decode usage record: %w", err)
	}
	accountRouter.observe(record, time.Now().UTC())
	// Usage has no request binding. AuthID alone may refer to a replaced file;
	// ticket capture belongs to the bound HTTP/stream response callbacks.
	return nil
}

func (m *accountRoutingManager) observe(record usageRecord, now time.Time) {
	model := routingModelKey(record.Model)
	if strings.TrimSpace(record.AuthID) == "" || model == "" || !providerLooksCodex(record.Provider+" "+record.AuthType) {
		return
	}
	stateLength := responseStateLength(record.ResponseHeaders)
	key := accountHealthKey(record.AuthID, model)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.health == nil {
		m.health = map[string]accountModelHealth{}
	}
	entry := m.health[key]
	entry.AuthID = record.AuthID
	entry.Account = publicAccountID(record.AuthID)
	entry.Model = model
	entry.ObservedAt = now
	ignoredFailure := record.Failed && (transportUsageFailure(record.Failure) || record.Failure.StatusCode == http.StatusTooManyRequests)
	if stateLength > 0 && !ignoredFailure {
		entry.LastStateLength = stateLength
	}
	if record.Failure.StatusCode > 0 {
		entry.LastStatusCode = record.Failure.StatusCode
	}
	m.usageObserved++
	// A completed request breaks a run of failures even without a state header.
	// It does not renew the separate healthy-state evidence timestamp.
	if !record.Failed {
		entry.ConsecutiveFailures = 0
	}

	switch {
	case record.Failed && transportUsageFailure(record.Failure):
		entry.LastReason = "transport_failure"
	case record.Failed && record.Failure.StatusCode == http.StatusTooManyRequests:
		entry.LastReason = "rate_limited"
	case !record.Failed && isAcceptedStateLength(stateLength):
		entry.State = "healthy"
		entry.HealthyAt = now
		entry.ConsecutiveFailures = 0
		entry.LastReason = "healthy_state"
	case stateLength > 0 && !isAcceptedStateLength(stateLength):
		entry.State = "degraded"
		entry.ConsecutiveFailures++
		entry.LastReason = fmt.Sprintf("state_length_%d", stateLength)
	case record.Failed && (record.Failure.StatusCode == http.StatusUnauthorized || record.Failure.StatusCode == http.StatusForbidden):
		entry.State = "auth_error"
		entry.ConsecutiveFailures++
		entry.LastReason = fmt.Sprintf("http_%d", record.Failure.StatusCode)
	case record.Failed:
		entry.ConsecutiveFailures++
		entry.LastReason = classifyUsageFailure(record.Failure)
		if entry.ConsecutiveFailures >= m.config.Config.FailureThreshold {
			entry.State = "transient_failure"
		}
	default:
		if entry.State == "" {
			entry.LastReason = "completed_without_state"
		}
	}
	m.health[key] = entry
	markStateDirty()
}

// observeProbe connects the plugin's own bounded probe evidence to the
// scheduler. Unlike the generic usage callback, the probe has both the exact
// host AuthID and the upstream state/model evidence needed for a reliable
// health decision.
func (m *accountRoutingManager) observeProbe(authID, requestedModel string, record probeRecord, now time.Time) {
	model := routingModelKey(requestedModel)
	if strings.TrimSpace(authID) == "" || model == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.health == nil {
		m.health = map[string]accountModelHealth{}
	}
	key := accountHealthKey(authID, model)
	entry := m.health[key]
	entry.AuthID = authID
	entry.Account = publicAccountID(authID)
	entry.Model = model
	entry.ObservedAt = now
	entry.LastStateLength = record.StateLength
	entry.LastStatusCode = record.StatusCode

	switch {
	case record.Success && isAcceptedStateLength(record.StateLength) && probeModelConsistent(requestedModel, record.ObservedModel):
		entry.State = "healthy"
		entry.HealthyAt = now
		entry.ConsecutiveFailures = 0
		entry.LastReason = "probe_healthy"
	case record.StatusCode == http.StatusUnauthorized || record.StatusCode == http.StatusForbidden:
		entry.State = "auth_error"
		entry.ConsecutiveFailures++
		entry.LastReason = fmt.Sprintf("probe_http_%d", record.StatusCode)
	case record.StateLength > 0 && !isAcceptedStateLength(record.StateLength):
		entry.State = "degraded"
		entry.ConsecutiveFailures++
		entry.LastReason = fmt.Sprintf("probe_state_length_%d", record.StateLength)
	case record.ObservedModel != "" && !probeModelConsistent(requestedModel, record.ObservedModel):
		entry.State = "degraded"
		entry.ConsecutiveFailures++
		entry.LastReason = "probe_model_mismatch"
	default:
		// Connection and proxy failures are not account-quality evidence. The
		// probe engine keeps those diagnostics and exit cooldowns separately.
		return
	}
	m.health[key] = entry
	markStateDirty()
}

func responseStateLength(headers http.Header) int {
	for key, values := range headers {
		if !strings.EqualFold(key, turnStateHeader) {
			continue
		}
		for i := len(values) - 1; i >= 0; i-- {
			if value := strings.TrimSpace(values[i]); value != "" {
				return len([]byte(value))
			}
		}
	}
	return 0
}

func classifyUsageFailure(failure usageFailure) string {
	body := strings.ToLower(failure.Body)
	switch {
	case strings.Contains(body, "server_is_overloaded"), strings.Contains(body, "at capacity"):
		return "overloaded"
	case failure.StatusCode >= 500:
		return fmt.Sprintf("http_%d", failure.StatusCode)
	default:
		return "request_failed"
	}
}

// Transport/proxy failures provide no evidence about credential quality.
// Keep only the fixed category in diagnostics, never the raw error body.
func transportUsageFailure(failure usageFailure) bool {
	if failure.StatusCode == 0 || failure.StatusCode == http.StatusProxyAuthRequired {
		return true
	}
	body := strings.ToLower(failure.Body)
	for _, marker := range []string{"proxyconnect", "socks", "dial tcp", "connection refused", "connection reset", "i/o timeout", "tls handshake timeout", "no such host", "context deadline exceeded"} {
		if strings.Contains(body, marker) {
			return true
		}
	}
	return false
}

func routingModelKey(model string) string {
	model = strings.ToLower(strings.TrimSpace(model))
	switch {
	case strings.Contains(model, "astra"):
		return "astra"
	case strings.Contains(model, "sol"):
		return "sol"
	default:
		return ""
	}
}

func accountHealthKey(authID, model string) string { return authID + "\x00" + model }

func publicAccountID(authID string) string {
	sum := sha256.Sum256([]byte(authID))
	return "auth-" + hex.EncodeToString(sum[:6])
}

func accountRoutingSummary() map[string]any {
	accountRouter.mu.Lock()
	defer accountRouter.mu.Unlock()
	entries := make([]accountModelHealth, 0, len(accountRouter.health))
	now := time.Now().UTC()
	for _, entry := range accountRouter.health {
		entry.AuthID = ""
		if entry.State == "healthy" && (!entry.healthyAt(now) || !isAcceptedStateLength(entry.LastStateLength)) {
			entry.State = ""
			entry.LastReason = "health_evidence_unavailable"
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ObservedAt.Equal(entries[j].ObservedAt) {
			return entries[i].Account < entries[j].Account
		}
		return entries[i].ObservedAt.After(entries[j].ObservedAt)
	})
	return map[string]any{
		"enabled":           accountRouter.config.Config.Enabled,
		"monitoring":        true,
		"error":             accountRouter.config.Error,
		"failure_threshold": accountRouter.config.Config.FailureThreshold,
		"usage_observed":    accountRouter.usageObserved,
		"scheduler_calls":   accountRouter.schedulerCalls,
		"routing_handled":   accountRouter.routingHandled,
		"last_scheduler":    accountRouter.lastScheduler,
		"accounts":          entries,
	}
}
