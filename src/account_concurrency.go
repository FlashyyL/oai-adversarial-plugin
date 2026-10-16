package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const concurrencyHeader = "X-Oai-Account-Reservation"
const maxAccountConcurrency = 10000

type concurrencySettings struct {
	Version          int                            `json:"version"`
	Limits           map[string]int                 `json:"limits"`
	WeeklyThresholds map[string]float64             `json:"weekly_thresholds,omitempty"`
	WeeklyUsage      map[string]weeklyQuotaSnapshot `json:"weekly_usage,omitempty"`
}

type concurrencyRequest struct {
	ID            string     `json:"request_id"`
	Account       string     `json:"account"`
	Model         string     `json:"model"`
	Kind          string     `json:"kind"`
	Status        string     `json:"status"`
	Started       time.Time  `json:"started_at"`
	Completed     *time.Time `json:"completed_at,omitempty"`
	Limit         int        `json:"limit"`
	Rerouted      bool       `json:"rerouted"`
	Token, AuthID string     `json:"-"`
	AttemptAt     time.Time  `json:"-"`
}

// The active set is independent of the bounded audit ring. A long stream must
// retain its slot even if its audit record is evicted; no time-based reclamation.
type concurrencyManager struct {
	mu                 sync.Mutex
	loaded             bool
	loadError          string
	limits             map[string]int
	requests           map[string]*concurrencyRequest
	tokens             map[string]string
	used               map[string]int
	recent             []concurrencyRequest
	cursor             uint64
	changed            chan struct{}
	weeklyThresholds   map[string]float64
	weeklyUsage        map[string]weeklyQuotaSnapshot
	weeklyNext         map[string]time.Time
	weeklyDirty        bool
	weeklyActive       bool
	weeklyCancel       context.CancelFunc
	weeklyWG           sync.WaitGroup
	weeklyWake         chan struct{}
	weeklyErrors       map[string]string
	weeklyStorageError string
}

func newConcurrencyManager() *concurrencyManager {
	return &concurrencyManager{limits: map[string]int{}, requests: map[string]*concurrencyRequest{}, tokens: map[string]string{}, used: map[string]int{}, changed: make(chan struct{}), weeklyThresholds: map[string]float64{}, weeklyUsage: map[string]weeklyQuotaSnapshot{}, weeklyNext: map[string]time.Time{}, weeklyWake: make(chan struct{}, 1), weeklyErrors: map[string]string{}}
}

var accountConcurrency = newConcurrencyManager()

func concurrencySettingsPath() string {
	return filepath.Join(filepath.Dir(stateFilePath()), "account-concurrency.json")
}

func validConcurrencyAccount(key string) bool {
	if len(key) != 69 || !strings.HasPrefix(key, "auth-") {
		return false
	}
	_, err := hex.DecodeString(key[5:])
	return err == nil
}

func (m *concurrencyManager) loadLocked() {
	if m.loaded {
		return
	}
	m.loaded = true
	raw, err := os.ReadFile(concurrencySettingsPath())
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	var settings concurrencySettings
	if err != nil || len(raw) > 1<<20 || json.Unmarshal(raw, &settings) != nil || settings.Version != 1 || settings.Limits == nil || len(settings.Limits) > 4096 {
		m.loadError = "concurrency_settings_unavailable"
		return
	}
	for key, limit := range settings.Limits {
		if !validConcurrencyAccount(key) || limit < 0 || limit > maxAccountConcurrency {
			m.loadError = "concurrency_settings_unavailable"
			return
		}
	}
	for key, threshold := range settings.WeeklyThresholds {
		if !validConcurrencyAccount(key) || !validWeeklyThreshold(threshold) {
			m.loadError = "concurrency_settings_unavailable"
			return
		}
	}
	if len(settings.WeeklyThresholds) > 4096 || len(settings.WeeklyUsage) > 4096 {
		m.loadError = "concurrency_settings_unavailable"
		return
	}
	for key, snapshot := range settings.WeeklyUsage {
		if !validConcurrencyAccount(key) || !validConcurrencyAccount(snapshot.Scope) || !validWeeklyPercent(snapshot.UsedPercent) || snapshot.ObservedAt.IsZero() || snapshot.CheckedAt.IsZero() {
			m.loadError = "concurrency_settings_unavailable"
			return
		}
		m.weeklyUsage[key] = snapshot
	}
	for key, threshold := range settings.WeeklyThresholds {
		m.weeklyThresholds[key] = threshold
	}
	m.limits = settings.Limits
}

func (m *concurrencyManager) limitedLocked() bool {
	for _, limit := range m.limits {
		if limit > 0 {
			return true
		}
	}
	for _, threshold := range m.weeklyThresholds {
		if threshold > 0 {
			return true
		}
	}
	return false
}

func (m *concurrencyManager) notifyLocked() { close(m.changed); m.changed = make(chan struct{}) }

func (m *concurrencyManager) save(account string, limit int) error {
	return m.saveSettings(account, &limit, nil)
}

func (m *concurrencyManager) saveSettings(account string, limit *int, threshold *float64) error {
	if !validConcurrencyAccount(account) || (limit == nil && threshold == nil) || (limit != nil && (*limit < 0 || *limit > maxAccountConcurrency)) {
		return errors.New("invalid_concurrency_limit")
	}
	if threshold != nil && !validWeeklyThreshold(*threshold) {
		return errors.New("invalid_weekly_threshold")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadLocked()
	if m.loadError != "" {
		return errors.New(m.loadError)
	}
	limits := make(map[string]int, len(m.limits)+1)
	if _, exists := m.limits[account]; limit != nil && !exists && len(m.limits) >= 4096 {
		return errors.New("invalid_concurrency_limit")
	}
	for key, value := range m.limits {
		limits[key] = value
	}
	if limit != nil {
		limits[account] = *limit
	}
	thresholds := make(map[string]float64, len(m.weeklyThresholds)+1)
	if _, exists := m.weeklyThresholds[account]; threshold != nil && !exists && len(m.weeklyThresholds) >= 4096 {
		return errors.New("invalid_weekly_threshold")
	}
	for key, value := range m.weeklyThresholds {
		thresholds[key] = value
	}
	if threshold != nil {
		thresholds[account] = *threshold
	}
	// Publish both fields only after one successful atomic file replacement.
	if err := m.persistSettingsLocked(limits, thresholds); err != nil {
		return err
	}
	m.limits = limits
	m.weeklyThresholds = thresholds
	m.notifyLocked()
	if threshold != nil {
		delete(m.weeklyNext, account)
		m.startWeeklyLocked()
		select {
		case m.weeklyWake <- struct{}{}:
		default:
		}
	}
	return nil
}

func (m *concurrencyManager) persistSettingsLocked(limits map[string]int, thresholds map[string]float64) error {
	data, err := json.Marshal(concurrencySettings{Version: 1, Limits: limits, WeeklyThresholds: thresholds, WeeklyUsage: m.weeklyUsage})
	if err != nil {
		return errors.New("concurrency_save_failed")
	}
	path := concurrencySettingsPath()
	if err := os.MkdirAll(filepath.Dir(path), persistDirMode); err != nil {
		return errors.New("concurrency_save_failed")
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".account-concurrency-*")
	if err != nil {
		return errors.New("concurrency_save_failed")
	}
	defer os.Remove(file.Name())
	if err = file.Chmod(persistFileMode); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	closed := file.Close()
	if err != nil || closed != nil || os.Rename(file.Name(), path) != nil {
		return errors.New("concurrency_save_failed")
	}
	m.weeklyDirty = false
	m.weeklyStorageError = ""
	return nil
}

func reservationToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// The host exposes RequestID in before/after hooks, but not scheduler metadata.
// Bridge it with a host-generated opaque token; overwrite client input and
// strip it in after-auth. Tokens never identify credentials or leave CPA.
func concurrencyBefore(raw []byte) (interceptResponse, error) {
	var req interceptRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return interceptResponse{}, err
	}
	m := accountConcurrency
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadLocked()
	if !m.limitedLocked() && m.loadError == "" {
		return stripConcurrencyHeader(req.Headers, interceptResponse{}), nil
	}
	if req.RequestID == "" {
		return concurrencyRejection("concurrency_request_untracked", http.StatusServiceUnavailable), nil
	}
	r := m.requests[req.RequestID]
	if r == nil {
		token, err := reservationToken()
		if err != nil {
			return concurrencyRejection("concurrency_request_untracked", http.StatusServiceUnavailable), nil
		}
		r = &concurrencyRequest{ID: req.RequestID, Token: token, Model: req.RequestedModel, Kind: "business", Status: "selecting", Started: time.Now().UTC()}
		m.requests[r.ID] = r
		m.tokens[token] = r.ID
	}
	return interceptResponse{Headers: http.Header{concurrencyHeader: {r.Token}}}, nil
}

func stripConcurrencyHeader(headers http.Header, response interceptResponse) interceptResponse {
	if headerValue(headers, concurrencyHeader) == "" {
		return response
	}
	if response.Headers == nil {
		response.Headers = http.Header{}
	}
	response.Headers[concurrencyHeader] = []string{""}
	response.ClearHeaders = append(response.ClearHeaders, concurrencyHeader)
	return response
}

func nativeConcurrencyCandidate(req schedulerPickRequest, candidate schedulerAuthCandidate) bool {
	provider := strings.ToLower(strings.TrimSpace(candidate.Provider))
	if provider == "" {
		provider = strings.ToLower(strings.TrimSpace(req.Provider))
	}
	return provider == "codex"
}

func (m *concurrencyManager) unoccupyLocked(r *concurrencyRequest) {
	if r.AuthID != "" {
		key := accountScope(r.AuthID)
		if m.used[key] > 0 {
			m.used[key]--
		}
		r.AuthID = ""
		m.notifyLocked()
	}
}

func (m *concurrencyManager) occupyLocked(r *concurrencyRequest, authID string) bool {
	key := accountScope(authID)
	if m.weeklyGateLocked(key, time.Now()) != "" {
		return false
	}
	limit := m.limits[key]
	if limit > 0 && m.used[key] >= limit {
		return false
	}
	r.AuthID = authID
	r.Account = key
	r.Limit = limit
	r.AttemptAt = time.Now().UTC()
	m.used[key]++
	return true
}

func (m *concurrencyManager) pick(req schedulerPickRequest, preferred schedulerPickResponse) schedulerPickResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadLocked()
	id := m.tokens[headerValue(req.Options.Headers, concurrencyHeader)]
	r := m.requests[id]
	if r == nil || m.loadError != "" || !m.limitedLocked() {
		return preferred
	}
	pool := make([]schedulerAuthCandidate, 0, len(req.Candidates))
	for _, c := range req.Candidates {
		if c.ID != "" && nativeConcurrencyCandidate(req, c) {
			pool = append(pool, c)
		} else {
			return preferred
		}
	}
	if len(pool) == 0 {
		return preferred
	}
	previous := r.Account
	m.unoccupyLocked(r)
	r.Status = "selecting"
	sort.Slice(pool, func(i, j int) bool { return pool[i].ID < pool[j].ID })
	start := int(m.cursor % uint64(len(pool)))
	m.cursor++
	if preferred.Handled && preferred.AuthID != "" {
		for i, c := range pool {
			if c.ID == preferred.AuthID {
				start = i
				break
			}
		}
	}
	for i := range pool {
		c := pool[(start+i)%len(pool)]
		if m.occupyLocked(r, c.ID) {
			r.Status = "reserved"
			r.Rerouted = r.Rerouted || i > 0 || (previous != "" && previous != r.Account)
			return schedulerPickResponse{AuthID: c.ID, Handled: true}
		}
	}
	// Keep this within the supplied pool so after-auth can return a proper 429,
	// rather than delegating to an unrestricted scheduler or retrying upstream.
	r.Status = "capacity_rejected"
	allQuota := true
	for _, c := range pool {
		if m.weeklyGateLocked(accountScope(c.ID), time.Now()) == "" {
			allQuota = false
		}
	}
	if allQuota {
		r.Status = "quota_rejected"
	}
	return schedulerPickResponse{AuthID: pool[start].ID, Handled: true}
}

func concurrencyRejection(code string, status int) interceptResponse {
	body, _ := json.Marshal(map[string]any{"error": map[string]string{"type": code, "message": code}})
	return interceptResponse{Terminate: true, StatusCode: status, ResponseHeaders: http.Header{"Content-Type": {"application/json"}, "Retry-After": {"1"}}, ResponseBody: body}
}

func (m *concurrencyManager) admit(id, authID, model, kind string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadLocked()
	if m.loadError != "" {
		return errors.New(m.loadError)
	}
	if id == "" || authID == "" {
		if m.limitedLocked() {
			return errors.New("concurrency_request_untracked")
		}
		return nil
	}
	r := m.requests[id]
	if r == nil || r.Status != "running" || r.AuthID != authID {
		if code := m.weeklyGateLocked(accountScope(authID), time.Now()); code != "" {
			if r != nil {
				m.unoccupyLocked(r)
				r.Status = "quota_rejected"
			}
			return errors.New(code)
		}
	}
	if r == nil {
		r = &concurrencyRequest{ID: id, Started: time.Now().UTC(), Kind: kind}
		m.requests[id] = r
	}
	if r.AuthID != authID {
		previous := r.Account
		m.unoccupyLocked(r)
		if !m.occupyLocked(r, authID) {
			r.Account = accountScope(authID)
			r.Limit = m.limits[r.Account]
			r.Status = "capacity_rejected"
			return errors.New("account_concurrency_exceeded")
		}
		r.Rerouted = r.Rerouted || (previous != "" && previous != r.Account)
	}
	// A limit reduction keeps existing reservations/streams until completion.
	r.Model = model
	r.Kind = kind
	r.Status = "running"
	return nil
}

func concurrencyAfter(req interceptRequest) interceptResponse {
	if req.ToFormat != "codex" {
		return interceptResponse{}
	}
	id, _ := req.Metadata["selected_auth_id"].(string)
	entries, err := hostAuthListFunc()
	if err != nil {
		accountConcurrency.mu.Lock()
		accountConcurrency.loadLocked()
		restricted := accountConcurrency.limitedLocked() || accountConcurrency.loadError != ""
		accountConcurrency.mu.Unlock()
		if restricted {
			return concurrencyRejection("concurrency_inventory_unavailable", http.StatusServiceUnavailable)
		}
		return interceptResponse{}
	}
	native, known := false, false
	for _, e := range entries {
		if e.ID == id {
			known = true
			native = nativeCodexEntry(e)
			if native && e.Disabled {
				accountConcurrency.mu.Lock()
				restricted := accountConcurrency.limits[accountScope(id)] > 0
				accountConcurrency.mu.Unlock()
				if restricted {
					return concurrencyRejection("concurrency_account_unavailable", http.StatusServiceUnavailable)
				}
				native = false
			}
			break
		}
	}
	if !known {
		accountConcurrency.mu.Lock()
		restricted := accountConcurrency.limitedLocked() || accountConcurrency.loadError != ""
		accountConcurrency.mu.Unlock()
		if restricted {
			return concurrencyRejection("concurrency_request_untracked", http.StatusServiceUnavailable)
		}
	}
	if !native {
		return interceptResponse{}
	}
	accountConcurrency.mu.Lock()
	quotaEnabled := accountConcurrency.weeklyThresholds[accountScope(id)] > 0
	accountConcurrency.mu.Unlock()
	if quotaEnabled {
		scope, _ := selectedAccountInfo(req.Metadata, req.Headers)
		accountConcurrency.checkWeeklyIdentity(accountScope(id), scope)
	}
	model := req.RequestedModel
	if model == "" {
		model = req.Model
	}
	if err := accountConcurrency.admit(req.RequestID, id, model, "business"); err != nil {
		status := http.StatusServiceUnavailable
		if err.Error() == "account_concurrency_exceeded" {
			status = http.StatusTooManyRequests
		}
		if err.Error() == "account_weekly_quota_exceeded" {
			status = http.StatusTooManyRequests
		}
		state := "capacity_rejected"
		if weeklyQuotaError(err.Error()) {
			state = "quota_rejected"
		}
		scope, email := selectedAccountInfo(req.Metadata, req.Headers)
		history.record(auditRecord{RequestID: req.RequestID, TraceID: req.TraceID, AuthBinding: accountScope(id), AccountScope: scope, AccountEmail: email, Account: targetAccount(scopedTarget(scope, req.Model)), Model: req.Model, RequestedModel: req.RequestedModel, Time: time.Now().UTC().Format(time.RFC3339Nano), ConcurrencyState: state, performanceAttemptAt: time.Now(), conversion: conversion{Target: currentTimezone(), Original: []string{}, Paths: []string{}}})
		return concurrencyRejection(err.Error(), status)
	}
	return interceptResponse{}
}

func nativeCodexEntry(e hostAuthEntry) bool {
	p := e.Provider
	if p == "" {
		p = e.Type
	}
	return strings.EqualFold(strings.TrimSpace(p), "codex")
}

func (m *concurrencyManager) finish(req requestCompletion) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.requests[req.RequestID]
	if r != nil && r.AuthID != "" {
		if id, _ := req.Metadata["selected_auth_id"].(string); id != "" && id != r.AuthID {
			return
		}
	}
	if r == nil || !completionOutcome(req.Outcome) || (!req.CompletedAt.IsZero() && req.CompletedAt.Before(r.AttemptAt)) {
		return
	}
	m.unoccupyLocked(r)
	delete(m.tokens, r.Token)
	delete(m.requests, r.ID)
	if r.Account != "" {
		copy := *r
		copy.Token = ""
		copy.Status = req.Outcome
		now := time.Now().UTC()
		copy.Completed = &now
		m.recent = append(m.recent, copy)
		if len(m.recent) > 50 {
			m.recent = append([]concurrencyRequest(nil), m.recent[len(m.recent)-50:]...)
		}
	}
}

func (m *concurrencyManager) state(id string) (string, *int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.requests[id]
	if r == nil || r.Account == "" {
		return "", nil, false
	}
	limit := r.Limit
	return r.Status, &limit, r.Rerouted
}

func (m *concurrencyManager) internal(ctx context.Context, id, authID, model, kind string, wait bool) error {
	for {
		if ctx.Err() != nil {
			m.finish(requestCompletion{RequestID: id, Outcome: "canceled"})
			return errors.New("account_concurrency_exceeded")
		}
		if err := m.admit(id, authID, model, kind); err == nil {
			return nil
		} else if err.Error() != "account_concurrency_exceeded" || !wait {
			// A probe polling for capacity has not dispatched a model request.
			m.mu.Lock()
			if r := m.requests[id]; r != nil {
				m.unoccupyLocked(r)
				delete(m.tokens, r.Token)
				delete(m.requests, id)
			}
			m.mu.Unlock()
			return err
		}
		m.mu.Lock()
		r := m.requests[id]
		if r != nil {
			r.Status = "waiting"
		}
		changed := m.changed
		// Close the check/subscribe race when a release precedes subscription.
		key := accountScope(authID)
		available := m.limits[key] <= 0 || m.used[key] < m.limits[key]
		m.mu.Unlock()
		if available {
			continue
		}
		select {
		case <-ctx.Done():
			m.finish(requestCompletion{RequestID: id, Outcome: "canceled"})
			return errors.New("account_concurrency_exceeded")
		case <-changed:
		}
	}
}

func concurrencyInventory() ([]hostAuthEntry, error) {
	entries, err := hostAuthListFunc()
	if err != nil {
		return nil, errors.New("concurrency_inventory_unavailable")
	}
	var native []hostAuthEntry
	seen := map[string]bool{}
	for _, e := range entries {
		if nativeCodexEntry(e) && e.ID != "" && !seen[e.ID] {
			native = append(native, e)
			seen[e.ID] = true
		}
	}
	sort.Slice(native, func(i, j int) bool { return native[i].ID < native[j].ID })
	return native, nil
}

func concurrencyManagement(body []byte, save bool) managementResponse {
	entries, err := concurrencyInventory()
	if err != nil {
		return evalJSON(503, map[string]string{"error": err.Error()})
	}
	if save {
		var req struct {
			Account         string   `json:"account"`
			Limit           *int     `json:"limit"`
			WeeklyThreshold *float64 `json:"weekly_threshold"`
		}
		if json.Unmarshal(body, &req) != nil || (req.Limit == nil && req.WeeklyThreshold == nil) {
			return evalJSON(400, map[string]string{"error": "invalid_concurrency_limit"})
		}
		found := false
		for _, e := range entries {
			if accountScope(e.ID) == req.Account {
				found = true
				break
			}
		}
		if !found {
			return evalJSON(409, map[string]string{"error": "concurrency_account_unavailable"})
		}
		if err := accountConcurrency.saveSettings(req.Account, req.Limit, req.WeeklyThreshold); err != nil {
			status := 500
			if err.Error() == "invalid_concurrency_limit" || err.Error() == "invalid_weekly_threshold" {
				status = 400
			}
			return evalJSON(status, map[string]string{"error": err.Error()})
		}
	}
	m := accountConcurrency
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadLocked()
	accounts := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		key := accountScope(e.ID)
		name := e.Name
		if name == "" {
			name = filepath.Base(e.Path)
		}
		name = filepath.Base(strings.ReplaceAll(name, "\\", "/"))
		accounts = append(accounts, map[string]any{"account": key, "file_name": name, "account_email": e.Email, "disabled": e.Disabled, "limit": m.limits[key], "in_flight": m.used[key], "weekly_threshold": m.weeklyThresholds[key], "weekly_quota": m.weeklyViewLocked(key, time.Now())})
	}
	active := make([]concurrencyRequest, 0, len(m.requests))
	for _, r := range m.requests {
		if r.Account != "" {
			active = append(active, *r)
		}
	}
	sort.Slice(active, func(i, j int) bool { return active[i].Started.After(active[j].Started) })
	recent := make([]concurrencyRequest, len(m.recent))
	for i, r := range m.recent {
		recent[len(recent)-1-i] = r
	}
	return evalJSON(200, map[string]any{"accounts": accounts, "active": active, "recent": recent, "storage_error": m.loadError, "max_limit": maxAccountConcurrency, "scope": "native-codex-single-process"})
}
