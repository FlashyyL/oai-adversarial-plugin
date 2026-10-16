package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const weeklyQuotaURL = "https://chatgpt.com/backend-api/wham/usage"
const weeklyQuotaInterval = 10 * time.Minute

// Only sanitized observations are persisted; the credential is never retained.
type weeklyQuotaSnapshot struct {
	Scope       string    `json:"scope"`
	UsedPercent float64   `json:"used_percent"`
	ResetAt     time.Time `json:"reset_at"`
	ObservedAt  time.Time `json:"observed_at"`
	CheckedAt   time.Time `json:"checked_at"`
}

func validWeeklyThreshold(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 && v <= 100
}
func validWeeklyPercent(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 && v <= 10000
}
func weeklyQuotaError(code string) bool {
	return code == "account_weekly_quota_exceeded" || code == "account_weekly_quota_unavailable"
}

func (m *concurrencyManager) weeklyGateLocked(key string, now time.Time) string {
	threshold := m.weeklyThresholds[key]
	if threshold <= 0 {
		return ""
	}
	snapshot, ok := m.weeklyUsage[key]
	if !ok || snapshot.CheckedAt.After(now) || (!snapshot.ResetAt.IsZero() && !now.Before(snapshot.ResetAt)) {
		return "account_weekly_quota_unavailable"
	}
	if snapshot.UsedPercent >= threshold {
		return "account_weekly_quota_exceeded"
	}
	return ""
}

func (m *concurrencyManager) weeklyViewLocked(key string, now time.Time) map[string]any {
	code := m.weeklyGateLocked(key, now)
	state := "ready"
	if m.weeklyThresholds[key] <= 0 {
		state = "disabled"
	} else if code == "account_weekly_quota_exceeded" {
		state = "paused"
	} else if code != "" {
		state = "unknown"
	}
	view := map[string]any{"state": state, "error": m.weeklyErrors[key], "storage_error": m.weeklyStorageError}
	if q, ok := m.weeklyUsage[key]; ok {
		view["used_percent"], view["updated_at"] = q.UsedPercent, q.CheckedAt
		if !q.ResetAt.IsZero() {
			view["reset_at"] = q.ResetAt
		}
		view["stale"] = now.Sub(q.CheckedAt) >= weeklyQuotaInterval
	}
	return view
}

func (m *concurrencyManager) saveWeeklyThreshold(key string, threshold float64) error {
	if !validConcurrencyAccount(key) || !validWeeklyThreshold(threshold) {
		return errors.New("invalid_weekly_threshold")
	}
	return m.saveSettings(key, nil, &threshold)
}

func (m *concurrencyManager) checkWeeklyIdentity(key, scope string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if q, ok := m.weeklyUsage[key]; ok && (scope == "" || q.Scope != scope) {
		delete(m.weeklyUsage, key)
		delete(m.weeklyNext, key)
		m.weeklyDirty = true
		m.notifyLocked()
		select {
		case m.weeklyWake <- struct{}{}:
		default:
		}
	}
}

// Order by the request's start, not callback arrival. A late older request may
// not undo a newer quota query. A different credential scope may not borrow it.
func (m *concurrencyManager) observeWeekly(key string, q weeklyQuotaSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadLocked()
	if m.loadError != "" || m.weeklyThresholds[key] <= 0 {
		return
	}
	if previous, ok := m.weeklyUsage[key]; ok && (previous.Scope != q.Scope || q.ObservedAt.Before(previous.ObservedAt)) {
		return
	}
	if len(m.weeklyUsage) >= 4096 {
		if _, ok := m.weeklyUsage[key]; !ok {
			return
		}
	}
	m.weeklyUsage[key] = q
	delete(m.weeklyErrors, key)
	m.weeklyDirty = true
	m.notifyLocked()
}

func weeklyQuotaFromHeaders(headers http.Header, started, received time.Time) (weeklyQuotaSnapshot, bool) {
	for _, window := range []string{"primary", "secondary"} {
		prefix := "x-codex-" + window + "-"
		minutes, err := strconv.ParseInt(headerValue(headers, prefix+"window-minutes"), 10, 64)
		if err != nil || minutes != 7*24*60 {
			continue
		}
		used, err := strconv.ParseFloat(headerValue(headers, prefix+"used-percent"), 64)
		if err != nil || !validWeeklyPercent(used) {
			return weeklyQuotaSnapshot{}, false
		}
		q := weeklyQuotaSnapshot{UsedPercent: used, ObservedAt: started, CheckedAt: received}
		if seconds, err := strconv.ParseInt(headerValue(headers, prefix+"reset-after-seconds"), 10, 64); err == nil && seconds >= 0 && seconds <= 31*24*60*60 {
			q.ResetAt = received.Add(time.Duration(seconds) * time.Second)
		}
		return q, true
	}
	return weeklyQuotaSnapshot{}, false
}

func observeRequestWeekly(requestID, scope string, headers http.Header) {
	if scope == "" {
		return
	}
	history.mu.Lock()
	key := ""
	started := time.Time{}
	for _, record := range history.records {
		if record.RequestID == requestID && record.AccountScope == scope {
			key, started = record.AuthBinding, record.performanceAttemptAt
			break
		}
	}
	history.mu.Unlock()
	if key == "" || started.IsZero() {
		return
	}
	if q, ok := weeklyQuotaFromHeaders(headers, started, time.Now()); ok {
		q.Scope = scope
		accountConcurrency.observeWeekly(key, q)
	}
}

func weeklyQuotaFromJSON(raw []byte, started, received time.Time) (weeklyQuotaSnapshot, error) {
	type window struct {
		Used       *float64 `json:"used_percent"`
		Seconds    int64    `json:"limit_window_seconds"`
		ResetAt    *int64   `json:"reset_at"`
		ResetAfter *int64   `json:"reset_after_seconds"`
	}
	var payload struct {
		RateLimit *struct {
			Primary   *window `json:"primary_window"`
			Secondary *window `json:"secondary_window"`
		} `json:"rate_limit"`
	}
	if len(raw) > 128<<10 || json.Unmarshal(raw, &payload) != nil || payload.RateLimit == nil {
		return weeklyQuotaSnapshot{}, errors.New("weekly_quota_invalid_response")
	}
	for _, w := range []*window{payload.RateLimit.Primary, payload.RateLimit.Secondary} {
		if w == nil || w.Seconds != 7*24*60*60 {
			continue
		}
		if w.Used == nil || !validWeeklyPercent(*w.Used) {
			break
		}
		q := weeklyQuotaSnapshot{UsedPercent: *w.Used, ObservedAt: started, CheckedAt: received}
		if w.ResetAt != nil && *w.ResetAt > 0 && *w.ResetAt < 253402300799 {
			q.ResetAt = time.Unix(*w.ResetAt, 0).UTC()
		} else if w.ResetAfter != nil && *w.ResetAfter >= 0 && *w.ResetAfter <= 31*24*60*60 {
			q.ResetAt = received.Add(time.Duration(*w.ResetAfter) * time.Second)
		}
		if !q.ResetAt.IsZero() && !q.ResetAt.After(received) {
			break
		}
		return q, nil
	}
	return weeklyQuotaSnapshot{}, errors.New("weekly_quota_invalid_response")
}

var weeklyQuotaFetch = fetchWeeklyQuota

type weeklyQuotaCredential struct {
	probeCredential
	ProxyURL string `json:"proxy_url"`
}

func weeklyCredential(entry hostAuthEntry) (weeklyQuotaCredential, string, error) {
	entries, err := hostAuthListFunc()
	if err == nil {
		for _, current := range entries {
			if current.ID == entry.ID && current.AuthIndex == entry.AuthIndex && !current.Disabled && nativeCodexEntry(current) {
				raw, err := hostAuthGetFunc(current.AuthIndex)
				if err != nil {
					break
				}
				var cred weeklyQuotaCredential
				if json.Unmarshal(raw, &cred) == nil && strings.TrimSpace(cred.AccessToken) != "" {
					cred.ProxyURL = strings.TrimSpace(cred.ProxyURL)
					return cred, credentialScope(current.ID, cred.AccessToken, cred.AccountID), nil
				}
			}
		}
	}
	return weeklyQuotaCredential{}, "", errors.New("weekly_quota_account_unavailable")
}

func fetchWeeklyQuota(ctx context.Context, entry hostAuthEntry) (weeklyQuotaSnapshot, error) {
	cred, scope, err := weeklyCredential(entry)
	if err != nil || ctx.Err() != nil {
		return weeklyQuotaSnapshot{}, errors.New("weekly_quota_account_unavailable")
	}
	started := time.Now()
	headers := http.Header{"Authorization": {"Bearer " + cred.AccessToken}, "Accept": {"application/json"}, "User-Agent": {"codex-tui/0.154.0"}, "Originator": {"codex-tui"}, "OpenAI-Beta": {"codex-1"}}
	if cred.AccountID != "" {
		headers.Set("Chatgpt-Account-Id", cred.AccountID)
	}
	// An explicit credential route must not silently use the global route.
	body, err := weeklyQuotaHTTP(ctx, cred.ProxyURL, headers)
	if err != nil || ctx.Err() != nil {
		if err != nil {
			return weeklyQuotaSnapshot{}, err
		}
		return weeklyQuotaSnapshot{}, errors.New("weekly_quota_query_failed")
	}
	confirmedCredential, confirmed, err := weeklyCredential(entry)
	if err != nil || confirmed != scope || confirmedCredential.ProxyURL != cred.ProxyURL {
		return weeklyQuotaSnapshot{}, errors.New("weekly_quota_account_unavailable")
	}
	q, err := weeklyQuotaFromJSON(body, started, time.Now())
	q.Scope = scope
	return q, err
}

func (m *concurrencyManager) activateWeekly() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadLocked()
	m.weeklyActive = true
	m.startWeeklyLocked()
}

func (m *concurrencyManager) startWeeklyLocked() {
	if !m.weeklyActive || m.weeklyCancel != nil || m.loadError != "" {
		return
	}
	enabled := false
	for _, threshold := range m.weeklyThresholds {
		enabled = enabled || threshold > 0
	}
	if !enabled {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.weeklyCancel = cancel
	m.weeklyWG.Add(1)
	go func() {
		defer m.weeklyWG.Done()
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			m.refreshWeekly(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-m.weeklyWake:
			}
		}
	}()
}

func (m *concurrencyManager) stopWeekly() {
	m.mu.Lock()
	m.weeklyActive = false
	if m.weeklyCancel != nil {
		m.weeklyCancel()
	}
	m.mu.Unlock()
	m.weeklyWG.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.weeklyCancel = nil
	if m.weeklyDirty && m.loadError == "" {
		if err := m.persistSettingsLocked(m.limits, m.weeklyThresholds); err != nil {
			m.weeklyStorageError = "weekly_quota_save_failed"
		}
	}
}

func (m *concurrencyManager) refreshWeekly(ctx context.Context) {
	m.mu.Lock()
	enabled := false
	for _, threshold := range m.weeklyThresholds {
		enabled = enabled || threshold > 0
	}
	m.mu.Unlock()
	if enabled && ctx.Err() == nil {
		entries, err := concurrencyInventory()
		if err == nil {
			for _, entry := range entries {
				if ctx.Err() != nil {
					break
				}
				key := accountScope(entry.ID)
				m.mu.Lock()
				due := !entry.Disabled && m.weeklyThresholds[key] > 0 && !time.Now().Before(m.weeklyNext[key])
				m.mu.Unlock()
				if !due {
					continue
				}
				_, scope, identityErr := weeklyCredential(entry)
				if identityErr == nil {
					m.checkWeeklyIdentity(key, scope)
				}
				q, err := weeklyQuotaFetch(ctx, entry)
				if ctx.Err() != nil {
					break
				}
				if err == nil {
					m.observeWeekly(key, q)
				}
				m.mu.Lock()
				next := time.Now().Add(weeklyQuotaInterval)
				if err != nil {
					m.weeklyErrors[key] = err.Error()
					next = time.Now().Add(time.Minute)
				} else if !q.ResetAt.IsZero() && q.ResetAt.Before(next) {
					next = q.ResetAt
				}
				m.weeklyNext[key] = next
				m.mu.Unlock()
			}
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.weeklyDirty && m.loadError == "" {
		if err := m.persistSettingsLocked(m.limits, m.weeklyThresholds); err != nil {
			m.weeklyStorageError = "weekly_quota_save_failed"
		}
	}
}
