package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type hostAuthEntry struct {
	Scope          string    `json:"-"`
	Path           string    `json:"path"`
	ID             string    `json:"id"`
	AuthIndex      string    `json:"auth_index"`
	Name           string    `json:"name"`
	Type           string    `json:"type"`
	Provider       string    `json:"provider"`
	Label          string    `json:"label"`
	Status         string    `json:"status"`
	Disabled       bool      `json:"disabled"`
	Unavailable    bool      `json:"unavailable"`
	Priority       int       `json:"priority"`
	Success        int64     `json:"success"`
	Failed         int64     `json:"failed"`
	LastRefresh    time.Time `json:"last_refresh"`
	NextRetryAfter time.Time `json:"next_retry_after"`
	Email          string    `json:"email"`
}
type hostAuthListResponse struct {
	Files []hostAuthEntry `json:"files"`
}
type hostAuthGetResponse struct {
	JSON json.RawMessage `json:"json"`
}

var hostAuthListFunc = func() ([]hostAuthEntry, error) {
	raw, err := callHost("host.auth.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var response hostAuthListResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("decode auth list")
	}
	return response.Files, nil
}
var hostAuthGetFunc = func(index string) (json.RawMessage, error) {
	raw, err := callHost("host.auth.get", map[string]string{"auth_index": index})
	if err != nil {
		return nil, err
	}
	var response hostAuthGetResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("decode auth credential")
	}
	return response.JSON, nil
}

func (e *probeEngine) resolveProbeCredential(cfg probeConfig) (probeCredential, error) {
	if cfg.AccountMode != "highest-priority" && !(cfg.AccountMode == "all-accounts" && cfg.TargetAuthID != "") {
		if cfg.TargetAuthID != "" {
			return probeCredential{}, fmt.Errorf("account renewal requires automatic credential selection")
		}
		return readProbeCredential(cfg.CredFile)
	}
	entries, err := hostAuthListFunc()
	if err != nil {
		return probeCredential{}, fmt.Errorf("list CPA credentials: %w", err)
	}
	eligible := e.eligibleProbeAccounts(entries, time.Now())
	if cfg.TargetAuthID != "" {
		filtered := make([]hostAuthEntry, 0, 1)
		for _, entry := range eligible {
			if entry.ID == cfg.TargetAuthID {
				filtered = append(filtered, entry)
			}
		}
		eligible = filtered
	}
	limit := cfg.CandidateLimit
	if limit <= 0 || limit > len(eligible) {
		limit = len(eligible)
	}
	for _, entry := range eligible[:limit] {
		raw, err := hostAuthGetFunc(entry.AuthIndex)
		if err != nil {
			continue
		}
		var cred probeCredential
		if json.Unmarshal(raw, &cred) != nil || strings.TrimSpace(cred.AccessToken) == "" {
			continue
		}
		cred.AuthIndex = entry.AuthIndex
		cred.AuthID = entry.ID
		cred.Priority = entry.Priority
		cred.Label = maskedAccountLabel(entry)
		return cred, nil
	}
	return probeCredential{}, fmt.Errorf("no eligible CPA credential")
}

func (e *probeEngine) eligibleProbeAccounts(entries []hostAuthEntry, now time.Time) []hostAuthEntry {
	eligible := make([]hostAuthEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.AuthIndex == "" || !authEntryEnabled(entry) {
			continue
		}
		eligible = append(eligible, entry)
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		if eligible[i].Priority != eligible[j].Priority {
			return eligible[i].Priority > eligible[j].Priority
		}
		si, sj := eligible[i].Success-eligible[i].Failed, eligible[j].Success-eligible[j].Failed
		if si != sj {
			return si > sj
		}
		if !eligible[i].LastRefresh.Equal(eligible[j].LastRefresh) {
			return eligible[i].LastRefresh.After(eligible[j].LastRefresh)
		}
		return eligible[i].AuthIndex < eligible[j].AuthIndex
	})
	return eligible
}

// The watcher reads credential metadata every ten seconds but sends no model
// traffic unless the highest-ranked active credential actually changes.
func (e *probeEngine) authSelectionWatchLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			e.syncProbeAccounts()
			e.authSelectionScan()
		}
	}
}

func (e *probeEngine) authSelectionScan() {
	e.mu.Lock()
	allAccounts := e.cfg.Config.AccountMode == "all-accounts"
	e.mu.Unlock()
	if allAccounts {
		e.prefetchScan()
		return
	}
	e.mu.Lock()
	cfg := e.cfg.Config
	suppressed := !cfg.Enabled || cfg.Prefetch <= 0 || !cfg.SleepHours.until(time.Now()).IsZero() || cfg.AccountMode != "highest-priority" || e.cfg.Error != "" || e.halted || e.stopping || e.shuttingDown
	e.mu.Unlock()
	if suppressed {
		return
	}
	e.syncProbeAccounts()
	entries, err := hostAuthListFunc()
	if err != nil {
		return
	}
	eligible := e.eligibleProbeAccounts(entries, time.Now())
	if len(eligible) == 0 {
		return
	}
	selected := eligible[0].AuthIndex
	e.mu.Lock()
	defer e.mu.Unlock()
	// The host callback runs without our mutex. Recheck operator settings
	// before enqueueing so a concurrent stop or zero window cannot be lost.
	cfg = e.cfg.Config
	if !cfg.Enabled || cfg.Prefetch <= 0 || !cfg.SleepHours.until(time.Now()).IsZero() || cfg.AccountMode != "highest-priority" || e.cfg.Error != "" || e.halted || e.stopping || e.shuttingDown {
		return
	}
	selectedScope := ""
	for _, entry := range e.accounts {
		if entry.ID == eligible[0].ID && authEntryAvailable(entry, time.Now()) {
			selectedScope = entry.Scope
			break
		}
	}
	if !e.autoAuthSeen {
		e.autoAuthSeen = true
		e.lastAutoAuth = selected
		e.lastAutoScope = selectedScope
		return
	}
	if selected == e.lastAutoAuth && selectedScope == e.lastAutoScope {
		return
	}
	e.lastAutoAuth = selected
	e.lastAutoScope = selectedScope
	for _, model := range cfg.Models {
		if degradationDetectionEnabled(model, "") {
			for _, entry := range e.accounts {
				if entry.ID == eligible[0].ID && entry.Scope != "" && authEntryAvailable(entry, time.Now()) {
					e.enqueueTaskLocked(scopedTarget(entry.Scope, model), true)
				}
			}
		}
	}
}

func maskedAccountLabel(entry hostAuthEntry) string {
	value := entry.Email
	if value == "" {
		value = entry.Label
	}
	if value == "" {
		value = entry.Name
	}
	parts := strings.SplitN(value, "@", 2)
	if len(parts) == 2 {
		local := parts[0]
		if len(local) > 3 {
			local = local[:3] + "***"
		}
		return local + "@" + parts[1]
	}
	if len(value) > 8 {
		return value[:5] + "***"
	}
	return "account"
}
