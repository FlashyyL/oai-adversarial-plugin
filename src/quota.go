package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Only error envelopes are inspected; ordinary generated text is not evidence.
func quotaFailure(status int, body string) string {
	var envelope struct {
		Error struct {
			Code string `json:"code"`
			Type string `json:"type"`
		} `json:"error"`
		Response struct {
			Error struct {
				Code string `json:"code"`
				Type string `json:"type"`
			} `json:"error"`
		} `json:"response"`
	}
	body = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(body), "data:"))
	_ = json.Unmarshal([]byte(body), &envelope)
	for _, code := range []string{envelope.Error.Code, envelope.Error.Type, envelope.Response.Error.Code, envelope.Response.Error.Type} {
		switch strings.ToLower(code) {
		case "usage_limit_reached", "insufficient_quota", "quota_exceeded", "quota_exhausted", "billing_hard_limit_reached", "credits_exhausted", "insufficient_credits":
			return "quota_exhausted"
		case "rate_limit_exceeded", "rate_limit_error", "rate_limited":
			return "rate_limited"
		}
	}
	if status == http.StatusTooManyRequests {
		return "rate_limited"
	}
	return ""
}

func quotaRetryAt(kind string, headers http.Header, body string, now time.Time) time.Time {
	fallback := now.Add(time.Minute)
	if kind == "quota_exhausted" {
		fallback = now.Add(5 * time.Minute)
	}
	if value := headers.Get("Retry-After"); value != "" {
		if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds > 0 && seconds <= 86400 {
			return now.Add(time.Duration(seconds * float64(time.Second)))
		}
		if date, err := http.ParseTime(value); err == nil && date.After(now) && date.Before(now.Add(24*time.Hour)) {
			return date
		}
	}
	var envelope struct {
		Error struct {
			ResetsAt        int64 `json:"resets_at"`
			ResetsInSeconds int64 `json:"resets_in_seconds"`
		} `json:"error"`
	}
	_ = json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(body), "data:"))), &envelope)
	if seconds := envelope.Error.ResetsInSeconds; seconds > 0 && seconds <= 86400 {
		return now.Add(time.Duration(seconds) * time.Second)
	}
	if date := time.Unix(envelope.Error.ResetsAt, 0); date.After(now) && date.Before(now.Add(24*time.Hour)) {
		return date
	}
	return fallback
}

func isQuotaState(state string) bool { return state == "quota_exhausted" || state == "rate_limited" }

func (s *auditState) observeQuota(requestID, kind string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.records {
		if s.records[i].RequestID == requestID {
			s.records[i].QuotaStatus = kind
			s.records[i].ModelMismatch = false
			break
		}
	}
	markStateDirty()
}

func applyQuotaEvidence(entry *accountModelHealth, kind string, until time.Time) {
	entry.State = kind
	entry.LastReason = kind
	entry.CooldownUntil = until
	entry.ConsecutiveFailures = 0
	// Keep the account's previously confirmed lease. Quota evidence says
	// nothing about its model capability and must not renew its TTL.
}

func (m *accountRoutingManager) observeQuota(authID, model, kind string, until, now time.Time) {
	if authID == "" || routingModelKey(model) == "" || kind == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.health == nil {
		m.health = map[string]accountModelHealth{}
	}
	key := accountHealthKey(authID, routingModelKey(model))
	entry := m.health[key]
	entry.AuthID, entry.Account, entry.Model = authID, publicAccountID(authID), routingModelKey(model)
	entry.ObservedAt = now
	entry.LastStatusCode = http.StatusTooManyRequests
	applyQuotaEvidence(&entry, kind, until)
	m.health[key] = entry
	markStateDirty()
}

func (m *accountRoutingManager) quotaBlock(authID, model string, now time.Time) (string, time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.config.Config.Enabled || m.config.Error != "" {
		return "", time.Time{}
	}
	entry := m.health[accountHealthKey(authID, routingModelKey(model))]
	if isQuotaState(entry.State) && now.Before(entry.CooldownUntil) {
		return entry.State, entry.CooldownUntil
	}
	return "", time.Time{}
}
