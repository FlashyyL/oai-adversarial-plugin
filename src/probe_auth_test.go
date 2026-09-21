package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestHighestPriorityCredentialSelectionStaysAvailable(t *testing.T) {
	oldList, oldGet := hostAuthListFunc, hostAuthGetFunc
	t.Cleanup(func() { hostAuthListFunc = oldList; hostAuthGetFunc = oldGet })
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{
			{AuthIndex: "disabled", Provider: "codex", Disabled: true, Priority: 99},
			{AuthIndex: "low", Provider: "codex", Email: "low@example.com", Priority: 5, Success: 10},
			{AuthIndex: "high-b", Provider: "codex", Email: "bravo@example.com", Priority: 20, Success: 1, LastRefresh: time.Unix(20, 0)},
			{AuthIndex: "high-a", Provider: "codex", Email: "alpha@example.com", Priority: 20, Success: 2, LastRefresh: time.Unix(10, 0)},
		}, nil
	}
	hostAuthGetFunc = func(index string) (json.RawMessage, error) {
		return json.RawMessage(`{"access_token":"token-` + index + `","account_id":"account"}`), nil
	}
	e := &probeEngine{}
	cfg := probeConfig{AccountMode: "highest-priority", CandidateLimit: 5}
	cred, err := e.resolveProbeCredential(cfg)
	if err != nil || cred.AuthIndex != "high-a" || cred.Priority != 20 || cred.Label != "alp***@example.com" {
		t.Fatalf("first=%+v err=%v", cred, err)
	}
	cred, err = e.resolveProbeCredential(cfg)
	if err != nil || cred.AuthIndex != "high-a" {
		t.Fatalf("fallback=%+v err=%v", cred, err)
	}
}

func TestRenewalCredentialNeverFallsBackToDifferentAccount(t *testing.T) {
	oldList, oldGet := hostAuthListFunc, hostAuthGetFunc
	t.Cleanup(func() { hostAuthListFunc, hostAuthGetFunc = oldList, oldGet })
	disabled := false
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{{ID: "other", AuthIndex: "other", Provider: "codex", Priority: 99}, {ID: "owner", AuthIndex: "owner-index", Provider: "codex", Priority: 1, Disabled: disabled}}, nil
	}
	hostAuthGetFunc = func(index string) (json.RawMessage, error) {
		if index != "owner-index" {
			t.Fatal("renewal selected another account")
		}
		return json.RawMessage(`{"access_token":"synthetic-test-token"}`), nil
	}
	e := &probeEngine{}
	cfg := probeConfig{AccountMode: "highest-priority", TargetAuthID: "owner", CandidateLimit: 1}
	cred, err := e.resolveProbeCredential(cfg)
	if err != nil || cred.AuthID != "owner" {
		t.Fatal("renewal did not select its target")
	}
	disabled = true
	if _, err = e.resolveProbeCredential(cfg); err == nil {
		t.Fatal("disabled target must not fall back to another account")
	}
	cfg.AccountMode = "fixed"
	if _, err = e.resolveProbeCredential(cfg); err == nil {
		t.Fatal("mode change must not switch renewal to fixed credentials")
	}
}

func TestHighestPriorityCredentialRejectsInvalidAndNonCodex(t *testing.T) {
	oldList, oldGet := hostAuthListFunc, hostAuthGetFunc
	t.Cleanup(func() { hostAuthListFunc = oldList; hostAuthGetFunc = oldGet })
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{{AuthIndex: "other", Provider: "anthropic", Priority: 100}, {AuthIndex: "bad", Provider: "codex", Priority: 10}, {AuthIndex: "good", Type: "codex", Priority: 1}}, nil
	}
	hostAuthGetFunc = func(index string) (json.RawMessage, error) {
		if index == "bad" {
			return json.RawMessage(`{"account_id":"missing-token"}`), nil
		}
		return json.RawMessage(`{"access_token":"ok","account_id":"account"}`), nil
	}
	e := &probeEngine{}
	cred, err := e.resolveProbeCredential(probeConfig{AccountMode: "highest-priority", CandidateLimit: 3})
	if err != nil || cred.AuthIndex != "good" {
		t.Fatalf("credential=%+v err=%v", cred, err)
	}
}

func TestAuthSelectionScanTracksEnabledAccountChanges(t *testing.T) {
	oldList := hostAuthListFunc
	t.Cleanup(func() { hostAuthListFunc = oldList })
	selected := "first"
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{{AuthIndex: selected, Provider: "codex", Priority: 10}}, nil
	}
	e := &probeEngine{
		cfg:    probeConfigState{Config: probeConfig{Enabled: true, AccountMode: "highest-priority", Prefetch: time.Minute, Models: []string{"gpt-6-astra"}}},
		paused: map[string]bool{"gpt-6-astra": true}, probing: map[string]bool{},
	}
	e.authSelectionScan()
	if !e.autoAuthSeen || e.lastAutoAuth != "first" || len(e.queue) != 0 {
		t.Fatal("initial snapshot must not probe")
	}
	selected = "second"
	e.authSelectionScan()
	if e.lastAutoAuth != "second" || len(e.queue) != 0 {
		t.Fatal("account change was not tracked safely")
	}
}
