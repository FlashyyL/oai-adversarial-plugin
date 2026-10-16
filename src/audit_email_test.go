package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestAuditEmailSurvivesRestartAndDisabledAccount(t *testing.T) {
	accountTestEngine(t)
	scopedRequest(t, "fixture-A", "")
	savePersistedState()
	raw, err := os.ReadFile(stateFilePath())
	if err != nil {
		t.Fatal(err)
	}
	var saved persistedState
	if err = json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.Records) != 1 || saved.Records[0].AccountEmail != "alpha@example.test" {
		t.Fatalf("request email was not persisted: %+v", saved.Records)
	}
	// Restart must retain the display value even when the auth is now disabled.
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{{ID: "fixture-A", AuthIndex: "index-A", Provider: "codex", Disabled: true}}, nil
	}
	history.mu.Lock()
	history.records = nil
	history.mu.Unlock()
	applyPersistedState(saved)
	rows := history.snapshot()["records"].([]auditRecord)
	if len(rows) != 1 || rows[0].AccountEmail != "alpha@example.test" {
		t.Fatal("restored email was overwritten by an empty inventory")
	}
	if rows[0].AccountScope != "" || rows[0].AuthBinding != "" || responseAccount(rows[0].RequestID, "fixture-A") != "" {
		t.Fatal("email display restored a live credential binding")
	}
}

func TestAuditEmailLegacyLabelRecoveredAndPersisted(t *testing.T) {
	accountTestEngine(t)
	legacy := auditRecord{RequestID: "legacy-email", Account: targetAccount(accountTarget("fixture-A")), Model: "gpt-6-astra", Time: "2026-10-01T00:00:00Z"}
	history.record(legacy)
	// collectState must recover the display metadata even before a page GET.
	savePersistedState()
	raw, err := os.ReadFile(stateFilePath())
	if err != nil {
		t.Fatal(err)
	}
	var saved persistedState
	if err = json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.Records) != 1 || saved.Records[0].AccountEmail != "alpha@example.test" {
		t.Fatal("legacy email not recovered into private snapshot")
	}
	history.mu.Lock()
	memory := history.records[0]
	history.mu.Unlock()
	if memory.AccountEmail != "alpha@example.test" || memory.AccountScope != "" || memory.AuthBinding != "" {
		t.Fatal("display recovery changed credential ownership or was not retained")
	}
	hostAuthListFunc = func() ([]hostAuthEntry, error) { return nil, nil }
	history.mu.Lock()
	history.records = nil
	history.mu.Unlock()
	applyPersistedState(saved)
	rows := history.snapshot()["records"].([]auditRecord)
	if rows[0].AccountEmail != "alpha@example.test" || responseAccount("legacy-email", "fixture-A") != "" {
		t.Fatal("restart lost metadata or restored a live binding")
	}
}

func TestAuditEmailLookupRejectsAmbiguousAndChangedScopes(t *testing.T) {
	scopeA := "auth-" + strings.Repeat("1", 12) + strings.Repeat("a", 52)
	scopeB := "auth-" + strings.Repeat("1", 12) + strings.Repeat("b", 52)
	label := targetAccount(scopeA + "/gpt-6-astra")
	lookup := auditEmails(map[string]hostAuthEntry{scopeA: {Email: "alpha@example.test"}, scopeB: {Email: "beta@example.test"}})
	if got := lookup.email(auditRecord{Account: label}); got != "" {
		t.Fatal("ambiguous label guessed an account email")
	}
	lookup = auditEmails(map[string]hostAuthEntry{scopeA: {Email: "alpha@example.test"}})
	if got := lookup.email(auditRecord{AccountScope: scopeB, Account: label}); got != "" {
		t.Fatal("mismatched full scope used a short-label fallback")
	}
	if got := lookup.email(auditRecord{Account: label}); got != "alpha@example.test" {
		t.Fatal("unique legacy display association missing")
	}
	if got := lookup.email(auditRecord{Account: "auth-unknown"}); got != "" {
		t.Fatal("unknown account guessed an email")
	}
}

func TestAuditEmailCapturedValueSurvivesRosterChanges(t *testing.T) {
	scope := credentialScope("fixture-A", "synthetic-index-A", "workspace-index-A")
	lookup := auditEmails(map[string]hostAuthEntry{scope: {Email: "new@example.test"}})
	r := auditRecord{AccountScope: scope, Account: targetAccount(scope + "/model"), AccountEmail: "original@example.test"}
	if lookup.email(r) != "original@example.test" {
		t.Fatal("historical display changed with current inventory")
	}
	if (auditEmails(nil)).email(r) != "original@example.test" {
		t.Fatal("disabled/removed credential lost captured email")
	}
	if lookup.email(auditRecord{Account: r.Account, RequestKind: "evaluation"}) != "" {
		t.Fatal("missing evaluation identity was fabricated")
	}
}
