package main

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestAccountSummaryUsesCurrentEnabledInventory(t *testing.T) {
	setupSessionGuardTest(t, sessionGuardModeObserve)
	now := time.Now().UTC()
	for _, id := range []string{"enabled", "disabled", "deleted", "temporary"} {
		setHealthyAccountState(accountRouter, id, "astra", strings.Repeat("x", 332), now)
	}
	old := hostAuthListFunc
	t.Cleanup(func() { hostAuthListFunc = old })
	hostAuthListFunc = func() ([]hostAuthEntry, error) {
		return []hostAuthEntry{
			{ID: "enabled", Provider: "codex"},
			{ID: "disabled", Provider: "codex", Disabled: true},
			{ID: "temporary", Provider: "codex", Unavailable: true, Status: "error"},
		}, nil
	}
	summary := accountRoutingSummary()
	rows := summary["accounts"].([]accountModelHealth)
	if summary["inventory_available"] != true || len(rows) != 2 || summary["hidden_history_entries"] != 2 {
		t.Fatalf("inventory filtering failed: rows=%d", len(rows))
	}
	for _, row := range rows {
		if row.Account != publicAccountID("enabled") && row.Account != publicAccountID("temporary") {
			t.Fatal("disabled or deleted account shown")
		}
	}
	hostAuthListFunc = func() ([]hostAuthEntry, error) { return nil, errors.New("synthetic host failure") }
	summary = accountRoutingSummary()
	if summary["inventory_available"] != false || len(summary["accounts"].([]accountModelHealth)) != 0 {
		t.Fatal("unconfirmed inventory displayed historical accounts")
	}
	// Inventory outage does not erase ownership or protection history.
	if len(accountRouter.health) != 4 {
		t.Fatal("inventory read deleted persisted health")
	}
	hostAuthListFunc = func() ([]hostAuthEntry, error) { return []hostAuthEntry{{ID: "disabled", Provider: "codex"}}, nil }
	rows = accountRoutingSummary()["accounts"].([]accountModelHealth)
	if len(rows) != 1 || rows[0].Account != publicAccountID("disabled") {
		t.Fatal("re-enabled account not restored to view")
	}
}
