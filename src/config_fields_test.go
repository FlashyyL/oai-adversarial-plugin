package main

import (
	"gopkg.in/yaml.v3"
	"net/http"
	"strings"
	"testing"
)

func TestPanelSchemaAndAliases(t *testing.T) {
	seen := map[string]bool{}
	for _, f := range visualConfigFields() {
		name := f["Name"].(string)
		if seen[name] || strings.Contains(name, ".") {
			t.Fatalf("invalid field %s", name)
		}
		seen[name] = true
	}
	if !seen["accepted-state-lengths"] || !seen["timezone"] || len(seen) != 26 {
		t.Fatal("missing fields")
	}
	b, err := normalizePanelConfig([]byte("operation-mode: business-only\nsession-guard-mode: enforce\nsession-provenance-ttl-minutes: 90\noverride-policy: always\noverride-models: [gpt-6-astra]\nprobe-interval-seconds: 10\nturn-state-override:\n  value: preserved\n  probe:\n    enabled: true\n    prefetch-minutes: 3\n"))
	if err != nil {
		t.Fatal(err)
	}
	var root struct {
		Config turnStateOverrideConfig `yaml:"turn-state-override"`
	}
	if err = yaml.Unmarshal(b, &root); err != nil {
		t.Fatal(err)
	}
	c := root.Config
	if !c.Enabled || !c.Force || c.SessionGuardMode != sessionGuardModeEnforce || c.SessionProvenanceTTLMinutes != 90 || probeEnabled(c.Probe) || *c.Probe.PrefetchMinutes != 0 || *c.Probe.IntervalSeconds != 10 || c.Value != "preserved" {
		t.Fatal("alias merge failed")
	}
}

func TestHighestPriorityPanelConfig(t *testing.T) {
	b, err := normalizePanelConfig([]byte("operation-mode: probe\nprobe-account-mode: highest-priority\nprobe-candidate-limit: 4\nprobe-auth-cooldown-minutes: 30\nturn-state-override:\n  models: [gpt-6-astra]\n  probe:\n    cred-file: /old/fixed.json\n"))
	if err != nil {
		t.Fatal(err)
	}
	var root struct {
		Config turnStateOverrideConfig `yaml:"turn-state-override"`
	}
	if yaml.Unmarshal(b, &root) != nil {
		t.Fatal("decode")
	}
	cfg := parseProbeConfig(root.Config.Probe)
	if !cfg.Enabled || cfg.AccountMode != "highest-priority" || cfg.CandidateLimit != 4 || root.Config.Probe.CredFile != "" {
		t.Fatal("automatic mode not applied")
	}
}

func TestPanelInvalidValuesRedacted(t *testing.T) {
	for _, input := range []string{"operation-mode: secret-invalid", "session-guard-mode: secret-invalid", "session-provenance-ttl-minutes: 0", "probe-interval-seconds: 0", "probe-timeout-seconds: 1.5", "override-models: [12]", "override-models: []", "probe-credential-file: https://secret-invalid", "prefetch-minutes: 55\nstate-ttl-minutes: 55", "turn-state-override: secret-invalid"} {
		_, err := normalizePanelConfig([]byte(input))
		if err == nil || strings.Contains(err.Error(), "secret-invalid") {
			t.Fatalf("unsafe validation: %v", err)
		}
	}
}

func TestBusinessOnlyNoStaticValue(t *testing.T) {
	old := probeTrack
	probeTrack = &probeEngine{values: map[string]stateEntry{}, business: map[string]businessDegradation{}}
	t.Cleanup(func() { probeTrack = old })
	err := configureTurnStateOverride([]byte("operation-mode: business-only\noverride-models: [gpt-6-astra]\n"))
	if err != nil || currentTurnStateOverride().Error != "" {
		t.Fatalf("business-only rejected: %v", err)
	}
	if currentProbeConfig().Config.Enabled {
		t.Fatal("probe enabled")
	}
	// A developer machine may have a persisted production snapshot at the
	// default path. Reset only the counters under test after configuration has
	// loaded it; the assertion below then detects probes started by this mode,
	// not historical probes from another process.
	probeTrack.mu.Lock()
	probeTrack.probesTotal = 0
	probeTrack.probesOK = 0
	probeTrack.values = map[string]stateEntry{}
	probeTrack.candidates = map[string]stateEntry{}
	probeTrack.mu.Unlock()
	headers, status := applyTurnStateOverride("gpt-6-astra", "", http.Header{})
	if headers != nil || status != "" {
		t.Fatal("injected without baseline")
	}
	observeBusinessState("gpt-6-astra", strings.Repeat("x", 332), "gpt-6-astra")
	headers, status = applyTurnStateOverride("gpt-6-astra", "", http.Header{})
	if status != "applied" || len(headers.Get(turnStateHeader)) != 332 {
		t.Fatal("business baseline not used")
	}
	if probeTrack.probesTotal != 0 {
		t.Fatal("unexpected active probe")
	}
}
