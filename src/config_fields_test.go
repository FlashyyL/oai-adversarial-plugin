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
	if len(seen) != 16 {
		t.Fatal("missing fields")
	}
	b, err := normalizePanelConfig([]byte("operation-mode: business-only\noverride-policy: always\noverride-models: [gpt-6-astra]\nprobe-interval-seconds: 10\nturn-state-override:\n  value: preserved\n  probe:\n    enabled: true\n    prefetch-minutes: 3\n"))
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
	if !c.Enabled || !c.Force || probeEnabled(c.Probe) || *c.Probe.PrefetchMinutes != 0 || *c.Probe.IntervalSeconds != 10 || c.Value != "preserved" {
		t.Fatal("alias merge failed")
	}
}

func TestPanelInvalidValuesRedacted(t *testing.T) {
	for _, input := range []string{"operation-mode: secret-invalid", "probe-interval-seconds: 0", "probe-timeout-seconds: 1.5", "override-models: [12]", "override-models: []", "probe-credential-file: https://secret-invalid", "prefetch-minutes: 55\nstate-ttl-minutes: 55", "turn-state-override: secret-invalid"} {
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
