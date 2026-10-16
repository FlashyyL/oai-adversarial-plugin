package main

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
)

func TestPluginRenameKeepsStatePaths(t *testing.T) {
	if pluginID != "oai-adversarial-plugin" {
		t.Fatalf("plugin ID does not match repository: %s", pluginID)
	}
	t.Setenv("LKS_TZ_STATE_FILE", "")
	const legacyState = "/CLIProxyAPI/logs/.plugins/timezone-override/state.json"
	if stateFilePath() != legacyState {
		t.Fatal("renaming must not orphan existing snapshots")
	}
	for _, statePath := range []string{legacyState, filepath.Join(t.TempDir(), "isolated.json")} {
		t.Setenv("LKS_TZ_STATE_FILE", statePath)
		if stateFilePath() != statePath || runtimeSettingsPath() != filepath.Join(filepath.Dir(statePath), "runtime-settings.json") || diagnosticsDir() != filepath.Dir(statePath) {
			t.Fatal("state, settings and diagnostics must share the same directory")
		}
	}
}

func TestPluginRenameControlAliases(t *testing.T) {
	for _, id := range []string{pluginID, legacyPluginID} {
		path := "/v0/management/" + id + "/probe-control"
		for _, tc := range []struct {
			method string
			status int
		}{
			{http.MethodGet, http.StatusMethodNotAllowed},
			{http.MethodPost, http.StatusBadRequest},
		} {
			// Unknown action validates routing without changing user mode or
			// settings and without scheduling any real upstream request.
			raw, _ := json.Marshal(managementRequest{Method: tc.method, Path: path, Body: []byte(`{"action":"rename-test-unknown"}`)})
			response, err := management(raw)
			if err != nil || response.StatusCode != tc.status {
				t.Fatalf("alias %s: status %d, error %v", path, response.StatusCode, err)
			}
		}
	}
}
