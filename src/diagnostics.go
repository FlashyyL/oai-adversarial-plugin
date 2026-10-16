package main

// Opt-in bounded diagnostics for the upstream risk-policy investigation.
// Only structural metadata is recorded: header NAMES (lowercase, sorted),
// byte lengths, short SHA-256 prefixes for correlation, JSON top-level key
// names and SSE event "type" values. Ticket values, credentials, message
// text and any other header values are never written.
//
// The journal lives next to the state snapshot, is 0600, capped at
// diagnosticsMaxBytes and rotated to a single ".1" fallback file.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	diagnosticsFileName = "diagnostics.jsonl"
	diagnosticsBackup   = "diagnostics.jsonl.1"
	diagnosticsMaxBytes = 4 << 20
)

type diagnosticsSink struct {
	mu      sync.Mutex
	enabled bool
	file    *os.File
	size    int64
}

var diagnostics diagnosticsSink

// setDiagnosticsEnabled toggles the opt-in diagnostic journal. The flag is
// persisted through runtime-settings.json alongside the other dashboard
// overrides.
func setDiagnosticsEnabled(enabled bool) {
	diagnostics.mu.Lock()
	defer diagnostics.mu.Unlock()
	diagnostics.enabled = enabled
	if !enabled && diagnostics.file != nil {
		_ = diagnostics.file.Close()
		diagnostics.file = nil
		diagnostics.size = 0
	}
}

func diagnosticsEnabled() bool {
	diagnostics.mu.Lock()
	defer diagnostics.mu.Unlock()
	return diagnostics.enabled
}

// syncDiagnosticsFromSettings applies the persisted switch (missing = off).
func syncDiagnosticsFromSettings(settings runtimeSettings) {
	setDiagnosticsEnabled(settings.Diagnostics != nil && *settings.Diagnostics)
}

func diagnosticsDir() string {
	return filepath.Dir(stateFilePath())
}

func diagnosticsPath() string {
	return filepath.Join(diagnosticsDir(), diagnosticsFileName)
}

// diagnosticsEvent appends one JSON line; rotation keeps the journal bounded.
func diagnosticsEvent(kind string, fields map[string]any) {
	diagnostics.mu.Lock()
	defer diagnostics.mu.Unlock()
	if !diagnostics.enabled {
		return
	}
	if diagnostics.file == nil {
		if err := diagnostics.openLocked(); err != nil {
			return
		}
	}
	entry := make(map[string]any, len(fields)+2)
	entry["t"] = time.Now().UTC().Format(time.RFC3339Nano)
	entry["k"] = kind
	for key, value := range fields {
		entry[key] = value
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		return
	}
	payload = append(payload, '\n')
	if diagnostics.size+int64(len(payload)) > diagnosticsMaxBytes {
		_ = diagnostics.file.Close()
		diagnostics.file = nil
		_ = os.Rename(diagnosticsPath(), filepath.Join(diagnosticsDir(), diagnosticsBackup))
		if err := diagnostics.openLocked(); err != nil {
			return
		}
	}
	if _, err := diagnostics.file.Write(payload); err != nil {
		return
	}
	diagnostics.size += int64(len(payload))
}

// Caller holds diagnostics.mu.
func (d *diagnosticsSink) openLocked() error {
	file, err := os.OpenFile(diagnosticsPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	d.file = file
	d.size = info.Size()
	return nil
}

// diagnosticsHeaderNames lists header NAMES only (lowercase, sorted).
func diagnosticsHeaderNames(headers http.Header) []string {
	names := make([]string, 0, len(headers))
	for name := range headers {
		lower := strings.ToLower(strings.TrimSpace(name))
		if lower != "" {
			names = append(names, lower)
		}
	}
	sort.Strings(names)
	return names
}

// digestShort correlates values without storing them.
func digestShort(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}

// bodyShape reports top-level key names and the byte length only. Large
// bodies keep the length without JSON parsing.
func bodyShape(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	shape := map[string]any{"length": len(raw)}
	if len(raw) > 256<<10 {
		return shape
	}
	var root map[string]json.RawMessage
	if json.Unmarshal(raw, &root) == nil && len(root) > 0 {
		keys := make([]string, 0, len(root))
		for key := range root {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		shape["keys"] = keys
	}
	return shape
}

// diagnosticsEventTypes extracts SSE "type" values from a bounded payload scan.
func diagnosticsEventTypes(payload []byte, limit int) []string {
	if len(payload) == 0 || limit <= 0 {
		return nil
	}
	if len(payload) > 64<<10 {
		payload = payload[:64<<10]
	}
	types := make([]string, 0, limit)
	for _, line := range strings.Split(string(payload), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "data:") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var probe struct {
			Type string `json:"type"`
		}
		if json.Unmarshal([]byte(line), &probe) == nil && probe.Type != "" {
			types = append(types, probe.Type)
			if len(types) >= limit {
				break
			}
		}
	}
	return types
}

// diagnosticsErrorKind trims an error message and removes URL host/path
// details while keeping the trailing reason (for example the final "EOF").
func diagnosticsErrorKind(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return ""
	}
	if start := strings.Index(message, "://"); start >= 0 {
		rest := message[start+3:]
		if end := strings.IndexAny(rest, " \t\"'"); end >= 0 {
			message = message[:start+3] + "..." + rest[end:]
		} else {
			message = message[:start+3] + "..."
		}
	}
	if len(message) > 90 {
		message = message[:90]
	}
	return message
}
