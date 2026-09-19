package main

// This file implements the plugin's state persistence. All dashboard state
// (probe values, failures, early-trigger suspicions, paused models, the
// degraded-rejection switch, counters and the audit records) is kept in
// memory for serving; a JSON snapshot next to the plugin files survives
// plugin hot reloads and container restarts.
//
// The snapshot is written atomically (temp file + rename, 0600) from a
// background goroutine every persistFlushInterval when something changed,
// plus once on shutdown. It is read back on the first lifecycle call of a
// fresh plugin instance.
//
// Size: bounded by design (200 audit records, 200 probe history entries, 50 successful probes,
// 4096-byte state values) to roughly one megabyte, so a JSON file is both
// sufficient and simpler than a database.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	persistFlushInterval = 15 * time.Second
	persistFileMode      = 0o600
	persistDirMode       = 0o700
)

// persistedState is the on-disk snapshot. Optional fields use omitempty so a
// minimal snapshot (only the switch) stays tiny.
type persistedState struct {
	StateVersion   int             `json:"state_version"`
	SavedAt        string          `json:"saved_at,omitempty"`
	RejectDegraded *bool           `json:"reject_degraded,omitempty"`
	Halted         *bool           `json:"halted,omitempty"`
	Paused         []string        `json:"paused,omitempty"`
	Values         []stateEntry    `json:"values,omitempty"`
	Candidates     []stateEntry    `json:"candidates,omitempty"`
	Failures       []probeFailure  `json:"failures,omitempty"`
	Suspects       []probeSuspicion `json:"suspects,omitempty"`
	Business       []businessDegradation `json:"business,omitempty"`
	ExitPenalties  []exitPenalty   `json:"exit_penalties,omitempty"`
	DisabledExits  []string        `json:"disabled_exits,omitempty"`
	ProbesTotal    uint64          `json:"probes_total,omitempty"`
	ProbesOK       uint64          `json:"probes_ok,omitempty"`
	ProbeHistory   []probeRecord   `json:"probe_history,omitempty"`
	// Keep an explicit empty array to distinguish new snapshots from legacy ones.
	ProbeSuccessHistory []probeRecord `json:"probe_success_history"`
	Records        []auditRecord   `json:"records,omitempty"`
	Total          uint64          `json:"total,omitempty"`
	Inserted       uint64          `json:"inserted,omitempty"`
	Replaced       uint64          `json:"replaced,omitempty"`
}

var (
	persistDirty    atomic.Bool
	persistOnce     sync.Once
	persistStop     = make(chan struct{})
	persistStopOnce sync.Once
	persistWriteMu  sync.Mutex
)

// stateFilePath returns the snapshot path. The environment override exists
// for tests; production uses the plugin's own subdirectory, which the host
// plugin scanner never treats as a plugin (.so is the only accepted suffix,
// and the scanner only looks at the two fixed candidate directories).
func stateFilePath() string {
	if value := strings.TrimSpace(os.Getenv("LKS_TZ_STATE_FILE")); value != "" {
		return value
	}
	return "/CLIProxyAPI/logs/.plugins/timezone-override/state.json"
}

// markStateDirty flags the snapshot as out of date for the flush loop.
func markStateDirty() {
	persistDirty.Store(true)
}

// ensurePersistence loads the previous snapshot (once per process) and
// starts the background flush loop (once per process). Lifecycle calls may
// happen many times; only the first one performs the load/start.
func ensurePersistence() {
	persistOnce.Do(func() {
		loadPersistedState()
		loadRuntimeSettings()
		// After the snapshot is restored, fill any entry that has no value yet
		// from the newest healthy (332-byte) turn-state values in the audit
		// journal, so the baseline table is never empty after a fresh start.
		probeTrack.seedBaselinesFromAudit()
		go persistLoop(persistStop)
	})
}

// closePersistence stops the flush loop; the loop flushes once more before
// exiting. Safe to call multiple times and from any lifecycle call.
func closePersistence() {
	persistStopOnce.Do(func() {
		// Persist mode and both baseline slots before returning to the host;
		// shutdown must not rely on an asynchronously scheduled final flush.
		flushStateNow()
		close(persistStop)
	})
}

// persistLoop periodically writes the snapshot when something changed.
func persistLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(persistFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			flushStateNow()
			return
		case <-ticker.C:
			if persistDirty.Swap(false) {
				savePersistedState()
			}
		}
	}
}

// flushStateNow writes the snapshot immediately (shutdown path).
func flushStateNow() {
	persistDirty.Store(false)
	savePersistedState()
}

// collectState builds a consistent-enough snapshot: probe state first, then
// the audit records. Each part is copied under its own lock; the two copies
// may be milliseconds apart, which is fine for a dashboard snapshot.
func collectState() persistedState {
	state := persistedState{
		StateVersion: 1,
		SavedAt:      time.Now().UTC().Format(time.RFC3339Nano),
	}
	probeTrack.mu.Lock()
	enabled := probeTrack.rejectDegraded
	state.RejectDegraded = &enabled
	halted := probeTrack.halted
	state.Halted = &halted
	for model := range probeTrack.paused {
		state.Paused = append(state.Paused, model)
	}
	for _, entry := range probeTrack.values {
		state.Values = append(state.Values, entry)
	}
	for _, entry := range probeTrack.candidates {
		state.Candidates = append(state.Candidates, entry)
	}
	for _, failure := range probeTrack.failures {
		state.Failures = append(state.Failures, failure)
	}
	for _, suspicion := range probeTrack.suspects {
		state.Suspects = append(state.Suspects, suspicion)
	}
	for _, mark := range probeTrack.business {
		state.Business = append(state.Business, mark)
	}
	for _, penalty := range probeTrack.exitPenalties {
		state.ExitPenalties = append(state.ExitPenalties, penalty)
	}
	for spec, off := range probeTrack.disabledExits {
		if off {
			state.DisabledExits = append(state.DisabledExits, spec)
		}
	}
	state.ProbesTotal = probeTrack.probesTotal
	state.ProbesOK = probeTrack.probesOK
	historyCopy := make([]probeRecord, len(probeTrack.history))
	copy(historyCopy, probeTrack.history)
	state.ProbeHistory = historyCopy
	state.ProbeSuccessHistory = make([]probeRecord, len(probeTrack.successHistory))
	copy(state.ProbeSuccessHistory, probeTrack.successHistory)
	probeTrack.mu.Unlock()

	history.mu.Lock()
	recordsCopy := make([]auditRecord, len(history.records))
	copy(recordsCopy, history.records)
	state.Records = recordsCopy
	state.Total = history.total
	state.Inserted = history.inserted
	state.Replaced = history.replaced
	history.mu.Unlock()
	return state
}

// savePersistedState writes the snapshot atomically. Best effort: failures
// only surface on the dashboard status line, never break traffic.
func savePersistedState() {
	state := collectState()
	payload, err := json.Marshal(&state)
	if err != nil {
		return
	}
	persistWriteMu.Lock()
	defer persistWriteMu.Unlock()
	path := stateFilePath()
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, persistDirMode); err != nil {
		return
	}
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, persistFileMode)
	if err != nil {
		return
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		os.Remove(temporary)
		return
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(temporary)
		return
	}
	if err := file.Close(); err != nil {
		os.Remove(temporary)
		return
	}
	// Keep the previous snapshot as a fallback copy before replacing it.
	if _, err := os.Stat(path); err == nil {
		_ = os.Rename(path, path+".bak")
	}
	if err := os.Rename(temporary, path); err != nil {
		os.Remove(temporary)
		return
	}
}

// loadPersistedState restores the previous snapshot into the live state.
// It first tries the primary file, then the fallback copy (.bak). Counters
// are merged with max() so a stale or partial snapshot can never make the
// cumulative totals go backwards. Missing or malformed files keep the
// defaults (switch on).
func loadPersistedState() {
	path := stateFilePath()
	for _, candidate := range []string{path, path + ".bak"} {
		raw, err := os.ReadFile(candidate)
		if err != nil || len(raw) == 0 {
			continue
		}
		var state persistedState
		if json.Unmarshal(raw, &state) != nil {
			continue
		}
		applyPersistedState(state)
		return
	}
}

// applyPersistedState merges a decoded snapshot into the live state.
func applyPersistedState(state persistedState) {
	probeTrack.mu.Lock()
	if state.RejectDegraded != nil {
		probeTrack.rejectDegraded = *state.RejectDegraded
	}
	if state.Halted != nil {
		probeTrack.halted = *state.Halted
	}
	if probeTrack.paused == nil {
		probeTrack.paused = map[string]bool{}
	}
	for _, model := range state.Paused {
		if model = strings.TrimSpace(model); model != "" {
			probeTrack.paused[model] = true
		}
	}
	if probeTrack.values == nil {
		probeTrack.values = map[string]stateEntry{}
	}
	for _, entry := range state.Values {
		if strings.TrimSpace(entry.Model) != "" {
			probeTrack.values[entry.Model] = entry
		}
	}
	if probeTrack.candidates == nil {
		probeTrack.candidates = map[string]stateEntry{}
	}
	for _, entry := range state.Candidates {
		if strings.TrimSpace(entry.Model) != "" {
			probeTrack.candidates[entry.Model] = entry
		}
	}
	if probeTrack.failures == nil {
		probeTrack.failures = map[string]probeFailure{}
	}
	for _, failure := range state.Failures {
		if strings.TrimSpace(failure.Model) != "" {
			probeTrack.failures[failure.Model] = failure
		}
	}
	if probeTrack.suspects == nil {
		probeTrack.suspects = map[string]probeSuspicion{}
	}
	for _, suspicion := range state.Suspects {
		if strings.TrimSpace(suspicion.Model) != "" {
			probeTrack.suspects[suspicion.Model] = suspicion
		}
	}
	if probeTrack.business == nil {
		probeTrack.business = map[string]businessDegradation{}
	}
	for _, mark := range state.Business {
		if strings.TrimSpace(mark.Model) != "" {
			probeTrack.business[mark.Model] = mark
		}
	}
	if probeTrack.exitPenalties == nil {
		probeTrack.exitPenalties = map[string]exitPenalty{}
	}
	for _, penalty := range state.ExitPenalties {
		if strings.TrimSpace(penalty.Proxy) != "" {
			probeTrack.exitPenalties[penalty.Proxy] = penalty
		}
	}
	if probeTrack.disabledExits == nil {
		probeTrack.disabledExits = map[string]bool{}
	}
	for _, spec := range state.DisabledExits {
		if spec = strings.TrimSpace(spec); spec != "" {
			probeTrack.disabledExits[spec] = true
		}
	}
	probeTrack.probesTotal = state.ProbesTotal
	probeTrack.probesOK = state.ProbesOK
	if state.ProbesTotal > probeTrack.probesTotal {
		probeTrack.probesTotal = state.ProbesTotal
	}
	if state.ProbesOK > probeTrack.probesOK {
		probeTrack.probesOK = state.ProbesOK
	}
	if len(state.ProbeHistory) > 0 {
		probeHistoryCopy := make([]probeRecord, len(state.ProbeHistory))
		copy(probeHistoryCopy, state.ProbeHistory)
		if len(probeHistoryCopy) > probeHistoryLimit {
			probeHistoryCopy = probeHistoryCopy[len(probeHistoryCopy)-probeHistoryLimit:]
		}
		probeTrack.history = probeHistoryCopy
	}
	successRecords := state.ProbeSuccessHistory
	if successRecords == nil {
		// Legacy snapshots can only recover successes still in their mixed history.
		successRecords = state.ProbeHistory
	}
	probeTrack.successHistory = make([]probeRecord, 0, probeSuccessHistoryLimit)
	for _, record := range successRecords {
		if record.Success {
			probeTrack.successHistory = appendBoundedProbeRecord(probeTrack.successHistory, record, probeSuccessHistoryLimit)
		}
	}
	probeTrack.mu.Unlock()

	history.mu.Lock()
	if len(state.Records) > 0 {
		recordsCopy := make([]auditRecord, 0, len(state.Records))
		for _, record := range state.Records {
			// Repair legacy snapshots: nil slices must become empty arrays so
			// the dashboard can render them.
			if record.Original == nil {
				record.Original = []string{}
			}
			if record.Paths == nil {
				record.Paths = []string{}
			}
			recordsCopy = append(recordsCopy, record)
		}
		if len(recordsCopy) > historyLimit {
			recordsCopy = recordsCopy[len(recordsCopy)-historyLimit:]
		}
		history.records = recordsCopy
	}
	history.total = state.Total
	history.inserted = state.Inserted
	history.replaced = state.Replaced
	if state.Total > history.total {
		history.total = state.Total
	}
	if state.Inserted > history.inserted {
		history.inserted = state.Inserted
	}
	if state.Replaced > history.replaced {
		history.replaced = state.Replaced
	}
	history.mu.Unlock()
}
