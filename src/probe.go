package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// This file implements the manual probe track: on explicit dashboard request
// ("start-round") a single round walks the configured models in priority order
// and captures a fresh X-Codex-Turn-State through the rotating egress pool.
// Round attempts skip models whose current baseline is still comfortably
// valid - one healthy capture is enough until the final hand-off window.
// Accepted captures (model-consistent, configured lengths) replace the model's healthy
// baseline, which the rewrite engine serves to business requests. Nothing is
// scheduled automatically except the hand-off (prefetch) watcher at the end
// of this file, and that watcher only acts once a value approaches expiry: no
// auto-recovery rounds, no other background probing.
//
// The probe track never touches the business request path, only reads auth
// material, and is gated by probe.enabled.

const (
	probeDefaultsTTLMinutes                 = 55
	probeDefaultsWindowMinutes              = 5
	probeDefaultsScanSeconds                = 30
	probeDefaultsIntervalSeconds            = 5
	probeDefaultsAttemptsPerHop             = 3
	probeDefaultsMaxAttempts                = 30
	probeDefaultsCooldownMinutes            = 20
	probeDefaultsSuspectThreshold           = 3
	probeDefaultsExitCooldownMinutes        = 180
	probeDefaultsExitFailThreshold          = 3
	probeDefaultsExitPoolFailThreshold      = 10
	probeDefaultsExitMinActive              = 1
	probeDefaultsExitSuccessCooldownMinutes = 30
	probeDefaultsPoolAttempts               = 100
	probeDefaultsPrefetchMinutes            = 3
	prefetchRetryWindow                     = 90 * time.Second
	probeDefaultsTimeoutSeconds             = 60
	probeRequiredStateLength                = 332
	probeDefaultsPrompt                     = "hi"
	probeDefaultsUpstreamURL                = "https://chatgpt.com/backend-api/codex/responses"
	probeDefaultsCredFile                   = "/root/.cli-proxy-api/your-codex-auth.json"
	probeHistoryLimit                       = 200
	probeSuccessHistoryLimit                = 50
)

// probeConfig is the parsed probe-track configuration block.
type probeConfig struct {
	Enabled               bool
	Models                []string
	CredFile              string
	AccountMode           string
	TargetAuthID          string // Per-task renewal target; never a global fixed account.
	CandidateLimit        int
	Proxies               []string
	TTL                   time.Duration
	Window                time.Duration
	ScanInterval          time.Duration
	ProbeInterval         time.Duration
	AttemptsPerHop        int
	MaxAttemptsPerRound   int
	Cooldown              time.Duration
	ExitCooldown          time.Duration
	ExitFailThreshold     int
	ExitPoolFailThreshold int
	ExitMinActive         int
	ExitSuccessCooldown   time.Duration
	PoolAttempts          int
	Prefetch              time.Duration
	SleepHours            probeSleepHours
	SuspectThreshold      int
	Timeout               time.Duration
	Prompt                string
	UpstreamURL           string
	SecretsFile           string
	ProxyPools            map[string]bool
	ProxyLabels           map[string]string
	ProxyIDs              map[string]string
	ProxyAttempts         map[string]int
	ProxyMultipliers      map[string]float64
}

// probeConfigYAML mirrors the YAML keys accepted under turn-state-override.probe.
type probeConfigYAML struct {
	Enabled                    *bool    `yaml:"enabled"`
	Models                     []string `yaml:"models"`
	CredFile                   string   `yaml:"cred-file"`
	AccountMode                string   `yaml:"account-mode"`
	CandidateLimit             *int     `yaml:"candidate-limit"`
	Proxies                    []string `yaml:"proxies"`
	ProxiesFile                string   `yaml:"proxies-file"`
	TTLMinutes                 *int     `yaml:"ttl-minutes"`
	WindowMinutes              *int     `yaml:"probe-window-minutes"`
	ScanSeconds                *int     `yaml:"scan-interval-seconds"`
	IntervalSeconds            *int     `yaml:"probe-interval-seconds"`
	AttemptsPerHop             *int     `yaml:"attempts-per-proxy"`
	MaxAttemptsRound           *int     `yaml:"max-attempts-per-round"`
	CooldownMinutes            *int     `yaml:"cooldown-minutes"`
	ExitCooldownMinutes        *int     `yaml:"exit-cooldown-minutes"`
	ExitFailThreshold          *int     `yaml:"exit-fail-threshold"`
	ExitPoolFailThreshold      *int     `yaml:"exit-pool-fail-threshold"`
	ExitMinActive              *int     `yaml:"exit-min-active"`
	ExitSuccessCooldownMinutes *int     `yaml:"exit-success-cooldown-minutes"`
	PoolAttempts               *int     `yaml:"pool-attempts"`
	PrefetchMinutes            *int     `yaml:"prefetch-minutes"`
	SuspectThreshold           *int     `yaml:"suspect-threshold"`
	TimeoutSeconds             *int     `yaml:"timeout-seconds"`
	Prompt                     string   `yaml:"prompt"`
	UpstreamURL                string   `yaml:"upstream-url"`
}

type probeConfigState struct {
	Config probeConfig
	Error  string
}

// stateEntry is one captured upstream turn-state value.
type stateEntry struct {
	Model       string `json:"model"`
	Value       string `json:"value,omitempty"`
	ValueLength int    `json:"value_length"`
	GeneratedAt string `json:"generated_at,omitempty"`
	CapturedAt  string `json:"captured_at"`
	ExpiresAt   string `json:"expires_at,omitempty"`
	Source      string `json:"source,omitempty"`
	Proxy       string `json:"proxy,omitempty"`
	Valid       bool   `json:"valid"`
}

// probeRecord is one probe attempt for the audit trail.
type probeRecord struct {
	Account       string    `json:"account,omitempty"`
	AccountEmail  string    `json:"account_email,omitempty"`
	ReasonCode    string    `json:"reason_code,omitempty"`
	RetryAt       time.Time `json:"retry_at,omitempty"`
	Time          string    `json:"time"`
	Model         string    `json:"model"`
	Proxy         string    `json:"proxy"`
	ProxyLabel    string    `json:"proxy_label,omitempty"`
	Success       bool      `json:"success"`
	StatusCode    int       `json:"status_code,omitempty"`
	DurationMS    int64     `json:"duration_ms"`
	EgressAddr    string    `json:"egress_addr,omitempty"`
	StateLength   int       `json:"state_length,omitempty"`
	ObservedModel string    `json:"observed_model,omitempty"`
	AuthLabel     string    `json:"auth_label,omitempty"`
	AuthPriority  int       `json:"auth_priority,omitempty"`
	Error         string    `json:"error,omitempty"`
}

// probeFailure marks a model whose latest probe round exhausted all retries
// without obtaining an acceptable (292/332-byte, consistent) state. CooldownUntil
// is the end of the quiet period; new rounds are suppressed until it passes.
type probeFailure struct {
	Model         string `json:"model"`
	Attempts      int    `json:"attempts"`
	Rounds        int    `json:"rounds"`
	LastError     string `json:"last_error,omitempty"`
	LastLength    int    `json:"last_length,omitempty"`
	FailedAt      string `json:"failed_at"`
	CooldownUntil string `json:"cooldown_until,omitempty"`
}

// probeSuspicion marks a model whose current probe round has accumulated
// enough degradation-evidence failures to be treated as degraded for the
// rejection switch before the round completes. It is cleared by a successful
// probe or promoted to a full probeFailure annotation when the round is
// exhausted.
type probeSuspicion struct {
	Model     string `json:"model"`
	Failures  int    `json:"failures"`
	LastError string `json:"last_error,omitempty"`
	Since     string `json:"since"`
}

// businessDegradation marks a model that real business traffic observed as
// degraded (length anomaly or model mismatch). Unlike probe suspicions it
// takes effect immediately - one unhealthy observation is enough - and it is
// cleared by a healthy business observation or a successful probe round.
type businessDegradation struct {
	Model  string `json:"model"`
	Reason string `json:"reason,omitempty"`
	Since  string `json:"since"`
}

// exitPenalty marks one egress temporarily out of rotation. Two kinds share
// the structure: a failure penalty (consecutive attempts failed to yield a
// healthy state - length anomaly, model mismatch or transport errors) and a
// scheduled rest after a healthy capture (Success=true) that keeps fixed
// endpoints from hammering the same address. Both are released automatically
// once Until passes; a healthy capture still clears an entry immediately when
// exit-success-cooldown-minutes is set to 0 (legacy behaviour).
type exitPenalty struct {
	Proxy     string `json:"proxy"`
	Success   bool   `json:"success,omitempty"`
	Failures  int    `json:"failures"`
	LastError string `json:"last_error,omitempty"`
	FirstAt   string `json:"first_at"`
	Until     string `json:"until"`
}

type probeEngine struct {
	activeTask        *probeTask
	authCooldowns     map[string]time.Time
	accounts          map[string]hostAuthEntry
	accountError      string
	mu                sync.Mutex
	cfg               probeConfigState
	baseCfg           *probeConfigState
	settings          runtimeSettings
	settingsError     string
	configRevision    uint64
	values            map[string]stateEntry
	failures          map[string]probeFailure
	suspects          map[string]probeSuspicion
	business          map[string]businessDegradation
	exitPenalties     map[string]exitPenalty
	candidates        map[string]stateEntry
	prefetchGate      map[string]time.Time
	prefetchStop      chan struct{}
	lastAttempt       map[string]time.Time
	paused            map[string]bool
	probing           map[string]bool
	abortCh           chan struct{}
	halted            bool
	stopping          bool
	shuttingDown      bool
	queue             []probeTask
	queueActive       bool
	disabledExits     map[string]bool
	autoAuthSeen      bool
	lastAutoAuth      string
	lastAutoScope     string
	accountBindings   map[string]string
	rejectDegraded    bool
	history           []probeRecord
	successHistory    []probeRecord
	exitSuccessCounts map[string]uint64
	exitSuccessSince  string
	proxyIndex        int
	consecutive       int
	lastActivity      string
	running           bool
	runNote           string
	runStartedAt      string
	runFinishedAt     string
	seeded            int
	probesTotal       uint64
	probesOK          uint64
	lastError         string
}

var probeTrack = &probeEngine{
	values:         map[string]stateEntry{},
	failures:       map[string]probeFailure{},
	suspects:       map[string]probeSuspicion{},
	business:       map[string]businessDegradation{},
	exitPenalties:  map[string]exitPenalty{},
	candidates:     map[string]stateEntry{},
	prefetchGate:   map[string]time.Time{},
	lastAttempt:    map[string]time.Time{},
	paused:         map[string]bool{},
	probing:        map[string]bool{},
	rejectDegraded: true,
}

// ---------------------------------------------------------------------------
// configuration

func parseProbeConfig(block probeConfigYAML) probeConfig {
	cfg := probeConfig{
		Enabled:               block.Enabled != nil && *block.Enabled,
		Models:                append([]string(nil), block.Models...),
		CredFile:              strings.TrimSpace(block.CredFile),
		AccountMode:           strings.TrimSpace(block.AccountMode),
		CandidateLimit:        5,
		Proxies:               append([]string(nil), block.Proxies...),
		TTL:                   time.Duration(probeDefaultsTTLMinutes) * time.Minute,
		Window:                time.Duration(probeDefaultsWindowMinutes) * time.Minute,
		ScanInterval:          time.Duration(probeDefaultsScanSeconds) * time.Second,
		ProbeInterval:         time.Duration(probeDefaultsIntervalSeconds) * time.Second,
		AttemptsPerHop:        probeDefaultsAttemptsPerHop,
		MaxAttemptsPerRound:   0, // 0 = auto: egress count × attempts-per-proxy
		Cooldown:              time.Duration(probeDefaultsCooldownMinutes) * time.Minute,
		ExitCooldown:          time.Duration(probeDefaultsExitCooldownMinutes) * time.Minute,
		ExitFailThreshold:     probeDefaultsExitFailThreshold,
		ExitPoolFailThreshold: probeDefaultsExitPoolFailThreshold,
		ExitMinActive:         probeDefaultsExitMinActive,
		ExitSuccessCooldown:   time.Duration(probeDefaultsExitSuccessCooldownMinutes) * time.Minute,
		PoolAttempts:          probeDefaultsPoolAttempts,
		Prefetch:              time.Duration(probeDefaultsPrefetchMinutes) * time.Minute,
		SuspectThreshold:      probeDefaultsSuspectThreshold,
		Timeout:               time.Duration(probeDefaultsTimeoutSeconds) * time.Second,
		Prompt:                probeDefaultsPrompt,
		UpstreamURL:           probeDefaultsUpstreamURL,
		SecretsFile:           strings.TrimSpace(block.ProxiesFile),
		ProxyPools:            map[string]bool{},
		ProxyLabels:           map[string]string{},
	}
	if len(cfg.Models) == 0 {
		cfg.Models = []string{"gpt-6-astra", "gpt-5.6-sol", "gpt-5.6-luna", "gpt-5.6-terra"}
	}
	if cfg.CredFile == "" {
		cfg.CredFile = probeDefaultsCredFile
	}
	if cfg.AccountMode == "" {
		cfg.AccountMode = "fixed"
	}
	if block.CandidateLimit != nil && *block.CandidateLimit > 0 {
		cfg.CandidateLimit = *block.CandidateLimit
	}
	if block.TTLMinutes != nil && *block.TTLMinutes > 0 {
		cfg.TTL = time.Duration(*block.TTLMinutes) * time.Minute
	}
	if block.WindowMinutes != nil && *block.WindowMinutes > 0 {
		cfg.Window = time.Duration(*block.WindowMinutes) * time.Minute
	}
	if block.ScanSeconds != nil && *block.ScanSeconds > 0 {
		cfg.ScanInterval = time.Duration(*block.ScanSeconds) * time.Second
	}
	if block.IntervalSeconds != nil && *block.IntervalSeconds > 0 {
		cfg.ProbeInterval = time.Duration(*block.IntervalSeconds) * time.Second
	}
	if block.AttemptsPerHop != nil && *block.AttemptsPerHop > 0 {
		cfg.AttemptsPerHop = *block.AttemptsPerHop
	}
	if block.MaxAttemptsRound != nil && *block.MaxAttemptsRound > 0 {
		cfg.MaxAttemptsPerRound = *block.MaxAttemptsRound
	}
	if block.CooldownMinutes != nil && *block.CooldownMinutes > 0 {
		cfg.Cooldown = time.Duration(*block.CooldownMinutes) * time.Minute
	}
	if block.ExitCooldownMinutes != nil && *block.ExitCooldownMinutes >= 0 {
		cfg.ExitCooldown = time.Duration(*block.ExitCooldownMinutes) * time.Minute
	}
	if block.ExitFailThreshold != nil && *block.ExitFailThreshold > 0 {
		cfg.ExitFailThreshold = *block.ExitFailThreshold
	}
	if block.ExitPoolFailThreshold != nil && *block.ExitPoolFailThreshold > 0 {
		cfg.ExitPoolFailThreshold = *block.ExitPoolFailThreshold
	}
	if block.ExitMinActive != nil && *block.ExitMinActive > 0 {
		cfg.ExitMinActive = *block.ExitMinActive
	}
	if block.ExitSuccessCooldownMinutes != nil && *block.ExitSuccessCooldownMinutes >= 0 {
		cfg.ExitSuccessCooldown = time.Duration(*block.ExitSuccessCooldownMinutes) * time.Minute
	}
	if block.PoolAttempts != nil && *block.PoolAttempts >= 0 {
		cfg.PoolAttempts = *block.PoolAttempts
	}
	if block.PrefetchMinutes != nil && *block.PrefetchMinutes >= 0 {
		cfg.Prefetch = time.Duration(*block.PrefetchMinutes) * time.Minute
	}
	if block.SuspectThreshold != nil && *block.SuspectThreshold > 0 {
		cfg.SuspectThreshold = *block.SuspectThreshold
	}
	if block.TimeoutSeconds != nil && *block.TimeoutSeconds > 0 {
		cfg.Timeout = time.Duration(*block.TimeoutSeconds) * time.Second
	}
	if trimmed := strings.TrimSpace(block.Prompt); trimmed != "" {
		cfg.Prompt = trimmed
	}
	if trimmed := strings.TrimSpace(block.UpstreamURL); trimmed != "" {
		cfg.UpstreamURL = trimmed
	}
	// Proxies may come inline or from the dedicated secrets file. The secrets
	// file also overrides inline entries when present.
	if len(cfg.Proxies) == 0 {
		cfg.Proxies = []string{"direct"}
	}
	return cfg
}

// loadProxiesFile reads the dedicated proxy secret file (one proxy per line;
// full-line "#" comments and blank lines ignored) and returns the parsed list
// plus per-entry metadata. A trailing comment may declare a multi-exit pool
// and a display label: "socks5h://192.0.2.1:1080  # pool:ipv6" marks the
// entry as a rotating pool (higher cool-down tolerance).
func loadProxiesFile(path string) ([]string, map[string]bool, map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, nil, err
	}
	var proxies []string
	pools := map[string]bool{}
	labels := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		spec := line
		comment := ""
		if idx := strings.Index(line, "#"); idx >= 0 {
			spec = strings.TrimSpace(line[:idx])
			comment = strings.TrimSpace(line[idx+1:])
		}
		if spec == "" {
			continue
		}
		proxies = append(proxies, spec)
		for _, token := range strings.Fields(comment) {
			if value, ok := strings.CutPrefix(token, "pool:"); ok && strings.TrimSpace(value) != "" {
				pools[spec] = true
				name := strings.TrimSpace(value)
				switch strings.ToLower(name) {
				case "ipv6":
					labels[spec] = "IPv6 池"
				case "ipv4":
					labels[spec] = "IPv4 池"
				default:
					// A custom label like "pool:Example" is shown as-is.
					labels[spec] = name
				}
			}
		}
	}
	if len(proxies) == 0 {
		return nil, nil, nil, fmt.Errorf("proxies file %s contains no entries", path)
	}
	return proxies, pools, labels, nil
}

func configureProbeTrack(block probeConfigYAML) error {
	ensurePersistence()
	cfg := parseProbeConfig(block)
	var cfgErr string
	if cfg.Enabled {
		if strings.TrimSpace(cfg.SecretsFile) != "" {
			proxies, pools, labels, err := loadProxiesFile(cfg.SecretsFile)
			if err == nil {
				cfg.Proxies = proxies
				cfg.ProxyPools = pools
				cfg.ProxyLabels = labels
			} else {
				cfgErr = fmt.Sprintf("load proxies file: %v", err)
			}
		}
	}
	probeTrack.mu.Lock()
	probeTrack.cancelCurrentLocked()
	base := probeConfigState{Config: cfg, Error: cfgErr}
	probeTrack.baseCfg = &base
	if effective, err := applyRuntimeSettings(cfg, probeTrack.settings); err == nil {
		cfg = effective
	} else {
		cfgErr = err.Error()
	}
	probeTrack.cfg = probeConfigState{Config: cfg, Error: cfgErr}
	probeTrack.ensureExitSuccessStatsLocked()
	probeTrack.configRevision++
	// Policy migration: plain exits whose accumulated failures are below the
	// (possibly raised) threshold are released at once. Rotating pools are
	// released unconditionally - v1.5.17 never benches them any more, so any
	// cool-down left by the older policy is stale state.
	for spec, penalty := range probeTrack.exitPenalties {
		if cfg.ProxyPools[spec] {
			delete(probeTrack.exitPenalties, spec)
			markStateDirty()
			continue
		}
		if penalty.Until == "" || penalty.Success {
			continue
		}
		threshold := cfg.ExitFailThreshold
		if penalty.Failures < threshold {
			delete(probeTrack.exitPenalties, spec)
			markStateDirty()
		}
	}
	probeTrack.mu.Unlock()
	// Configuration does not enqueue a full manual round. The watcher handles
	// bounded account-expiry renewal and optional early-prefetch independently.
	ensurePersistence()
	probeTrack.syncProbeAccounts()
	probeTrack.ensurePrefetchWatcher()
	return nil
}

// currentProbeConfig returns a copy of the active probe configuration.
func currentProbeConfig() probeConfigState {
	probeTrack.mu.Lock()
	defer probeTrack.mu.Unlock()
	return probeTrack.cfg
}

// ---------------------------------------------------------------------------
// engine lifecycle

// probeTask is one queued probe: a model to probe, with Force marking a
// one-off operator refresh (bypasses the settled-baseline gate and survives
// a halted engine).
type probeTask struct {
	TargetAuthID string
	Generation   string
	Model        string
	Force        bool
	Progress     *probeProgress
}

// A yielded task keeps its round budget; resuming never starts a fresh round.
type probeProgress struct {
	Spent      map[string]int
	Attempts   int
	Cursor     int
	LastError  string
	LastLength int
}

func (e *probeEngine) start() bool {
	e.syncProbeAccounts()
	e.mu.Lock()
	defer e.mu.Unlock()
	cfg := e.cfg.Config
	if !cfg.Enabled || e.cfg.Error != "" || e.stopping || e.shuttingDown {
		if !cfg.Enabled {
			e.runNote = "探测轨未启用（probe.enabled=false）"
		}
		return false
	}
	if cfg.AccountMode != "" && len(e.probeTargetsLocked(cfg)) == 0 {
		e.runNote = "没有可用的账号绑定，请检查 CPA 认证接口和账号模式"
		return false
	}
	// An explicit start re-ignites the engine: the automatic hand-off watcher
	// resumes replenishing expiring baselines.
	e.halted = false
	markStateDirty()
	if e.queueActive {
		return true
	}
	e.runNote = ""
	e.runStartedAt = time.Now().UTC().Format(time.RFC3339Nano)
	e.runFinishedAt = ""
	skipped := 0
	for _, model := range e.probeTargetsLocked(cfg) {
		if e.targetPausedLocked(model) || !degradationDetectionEnabled(model, "") {
			continue
		}
		if e.settledBaselineLocked(model, cfg, time.Now().UTC()) {
			// One healthy capture is enough: a model whose baseline is still
			// comfortably far from expiry is skipped until its hand-off window
			// comes up (the prefetch watcher refills it then). The row's "probe
			// now" button remains available for an explicit refresh.
			skipped++
			continue
		}
		e.enqueueTaskLocked(model, false)
	}
	if skipped > 0 {
		e.runNote = fmt.Sprintf("已跳过 %d 个基线仍充裕或已备好接班值的模型", skipped)
	}
	if !e.queueActive {
		e.runFinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	return true
}

func (e *probeEngine) stop() {
	e.mu.Lock()
	e.cancelCurrentLocked()
	// Only start-round re-arms automatic prefetch after a full stop.
	e.halted = true
	e.runNote = "已停止所有探测；自动预备已停止，在途请求将在尝试边界结束"
	e.mu.Unlock()
	markStateDirty()
}

// stopCurrent cancels this batch without changing the automatic-prefetch
// switch. It never re-arms an engine previously stopped with stop-all.
func (e *probeEngine) stopCurrent() {
	e.mu.Lock()
	e.cancelCurrentLocked()
	e.runNote = "已取消本轮排队与在途探测；自动预备保持原状态"
	e.mu.Unlock()
	markStateDirty()
}

func (e *probeEngine) shutdown() {
	e.mu.Lock()
	e.shuttingDown = true
	e.cancelCurrentLocked()
	e.mu.Unlock()
	e.stopPrefetchWatcher()
}

// Caller holds e.mu. Keep the worker visible until its in-flight request
// actually returns, and reject new tasks during that drain interval.
func (e *probeEngine) cancelCurrentLocked() {
	if e.abortCh != nil {
		close(e.abortCh)
	}
	e.abortCh = make(chan struct{})
	for _, task := range e.queue {
		delete(e.prefetchGate, task.Model)
	}
	for model := range e.probing {
		delete(e.prefetchGate, model)
	}
	e.queue = nil
	e.suspects = map[string]probeSuspicion{}
	e.stopping = e.queueActive || len(e.probing) > 0
	e.running = e.stopping
	if !e.stopping {
		e.runFinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
}

// abortSignal returns the current global brake channel. All in-flight probe
// rounds watch it so the dashboard's "stop" control can terminate every
// running probe (sequential round and per-model asynchronous ones alike) at
// the next attempt boundary.
func (e *probeEngine) abortSignal() <-chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.abortCh == nil {
		e.abortCh = make(chan struct{})
	}
	return e.abortCh
}

// enqueueTask adds one probe task to the unified execution queue and starts the
// worker when it is idle. Waiting tasks are ordered by cfg.Models priority;
// tasks with the same priority remain FIFO. A model already queued or
// currently executing is never queued twice.
func (e *probeEngine) enqueueTask(model string, force bool) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.enqueueTaskLocked(model, force)
}

// Renew only the selected account, once per 90 seconds, on demand. Respect
// manual stop/pause and disabled probing; never switch the global probe account.
func (e *probeEngine) renewAccount(authID, model string, now time.Time) bool {
	if strings.TrimSpace(authID) == "" {
		return false
	}
	generation := ""
	for _, entry := range accountRouter.renewalCandidates(now) {
		if entry.AuthID == authID && entry.Model == routingModelKey(model) {
			generation = renewalGeneration(entry)
			break
		}
	}
	if generation == "" {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if (e.cfg.Config.AccountMode != "highest-priority" && e.cfg.Config.AccountMode != "all-accounts") || e.halted || !e.cfg.Config.SleepHours.until(now).IsZero() {
		return false
	}
	key := "account-renewal:" + accountHealthKey(authID, routingModelKey(model))
	if gate := e.prefetchGate[key]; !gate.IsZero() && now.Sub(gate) < prefetchRetryWindow {
		return false
	}
	if !e.enqueueTargetTaskLocked(model, false, authID, generation) {
		return false
	}
	if e.prefetchGate == nil {
		e.prefetchGate = map[string]time.Time{}
	}
	e.prefetchGate[key] = now
	return true
}

func (e *probeEngine) enqueueTaskLocked(model string, force bool) bool {
	return e.enqueueTargetTaskLocked(model, force, "", "")
}

func (e *probeEngine) enqueueTargetTaskLocked(model string, force bool, target, generation string) bool {
	model = strings.TrimSpace(model)
	if model == "" || !degradationDetectionEnabled(model, "") || !e.cfg.Config.Enabled || e.cfg.Error != "" || e.stopping || e.shuttingDown || e.targetPausedLocked(model) || (!force && e.halted) {
		return false
	}
	if e.queue == nil {
		e.queue = []probeTask{}
	}
	for _, task := range e.queue {
		if task.Model == model && task.TargetAuthID == target {
			return false
		}
	}
	if e.activeTask != nil && e.activeTask.Model == model && e.activeTask.TargetAuthID == target {
		return false
	}
	if target == "" && e.activeTask == nil && e.probing[model] {
		return false
	}
	e.insertTaskLocked(probeTask{Model: model, Force: force, TargetAuthID: target, Generation: generation}, false)
	active := e.queueActive
	if !active {
		e.queueActive = true
		e.running = true
		e.runStartedAt = time.Now().UTC().Format(time.RFC3339Nano)
		e.runFinishedAt = ""
	}
	markStateDirty()
	if !active {
		go e.queueLoop()
	}
	return true
}

// Caller holds e.mu. A resumed task preceded equal-priority waiting work.
func (e *probeEngine) insertTaskLocked(task probeTask, resumed bool) {
	priority := modelPriority(e.cfg.Config, task.Model)
	insertAt := len(e.queue)
	for i, queued := range e.queue {
		rank := modelPriority(e.cfg.Config, queued.Model)
		if rank > priority || (resumed && rank == priority) {
			insertAt = i
			break
		}
	}
	e.queue = append(e.queue, probeTask{})
	copy(e.queue[insertAt+1:], e.queue[insertAt:])
	e.queue[insertAt] = task
}

// Called at an attempt boundary with e.mu held, never during a request.
func (e *probeEngine) yieldProbeTaskLocked(task probeTask, progress probeProgress, stop, brake <-chan struct{}) bool {
	if !e.queueActive || e.stopping || e.shuttingDown || e.targetPausedLocked(task.Model) {
		return false
	}
	select {
	case <-stop:
		return false
	case <-brake:
		return false
	default:
	}
	for _, queued := range e.queue {
		if modelPriority(e.cfg.Config, queued.Model) < modelPriority(e.cfg.Config, task.Model) &&
			!e.targetPausedLocked(queued.Model) && (queued.Force || !e.halted) {
			task.Progress = &progress
			e.insertTaskLocked(task, true)
			return true
		}
	}
	return false
}

// modelPaused reports whether the model was paused from the dashboard.
func (e *probeEngine) modelPaused(model string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.targetPausedLocked(model)
}

// queueLoop is the single executor: it takes the highest-priority waiting
// task (FIFO within one priority), probes one model, waits the queue interval,
// then moves on to the next task. All probing paths (start-round, one-off
// refresh, resumed model, hand-off watcher) funnel through this queue, so
// attempts are serialized and spread evenly across the interval instead of
// running as independent per-model state machines. stop() cancels the
// whole queue; a reload drains it.
func (e *probeEngine) queueLoop() {
	for {
		e.mu.Lock()
		if len(e.queue) == 0 {
			e.queueActive = false
			e.running = false
			e.stopping = false
			e.runFinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
			e.mu.Unlock()
			markStateDirty()
			return
		}
		task := e.queue[0]
		e.queue = e.queue[1:]
		e.activeTask = &task
		cfg := e.cfg.Config
		if task.TargetAuthID != "" {
			cfg.TargetAuthID = task.TargetAuthID
			cfg.MaxAttemptsPerRound = 1
		}
		brake := e.abortCh
		if brake == nil {
			e.abortCh = make(chan struct{})
			brake = e.abortCh
		}
		paused := e.targetPausedLocked(task.Model)
		halted := e.halted
		e.mu.Unlock()
		// Re-evaluate at dequeue time: a task may have become obsolete while
		// waiting (model paused, baseline replenished, engine halted). A
		// one-off refresh (Force) survives both gates - it is an explicit
		// command - but still respects an operator pause.
		cancelled := false
		intervalWaited := false
		select {
		case <-brake:
			cancelled = true
		default:
		}
		if !cancelled && cfg.Enabled && !paused && (task.Force || !halted) &&
			(task.Force || task.TargetAuthID != "" || !e.settledBaseline(task.Model, cfg, time.Now().UTC())) {
			// Capture cancellation before dequeue: a stop between dequeue and
			// probeModel must not be lost by reading a fresh brake channel.
			intervalWaited = e.runProbeTask(task, cfg, brake)
		}
		e.mu.Lock()
		e.activeTask = nil
		e.mu.Unlock()
		// The queue interval: the pacing between two tasks (and, inside a
		// task, between retries). Interrupted by the global brake.
		if !intervalWaited {
			e.waitProbeInterval(cfg.ProbeInterval, brake, nil)
		}
	}
}

// modelPriority follows the configured model order. The first detection model
// shown by the dashboard therefore cuts ahead of lower-priority waiting work.
// Unknown models remain valid but run after configured models.
func modelPriority(cfg probeConfig, model string) int {
	model = targetModel(model)
	for i, candidate := range cfg.Models {
		if candidate == model {
			return i
		}
	}
	return len(cfg.Models)
}

// Snapshot the latest interval when a wait starts. A saved change does not
// reset an existing timer, interrupt a request, or re-arm the stopped worker.
func (e *probeEngine) waitProbeInterval(interval time.Duration, stop, brake <-chan struct{}) bool {
	e.mu.Lock()
	if e.baseCfg != nil {
		interval = e.cfg.Config.ProbeInterval
	}
	e.mu.Unlock()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-stop:
		return false
	case <-brake:
		return false
	}
}

// probeSuppressed reports whether probing for a model is held off: the
// model was paused from the dashboard, a probe is running for it, or a task
// for it is already waiting in the unified queue.
func (e *probeEngine) probeSuppressed(model string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.targetPausedLocked(model) || e.probing[model] {
		return true
	}
	for _, task := range e.queue {
		if task.Model == model {
			return true
		}
	}
	return false
}

// settledBaseline reports whether a model already holds a healthy baseline
// whose remaining validity is still comfortably beyond the hand-off window.
// While that is true there is nothing to do: one successful capture is
// enough, and automatic activity stays silent until the value actually
// approaches expiry (when the prefetch watcher takes over with a single
// successor capture). With the automatic window disabled (prefetch-minutes:
// 0) this gate stays open so fully-manual rounds behave exactly as before.
func (e *probeEngine) settledBaseline(model string, cfg probeConfig, now time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.settledBaselineLocked(model, cfg, now)
}

func (e *probeEngine) settledBaselineLocked(model string, cfg probeConfig, now time.Time) bool {
	if e.promoteCandidateLocked(model, cfg, now) {
		markStateDirty()
	}
	window := cfg.Prefetch
	if window <= 0 {
		return false
	}
	if candidate, ok := e.candidates[model]; ok && stateEntryAccepted(candidate) && !entryExpired(candidate, cfg.TTL, now) {
		return true
	}
	active, ok := e.values[model]
	if !ok || !stateEntryAccepted(active) {
		return false
	}
	issued, ok := parseTurnStateTimestamp(active.Value)
	if !ok {
		return false
	}
	return issued.Add(cfg.TTL).Sub(now) > window
}

// setModelPaused pauses or resumes a model from the dashboard. Pausing keeps
// the model out of the queue while preserving its value and failure record;
// an in-flight probe for that model also stops at its next attempt boundary
// (see probeModel). Resuming clears the stale annotation and queues one
// forced probe for the model - it runs even while the engine is halted, but
// the halt itself and every other model stay untouched.
func (e *probeEngine) setModelPaused(model string, paused bool) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !paused && (e.stopping || e.shuttingDown) {
		return false
	}
	if e.paused == nil {
		e.paused = map[string]bool{}
	}
	if paused {
		e.paused[model] = true
		pending := e.queue[:0]
		for _, task := range e.queue {
			if task.Model != model && (targetModel(model) != model || targetModel(task.Model) != model) {
				pending = append(pending, task)
			}
		}
		e.queue = pending
		delete(e.suspects, model)
		delete(e.prefetchGate, model)
	} else {
		if scope, bare := splitTarget(model); scope != "" && e.paused[bare] {
			for _, target := range e.displayTargetsLocked(e.cfg.Config) {
				if targetModel(target) == bare && target != model {
					e.paused[target] = true
				}
			}
			delete(e.paused, bare)
		}
		delete(e.paused, model)
		delete(e.failures, model)
	}
	markStateDirty()
	if !paused {
		// Resuming lifts the pause and probes the model once through the
		// unified queue. This works even while the engine is halted - it is
		// a single-model action, so it must not stay invisible - but it
		// never re-ignites the engine: the halt (silent hand-off watcher)
		// stays and no other model is disturbed. The task is forced so the
		// dequeue gate lets it run regardless of the halt.
		if !e.cfg.Config.Enabled || e.cfg.Error != "" {
			return true
		}
		return e.enqueueTaskLocked(model, true)
	}
	return true
}

// ---------------------------------------------------------------------------
// prefetch watch (smooth hand-off)

// ensurePrefetchWatcher starts account-expiry and early-prefetch scanning.
// Expiry is independent of the early-prefetch window but respects operator stops.
func (e *probeEngine) ensurePrefetchWatcher() {
	e.mu.Lock()
	if e.prefetchStop != nil {
		e.mu.Unlock()
		return
	}
	stop := make(chan struct{})
	e.prefetchStop = stop
	e.mu.Unlock()
	go e.prefetchWatchLoop(stop)
	go e.authSelectionWatchLoop(stop)
}

// stopPrefetchWatcher shuts the watcher down (plugin shutdown).
func (e *probeEngine) stopPrefetchWatcher() {
	e.mu.Lock()
	stop := e.prefetchStop
	e.prefetchStop = nil
	e.mu.Unlock()
	if stop != nil {
		close(stop)
	}
}

// prefetchWatchLoop scans every 30 seconds for models whose active token is
// about to expire (remaining <= prefetch-minutes, default 3) and, only then,
// starts one background probe to park a fresh token as the successor. A model
// that still owns a comfortably-valid baseline fails the window test and is
// left alone, a parked successor suppresses repeats, and per-model attempts
// are throttled by prefetchRetryWindow so a failing probe cannot spin.
func (e *probeEngine) prefetchWatchLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			e.expiryScan(time.Now().UTC())
			e.prefetchScan()
		}
	}
}

// Scan account leases even when the early-prefetch window is zero. The
// configured model allowlist, account eligibility and operator stops still apply.
func (e *probeEngine) expiryScan(now time.Time) {
	e.mu.Lock()
	cfg := e.cfg.Config
	suppressed := !cfg.Enabled || e.cfg.Error != "" || (cfg.AccountMode != "highest-priority" && cfg.AccountMode != "all-accounts") || e.halted || e.stopping || e.shuttingDown || !cfg.SleepHours.until(now).IsZero()
	e.mu.Unlock()
	if suppressed {
		return
	}
	entries := accountRouter.renewalCandidates(now)
	if len(entries) == 0 {
		return
	}
	auths, err := hostAuthListFunc()
	if err != nil {
		return
	}
	eligible := map[string]bool{}
	for _, auth := range e.eligibleProbeAccounts(auths, now) {
		eligible[auth.ID] = true
	}
	for _, entry := range entries {
		if !eligible[entry.AuthID] {
			continue
		}
		for _, model := range cfg.Models {
			if routingModelKey(model) != entry.Model {
				continue
			}
			e.mu.Lock()
			if (e.cfg.Config.AccountMode == "highest-priority" || e.cfg.Config.AccountMode == "all-accounts") && e.cfg.Config.SleepHours.until(time.Now()).IsZero() {
				e.enqueueTargetTaskLocked(model, false, entry.AuthID, renewalGeneration(entry))
			}
			e.mu.Unlock()
			break
		}
	}
}

func (e *probeEngine) prefetchScan() {
	e.mu.Lock()
	initial := e.cfg.Config
	skip := !initial.Enabled || initial.Prefetch <= 0 || e.halted || e.stopping || e.shuttingDown || !initial.SleepHours.until(time.Now()).IsZero()
	e.mu.Unlock()
	if skip {
		return
	}
	e.syncProbeAccounts()
	e.mu.Lock()
	cfg := e.cfg.Config
	targets := e.probeTargetsLocked(cfg)
	suppressed := !cfg.Enabled || e.cfg.Error != "" || cfg.Prefetch <= 0 || e.halted || e.stopping || e.shuttingDown || !cfg.SleepHours.until(time.Now()).IsZero()
	e.mu.Unlock()
	if suppressed {
		// The operator stopped all probing; the automatic hand-off watcher
		// waits for an explicit restart before replenishing again.
		return
	}
	now := time.Now().UTC()
	accountRouter.mu.Lock()
	accountScoped := accountRouter.config.Config.Enabled && accountRouter.config.Error == ""
	accountRouter.mu.Unlock()
	for _, model := range targets {
		if !degradationDetectionEnabled(model, "") {
			continue
		}
		e.mu.Lock()
		cfg = e.cfg.Config
		if !cfg.Enabled || cfg.Prefetch <= 0 || e.halted || e.stopping || e.shuttingDown || !cfg.SleepHours.until(time.Now()).IsZero() {
			e.mu.Unlock()
			return
		}
		if e.promoteCandidateLocked(model, cfg, now) {
			markStateDirty()
		}
		if e.targetPausedLocked(model) || e.probing[model] {
			e.mu.Unlock()
			continue
		}
		active, has := e.values[model]
		if !has || !stateEntryAccepted(active) {
			if cfg.AccountMode == "all-accounts" && now.Sub(e.prefetchGate[model]) >= prefetchRetryWindow {
				if e.prefetchGate == nil {
					e.prefetchGate = map[string]time.Time{}
				}
				if e.enqueueTaskLocked(model, false) {
					e.prefetchGate[model] = now
				}
			}
			e.mu.Unlock()
			continue
		}
		issued, okTime := parseTurnStateTimestamp(active.Value)
		if !okTime {
			e.mu.Unlock()
			continue
		}
		remaining := issued.Add(cfg.TTL).Sub(now)
		if remaining <= 0 && accountScoped && (cfg.AccountMode == "highest-priority" || cfg.AccountMode == "all-accounts") {
			// Account-expiry scanning owns expired leases; do not repeatedly
			// retry them through the model-global prefetch path as well.
			e.mu.Unlock()
			continue
		}
		if remaining > cfg.Prefetch {
			// The model still holds a baseline whose validity is comfortably
			// beyond the hand-off window: there is nothing to replenish, one
			// healthy capture is enough until it actually approaches expiry.
			e.mu.Unlock()
			continue
		}
		// A valid candidate already parked? Nothing to do.
		if candidate, ok := e.candidates[model]; ok && stateEntryAccepted(candidate) && !entryExpired(candidate, cfg.TTL, now) {
			e.mu.Unlock()
			continue
		}
		if gate := e.prefetchGate[model]; !gate.IsZero() && now.Sub(gate) < prefetchRetryWindow {
			e.mu.Unlock()
			continue
		}
		if e.prefetchGate == nil {
			e.prefetchGate = map[string]time.Time{}
		}
		e.prefetchGate[model] = now
		e.mu.Unlock()
		e.probeModelAsyncFromWatcher(model)
	}
}

// setExitEnabled enables or disables one egress. A disabled egress is never
// used by probing (whatever its cool-down state), but its counters and last
// error stay visible on the dashboard. Returns false when the spec is not a
// configured egress.
func (e *probeEngine) setExitEnabled(spec string, enabled bool) bool {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return false
	}
	configured := false
	e.mu.Lock()
	for _, candidate := range e.cfg.Config.Proxies {
		if candidate == spec {
			configured = true
			break
		}
	}
	if configured {
		if e.disabledExits == nil {
			e.disabledExits = map[string]bool{}
		}
		if enabled {
			delete(e.disabledExits, spec)
		} else {
			e.disabledExits[spec] = true
		}
	}
	e.mu.Unlock()
	if configured {
		markStateDirty()
	}
	return configured
}

// exitDisabled reports whether the egress is currently switched off from
// the dashboard.
func (e *probeEngine) exitDisabled(spec string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.disabledExits[spec]
}

// resetExit clears the cool-down or scheduled rest of one egress and puts it
// back into rotation immediately. An empty spec clears every entry. Returns
// the number of entries removed (for the dashboard response).
func (e *probeEngine) resetExit(spec string) int {
	spec = strings.TrimSpace(spec)
	e.mu.Lock()
	defer e.mu.Unlock()
	removed := 0
	if spec == "" {
		removed = len(e.exitPenalties)
		e.exitPenalties = map[string]exitPenalty{}
	} else if _, ok := e.exitPenalties[spec]; ok {
		delete(e.exitPenalties, spec)
		removed = 1
	}
	if removed > 0 {
		markStateDirty()
	}
	return removed
}

// probeModelAsync queues one probe for a single model on explicit user
// request (the row's "probe now" control). The task is enqueued as a forced
// one-off: it is honoured even while the engine is halted after a global
// stop and skips the settled-baseline gate, but it never re-ignites the
// engine (the halt and the silent hand-off watcher stay in place) and no
// other model is disturbed. The deliberate global re-ignition happens on
// "start-round" only.
func (e *probeEngine) probeModelAsync(model string) bool {
	return e.enqueueTask(model, true)
}

// probeModelAsyncFromWatcher queues the automatic hand-off probe. Unlike the
// explicit entry point it is suppressed while the engine is halted: a scan
// that raced with a dashboard stop must not resurrect probing.
func (e *probeEngine) probeModelAsyncFromWatcher(model string) {
	e.enqueueTask(model, false)
}

// pausedModels lists the models currently paused from the dashboard.
func (e *probeEngine) pausedModels() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	models := make([]string, 0, len(e.paused))
	for model, paused := range e.paused {
		if paused {
			models = append(models, model)
		}
	}
	sort.Strings(models)
	return models
}

// setRejectDegraded toggles the degraded-model rejection switch.
func (e *probeEngine) setRejectDegraded(enabled bool) {
	e.mu.Lock()
	e.rejectDegraded = enabled
	e.mu.Unlock()
	markStateDirty()
}

// rejectDegradedEnabled returns the current switch state.
func (e *probeEngine) rejectDegradedEnabled() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rejectDegraded
}

// degradationEvidence maps a probe failure message to its Chinese degradation
// reason, or returns "" when the failure is not degradation evidence (rate
// limits, timeouts and network errors do not count).
func degradationEvidence(message string) string {
	switch {
	case strings.Contains(message, "state length"):
		return "上游状态长度异常（疑似风控降级）"
	case strings.Contains(message, "model mismatch"):
		return "上游请求被路由至其它模型（模型不一致）"
	}
	return ""
}

// noteProbeFailure advances the early "suspected degraded" tracking for one
// in-round failure. Only degradation-evidence failures count; the running
// count is kept (even below the threshold) so the series continues, and once
// the configured threshold is reached the model becomes rejection-eligible
// before the round completes. A successful probe or a completed round clears
// it (success) or promotes it (exhausted round).
func (e *probeEngine) noteProbeFailure(model string, record probeRecord, cfg probeConfig) {
	if !degradationDetectionEnabled(model, "") || degradationEvidence(record.Error) == "" {
		return
	}
	markStateDirty()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.suspects == nil {
		e.suspects = map[string]probeSuspicion{}
	}
	suspicion := e.suspects[model]
	if suspicion.Failures == 0 {
		suspicion = probeSuspicion{Model: model, Since: time.Now().UTC().Format(time.RFC3339Nano)}
	}
	suspicion.Failures++
	suspicion.LastError = record.Error
	e.suspects[model] = suspicion
}

// clearSuspect removes the early suspicion for a model (probe success or a
// completed round that produced a full annotation).
func (e *probeEngine) clearSuspect(model string) {
	e.mu.Lock()
	delete(e.suspects, model)
	e.mu.Unlock()
	markStateDirty()
}

// degradedRejectReason reports the Chinese reason used by the degraded-model
// rejection when the switch is enabled. Probe evidence cannot reject traffic
// protected by a usable baseline; actual business evidence remains separate.
func (e *probeEngine) degradedRejectReason(model string) string {
	return e.degradedRejectReasonFor(model, []string{model})
}

func (e *probeEngine) degradedRejectReasonFor(model string, servingModels []string) string {
	// A failed renewal is diagnostic, not business-confirmed degradation.
	// Resolve outside the engine lock to avoid nesting routing/engine locks.
	renewalOnly := e.scopedLeaseAwaitingRenewal(model)
	requestedModel := ""
	if len(servingModels) > 1 {
		requestedModel = servingModels[1]
	}
	if !degradationDetectionEnabled(model, requestedModel) {
		return ""
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.rejectDegraded {
		return ""
	}
	candidate := strings.TrimSpace(model)
	for key, mark := range e.business {
		if !degradationDetectionEnabled(key, "") || !sameTargetModel(candidate, key) {
			continue
		}
		if mark.Reason != "" {
			return mark.Reason + "（业务流量确认）"
		}
	}
	// Use the same exact model / requested-model choices as the header
	// rewrite. Promote before checking so an expired active with a ready
	// successor cannot produce a transient 403 during hand-off.
	now := time.Now().UTC()
	for _, servingModel := range servingModels {
		servingModel = strings.TrimSpace(servingModel)
		if !degradationDetectionEnabled(servingModel, "") {
			continue
		}
		if e.promoteCandidateLocked(servingModel, e.cfg.Config, now) {
			markStateDirty()
		}
		if entry, ok := e.values[servingModel]; ok && stateEntryAccepted(entry) && !entryExpired(entry, e.cfg.Config.TTL, now) {
			return ""
		}
	}
	if renewalOnly {
		return ""
	}
	for key, failure := range e.failures {
		if !degradationDetectionEnabled(key, "") || !sameTargetModel(candidate, key) {
			continue
		}
		if reason := degradationEvidence(failure.LastError); reason != "" {
			return reason
		}
	}
	for key, suspicion := range e.suspects {
		if !degradationDetectionEnabled(key, "") || !sameTargetModel(candidate, key) {
			continue
		}
		threshold := e.cfg.Config.SuspectThreshold
		if threshold <= 0 {
			threshold = probeDefaultsSuspectThreshold
		}
		if suspicion.Failures < threshold {
			continue
		}
		if reason := degradationEvidence(suspicion.LastError); reason != "" {
			return fmt.Sprintf("%s（本轮已连续 %d 次失败，尚未达到正式判定）", reason, suspicion.Failures)
		}
	}
	return ""
}

// degradedRejectMessage evaluates the switch for one request and returns the
// Chinese 403 message when the request targets a degraded model.
func degradedRejectMessage(models ...string) string {
	for _, candidate := range models {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if reason := probeTrack.degradedRejectReasonFor(candidate, models); reason != "" {
			return fmt.Sprintf("模型 %s 当前处于风控降智状态：%s。请求已被 O/对抗插件拦截，请稍后重试或切换模型。", targetModel(candidate), reason)
		}
	}
	return ""
}

// effectiveMaxAttempts resolves the per-round attempt cap. Zero means auto:
// each plain egress contributes attempts-per-proxy tries, while rotating
// pools ("# pool:..." entries) contribute their own pool-attempts budget
// (default 100) because every draw there is an independent lottery with a
// fresh random source address - a failing streak says nothing about the
// endpoint. An explicit max-attempts-per-round still overrides everything.
func effectiveMaxAttempts(cfg probeConfig, proxies []string) int {
	if cfg.MaxAttemptsPerRound > 0 {
		return cfg.MaxAttemptsPerRound
	}
	total := 0
	for _, spec := range proxies {
		total += exitBudget(cfg, spec)
	}
	if total > 0 {
		return total
	}
	return probeDefaultsMaxAttempts
}

// poolAttemptsFor resolves the per-round budget of one rotating pool (0
// disables the pool in the rotation).
func poolAttemptsFor(cfg probeConfig) int {
	if cfg.PoolAttempts > 0 {
		return cfg.PoolAttempts
	}
	return probeDefaultsPoolAttempts
}

// pickRotation returns the next usable egress that still has budget, walking
// rotation order from cursor; ok=false when every usable egress has spent its
// share of the round budget (the round is then over). The next cursor is
// returned too, so the round-robin keeps rotating while shares last: plain
// egresses are sampled first alongside the pool, and once their small shares
// are exhausted a large rotating pool keeps drawing its remaining budget.
func pickRotation(rotation []string, budgets map[string]int, cursor int) (string, int, bool) {
	for offset := 0; offset < len(rotation); offset++ {
		index := (cursor + offset) % len(rotation)
		spec := rotation[index]
		if budgets[spec] > 0 {
			return spec, (index + 1) % len(rotation), true
		}
	}
	return "", cursor, false
}

// noteBusinessDegradation marks a model immediately upon a single unhealthy
// business observation (state length anomaly or upstream model mismatch) and
// triggers one gentle recovery round (throttled to at most once per five
// minutes per model, and suppressed while a round or cooldown is active).
func (e *probeEngine) noteBusinessDegradation(model, reason string) {
	model = strings.TrimSpace(model)
	if model == "" || !degradationDetectionEnabled(model, "") {
		return
	}
	e.mu.Lock()
	if e.business == nil {
		e.business = map[string]businessDegradation{}
	}
	e.business[model] = businessDegradation{
		Model:  model,
		Reason: reason,
		Since:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	e.mu.Unlock()
	markStateDirty()
}

// clearBusinessDegradation removes the business mark (healthy observation or
// successful probe).
func (e *probeEngine) clearBusinessDegradation(model string) {
	e.mu.Lock()
	_, present := e.business[model]
	if present {
		delete(e.business, model)
	}
	e.mu.Unlock()
	if present {
		markStateDirty()
	}
}

// triggerRecovery was removed: recovery probing is user-initiated
// only (the dashboard round control). Unhealthy observations keep marking the
// model for the rejection switch, but never start upstream traffic on their
// own.

// observeBusinessState feeds one state value observed on real business
// traffic into the engine:
//
//   - healthy (accepted length, confirmed model) -> stored in the account's
//     active/candidate slots with source "business", marks cleared;
//   - unhealthy (length anomaly, or state empty + model mismatch) -> one
//     observation is enough to mark the model degraded for the rejection
//     switch;
//   - state empty with no mismatch -> no signal, ignored.
func observeBusinessState(model, state, observedModel string) string {
	model = strings.TrimSpace(model)
	if model == "" || !degradationDetectionEnabled(model, "") {
		return "exempt"
	}
	state = strings.TrimSpace(state)
	consistent := observedModel == "" || probeModelConsistent(model, observedModel)
	if state == "" {
		if observedModel != "" && !consistent {
			probeTrack.noteBusinessDegradation(model, "上游请求被路由至其它模型（模型不一致）")
			return "model-mismatch"
		}
		return "missing"
	}
	if !isAcceptedStateLength(len(state)) {
		probeTrack.noteBusinessDegradation(model, fmt.Sprintf("业务请求观测到状态长度异常（%d 字节）", len(state)))
		return "invalid-length"
	}
	if !consistent {
		probeTrack.noteBusinessDegradation(model, "上游请求被路由至其它模型（模型不一致）")
		return "model-mismatch"
	}
	if observedModel == "" {
		return "awaiting-model"
	}
	cfg := currentProbeConfig().Config
	result := probeTrack.storeValue(model, state, "business", "", cfg)
	if result != "expired" {
		probeTrack.clearBusinessDegradation(model)
	}
	return result
}

// ---------------------------------------------------------------------------
// exit (egress) circuit breaker

// availableProxies returns the rotation list for the next attempt: configured
// egresses that are not in an active cool-down. A cool-down that has expired
// is released here with a fresh failure window. When the usable set would drop
// below exit-min-active, the soonest-expiring cool-downs are released early so
// a round can always make progress; a fully empty rotation also falls back to
// the first configured egress.
func (e *probeEngine) availableProxies(proxies []string, now time.Time) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	minActive := e.cfg.Config.ExitMinActive
	if minActive <= 0 {
		minActive = probeDefaultsExitMinActive
	}
	usable := make([]string, 0, len(proxies))
	for _, spec := range proxies {
		if e.disabledExits[spec] {
			// Switched off from the dashboard: never used, no matter what.
			continue
		}
		penalty, ok := e.exitPenalties[spec]
		if !ok {
			usable = append(usable, spec)
			continue
		}
		until, err := time.Parse(time.RFC3339Nano, penalty.Until)
		if penalty.Until == "" || err != nil || !now.Before(until) {
			if penalty.Until != "" {
				// Cool-down or rest finished: release with a fresh window.
				penalty.Failures = 0
				penalty.Success = false
				penalty.Until = ""
				e.exitPenalties[spec] = penalty
				markStateDirty()
			}
			usable = append(usable, spec)
		}
	}
	if len(usable) >= minActive {
		return usable
	}
	// Emergency release: free the soonest-expiring entries until the minimum
	// active count is met, so probing can never permanently starve.
	type hold struct {
		spec  string
		until time.Time
	}
	inUsable := func(spec string) bool {
		for _, item := range usable {
			if item == spec {
				return true
			}
		}
		return false
	}
	holds := make([]hold, 0, len(proxies))
	for _, spec := range proxies {
		if e.disabledExits[spec] || inUsable(spec) {
			continue
		}
		if penalty, ok := e.exitPenalties[spec]; ok {
			if until, err := time.Parse(time.RFC3339Nano, penalty.Until); err == nil {
				holds = append(holds, hold{spec: spec, until: until})
			}
		}
	}
	sort.Slice(holds, func(i, j int) bool { return holds[i].until.Before(holds[j].until) })
	for _, item := range holds {
		if len(usable) >= minActive {
			break
		}
		delete(e.exitPenalties, item.spec)
		usable = append(usable, item.spec)
		markStateDirty()
	}
	if len(usable) == 0 && len(proxies) > 0 {
		// Last-resort fallback: the first egress that is not switched off.
		// A disabled egress is never resurrected by rotation logic.
		for _, spec := range proxies {
			if !e.disabledExits[spec] {
				usable = append(usable, spec)
				break
			}
		}
	}
	return usable
}

// noteExitOutcome records one attempt result for an egress. Rotating pools
// (one endpoint that presents many source addresses, marked "# pool:..." in
// the proxies file) are never benched: every connection draws a fresh address,
// so a failing streak says nothing about the endpoint being broken - it only
// reflects the current upstream state, which the model-level tracks already
// describe. Their counters keep accumulating for the dashboard, a healthy
// capture clears them entirely, and any cool-down recorded by an older policy
// is cleared on the next observation.
//
// Fixed (plain) endpoints get two kinds of temporary removal, both released
// automatically once their window passes:
//
//   - a scheduled rest after a healthy capture (Success=true) for
//     exit-success-cooldown-minutes, so consecutive attempts spread across
//     the rotation instead of hammering the same address; set the duration to
//     0 for the legacy behaviour (success clears the entry immediately);
//   - a failure penalty: a streak of consecutive failures reaching
//     exit-fail-threshold removes the endpoint for exit-cooldown-minutes.
func (e *probeEngine) noteExitOutcome(spec string, healthy bool, errorText string) {
	if spec == "" {
		return
	}
	now := time.Now().UTC()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.baseCfg != nil {
		present := false
		for _, current := range e.cfg.Config.Proxies {
			present = present || current == spec
		}
		if !present {
			return // An edited endpoint's in-flight result remains history only.
		}
	}
	if e.exitPenalties == nil {
		e.exitPenalties = map[string]exitPenalty{}
	}
	if healthy {
		if e.cfg.Config.ProxyPools[spec] {
			if _, ok := e.exitPenalties[spec]; ok {
				delete(e.exitPenalties, spec)
				markStateDirty()
			}
			return
		}
		rest := e.cfg.Config.ExitSuccessCooldown
		if rest <= 0 {
			// Legacy behaviour: success puts the endpoint straight back into
			// rotation.
			if _, ok := e.exitPenalties[spec]; ok {
				delete(e.exitPenalties, spec)
				markStateDirty()
			}
			return
		}
		// Scheduled rest: the endpoint just served a healthy capture, so let
		// the rotation spread the next attempts across other exits. It returns
		// automatically when the window ends.
		e.exitPenalties[spec] = exitPenalty{
			Proxy:   spec,
			Success: true,
			FirstAt: now.Format(time.RFC3339Nano),
			Until:   now.Add(rest).Format(time.RFC3339Nano),
		}
		markStateDirty()
		return
	}
	penalty := e.exitPenalties[spec]
	if penalty.Proxy == "" || penalty.Success {
		// Fresh failure window (also converts a rest entry that an emergency
		// release pushed back into rotation early).
		penalty = exitPenalty{Proxy: spec, FirstAt: now.Format(time.RFC3339Nano)}
	}
	penalty.Failures++
	penalty.LastError = errorText
	if e.cfg.Config.ProxyPools[spec] {
		penalty.Until = ""
		e.exitPenalties[spec] = penalty
		markStateDirty()
		return
	}
	cfg := e.cfg.Config
	threshold := cfg.ExitFailThreshold
	if threshold <= 0 {
		threshold = probeDefaultsExitFailThreshold
	}
	if penalty.Failures >= threshold && cfg.ExitCooldown > 0 {
		penalty.Until = now.Add(cfg.ExitCooldown).Format(time.RFC3339Nano)
	}
	e.exitPenalties[spec] = penalty
	markStateDirty()
}

// probeModel runs the probe sequence for one model: attempts are distributed
// across the egress pool (skipping cool-down entries) until an acceptable
// state is captured (success clears any failure mark) or the resolved attempt
// cap is exhausted (failure is recorded for the dashboard).
func (e *probeEngine) probeModel(model string, cfg probeConfig, stop <-chan struct{}) {
	e.runProbeTask(probeTask{Model: model}, cfg, stop)
}

// Returns whether the serial interval has already elapsed since the last
// attempt, so yielding does not add a second delay before priority work.
func (e *probeEngine) runProbeTask(task probeTask, cfg probeConfig, stop <-chan struct{}) (intervalWaited bool) {
	model := task.Model
	if !degradationDetectionEnabled(model, "") {
		return
	}
	targetAuthID := cfg.TargetAuthID
	if targetAuthID != "" {
		// Renewals are queued by AuthID, but their results must enter the same
		// credential-bound slots as ordinary scoped probes and business traffic.
		cred, err := e.resolveProbeCredential(cfg)
		if err != nil {
			e.noteError("renewal credential unavailable; no probe sent")
			return
		}
		scope := credentialScope(cred.AuthID, cred.AccessToken, cred.AccountID)
		e.mu.Lock()
		bound := e.bindAccountLocked(cred.AuthID, scope)
		if bound {
			if e.accounts == nil {
				e.accounts = map[string]hostAuthEntry{}
			}
			e.accounts[scope] = hostAuthEntry{ID: cred.AuthID, AuthIndex: cred.AuthIndex, Scope: scope, Provider: "codex"}
		}
		e.mu.Unlock()
		if !bound {
			return
		}
		model = scopedTarget(scope, model)
	}
	e.mu.Lock()
	if e.baseCfg != nil {
		cfg = e.cfg.Config
	}
	if targetAuthID != "" {
		cfg.TargetAuthID, cfg.MaxAttemptsPerRound = targetAuthID, 1
	}
	if e.probing == nil {
		e.probing = map[string]bool{}
	}
	if e.probing[model] {
		e.mu.Unlock()
		return
	}
	proxies := append([]string(nil), cfg.Proxies...)
	if len(proxies) == 0 {
		e.mu.Unlock()
		return
	}
	if e.lastAttempt == nil {
		e.lastAttempt = map[string]time.Time{}
	}
	e.probing[model] = true
	e.lastAttempt[model] = time.Now().UTC()
	revision := e.configRevision
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.probing, model)
		if !e.queueActive && len(e.probing) == 0 {
			e.stopping = false
			e.running = false
		}
		e.mu.Unlock()
	}()
	maxAttempts := effectiveMaxAttempts(cfg, proxies)
	budgets := make(map[string]int, len(proxies))
	spent := make(map[string]int, len(proxies))
	attempts := 0
	lastError := ""
	lastLength := 0
	cursor := 0
	if task.Progress != nil {
		spent = task.Progress.Spent
		attempts = task.Progress.Attempts
		cursor = task.Progress.Cursor
		lastError = task.Progress.LastError
		lastLength = task.Progress.LastLength
	}
	for _, spec := range proxies {
		budgets[spec] = max(0, exitBudget(cfg, spec)-spent[exitID(cfg, spec)])
	}
	brake := e.abortSignal()
	for {
		if !e.waitForProbeWake(model, stop, brake) {
			return
		}
		select {
		case <-stop:
			return
		case <-brake:
			// Global stop (dashboard "stop" control): leave at once without a
			// failure annotation - it is an operator command, not a probe
			// outcome. The suspicion counters are cleared by stop().
			return
		default:
		}
		// The operator may pause the model while this round is in flight:
		// honour the command at the next attempt boundary and leave without
		// a failure annotation - pausing is a deliberate act, not a probe
		// outcome (v1.5.20).
		e.mu.Lock()
		pausedMidRound := e.targetPausedLocked(model)
		if revision != e.configRevision {
			cfg = e.cfg.Config
			if targetAuthID != "" {
				cfg.TargetAuthID, cfg.MaxAttemptsPerRound = targetAuthID, 1
			}
			revision = e.configRevision
			proxies = append([]string(nil), cfg.Proxies...)
			maxAttempts = effectiveMaxAttempts(cfg, proxies)
			budgets = make(map[string]int, len(proxies))
			for _, spec := range proxies {
				budgets[spec] = max(0, exitBudget(cfg, spec)-spent[exitID(cfg, spec)])
			}
		}
		e.mu.Unlock()
		if pausedMidRound {
			return
		}
		if attempts >= maxAttempts {
			break
		}
		rotation := e.availableProxies(proxies, time.Now().UTC())
		if len(rotation) == 0 {
			break
		}
		proxySpec, next, ok := pickRotation(rotation, budgets, cursor)
		if !ok {
			// Every usable egress has spent its share of the round budget.
			break
		}
		e.mu.Lock()
		if revision != e.configRevision {
			e.mu.Unlock()
			continue
		}
		// A save or the daily boundary may have begun sleep since the wait.
		if !e.cfg.Config.SleepHours.until(time.Now()).IsZero() {
			e.mu.Unlock()
			continue
		}
		if e.yieldProbeTaskLocked(task, probeProgress{Spent: spent, Attempts: attempts, Cursor: cursor, LastError: lastError, LastLength: lastLength}, stop, brake) {
			e.mu.Unlock()
			return
		}
		cursor = next
		// Reserve this attempt before an edit can publish a new configuration.
		budgets[proxySpec]--
		spent[exitID(cfg, proxySpec)]++
		attempts++
		e.mu.Unlock()
		intervalWaited = false
		if targetAuthID != "" {
			if (cfg.AccountMode != "highest-priority" && cfg.AccountMode != "all-accounts") || !accountRouter.claimRenewal(targetAuthID, targetModel(model), task.Generation, time.Now().UTC()) {
				return
			}
			if savePersistedState() != nil {
				e.noteError("renewal state persistence failed; no probe sent")
				return
			}
		}
		record, value := e.probeOnce(model, proxySpec, cfg)
		e.appendRecord(record)
		if record.Account != "" && strings.HasPrefix(record.Error, "read cred:") {
			return
		}
		if value != "" {
			e.noteExitOutcome(proxySpec, true, "")
			e.storeValue(model, value, "probe", proxySpec, cfg)
			e.mu.Lock()
			delete(e.failures, model)
			delete(e.suspects, model)
			delete(e.business, model)
			e.proxyIndex = 0
			e.mu.Unlock()
			markStateDirty()
			return
		}
		lastError = record.Error
		select {
		case <-stop:
			return
		case <-brake:
			return
		default:
		}
		if e.modelPaused(model) {
			return
		}
		if record.StateLength > 0 {
			lastLength = record.StateLength
		}
		e.noteExitOutcome(proxySpec, false, record.Error)
		e.noteProbeFailure(model, record, cfg)
		if !e.waitProbeInterval(cfg.ProbeInterval, stop, brake) {
			return
		}
		intervalWaited = true
	}
	// Retries exhausted: annotate the failure for the dashboard (attempt
	// counters, last error and a cool-down hint). Nothing restarts
	// automatically - the next attempt is a manual round. The
	// in-round suspicion is promoted to the full annotation.
	now := time.Now().UTC()
	e.mu.Lock()
	delete(e.suspects, model)
	e.failures[model] = probeFailure{
		Model:         model,
		Attempts:      attempts,
		Rounds:        (attempts + cfg.AttemptsPerHop - 1) / cfg.AttemptsPerHop,
		LastError:     lastError,
		LastLength:    lastLength,
		FailedAt:      now.Format(time.RFC3339Nano),
		CooldownUntil: now.Add(cfg.Cooldown).Format(time.RFC3339Nano),
	}
	e.mu.Unlock()
	markStateDirty()
	return
}

// probeOnce sends one minimal upstream request through the given egress and
// returns its record plus the captured state (empty on failure).
func (e *probeEngine) probeOnce(model, proxySpec string, cfg probeConfig) (probeRecord, string) {
	target := model
	model = targetModel(target)
	started := time.Now()
	record := probeRecord{
		Account: targetAccount(target),
		Time:    started.UTC().Format(time.RFC3339Nano),
		Model:   model,
		Proxy:   proxySpec,
		Success: false,
	}
	var binder *socksBind
	stage := "credential"
	defer func() {
		fmt.Fprint(os.Stderr, probeEgressLog(record, exitID(cfg, proxySpec), proxySpec, binder, stage))
	}()
	cred, err := e.credentialForTarget(target, cfg)
	if err != nil {
		record.DurationMS = time.Since(started).Milliseconds()
		record.Error = fmt.Sprintf("read cred: %v", err)
		e.noteError(record.Error)
		return record, ""
	}
	record.AuthLabel = cred.Label
	stage = "transport"
	record.AuthPriority = cred.Priority
	capturedState := ""
	defer func() {
		accountRouter.observeProbe(cred.AuthID, model, record, capturedState, time.Now().UTC())
	}()
	transport, binder, err := buildProbeTransport(proxySpec)
	if err != nil {
		record.DurationMS = time.Since(started).Milliseconds()
		record.Error = fmt.Sprintf("transport: %v", err)
		e.noteError(record.Error)
		return record, ""
	}
	client := &http.Client{Transport: transport, Timeout: cfg.Timeout}

	payload := map[string]any{
		"model": model,
		"input": []any{map[string]any{
			"type": "message", "role": "user",
			"content": []any{map[string]any{"type": "input_text", "text": cfg.Prompt}},
		}},
		"stream": true,
		"store":  false,
	}
	encoded, _ := json.Marshal(payload)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	stage = "request"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.UpstreamURL, strings.NewReader(string(encoded)))
	if err != nil {
		record.DurationMS = time.Since(started).Milliseconds()
		record.Error = fmt.Sprintf("request: %v", err)
		e.noteError(record.Error)
		return record, ""
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cred.AccessToken)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Connection", "Keep-Alive")
	req.Header.Set("Originator", "codex-tui")
	req.Header.Set("User-Agent", "codex-tui/0.154.0 (Mac OS 26.5.2; arm64) iTerm.app/3.6.11 (codex-tui; 0.154.0)")
	if cred.AccountID != "" {
		req.Header.Set("Chatgpt-Account-Id", cred.AccountID)
	}

	stage = "roundtrip"
	resp, err := client.Do(req)
	record.DurationMS = time.Since(started).Milliseconds()
	if binder != nil {
		// The SOCKS5 handshake reports the bound address for this connection.
		// It is not proof of the public IP seen by the upstream (e.g. NAT).
		record.EgressAddr = binder.load()
	}
	if err != nil {
		record.Error = fmt.Sprintf("do: %v", err)
		e.noteError(record.Error)
		return record, ""
	}
	defer resp.Body.Close()
	stage = "response"
	record.StatusCode = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		// Read a bounded snippet for diagnostics.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		record.ReasonCode = quotaFailure(resp.StatusCode, string(snippet))
		if record.ReasonCode != "" {
			record.RetryAt = quotaRetryAt(record.ReasonCode, resp.Header, string(snippet), time.Now().UTC())
		}
		record.Error = fmt.Sprintf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
		e.noteError(record.Error)
		return record, ""
	}
	state := strings.TrimSpace(resp.Header.Get(turnStateHeader))
	// Read the first SSE data event: it carries the upstream model claim used
	// for the model-consistency check (bounded to a small number of lines).
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	firstEvent := ""
	for i := 0; i < 24 && scanner.Scan(); i++ {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") {
			firstEvent = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			break
		}
	}
	// Drain in the background so the connection can be reused cleanly.
	go func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	}()
	if state == "" {
		record.Error = "no turn-state header in response"
		e.noteError(record.Error)
		return record, ""
	}
	// Model consistency: the upstream must confirm the requested model before
	// the captured state is accepted as valid.
	observedModel, okModel := probeUpstreamModel([]byte(firstEvent))
	if !okModel {
		record.Error = "model evidence missing in first event"
		e.noteError(record.Error)
		return record, ""
	}
	if !probeModelConsistent(model, observedModel) {
		record.Error = fmt.Sprintf("model mismatch: requested %s got %s", model, observedModel)
		record.ObservedModel = observedModel
		e.noteError(record.Error)
		return record, ""
	}
	// Length acceptance follows the configured empirical policy. It is not
	// inferred from account type and does not itself prove model capability.
	if !isAcceptedStateLength(len(state)) {
		record.Error = fmt.Sprintf("state length %d not in configured lengths %v (suspected degraded)", len(state), acceptedStateLengths())
		record.ObservedModel = observedModel
		record.StateLength = len(state)
		e.noteError(record.Error)
		return record, ""
	}
	if entryExpired(stateEntry{Value: state}, cfg.TTL, time.Now().UTC()) {
		record.Error = "captured state already expired"
		e.noteError(record.Error)
		return record, ""
	}
	record.Success = true
	record.StateLength = len(state)
	record.ObservedModel = observedModel
	capturedState = state
	e.mu.Lock()
	e.probesTotal++
	e.probesOK++
	e.ensureExitSuccessStatsLocked()
	// cfg is the attempt's snapshot: edits during a request cannot move its
	// result to a different exit. Stable IDs survive managed address changes.
	e.exitSuccessCounts[exitID(cfg, proxySpec)]++
	markStateDirty()
	e.lastError = ""
	e.lastActivity = fmt.Sprintf("%s via %s", model, publicProxyURL(proxySpec))
	e.mu.Unlock()
	return record, state
}

func (e *probeEngine) noteError(message string) {
	e.mu.Lock()
	e.probesTotal++
	e.lastError = message
	e.mu.Unlock()
	markStateDirty()
}

// storeValue saves a freshly captured state for the model with two-slot
// semantics: while the current active value is still valid it is kept serving
// and the new value parks in the candidate slot for a seamless hand-off; once
// the active value expires (or no active value exists) the new value becomes
// active immediately. This is the "smooth switch" the dashboard relies on:
// business traffic always overwrites with a currently-valid token while the
// prefetch machinery replenishes the next one invisibly.
func (e *probeEngine) storeValue(model, value, source, proxySpec string, cfg probeConfig) string {
	generated := ""
	expires := ""
	if ts, ok := parseTurnStateTimestamp(value); ok {
		generated = ts.UTC().Format(time.RFC3339)
		expires = ts.Add(cfg.TTL).UTC().Format(time.RFC3339)
	}
	entry := stateEntry{
		Model:       model,
		Value:       value,
		ValueLength: len(value),
		GeneratedAt: generated,
		CapturedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		ExpiresAt:   expires,
		Source:      source,
		Proxy:       proxySpec,
		Valid:       true,
	}
	now := time.Now().UTC()
	if entryExpired(entry, cfg.TTL, now) {
		return "expired"
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.candidates == nil {
		e.candidates = map[string]stateEntry{}
	}
	if e.promoteCandidateLocked(model, cfg, now) {
		markStateDirty()
	}
	active, ok := e.values[model]
	activeUsable := ok && stateEntryAccepted(active) && !entryExpired(active, cfg.TTL, now)
	if activeUsable {
		// Echoed business state is not a successor. Neither it nor an older
		// capture may replace a newer value already parked for takeover.
		if value == active.Value {
			return "same-active"
		}
		if !newerState(value, active.Value) {
			return "older"
		}
		if candidate, ok := e.candidates[model]; ok && stateEntryAccepted(candidate) && !entryExpired(candidate, cfg.TTL, now) && !newerState(value, candidate.Value) {
			if value == candidate.Value {
				return "same-candidate"
			}
			return "older"
		}
		// Keep serving the old token; park the fresh one as the next slot.
		e.candidates[model] = entry
		markStateDirty()
		return "updated-candidate"
	} else {
		e.values[model] = entry
		delete(e.candidates, model)
	}
	markStateDirty()
	return "updated-active"
}

func newerState(value, previous string) bool {
	if value == previous {
		return false
	}
	issued, known := parseTurnStateTimestamp(value)
	prior, priorKnown := parseTurnStateTimestamp(previous)
	return !known || !priorKnown || issued.After(prior)
}

// entryExpired reports whether a stored value's embedded timestamp plus the
// TTL has passed; values without a decodable timestamp are treated as usable
// (expiry cannot be proven).
func entryExpired(entry stateEntry, ttl time.Duration, now time.Time) bool {
	if ts, ok := parseTurnStateTimestamp(entry.Value); ok {
		return !now.Before(ts.Add(ttl))
	}
	return false
}

// promoteCandidateLocked moves a valid candidate into the active slot when
// the active value is missing or expired. Caller must hold e.mu; returns true
// when state changed (caller marks dirty).
func (e *probeEngine) promoteCandidateLocked(model string, cfg probeConfig, now time.Time) bool {
	candidate, ok := e.candidates[model]
	if !ok || !stateEntryAccepted(candidate) {
		return false
	}
	if entryExpired(candidate, cfg.TTL, now) {
		delete(e.candidates, model)
		return true
	}
	active, has := e.values[model]
	if has && stateEntryAccepted(active) && !entryExpired(active, cfg.TTL, now) {
		// Older snapshots may contain an echoed or older active in this slot.
		// It cannot extend coverage and must not suppress the prefetch window.
		if !newerState(candidate.Value, active.Value) {
			delete(e.candidates, model)
			return true
		}
		return false
	}
	e.values[model] = candidate
	delete(e.candidates, model)
	return true
}

// activeValueFor returns the model's current healthy baseline value: one that
// is stored (seed / business observation / manual probe) and still inside the
// TTL window. Serving does not depend on the probe track being enabled - the
// baseline is the plugin's cached known-good state and must keep protecting
// requests while probing stays idle. Expired actives are seamlessly replaced
// by a parked candidate when one is available.
func (e *probeEngine) activeValueFor(model string) string {
	now := time.Now().UTC()
	e.mu.Lock()
	cfg := e.cfg.Config
	dirty := e.promoteCandidateLocked(model, cfg, now)
	entry, ok := e.values[model]
	e.mu.Unlock()
	if dirty {
		markStateDirty()
	}
	if !ok || !stateEntryAccepted(entry) {
		return ""
	}
	if entryExpired(entry, cfg.TTL, now) {
		return ""
	}
	return entry.Value
}

// seedBaselinesFromAudit restores missing baseline entries from recorded
// history: first the deployment seeds file (last known-good values extracted
// from server records by the open-source-prep scanner), then the newest
// healthy (accepted length, decodable) turn-state values in the audit journal. A
// fresh plugin instance therefore keeps protecting traffic with the last
// known-good states before any business observation or manual probe refreshes
// them, and nothing is drafted into the baseline unless it passes the same
// length + decodability acceptance as probing.
func (e *probeEngine) seedBaselinesFromAudit() {
	type candidate struct {
		value string
		at    time.Time
	}
	best := map[string]candidate{}
	// Deployment seeds file first (richest history, includes backups).
	seedsPath := "/CLIProxyAPI/logs/.plugins/timezone-override/seeds.json"
	if override := strings.TrimSpace(os.Getenv("LKS_TZ_STATE_FILE")); override != "" {
		seedsPath = filepath.Join(filepath.Dir(override), "seeds.json")
	}
	if raw, err := os.ReadFile(seedsPath); err == nil {
		var seeds map[string]string
		if json.Unmarshal(raw, &seeds) == nil {
			for model, value := range seeds {
				model = strings.TrimSpace(model)
				value = strings.TrimSpace(value)
				if model == "" || !isAcceptedStateLength(len(value)) {
					continue
				}
				if ts, ok := parseTurnStateTimestamp(value); ok {
					best[model] = candidate{value: value, at: ts}
				}
			}
		}
	}
	history.mu.Lock()
	for _, record := range history.records {
		model := strings.TrimSpace(record.Model)
		if model == "" {
			model = strings.TrimSpace(record.RequestedModel)
		}
		if model == "" || !isAcceptedStateLength(record.TurnStateLength) || record.TurnStateValue == "" {
			continue
		}
		ts, ok := parseTurnStateTimestamp(record.TurnStateValue)
		if !ok {
			continue
		}
		if previous, exists := best[model]; !exists || ts.After(previous.at) {
			best[model] = candidate{value: record.TurnStateValue, at: ts}
		}
	}
	history.mu.Unlock()
	seeded := 0
	e.mu.Lock()
	if e.values == nil {
		e.values = map[string]stateEntry{}
	}
	cfg := e.cfg.Config
	for model, found := range best {
		if entry, ok := e.values[model]; ok && entry.Value != "" {
			continue
		}
		e.values[model] = stateEntry{
			Model:       model,
			Value:       found.value,
			ValueLength: len(found.value),
			GeneratedAt: found.at.UTC().Format(time.RFC3339),
			CapturedAt:  time.Now().UTC().Format(time.RFC3339Nano),
			ExpiresAt:   found.at.Add(cfg.TTL).UTC().Format(time.RFC3339),
			Source:      "seed",
			Valid:       true,
		}
		seeded++
	}
	e.seeded += seeded
	e.mu.Unlock()
	if seeded > 0 {
		markStateDirty()
	}
}

// Caller holds e.mu. Legacy history is bounded, so start a new, explicit
// counting epoch rather than presenting partial history as lifetime totals.
func (e *probeEngine) ensureExitSuccessStatsLocked() {
	if e.exitSuccessCounts == nil {
		e.exitSuccessCounts = make(map[string]uint64)
	}
	if e.exitSuccessSince == "" {
		e.exitSuccessSince = time.Now().UTC().Format(time.RFC3339Nano)
		markStateDirty()
	}
}

func (e *probeEngine) appendRecord(record probeRecord) {
	defer markStateDirty()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.history = appendBoundedProbeRecord(e.history, record, probeHistoryLimit)
	if record.Success {
		e.successHistory = appendBoundedProbeRecord(e.successHistory, record, probeSuccessHistoryLimit)
	}
}

func appendBoundedProbeRecord(records []probeRecord, record probeRecord, limit int) []probeRecord {
	if len(records) >= limit {
		copy(records, records[1:])
		records[len(records)-1] = record
		return records
	}
	return append(records, record)
}

// queueModels extracts the model names of the pending queue for the
// dashboard snapshot.
func queueModels(tasks []probeTask) []string {
	models := make([]string, 0, len(tasks))
	for _, task := range tasks {
		models = append(models, task.Model)
	}
	return models
}

// activeModels lists the models that currently have probing activity -
// executing or waiting in the queue. The dashboard uses it to keep a row in
// its "engaged" (pause-icon) state while a one-off probe runs, even when the
// engine itself is halted.
func activeModels(probing map[string]bool, tasks []probeTask) []string {
	seen := map[string]bool{}
	models := make([]string, 0, len(probing)+len(tasks))
	for model, busy := range probing {
		if busy && !seen[model] {
			seen[model] = true
			models = append(models, model)
		}
	}
	for _, task := range tasks {
		if !seen[task.Model] {
			seen[task.Model] = true
			models = append(models, task.Model)
		}
	}
	return models
}

// ---------------------------------------------------------------------------
// helpers

type probeCredential struct {
	AccessToken string `json:"access_token"`
	AccountID   string `json:"account_id"`
	AuthID      string `json:"-"`
	AuthIndex   string `json:"-"`
	Label       string `json:"-"`
	Priority    int    `json:"-"`
}

func readProbeCredential(path string) (probeCredential, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return probeCredential{}, err
	}
	return decodeProbeCredential(raw)
}

func decodeProbeCredential(raw []byte) (probeCredential, error) {
	var cred probeCredential
	if err := json.Unmarshal(raw, &cred); err != nil {
		return probeCredential{}, fmt.Errorf("invalid credential JSON")
	}
	if strings.TrimSpace(cred.AccessToken) == "" {
		return probeCredential{}, fmt.Errorf("access_token empty")
	}
	return cred, nil
}

// probeModelConsistent reports whether the upstream-reported model matches the
// requested model. Matching is case-insensitive and accepts suffix variants
// (for example "gpt-6-astra" vs "gpt-6-astra-preview").
func probeModelConsistent(requested, observed string) bool {
	requested = strings.ToLower(strings.TrimSpace(targetModel(requested)))
	observed = strings.ToLower(strings.TrimSpace(observed))
	if requested == "" || observed == "" {
		return false
	}
	return observed == requested || strings.HasPrefix(observed, requested)
}

// parseTurnStateTimestamp decodes the embedded Fernet timestamp of a
// X-Codex-Turn-State token: base64url([version][8-byte BE unix seconds][...]).
func parseTurnStateTimestamp(value string) (time.Time, bool) {
	raw, err := base64.URLEncoding.DecodeString(value)
	if err != nil || len(raw) < 9 || raw[0] != 0x80 {
		return time.Time{}, false
	}
	seconds := binary.BigEndian.Uint64(raw[1:9])
	return time.Unix(int64(seconds), 0).UTC(), true
}

// One privacy-safe diagnostic per model attempt, including early failures.
// Address categories are evidence about BND.ADDR, never verified public IPs.
func probeEgressLog(record probeRecord, id, spec string, binder *socksBind, stage string) string {
	protocol, reason := "unknown", "transport_not_ready"
	spec = strings.TrimSpace(spec)
	if spec == "" || strings.EqualFold(spec, "direct") {
		protocol, reason = "direct", "direct_no_proxy_report"
	} else if parsed, err := url.Parse(spec); err == nil {
		switch strings.ToLower(parsed.Scheme) {
		case "http", "https":
			protocol, reason = strings.ToLower(parsed.Scheme), "http_no_standard_exit_field"
		case "socks5", "socks5h":
			protocol = strings.ToLower(parsed.Scheme)
		}
	}
	diagnostic := socksDiagnostic{Stage: "not_started", Reply: -1, AddressType: -1, AddressKind: "not_received"}
	dials := 0
	if binder != nil {
		diagnostic, dials = binder.diagnostics()
		if dials == 0 {
			diagnostic = socksDiagnostic{Stage: "not_started", Reply: -1, AddressType: -1, AddressKind: "not_received"}
			reason = "dial_not_started"
		} else if diagnostic.Stage == "in_progress" {
			reason = "dial_pending_at_return"
		} else if diagnostic.Stage != "complete" {
			reason = "socks_handshake_failed"
		} else {
			switch diagnostic.AddressKind {
			case "empty":
				reason = "proxy_reported_empty"
			case "unspecified":
				reason = "proxy_reported_unspecified"
			default:
				reason = "proxy_reported_unverified"
			}
		}
	}
	return fmt.Sprintf("[INFO] - probe-egress time=%s exit_id=%q model=%q protocol=%s stage=%s reason=%s socks_stage=%s socks_error=%q socks_reply=%d atyp=%d address_kind=%s dials=%d recorded=%t http_status=%d success=%t\n",
		record.Time, id, record.Model, protocol, stage, reason, diagnostic.Stage, diagnostic.Error,
		diagnostic.Reply, diagnostic.AddressType, diagnostic.AddressKind, dials, record.EgressAddr != "", record.StatusCode, record.Success)
}

// buildProbeTransport builds an HTTP transport bound to the given egress:
// "direct", "socks5://...", or "http(s)://...". For socks5 / socks5h it also
// returns a recorder for the server-reported bound address (BND.ADDR).
// This is not an independently verified public egress IP; other proxies
// return nil because HTTP CONNECT has no standard exit-IP field.
func buildProbeTransport(spec string) (*http.Transport, *socksBind, error) {
	transport := &http.Transport{
		TLSClientConfig:   &tls.Config{},
		ForceAttemptHTTP2: true,
		IdleConnTimeout:   30 * time.Second,
	}
	spec = strings.TrimSpace(spec)
	if spec == "" || strings.EqualFold(spec, "direct") {
		return transport, nil, nil
	}
	parsed, err := url.Parse(spec)
	if err != nil {
		return nil, nil, fmt.Errorf("parse proxy url: %w", err)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "socks5", "socks5h":
		binder := &socksBind{host: parsed.Host}
		if parsed.User != nil {
			binder.user = parsed.User.Username()
			binder.pass, _ = parsed.User.Password()
		}
		transport.DialContext = binder.dialContext
		return transport, binder, nil
	case "http", "https":
		transport.Proxy = http.ProxyURL(parsed)
	default:
		return nil, nil, fmt.Errorf("unsupported proxy scheme %q", parsed.Scheme)
	}
	return transport, nil, nil
}

// probeSummary describes the probe engine state for the management API.
func probeSummary() map[string]any {
	probeTrack.syncProbeAccounts()
	probeTrack.mu.Lock()
	cfgState := probeTrack.cfg
	cfg := cfgState.Config
	targets := probeTrack.displayTargetsLocked(cfg)
	now := time.Now().UTC()
	// Each value carries the real validity window derived from the token's own
	// embedded timestamp (issue time) plus the configured validity duration:
	// issued_at / expires_at / remaining_seconds / expired are computed fresh
	// on every summary so the dashboard never relies on a stale estimate.
	values := make([]map[string]any, 0, len(cfg.Models))
	detectionModels := make([]string, 0, len(cfg.Models))
	for _, model := range targets {
		if !degradationDetectionEnabled(model, "") {
			values = append(values, map[string]any{"model": targetModel(model), "target": model, "account": targetAccount(model), "account_email": probeTrack.accounts[strings.SplitN(model, "/", 2)[0]].Email, "detection_enabled": false})
			continue
		}
		detectionModels = append(detectionModels, model)
		if probeTrack.promoteCandidateLocked(model, cfg, now) {
			markStateDirty()
		}
		entry, ok := probeTrack.values[model]
		if !ok {
			entry = stateEntry{Model: model}
		}
		item := map[string]any{
			"model":             targetModel(model),
			"target":            model,
			"account":           targetAccount(model),
			"account_available": probeTrack.targetAllowedLocked(model),
			"detection_enabled": true,
			"value_length":      entry.ValueLength,
			"source":            entry.Source,
			"proxy":             publicProxyURL(entry.Proxy),
			"captured_at":       entry.CapturedAt,
			"valid":             stateEntryAccepted(entry),
		}
		scope, _ := splitTarget(model)
		if account, exists := probeTrack.accounts[scope]; exists {
			item["account_email"] = account.Email
			item["account_identity_changed"] = probeTrack.accountBindings[account.ID] == ""
		}
		if entry.Value != "" {
			item["value_preview"] = previewValue(entry.Value, turnStatePreviewLength)
		}
		// Candidate slot (two-slot smooth hand-off): expose its validity when
		// a fresh token is parked and waiting for the active one to expire.
		// A candidate whose own validity has already lapsed is not shown:
		// it can no longer take over (and is discarded on the next promote or
		// hand-off scan), so a stale "预备就绪（00m 00s）" badge must not stick.
		if candidate, ok := probeTrack.candidates[model]; ok && stateEntryAccepted(candidate) {
			citem := map[string]any{
				"value_length": len(candidate.Value),
				"source":       candidate.Source,
				"captured_at":  candidate.CapturedAt,
			}
			show := true
			if ts, okTime := parseTurnStateTimestamp(candidate.Value); okTime {
				expires := ts.Add(cfg.TTL)
				citem["issued_at"] = ts.Format(time.RFC3339)
				citem["expires_at"] = expires.Format(time.RFC3339)
				citem["remaining_seconds"] = int64(expires.Sub(now).Seconds())
				expired := !now.Before(expires)
				citem["expired"] = expired
				if expired {
					show = false
				}
			}
			if show {
				item["candidate"] = citem
			}
		}
		if ts, okTime := parseTurnStateTimestamp(entry.Value); okTime {
			expires := ts.Add(cfg.TTL)
			item["issued_at"] = ts.Format(time.RFC3339)
			item["expires_at"] = expires.Format(time.RFC3339)
			item["remaining_seconds"] = int64(expires.Sub(now).Seconds())
			item["expired"] = !now.Before(expires)
		} else if entry.ExpiresAt != "" {
			// Legacy fallback: values recorded before the embedded-timestamp
			// pipeline still carry their stored window.
			item["issued_at"] = entry.GeneratedAt
			item["expires_at"] = entry.ExpiresAt
			if exp, err := time.Parse(time.RFC3339, entry.ExpiresAt); err == nil {
				item["remaining_seconds"] = int64(exp.Sub(now).Seconds())
				item["expired"] = !now.Before(exp)
			}
		}
		values = append(values, item)
	}
	publicHistory := func(records []probeRecord) []probeRecord {
		result := make([]probeRecord, len(records))
		for i, record := range records {
			for scope, entry := range probeTrack.accounts {
				if targetAccount(scopedTarget(scope, record.Model)) == record.Account {
					record.AccountEmail = entry.Email
					break
				}
			}
			// Resolve before redaction: different credentials may share an endpoint.
			record.ProxyLabel = cfg.ProxyLabels[record.Proxy]
			record.Proxy = publicProxyURL(record.Proxy)
			record.Error = redactProxyText(record.Error)
			result[len(records)-1-i] = record // newest first
		}
		return result
	}
	history := publicHistory(probeTrack.history)
	successHistory := publicHistory(probeTrack.successHistory)
	failures := make([]probeFailure, 0, len(cfg.Models))
	for _, model := range targets {
		if failure, ok := probeTrack.failures[model]; ok && degradationDetectionEnabled(model, "") {
			failure.LastError = redactProxyText(failure.LastError)
			failures = append(failures, failure)
		}
	}
	paused := make([]string, 0, len(cfg.Models))
	for _, model := range targets {
		if probeTrack.targetPausedLocked(model) {
			paused = append(paused, model)
		}
	}
	var suspects []probeSuspicion
	threshold := cfg.SuspectThreshold
	if threshold <= 0 {
		threshold = probeDefaultsSuspectThreshold
	}
	for _, model := range targets {
		if suspicion, ok := probeTrack.suspects[model]; ok && degradationDetectionEnabled(model, "") && suspicion.Failures >= threshold {
			suspicion.LastError = redactProxyText(suspicion.LastError)
			suspects = append(suspects, suspicion)
		}
	}
	businessMarks := make([]businessDegradation, 0, len(cfg.Models))
	for _, model := range targets {
		if mark, ok := probeTrack.business[model]; ok && degradationDetectionEnabled(model, "") {
			businessMarks = append(businessMarks, mark)
		}
	}
	// Egress pool state: one entry per configured proxy with its current
	// rotation status, cool-down remainder and last error (for the pool tab).
	poolNow := time.Now().UTC()
	pool := make([]map[string]any, 0, len(cfg.Proxies))
	activeCount := 0
	disabledCount := 0
	for _, spec := range cfg.Proxies {
		multiplier := cfg.ProxyMultipliers[spec]
		if multiplier <= 0 {
			multiplier = 1
		}
		parsed, _ := url.Parse(spec)
		item := map[string]any{"id": exitID(cfg, spec), "proxy": publicProxyURL(spec), "active": true,
			"success_count": fmt.Sprint(probeTrack.exitSuccessCounts[exitID(cfg, spec)]),
			"disabled":      probeTrack.disabledExits[spec], "attempts": cfg.ProxyAttempts[spec],
			"multiplier": multiplier, "budget": exitBudget(cfg, spec), "has_auth": parsed != nil && parsed.User != nil}
		if cfg.ProxyPools[spec] {
			item["pool"] = true
		}
		if label := cfg.ProxyLabels[spec]; label != "" {
			item["label"] = label
		}
		if probeTrack.disabledExits[spec] {
			item["active"] = false
		}
		if penalty, ok := probeTrack.exitPenalties[spec]; ok {
			item["failures"] = penalty.Failures
			if penalty.Success {
				item["rest"] = true
			}
			if penalty.LastError != "" {
				item["last_error"] = redactProxyText(penalty.LastError)
			}
			if penalty.FirstAt != "" {
				item["first_at"] = penalty.FirstAt
			}
			if penalty.Until != "" {
				if until, err := time.Parse(time.RFC3339Nano, penalty.Until); err == nil {
					item["until"] = penalty.Until
					item["remaining_seconds"] = int64(until.Sub(poolNow).Seconds())
					if poolNow.Before(until) {
						item["active"] = false
					}
				}
			}
		}
		if item["active"] == true {
			activeCount++
		}
		if item["disabled"] == true {
			disabledCount++
		}
		pool = append(pool, item)
	}
	summary := map[string]any{
		"accepted_state_lengths":        append([]int(nil), acceptedStateLengths()...),
		"enabled":                       cfg.Enabled,
		"error":                         cfgState.Error,
		"models":                        append([]string(nil), cfg.Models...),
		"account_mode":                  cfg.AccountMode,
		"account_error":                 probeTrack.accountError,
		"account_binding_ready":         probeTrack.accountError == "" && len(probeTrack.accounts) > 0,
		"detection_models":              detectionModels,
		"proxies":                       publicProxyList(cfg.Proxies),
		"proxies_state":                 pool,
		"pool_total":                    len(cfg.Proxies),
		"pool_active":                   activeCount,
		"pool_disabled":                 disabledCount,
		"exit_fail_threshold":           cfg.ExitFailThreshold,
		"exit_pool_fail_threshold":      cfg.ExitPoolFailThreshold,
		"exit_cooldown_minutes":         int(cfg.ExitCooldown / time.Minute),
		"exit_success_cooldown_minutes": int(cfg.ExitSuccessCooldown / time.Minute),
		"exit_min_active":               cfg.ExitMinActive,
		"prefetch_minutes":              int(cfg.Prefetch / time.Minute),
		"prefetch_override":             probeTrack.settings.PrefetchMinutes != nil,
		"sleep_start_hour":              cfg.SleepHours.Start,
		"sleep_end_hour":                cfg.SleepHours.End,
		"sleeping":                      cfg.Enabled && !cfg.SleepHours.until(poolNow).IsZero(),
		"sleep_until":                   sleepUntilText(cfg.SleepHours, poolNow),
		"settings_error":                probeTrack.settingsError,
		"proxy_index":                   probeTrack.proxyIndex,
		"ttl_minutes":                   int(cfg.TTL / time.Minute),
		"window_minutes":                int(cfg.Window / time.Minute),
		"scan_seconds":                  int(cfg.ScanInterval / time.Second),
		"interval_seconds":              int(cfg.ProbeInterval / time.Second),
		"interval_override":             probeTrack.settings.IntervalSeconds != nil,
		"attempts_per_proxy":            cfg.AttemptsPerHop,
		"pool_attempts":                 poolAttemptsFor(cfg),
		"max_attempts_per_round":        effectiveMaxAttempts(cfg, cfg.Proxies),
		"max_attempts_explicit":         cfg.MaxAttemptsPerRound > 0,
		"cooldown_minutes":              int(cfg.Cooldown / time.Minute),
		"business":                      businessMarks,
		"suspect_threshold":             cfg.SuspectThreshold,
		"suspects":                      suspects,
		"paused":                        paused,
		"reject_degraded":               probeTrack.rejectDegraded,
		"running":                       probeTrack.running,
		"halted":                        probeTrack.halted,
		"stopping":                      probeTrack.stopping,
		"prefetch_enabled":              cfg.Enabled && cfgState.Error == "" && cfg.Prefetch > 0 && !probeTrack.halted && len(detectionModels) > 0,
		"queue_length":                  len(probeTrack.queue),
		"queue_models":                  queueModels(probeTrack.queue),
		"active_models":                 activeModels(probeTrack.probing, probeTrack.queue),
		"run_started_at":                probeTrack.runStartedAt,
		"run_finished_at":               probeTrack.runFinishedAt,
		"run_note":                      probeTrack.runNote,
		"seeded":                        probeTrack.seeded,
		"probes_total":                  probeTrack.probesTotal,
		"probes_ok":                     probeTrack.probesOK,
		"exit_success_since":            probeTrack.exitSuccessSince,
		"last_error":                    redactProxyText(probeTrack.lastError),
		"last_activity":                 redactProxyText(probeTrack.lastActivity),
		"values":                        values,
		"failures":                      failures,
		"history":                       history,
		"success_history":               successHistory,
	}
	probeTrack.mu.Unlock()
	return summary
}

// probeTrackShutdown is called from plugin.shutdown.
func probeTrackShutdown() {
	// Shutdown cancels execution, but must not persist a user-issued full stop.
	probeTrack.shutdown()
	closePersistence()
}
