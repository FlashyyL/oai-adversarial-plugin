package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const maxExitAttempts = 1000000

// Dashboard overrides are separate from the frequently written audit snapshot.
// Confirmed changes are durable before they become visible to the worker.
type runtimeSettings struct {
	Version         int              `json:"version"`
	Timezone        *string          `json:"timezone,omitempty"`
	PrefetchMinutes *int             `json:"prefetch_minutes,omitempty"`
	IntervalSeconds *int             `json:"interval_seconds,omitempty"`
	SleepHours      *probeSleepHours `json:"sleep_hours,omitempty"`
	Exits           []managedExit    `json:"exits,omitempty"`
	// Diagnostics turns on the bounded structural journal used for the
	// upstream risk-policy investigation (header names, lengths and short
	// digests only). Missing means off; legacy files stay quiet.
	Diagnostics *bool `json:"diagnostics,omitempty"`
}

type managedExit struct {
	ID         string  `json:"id"`
	Original   string  `json:"original,omitempty"`
	URL        string  `json:"url"`
	Label      string  `json:"label"`
	Pool       bool    `json:"pool"`
	Attempts   int     `json:"attempts"`
	Multiplier float64 `json:"multiplier"`
}

type exitEdit struct {
	ID         string  `json:"id"`
	URL        string  `json:"url"`
	Label      string  `json:"label"`
	Pool       bool    `json:"pool"`
	Attempts   int     `json:"attempts"`
	Multiplier float64 `json:"multiplier"`
	Username   *string `json:"username,omitempty"`
	Password   *string `json:"password,omitempty"`
	ClearAuth  bool    `json:"clear_auth"`
}

func runtimeSettingsPath() string {
	return filepath.Join(filepath.Dir(stateFilePath()), "runtime-settings.json")
}

func defaultExitID(spec string) string {
	digest := sha256.Sum256([]byte(spec))
	return "exit-" + hex.EncodeToString(digest[:16])
}

func exitID(cfg probeConfig, spec string) string {
	if id := cfg.ProxyIDs[spec]; id != "" {
		return id
	}
	return defaultExitID(spec)
}

func exitBudget(cfg probeConfig, spec string) int {
	base := cfg.AttemptsPerHop
	if base <= 0 {
		base = probeDefaultsAttemptsPerHop
	}
	if cfg.ProxyPools[spec] {
		base = poolAttemptsFor(cfg)
	}
	if cfg.ProxyAttempts[spec] > 0 {
		base = cfg.ProxyAttempts[spec]
	}
	multiplier := cfg.ProxyMultipliers[spec]
	if multiplier <= 0 {
		multiplier = 1
	}
	return int(math.Min(maxExitAttempts, math.Ceil(float64(base)*multiplier)))
}

func normalizeExitURL(input, previous string, edit exitEdit) (string, error) {
	input = strings.TrimSpace(input)
	if input == "direct" {
		if edit.Pool || edit.Username != nil || edit.Password != nil {
			return "", errors.New("直连不能设置为聚合池或使用代理认证")
		}
		return input, nil
	}
	u, err := url.Parse(input)
	if err != nil || u.Hostname() == "" || strings.ContainsAny(input, "\r\n\t ") {
		return "", errors.New("代理地址格式无效")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme != "socks5" && u.Scheme != "socks5h" && u.Scheme != "http" && u.Scheme != "https" {
		return "", errors.New("代理协议仅支持 socks5、socks5h、http、https")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("代理地址须含有效端口，不能包含路径、查询参数或片段")
	}
	u.Path = ""
	u.Host = strings.ToLower(u.Host)
	if edit.ClearAuth {
		u.User = nil
	} else {
		if u.User == nil && previous != "" {
			if old, err := url.Parse(previous); err == nil {
				u.User = old.User
			}
		}
		if edit.Username != nil || edit.Password != nil {
			username, password := "", ""
			if u.User != nil {
				username = u.User.Username()
				password, _ = u.User.Password()
			}
			if edit.Username != nil {
				username = *edit.Username
			}
			if edit.Password != nil {
				password = *edit.Password
			}
			if username == "" {
				return "", errors.New("代理认证用户名不能为空")
			}
			u.User = url.UserPassword(username, password)
		}
	}
	return u.String(), nil
}

// Copies all mutable routing maps: in-flight requests may retain older cfgs.
func applyRuntimeSettings(base probeConfig, settings runtimeSettings) (probeConfig, error) {
	cfg := base
	if settings.Timezone != nil {
		if err := validateTargetTimezone(*settings.Timezone); err != nil {
			return cfg, err
		}
	}
	cfg.Proxies = append([]string(nil), base.Proxies...)
	cfg.ProxyPools = make(map[string]bool)
	cfg.ProxyLabels = make(map[string]string)
	cfg.ProxyIDs = make(map[string]string)
	cfg.ProxyAttempts = make(map[string]int)
	cfg.ProxyMultipliers = make(map[string]float64)
	for _, spec := range cfg.Proxies {
		cfg.ProxyPools[spec] = base.ProxyPools[spec]
		cfg.ProxyLabels[spec] = base.ProxyLabels[spec]
		cfg.ProxyIDs[spec] = defaultExitID(spec)
	}
	if settings.PrefetchMinutes != nil {
		minutes := *settings.PrefetchMinutes
		if minutes < 0 {
			return cfg, errors.New("预备时间必须为非负整数")
		}
		// A later TTL reduction must not turn the entire lifetime into prefetch.
		maximum := max(0, int(cfg.TTL/time.Minute)-1)
		cfg.Prefetch = time.Duration(min(minutes, maximum)) * time.Minute
	}
	ids := make(map[string]bool)
	if settings.SleepHours != nil {
		if err := settings.SleepHours.validate(); err != nil {
			return cfg, err
		}
		cfg.SleepHours = *settings.SleepHours
	}
	if settings.IntervalSeconds != nil {
		seconds := *settings.IntervalSeconds
		if seconds < 1 || seconds > 3600 {
			return cfg, errors.New("串行间隔须为 1–3600 的整数秒")
		}
		cfg.ProbeInterval = time.Duration(seconds) * time.Second
	}
	for _, item := range settings.Exits {
		if item.ID == "" || ids[item.ID] || item.Attempts < 0 || item.Attempts > maxExitAttempts ||
			math.IsNaN(item.Multiplier) || math.IsInf(item.Multiplier, 0) || item.Multiplier < 0.01 || item.Multiplier > 100 ||
			len(item.Label) > 128 || strings.ContainsAny(item.Label, "\r\n") {
			return cfg, errors.New("代理参数无效：倍率须为 0.01–100，基础次数须为 0–1000000")
		}
		ids[item.ID] = true
		spec, err := normalizeExitURL(item.URL, "", exitEdit{Pool: item.Pool})
		if err != nil {
			return cfg, err
		}
		index := -1
		for i, existing := range cfg.Proxies {
			if existing == item.Original {
				index = i
			} else {
				normalized, _ := normalizeExitURL(existing, "", exitEdit{Pool: cfg.ProxyPools[existing]})
				if existing == spec || normalized == spec {
					return cfg, errors.New("代理地址已存在，请编辑原有出口")
				}
			}
		}
		if index >= 0 {
			cfg.Proxies[index] = spec
		} else {
			cfg.Proxies = append(cfg.Proxies, spec)
		}
		cfg.ProxyPools[spec] = item.Pool
		cfg.ProxyLabels[spec] = item.Label
		cfg.ProxyIDs[spec] = item.ID
		cfg.ProxyAttempts[spec] = item.Attempts
		cfg.ProxyMultipliers[spec] = item.Multiplier
		baseAttempts := item.Attempts
		if baseAttempts == 0 {
			baseAttempts = max(1, cfg.AttemptsPerHop)
			if item.Pool {
				baseAttempts = poolAttemptsFor(cfg)
			}
		}
		if math.Ceil(float64(baseAttempts)*item.Multiplier) > maxExitAttempts {
			return cfg, errors.New("单出口每轮预算不能超过 1000000 次")
		}
	}
	return cfg, nil
}

func writeRuntimeSettings(settings runtimeSettings) error {
	settings.Version = 1
	payload, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	path := runtimeSettingsPath()
	if err := os.MkdirAll(filepath.Dir(path), persistDirMode); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".runtime-settings-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err = file.Chmod(persistFileMode); err == nil {
		_, err = file.Write(payload)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(file.Name(), path)
}

func loadRuntimeSettings() {
	raw, err := os.ReadFile(runtimeSettingsPath())
	if os.IsNotExist(err) {
		syncDiagnosticsFromSettings(runtimeSettings{})
		return
	}
	var settings runtimeSettings
	if err == nil {
		err = json.Unmarshal(raw, &settings)
	}
	if err != nil || settings.Version != 1 {
		probeTrack.mu.Lock()
		probeTrack.settingsError = "运行设置读取失败，请检查文件格式和权限"
		probeTrack.mu.Unlock()
		syncDiagnosticsFromSettings(runtimeSettings{})
		return
	}
	probeTrack.mu.Lock()
	probeTrack.settings = settings
	probeTrack.mu.Unlock()
	syncDiagnosticsFromSettings(settings)
}

// Caller holds mu. Disk failure leaves the old effective settings untouched.
func (e *probeEngine) saveSettingsLocked(settings runtimeSettings) error {
	if e.shuttingDown || e.settingsError != "" {
		return errors.New("运行设置不可写，请检查加载错误或等待关闭结束")
	}
	base := e.cfg
	if e.baseCfg != nil {
		base = *e.baseCfg
	}
	cfg, err := applyRuntimeSettings(base.Config, settings)
	if err != nil {
		return err
	}
	if err = writeRuntimeSettings(settings); err != nil {
		return errors.New("运行设置保存失败，原设置保持不变；请检查目录权限和磁盘空间")
	}
	if e.baseCfg == nil {
		e.baseCfg = &base
	}
	// Transfer user state by stable ID when an address or credentials change.
	for _, previous := range e.cfg.Config.Proxies {
		for _, next := range cfg.Proxies {
			if previous == next || exitID(e.cfg.Config, previous) != exitID(cfg, next) {
				continue
			}
			if e.disabledExits[previous] {
				e.disabledExits[next] = true
				delete(e.disabledExits, previous)
			}
			if penalty, ok := e.exitPenalties[previous]; ok {
				penalty.Proxy = next
				e.exitPenalties[next] = penalty
				delete(e.exitPenalties, previous)
			}
		}
	}
	for _, spec := range cfg.Proxies {
		if cfg.ProxyPools[spec] && !e.cfg.Config.ProxyPools[spec] {
			if penalty, ok := e.exitPenalties[spec]; ok {
				penalty.Until, penalty.Success = "", false
				e.exitPenalties[spec] = penalty
			}
		}
	}
	e.settings = settings
	syncDiagnosticsFromSettings(settings)
	if settings.Timezone != nil {
		configuredTimezone.Store(*settings.Timezone)
	}
	e.cfg = probeConfigState{Config: cfg, Error: base.Error}
	e.configRevision++
	markStateDirty()
	return nil
}

func validateTargetTimezone(zone string) error {
	if zone == "" || zone == "Local" || len(zone) > 128 {
		return errors.New("请输入有效的 IANA 时区名称")
	}
	if _, err := time.LoadLocation(zone); err != nil {
		return errors.New("请输入有效的 IANA 时区名称")
	}
	return nil
}

func (e *probeEngine) setTargetTimezone(zone string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	zone = strings.TrimSpace(zone)
	settings := e.settings
	settings.Timezone = &zone
	return e.saveSettingsLocked(settings)
}

// setDiagnosticsJournal persists the opt-in structural journal switch. The
// journal itself is written by diagnostics.go and contains only header
// names, lengths and short digests - never values.
func (e *probeEngine) setDiagnosticsJournal(enabled bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	settings := e.settings
	settings.Diagnostics = &enabled
	return e.saveSettingsLocked(settings)
}

func (e *probeEngine) diagnosticsJournalEnabled() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.settings.Diagnostics != nil && *e.settings.Diagnostics
}

func (e *probeEngine) setPrefetchMinutes(minutes int) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if minutes < 0 || minutes >= int(e.cfg.Config.TTL/time.Minute) {
		return errors.New("预备时间须为非负整数分钟，并小于 token 有效期；0 表示关闭自动预备")
	}
	settings := e.settings
	settings.PrefetchMinutes = &minutes
	return e.saveSettingsLocked(settings)
}

func (e *probeEngine) setProbeIntervalSeconds(seconds int) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	settings := e.settings
	settings.IntervalSeconds = &seconds
	return e.saveSettingsLocked(settings)
}

func (e *probeEngine) saveExit(edit exitEdit) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	settings := e.settings
	settings.Exits = append([]managedExit(nil), settings.Exits...)
	previous := ""
	if edit.ID != "" {
		for _, spec := range e.cfg.Config.Proxies {
			if exitID(e.cfg.Config, spec) == edit.ID {
				previous = spec
				break
			}
		}
		if previous == "" {
			return errors.New("出口已不存在，请刷新列表")
		}
	}
	input := edit.URL
	if input == "" {
		input = previous
	}
	if previous == "direct" && input != "direct" {
		return errors.New("直连地址不可修改，请新增代理出口")
	}
	spec, err := normalizeExitURL(input, previous, edit)
	if err != nil {
		return err
	}
	item := managedExit{ID: edit.ID, Original: previous, URL: spec, Label: strings.TrimSpace(edit.Label),
		Pool: edit.Pool, Attempts: edit.Attempts, Multiplier: edit.Multiplier}
	if item.ID == "" {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return errors.New("无法生成出口标识")
		}
		item.ID = "exit-" + hex.EncodeToString(id[:])
	}
	index := -1
	for i, current := range settings.Exits {
		if current.ID == item.ID {
			item.Original = current.Original
			index = i
			break
		}
	}
	if index < 0 {
		settings.Exits = append(settings.Exits, item)
	} else {
		settings.Exits[index] = item
	}
	return e.saveSettingsLocked(settings)
}

func (e *probeEngine) resolveExit(id, legacySpec string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, spec := range e.cfg.Config.Proxies {
		if (id != "" && exitID(e.cfg.Config, spec) == id) || (id == "" && spec == legacySpec) {
			return spec
		}
	}
	return ""
}

var proxyURLPattern = regexp.MustCompile(`(?i)(?:https?|socks5h?)://[^\s"'<>]+`)

func publicProxyURL(spec string) string {
	if spec == "" || spec == "direct" {
		return spec
	}
	u, err := url.Parse(spec)
	if err != nil || u.Host == "" {
		return "[代理地址已隐藏]"
	}
	u.User, u.RawQuery, u.Fragment = nil, "", ""
	return u.String()
}

func redactProxyText(text string) string {
	return proxyURLPattern.ReplaceAllStringFunc(text, publicProxyURL)
}

func publicProxyList(proxies []string) []string {
	result := make([]string, len(proxies))
	for i, spec := range proxies {
		result[i] = publicProxyURL(spec)
	}
	return result
}

func settingsErrorResponse(err error) (managementResponse, error) {
	return jsonErrorResponse(400, fmt.Sprint(err)), nil
}
