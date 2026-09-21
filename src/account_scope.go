package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"
)

// Hashes are internal identifiers, never a substitute for credential ownership.
func accountScope(authID string) string {
	if strings.TrimSpace(authID) == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(authID))
	return "auth-" + hex.EncodeToString(sum[:])
}

func splitTarget(target string) (string, string) {
	scope, model, ok := strings.Cut(target, "/")
	if ok && len(scope) == 69 && strings.HasPrefix(scope, "auth-") {
		if _, err := hex.DecodeString(scope[5:]); err == nil {
			return scope, model
		}
	}
	return "", target
}

func targetModel(target string) string { _, model := splitTarget(target); return model }
func scopedTarget(scope, model string) string {
	if model == "" {
		return ""
	}
	if scope == "" {
		return model
	}
	return scope + "/" + targetModel(model)
}
func targetAccount(target string) string {
	scope, _ := splitTarget(target)
	if scope == "" {
		return ""
	}
	return scope[:17]
}

// The host AuthID may be reused when a file is replaced. Bind to the actual
// upstream credential as well. Stable JWT issuer/subject/workspace survive
// refresh; an opaque credential rotation requires a host restart to rebind.
// Tokens here must come from the Host Auth credential callback, never the
// after-auth request headers, which may still contain client credentials.
func credentialScope(authID, token, workspace string) string {
	if strings.TrimSpace(authID) == "" || strings.TrimSpace(token) == "" {
		return ""
	}
	identity := []string{"credential-v1", authID, "token", token, workspace}
	parts := strings.Split(token, ".")
	if len(parts) == 3 && workspace != "" {
		var claims struct {
			Issuer  string `json:"iss"`
			Subject string `json:"sub"`
		}
		if raw, err := base64.RawURLEncoding.DecodeString(parts[1]); err == nil && json.Unmarshal(raw, &claims) == nil && claims.Issuer != "" && claims.Subject != "" {
			identity = []string{"credential-v1", authID, "principal", claims.Issuer, claims.Subject, workspace}
		}
	}
	raw, _ := json.Marshal(identity)
	return accountScope(string(raw))
}

func selectedAccount(metadata map[string]any, _ http.Header) string {
	id, _ := metadata["selected_auth_id"].(string)
	index, _ := metadata["selected_auth_index"].(string)
	if id == "" || index == "" {
		return ""
	}
	// CPA's after-auth hook is after selection but before the executor fills
	// Authorization. Its headers can still belong to the client. Only the
	// host-selected index and Host Auth callback identify the credential.
	entries, err := hostAuthListFunc()
	if err != nil {
		return ""
	}
	matched := false
	for _, entry := range entries {
		if entry.ID == id && entry.AuthIndex == index && !entry.Disabled {
			matched = true
			break
		}
	}
	if !matched {
		return ""
	}
	raw, err := hostAuthGetFunc(index)
	if err != nil {
		return ""
	}
	cred, err := decodeProbeCredential(raw)
	if err != nil {
		return ""
	}
	scope := credentialScope(id, cred.AccessToken, cred.AccountID)
	probeTrack.mu.Lock()
	defer probeTrack.mu.Unlock()
	if !probeTrack.bindAccountLocked(id, scope) {
		return ""
	}
	return scope
}

// The host does not expose its selected in-memory credential snapshot. Pin an
// identity for the process lifetime; any identity replacement quarantines this
// AuthID until CPA restarts. Refreshes with the same JWT principal are safe.
func (e *probeEngine) bindAccountLocked(id, scope string) bool {
	if e.accountBindings == nil {
		e.accountBindings = map[string]string{}
	}
	if previous, exists := e.accountBindings[id]; exists {
		if previous == "" || previous != scope {
			e.accountBindings[id] = ""
			return false
		}
	} else {
		e.accountBindings[id] = scope
	}
	return scope != ""
}

// Response callbacks carry client headers, not necessarily the upstream
// credential. Reuse the binding captured before that request was sent. Never
// look up today's credential file to attribute a delayed response.
func responseAccount(requestID, authID string) string {
	if requestID == "" {
		return ""
	}
	history.mu.Lock()
	defer history.mu.Unlock()
	for i := len(history.records) - 1; i >= 0; i-- {
		r := history.records[i]
		// Older hosts omit selected-auth metadata on response callbacks. The
		// host request ID still identifies the exact after-auth capture; when
		// metadata is present it must agree, never override that binding.
		if r.RequestID == requestID && r.AuthBinding != "" && (authID == "" || r.AuthBinding == accountScope(authID)) {
			return r.AccountScope
		}
	}
	return ""
}

func responseMetadataAccount(requestID string, metadata map[string]any) string {
	id, _ := metadata["selected_auth_id"].(string)
	return responseAccount(requestID, id)
}

func scopedRejectMessage(scope, model, requested string) string {
	if scope == "" {
		return ""
	}
	return degradedRejectMessage(scopedTarget(scope, model), scopedTarget(scope, requested))
}

func (e *probeEngine) scopedLeaseAwaitingRenewal(target string) bool {
	scope, model := splitTarget(target)
	if scope == "" {
		return false
	}
	e.mu.Lock()
	authID := ""
	for id, binding := range e.accountBindings {
		if binding == scope {
			authID = id
			break
		}
	}
	e.mu.Unlock()
	if authID == "" {
		return false
	}
	accountRouter.mu.Lock()
	defer accountRouter.mu.Unlock()
	if !accountRouter.config.Config.Enabled || accountRouter.config.Error != "" {
		return false
	}
	entry, ok := accountRouter.health[accountHealthKey(authID, routingModelKey(model))]
	if !ok {
		return false
	}
	expireAccountHealth(&entry, time.Now().UTC())
	return entry.State == "expired" || entry.State == "probe_pending"
}

func applyScopedOverride(scope, model, requested string, headers http.Header) (http.Header, string) {
	if !degradationDetectionEnabled(model, requested) {
		return nil, ""
	}
	state := currentTurnStateOverride()
	if state == nil {
		return nil, ""
	}
	if scope == "" {
		if !state.Config.Enabled || !turnStateOverrideMatches(state.Config, model, requested) {
			return nil, ""
		}
		return nil, "account-unavailable"
	}
	// A client's ticket may belong to the account used by its previous request.
	// Its byte length alone cannot establish ownership after CPA rotates auth.
	return applyTurnStateOverride(scopedTarget(scope, model), scopedTarget(scope, requested), nil)
}

func observeScopedBusiness(scope, model, requested, state, upstream string) string {
	if scope == "" {
		return "account-unavailable"
	}
	return observeBusinessStateForRequest(scopedTarget(scope, model), scopedTarget(scope, requested), state, upstream)
}

func repairScopedHeader(scope, model, requested, upstream, state string) http.Header {
	if scope == "" {
		return nil
	}
	return repairTurnStateHeader(scopedTarget(scope, model), scopedTarget(scope, requested), upstream, state)
}

func sameTargetModel(a, b string) bool {
	sa, ma := splitTarget(a)
	sb, mb := splitTarget(b)
	return sa == sb && strings.HasPrefix(strings.ToLower(ma), strings.ToLower(mb))
}

func (e *probeEngine) targetPausedLocked(target string) bool {
	return e.paused[target] || e.paused[targetModel(target)]
}

// CPA's explicit enable/disable flag controls participation. Transient host
// errors and retry timers do not create a second account cooldown in this plugin.
func authEntryEnabled(entry hostAuthEntry) bool {
	return !entry.Disabled && !strings.EqualFold(strings.TrimSpace(entry.Status), "disabled") && providerLooksCodex(entry.Provider+" "+entry.Type)
}

func authEntryAvailable(entry hostAuthEntry, _ time.Time) bool {
	return entry.ID != "" && entry.AuthIndex != "" && authEntryEnabled(entry)
}

func (e *probeEngine) syncProbeAccounts() {
	e.mu.Lock()
	mode := e.cfg.Config.AccountMode
	e.mu.Unlock()
	if mode == "" {
		return
	} // Internal legacy state is never selected for scoped requests.
	entries, err := hostAuthListFunc()
	if err != nil {
		e.mu.Lock()
		e.accountError = "CPA authentication inventory unavailable"
		e.mu.Unlock()
		return
	}
	accounts := make(map[string]hostAuthEntry)
	for _, entry := range entries {
		if entry.ID != "" && entry.AuthIndex != "" && authEntryEnabled(entry) {
			raw, err := hostAuthGetFunc(entry.AuthIndex)
			if err != nil {
				continue
			}
			cred, err := decodeProbeCredential(raw)
			if err != nil {
				continue
			}
			entry.Scope = credentialScope(entry.ID, cred.AccessToken, cred.AccountID)
			accounts[entry.Scope] = entry
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for scope, entry := range accounts {
		if !e.bindAccountLocked(entry.ID, scope) {
			entry.Disabled = true
			accounts[scope] = entry
		}
	}
	e.accounts, e.accountError = accounts, ""
}

// Caller holds e.mu. Disabled/removed accounts retain historical slots, but
// cannot create tasks. A failed inventory read never enables stale accounts.
func (e *probeEngine) probeTargetsLocked(cfg probeConfig) []string {
	if cfg.AccountMode == "" {
		return append([]string(nil), cfg.Models...)
	}
	if e.accountError != "" {
		return nil
	}
	var accounts []hostAuthEntry
	for _, entry := range e.accounts {
		if !authEntryAvailable(entry, time.Now()) {
			continue
		}
		accounts = append(accounts, entry)
	}
	sort.Slice(accounts, func(i, j int) bool {
		if accounts[i].Priority != accounts[j].Priority {
			return accounts[i].Priority > accounts[j].Priority
		}
		return accounts[i].ID < accounts[j].ID
	})
	if cfg.AccountMode == "fixed" {
		var matches []hostAuthEntry
		file := path.Clean(strings.ReplaceAll(cfg.CredFile, "\\", "/"))
		for _, entry := range accounts {
			if entry.Path != "" && path.Clean(strings.ReplaceAll(entry.Path, "\\", "/")) == file {
				matches = append(matches, entry)
			}
		}
		if len(matches) != 1 {
			return nil
		}
		accounts = matches
	} else if cfg.AccountMode == "highest-priority" && len(accounts) > 1 {
		accounts = accounts[:1]
	}
	var targets []string
	for _, model := range cfg.Models {
		for _, entry := range accounts {
			targets = append(targets, scopedTarget(entry.Scope, model))
		}
	}
	return targets
}

func (e *probeEngine) targetAllowedLocked(target string) bool {
	for _, candidate := range e.probeTargetsLocked(e.cfg.Config) {
		if candidate == target {
			return true
		}
	}
	return false
}

func (e *probeEngine) displayTargetsLocked(cfg probeConfig) []string {
	if cfg.AccountMode == "" {
		return append([]string(nil), cfg.Models...)
	}
	seen := map[string]bool{}
	for scope := range e.accounts {
		for _, model := range cfg.Models {
			seen[scopedTarget(scope, model)] = true
		}
	}
	result := make([]string, 0, len(seen))
	for target := range seen {
		result = append(result, target)
	}
	sort.Slice(result, func(i, j int) bool {
		pi, pj := modelPriority(cfg, result[i]), modelPriority(cfg, result[j])
		if pi != pj {
			return pi < pj
		}
		return result[i] < result[j]
	})
	return result
}

// Pin every retry to the task's credential. No fallback to another account.
func (e *probeEngine) credentialForTarget(target string, cfg probeConfig) (probeCredential, error) {
	scope, _ := splitTarget(target)
	if scope == "" {
		if cfg.AccountMode == "all-accounts" || cfg.AccountMode == "highest-priority" {
			return probeCredential{}, fmt.Errorf("account identity unavailable")
		}
		return e.resolveProbeCredential(cfg)
	}
	entries, err := hostAuthListFunc()
	if err != nil {
		return probeCredential{}, fmt.Errorf("CPA authentication inventory unavailable")
	}
	e.mu.Lock()
	bound, exists := e.accounts[scope]
	quarantined := e.accountBindings[bound.ID] == ""
	e.mu.Unlock()
	if !exists || quarantined {
		return probeCredential{}, fmt.Errorf("selected account binding unavailable")
	}
	for _, entry := range entries {
		if entry.ID != bound.ID {
			continue
		}
		if !authEntryAvailable(entry, time.Now()) {
			return probeCredential{}, fmt.Errorf("selected account unavailable")
		}
		raw, err := hostAuthGetFunc(entry.AuthIndex)
		if err != nil {
			return probeCredential{}, fmt.Errorf("selected credential unavailable")
		}
		cred, err := decodeProbeCredential(raw)
		if err != nil {
			return probeCredential{}, err
		}
		if credentialScope(entry.ID, cred.AccessToken, cred.AccountID) != scope {
			return probeCredential{}, fmt.Errorf("selected credential identity changed")
		}
		cred.AuthID, cred.AuthIndex, cred.Priority, cred.Label = entry.ID, entry.AuthIndex, entry.Priority, targetAccount(target)
		return cred, nil
	}
	return probeCredential{}, fmt.Errorf("selected account removed")
}

// A model-only management action expands to eligible account targets. The
// explicit target from a row affects exactly that account/model pair.
func (e *probeEngine) resolveTargets(model, target string) ([]string, error) {
	e.syncProbeAccounts()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cfg.Config.AccountMode == "" && target == "" {
		return []string{model}, nil
	}
	var result []string
	for _, candidate := range e.probeTargetsLocked(e.cfg.Config) {
		if target != "" && candidate != target {
			continue
		}
		if targetModel(candidate) == model {
			result = append(result, candidate)
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no eligible account for model")
	}
	return result, nil
}
