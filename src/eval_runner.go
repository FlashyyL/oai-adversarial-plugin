package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"cpa-timezone/internal/eval"
)

var evalModelName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)

func validEvalModel(model string) bool { return evalModelName.MatchString(model) }

type evalJob struct {
	Record     evalResult
	Challenges []eval.TraceChallenge
	Stop       chan struct{}
	Cancel     context.CancelFunc
	Executing  bool
	Cancelled  bool
	Finished   bool
	Phase      string
	Started    time.Time
}

// Unlike ticket probing, manual evaluations are available even when the
// probe track is disabled. Only enabled native Codex identities are exposed.
func (e *probeEngine) evalAccounts() ([]hostAuthEntry, error) {
	entries, err := hostAuthListFunc()
	if err != nil {
		return nil, errors.New("account_inventory_unavailable")
	}
	var accounts []hostAuthEntry
	seen := map[string]bool{}
	for _, entry := range entries {
		provider := strings.ToLower(strings.TrimSpace(entry.Provider))
		if provider == "" {
			provider = strings.ToLower(strings.TrimSpace(entry.Type))
		}
		if provider != "codex" || !authEntryAvailable(entry, time.Now()) || seen[entry.ID] {
			continue
		}
		seen[entry.ID] = true
		raw, err := hostAuthGetFunc(entry.AuthIndex)
		if err != nil {
			continue
		}
		cred, err := decodeProbeCredential(raw)
		if err != nil {
			continue
		}
		entry.Scope = credentialScope(entry.ID, cred.AccessToken, cred.AccountID)
		e.mu.Lock()
		bound := e.bindAccountLocked(entry.ID, entry.Scope)
		e.mu.Unlock()
		if bound {
			accounts = append(accounts, entry)
		}
		if len(accounts) == 128 {
			break
		}
	}
	sort.Slice(accounts, func(i, j int) bool { return accounts[i].Scope < accounts[j].Scope })
	return accounts, nil
}

func (e *probeEngine) evalAccount(scope string) (hostAuthEntry, error) {
	accounts, err := e.evalAccounts()
	if err != nil {
		return hostAuthEntry{}, err
	}
	for _, account := range accounts {
		if account.Scope == scope {
			return account, nil
		}
	}
	return hostAuthEntry{}, errors.New("account_unavailable_or_changed")
}

func evalJSON(status int, value any) managementResponse {
	raw, _ := json.Marshal(value)
	return managementResponse{StatusCode: status, Headers: http.Header{"Content-Type": {"application/json; charset=utf-8"}, "Cache-Control": {"no-store"}}, Body: raw}
}

func (e *probeEngine) evalRunResponse(body []byte) managementResponse {
	var req struct {
		Accounts  []string `json:"accounts"`
		Kind      string   `json:"kind"`
		Model     string   `json:"model"`
		Effort    string   `json:"effort"`
		Runs      int      `json:"runs"`
		Confirmed bool     `json:"confirmed"`
	}
	if len(body) > 8192 || json.Unmarshal(body, &req) != nil || !req.Confirmed || len(req.Accounts) < 1 || len(req.Accounts) > 16 || !validEvalModel(req.Model) || (req.Kind != "candy" && req.Kind != "modeltrace") {
		return jsonErrorResponse(400, "invalid_eval_request")
	}
	if req.Runs == 0 {
		req.Runs = 1
	}
	if req.Effort == "" {
		req.Effort = "none"
	}
	validEffort := req.Effort == "none" || req.Effort == "minimal" || req.Effort == "low" || req.Effort == "medium" || req.Effort == "high" || req.Effort == "xhigh"
	if !validEffort || req.Runs < 1 || req.Runs > 10 || (req.Kind == "modeltrace" && (req.Runs != 1 || req.Effort != "none")) {
		return jsonErrorResponse(400, "invalid_eval_parameters")
	}
	initialBrake := e.abortSignal()
	accounts, err := e.evalAccounts()
	if err != nil {
		return jsonErrorResponse(502, err.Error())
	}
	byScope := map[string]hostAuthEntry{}
	for _, account := range accounts {
		byScope[account.Scope] = account
	}
	seen := map[string]bool{}
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.evalStateLocked()
	if s.LoadError != "" || s.StorageError != "" {
		return jsonErrorResponse(503, "eval_storage_unavailable")
	}
	if e.stopping || e.shuttingDown || e.cfg.Error != "" || e.abortCh != initialBrake {
		return jsonErrorResponse(409, "engine_unavailable")
	}
	for _, scope := range req.Accounts {
		account, ok := byScope[scope]
		if !ok || seen[scope] || e.accountBindings[account.ID] != scope {
			return jsonErrorResponse(400, "account_unavailable_or_changed")
		}
		seen[scope] = true
		if s.Jobs[scope] != nil {
			return jsonErrorResponse(409, "account_eval_busy")
		}
	}
	// Generate all IDs before touching the queue; there is no partial launch.
	ids := make([]string, len(req.Accounts))
	for i := range ids {
		var bytes [16]byte
		if _, err := rand.Read(bytes[:]); err != nil {
			return jsonErrorResponse(500, "eval_id_unavailable")
		}
		ids[i] = hex.EncodeToString(bytes[:])
	}
	requests := req.Runs
	if req.Kind == "modeltrace" {
		requests = eval.TraceTarget
	}
	for i, scope := range req.Accounts {
		r := evalResult{ID: ids[i], Scope: scope, Kind: req.Kind, Model: req.Model, Effort: req.Effort, Revision: eval.CandyRevision, Route: "cpa-no-plugin-injection", Time: time.Now().UTC().Format(time.RFC3339Nano), Status: "queued", Total: requests}
		job := &evalJob{Record: r, Stop: make(chan struct{}), Phase: "queued", Started: time.Now()}
		if req.Kind == "modeltrace" {
			job.Challenges = eval.TraceChallenges()
			job.Record.Revision = eval.TraceRevision()
		}
		s.Jobs[scope] = job
		e.insertTaskLocked(probeTask{Model: scopedTarget(scope, req.Model), Force: true, Evaluation: job}, false)
	}
	if !e.queueActive {
		e.queueActive, e.running = true, true
		e.queueWG.Add(1)
		go func() { defer e.queueWG.Done(); e.queueLoop() }()
	}
	return evalJSON(202, map[string]any{"started": len(ids), "requests": len(ids) * requests, "ids": ids})
}

func (e *probeEngine) cancelEvalLocked(job *evalJob) {
	if job.Cancelled || job.Finished {
		return
	}
	job.Cancelled = true
	job.Phase = "cancelling"
	close(job.Stop)
	if job.Cancel != nil {
		job.Cancel()
	}
	if !job.Executing {
		e.finishEvalLocked(job, "cancelled", "")
	}
}

func (e *probeEngine) cancelEvaluationsLocked() {
	if e.evaluations == nil {
		return
	}
	for _, job := range e.evaluations.Jobs {
		e.cancelEvalLocked(job)
	}
}

func (e *probeEngine) finishEvalLocked(job *evalJob, status, message string) {
	if job.Finished {
		return
	}
	job.Finished, job.Executing = true, false
	job.Record.Status, job.Record.Error = status, message
	job.Record.DurationMS = time.Since(job.Started).Milliseconds()
	s := e.evalStateLocked()
	delete(s.Jobs, job.Record.Scope)
	s.Results = append(s.Results, job.Record)
	e.saveEvalLocked()
}

// One attempt per dequeue; subsequent samples are requeued with their budget
// intact, so health work retains model priority and every attempt is paced.
func (e *probeEngine) runEvalStep(task probeTask, brake <-chan struct{}) {
	job := task.Evaluation
	if !e.waitForTaskWake(task, job.Stop, brake) {
		e.mu.Lock()
		e.finishEvalLocked(job, "cancelled", "")
		e.mu.Unlock()
		return
	}
	auth, err := e.evalAccount(job.Record.Scope)
	if err != nil {
		e.mu.Lock()
		e.finishEvalLocked(job, "failed", err.Error())
		e.mu.Unlock()
		return
	}
	e.mu.Lock()
	select {
	case <-brake:
		e.cancelEvalLocked(job)
	default:
	}
	if job.Cancelled || job.Finished || e.shuttingDown {
		e.finishEvalLocked(job, "cancelled", "")
		e.mu.Unlock()
		return
	}
	if state := e.evalStateLocked(); state.LoadError != "" || state.StorageError != "" {
		e.finishEvalLocked(job, "failed", "eval_storage_unavailable")
		e.mu.Unlock()
		return
	}
	if !e.cfg.Config.SleepHours.until(time.Now()).IsZero() {
		job.Executing = false
		job.Phase = "waiting"
		e.insertTaskLocked(task, false)
		e.mu.Unlock()
		return
	}
	prompt, expected := eval.CandyPrompt, 0
	if job.Record.Kind == "modeltrace" {
		challenge := job.Challenges[job.Record.Done]
		prompt, expected = challenge.Prompt, challenge.ExpectedCount
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	job.Cancel, job.Phase = cancel, "running"
	audit := newEvalRequestAudit(job, auth)
	e.mu.Unlock()
	started := time.Now()
	out, requestErr := executeEvalModel(ctx, auth, job.Record.Model, prompt, job.Record.Effort, audit)
	cancel()
	// Do not attribute a late result to a replacement/disabled credential.
	_, identityErr := e.evalAccount(job.Record.Scope)
	auditError := ""
	if requestErr != nil {
		auditError = requestErr.Error()
	}
	if identityErr != nil {
		auditError = "account_unavailable_or_changed"
		out.Model, out.Usage = "", evalUsage{}
	}
	select {
	case <-job.Stop:
		auditError = "cancelled"
	default:
	}
	audit.finish(out, auditError)
	e.mu.Lock()
	defer e.mu.Unlock()
	job.Cancel = nil
	if identityErr != nil {
		e.finishEvalLocked(job, "failed", "account_unavailable_or_changed")
		return
	}
	if job.Cancelled {
		e.finishEvalLocked(job, "cancelled", "")
		return
	}
	sample := evalSample{RequestID: audit.ID, Timezone: audit.Target, Prompt: prompt, ExpectedCount: expected, ObservedModel: out.Model, Usage: out.Usage, DurationMS: time.Since(started).Milliseconds(), Status: "completed"}
	if requestErr != nil {
		sample.Status, sample.Error = "error", requestErr.Error()
	} else {
		sample.Text = out.Text
		if job.Record.Kind == "candy" {
			if len(sample.Text) > 4000 {
				sample.Text = ""
				sample.Status, sample.Error = "error", "answer_too_large"
			} else {
				grade := eval.GradeCandy(sample.Text)
				sample.Grade = &grade
				sample.Status = grade.Status
				if grade.Status != "uncertain" {
					job.Record.Graded++
				}
				if grade.Status == "correct" {
					job.Record.Correct++
				}
			}
		}
	}
	job.Record.Samples = append(job.Record.Samples, sample)
	job.Record.Done++
	if job.Record.Done < job.Record.Total {
		job.Executing = false
		job.Phase = "queued"
		e.insertTaskLocked(task, false)
		return
	}
	status := "completed"
	errorsCount := 0
	for _, s := range job.Record.Samples {
		if s.Status == "error" {
			status = "partial"
			errorsCount++
		}
	}
	if errorsCount == len(job.Record.Samples) {
		status = "failed"
	}
	if job.Record.Kind == "modeltrace" {
		var outputs []eval.TraceOutput
		for _, s := range job.Record.Samples {
			outputs = append(outputs, eval.TraceOutput{Text: s.Text, ExpectedCount: s.ExpectedCount})
		}
		attribution, err := eval.AnalyzeModelTrace(outputs)
		if err != nil {
			e.finishEvalLocked(job, "failed", "insufficient_trace_samples")
			return
		}
		job.Record.Attribution = &attribution
		if attribution.Used < eval.TraceTarget {
			status = "partial"
		}
	}
	e.finishEvalLocked(job, status, "")
}

func (e *probeEngine) evalStateResponse() managementResponse {
	accounts, err := e.evalAccounts()
	if err != nil {
		return jsonErrorResponse(502, err.Error())
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.evalStateLocked()
	var views []map[string]any
	var jobs []map[string]any
	var results []evalResult
	wanted := map[string]bool{}
	for _, account := range accounts {
		wanted[account.Scope] = true
		views = append(views, map[string]any{"scope": account.Scope, "account_email": account.Email})
	}
	for _, job := range s.Jobs {
		jobs = append(jobs, map[string]any{"id": job.Record.ID, "scope": job.Record.Scope, "kind": job.Record.Kind, "model": job.Record.Model, "phase": job.Phase, "done": job.Record.Done, "total": job.Record.Total})
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i]["id"].(string) < jobs[j]["id"].(string) })
	for i := len(s.Results) - 1; i >= 0; i-- {
		if wanted[s.Results[i].Scope] {
			results = append(results, evalSummaryResult(s.Results[i]))
		}
	}
	storageError := s.LoadError
	if storageError == "" {
		storageError = s.StorageError
	}
	return evalJSON(200, map[string]any{"accounts": views, "jobs": jobs, "results": results, "models": e.cfg.Config.Models, "storage_error": storageError,
		"can_run": !e.stopping && !e.shuttingDown && e.cfg.Error == "" && storageError == "", "sleep_until": sleepUntilText(e.cfg.Config.SleepHours, time.Now()),
		"route": "cpa-no-plugin-injection", "trace_revision": eval.TraceRevision(), "candy_revision": eval.CandyRevision, "candy_expected": eval.CandyExpected,
		"licenses": map[string]string{"candy_eval": eval.PluginLicense(), "modeltrace": eval.TraceLicense()}})
}

func (e *probeEngine) evalCancelResponse(body []byte) managementResponse {
	var req struct {
		ID string `json:"id"`
	}
	if len(body) > 1024 || json.Unmarshal(body, &req) != nil || req.ID == "" {
		return jsonErrorResponse(400, "invalid_eval_request")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.evalStateLocked()
	for _, job := range s.Jobs {
		if job.Record.ID == req.ID {
			e.cancelEvalLocked(job)
			pending := e.queue[:0]
			for _, task := range e.queue {
				if task.Evaluation != job {
					pending = append(pending, task)
				}
			}
			e.queue = pending
			return evalJSON(200, map[string]bool{"cancelled": true})
		}
	}
	return jsonErrorResponse(404, "eval_not_found")
}

func (e *probeEngine) evalDetailResponse(body []byte) managementResponse {
	var req struct {
		ID string `json:"id"`
	}
	if len(body) > 1024 || json.Unmarshal(body, &req) != nil {
		return jsonErrorResponse(400, "invalid_eval_request")
	}
	accounts, err := e.evalAccounts()
	if err != nil {
		return jsonErrorResponse(502, err.Error())
	}
	wanted := map[string]bool{}
	for _, account := range accounts {
		wanted[account.Scope] = true
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, result := range e.evalStateLocked().Results {
		if result.ID == req.ID && wanted[result.Scope] {
			return evalJSON(200, result)
		}
	}
	return jsonErrorResponse(404, "eval_not_found")
}
