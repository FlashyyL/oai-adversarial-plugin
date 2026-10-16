package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"cpa-timezone/internal/eval"
)

const evalStoreLimit = 8 << 20

type evalSample struct {
	RequestID     string           `json:"request_id,omitempty"`
	Timezone      string           `json:"timezone,omitempty"`
	Prompt        string           `json:"prompt"`
	ExpectedCount int              `json:"expected_count,omitempty"`
	Text          string           `json:"text,omitempty"`
	ObservedModel string           `json:"observed_model,omitempty"`
	Status        string           `json:"status"`
	Error         string           `json:"error,omitempty"`
	Grade         *eval.CandyGrade `json:"grade,omitempty"`
	Usage         evalUsage        `json:"usage"`
	DurationMS    int64            `json:"duration_ms"`
}

type evalResult struct {
	ID          string                 `json:"id"`
	Scope       string                 `json:"scope"`
	Kind        string                 `json:"kind"`
	Model       string                 `json:"model"`
	Effort      string                 `json:"effort"`
	Revision    string                 `json:"revision"`
	Route       string                 `json:"route"`
	Time        string                 `json:"time"`
	Status      string                 `json:"status"`
	Error       string                 `json:"error,omitempty"`
	Done        int                    `json:"done"`
	Total       int                    `json:"total"`
	Correct     int                    `json:"correct"`
	Graded      int                    `json:"graded"`
	DurationMS  int64                  `json:"duration_ms"`
	Samples     []evalSample           `json:"samples,omitempty"`
	Attribution *eval.TraceAttribution `json:"attribution,omitempty"`
}

type evalState struct {
	Loaded       bool
	LoadError    string
	StorageError string
	Results      []evalResult
	Jobs         map[string]*evalJob
}

func (e *probeEngine) evalStateLocked() *evalState {
	if e.evaluations == nil {
		e.evaluations = &evalState{Jobs: map[string]*evalJob{}}
	}
	s := e.evaluations
	if s.Loaded {
		return s
	}
	s.Loaded = true
	file, err := os.Open(filepath.Join(filepath.Dir(stateFilePath()), "eval-results.json"))
	if os.IsNotExist(err) {
		return s
	}
	if err != nil {
		s.LoadError = "eval_storage_unreadable"
		return s
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, evalStoreLimit+1))
	var snapshot struct {
		Version int          `json:"version"`
		Results []evalResult `json:"results"`
	}
	if err != nil || len(raw) > evalStoreLimit || json.Unmarshal(raw, &snapshot) != nil || snapshot.Version != 1 || len(snapshot.Results) > 256 {
		s.LoadError = "eval_storage_invalid"
		return s
	}
	for _, r := range snapshot.Results {
		if r.ID == "" || len(r.ID) > 64 || (r.Kind != "candy" && r.Kind != "modeltrace") || !validEvalModel(r.Model) || len(r.Samples) > 10 {
			s.LoadError = "eval_storage_invalid"
			return s
		}
	}
	s.Results = snapshot.Results
	return s
}

// Called with e.mu held. A separate file cannot overwrite health snapshots.
func (e *probeEngine) saveEvalLocked() {
	s := e.evalStateLocked()
	if s.LoadError != "" {
		return
	}
	counts := map[string]int{}
	var keep []evalResult
	for i := len(s.Results) - 1; i >= 0; i-- {
		r := s.Results[i]
		limit := 20
		if r.Kind == "modeltrace" {
			limit = 5
		}
		key := r.Scope + "/" + r.Kind
		if counts[key] < limit {
			keep = append(keep, r)
			counts[key]++
		}
	}
	s.Results = nil
	for i := len(keep) - 1; i >= 0; i-- {
		s.Results = append(s.Results, keep[i])
	}
	if len(s.Results) > 256 {
		s.Results = s.Results[len(s.Results)-256:]
	}
	var raw []byte
	for {
		raw, _ = json.Marshal(struct {
			Version int          `json:"version"`
			Results []evalResult `json:"results"`
		}{1, s.Results})
		if len(raw) <= evalStoreLimit {
			break
		}
		s.Results = s.Results[1:]
	}
	path := filepath.Join(filepath.Dir(stateFilePath()), "eval-results.json")
	s.StorageError = "eval_storage_write_failed"
	if os.MkdirAll(filepath.Dir(path), persistDirMode) != nil {
		return
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".eval-results-*")
	if err != nil {
		return
	}
	defer os.Remove(file.Name())
	if err = file.Chmod(persistFileMode); err == nil {
		_, err = file.Write(raw)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil || closeErr != nil || os.Rename(file.Name(), path) != nil {
		return
	}
	s.StorageError = ""
}

func evalSummaryResult(r evalResult) evalResult {
	// Correct the old "partial" presentation without changing persisted
	// scores: an all-error batch contains no valid response at all.
	if len(r.Samples) > 0 {
		allErrors := true
		for _, sample := range r.Samples {
			allErrors = allErrors && sample.Status == "error"
		}
		if allErrors && r.Status == "partial" {
			r.Status = "failed"
		}
	}
	r.Samples = nil
	if r.Attribution != nil {
		a := *r.Attribution
		a.Results, a.Families, a.Diagnostics = nil, nil, nil
		r.Attribution = &a
	}
	return r
}
