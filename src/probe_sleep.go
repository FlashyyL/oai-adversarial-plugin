package main

import (
	"errors"
	"time"
)

var probeSleepZone = time.FixedZone("UTC+8", 8*60*60)

// Equal endpoints disable the daily sleep window. End is exclusive.
type probeSleepHours struct {
	Start int `json:"start_hour"`
	End   int `json:"end_hour"`
}

func (s probeSleepHours) validate() error {
	if s.Start < 0 || s.Start > 23 || s.End < 0 || s.End > 23 {
		return errors.New("休眠时间须为 0–23 的整数小时")
	}
	return nil
}

func (s probeSleepHours) until(now time.Time) time.Time {
	if s.Start == s.End {
		return time.Time{}
	}
	local := now.In(probeSleepZone)
	hour := local.Hour()
	inside := hour >= s.Start && hour < s.End
	if s.Start > s.End {
		inside = hour >= s.Start || hour < s.End
	}
	if !inside {
		return time.Time{}
	}
	end := time.Date(local.Year(), local.Month(), local.Day(), s.End, 0, 0, 0, probeSleepZone)
	if !end.After(local) {
		end = end.AddDate(0, 0, 1)
	}
	return end
}

func (e *probeEngine) setSleepHours(hours probeSleepHours) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	settings := e.settings
	settings.SleepHours = &hours
	return e.saveSettingsLocked(settings)
}

func sleepUntilText(hours probeSleepHours, now time.Time) string {
	if until := hours.until(now); !until.IsZero() {
		return until.UTC().Format(time.RFC3339)
	}
	return ""
}

func (e *probeEngine) waitForProbeWake(model string, stop, brake <-chan struct{}) bool {
	return e.waitForTaskWake(probeTask{Model: model}, stop, brake)
}

// Share sleep and the global brake, but health participation pauses must not
// cancel explicit ability tests. Their own stop channel remains effective.
func (e *probeEngine) waitForTaskWake(task probeTask, stop, brake <-chan struct{}) bool {
	for {
		e.mu.Lock()
		until := e.cfg.Config.SleepHours.until(time.Now())
		cancelled := e.shuttingDown || (task.Evaluation == nil && e.targetPausedLocked(task.Model))
		e.mu.Unlock()
		if cancelled {
			return false
		}
		select {
		case <-stop:
			return false
		case <-brake:
			return false
		default:
		}
		if until.IsZero() {
			return true
		}
		timer := time.NewTimer(min(time.Second, max(0, time.Until(until))))
		select {
		case <-stop:
			timer.Stop()
			return false
		case <-brake:
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}
