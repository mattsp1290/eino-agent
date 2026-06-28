package session

import "time"

// ResumeState classifies whether a durable run may be resumed by a worker.
type ResumeState string

const (
	ResumeTerminal ResumeState = "terminal"
	ResumeOwned    ResumeState = "owned"
	ResumeStale    ResumeState = "stale"
	ResumeBusy     ResumeState = "busy"
)

// ClassifyResume reports whether owner may resume run at now.
func ClassifyResume(run Run, owner string, now time.Time) ResumeState {
	if run.Terminal() {
		return ResumeTerminal
	}
	if run.OwnerID == owner {
		return ResumeOwned
	}
	if !run.LeaseUntil.IsZero() && run.LeaseUntil.After(now) {
		return ResumeBusy
	}
	return ResumeStale
}
