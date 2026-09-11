package session

// RepauseRunRequest atomically reverts a fenced run's claim back to paused,
// with no live lease, keeping whatever checkpoint revision is currently
// promoted unchanged (it neither promotes nor retires anything). Used to
// compensate a claim (ClaimRun) that cannot be safely driven forward: a
// post-claim StartRun failure on ResumeRun's path, or a crash-recovery
// reclaim that has finished conservatively reconciling any turn a prior
// process left dangling (see ReconcileInterruptedTurnRequest) and has
// nothing further to drive automatically. Distinct from PromotePause, which
// requires (and promotes) a freshly staged checkpoint revision: RepauseRun
// touches no checkpoint at all, since the run's most recently promoted
// revision -- from whenever it was last durably paused -- is already the
// correct one for a later ResumeRun to resume from.
type RepauseRunRequest struct {
	Event EventRecord
}

// RepauseRunResult is the canonical run/event pair committed by a repause.
type RepauseRunResult struct {
	Run   Run
	Event EventRecord
}

// ValidateRepauseRun checks the caller-owned shape of a repause request
// against the currently fenced run before any store mutation.
func ValidateRepauseRun(run Run, request RepauseRunRequest) error {
	if request.Event.ID == "" || request.Event.Kind != RunPausedEventKind ||
		request.Event.RunID != run.ID || request.Event.SessionID != run.SessionID {
		return ErrConflict
	}
	return nil
}
