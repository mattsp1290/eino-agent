package session

// RepauseRunRequest atomically reverts a fenced run's claim back to paused,
// with no live lease. By default it touches no checkpoint at all: the run's
// most recently promoted revision -- from whenever it was last durably
// paused -- is already the correct one for a later ResumeRun to resume
// from. Used to compensate a claim (ClaimRun) that cannot be safely driven
// forward: a post-claim StartRun failure on ResumeRun's path, or a
// crash-recovery reclaim that has finished conservatively reconciling any
// turn a prior process left dangling (see ReconcileInterruptedTurnRequest).
//
// PromoteRevision, when nonzero, also promotes that already-staged
// checkpoint revision as part of the same atomic repause (round-five
// reconciliation item 2/TR-I1): used when crash reconciliation has just
// settled a dangling turn interrupted and staged a fresh, correctly-
// identified Kind=loop checkpoint for it, so a later ResumeRun reads a
// checkpoint whose recorded turn agrees with the turn reconciliation just
// reconciled, instead of an older, now-superseded promoted revision that
// belongs to a different turn. Distinct from PromotePause, which also
// requires (and performs) interrupting an admitted/running turn as part of
// promotion: RepauseRun's target turn is already durably interrupted by the
// time PromoteRevision is used, so no turn/inbox transition happens here.
type RepauseRunRequest struct {
	Event           EventRecord
	PromoteRevision int64
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
	if request.PromoteRevision < 0 {
		return ErrConflict
	}
	return nil
}
