package session

import "time"

// TurnState is the durable lifecycle of one admitted turn.
type TurnState string

const (
	// TurnAdmitted means AdmitTurn created the turn but it has not settled.
	TurnAdmitted TurnState = "admitted"
	// TurnRunning means ResumeInterruptedTurn durably resumed a turn a
	// prior process left TurnInterrupted (whether by a genuine mid-dispatch
	// pause or by crash reconciliation -- see ReconcileInterruptedTurnRequest
	// and ResumeInterruptedTurnRequest) and it is once again being actively
	// driven. It is also the state runtime callers may project locally for
	// the equivalent, checkpoint-driven ADK resume path (see
	// runtime/turn_loop.go's genResume), where no store write marks the
	// transition explicitly (ADK's own checkpoint restoration is the
	// durability boundary there).
	TurnRunning TurnState = "running"
	// TurnCompleted means CompleteTurn settled the turn normally.
	TurnCompleted TurnState = "completed"
	// TurnInterrupted means InterruptTurn or PromotePause settled the turn
	// without a normal response.
	TurnInterrupted TurnState = "interrupted"
	// TurnFailed means the turn's own run settled terminally (SettleRun)
	// while the turn was still TurnAdmitted, TurnRunning, or TurnInterrupted
	// (see ApplyFailTurn): unlike TurnInterrupted, which always implies a
	// turn a later ResumeRun may still redrive, TurnFailed marks a turn its
	// now-terminal run will never touch again -- no store method transitions
	// a TurnFailed turn onward.
	TurnFailed TurnState = "failed"
)

// Turn records one admitted execution step inside a run: the user input that
// triggered it, the assistant placeholder it produced, and (once settled) the
// response messages it produced.
type Turn struct {
	ID                 TurnID
	RunID              RunID
	SessionID          ID
	Ordinal            int64
	State              TurnState
	InboxIDs           []InboxID
	UserMessageIDs     []MessageID
	AssistantMessageID MessageID
	ResponseMessageIDs []MessageID
	// Usage records this turn's own provider usage, written atomically by
	// CompleteTurn. Zero for a turn that never completed normally.
	Usage      Usage
	EpochID    EpochID
	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time
}

// AdmitTurnRequest atomically admits the next-ordinal turn for a run.
type AdmitTurnRequest struct {
	Turn                 Turn
	UserMessages         []Message
	UserParts            []Part
	AssistantPlaceholder Message
	Event                EventRecord
	InboxIDs             []InboxID
}

// AdmitTurnResult is the canonical turn/event pair committed by admission.
type AdmitTurnResult struct {
	Turn  Turn
	Event EventRecord
}

// CompleteTurnRequest atomically settles an admitted turn as completed.
type CompleteTurnRequest struct {
	TurnID             TurnID
	ResponseMessageIDs []MessageID
	Usage              Usage
	Event              EventRecord
}

// CompleteTurnResult is the canonical turn/event pair committed by completion.
type CompleteTurnResult struct {
	Turn  Turn
	Event EventRecord
}

// InterruptTurnRequest atomically settles an admitted turn as interrupted.
type InterruptTurnRequest struct {
	TurnID TurnID
	Event  EventRecord
}

// InterruptTurnResult is the canonical turn/event pair committed by an
// interruption.
type InterruptTurnResult struct {
	Turn  Turn
	Event EventRecord
}

// ReconcileInterruptedTurnRequest atomically reconciles a turn a crashed
// process left `admitted`/`running` with no checkpoint describing it: it
// settles the turn interrupted, exactly like InterruptTurn -- including
// carrying the turn's InboxConsumed rows forward as InboxInterrupted, never
// requeued to InboxQueued. The turn's user messages/parts were already
// durably committed by the AdmitTurn that admitted it, so they must never be
// re-admitted into a second turn (that would duplicate them in provider
// history); the conservative, correct continuation is to resume THIS SAME
// turn later, by TurnID, via ResumeInterruptedTurnRequest -- never a fresh
// AdmitTurn over the same inbox items.
type ReconcileInterruptedTurnRequest struct {
	TurnID TurnID
	Event  EventRecord
}

// ReconcileInterruptedTurnResult is the canonical turn/event pair committed
// by a reconciliation.
type ReconcileInterruptedTurnResult struct {
	Turn  Turn
	Event EventRecord
}

// ResumeInterruptedTurnRequest atomically resumes a turn a prior process
// left TurnInterrupted -- whether by a genuine mid-dispatch pause this same
// mechanism already resumed once, or by crash reconciliation
// (ReconcileInterruptedTurnRequest) -- for a fresh redrive under the SAME
// TurnID and the SAME already-committed user message rows: never a second
// AdmitTurn, never a new user message. It transitions the turn to
// TurnRunning and its own InboxInterrupted rows back to InboxConsumed
// (mirroring AdmitTurn's queued -> consumed claim, but from the interrupted
// state), so a later crash mid-redrive leaves the turn TurnRunning, which
// reconcileCrashedRun's existing dangling-turn detection (TurnAdmitted ||
// TurnRunning) already recognizes: the resume/reconcile cycle is safely
// repeatable until CompleteTurn (which already accepts TurnRunning as a
// starting state) finally settles it.
type ResumeInterruptedTurnRequest struct {
	TurnID TurnID
	// ResumedAt stamps the moment this resume is durably recorded, used to
	// timestamp the turn's own inbox items' transition back to consumed.
	ResumedAt time.Time
}

// ResumeInterruptedTurnResult is the canonical turn resumed by
// ResumeInterruptedTurn.
type ResumeInterruptedTurnResult struct {
	Turn Turn
}

// ApplyResumeInterruptedTurn derives the canonical resumed turn. Applying an
// identical resume to an already-resumed (TurnRunning) turn is idempotent.
func ApplyResumeInterruptedTurn(current Turn, request ResumeInterruptedTurnRequest) (Turn, error) {
	if current.ID == "" || current.ID != request.TurnID || request.ResumedAt.IsZero() {
		return Turn{}, ErrConflict
	}
	if current.State == TurnRunning {
		return current, nil
	}
	if current.State != TurnInterrupted {
		return Turn{}, ErrConflict
	}
	current.State = TurnRunning
	return current, nil
}

// ValidateAdmitTurn checks the caller-owned shape of an admission request
// against the currently fenced run, before any store mutation: identities
// must agree, the turn must be freshly admitted with no terminal fields set,
// and the event must be the canonical turn_started envelope correlated to
// this turn.
func ValidateAdmitTurn(run Run, request AdmitTurnRequest) error {
	t := request.Turn
	if t.ID == "" || t.RunID != run.ID || t.SessionID != run.SessionID || t.Ordinal <= 0 ||
		t.State != TurnAdmitted || !t.FinishedAt.IsZero() || len(t.ResponseMessageIDs) != 0 {
		return ErrConflict
	}
	if request.Event.ID == "" || request.Event.Kind != TurnStartedEventKind ||
		request.Event.RunID != run.ID || request.Event.SessionID != run.SessionID || request.Event.TurnID != t.ID {
		return ErrConflict
	}
	for _, m := range request.UserMessages {
		if m.RunID != run.ID || m.SessionID != run.SessionID || m.Role != RoleUser {
			return ErrConflict
		}
	}
	for _, p := range request.UserParts {
		if p.RunID != run.ID || p.SessionID != run.SessionID {
			return ErrConflict
		}
	}
	if request.AssistantPlaceholder.ID != "" {
		placeholder := request.AssistantPlaceholder
		if placeholder.RunID != run.ID || placeholder.SessionID != run.SessionID || placeholder.Role != RoleAssistant {
			return ErrConflict
		}
	}
	return nil
}

// ApplyCompleteTurn derives the canonical completed turn from its currently
// admitted durable state and the caller-owned completion fields. Applying an
// identical completion to an already-completed turn is idempotent; applying
// different terminal fields to a settled turn reports ErrConflict.
func ApplyCompleteTurn(current Turn, request CompleteTurnRequest) (Turn, error) {
	if current.ID == "" || current.ID != request.TurnID || request.Event.ID == "" ||
		request.Event.Kind != TurnCompletedEventKind || request.Event.CreatedAt.IsZero() {
		return Turn{}, ErrConflict
	}
	if current.State == TurnCompleted {
		if sameMessageIDs(current.ResponseMessageIDs, request.ResponseMessageIDs) &&
			current.FinishedAt.Equal(request.Event.CreatedAt.UTC()) && current.Usage == request.Usage {
			return current, nil
		}
		return Turn{}, ErrConflict
	}
	// TurnInterrupted is a valid starting state (not just TurnAdmitted/
	// TurnRunning): a durably paused turn resumed via ResumeRun completes
	// through this same atomic path once its checkpoint-resumed execution
	// reaches normal completion (see runtime/turn_loop.go's onAgentEvents).
	if current.State != TurnAdmitted && current.State != TurnRunning && current.State != TurnInterrupted {
		return Turn{}, ErrConflict
	}
	current.State = TurnCompleted
	current.ResponseMessageIDs = append([]MessageID(nil), request.ResponseMessageIDs...)
	current.Usage = request.Usage
	current.FinishedAt = request.Event.CreatedAt.UTC()
	return current, nil
}

// ApplyInterruptTurn derives the canonical interrupted turn. Applying an
// identical interruption to an already-interrupted turn is idempotent.
func ApplyInterruptTurn(current Turn, request InterruptTurnRequest) (Turn, error) {
	if current.ID == "" || current.ID != request.TurnID || request.Event.ID == "" || request.Event.CreatedAt.IsZero() {
		return Turn{}, ErrConflict
	}
	if current.State == TurnInterrupted {
		if current.FinishedAt.Equal(request.Event.CreatedAt.UTC()) {
			return current, nil
		}
		return Turn{}, ErrConflict
	}
	if current.State != TurnAdmitted && current.State != TurnRunning {
		return Turn{}, ErrConflict
	}
	current.State = TurnInterrupted
	current.FinishedAt = request.Event.CreatedAt.UTC()
	return current, nil
}

// ApplyFailTurn derives the canonical failed turn from its currently
// non-terminal durable state (TurnAdmitted, TurnRunning, or TurnInterrupted).
// It exists for exactly one caller: a run's own terminal settlement
// (SettleRun), which must never leave a turn behind in a state that implies
// it might still run again once its run cannot ever be resumed (every
// mutation path -- ResumeInterruptedTurn, CompleteTurn, ReconcileInterrupted
// Turn -- fences through a live, nonterminal run). TurnInterrupted's usual
// meaning ("durably paused, may still resume") no longer applies once the
// run itself is terminal, so this is TurnFailed's first writer -- see its
// own doc comment. Applying an identical failure to an already-failed turn
// is idempotent.
func ApplyFailTurn(current Turn, at time.Time) (Turn, error) {
	if current.ID == "" || at.IsZero() {
		return Turn{}, ErrConflict
	}
	if current.State == TurnFailed {
		if current.FinishedAt.Equal(at.UTC()) {
			return current, nil
		}
		return Turn{}, ErrConflict
	}
	if current.State != TurnAdmitted && current.State != TurnRunning && current.State != TurnInterrupted {
		return Turn{}, ErrConflict
	}
	current.State = TurnFailed
	current.FinishedAt = at.UTC()
	return current, nil
}

func sameMessageIDs(left, right []MessageID) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
