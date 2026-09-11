package session

import "time"

// TurnState is the durable lifecycle of one admitted turn.
type TurnState string

const (
	// TurnAdmitted means AdmitTurn created the turn but it has not settled.
	TurnAdmitted TurnState = "admitted"
	// TurnRunning is a transient, non-store-owned state runtime callers may
	// project locally; no store method in this package writes it.
	TurnRunning TurnState = "running"
	// TurnCompleted means CompleteTurn settled the turn normally.
	TurnCompleted TurnState = "completed"
	// TurnInterrupted means InterruptTurn or PromotePause settled the turn
	// without a normal response.
	TurnInterrupted TurnState = "interrupted"
	// TurnFailed is reserved for a future terminal failure path; no store
	// method in this package writes it.
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
	EpochID            EpochID
	CreatedAt          time.Time
	StartedAt          time.Time
	FinishedAt         time.Time
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
			current.FinishedAt.Equal(request.Event.CreatedAt.UTC()) {
			return current, nil
		}
		return Turn{}, ErrConflict
	}
	if current.State != TurnAdmitted && current.State != TurnRunning {
		return Turn{}, ErrConflict
	}
	current.State = TurnCompleted
	current.ResponseMessageIDs = append([]MessageID(nil), request.ResponseMessageIDs...)
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
