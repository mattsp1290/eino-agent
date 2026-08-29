package session

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/mattsp1290/eino-agent/internal/jsonequal"
)

const ToolTransitionEventKind = "tool_call_updated"

// ToolTransitionEvent contains the bounded event-envelope values that are not
// part of tool state. CreatedAt is authoritative for creation and must match
// StartedAt or CompletedAt for later phases.
type ToolTransitionEvent struct {
	ID         EventID
	EpochID    EpochID
	ProviderID string
	ModelID    string
	CreatedAt  time.Time
}

// CreateToolCallRequest atomically creates pending state and its event.
type CreateToolCallRequest struct {
	Call  ToolCall
	Event ToolTransitionEvent
}

// ClaimToolCallRequest atomically claims pending state, renews the run lease,
// and records the running event. The store derives LeaseUntil.
type ClaimToolCallRequest struct {
	ID            ToolCallID
	ClaimedBy     string
	ClaimToken    string
	StartedAt     time.Time
	LeaseDuration time.Duration
	Event         ToolTransitionEvent
}

// SettleToolCallRequest atomically commits terminal state, the reserved result
// envelope, and the terminal event.
type SettleToolCallRequest struct {
	Settlement ToolSettlement
	Event      ToolTransitionEvent
}

// ToolTransitionResult is the canonical call/event pair committed by one
// atomic tool transition.
type ToolTransitionResult struct {
	Call  ToolCall
	Event EventRecord
}

type toolTransitionPayload struct {
	ID        ToolCallID        `json:"id"`
	Name      string            `json:"name"`
	Arguments json.RawMessage   `json:"arguments"`
	Output    json.RawMessage   `json:"output,omitempty"`
	Status    ToolCallStatus    `json:"status"`
	Error     string            `json:"error,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// ToolTransitionRecord returns the canonical durable event derived from a
// complete phase state and its bounded event envelope.
func ToolTransitionRecord(call ToolCall, event ToolTransitionEvent) (EventRecord, error) {
	if call.ID == "" || call.SessionID == "" || call.RunID == "" || call.MessageID == "" || call.Name == "" || event.ID == "" || event.CreatedAt.IsZero() {
		return EventRecord{}, ErrConflict
	}
	switch call.Status {
	case ToolCallPending:
		if call.ClaimedBy != "" || call.ClaimToken != "" || !call.StartedAt.IsZero() || !call.CompletedAt.IsZero() {
			return EventRecord{}, ErrConflict
		}
	case ToolCallRunning:
		if call.ClaimedBy == "" || call.ClaimToken == "" || call.StartedAt.IsZero() || !call.CompletedAt.IsZero() || !call.StartedAt.Equal(event.CreatedAt) {
			return EventRecord{}, ErrConflict
		}
	case ToolCallCompleted, ToolCallFailed, ToolCallInterrupted:
		if call.ClaimedBy == "" || call.ClaimToken == "" || call.CompletedAt.IsZero() || !call.CompletedAt.Equal(event.CreatedAt) {
			return EventRecord{}, ErrConflict
		}
	default:
		return EventRecord{}, ErrConflict
	}
	var phase ToolTransitionPhase
	switch call.Status {
	case ToolCallPending:
		phase = ToolTransitionPending
	case ToolCallRunning:
		phase = ToolTransitionRunning
	default:
		phase = ToolTransitionTerminal
	}
	payload, err := json.Marshal(toolTransitionPayload{
		ID: call.ID, Name: call.Name, Arguments: cloneRawMessage(call.Input), Output: cloneRawMessage(call.Output),
		Status: call.Status, Error: call.Error, Metadata: cloneStringMap(call.Metadata),
	})
	if err != nil {
		return EventRecord{}, err
	}
	return EventRecord{
		ID: event.ID, SessionID: call.SessionID, RunID: call.RunID, MessageID: call.MessageID,
		ToolCallID: call.ID, ToolTransition: phase, EpochID: event.EpochID,
		ProviderID: event.ProviderID, ModelID: event.ModelID, Kind: ToolTransitionEventKind,
		Redaction: RedactionContent, Payload: payload, CreatedAt: event.CreatedAt.UTC(),
	}, nil
}

// SameToolTransitionState compares authoritative phase state while ignoring a
// store-derived lease deadline.
func SameToolTransitionState(left, right ToolCall) bool {
	left.LeaseUntil = time.Time{}
	right.LeaseUntil = time.Time{}
	return left.ID == right.ID && left.SessionID == right.SessionID && left.RunID == right.RunID && left.MessageID == right.MessageID &&
		left.ResultMessageID == right.ResultMessageID && left.ResultPartID == right.ResultPartID && left.Name == right.Name && left.Pattern == right.Pattern &&
		jsonequal.Equal(left.Input, right.Input) && jsonequal.Equal(left.Output, right.Output) && left.Status == right.Status && left.RetrySafe == right.RetrySafe &&
		reflect.DeepEqual(left.Metadata, right.Metadata) && left.ClaimedBy == right.ClaimedBy && left.ClaimToken == right.ClaimToken &&
		left.StartedAt.Equal(right.StartedAt) && left.CompletedAt.Equal(right.CompletedAt) && left.Error == right.Error
}
