package session

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/mattsp1290/eino-agent/internal/jsonequal"
)

// ToolSettlement is the terminal state produced by executing a durable tool
// call. Stores can use Apply to make SettleToolCall idempotent while still
// rejecting conflicting repeated settlements.
type ToolSettlement struct {
	ID            ToolCallID
	ClaimedBy     string
	ClaimToken    string
	Status        ToolCallStatus
	Output        json.RawMessage
	Error         string
	Metadata      map[string]string
	CompletedAt   time.Time
	ResultMessage Message
	ResultPart    Part
}

// Apply returns call with this terminal settlement applied. Applying the same
// settlement to an already terminal call is idempotent; applying a different
// terminal settlement reports ErrConflict.
func (s ToolSettlement) Apply(call ToolCall) (ToolCall, error) {
	if s.CompletedAt.IsZero() {
		return ToolCall{}, ErrConflict
	}
	if s.ID != "" && call.ID != s.ID {
		return ToolCall{}, ErrConflict
	}
	if s.ClaimedBy == "" || s.ClaimToken == "" || call.ClaimedBy != s.ClaimedBy || call.ClaimToken != s.ClaimToken {
		return ToolCall{}, ErrConflict
	}
	if callTerminal(call.Status) {
		if settlementMatches(call, s) {
			return call, nil
		}
		return ToolCall{}, ErrConflict
	}
	if !settlementTerminal(s.Status) {
		return ToolCall{}, ErrConflict
	}
	call.Status = s.Status
	call.Output = cloneRawMessage(s.Output)
	call.Error = s.Error
	call.Metadata = cloneStringMap(s.Metadata)
	call.CompletedAt = s.CompletedAt
	return call, nil
}

// TerminalToolCall reports whether status is a terminal tool-call state.
func TerminalToolCall(status ToolCallStatus) bool {
	return callTerminal(status)
}

func settlementTerminal(status ToolCallStatus) bool {
	return status == ToolCallCompleted || status == ToolCallFailed || status == ToolCallInterrupted
}

func callTerminal(status ToolCallStatus) bool {
	return settlementTerminal(status)
}

func settlementMatches(call ToolCall, settlement ToolSettlement) bool {
	if call.Status != settlement.Status || call.Error != settlement.Error {
		return false
	}
	if !jsonequal.Equal(call.Output, settlement.Output) {
		return false
	}
	if !reflect.DeepEqual(call.Metadata, settlement.Metadata) {
		return false
	}
	if !call.CompletedAt.Equal(settlement.CompletedAt) {
		return false
	}
	return true
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	clone := make(json.RawMessage, len(raw))
	copy(clone, raw)
	return clone
}

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	clone := make(map[string]string, len(src))
	for key, value := range src {
		clone[key] = value
	}
	return clone
}
