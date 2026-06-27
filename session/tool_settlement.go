package session

import (
	"bytes"
	"encoding/json"
	"reflect"
	"time"
)

// ToolSettlement is the terminal state produced by executing a durable tool
// call. Stores can use Apply to make FinishToolCall idempotent while still
// rejecting conflicting repeated settlements.
type ToolSettlement struct {
	ID          ToolCallID
	Status      ToolCallStatus
	Output      json.RawMessage
	Error       string
	Metadata    map[string]string
	CompletedAt time.Time
}

// Apply returns call with this terminal settlement applied. Applying the same
// settlement to an already terminal call is idempotent; applying a different
// terminal settlement reports ErrConflict.
func (s ToolSettlement) Apply(call ToolCall) (ToolCall, error) {
	if s.ID != "" && call.ID != s.ID {
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
	call.Metadata = mergeStringMaps(call.Metadata, s.Metadata)
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
	if !rawMessageEqual(call.Output, settlement.Output) {
		return false
	}
	for key, value := range settlement.Metadata {
		if call.Metadata[key] != value {
			return false
		}
	}
	if !settlement.CompletedAt.IsZero() && !call.CompletedAt.Equal(settlement.CompletedAt) {
		return false
	}
	return true
}

func rawMessageEqual(left json.RawMessage, right json.RawMessage) bool {
	left = bytes.TrimSpace(left)
	right = bytes.TrimSpace(right)
	if len(left) == 0 || len(right) == 0 {
		return len(left) == len(right)
	}
	var leftValue any
	var rightValue any
	if json.Unmarshal(left, &leftValue) == nil && json.Unmarshal(right, &rightValue) == nil {
		return reflect.DeepEqual(leftValue, rightValue)
	}
	return bytes.Equal(left, right)
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	clone := make(json.RawMessage, len(raw))
	copy(clone, raw)
	return clone
}

func mergeStringMaps(base map[string]string, overlay map[string]string) map[string]string {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	merged := make(map[string]string, len(base)+len(overlay))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overlay {
		merged[key] = value
	}
	return merged
}
