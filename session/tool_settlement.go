package session

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"time"
)

// ToolSettlement is the terminal state produced by executing a durable tool
// call. Stores can use Apply to make FinishToolCall idempotent while still
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

// ToolClaimIdentity is the fencing identity a settlement must present for the
// durable claim whose work it is committing.
type ToolClaimIdentity struct {
	ClaimedBy  string
	ClaimToken string
}

// ToolSettlementStore atomically commits terminal tool state and its reserved
// model-visible result message/part. Implementations must be idempotent by call
// and reserved result IDs, and must reject settlements whose claim identity
// does not exactly match the durable call.
type ToolSettlementStore interface {
	SettleToolCall(context.Context, ToolSettlement) error
	ListUnreconciledToolSettlements(context.Context, RunID) ([]ToolSettlement, error)
}

// Apply returns call with this terminal settlement applied. Applying the same
// settlement to an already terminal call is idempotent; applying a different
// terminal settlement reports ErrConflict.
func (s ToolSettlement) Apply(call ToolCall) (ToolCall, error) {
	if s.ID != "" && call.ID != s.ID {
		return ToolCall{}, ErrConflict
	}
	if s.ClaimedBy == "" || s.ClaimToken == "" || call.ClaimedBy != s.ClaimedBy || call.ClaimToken != s.ClaimToken {
		return ToolCall{}, ErrConflict
	}
	if callTerminal(call.Status) {
		if s.CompletedAt.IsZero() {
			s.CompletedAt = call.CompletedAt
		}
		if settlementMatches(call, s) {
			return call, nil
		}
		return ToolCall{}, ErrConflict
	}
	if !settlementTerminal(s.Status) {
		return ToolCall{}, ErrConflict
	}
	if s.CompletedAt.IsZero() {
		s.CompletedAt = time.Now().UTC()
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
	if !rawMessageEqual(call.Output, settlement.Output) {
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
