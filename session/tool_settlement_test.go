package session

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestToolSettlementApplyIsIdempotent(t *testing.T) {
	now := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	settlement := ToolSettlement{
		ID:          "call-1",
		ClaimedBy:   "worker",
		ClaimToken:  "token",
		Status:      ToolCallCompleted,
		Output:      json.RawMessage(`{"content":"ok"}`),
		Metadata:    map[string]string{"output_status": "completed"},
		CompletedAt: now,
	}
	call := ToolCall{
		ID:         "call-1",
		Status:     ToolCallRunning,
		Metadata:   map[string]string{"tool": "read"},
		ClaimedBy:  "worker",
		ClaimToken: "token",
	}
	settled, err := settlement.Apply(call)
	if err != nil {
		t.Fatalf("apply settlement: %v", err)
	}
	if settled.Status != ToolCallCompleted || string(settled.Output) != `{"content":"ok"}` {
		t.Fatalf("settled call = %+v", settled)
	}
	if _, ok := settled.Metadata["tool"]; ok || settled.Metadata["output_status"] != "completed" {
		t.Fatalf("metadata = %#v", settled.Metadata)
	}
	again, err := settlement.Apply(settled)
	if err != nil {
		t.Fatalf("repeat settlement: %v", err)
	}
	if again.Status != settled.Status || string(again.Output) != string(settled.Output) {
		t.Fatalf("repeat changed settlement: got %+v want %+v", again, settled)
	}
}

func TestToolSettlementApplyRejectsConflictingTerminalState(t *testing.T) {
	now := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	call := ToolCall{
		ID:          "call-1",
		Status:      ToolCallCompleted,
		Output:      json.RawMessage(`{"content":"ok"}`),
		Metadata:    map[string]string{"output_status": "completed"},
		CompletedAt: now,
		ClaimedBy:   "worker",
		ClaimToken:  "token",
	}
	tests := map[string]ToolSettlement{
		"output": {
			ID:          "call-1",
			ClaimedBy:   "worker",
			ClaimToken:  "token",
			Status:      ToolCallCompleted,
			Output:      json.RawMessage(`{"content":"different"}`),
			Metadata:    map[string]string{"output_status": "completed"},
			CompletedAt: now,
		},
		"metadata": {
			ID:          "call-1",
			ClaimedBy:   "worker",
			ClaimToken:  "token",
			Status:      ToolCallCompleted,
			Output:      json.RawMessage(`{"content":"ok"}`),
			Metadata:    map[string]string{"output_status": "changed"},
			CompletedAt: now,
		},
		"completed_at": {
			ID:          "call-1",
			ClaimedBy:   "worker",
			ClaimToken:  "token",
			Status:      ToolCallCompleted,
			Output:      json.RawMessage(`{"content":"ok"}`),
			Metadata:    map[string]string{"output_status": "completed"},
			CompletedAt: now.Add(time.Second),
		},
		"missing_completed_at": {
			ID:         "call-1",
			ClaimedBy:  "worker",
			ClaimToken: "token",
			Status:     ToolCallCompleted,
			Output:     json.RawMessage(`{"content":"ok"}`),
			Metadata:   map[string]string{"output_status": "completed"},
		},
	}
	for name, conflict := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := conflict.Apply(call); !errors.Is(err, ErrConflict) {
				t.Fatalf("conflicting settlement err = %v, want ErrConflict", err)
			}
		})
	}
}

func TestToolSettlementApplyRejectsMissingCompletionTimeForRunningCall(t *testing.T) {
	settlement := ToolSettlement{
		ID:         "call-1",
		ClaimedBy:  "worker",
		ClaimToken: "token",
		Status:     ToolCallCompleted,
		Output:     json.RawMessage(`{"content":"ok"}`),
		Metadata:   map[string]string{"output_status": "completed"},
	}
	if _, err := settlement.Apply(ToolCall{ID: "call-1", Status: ToolCallRunning, ClaimedBy: "worker", ClaimToken: "token"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("missing completion time error = %v, want ErrConflict", err)
	}
}

func TestToolSettlementApplyRejectsNonterminalSettlement(t *testing.T) {
	settlement := ToolSettlement{ID: "call-1", ClaimedBy: "worker", ClaimToken: "token", Status: ToolCallRunning}
	if _, err := settlement.Apply(ToolCall{ID: "call-1", Status: ToolCallRunning, ClaimedBy: "worker", ClaimToken: "token"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("nonterminal settlement err = %v, want ErrConflict", err)
	}
}

func TestToolSettlementApplyRejectsMissingOrMismatchedClaim(t *testing.T) {
	call := ToolCall{ID: "call-1", Status: ToolCallRunning, ClaimedBy: "worker", ClaimToken: "token"}
	for name, settlement := range map[string]ToolSettlement{
		"missing":           {ID: call.ID, Status: ToolCallCompleted},
		"wrong owner":       {ID: call.ID, ClaimedBy: "stale", ClaimToken: call.ClaimToken, Status: ToolCallCompleted},
		"wrong claim token": {ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: "stale", Status: ToolCallCompleted},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := settlement.Apply(call); !errors.Is(err, ErrConflict) {
				t.Fatalf("Apply error = %v, want ErrConflict", err)
			}
		})
	}

	call.Status = ToolCallCompleted
	call.CompletedAt = time.Now().UTC()
	terminalRetry := ToolSettlement{ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: "stale", Status: call.Status, CompletedAt: call.CompletedAt}
	if _, err := terminalRetry.Apply(call); !errors.Is(err, ErrConflict) {
		t.Fatalf("terminal stale claim error = %v, want ErrConflict", err)
	}
}
