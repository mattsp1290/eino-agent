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
		Status:      ToolCallCompleted,
		Output:      json.RawMessage(`{"content":"ok"}`),
		Metadata:    map[string]string{"output_status": "completed"},
		CompletedAt: now,
	}
	call := ToolCall{
		ID:        "call-1",
		Status:    ToolCallRunning,
		Metadata:  map[string]string{"tool": "read"},
		ClaimedBy: "worker",
	}
	settled, err := settlement.Apply(call)
	if err != nil {
		t.Fatalf("apply settlement: %v", err)
	}
	if settled.Status != ToolCallCompleted || string(settled.Output) != `{"content":"ok"}` {
		t.Fatalf("settled call = %+v", settled)
	}
	if settled.Metadata["tool"] != "read" || settled.Metadata["output_status"] != "completed" {
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
	call := ToolCall{
		ID:     "call-1",
		Status: ToolCallCompleted,
		Output: json.RawMessage(`{"content":"ok"}`),
	}
	conflict := ToolSettlement{
		ID:     "call-1",
		Status: ToolCallCompleted,
		Output: json.RawMessage(`{"content":"different"}`),
	}
	if _, err := conflict.Apply(call); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting settlement err = %v, want ErrConflict", err)
	}
}

func TestToolSettlementApplyRejectsNonterminalSettlement(t *testing.T) {
	settlement := ToolSettlement{ID: "call-1", Status: ToolCallRunning}
	if _, err := settlement.Apply(ToolCall{ID: "call-1", Status: ToolCallRunning}); !errors.Is(err, ErrConflict) {
		t.Fatalf("nonterminal settlement err = %v, want ErrConflict", err)
	}
}
