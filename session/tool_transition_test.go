package session

import (
	"encoding/json"
	"testing"
	"time"
)

func TestToolTransitionRecordUsesOneTerminalPhase(t *testing.T) {
	at := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	base := ToolCall{ID: "call", SessionID: "session", RunID: "run", MessageID: "message", Name: "tool", Input: json.RawMessage(`{"ok":true}`), ClaimedBy: "worker", ClaimToken: "claim", StartedAt: at.Add(-time.Second), CompletedAt: at}
	for _, status := range []ToolCallStatus{ToolCallCompleted, ToolCallFailed, ToolCallInterrupted} {
		call := base
		call.Status = status
		record, err := ToolTransitionRecord(call, ToolTransitionEvent{ID: EventID("event-" + status), CreatedAt: at})
		if err != nil {
			t.Fatalf("status %q: %v", status, err)
		}
		if record.ToolTransition != ToolTransitionTerminal {
			t.Fatalf("status %q phase = %q, want terminal", status, record.ToolTransition)
		}
	}
}

func TestToolTransitionRecordRejectsContradictoryPhaseState(t *testing.T) {
	at := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	tests := map[string]ToolCall{
		"pending with claim":          {ID: "call", SessionID: "session", RunID: "run", MessageID: "message", Name: "tool", Status: ToolCallPending, ClaimedBy: "worker"},
		"running without token":       {ID: "call", SessionID: "session", RunID: "run", MessageID: "message", Name: "tool", Status: ToolCallRunning, ClaimedBy: "worker", StartedAt: at},
		"terminal timestamp mismatch": {ID: "call", SessionID: "session", RunID: "run", MessageID: "message", Name: "tool", Status: ToolCallCompleted, ClaimedBy: "worker", ClaimToken: "claim", CompletedAt: at.Add(time.Second)},
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ToolTransitionRecord(call, ToolTransitionEvent{ID: "event", CreatedAt: at}); err != ErrConflict {
				t.Fatalf("error = %v, want ErrConflict", err)
			}
		})
	}
}
