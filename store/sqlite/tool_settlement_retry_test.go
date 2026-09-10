package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func TestTerminalToolSettlementRetryValidatesReservedResultRows(t *testing.T) {
	st, execution, call := setupClaimedToolCall(t)
	defer func() { _ = st.db.Close() }()

	ctx := context.Background()
	now := time.Now().UTC()
	output := json.RawMessage(`{"tool_call_id":"call-tool","status":"completed","content":"ok"}`)
	resultMessage, resultPart := sqliteToolResultEnvelope(call, output, now)
	settlement := session.ToolSettlement{
		ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken,
		Status: session.ToolCallCompleted, Output: output, CompletedAt: now,
		ResultMessage: resultMessage,
		ResultPart:    resultPart,
	}
	request := sqliteSettleRequest(settlement, "event-settle-retry")
	if _, err := execution.SettleToolCall(ctx, request); err != nil {
		t.Fatal(err)
	}

	mutated := settlement.ResultPart
	mutated.Payload = json.RawMessage(`{"tool_call_id":"call-tool","status":"completed","content":"mutated"}`)
	if err := execution.UpdatePart(ctx, mutated); err != nil {
		t.Fatalf("mutate settled result part: %v", err)
	}
	if _, err := execution.SettleToolCall(ctx, request); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("repeated settlement = %v, want ErrConflict", err)
	}

	var raw []byte
	if err := st.db.QueryRowContext(ctx, "SELECT record FROM parts WHERE id = ?", []byte(mutated.ID)).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var stored session.Part
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored, mutated) {
		t.Fatalf("mutated result part changed after rejected retry: stored=%#v mutated=%#v", stored, mutated)
	}
	var status string
	if err := st.db.QueryRowContext(ctx, "SELECT status FROM tool_calls WHERE id = ?", []byte(call.ID)).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(session.ToolCallCompleted) {
		t.Fatalf("settled call status = %q", status)
	}
}
