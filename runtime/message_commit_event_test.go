package runtime

import (
	"context"
	"encoding/json"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// TestMessageCommittedEventFiresAfterAssistantCommitAndToolSettlement is W7
// item 2: a durable session.MessageCommittedEventKind event must be emitted
// once after an assistant turn's content commits (persistAssistantTurn) and
// once after each tool call settles (persistToolSettlement). This drives a
// full turn through a real tool call so both call sites fire: the first
// assistant message commits with a function_tool_call, the tool settles,
// and the second (final) assistant message commits with the reply text --
// three distinct durable messages, three message_committed events, each
// correlated to its own message id.
func TestMessageCommittedEventFiresAfterAssistantCommitAndToolSettlement(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		for _, msg := range request.Messages {
			if msg.Role == einoschema.AgenticRoleTypeUser && isFunctionToolResultMessage(msg) {
				return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
			}
		}
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "echo", `{}`))}, nil
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "ok"}, nil
		}),
	}}})
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}

	batch, err := store.ListEvents(context.Background(), "session-1", session.EventCursor{Limit: 1000})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	var committed []session.EventRecord
	for _, event := range batch.Events {
		if event.Kind == session.MessageCommittedEventKind {
			committed = append(committed, event)
		}
	}
	if len(committed) != 3 {
		t.Fatalf("message_committed events = %d, want 3: %#v", len(committed), committed)
	}
	seenMessages := map[session.MessageID]bool{}
	for _, event := range committed {
		if event.MessageID == "" {
			t.Fatalf("message_committed event missing MessageID: %#v", event)
		}
		if seenMessages[event.MessageID] {
			t.Fatalf("duplicate message_committed event for message %s", event.MessageID)
		}
		seenMessages[event.MessageID] = true
		if event.RunID != result.RunID || event.SessionID != "session-1" {
			t.Fatalf("message_committed event identity = %#v", event)
		}
		var payload struct {
			Revision int64 `json:"revision"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatalf("decode message_committed payload: %v", err)
		}
	}

	// Every committed message must itself carry a non-empty TurnID (W7 item
	// 1: durable identity stamped at append time).
	replay, err := store.ListMessages(context.Background(), "session-1", session.ReplayCursor{Limit: 1000})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	for _, msg := range replay.Messages {
		if seenMessages[msg.ID] && msg.TurnID == "" {
			t.Fatalf("committed message %s has no TurnID: %#v", msg.ID, msg)
		}
	}
}
