package history

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

func TestProjectReplayHistoryGolden(t *testing.T) {
	t.Parallel()

	batch := session.ReplayBatch{
		Messages: []session.Message{
			message("user-1", session.RoleUser),
			message("assistant-1", session.RoleAssistant),
			message("tool-1", session.RoleTool),
			message("assistant-2", session.RoleAssistant),
			message("assistant-live", session.RoleAssistant),
		},
		Parts: []session.Part{
			part("p2", "assistant-1", session.PartToolCall, 20, `{"id":"call-1","name":"file_read","arguments":{"path":"README.md"}}`),
			part("p1", "assistant-1", session.PartText, 10, `{"text":"I will read it."}`),
			part("p0", "user-1", session.PartText, 10, `{"text":"Read README"}`),
			part("p3", "tool-1", session.PartToolResult, 10, `{"tool_call_id":"call-1","content":"README contents"}`),
			part("p4", "assistant-2", session.PartText, 10, `{"text":"Summary"}`),
			part("p5", "assistant-live", session.PartText, 10, `{"text":"settled"}`),
		},
	}
	projected, err := Project(batch, Options{})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	if len(projected) != 5 {
		t.Fatalf("projected len = %d", len(projected))
	}
	assertMessage(t, projected[0], schema.User, "Read README")
	assertMessage(t, projected[1], schema.Assistant, "I will read it.")
	if len(projected[1].ToolCalls) != 1 || projected[1].ToolCalls[0].Function.Name != "file_read" {
		t.Fatalf("tool calls = %#v", projected[1].ToolCalls)
	}
	assertMessage(t, projected[2], schema.Tool, "README contents")
	if projected[2].ToolCallID != "call-1" {
		t.Fatalf("tool call id = %q", projected[2].ToolCallID)
	}
	assertMessage(t, projected[3], schema.Assistant, "Summary")
	assertMessage(t, projected[4], schema.Assistant, "settled")
}

func TestProjectExcludesReasoningAndIncludesStateWhenEnabled(t *testing.T) {
	t.Parallel()

	batch := session.ReplayBatch{
		Messages: []session.Message{message("assistant-1", session.RoleAssistant)},
		Parts: []session.Part{
			part("reasoning", "assistant-1", session.PartReasoning, 10, `{"text":"private reasoning"}`),
			part("state", "assistant-1", session.PartState, 20, `{"text":"state snapshot"}`),
		},
	}
	projected, err := Project(batch, Options{})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	if projected[0].Content != "" {
		t.Fatalf("content with defaults = %q, want empty", projected[0].Content)
	}
	projected, err = Project(batch, Options{IncludeReasoning: true, IncludeState: true})
	if err != nil {
		t.Fatalf("Project with options error = %v", err)
	}
	if projected[0].Content != "private reasoningstate snapshot" {
		t.Fatalf("content with options = %q", projected[0].Content)
	}
}

func TestProjectCompactionBoundaryIncludesSummary(t *testing.T) {
	t.Parallel()

	batch := session.ReplayBatch{
		Messages: []session.Message{message("summary", session.RoleSystem)},
		Parts: []session.Part{
			part("compaction", "summary", session.PartCompaction, 10, `{"text":"Earlier context summary."}`),
			part("text", "summary", session.PartText, 20, `{"text":" Tail instruction."}`),
		},
	}
	projected, err := Project(batch, Options{})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	assertMessage(t, projected[0], schema.System, "Earlier context summary. Tail instruction.")
}

func message(id session.MessageID, role session.Role) session.Message {
	now := time.Unix(1, 0)
	return session.Message{
		ID:        id,
		SessionID: "session-1",
		RunID:     "run-1",
		Role:      role,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func part(id session.PartID, messageID session.MessageID, kind session.PartKind, ordinal int64, payload string) session.Part {
	now := time.Unix(1, 0)
	return session.Part{
		ID:        id,
		MessageID: messageID,
		SessionID: "session-1",
		RunID:     "run-1",
		Kind:      kind,
		Ordinal:   ordinal,
		Payload:   json.RawMessage(payload),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func assertMessage(t *testing.T, message *schema.Message, role schema.RoleType, content string) {
	t.Helper()
	if message.Role != role || message.Content != content {
		t.Fatalf("message = %#v, want %s/%q", message, role, content)
	}
}
