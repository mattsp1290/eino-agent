package history

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
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
			part("p3", "assistant-1", session.PartToolResult, 30, `{"tool_call_id":"call-1","content":"README contents"}`),
			part("p4", "assistant-2", session.PartText, 10, `{"text":"Summary"}`),
			part("p5", "assistant-live", session.PartText, 10, `{"text":"settled"}`),
		},
	}
	projected, err := Project(batch, Options{})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	got := goldenMessages(projected)
	want := readHistoryGolden(t, "../../testdata/history/replay_projection.json")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("projected history = %#v, want %#v", got, want)
	}
}

func TestProjectToolResultStructuredAndExpectedFailurePayloads(t *testing.T) {
	t.Parallel()

	batch := session.ReplayBatch{
		Messages: []session.Message{message("assistant-1", session.RoleAssistant)},
		Parts: []session.Part{
			part("structured", "assistant-1", session.PartToolResult, 10, `{"tool_call_id":"call-1","status":"completed","structured":{"ok":true}}`),
			part("failure", "assistant-1", session.PartToolResult, 20, `{"tool_call_id":"call-2","status":"expected_failure","content":"denied"}`),
		},
	}
	projected, err := Project(batch, Options{})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	if len(projected) != 3 {
		t.Fatalf("projected len = %d", len(projected))
	}
	assertMessage(t, projected[1], schema.Tool, `{"ok":true}`)
	if projected[1].ToolCallID != "call-1" {
		t.Fatalf("structured tool call id = %q", projected[1].ToolCallID)
	}
	if projected[2].ToolCallID != "call-2" || projected[2].Content == "denied" {
		t.Fatalf("expected failure projection = %#v", projected[2])
	}
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

func TestProjectEpochExcludesCompactedRawHistory(t *testing.T) {
	t.Parallel()

	batch := session.ReplayBatch{
		Messages: []session.Message{
			message("old", session.RoleUser),
			message("summary", session.RoleSystem),
			message("tail", session.RoleUser),
		},
		Parts: []session.Part{
			part("old-secret", "old", session.PartText, 10, `{"text":"SECRET old raw prompt"}`),
			part("summary", "summary", session.PartCompaction, 10, `{"text":"Summarized safely."}`),
			part("tail", "tail", session.PartText, 10, `{"text":"Continue"}`),
		},
	}
	projected, err := Project(batch, Options{Epoch: &session.ContextEpoch{
		SummaryMessageID: "summary",
		SummarizedToID:   "old",
		TailStartID:      "tail",
	}})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	if len(projected) != 2 {
		t.Fatalf("projected len = %d", len(projected))
	}
	assertMessage(t, projected[0], schema.System, "Summarized safely.")
	assertMessage(t, projected[1], schema.User, "Continue")
}

func TestProjectEpochWithNoTailIncludesSummaryOnly(t *testing.T) {
	t.Parallel()

	batch := session.ReplayBatch{
		Messages: []session.Message{
			message("old", session.RoleUser),
			message("summary", session.RoleSystem),
		},
		Parts: []session.Part{
			part("old-secret", "old", session.PartText, 10, `{"text":"SECRET old raw prompt"}`),
			part("summary", "summary", session.PartCompaction, 10, `{"text":"Summarized safely."}`),
		},
	}
	projected, err := Project(batch, Options{Epoch: &session.ContextEpoch{
		SummaryMessageID: "summary",
		SummarizedToID:   "old",
	}})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	if len(projected) != 1 {
		t.Fatalf("projected len = %d, want 1", len(projected))
	}
	assertMessage(t, projected[0], schema.System, "Summarized safely.")
	if strings.Contains(projected[0].Content, "SECRET old raw prompt") {
		t.Fatal("projected compacted raw prompt")
	}
}

func TestProjectEpochPlacesSummaryBeforeRetainedTail(t *testing.T) {
	t.Parallel()

	batch := session.ReplayBatch{
		Messages: []session.Message{
			message("old", session.RoleUser),
			message("tail", session.RoleUser),
			message("summary", session.RoleSystem),
		},
		Parts: []session.Part{
			part("old-secret", "old", session.PartText, 10, `{"text":"SECRET old raw prompt"}`),
			part("tail", "tail", session.PartText, 10, `{"text":"Continue"}`),
			part("summary", "summary", session.PartCompaction, 10, `{"text":"Summarized safely."}`),
		},
	}
	projected, err := Project(batch, Options{Epoch: &session.ContextEpoch{
		SummaryMessageID: "summary",
		SummarizedToID:   "old",
		TailStartID:      "tail",
	}})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	if len(projected) != 2 {
		t.Fatalf("projected len = %d, want 2", len(projected))
	}
	assertMessage(t, projected[0], schema.System, "Summarized safely.")
	assertMessage(t, projected[1], schema.User, "Continue")
}

func TestLoadIgnoresLiveOnlyEvents(t *testing.T) {
	t.Parallel()

	store := historyStore{
		batch: session.ReplayBatch{
			Messages: []session.Message{message("assistant", session.RoleAssistant)},
			Parts: []session.Part{
				part("settled", "assistant", session.PartText, 10, `{"text":"settled"}`),
			},
		},
		events: []session.EventRecord{{
			ID:        "live",
			SessionID: "session-1",
			Kind:      "message_delta",
			Payload:   json.RawMessage(`{"text":"LIVE_ONLY_SECRET"}`),
			LiveOnly:  true,
		}},
	}
	projected, err := Load(t.Context(), store, "session-1", Options{})
	if err != nil {
		t.Fatalf("Load error = %v", err)
	}
	assertMessage(t, projected[0], schema.Assistant, "settled")
}

func TestProjectRejectsMalformedIncludedPayload(t *testing.T) {
	t.Parallel()

	_, err := Project(session.ReplayBatch{
		Messages: []session.Message{message("assistant-1", session.RoleAssistant)},
		Parts: []session.Part{
			part("bad", "assistant-1", session.PartText, 10, `{`),
		},
	}, Options{})
	if err == nil {
		t.Fatal("Project error = nil, want malformed payload error")
	}
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

type goldenMessage struct {
	Role       string           `json:"role"`
	Content    string           `json:"content,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	ToolCalls  []goldenToolCall `json:"tool_calls,omitempty"`
}

type goldenToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

func goldenMessages(messages []*schema.Message) []goldenMessage {
	result := make([]goldenMessage, 0, len(messages))
	for _, message := range messages {
		item := goldenMessage{
			Role:       string(message.Role),
			Content:    message.Content,
			ToolCallID: message.ToolCallID,
		}
		for _, call := range message.ToolCalls {
			item.ToolCalls = append(item.ToolCalls, goldenToolCall{
				ID:        call.ID,
				Name:      call.Function.Name,
				Arguments: call.Function.Arguments,
			})
		}
		result = append(result, item)
	}
	return result
}

func readHistoryGolden(t *testing.T, path string) []goldenMessage {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read history golden: %v", err)
	}
	var result []goldenMessage
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("decode history golden: %v", err)
	}
	return result
}

type historyStore struct {
	session.Store
	batch  session.ReplayBatch
	events []session.EventRecord
}

func (s historyStore) ListMessages(context.Context, session.ID, session.ReplayCursor) (session.ReplayBatch, error) {
	return s.batch, nil
}

func (s historyStore) ListEvents(context.Context, session.ID, session.EventCursor) (session.EventBatch, error) {
	return session.EventBatch{Events: s.events}, nil
}
