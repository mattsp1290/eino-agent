package history

import (
	"context"
	"encoding/json"
	"errors"
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

	userParts := encodeRichParts(t, session.Content{
		Role:   session.RoleUser,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "Read README"}}},
	}, "user-1", "u")
	assistant1Parts := encodeRichParts(t, session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{
			{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "I will read it."}},
			{ID: "b2", Kind: session.BlockKindFunctionToolCall, FunctionCall: &session.FunctionCallBlock{CallID: "call-1", Name: "file_read", Arguments: `{"path":"README.md"}`}},
		},
	}, "assistant-1", "a1")
	toolParts := encodeRichParts(t, session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindFunctionToolResult, FunctionResult: &session.FunctionResultBlock{
			CallID: "call-1", Name: "file_read",
			Content: []session.ResultContent{{Type: session.ResultContentText, Text: "README contents"}},
		}}},
	}, "tool-1", "t1")
	assistant2Parts := encodeRichParts(t, session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{
			{ID: "b1", Kind: session.BlockKindReasoning, Reasoning: &session.ReasoningBlock{Text: "LIVE_ONLY_STYLE_REASONING"}},
			{ID: "b2", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "Summary"}},
		},
	}, "assistant-2", "a2")
	liveParts := encodeRichParts(t, session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "settled"}}},
	}, "assistant-live", "al")

	batch := session.ReplayBatch{
		Messages: []session.Message{
			message("user-1", session.RoleUser),
			message("assistant-1", session.RoleAssistant),
			message("tool-1", session.RoleUser),
			message("assistant-2", session.RoleAssistant),
			message("assistant-live", session.RoleAssistant),
		},
		Parts: append(append(append(append(userParts, assistant1Parts...), toolParts...), assistant2Parts...), liveParts...),
	}
	projected, err := Project(batch, Options{})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	got := goldenMessages(projected)
	want := readHistoryGolden(t, "../../testdata/history/replay_projection.json")
	requireGoldenEqual(t, got, want)
}

func TestProjectCompactionBoundaryIncludesSummary(t *testing.T) {
	t.Parallel()

	textParts := encodeRichParts(t, session.Content{
		Role:   session.RoleSystem,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: " Tail instruction."}}},
	}, "summary", "s")
	batch := session.ReplayBatch{
		Messages: []session.Message{message("summary", session.RoleSystem)},
		Parts: append([]session.Part{
			part("compaction", "summary", session.PartCompaction, 10, `{"text":"Earlier context summary.","epoch_id":"epoch","redacted":true}`),
		}, reorderedOrdinal(textParts, 20)...),
	}
	projected, err := Project(batch, Options{})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	assertMessage(t, projected[0], schema.System, "Earlier context summary. Tail instruction.")
}

// reorderedOrdinal returns parts with every ordinal shifted to start at
// start, preserving relative order, so a caller can interleave rich content
// parts after a fixed-ordinal legacy-family part in the same message.
func reorderedOrdinal(parts []session.Part, start int64) []session.Part {
	out := make([]session.Part, len(parts))
	for i, p := range parts {
		p.Ordinal = start + int64(i)
		out[i] = p
	}
	return out
}

func TestProjectEpochExcludesCompactedRawHistory(t *testing.T) {
	t.Parallel()

	tailParts := encodeRichParts(t, session.Content{
		Role:   session.RoleUser,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "Continue"}}},
	}, "tail", "tl")
	batch := session.ReplayBatch{
		Messages: []session.Message{
			message("old", session.RoleUser),
			message("summary", session.RoleSystem),
			message("tail", session.RoleUser),
		},
		Parts: append([]session.Part{
			part("old-secret", "old", session.PartProviderState, 10, `{"text":"SECRET old raw prompt"}`),
			part("summary", "summary", session.PartCompaction, 10, `{"text":"Summarized safely.","epoch_id":"epoch","redacted":true}`),
		}, tailParts...),
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
			part("old-secret", "old", session.PartProviderState, 10, `{"text":"SECRET old raw prompt"}`),
			part("summary", "summary", session.PartCompaction, 10, `{"text":"Summarized safely.","epoch_id":"epoch","redacted":true}`),
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

	tailParts := encodeRichParts(t, session.Content{
		Role:   session.RoleUser,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "Continue"}}},
	}, "tail", "tl")
	batch := session.ReplayBatch{
		Messages: []session.Message{
			message("old", session.RoleUser),
			message("tail", session.RoleUser),
			message("summary", session.RoleSystem),
		},
		Parts: append(append([]session.Part{
			part("old-secret", "old", session.PartProviderState, 10, `{"text":"SECRET old raw prompt"}`),
		}, tailParts...),
			part("summary", "summary", session.PartCompaction, 10, `{"text":"Summarized safely.","epoch_id":"epoch","redacted":true}`),
		),
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
			Parts: encodeRichParts(t, session.Content{
				Role:   session.RoleAssistant,
				Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "settled"}}},
			}, "assistant", "st"),
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

func TestLoadBatchRejectsNonParallelPartOwnerMetadata(t *testing.T) {
	t.Parallel()
	parts := []session.Part{
		part("first", "assistant", session.PartProviderState, 0, `{"text":"one"}`),
		part("second", "assistant", session.PartProviderState, 1, `{"text":"two"}`),
	}
	for name, owners := range map[string][]session.MessageID{
		"partial":  {"assistant"},
		"overlong": {"assistant", "assistant", "assistant"},
	} {
		t.Run(name, func(t *testing.T) {
			store := historyStore{batch: session.ReplayBatch{
				Messages:            []session.Message{message("assistant", session.RoleAssistant)},
				Parts:               parts,
				PartOwnerMessageIDs: owners,
			}}
			if _, err := LoadBatch(t.Context(), store, "session-1"); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("LoadBatch error = %v, want ErrConflict", err)
			}
		})
	}
}

func TestProjectRejectsNonParallelPartOwnersWhenApplyingEpoch(t *testing.T) {
	t.Parallel()
	batch := session.ReplayBatch{
		Messages: []session.Message{message("assistant", session.RoleAssistant)},
		Parts: []session.Part{
			part("first", "assistant", session.PartProviderState, 0, `{"text":"one"}`),
			part("second", "assistant", session.PartProviderState, 1, `{"text":"two"}`),
		},
		PartOwnerMessageIDs: []session.MessageID{"assistant"},
	}
	_, err := Project(batch, Options{Epoch: &session.ContextEpoch{TailStartID: "assistant"}})
	if !errors.Is(err, session.ErrConflict) {
		t.Fatalf("Project error = %v, want ErrConflict", err)
	}
}

func TestProjectRejectsMalformedIncludedPayload(t *testing.T) {
	t.Parallel()

	_, err := Project(session.ReplayBatch{
		Messages: []session.Message{message("assistant-1", session.RoleAssistant)},
		Parts: []session.Part{
			part("bad", "assistant-1", session.PartAssistantGenText, 10, `{`),
		},
	}, Options{})
	if err == nil {
		t.Fatal("Project error = nil, want malformed payload error")
	}
}

func TestProjectWithSourcesOmitsProviderStateAndTracksExpansion(t *testing.T) {
	t.Parallel()
	// FunctionToolResult is only a valid content block on RoleUser (see
	// roleAllowedKinds in session/content.go), so it is encoded separately
	// from the RoleAssistant text block even though both parts end up on
	// the same durable "assistant" message below -- classic projection
	// (projectMessage's switch) does not require a part's kind to match its
	// owning message's declared Role for PartFunctionToolResult.
	textParts := encodeRichParts(t, session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "answer"}}},
	}, "assistant", "ex-text")
	toolParts := encodeRichParts(t, session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindFunctionToolResult, FunctionResult: &session.FunctionResultBlock{
			CallID: "call", Name: "lookup",
			Content: []session.ResultContent{{Type: session.ResultContentText, Text: "result"}},
		}}},
	}, "assistant", "ex-tool")
	batch := session.ReplayBatch{
		Messages: []session.Message{message("assistant", session.RoleAssistant)},
		Parts: append(append(reorderedOrdinal(textParts, 0),
			part("private", "assistant", session.PartProviderState, 1, `not even valid JSON SENTINEL`)),
			reorderedOrdinal(toolParts, 2)...,
		),
	}
	projection, err := ProjectWithSources(batch, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.Messages) != 2 || len(projection.SourceMessageIDs) != 2 || projection.SourceMessageIDs[0] != "assistant" || projection.SourceMessageIDs[1] != "assistant" {
		t.Fatalf("projection = %#v", projection)
	}
	for _, message := range projection.Messages {
		if len(message.Extra) != 0 || strings.Contains(message.Content, "SENTINEL") {
			t.Fatalf("provider state leaked: %#v", message)
		}
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

func requireGoldenEqual[T any](t *testing.T, got, want T) {
	t.Helper()
	if reflect.DeepEqual(got, want) {
		return
	}
	gotJSON, _ := json.MarshalIndent(got, "", "  ")
	wantJSON, _ := json.MarshalIndent(want, "", "  ")
	t.Fatalf("golden mismatch\n--- got ---\n%s\n--- want ---\n%s", gotJSON, wantJSON)
}

func TestProjectRichTextAndFunctionToolCall(t *testing.T) {
	t.Parallel()

	content := session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{
			{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "part one "}},
			{ID: "b2", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "part two"}},
			{ID: "b3", Kind: session.BlockKindFunctionToolCall, FunctionCall: &session.FunctionCallBlock{CallID: "call-1", Name: "lookup", Arguments: `{"q":"x"}`}},
		},
	}
	parts := encodeRichParts(t, content, "assistant-1", "rp")
	projected, err := Project(session.ReplayBatch{
		Messages: []session.Message{message("assistant-1", session.RoleAssistant)},
		Parts:    parts,
	}, Options{})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	if len(projected) != 1 {
		t.Fatalf("projected len = %d, want 1", len(projected))
	}
	if projected[0].Content != "part one part two" {
		t.Fatalf("content = %q", projected[0].Content)
	}
	if len(projected[0].ToolCalls) != 1 || projected[0].ToolCalls[0].ID != "call-1" || projected[0].ToolCalls[0].Function.Name != "lookup" || projected[0].ToolCalls[0].Function.Arguments != `{"q":"x"}` {
		t.Fatalf("tool calls = %#v", projected[0].ToolCalls)
	}
}

func TestProjectUserInputTextConcatenation(t *testing.T) {
	t.Parallel()

	content := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{
			{ID: "b1", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "hello "}},
			{ID: "b2", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "world"}},
		},
	}
	parts := encodeRichParts(t, content, "user-1", "up")
	projected, err := Project(session.ReplayBatch{
		Messages: []session.Message{message("user-1", session.RoleUser)},
		Parts:    parts,
	}, Options{})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	assertMessage(t, projected[0], schema.User, "hello world")
}

func TestProjectReasoningEnvelopeGoesToReasoningContent(t *testing.T) {
	t.Parallel()

	content := session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{
			{ID: "b1", Kind: session.BlockKindReasoning, Reasoning: &session.ReasoningBlock{Text: "thinking"}},
			{ID: "b2", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "answer"}},
		},
	}
	parts := encodeRichParts(t, content, "assistant-1", "rp")
	batch := session.ReplayBatch{
		Messages: []session.Message{message("assistant-1", session.RoleAssistant)},
		Parts:    parts,
	}

	projected, err := Project(batch, Options{})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	if projected[0].ReasoningContent != "" {
		t.Fatalf("reasoning content with defaults = %q, want empty", projected[0].ReasoningContent)
	}
	if projected[0].Content != "answer" {
		t.Fatalf("content with defaults = %q", projected[0].Content)
	}

	projected, err = Project(batch, Options{IncludeReasoning: true})
	if err != nil {
		t.Fatalf("Project with IncludeReasoning error = %v", err)
	}
	if projected[0].ReasoningContent != "thinking" {
		t.Fatalf("reasoning content = %q, want %q", projected[0].ReasoningContent, "thinking")
	}
	if projected[0].Content != "answer" {
		t.Fatalf("content = %q, want reasoning kept out of Content", projected[0].Content)
	}
}

func TestProjectLegacyReasoningStillAppendsToContent(t *testing.T) {
	t.Parallel()

	batch := session.ReplayBatch{
		Messages: []session.Message{message("assistant-1", session.RoleAssistant)},
		Parts: []session.Part{
			part("reasoning", "assistant-1", session.PartReasoning, 10, `{"text":"legacy reasoning"}`),
		},
	}
	projected, err := Project(batch, Options{IncludeReasoning: true})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	if projected[0].Content != "legacy reasoning" || projected[0].ReasoningContent != "" {
		t.Fatalf("legacy reasoning projection = %#v", projected[0])
	}
}

func TestProjectFunctionToolResultJoinsTextParts(t *testing.T) {
	t.Parallel()

	content := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{
			{ID: "b1", Kind: session.BlockKindFunctionToolResult, FunctionResult: &session.FunctionResultBlock{
				CallID: "call-1", Name: "lookup",
				Content: []session.ResultContent{{Type: session.ResultContentText, Text: "part one "}, {Type: session.ResultContentText, Text: "part two"}},
			}},
		},
	}
	parts := encodeRichParts(t, content, "tool-1", "tp")
	projected, err := Project(session.ReplayBatch{
		Messages: []session.Message{message("tool-1", session.RoleTool)},
		Parts:    parts,
	}, Options{})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	if len(projected) != 1 {
		t.Fatalf("projected len = %d, want 1", len(projected))
	}
	assertMessage(t, projected[0], schema.Tool, "part one part two")
	if projected[0].ToolCallID != "call-1" {
		t.Fatalf("tool call id = %q", projected[0].ToolCallID)
	}
}

func TestProjectFunctionToolResultNonTextRejected(t *testing.T) {
	t.Parallel()

	content := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{
			{ID: "b1", Kind: session.BlockKindFunctionToolResult, FunctionResult: &session.FunctionResultBlock{
				CallID: "call-1", Name: "lookup",
				Content: []session.ResultContent{{Type: session.ResultContentImage, Media: &session.MediaBlock{URL: "https://example.com/x.png"}}},
			}},
		},
	}
	parts := encodeRichParts(t, content, "tool-1", "tp")
	_, err := Project(session.ReplayBatch{
		Messages: []session.Message{message("tool-1", session.RoleTool)},
		Parts:    parts,
	}, Options{})
	if !errors.Is(err, ErrClassicUnsupported) {
		t.Fatalf("Project error = %v, want ErrClassicUnsupported", err)
	}
}

func TestProjectUnsupportedRichKindRejected(t *testing.T) {
	t.Parallel()

	content := session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{
			{ID: "b1", Kind: session.BlockKindServerToolCall, ServerCall: &session.ServerCallBlock{Name: "search"}},
		},
	}
	parts := encodeRichParts(t, content, "assistant-1", "rp")
	_, err := Project(session.ReplayBatch{
		Messages: []session.Message{message("assistant-1", session.RoleAssistant)},
		Parts:    parts,
	}, Options{})
	if !errors.Is(err, ErrClassicUnsupported) {
		t.Fatalf("Project error = %v, want ErrClassicUnsupported", err)
	}
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
