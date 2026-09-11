package history

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

func agenticSequentialPartIDs(prefix string) func() session.PartID {
	n := 0
	return func() session.PartID {
		n++
		return session.PartID(fmt.Sprintf("%s-%d", prefix, n))
	}
}

func encodeRichParts(t *testing.T, content session.Content, messageID session.MessageID, idSeed string) []session.Part {
	t.Helper()
	parts, err := session.EncodeContentParts(content, agenticSequentialPartIDs(idSeed), messageID, "session-1", "run-1", time.Unix(1, 0), session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("EncodeContentParts: %v", err)
	}
	return parts
}

func providerStatePart(id session.PartID, messageID session.MessageID, ordinal int64, sentinel string) session.Part {
	return session.Part{
		ID:        id,
		MessageID: messageID,
		SessionID: "session-1",
		RunID:     "run-1",
		Kind:      session.PartProviderState,
		Ordinal:   ordinal,
		Payload:   json.RawMessage(fmt.Sprintf(`{"sentinel":%q}`, sentinel)),
		CreatedAt: time.Unix(1, 0),
		UpdatedAt: time.Unix(1, 0),
	}
}

// richAgenticFixture builds one batch spanning every content family exercised
// by ProjectAgentic: a rich user message (text + image), a rich assistant
// message (reasoning + text + function_tool_call + response_meta, plus a
// provider_state part carrying a sentinel that must never surface), a rich
// user message that already carries a function_tool_result block, and a
// durable RoleTool message carrying a rich function_tool_result block (which
// must decode as though it were RoleUser).
func richAgenticFixture(t *testing.T) (session.ReplayBatch, map[BlockRef]session.PartID) {
	t.Helper()

	userContent := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{
			{ID: "blk-utext", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "look at this"}},
			{ID: "blk-uimage", Kind: session.BlockKindUserInputImage, Media: &session.MediaBlock{URL: "https://example.com/x.png", MIMEType: "image/png"}},
		},
	}
	userParts := encodeRichParts(t, userContent, "user-rich", "up")

	assistantContent := session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{
			{ID: "blk-reasoning", Kind: session.BlockKindReasoning, Reasoning: &session.ReasoningBlock{Text: "thinking it through", Summary: []string{"step one"}}},
			{ID: "blk-text", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "here is the answer"}},
			{ID: "blk-call", Kind: session.BlockKindFunctionToolCall, FunctionCall: &session.FunctionCallBlock{CallID: "call-1", Name: "lookup", Arguments: `{"q":"x"}`}},
		},
		Meta: &session.ResponseMeta{Usage: &session.Usage{InputTokens: 5, OutputTokens: 7}},
	}
	assistantParts := encodeRichParts(t, assistantContent, "assistant-rich", "ap")
	assistantParts = append(assistantParts, providerStatePart("prov-1", "assistant-rich", 99, "PROVIDER_STATE_SENTINEL_XYZ"))

	resultContent := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{
			{ID: "blk-result", Kind: session.BlockKindFunctionToolResult, FunctionResult: &session.FunctionResultBlock{
				CallID: "call-1", Name: "lookup", Content: []session.ResultContent{{Type: session.ResultContentText, Text: "42"}},
			}},
		},
	}
	resultParts := encodeRichParts(t, resultContent, "user-toolresult-rich", "rp")

	toolRoleContent := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{
			{ID: "blk-tool-result", Kind: session.BlockKindFunctionToolResult, FunctionResult: &session.FunctionResultBlock{
				CallID: "call-2", Name: "search", Content: []session.ResultContent{{Type: session.ResultContentText, Text: "7 results"}},
			}},
		},
	}
	toolRoleParts := encodeRichParts(t, toolRoleContent, "tool-rich", "tp")

	wantPartIDs := map[BlockRef]session.PartID{
		{MessageID: "user-rich", BlockID: "blk-utext"}:             userParts[0].ID,
		{MessageID: "user-rich", BlockID: "blk-uimage"}:            userParts[1].ID,
		{MessageID: "assistant-rich", BlockID: "blk-reasoning"}:    assistantParts[0].ID,
		{MessageID: "assistant-rich", BlockID: "blk-text"}:         assistantParts[1].ID,
		{MessageID: "assistant-rich", BlockID: "blk-call"}:         assistantParts[2].ID,
		{MessageID: "user-toolresult-rich", BlockID: "blk-result"}: resultParts[0].ID,
		{MessageID: "tool-rich", BlockID: "blk-tool-result"}:       toolRoleParts[0].ID,
	}

	batch := session.ReplayBatch{
		Messages: []session.Message{
			message("user-rich", session.RoleUser),
			message("assistant-rich", session.RoleAssistant),
			message("user-toolresult-rich", session.RoleUser),
			message("tool-rich", session.RoleTool),
		},
		Parts: append(append(append(append([]session.Part{}, userParts...), assistantParts...), resultParts...), toolRoleParts...),
	}
	return batch, wantPartIDs
}

func TestProjectAgenticRichRoundTrip(t *testing.T) {
	t.Parallel()

	batch, wantPartIDs := richAgenticFixture(t)
	projection, err := ProjectAgentic(batch, Options{IncludeReasoning: true})
	if err != nil {
		t.Fatalf("ProjectAgentic error = %v", err)
	}
	if len(projection.Messages) != 4 {
		t.Fatalf("projected messages = %d, want 4", len(projection.Messages))
	}
	wantSources := []session.MessageID{"user-rich", "assistant-rich", "user-toolresult-rich", "tool-rich"}
	for i, want := range wantSources {
		if projection.SourceMessageIDs[i] != want {
			t.Fatalf("source[%d] = %q, want %q", i, projection.SourceMessageIDs[i], want)
		}
	}

	userMsg := projection.Messages[0]
	if userMsg.Role != einoschema.AgenticRoleTypeUser || len(userMsg.ContentBlocks) != 2 {
		t.Fatalf("user message = %#v", userMsg)
	}
	if userMsg.ContentBlocks[0].Type != einoschema.ContentBlockTypeUserInputText || userMsg.ContentBlocks[0].UserInputText.Text != "look at this" {
		t.Fatalf("user text block = %#v", userMsg.ContentBlocks[0])
	}
	if userMsg.ContentBlocks[1].Type != einoschema.ContentBlockTypeUserInputImage || userMsg.ContentBlocks[1].UserInputImage.URL != "https://example.com/x.png" {
		t.Fatalf("user image block = %#v", userMsg.ContentBlocks[1])
	}

	assistantMsg := projection.Messages[1]
	if assistantMsg.Role != einoschema.AgenticRoleTypeAssistant || len(assistantMsg.ContentBlocks) != 3 {
		t.Fatalf("assistant message = %#v", assistantMsg)
	}
	if assistantMsg.ContentBlocks[0].Type != einoschema.ContentBlockTypeReasoning || assistantMsg.ContentBlocks[0].Reasoning.Text != "thinking it through" {
		t.Fatalf("assistant reasoning block = %#v", assistantMsg.ContentBlocks[0])
	}
	if assistantMsg.ContentBlocks[1].Type != einoschema.ContentBlockTypeAssistantGenText || assistantMsg.ContentBlocks[1].AssistantGenText.Text != "here is the answer" {
		t.Fatalf("assistant text block = %#v", assistantMsg.ContentBlocks[1])
	}
	call := assistantMsg.ContentBlocks[2]
	if call.Type != einoschema.ContentBlockTypeFunctionToolCall || call.FunctionToolCall.CallID != "call-1" || call.FunctionToolCall.Name != "lookup" || call.FunctionToolCall.Arguments != `{"q":"x"}` {
		t.Fatalf("assistant function call block = %#v", call)
	}
	if assistantMsg.ResponseMeta == nil || assistantMsg.ResponseMeta.TokenUsage == nil || assistantMsg.ResponseMeta.TokenUsage.PromptTokens != 5 || assistantMsg.ResponseMeta.TokenUsage.CompletionTokens != 7 {
		t.Fatalf("assistant response meta = %#v", assistantMsg.ResponseMeta)
	}

	resultMsg := projection.Messages[2]
	if resultMsg.Role != einoschema.AgenticRoleTypeUser || len(resultMsg.ContentBlocks) != 1 {
		t.Fatalf("result message = %#v", resultMsg)
	}
	fr := resultMsg.ContentBlocks[0]
	if fr.Type != einoschema.ContentBlockTypeFunctionToolResult || fr.FunctionToolResult.CallID != "call-1" || fr.FunctionToolResult.Content[0].Text.Text != "42" {
		t.Fatalf("function tool result block = %#v", fr)
	}

	toolRoleMsg := projection.Messages[3]
	if toolRoleMsg.Role != einoschema.AgenticRoleTypeUser {
		t.Fatalf("RoleTool message projected role = %q, want user", toolRoleMsg.Role)
	}
	if len(toolRoleMsg.ContentBlocks) != 1 || toolRoleMsg.ContentBlocks[0].Type != einoschema.ContentBlockTypeFunctionToolResult {
		t.Fatalf("RoleTool message blocks = %#v", toolRoleMsg.ContentBlocks)
	}
	if toolRoleMsg.ContentBlocks[0].FunctionToolResult.CallID != "call-2" {
		t.Fatalf("RoleTool function result call id = %q", toolRoleMsg.ContentBlocks[0].FunctionToolResult.CallID)
	}

	for ref, wantPartID := range wantPartIDs {
		if got := projection.PartIDs[ref]; got != wantPartID {
			t.Fatalf("PartIDs[%+v] = %q, want %q", ref, got, wantPartID)
		}
	}
}

// TestProjectAgenticPartIDsDistinctAcrossMessages pins the fix for I2: two
// different messages using the same block ID must keep distinct PartIDs
// entries, keyed by (MessageID, BlockID), instead of one silently
// overwriting the other in a map keyed by bare BlockID.
func TestProjectAgenticPartIDsDistinctAcrossMessages(t *testing.T) {
	t.Parallel()

	firstContent := session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "first"}}},
	}
	secondContent := session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "second"}}},
	}
	firstParts := encodeRichParts(t, firstContent, "msg-1", "m1")
	secondParts := encodeRichParts(t, secondContent, "msg-2", "m2")

	batch := session.ReplayBatch{
		Messages: []session.Message{message("msg-1", session.RoleAssistant), message("msg-2", session.RoleAssistant)},
		Parts:    append(append([]session.Part{}, firstParts...), secondParts...),
	}
	projection, err := ProjectAgentic(batch, Options{})
	if err != nil {
		t.Fatalf("ProjectAgentic: %v", err)
	}
	if len(projection.PartIDs) != 2 {
		t.Fatalf("PartIDs = %+v, want 2 distinct entries", projection.PartIDs)
	}
	if got := projection.PartIDs[BlockRef{MessageID: "msg-1", BlockID: "b1"}]; got != firstParts[0].ID {
		t.Fatalf("PartIDs[msg-1/b1] = %q, want %q", got, firstParts[0].ID)
	}
	if got := projection.PartIDs[BlockRef{MessageID: "msg-2", BlockID: "b1"}]; got != secondParts[0].ID {
		t.Fatalf("PartIDs[msg-2/b1] = %q, want %q", got, secondParts[0].ID)
	}
}

func TestProjectAgenticReasoningOption(t *testing.T) {
	t.Parallel()

	batch, _ := richAgenticFixture(t)
	projection, err := ProjectAgentic(batch, Options{IncludeReasoning: false})
	if err != nil {
		t.Fatalf("ProjectAgentic error = %v", err)
	}
	assistantMsg := projection.Messages[1]
	if len(assistantMsg.ContentBlocks) != 2 {
		t.Fatalf("assistant blocks with reasoning excluded = %#v", assistantMsg.ContentBlocks)
	}
	for _, block := range assistantMsg.ContentBlocks {
		if block.Type == einoschema.ContentBlockTypeReasoning {
			t.Fatalf("reasoning block leaked despite IncludeReasoning=false: %#v", block)
		}
	}
}

func TestProjectAgenticProviderStateNeverDecoded(t *testing.T) {
	t.Parallel()

	batch, _ := richAgenticFixture(t)
	for _, includeReasoning := range []bool{true, false} {
		projection, err := ProjectAgentic(batch, Options{IncludeReasoning: includeReasoning})
		if err != nil {
			t.Fatalf("ProjectAgentic error = %v", err)
		}
		raw, err := json.Marshal(projection.Messages)
		if err != nil {
			t.Fatalf("marshal projection: %v", err)
		}
		if bytes.Contains(raw, []byte("PROVIDER_STATE_SENTINEL_XYZ")) {
			t.Fatalf("provider_state sentinel leaked into projected agentic messages: %s", raw)
		}
	}
}

func TestProjectAgenticMixedKindsRejected(t *testing.T) {
	t.Parallel()

	richContent := session.Content{
		Role:   session.RoleUser,
		Blocks: []session.ContentBlock{{ID: "blk-1", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "rich"}}},
	}
	richPart := encodeRichParts(t, richContent, "mixed", "mp")[0]

	batch := session.ReplayBatch{
		Messages: []session.Message{message("mixed", session.RoleUser)},
		Parts: []session.Part{
			part("legacy-compaction", "mixed", session.PartCompaction, 0, `{"text":"legacy","epoch_id":"epoch","redacted":true}`),
			richPart,
		},
	}
	_, err := ProjectAgentic(batch, Options{})
	if !errors.Is(err, ErrMixedContentKinds) {
		t.Fatalf("ProjectAgentic error = %v, want ErrMixedContentKinds", err)
	}
}

func TestProjectAgenticAppliesEpoch(t *testing.T) {
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
	projection, err := ProjectAgentic(batch, Options{Epoch: &session.ContextEpoch{
		SummaryMessageID: "summary",
		SummarizedToID:   "old",
		TailStartID:      "tail",
	}})
	if err != nil {
		t.Fatalf("ProjectAgentic error = %v", err)
	}
	if len(projection.Messages) != 2 {
		t.Fatalf("projected messages = %d, want 2", len(projection.Messages))
	}
	summary := projection.Messages[0]
	if summary.Role != einoschema.AgenticRoleTypeSystem || len(summary.ContentBlocks) != 1 || summary.ContentBlocks[0].UserInputText.Text != "Summarized safely." {
		t.Fatalf("summary message = %#v", summary)
	}
	tail := projection.Messages[1]
	if tail.Role != einoschema.AgenticRoleTypeUser || len(tail.ContentBlocks) != 1 || tail.ContentBlocks[0].UserInputText.Text != "Continue" {
		t.Fatalf("tail message = %#v", tail)
	}
	raw, err := json.Marshal(projection.Messages)
	if err != nil {
		t.Fatalf("marshal projection: %v", err)
	}
	if bytes.Contains(raw, []byte("SECRET old raw prompt")) {
		t.Fatalf("projected compacted raw prompt: %s", raw)
	}
}

// TestProjectAgenticClassicParity asserts that classic (Project) and agentic
// (ProjectAgentic) projection agree on the same durable block-kind content:
// a rich user text message, and a rich assistant message combining text,
// a function_tool_call, and its function_tool_result.
func TestProjectAgenticClassicParity(t *testing.T) {
	t.Parallel()

	userParts := encodeRichParts(t, session.Content{
		Role:   session.RoleUser,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "Read README"}}},
	}, "user-1", "u")
	assistantParts := encodeRichParts(t, session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{
			{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "I will read it."}},
			{ID: "b2", Kind: session.BlockKindFunctionToolCall, FunctionCall: &session.FunctionCallBlock{CallID: "call-1", Name: "file_read", Arguments: `{"path":"README.md"}`}},
		},
	}, "assistant-1", "a")
	toolParts := encodeRichParts(t, session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindFunctionToolResult, FunctionResult: &session.FunctionResultBlock{
			CallID: "call-1", Name: "file_read",
			Content: []session.ResultContent{{Type: session.ResultContentText, Text: "README contents"}},
		}}},
	}, "tool-1", "tr")

	batch := session.ReplayBatch{
		Messages: []session.Message{
			message("user-1", session.RoleUser),
			message("assistant-1", session.RoleAssistant),
			message("tool-1", session.RoleUser),
		},
		Parts: append(append(userParts, assistantParts...), toolParts...),
	}
	classic, err := Project(batch, Options{})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	agentic, err := ProjectAgentic(batch, Options{})
	if err != nil {
		t.Fatalf("ProjectAgentic error = %v", err)
	}
	if len(classic) != len(agentic.Messages) {
		t.Fatalf("classic len = %d, agentic len = %d", len(classic), len(agentic.Messages))
	}

	// user-1: classic Content == agentic user_input_text.
	if classic[0].Content != agentic.Messages[0].ContentBlocks[0].UserInputText.Text {
		t.Fatalf("user text mismatch: classic=%q agentic=%q", classic[0].Content, agentic.Messages[0].ContentBlocks[0].UserInputText.Text)
	}

	// assistant-1: classic Content/ToolCalls == agentic assistant_gen_text/function_tool_call.
	assistantAgentic := agentic.Messages[1]
	if assistantAgentic.ContentBlocks[0].AssistantGenText.Text != classic[1].Content {
		t.Fatalf("assistant text mismatch: classic=%q agentic=%q", classic[1].Content, assistantAgentic.ContentBlocks[0].AssistantGenText.Text)
	}
	if len(classic[1].ToolCalls) != 1 {
		t.Fatalf("classic tool calls = %#v", classic[1].ToolCalls)
	}
	call := assistantAgentic.ContentBlocks[1].FunctionToolCall
	if call.CallID != classic[1].ToolCalls[0].ID || call.Name != classic[1].ToolCalls[0].Function.Name || call.Arguments != classic[1].ToolCalls[0].Function.Arguments {
		t.Fatalf("tool call mismatch: classic=%#v agentic=%#v", classic[1].ToolCalls[0], call)
	}

	// The tool result message: classic schema.Tool message content == agentic function_tool_result text.
	if classic[2].Content != agentic.Messages[2].ContentBlocks[0].FunctionToolResult.Content[0].Text.Text {
		t.Fatalf("tool result mismatch: classic=%q agentic=%q", classic[2].Content, agentic.Messages[2].ContentBlocks[0].FunctionToolResult.Content[0].Text.Text)
	}
	if classic[2].ToolCallID != agentic.Messages[2].ContentBlocks[0].FunctionToolResult.CallID {
		t.Fatalf("tool call id mismatch: classic=%q agentic=%q", classic[2].ToolCallID, agentic.Messages[2].ContentBlocks[0].FunctionToolResult.CallID)
	}
}
