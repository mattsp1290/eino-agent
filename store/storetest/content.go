package storetest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/cloudwego/eino/schema/openai"

	"github.com/mattsp1290/eino-agent/session"
)

// contentContract exercises the durable ordered rich content contract
// (session.Content / session.ContentBlock, encoded with
// session.EncodeContentParts and decoded with session.DecodeContentParts)
// across a full store round trip: build an Eino *schema.AgenticMessage,
// convert it, append the resulting message and parts under a fenced
// ExecutionStore, replay through Store.ListMessages, and decode back into
// public Content.
func contentContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("durable rich content", func(t *testing.T) {
		t.Run("every block kind round trips through the store", func(t *testing.T) {
			testContentAllBlockKinds(t, factory)
		})
		t.Run("mixed message preserves ordinals kinds and response meta", func(t *testing.T) {
			testContentMixedMessage(t, factory)
		})
		t.Run("observation snapshot shows only text-kind parts", func(t *testing.T) {
			testContentObservationTextOnly(t, factory)
		})
		t.Run("provider state sentinel never leaks into public content", func(t *testing.T) {
			testContentProviderStateSentinel(t, factory)
		})
		t.Run("provider state part preceding a block part decodes successfully", func(t *testing.T) {
			testContentProviderStateInterleavedOrdinal(t, factory)
		})
	})
}

func contentMustJSON(t testing.TB, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}

func contentAssertJSONEqual(t testing.TB, got, want any) {
	t.Helper()
	g, w := contentMustJSON(t, got), contentMustJSON(t, want)
	if g != w {
		t.Fatalf("json mismatch:\n got=%s\nwant=%s", g, w)
	}
}

func contentSequentialBlockIDs(prefix string) func() string {
	n := 0
	return func() string {
		n++
		return fmt.Sprintf("%s-b%d", prefix, n)
	}
}

func contentSequentialPartIDs(prefix string) func() session.PartID {
	n := 0
	return func() session.PartID {
		n++
		return session.PartID(fmt.Sprintf("%s-p%d", prefix, n))
	}
}

func contentAgenticRole(role session.Role) einoschema.AgenticRoleType {
	switch role {
	case session.RoleSystem:
		return einoschema.AgenticRoleTypeSystem
	case session.RoleAssistant:
		return einoschema.AgenticRoleTypeAssistant
	default:
		return einoschema.AgenticRoleTypeUser
	}
}

func contentInt64Ptr(v int64) *int64 { return &v }

// appendContentMessage converts msg into durable Content, encodes it into
// Parts, and appends the message and every part under execution. It returns
// the converted Content for further assertions.
//
// function_tool_call and function_tool_result parts are not generically
// appendable (see store/internal/sqlstore/execution.go's AppendPart guard):
// the store requires them to arrive through the tool-call lifecycle
// (CreateToolCall / ClaimToolCall / SettleToolCall) it already owns, so this
// helper routes each such part through that lifecycle transparently to keep
// exercising ordinary content round-tripping for every block kind.
func appendContentMessage(t testing.TB, ctx context.Context, execution session.ExecutionStore, sessionID session.ID, runID session.RunID, id session.MessageID, role session.Role, msg *einoschema.AgenticMessage) session.Content {
	t.Helper()
	content, _, err := session.ContentFromAgenticMessage(msg, contentSequentialBlockIDs(string(id)))
	if err != nil {
		t.Fatalf("ContentFromAgenticMessage(%s): %v", id, err)
	}
	parts, err := session.EncodeContentParts(content, contentSequentialPartIDs(string(id)), id, sessionID, runID, time.Now().UTC(), session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("EncodeContentParts(%s): %v", id, err)
	}
	// A message whose sole block is a function_tool_result or
	// tool_search_result settles against a synthetic prerequisite
	// request/claim (see appendFunctionToolResultPart /
	// appendToolSearchResultPart) and creates its own result message as
	// part of that settlement, so the generic message append below is
	// skipped for those shapes to avoid double-appending (and mismatching)
	// the same message ID.
	onlyResultBlock := len(parts) == 1 && (parts[0].Kind == session.PartFunctionToolResult || parts[0].Kind == session.PartToolSearchResult)
	if !onlyResultBlock {
		appendMessage(t, ctx, execution, message(id, sessionID, runID, role))
	}
	for i, p := range parts {
		switch {
		case p.Kind == session.PartFunctionToolCall && i < len(content.Blocks):
			appendFunctionToolCallPart(t, ctx, execution, id, content.Blocks[i], p)
		case p.Kind == session.PartFunctionToolResult && i < len(content.Blocks):
			appendFunctionToolResultPart(t, ctx, execution, id, sessionID, runID, role, content.Blocks[i], p)
		case p.Kind == session.PartToolSearchResult && i < len(content.Blocks):
			appendToolSearchResultPart(t, ctx, execution, id, sessionID, runID, role, content.Blocks[i], p)
		default:
			appendPart(t, ctx, execution, p)
		}
	}
	return content
}

// appendFunctionToolCallPart persists a function_tool_call content-block
// part through CreateToolCall, matching how runtime persists an assistant
// message's tool calls.
func appendFunctionToolCallPart(t testing.TB, ctx context.Context, execution session.ExecutionStore, messageID session.MessageID, block session.ContentBlock, part session.Part) {
	t.Helper()
	if block.FunctionCall == nil {
		t.Fatalf("function_tool_call part %s: block missing FunctionCall payload", part.ID)
	}
	call := session.ToolCall{
		ID: session.ToolCallID(block.FunctionCall.CallID), SessionID: part.SessionID, RunID: part.RunID, MessageID: messageID,
		RequestPartID: part.ID, Name: block.FunctionCall.Name, Input: json.RawMessage(block.FunctionCall.Arguments), Status: session.ToolCallPending,
	}
	if _, err := execution.CreateToolCall(ctx, session.CreateToolCallRequest{
		Call: call, RequestPart: part, Event: session.ToolTransitionEvent{ID: session.EventID("event-create-" + string(part.ID)), CreatedAt: part.CreatedAt},
	}); err != nil {
		t.Fatalf("create tool call for part %s: %v", part.ID, err)
	}
}

// appendFunctionToolResultPart persists a function_tool_result content-block
// part through the full CreateToolCall/ClaimToolCall/SettleToolCall
// lifecycle. Content round-trip fixtures exercise this block kind in
// isolation (no preceding function_tool_call in the same fixture message),
// so a synthetic prerequisite request/claim is created first, under its own
// message: settlement requires an existing claimed call with a matching ID,
// and the result message (resultMessageID, matching the fixture's own
// message ID so the caller's part-ownership filter still finds it) is
// created by the settlement itself rather than pre-appended, since its
// fields (in particular ParentID) are only known once the synthetic call
// exists.
func appendFunctionToolResultPart(t testing.TB, ctx context.Context, execution session.ExecutionStore, resultMessageID session.MessageID, sessionID session.ID, runID session.RunID, role session.Role, block session.ContentBlock, part session.Part) {
	t.Helper()
	if block.FunctionResult == nil {
		t.Fatalf("function_tool_result part %s: block missing FunctionResult payload", part.ID)
	}
	callID := session.ToolCallID(block.FunctionResult.CallID)
	at := part.CreatedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	requestMessageID := session.MessageID(string(resultMessageID) + "-synthetic-request")
	appendMessage(t, ctx, execution, message(requestMessageID, sessionID, runID, session.RoleAssistant))
	requestPartID := session.PartID(string(part.ID) + "-synthetic-request")
	requestParts, err := session.EncodeContentParts(session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{{
			ID: "block-" + string(requestPartID), Kind: session.BlockKindFunctionToolCall,
			FunctionCall: &session.FunctionCallBlock{CallID: string(callID), Name: block.FunctionResult.Name, Arguments: "{}"},
		}},
	}, func() session.PartID { return requestPartID }, requestMessageID, sessionID, runID, at, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("encode synthetic tool request for part %s: %v", part.ID, err)
	}
	call := session.ToolCall{
		ID: callID, SessionID: sessionID, RunID: runID, MessageID: requestMessageID,
		RequestPartID: requestPartID, ResultMessageID: resultMessageID, ResultPartID: part.ID,
		Name: block.FunctionResult.Name, Input: json.RawMessage(`{}`), Status: session.ToolCallPending,
	}
	if _, err := execution.CreateToolCall(ctx, session.CreateToolCallRequest{
		Call: call, RequestPart: requestParts[0], Event: session.ToolTransitionEvent{ID: session.EventID("event-create-" + string(part.ID)), CreatedAt: at},
	}); err != nil {
		t.Fatalf("create synthetic tool call for result part %s: %v", part.ID, err)
	}
	claimed, err := execution.ClaimToolCall(ctx, session.ClaimToolCallRequest{
		ID: callID, ClaimedBy: "storetest", ClaimToken: "storetest-" + string(part.ID), StartedAt: at, LeaseDuration: time.Minute,
		Event: session.ToolTransitionEvent{ID: session.EventID("event-claim-" + string(part.ID)), CreatedAt: at},
	})
	if err != nil {
		t.Fatalf("claim synthetic tool call for result part %s: %v", part.ID, err)
	}
	// store/internal/sqlstore's validFunctionToolResultEnvelope checks the
	// durable content against a shape recorded in settlement.Output: the
	// classic scalar shape (a single text item) is checked by exact
	// equality against the recorded Output JSON itself; an enhanced
	// (multi-item, or single non-text item) result is checked structurally
	// against a minimal {"parts":[{"type":...}]} view agreeing in
	// count/type/order with the content. Build whichever shape this
	// fixture's content actually is.
	var settlementOutput json.RawMessage
	if len(block.FunctionResult.Content) == 1 && block.FunctionResult.Content[0].Type == session.ResultContentText {
		settlementOutput = json.RawMessage(block.FunctionResult.Content[0].Text)
	} else {
		type recordedPart struct {
			Type string `json:"type"`
		}
		parts := make([]recordedPart, len(block.FunctionResult.Content))
		for i, item := range block.FunctionResult.Content {
			parts[i] = recordedPart{Type: string(item.Type)}
		}
		raw, err := json.Marshal(struct {
			ToolCallID string         `json:"tool_call_id"`
			Status     string         `json:"status"`
			Parts      []recordedPart `json:"parts"`
		}{ToolCallID: string(callID), Status: "completed", Parts: parts})
		if err != nil {
			t.Fatalf("encode recorded output shape for result part %s: %v", part.ID, err)
		}
		settlementOutput = raw
	}
	settlement := session.ToolSettlement{
		ID: callID, ClaimedBy: claimed.Call.ClaimedBy, ClaimToken: claimed.Call.ClaimToken, Status: session.ToolCallCompleted,
		Output: settlementOutput, CompletedAt: at,
		ResultMessage: session.Message{ID: resultMessageID, SessionID: sessionID, RunID: runID, ParentID: requestMessageID, Role: role, CreatedAt: at, UpdatedAt: at},
		ResultPart:    part,
	}
	if _, err := execution.SettleToolCall(ctx, session.SettleToolCallRequest{
		Settlement: settlement, Event: session.ToolTransitionEvent{ID: session.EventID("event-settle-" + string(part.ID)), CreatedAt: at},
	}); err != nil {
		t.Fatalf("settle synthetic tool call for result part %s: %v", part.ID, err)
	}
}

// appendToolSearchResultPart persists a tool_search_result content-block
// part through the full CreateToolCall/ClaimToolCall/SettleToolCall
// lifecycle, mirroring appendFunctionToolResultPart above.
// session.PartToolSearchResult is a reserved kind on the fenced
// executionStore.AppendPart (its only legitimate writer is settleToolCall's
// reserved ResultPart, see store/internal/sqlstore/execution.go and
// runtime/tool_search.go's buildTerminalToolSearchEnvelope), so a content
// round-trip fixture exercising this block kind in isolation must go
// through the same lifecycle rather than a generic AppendPart.
func appendToolSearchResultPart(t testing.TB, ctx context.Context, execution session.ExecutionStore, resultMessageID session.MessageID, sessionID session.ID, runID session.RunID, role session.Role, block session.ContentBlock, part session.Part) {
	t.Helper()
	if block.ToolSearch == nil {
		t.Fatalf("tool_search_result part %s: block missing ToolSearch payload", part.ID)
	}
	callID := session.ToolCallID(block.ToolSearch.CallID)
	at := part.CreatedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	requestMessageID := session.MessageID(string(resultMessageID) + "-synthetic-request")
	appendMessage(t, ctx, execution, message(requestMessageID, sessionID, runID, session.RoleAssistant))
	requestPartID := session.PartID(string(part.ID) + "-synthetic-request")
	requestParts, err := session.EncodeContentParts(session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{{
			ID: "block-" + string(requestPartID), Kind: session.BlockKindFunctionToolCall,
			FunctionCall: &session.FunctionCallBlock{CallID: string(callID), Name: block.ToolSearch.Name, Arguments: "{}"},
		}},
	}, func() session.PartID { return requestPartID }, requestMessageID, sessionID, runID, at, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("encode synthetic tool request for part %s: %v", part.ID, err)
	}
	call := session.ToolCall{
		ID: callID, SessionID: sessionID, RunID: runID, MessageID: requestMessageID,
		RequestPartID: requestPartID, ResultMessageID: resultMessageID, ResultPartID: part.ID,
		// call.Name must equal block.ToolSearch.Name exactly:
		// validToolSearchResultEnvelope compares them directly (the search
		// tool is never aliased).
		Name: block.ToolSearch.Name, Input: json.RawMessage(`{}`), Status: session.ToolCallPending,
	}
	if _, err := execution.CreateToolCall(ctx, session.CreateToolCallRequest{
		Call: call, RequestPart: requestParts[0], Event: session.ToolTransitionEvent{ID: session.EventID("event-create-" + string(part.ID)), CreatedAt: at},
	}); err != nil {
		t.Fatalf("create synthetic tool call for result part %s: %v", part.ID, err)
	}
	claimed, err := execution.ClaimToolCall(ctx, session.ClaimToolCallRequest{
		ID: callID, ClaimedBy: "storetest", ClaimToken: "storetest-" + string(part.ID), StartedAt: at, LeaseDuration: time.Minute,
		Event: session.ToolTransitionEvent{ID: session.EventID("event-claim-" + string(part.ID)), CreatedAt: at},
	})
	if err != nil {
		t.Fatalf("claim synthetic tool call for result part %s: %v", part.ID, err)
	}
	// store/internal/sqlstore's validToolSearchResultEnvelope cross-checks
	// the durable tool_search_result block's discovered tool names against
	// settlement.Output's recorded_tool_search_output_shape
	// (discovered_tool_names), in order.
	infos, err := block.ToolSearch.ToolInfos()
	if err != nil {
		t.Fatalf("decode tool search block tool infos for part %s: %v", part.ID, err)
	}
	names := make([]string, len(infos))
	for i, info := range infos {
		if info != nil {
			names[i] = info.Name
		}
	}
	settlementOutput, err := json.Marshal(struct {
		DiscoveredToolNames []string `json:"discovered_tool_names"`
	}{DiscoveredToolNames: names})
	if err != nil {
		t.Fatalf("encode recorded tool search output shape for part %s: %v", part.ID, err)
	}
	settlement := session.ToolSettlement{
		ID: callID, ClaimedBy: claimed.Call.ClaimedBy, ClaimToken: claimed.Call.ClaimToken, Status: session.ToolCallCompleted,
		Output: settlementOutput, CompletedAt: at,
		ResultMessage: session.Message{ID: resultMessageID, SessionID: sessionID, RunID: runID, ParentID: requestMessageID, Role: role, CreatedAt: at, UpdatedAt: at},
		ResultPart:    part,
	}
	if _, err := execution.SettleToolCall(ctx, session.SettleToolCallRequest{
		Settlement: settlement, Event: session.ToolTransitionEvent{ID: session.EventID("event-settle-" + string(part.ID)), CreatedAt: at},
	}); err != nil {
		t.Fatalf("settle synthetic tool call for result part %s: %v", part.ID, err)
	}
}

// testContentAllBlockKinds round trips one message per BlockKind (plus all
// five nested FunctionToolResultContentBlock variants inside the
// function_tool_result case) through ContentFromAgenticMessage ->
// EncodeContentParts -> store append -> Store.ListMessages replay ->
// DecodeContentParts -> ContentToAgenticMessage.
func testContentAllBlockKinds(t *testing.T, factory Factory) {
	subject := setup(t, factory)
	ctx := context.Background()

	type testCase struct {
		name  string
		kind  session.BlockKind
		role  session.Role
		block *einoschema.ContentBlock
	}

	cases := []testCase{
		{
			name: "reasoning", kind: session.BlockKindReasoning, role: session.RoleAssistant,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeReasoning, Reasoning: &einoschema.Reasoning{Text: "thinking..."}},
		},
		{
			name: "user_input_text", kind: session.BlockKindUserInputText, role: session.RoleUser,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputText, UserInputText: &einoschema.UserInputText{Text: "hello there"}},
		},
		{
			name: "user_input_image", kind: session.BlockKindUserInputImage, role: session.RoleUser,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputImage, UserInputImage: &einoschema.UserInputImage{
				URL: "https://example.com/pic.png", MIMEType: "image/png", Detail: einoschema.ImageURLDetailHigh,
			}},
		},
		{
			name: "user_input_audio", kind: session.BlockKindUserInputAudio, role: session.RoleUser,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputAudio, UserInputAudio: &einoschema.UserInputAudio{
				Base64Data: base64.StdEncoding.EncodeToString([]byte("audio-bytes")), MIMEType: "audio/wav",
			}},
		},
		{
			name: "user_input_video", kind: session.BlockKindUserInputVideo, role: session.RoleUser,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputVideo, UserInputVideo: &einoschema.UserInputVideo{
				URL: "https://example.com/clip.mp4", MIMEType: "video/mp4",
			}},
		},
		{
			name: "user_input_file", kind: session.BlockKindUserInputFile, role: session.RoleUser,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputFile, UserInputFile: &einoschema.UserInputFile{
				URL: "https://example.com/doc.pdf", Name: "doc.pdf", MIMEType: "application/pdf",
			}},
		},
		{
			name: "tool_search_result", kind: session.BlockKindToolSearchResult, role: session.RoleUser,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeToolSearchResult, ToolSearchFunctionToolResult: &einoschema.ToolSearchFunctionToolResult{
				CallID: "search-1", Name: "tool_search",
				Result: &einoschema.ToolSearchResult{Tools: []*einoschema.ToolInfo{
					{Name: "no_params_tool", Desc: "takes nothing"},
					{Name: "with_params_tool", Desc: "takes params", ParamsOneOf: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{
						"x": {Type: einoschema.String, Desc: "an x"},
					})},
				}},
			}},
		},
		{
			name: "assistant_gen_text", kind: session.BlockKindAssistantGenText, role: session.RoleAssistant,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: "here is the answer"}},
		},
		{
			name: "assistant_gen_image", kind: session.BlockKindAssistantGenImage, role: session.RoleAssistant,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenImage, AssistantGenImage: &einoschema.AssistantGenImage{
				Base64Data: base64.StdEncoding.EncodeToString([]byte("img-bytes")), MIMEType: "image/png",
			}},
		},
		{
			name: "assistant_gen_audio", kind: session.BlockKindAssistantGenAudio, role: session.RoleAssistant,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenAudio, AssistantGenAudio: &einoschema.AssistantGenAudio{
				URL: "https://example.com/out.wav", MIMEType: "audio/wav",
			}},
		},
		{
			name: "assistant_gen_video", kind: session.BlockKindAssistantGenVideo, role: session.RoleAssistant,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenVideo, AssistantGenVideo: &einoschema.AssistantGenVideo{
				URL: "https://example.com/out.mp4", MIMEType: "video/mp4",
			}},
		},
		{
			name: "function_tool_call", kind: session.BlockKindFunctionToolCall, role: session.RoleAssistant,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &einoschema.FunctionToolCall{
				CallID: "call-1", Name: "get_weather", Arguments: `{"city":"nyc"}`,
			}},
		},
		{
			// All five nested FunctionToolResultContentBlock variants (text,
			// image, audio, video, file) in one function_tool_result, exactly
			// as session.TestContentBlockRoundTrip_AllKinds's own
			// "function_tool_result_all_variants" case exercises without a
			// store dependency. This fixture additionally routes through the
			// real CreateToolCall/ClaimToolCall/SettleToolCall lifecycle (see
			// appendFunctionToolResultPart) to prove the store round-trips
			// non-text result content end to end, including through the
			// tool_calls table and store/internal/sqlstore's
			// ValidToolResultEnvelope (which requires call-ID identity plus a
			// text-item concatenation equal to ToolSettlement.Output, and
			// otherwise allows any of the five variants alongside it).
			name: "function_tool_result_all_variants", kind: session.BlockKindFunctionToolResult, role: session.RoleUser,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeFunctionToolResult, FunctionToolResult: &einoschema.FunctionToolResult{
				// A distinct CallID from the "function_tool_call" fixture
				// above: both fixtures run against the same shared store
				// (see testContentAllBlockKinds) and function_tool_call/
				// function_tool_result parts are now routed through the
				// tool_calls table, whose ID is a store-wide primary key.
				CallID: "call-2", Name: "get_weather",
				Content: []*einoschema.FunctionToolResultContentBlock{
					{Type: einoschema.FunctionToolResultContentBlockTypeText, Text: &einoschema.UserInputText{Text: `{"forecast":"sunny"}`}},
					{Type: einoschema.FunctionToolResultContentBlockTypeImage, Image: &einoschema.UserInputImage{URL: "https://example.com/a.png", MIMEType: "image/png", Detail: einoschema.ImageURLDetailLow}},
					{Type: einoschema.FunctionToolResultContentBlockTypeAudio, Audio: &einoschema.UserInputAudio{URL: "https://example.com/a.wav", MIMEType: "audio/wav"}},
					{Type: einoschema.FunctionToolResultContentBlockTypeVideo, Video: &einoschema.UserInputVideo{URL: "https://example.com/a.mp4", MIMEType: "video/mp4"}},
					{Type: einoschema.FunctionToolResultContentBlockTypeFile, File: &einoschema.UserInputFile{URL: "https://example.com/a.pdf", Name: "a.pdf", MIMEType: "application/pdf"}},
				},
			}},
		},
		{
			name: "server_tool_call", kind: session.BlockKindServerToolCall, role: session.RoleAssistant,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeServerToolCall, ServerToolCall: &einoschema.ServerToolCall{
				Name: "web_search", CallID: "srv-1", Arguments: map[string]any{"query": "weather nyc", "count": float64(3)},
			}},
		},
		{
			name: "server_tool_result", kind: session.BlockKindServerToolResult, role: session.RoleAssistant,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeServerToolResult, ServerToolResult: &einoschema.ServerToolResult{
				Name: "web_search", CallID: "srv-1", Content: map[string]any{"results": []any{"a", "b"}},
			}},
		},
		{
			name: "mcp_tool_call", kind: session.BlockKindMCPToolCall, role: session.RoleAssistant,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPToolCall, MCPToolCall: &einoschema.MCPToolCall{
				ServerLabel: "my-mcp", ApprovalRequestID: "appr-1", CallID: "mcp-call-1", Name: "list_files", Arguments: `{"dir":"/tmp"}`,
			}},
		},
		{
			name: "mcp_tool_result", kind: session.BlockKindMCPToolResult, role: session.RoleAssistant,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPToolResult, MCPToolResult: &einoschema.MCPToolResult{
				ServerLabel: "my-mcp", CallID: "mcp-call-1", Name: "list_files", Content: `["a.txt"]`,
				Error: &einoschema.MCPToolCallError{Code: contentInt64Ptr(42), Message: "partial failure"},
			}},
		},
		{
			name: "mcp_list_tools_result", kind: session.BlockKindMCPListToolsResult, role: session.RoleAssistant,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPListToolsResult, MCPListToolsResult: &einoschema.MCPListToolsResult{
				ServerLabel: "my-mcp",
				Tools:       []*einoschema.MCPListToolsItem{{Name: "list_files", Description: "lists files"}},
			}},
		},
		{
			name: "mcp_tool_approval_request", kind: session.BlockKindMCPToolApprovalRequest, role: session.RoleAssistant,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPToolApprovalRequest, MCPToolApprovalRequest: &einoschema.MCPToolApprovalRequest{
				ID: "appr-1", Name: "delete_file", Arguments: `{"path":"/tmp/a"}`, ServerLabel: "my-mcp",
			}},
		},
		{
			name: "mcp_tool_approval_response", kind: session.BlockKindMCPToolApprovalResponse, role: session.RoleUser,
			block: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPToolApprovalResponse, MCPToolApprovalResponse: &einoschema.MCPToolApprovalResponse{
				ApprovalRequestID: "appr-1", Approve: true, Reason: "looks safe",
			}},
		},
	}

	if len(cases) != len(session.AllBlockKinds()) {
		t.Fatalf("fixture count = %d, want %d (one per BlockKind)", len(cases), len(session.AllBlockKinds()))
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sessionID := session.ID(fmt.Sprintf("content-kind-%d", i))
			runID := session.RunID(fmt.Sprintf("content-kind-run-%d", i))
			messageID := session.MessageID(fmt.Sprintf("content-kind-msg-%d", i))

			s := createSession(t, ctx, subject.Store, sessionID)
			r := admitRun(t, ctx, subject.Store, run(runID, s.ID, "owner"))
			execution := executionFor(subject.Store, r)

			msg := &einoschema.AgenticMessage{Role: contentAgenticRole(c.role), ContentBlocks: []*einoschema.ContentBlock{c.block}}
			content := appendContentMessage(t, ctx, execution, sessionID, runID, messageID, c.role, msg)
			if len(content.Blocks) != 1 || content.Blocks[0].Kind != c.kind {
				t.Fatalf("converted block kind = %+v, want %s", content.Blocks, c.kind)
			}

			batch, err := subject.Store.ListMessages(ctx, sessionID, session.ReplayCursor{Limit: 50})
			if err != nil {
				t.Fatalf("list messages: %v", err)
			}
			// function_tool_result and tool_search_result parts settle
			// against a synthetic prerequisite request (see
			// appendFunctionToolResultPart / appendToolSearchResultPart),
			// which adds its own message/part to the session; filter down
			// to the fixture's own message so the content-fidelity
			// comparison below still targets exactly the one block under
			// test.
			var ownParts []session.Part
			for _, part := range batch.Parts {
				if part.MessageID == messageID {
					ownParts = append(ownParts, part)
				}
			}
			if len(batch.Messages) < 1 || len(ownParts) != len(content.Blocks) {
				t.Fatalf("replay = %d messages, %d own parts; want >=1, %d", len(batch.Messages), len(ownParts), len(content.Blocks))
			}

			decoded, err := session.DecodeContentParts(c.role, ownParts, session.DefaultContentLimits())
			if err != nil {
				t.Fatalf("DecodeContentParts: %v", err)
			}
			contentAssertJSONEqual(t, decoded, content)

			wantMsg, err := session.ContentToAgenticMessage(content)
			if err != nil {
				t.Fatalf("ContentToAgenticMessage(original): %v", err)
			}
			gotMsg, err := session.ContentToAgenticMessage(decoded)
			if err != nil {
				t.Fatalf("ContentToAgenticMessage(decoded): %v", err)
			}
			contentAssertJSONEqual(t, gotMsg, wantMsg)
		})
	}
}

// testContentMixedMessage asserts that ordinals, part kinds, and the trailing
// response_meta part all survive a real store append/replay cycle for one
// message carrying reasoning, annotated assistant text, a function call, a
// server call, and an MCP approval request.
func testContentMixedMessage(t *testing.T, factory Factory) {
	subject := setup(t, factory)
	ctx := context.Background()
	sessionID := session.ID("content-mixed")
	runID := session.RunID("content-mixed-run")
	messageID := session.MessageID("msg-mixed")
	role := session.RoleAssistant

	s := createSession(t, ctx, subject.Store, sessionID)
	r := admitRun(t, ctx, subject.Store, run(runID, s.ID, "owner"))
	execution := executionFor(subject.Store, r)

	msg := &einoschema.AgenticMessage{
		Role: contentAgenticRole(role),
		ContentBlocks: []*einoschema.ContentBlock{
			{Type: einoschema.ContentBlockTypeReasoning, Reasoning: &einoschema.Reasoning{Text: "thinking it through"}},
			{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{
				Text: "the answer is 42",
				OpenAIExtension: &openai.AssistantGenTextExtension{
					Annotations: []*openai.TextAnnotation{
						{Type: openai.TextAnnotationTypeURLCitation, URLCitation: &openai.TextAnnotationURLCitation{
							Title: "source", URL: "https://example.com/source", StartIndex: 1, EndIndex: 5,
						}},
					},
				},
			}},
			{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &einoschema.FunctionToolCall{CallID: "c1", Name: "f", Arguments: `{}`}},
			{Type: einoschema.ContentBlockTypeServerToolCall, ServerToolCall: &einoschema.ServerToolCall{Name: "search", CallID: "s1"}},
			{Type: einoschema.ContentBlockTypeMCPToolApprovalRequest, MCPToolApprovalRequest: &einoschema.MCPToolApprovalRequest{ID: "a1", Name: "del"}},
		},
		ResponseMeta: &einoschema.AgenticResponseMeta{
			TokenUsage: &einoschema.TokenUsage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
		},
	}

	content := appendContentMessage(t, ctx, execution, sessionID, runID, messageID, role, msg)

	wantBlockKinds := []session.BlockKind{
		session.BlockKindReasoning, session.BlockKindAssistantGenText, session.BlockKindFunctionToolCall,
		session.BlockKindServerToolCall, session.BlockKindMCPToolApprovalRequest,
	}
	if len(content.Blocks) != len(wantBlockKinds) || content.Meta == nil {
		t.Fatalf("content = %#v, want %d blocks with response meta", content, len(wantBlockKinds))
	}
	for i, k := range wantBlockKinds {
		if content.Blocks[i].Kind != k {
			t.Fatalf("block[%d].Kind = %s, want %s", i, content.Blocks[i].Kind, k)
		}
	}

	wantPartKinds := []session.PartKind{
		session.PartReasoning, session.PartAssistantGenText, session.PartFunctionToolCall,
		session.PartServerToolCall, session.PartMCPToolApprovalRequest, session.PartResponseMeta,
	}

	batch, err := subject.Store.ListMessages(ctx, sessionID, session.ReplayCursor{Limit: 50})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(batch.Parts) != len(wantPartKinds) {
		t.Fatalf("replayed parts = %d, want %d", len(batch.Parts), len(wantPartKinds))
	}
	for i, k := range wantPartKinds {
		if batch.Parts[i].Kind != k || batch.Parts[i].Ordinal != int64(i) {
			t.Fatalf("replayed part[%d] = kind %s ordinal %d; want kind %s ordinal %d", i, batch.Parts[i].Kind, batch.Parts[i].Ordinal, k, i)
		}
	}

	decoded, err := session.DecodeContentParts(role, batch.Parts, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("DecodeContentParts: %v", err)
	}
	contentAssertJSONEqual(t, decoded, content)
	if decoded.Meta == nil || decoded.Meta.Usage == nil || decoded.Meta.Usage.InputTokens != 10 || decoded.Meta.Usage.OutputTokens != 5 {
		t.Fatalf("decoded response meta = %#v, want usage 10/5", decoded.Meta)
	}
	if decoded.Blocks[1].Text == nil || len(decoded.Blocks[1].Text.Annotations) != 1 || decoded.Blocks[1].Text.Annotations[0].URL != "https://example.com/source" {
		t.Fatalf("decoded annotations = %#v, want one url citation", decoded.Blocks[1].Text)
	}

	wantMsg, err := session.ContentToAgenticMessage(content)
	if err != nil {
		t.Fatalf("ContentToAgenticMessage(original): %v", err)
	}
	gotMsg, err := session.ContentToAgenticMessage(decoded)
	if err != nil {
		t.Fatalf("ContentToAgenticMessage(decoded): %v", err)
	}
	contentAssertJSONEqual(t, gotMsg, wantMsg)
}

// testContentObservationTextOnly asserts that ReadObservationSnapshot surfaces
// text only from PartUserInputText and PartAssistantGenText parts; reasoning,
// media, and function-call blocks in the very same messages must never
// contribute to observation text.
func testContentObservationTextOnly(t *testing.T, factory Factory) {
	subject := setup(t, factory)
	reader, ok := subject.Store.(session.ObservationReader)
	if !ok {
		t.Fatal("store must implement session.ObservationReader")
	}
	ctx := context.Background()
	sessionID := session.ID("content-observation")
	runID := session.RunID("content-observation-run")

	s := createSession(t, ctx, subject.Store, sessionID)
	r := admitRun(t, ctx, subject.Store, run(runID, s.ID, "owner"))
	execution := executionFor(subject.Store, r)

	// User message: a visible user_input_text block plus a non-text media
	// block that must never contribute to observation text.
	userMsg := &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeUser, ContentBlocks: []*einoschema.ContentBlock{
		{Type: einoschema.ContentBlockTypeUserInputText, UserInputText: &einoschema.UserInputText{Text: "please help"}},
		{Type: einoschema.ContentBlockTypeUserInputImage, UserInputImage: &einoschema.UserInputImage{URL: "https://example.com/x.png", MIMEType: "image/png"}},
	}}
	appendContentMessage(t, ctx, execution, sessionID, runID, "u1", session.RoleUser, userMsg)

	// Assistant message: a visible assistant_gen_text block plus reasoning and
	// function-call blocks that must never contribute to observation text.
	assistantMsg := &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ContentBlocks: []*einoschema.ContentBlock{
		{Type: einoschema.ContentBlockTypeReasoning, Reasoning: &einoschema.Reasoning{Text: "PRIVATE_REASONING_NOT_OBSERVABLE"}},
		{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: "sure thing"}},
		{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &einoschema.FunctionToolCall{CallID: "c1", Name: "f", Arguments: `{}`}},
	}}
	appendContentMessage(t, ctx, execution, sessionID, runID, "a1", session.RoleAssistant, assistantMsg)

	limits := session.ObservationLimits{MaxMessages: 10, MaxTools: 10, MaxParts: 10, MaxSnapshotBytes: 64000, MaxTextBytes: 1000}
	snapshot, err := reader.ReadObservationSnapshot(ctx, sessionID, limits)
	if err != nil {
		t.Fatalf("read observation snapshot: %v", err)
	}
	texts := make(map[session.MessageID]string, len(snapshot.Messages))
	for _, m := range snapshot.Messages {
		texts[m.ID] = m.Text
	}
	if texts["u1"] != "please help" {
		t.Fatalf("u1 observation text = %q, want %q", texts["u1"], "please help")
	}
	if texts["a1"] != "sure thing" {
		t.Fatalf("a1 observation text = %q, want %q", texts["a1"], "sure thing")
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if strings.Contains(string(raw), "PRIVATE_REASONING_NOT_OBSERVABLE") {
		t.Fatal("observation snapshot leaked reasoning text")
	}
}

// testContentProviderStateSentinel asserts that a sentinel string carried by
// a PartProviderState payload never appears in decoded public content, in the
// rebuilt *schema.AgenticMessage, or in an observation snapshot.
func testContentProviderStateSentinel(t *testing.T, factory Factory) {
	subject := setup(t, factory)
	ctx := context.Background()
	sessionID := session.ID("content-provider-state")
	runID := session.RunID("content-provider-state-run")
	messageID := session.MessageID("msg-provider-state")
	role := session.RoleAssistant

	const sentinel = "PROVIDER_STATE_SENTINEL_MUST_NEVER_LEAK"

	s := createSession(t, ctx, subject.Store, sessionID)
	r := admitRun(t, ctx, subject.Store, run(runID, s.ID, "owner"))
	execution := executionFor(subject.Store, r)

	msg := &einoschema.AgenticMessage{Role: contentAgenticRole(role), ContentBlocks: []*einoschema.ContentBlock{
		{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: "visible answer"}},
	}}
	content := appendContentMessage(t, ctx, execution, sessionID, runID, messageID, role, msg)

	sentinelData := json.RawMessage(fmt.Sprintf(`{"secret":%q}`, sentinel))
	statePayload, err := session.EncodeProviderStatePayload(session.ProviderStateEnvelope{
		// CompatibilityKey is stored verbatim (not base64-encoded), unlike
		// Data, so putting the sentinel there too makes the scans below
		// meaningful for a leak of either field.
		CodecID: "codec", Version: 1, ProviderID: "provider", SourceModelID: "model",
		CompatibilityKey: sentinel, ItemIndex: 0, BlockID: content.Blocks[0].ID,
		Data: sentinelData,
	})
	if err != nil {
		t.Fatalf("encode provider state: %v", err)
	}
	// EncodeProviderStatePayload stores Data as base64(Data), so a scan for
	// the literal sentinel string alone would pass even if the entire
	// envelope (including Data) leaked verbatim into decoded output. Scan
	// for both forms.
	encodedSentinelData := base64.StdEncoding.EncodeToString(sentinelData)
	leaked := func(t *testing.T, label, raw string) {
		t.Helper()
		if strings.Contains(raw, sentinel) {
			t.Fatalf("%s leaked provider state sentinel: %s", label, raw)
		}
		if strings.Contains(raw, encodedSentinelData) {
			t.Fatalf("%s leaked base64-encoded provider state data: %s", label, raw)
		}
	}
	appendPart(t, ctx, execution, session.Part{
		ID: "provider-state-1", MessageID: messageID, SessionID: sessionID, RunID: runID,
		Kind: session.PartProviderState, Ordinal: 1, Payload: statePayload,
	})

	batch, err := subject.Store.ListMessages(ctx, sessionID, session.ReplayCursor{Limit: 50})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	decoded, err := session.DecodeContentParts(role, batch.Parts, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("DecodeContentParts: %v", err)
	}
	if len(decoded.Blocks) != 1 {
		t.Fatalf("decoded blocks = %d, want 1 (provider_state must be ignored)", len(decoded.Blocks))
	}
	raw, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("marshal decoded content: %v", err)
	}
	leaked(t, "decoded content", string(raw))

	rebuilt, err := session.ContentToAgenticMessage(decoded)
	if err != nil {
		t.Fatalf("ContentToAgenticMessage: %v", err)
	}
	rebuiltRaw, err := json.Marshal(rebuilt)
	if err != nil {
		t.Fatalf("marshal rebuilt message: %v", err)
	}
	leaked(t, "rebuilt agentic message", string(rebuiltRaw))

	reader, ok := subject.Store.(session.ObservationReader)
	if !ok {
		t.Fatal("store must implement session.ObservationReader")
	}
	limits := session.ObservationLimits{MaxMessages: 10, MaxTools: 10, MaxParts: 10, MaxSnapshotBytes: 64000, MaxTextBytes: 1000}
	snapshot, err := reader.ReadObservationSnapshot(ctx, sessionID, limits)
	if err != nil {
		t.Fatalf("read observation snapshot: %v", err)
	}
	snapshotRaw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	leaked(t, "observation snapshot", string(snapshotRaw))
	for _, m := range snapshot.Messages {
		if m.ID == messageID && m.Text != "visible answer" {
			t.Fatalf("observation text = %q, want %q", m.Text, "visible answer")
		}
	}
}

// testContentProviderStateInterleavedOrdinal asserts that a provider_state
// part sharing an ordinal range with content-kind block parts (per
// DecodeContentParts' documented contract: block/response_meta ordinals must
// be strictly increasing among themselves but need not be contiguous, since
// other part kinds may occupy interleaved ordinals) decodes successfully
// even when the provider_state part's ordinal precedes the first block.
func testContentProviderStateInterleavedOrdinal(t *testing.T, factory Factory) {
	subject := setup(t, factory)
	ctx := context.Background()
	sessionID := session.ID("content-provider-state-ordinal")
	runID := session.RunID("content-provider-state-ordinal-run")
	messageID := session.MessageID("msg-provider-state-ordinal")
	role := session.RoleAssistant

	s := createSession(t, ctx, subject.Store, sessionID)
	r := admitRun(t, ctx, subject.Store, run(runID, s.ID, "owner"))
	execution := executionFor(subject.Store, r)

	msg := &einoschema.AgenticMessage{Role: contentAgenticRole(role), ContentBlocks: []*einoschema.ContentBlock{
		{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: "ordinal check"}},
	}}
	content, _, err := session.ContentFromAgenticMessage(msg, contentSequentialBlockIDs(string(messageID)))
	if err != nil {
		t.Fatalf("ContentFromAgenticMessage: %v", err)
	}
	blockParts, err := session.EncodeContentParts(content, contentSequentialPartIDs(string(messageID)), messageID, sessionID, runID, time.Now().UTC(), session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("EncodeContentParts: %v", err)
	}
	if len(blockParts) != 1 {
		t.Fatalf("EncodeContentParts produced %d parts, want 1", len(blockParts))
	}
	blockPart := blockParts[0]
	// Move the block part's ordinal past the provider_state part appended
	// below, so provider_state occupies the lower ordinal.
	blockPart.Ordinal = 1

	statePayload, err := session.EncodeProviderStatePayload(session.ProviderStateEnvelope{
		CodecID: "codec", Version: 1, ProviderID: "provider", SourceModelID: "model",
		CompatibilityKey: "compat", ItemIndex: 0, BlockID: content.Blocks[0].ID, Data: json.RawMessage(`{"k":"v"}`),
	})
	if err != nil {
		t.Fatalf("encode provider state: %v", err)
	}

	appendMessage(t, ctx, execution, message(messageID, sessionID, runID, role))
	// provider_state precedes the block part in ordinal order.
	appendPart(t, ctx, execution, session.Part{
		ID: "provider-state-1", MessageID: messageID, SessionID: sessionID, RunID: runID,
		Kind: session.PartProviderState, Ordinal: 0, Payload: statePayload,
	})
	appendPart(t, ctx, execution, blockPart)

	batch, err := subject.Store.ListMessages(ctx, sessionID, session.ReplayCursor{Limit: 50})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	decoded, err := session.DecodeContentParts(role, batch.Parts, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("DecodeContentParts with provider_state preceding block part: %v", err)
	}
	if len(decoded.Blocks) != 1 || decoded.Blocks[0].ID != content.Blocks[0].ID {
		t.Fatalf("decoded blocks = %+v, want one block with ID %q", decoded.Blocks, content.Blocks[0].ID)
	}
}
