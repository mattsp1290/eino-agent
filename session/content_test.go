package session

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/cloudwego/eino/schema/claude"
	"github.com/cloudwego/eino/schema/gemini"
	"github.com/cloudwego/eino/schema/openai"
)

func sequentialIDs(prefix string) func() string {
	n := 0
	return func() string {
		n++
		return prefix + string(rune('0'+n))
	}
}

func sequentialPartIDs(prefix string) func() PartID {
	n := 0
	return func() PartID {
		n++
		return PartID(prefix + string(rune('0'+n)))
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}

func assertJSONEqual(t *testing.T, got, want any) {
	t.Helper()
	g := mustJSON(t, got)
	w := mustJSON(t, want)
	if g != w {
		t.Fatalf("json mismatch:\n got=%s\nwant=%s", g, w)
	}
}

// roundTripMessage runs the full W2 pipeline: ContentFromAgenticMessage ->
// EncodeContentParts -> DecodeContentParts -> ContentToAgenticMessage. It
// returns the decoded Content, the encoded Parts (for byte-scanning), and the
// rebuilt public message.
func roundTripMessage(t *testing.T, role Role, msg *einoschema.AgenticMessage) (Content, []Part, *einoschema.AgenticMessage) {
	t.Helper()
	content, private, err := ContentFromAgenticMessage(msg, sequentialIDs("b"))
	if err != nil {
		t.Fatalf("ContentFromAgenticMessage: %v", err)
	}
	_ = private
	parts, err := EncodeContentParts(content, sequentialPartIDs("p"), MessageID("msg-1"), ID("sess-1"), RunID("run-1"), time.Unix(0, 0), DefaultContentLimits())
	if err != nil {
		t.Fatalf("EncodeContentParts: %v", err)
	}
	decoded, err := DecodeContentParts(role, parts, DefaultContentLimits())
	if err != nil {
		t.Fatalf("DecodeContentParts: %v", err)
	}
	rebuilt, err := ContentToAgenticMessage(decoded)
	if err != nil {
		t.Fatalf("ContentToAgenticMessage: %v", err)
	}
	return decoded, parts, rebuilt
}

// contentKindCase is one fixture in the shared 20-kind BlockKind table: an
// Eino content block that should round trip through
// ContentFromAgenticMessage -> EncodeContentParts -> DecodeContentParts ->
// ContentToAgenticMessage unchanged (or into expect, if non-nil).
type contentKindCase struct {
	name   string
	role   Role
	in     *einoschema.ContentBlock
	expect *einoschema.ContentBlock // nil means identical to in
}

// contentAllKindsCases returns one fixture per BlockKind (20 total). It is
// shared by TestContentBlockRoundTrip_AllKinds and
// TestContentValidateImpliesProjectable, which both need the same
// known-good fixture table: the former proves the full durable round trip
// is exact, the latter proves the narrower invariant that anything
// Content.Validate accepts, ContentToAgenticMessage can always project.
func contentAllKindsCases() []contentKindCase {
	return []contentKindCase{
		{
			name: "reasoning",
			role: RoleAssistant,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeReasoning, Reasoning: &einoschema.Reasoning{
				Text: "thinking...", Signature: "SIGNATURE_SENTINEL",
				OpenAIExtension: &openai.ReasoningExtension{Content: []*openai.ReasoningContent{{Text: "step one"}, {Text: "step two"}}},
			}},
			expect: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeReasoning, Reasoning: &einoschema.Reasoning{
				Text:            "thinking...",
				OpenAIExtension: &openai.ReasoningExtension{Content: []*openai.ReasoningContent{{Text: "step one"}, {Text: "step two"}}},
			}},
		},
		{
			name: "user_input_text",
			role: RoleUser,
			in:   &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputText, UserInputText: &einoschema.UserInputText{Text: "hello there"}},
		},
		{
			name: "user_input_image",
			role: RoleUser,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputImage, UserInputImage: &einoschema.UserInputImage{
				URL: "https://example.com/pic.png", MIMEType: "image/png", Detail: einoschema.ImageURLDetailHigh,
			}},
		},
		{
			name: "user_input_audio",
			role: RoleUser,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputAudio, UserInputAudio: &einoschema.UserInputAudio{
				Base64Data: base64.StdEncoding.EncodeToString([]byte("audio-bytes")), MIMEType: "audio/wav",
			}},
		},
		{
			name: "user_input_video",
			role: RoleUser,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputVideo, UserInputVideo: &einoschema.UserInputVideo{
				URL: "https://example.com/clip.mp4", MIMEType: "video/mp4",
			}},
		},
		{
			name: "user_input_file",
			role: RoleUser,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputFile, UserInputFile: &einoschema.UserInputFile{
				URL: "https://example.com/doc.pdf", Name: "doc.pdf", MIMEType: "application/pdf",
			}},
		},
		{
			name: "tool_search_result",
			role: RoleUser,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeToolSearchResult, ToolSearchFunctionToolResult: &einoschema.ToolSearchFunctionToolResult{
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
			name:   "assistant_gen_text",
			role:   RoleAssistant,
			in:     assistantGenTextFixture(),
			expect: assistantGenTextFixtureExpected(),
		},
		{
			name: "assistant_gen_image",
			role: RoleAssistant,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenImage, AssistantGenImage: &einoschema.AssistantGenImage{
				Base64Data: base64.StdEncoding.EncodeToString([]byte("img-bytes")), MIMEType: "image/png",
			}},
		},
		{
			name: "assistant_gen_audio",
			role: RoleAssistant,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenAudio, AssistantGenAudio: &einoschema.AssistantGenAudio{
				URL: "https://example.com/out.wav", MIMEType: "audio/wav",
			}},
		},
		{
			name: "assistant_gen_video",
			role: RoleAssistant,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenVideo, AssistantGenVideo: &einoschema.AssistantGenVideo{
				URL: "https://example.com/out.mp4", MIMEType: "video/mp4",
			}},
		},
		{
			name: "function_tool_call",
			role: RoleAssistant,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &einoschema.FunctionToolCall{
				CallID: "call-1", Name: "get_weather", Arguments: `{"city":"nyc"}`,
			}},
		},
		{
			name: "function_tool_result_all_variants",
			role: RoleUser,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeFunctionToolResult, FunctionToolResult: &einoschema.FunctionToolResult{
				CallID: "call-1", Name: "get_weather",
				Content: []*einoschema.FunctionToolResultContentBlock{
					{Type: einoschema.FunctionToolResultContentBlockTypeText, Text: &einoschema.UserInputText{Text: "sunny"}},
					{Type: einoschema.FunctionToolResultContentBlockTypeImage, Image: &einoschema.UserInputImage{URL: "https://example.com/a.png", MIMEType: "image/png", Detail: einoschema.ImageURLDetailLow}},
					{Type: einoschema.FunctionToolResultContentBlockTypeAudio, Audio: &einoschema.UserInputAudio{URL: "https://example.com/a.wav", MIMEType: "audio/wav"}},
					{Type: einoschema.FunctionToolResultContentBlockTypeVideo, Video: &einoschema.UserInputVideo{URL: "https://example.com/a.mp4", MIMEType: "video/mp4"}},
					{Type: einoschema.FunctionToolResultContentBlockTypeFile, File: &einoschema.UserInputFile{URL: "https://example.com/a.pdf", Name: "a.pdf", MIMEType: "application/pdf"}},
				},
			}},
		},
		{
			name: "server_tool_call",
			role: RoleAssistant,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeServerToolCall, ServerToolCall: &einoschema.ServerToolCall{
				Name: "web_search", CallID: "srv-1",
				Arguments: map[string]any{"query": "weather nyc", "count": float64(3)},
			}},
		},
		{
			name: "server_tool_result",
			role: RoleAssistant,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeServerToolResult, ServerToolResult: &einoschema.ServerToolResult{
				Name: "web_search", CallID: "srv-1",
				Content: map[string]any{"results": []any{"a", "b"}},
			}},
		},
		{
			name: "mcp_tool_call",
			role: RoleAssistant,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPToolCall, MCPToolCall: &einoschema.MCPToolCall{
				ServerLabel: "my-mcp", ApprovalRequestID: "appr-1", CallID: "mcp-call-1", Name: "list_files", Arguments: `{"dir":"/tmp"}`,
			}},
		},
		{
			name: "mcp_tool_result",
			role: RoleAssistant,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPToolResult, MCPToolResult: &einoschema.MCPToolResult{
				ServerLabel: "my-mcp", CallID: "mcp-call-1", Name: "list_files", Content: `["a.txt"]`,
				Error: &einoschema.MCPToolCallError{Code: int64Ptr(42), Message: "partial failure"},
			}},
		},
		{
			name: "mcp_list_tools_result",
			role: RoleAssistant,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPListToolsResult, MCPListToolsResult: &einoschema.MCPListToolsResult{
				ServerLabel: "my-mcp",
				Tools: []*einoschema.MCPListToolsItem{
					{Name: "list_files", Description: "lists files"},
				},
			}},
		},
		{
			name: "mcp_tool_approval_request",
			role: RoleAssistant,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPToolApprovalRequest, MCPToolApprovalRequest: &einoschema.MCPToolApprovalRequest{
				ID: "appr-1", Name: "delete_file", Arguments: `{"path":"/tmp/a"}`, ServerLabel: "my-mcp",
			}},
		},
		{
			name: "mcp_tool_approval_response",
			role: RoleUser,
			in: &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeMCPToolApprovalResponse, MCPToolApprovalResponse: &einoschema.MCPToolApprovalResponse{
				ApprovalRequestID: "appr-1", Approve: true, Reason: "looks safe",
			}},
		},
	}
}

func TestContentBlockRoundTrip_AllKinds(t *testing.T) {
	cases := contentAllKindsCases()

	if len(cases) != len(AllBlockKinds()) {
		t.Fatalf("fixture count = %d, want %d (one per BlockKind)", len(cases), len(AllBlockKinds()))
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			msg := &einoschema.AgenticMessage{Role: agenticRole(c.role), ContentBlocks: []*einoschema.ContentBlock{c.in}}
			_, parts, rebuilt := roundTripMessage(t, c.role, msg)

			expect := c.expect
			if expect == nil {
				expect = c.in
			}
			assertJSONEqual(t, rebuilt.ContentBlocks[0], expect)

			// Private material must never appear in the encoded parts.
			for _, part := range parts {
				if bytes.Contains(part.Payload, []byte("SIGNATURE_SENTINEL")) {
					t.Fatalf("part payload leaks private signature: %s", part.Payload)
				}
			}
		})
	}
}

// TestContentValidateImpliesProjectable pins the invariant behind C1: every
// Content value that Content.Validate accepts must also succeed through
// ContentToAgenticMessage. Before that fix, an unrecognised OpenAI/Claude
// annotation type or a non-ToolInfo tool_search entry could pass Validate
// (and therefore be durably persisted) but permanently fail projection.
// This drives the invariant over the full 20-kind fixture table (built
// directly through ContentFromAgenticMessage, independent of the encode/
// decode round trip TestContentBlockRoundTrip_AllKinds already covers) plus
// the two hostile inputs C1 identified.
func TestContentValidateImpliesProjectable(t *testing.T) {
	assertInvariant := func(t *testing.T, content Content) {
		t.Helper()
		limits := DefaultContentLimits()
		err := content.Validate(limits)
		if err != nil {
			// Validate rejected it: the invariant holds vacuously, but make
			// sure it is rejected for a content-contract reason, not
			// something unrelated.
			if !errors.Is(err, ErrContentInvalid) && !errors.Is(err, ErrContentUnsupported) && !errors.Is(err, ErrContentTooLarge) {
				t.Fatalf("Validate returned unexpected error: %v", err)
			}
			return
		}
		if _, err := ContentToAgenticMessage(content); err != nil {
			t.Fatalf("Content.Validate accepted content that ContentToAgenticMessage rejected: %v\ncontent: %s", err, mustJSON(t, content))
		}
	}

	t.Run("every fixture in the 20-kind table", func(t *testing.T) {
		for _, c := range contentAllKindsCases() {
			t.Run(c.name, func(t *testing.T) {
				msg := &einoschema.AgenticMessage{Role: agenticRole(c.role), ContentBlocks: []*einoschema.ContentBlock{c.in}}
				content, _, err := ContentFromAgenticMessage(msg, sequentialIDs("b"))
				if err != nil {
					t.Fatalf("ContentFromAgenticMessage: %v", err)
				}
				assertInvariant(t, content)
			})
		}
	})

	t.Run("hostile: unknown openai/claude annotation type", func(t *testing.T) {
		content := Content{Role: RoleAssistant, Blocks: []ContentBlock{
			{ID: "b1", Kind: BlockKindAssistantGenText, Text: &TextBlock{
				Text: "hi", Annotations: []TextAnnotation{{Type: "future_citation_type"}},
			}},
		}}
		// This must be rejected outright (not merely "vacuously fine"):
		// before the fix, validateAnnotation accepted any non-empty,
		// UTF-8-valid Type, so this exact fixture passed Validate and then
		// failed ContentToAgenticMessage forever once persisted.
		if err := content.Validate(DefaultContentLimits()); !errors.Is(err, ErrContentUnsupported) {
			t.Fatalf("Validate err = %v, want ErrContentUnsupported", err)
		}
		assertInvariant(t, content)
	})

	t.Run("hostile: tool_search entry that is valid JSON but not a ToolInfo", func(t *testing.T) {
		content := Content{Role: RoleUser, Blocks: []ContentBlock{
			{ID: "b1", Kind: BlockKindToolSearchResult, ToolSearch: &ToolSearchBlock{
				CallID: "call-1", Name: "search", Tools: []json.RawMessage{json.RawMessage(`[1,2,3]`)},
			}},
		}}
		// Before the fix, validateVariant only checked json.Valid on each
		// Tools entry, so this exact fixture passed Validate and then
		// failed toolSearchToEino's json.Unmarshal into *ToolInfo forever
		// once persisted.
		if err := content.Validate(DefaultContentLimits()); !errors.Is(err, ErrContentInvalid) {
			t.Fatalf("Validate err = %v, want ErrContentInvalid", err)
		}
		assertInvariant(t, content)
	})
}

func agenticRole(role Role) einoschema.AgenticRoleType {
	switch role {
	case RoleSystem:
		return einoschema.AgenticRoleTypeSystem
	case RoleAssistant:
		return einoschema.AgenticRoleTypeAssistant
	default:
		return einoschema.AgenticRoleTypeUser
	}
}

func int64Ptr(v int64) *int64 { return &v }

func assistantGenTextFixture() *einoschema.ContentBlock {
	return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{
		Text: "here is the answer",
		OpenAIExtension: &openai.AssistantGenTextExtension{
			Refusal: &openai.OutputRefusal{Reason: "none"},
			Annotations: []*openai.TextAnnotation{
				{Type: openai.TextAnnotationTypeURLCitation, URLCitation: &openai.TextAnnotationURLCitation{Title: "src", URL: "https://example.com", StartIndex: 1, EndIndex: 5}},
				{Type: openai.TextAnnotationTypeFileCitation, FileCitation: &openai.TextAnnotationFileCitation{FileID: "file-1", Filename: "report.pdf", Index: 42}},
				{Type: openai.TextAnnotationTypeFilePath, FilePath: &openai.TextAnnotationFilePath{FileID: "file-2", Index: 7}},
			},
		},
		ClaudeExtension: &claude.AssistantGenTextExtension{
			Citations: []*claude.TextCitation{
				{Type: claude.TextCitationTypeWebSearchResultLocation, WebSearchResultLocation: &claude.CitationWebSearchResultLocation{
					CitedText: "quoted text", Title: "web title", URL: "https://example.com/web", EncryptedIndex: "ENCRYPTED_INDEX_SENTINEL",
				}},
			},
		},
	}}
}

func assistantGenTextFixtureExpected() *einoschema.ContentBlock {
	return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{
		Text: "here is the answer",
		OpenAIExtension: &openai.AssistantGenTextExtension{
			Refusal: &openai.OutputRefusal{Reason: "none"},
			Annotations: []*openai.TextAnnotation{
				{Type: openai.TextAnnotationTypeURLCitation, URLCitation: &openai.TextAnnotationURLCitation{Title: "src", URL: "https://example.com", StartIndex: 1, EndIndex: 5}},
				{Type: openai.TextAnnotationTypeFileCitation, FileCitation: &openai.TextAnnotationFileCitation{FileID: "file-1", Filename: "report.pdf", Index: 42}},
				{Type: openai.TextAnnotationTypeFilePath, FilePath: &openai.TextAnnotationFilePath{FileID: "file-2", Index: 7}},
			},
		},
		ClaudeExtension: &claude.AssistantGenTextExtension{
			Citations: []*claude.TextCitation{
				{Type: claude.TextCitationTypeWebSearchResultLocation, WebSearchResultLocation: &claude.CitationWebSearchResultLocation{
					CitedText: "quoted text", Title: "web title", URL: "https://example.com/web",
				}},
			},
		},
	}}
}

func TestContentMixedMessageOrdering(t *testing.T) {
	msg := &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{
			{Type: einoschema.ContentBlockTypeReasoning, Reasoning: &einoschema.Reasoning{Text: "thinking"}},
			{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: "answer"}},
			{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &einoschema.FunctionToolCall{CallID: "c1", Name: "f", Arguments: `{}`}},
			{Type: einoschema.ContentBlockTypeServerToolCall, ServerToolCall: &einoschema.ServerToolCall{Name: "search", CallID: "s1"}},
			{Type: einoschema.ContentBlockTypeMCPToolApprovalRequest, MCPToolApprovalRequest: &einoschema.MCPToolApprovalRequest{ID: "a1", Name: "del"}},
		},
	}
	content, private, err := ContentFromAgenticMessage(msg, sequentialIDs("b"))
	if err != nil {
		t.Fatalf("ContentFromAgenticMessage: %v", err)
	}
	_ = private
	wantKinds := []BlockKind{
		BlockKindReasoning, BlockKindAssistantGenText, BlockKindFunctionToolCall, BlockKindServerToolCall, BlockKindMCPToolApprovalRequest,
	}
	if len(content.Blocks) != len(wantKinds) {
		t.Fatalf("blocks = %d, want %d", len(content.Blocks), len(wantKinds))
	}
	for i, k := range wantKinds {
		if content.Blocks[i].Kind != k {
			t.Fatalf("block[%d].Kind = %s, want %s", i, content.Blocks[i].Kind, k)
		}
	}
	parts, err := EncodeContentParts(content, sequentialPartIDs("p"), MessageID("m"), ID("s"), RunID("r"), time.Unix(0, 0), DefaultContentLimits())
	if err != nil {
		t.Fatalf("EncodeContentParts: %v", err)
	}
	for i, part := range parts {
		if part.Ordinal != int64(i) {
			t.Fatalf("part[%d].Ordinal = %d, want %d", i, part.Ordinal, i)
		}
		if part.Kind != PartKindForBlock(wantKinds[i]) {
			t.Fatalf("part[%d].Kind = %s, want %s", i, part.Kind, PartKindForBlock(wantKinds[i]))
		}
	}
	decoded, err := DecodeContentParts(RoleAssistant, parts, DefaultContentLimits())
	if err != nil {
		t.Fatalf("DecodeContentParts: %v", err)
	}
	for i, k := range wantKinds {
		if decoded.Blocks[i].Kind != k {
			t.Fatalf("decoded block[%d].Kind = %s, want %s", i, decoded.Blocks[i].Kind, k)
		}
	}
}

func TestContentRoleKindRejection(t *testing.T) {
	content := Content{Role: RoleUser, Blocks: []ContentBlock{
		{ID: "b1", Kind: BlockKindReasoning, Reasoning: &ReasoningBlock{Text: "x"}},
	}}
	if err := content.Validate(DefaultContentLimits()); !errors.Is(err, ErrContentInvalid) {
		t.Fatalf("err = %v, want ErrContentInvalid", err)
	}

	sysContent := Content{Role: RoleSystem, Blocks: []ContentBlock{
		{ID: "b1", Kind: BlockKindFunctionToolCall, FunctionCall: &FunctionCallBlock{CallID: "c", Name: "f"}},
	}}
	if err := sysContent.Validate(DefaultContentLimits()); !errors.Is(err, ErrContentInvalid) {
		t.Fatalf("err = %v, want ErrContentInvalid", err)
	}

	toolContent := Content{Role: RoleTool, Blocks: nil}
	if err := toolContent.Validate(DefaultContentLimits()); !errors.Is(err, ErrContentInvalid) {
		t.Fatalf("tool role err = %v, want ErrContentInvalid", err)
	}
}

func TestContentDuplicateBlockIDs(t *testing.T) {
	content := Content{Role: RoleUser, Blocks: []ContentBlock{
		{ID: "dup", Kind: BlockKindUserInputText, Text: &TextBlock{Text: "a"}},
		{ID: "dup", Kind: BlockKindUserInputText, Text: &TextBlock{Text: "b"}},
	}}
	if err := content.Validate(DefaultContentLimits()); !errors.Is(err, ErrContentInvalid) {
		t.Fatalf("err = %v, want ErrContentInvalid", err)
	}
}

func TestContentMalformedUnions(t *testing.T) {
	t.Run("two variants set", func(t *testing.T) {
		content := Content{Role: RoleUser, Blocks: []ContentBlock{
			{ID: "b1", Kind: BlockKindUserInputText, Text: &TextBlock{Text: "a"}, Media: &MediaBlock{URL: "https://x"}},
		}}
		if err := content.Validate(DefaultContentLimits()); !errors.Is(err, ErrContentInvalid) {
			t.Fatalf("err = %v, want ErrContentInvalid", err)
		}
	})
	t.Run("variant not matching kind", func(t *testing.T) {
		content := Content{Role: RoleUser, Blocks: []ContentBlock{
			{ID: "b1", Kind: BlockKindUserInputText, Media: &MediaBlock{URL: "https://x"}},
		}}
		if err := content.Validate(DefaultContentLimits()); !errors.Is(err, ErrContentInvalid) {
			t.Fatalf("err = %v, want ErrContentInvalid", err)
		}
	})
	t.Run("no variant set", func(t *testing.T) {
		content := Content{Role: RoleUser, Blocks: []ContentBlock{
			{ID: "b1", Kind: BlockKindUserInputText},
		}}
		if err := content.Validate(DefaultContentLimits()); !errors.Is(err, ErrContentInvalid) {
			t.Fatalf("err = %v, want ErrContentInvalid", err)
		}
	})
}

func TestContentUnknownKind(t *testing.T) {
	content := Content{Role: RoleUser, Blocks: []ContentBlock{
		{ID: "b1", Kind: BlockKind("future_kind"), Text: &TextBlock{Text: "a"}},
	}}
	if err := content.Validate(DefaultContentLimits()); !errors.Is(err, ErrContentUnsupported) {
		t.Fatalf("err = %v, want ErrContentUnsupported", err)
	}
}

func TestContentUnknownEnvelopeFields(t *testing.T) {
	limits := DefaultContentLimits()
	raw := json.RawMessage(`{"schema":1,"block_id":"b1","kind":"user_input_text","text":{"text":"hi"},"unexpected":true}`)
	if _, err := decodeBlockEnvelope(raw, BlockKindUserInputText, limits); !errors.Is(err, ErrContentInvalid) {
		t.Fatalf("err = %v, want ErrContentInvalid", err)
	}
}

// TestContentDuplicateEnvelopeKeys pins I1's fix: encoding/json silently
// accepts duplicate object keys (last value wins) even with
// DisallowUnknownFields set, so decodeStrict must additionally re-marshal
// the decoded value and require byte equality with the trimmed input. Before
// that canonical-form gate, both fixtures below decoded successfully (the
// first silently resolving to "second", the second to schema 1), which
// means the same durable bytes could mean two different things to two
// readers (this package's encoding/json versus Eino's sonic decoder).
func TestContentDuplicateEnvelopeKeys(t *testing.T) {
	limits := DefaultContentLimits()
	t.Run("duplicate text key", func(t *testing.T) {
		raw := json.RawMessage(`{"schema":1,"block_id":"b1","kind":"user_input_text","text":{"text":"first"},"text":{"text":"second"}}`)
		if _, err := decodeBlockEnvelope(raw, BlockKindUserInputText, limits); !errors.Is(err, ErrContentInvalid) {
			t.Fatalf("err = %v, want ErrContentInvalid", err)
		}
	})
	t.Run("duplicate schema key", func(t *testing.T) {
		raw := json.RawMessage(`{"schema":99,"schema":1,"block_id":"b1","kind":"user_input_text","text":{"text":"hi"}}`)
		if _, err := decodeBlockEnvelope(raw, BlockKindUserInputText, limits); !errors.Is(err, ErrContentInvalid) {
			t.Fatalf("err = %v, want ErrContentInvalid", err)
		}
	})
	t.Run("duplicate response_meta schema key", func(t *testing.T) {
		raw := json.RawMessage(`{"schema":1,"schema":1,"meta":{}}`)
		if _, err := decodeResponseMetaEnvelope(raw, limits); !errors.Is(err, ErrContentInvalid) {
			t.Fatalf("err = %v, want ErrContentInvalid", err)
		}
	})
}

func TestContentLimitsBoundaries(t *testing.T) {
	t.Run("non-positive limits rejected", func(t *testing.T) {
		bad := []ContentLimits{
			{MaxMessageBytes: 0, MaxBlocks: 1, MaxBlockBytes: 1},
			{MaxMessageBytes: 1, MaxBlocks: 0, MaxBlockBytes: 1},
			{MaxMessageBytes: 1, MaxBlocks: 1, MaxBlockBytes: 0},
			{MaxMessageBytes: -1, MaxBlocks: 1, MaxBlockBytes: 1},
		}
		for _, limits := range bad {
			if err := limits.Validate(); !errors.Is(err, ErrContentInvalid) {
				t.Fatalf("limits %+v err = %v, want ErrContentInvalid", limits, err)
			}
			content := Content{Role: RoleUser, Blocks: []ContentBlock{{ID: "b1", Kind: BlockKindUserInputText, Text: &TextBlock{Text: "x"}}}}
			if err := content.Validate(limits); !errors.Is(err, ErrContentInvalid) {
				t.Fatalf("Validate with limits %+v err = %v, want ErrContentInvalid", limits, err)
			}
		}
	})

	t.Run("max blocks exceeded", func(t *testing.T) {
		limits := ContentLimits{MaxMessageBytes: 1 << 20, MaxBlocks: 2, MaxBlockBytes: 1 << 10}
		blocks := make([]ContentBlock, 3)
		for i := range blocks {
			blocks[i] = ContentBlock{ID: string(rune('a' + i)), Kind: BlockKindUserInputText, Text: &TextBlock{Text: "x"}}
		}
		content := Content{Role: RoleUser, Blocks: blocks}
		if err := content.Validate(limits); !errors.Is(err, ErrContentTooLarge) {
			t.Fatalf("err = %v, want ErrContentTooLarge", err)
		}
	})

	t.Run("one byte over MaxBlockBytes", func(t *testing.T) {
		limits := DefaultContentLimits()
		// Measure the fixed marshaled overhead of *TextBlock (everything but
		// the Text payload itself) so the fixture can hit the MaxBlockBytes
		// boundary exactly, then one byte past it.
		probe, err := json.Marshal(&TextBlock{Text: "x"})
		if err != nil {
			t.Fatal(err)
		}
		fixed := len(probe) - 1
		atLimit := strings.Repeat("a", limits.MaxBlockBytes-fixed)
		okContent := Content{Role: RoleUser, Blocks: []ContentBlock{{ID: "b1", Kind: BlockKindUserInputText, Text: &TextBlock{Text: atLimit}}}}
		raw, err := json.Marshal(okContent.Blocks[0].Text)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) != limits.MaxBlockBytes {
			t.Fatalf("boundary fixture measured %d bytes, want exactly %d", len(raw), limits.MaxBlockBytes)
		}
		if err := okContent.Validate(limits); err != nil {
			t.Fatalf("at-limit content rejected: %v", err)
		}
		overLimit := atLimit + "a"
		overContent := Content{Role: RoleUser, Blocks: []ContentBlock{{ID: "b1", Kind: BlockKindUserInputText, Text: &TextBlock{Text: overLimit}}}}
		if err := overContent.Validate(limits); !errors.Is(err, ErrContentTooLarge) {
			t.Fatalf("err = %v, want ErrContentTooLarge", err)
		}
	})

	t.Run("MaxMessageBytes+1 exceeded across blocks", func(t *testing.T) {
		probe, err := json.Marshal(&TextBlock{Text: "x"})
		if err != nil {
			t.Fatal(err)
		}
		fixed := len(probe) - 1
		blockBytes := fixed + 10
		limits := ContentLimits{MaxMessageBytes: blockBytes*2 - 1, MaxBlocks: 100, MaxBlockBytes: blockBytes}
		block := func(id string) ContentBlock {
			return ContentBlock{ID: id, Kind: BlockKindUserInputText, Text: &TextBlock{Text: strings.Repeat("a", 10)}}
		}
		single := Content{Role: RoleUser, Blocks: []ContentBlock{block("b1")}}
		raw, err := json.Marshal(single.Blocks[0].Text)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) != blockBytes {
			t.Fatalf("per-block fixture measured %d bytes, want %d", len(raw), blockBytes)
		}
		two := Content{Role: RoleUser, Blocks: []ContentBlock{block("b1"), block("b2")}}
		if err := two.Validate(limits); !errors.Is(err, ErrContentTooLarge) {
			t.Fatalf("err = %v, want ErrContentTooLarge", err)
		}
		if err := single.Validate(limits); err != nil {
			t.Fatalf("single block under MaxMessageBytes rejected: %v", err)
		}
	})
}

func TestContentPrivateSplitNeverLeaks(t *testing.T) {
	msg := &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{
			{Type: einoschema.ContentBlockTypeReasoning, Reasoning: &einoschema.Reasoning{
				Text: "thinking", Signature: "REASONING_SIG_SENTINEL",
			}},
			assistantGenTextFixture(), // carries ENCRYPTED_INDEX_SENTINEL
		},
		ResponseMeta: &einoschema.AgenticResponseMeta{
			OpenAIExtension: &openai.ResponseMetaExtension{ID: "RESPONSE_ID_SENTINEL", PreviousResponseID: "PREV_RESPONSE_ID_SENTINEL", CreatedAt: 1234},
			GeminiExtension: &gemini.ResponseMetaExtension{
				FinishReason: "stop",
				GroundingMeta: &gemini.GroundingMetadata{
					SearchEntryPoint: &gemini.SearchEntryPoint{SDKBlob: []byte("SDK_BLOB_SENTINEL")},
				},
			},
		},
	}
	content, private, err := ContentFromAgenticMessage(msg, sequentialIDs("b"))
	if err != nil {
		t.Fatalf("ContentFromAgenticMessage: %v", err)
	}
	if len(private) == 0 {
		t.Fatal("expected private state entries")
	}
	parts, err := EncodeContentParts(content, sequentialPartIDs("p"), MessageID("m"), ID("s"), RunID("r"), time.Unix(0, 0), DefaultContentLimits())
	if err != nil {
		t.Fatalf("EncodeContentParts: %v", err)
	}
	sentinels := []string{"REASONING_SIG_SENTINEL", "ENCRYPTED_INDEX_SENTINEL", "RESPONSE_ID_SENTINEL", "PREV_RESPONSE_ID_SENTINEL", "SDK_BLOB_SENTINEL"}
	for _, part := range parts {
		for _, sentinel := range sentinels {
			if bytes.Contains(part.Payload, []byte(sentinel)) {
				t.Fatalf("part %s payload leaks private sentinel %q: %s", part.Kind, sentinel, part.Payload)
			}
		}
	}
	decoded, err := DecodeContentParts(RoleAssistant, parts, DefaultContentLimits())
	if err != nil {
		t.Fatalf("DecodeContentParts: %v", err)
	}
	rebuilt, err := ContentToAgenticMessage(decoded)
	if err != nil {
		t.Fatalf("ContentToAgenticMessage: %v", err)
	}
	rebuiltRaw, err := json.Marshal(rebuilt)
	if err != nil {
		t.Fatal(err)
	}
	for _, sentinel := range sentinels {
		if bytes.Contains(rebuiltRaw, []byte(sentinel)) {
			t.Fatalf("rebuilt public message leaks private sentinel %q: %s", sentinel, rebuiltRaw)
		}
	}

	// The SDK blob must have actually been captured, base64-encoded, in the
	// private side channel (proving it was split out, not silently dropped).
	found := false
	for _, p := range private {
		if bytes.Contains(p.Data, []byte(base64.StdEncoding.EncodeToString([]byte("SDK_BLOB_SENTINEL")))) {
			found = true
		}
	}
	if !found {
		t.Fatal("gemini SDK blob was not captured into private state")
	}
}

func TestContentStreamingMetaAndExtraRejection(t *testing.T) {
	t.Run("streaming meta", func(t *testing.T) {
		msg := &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ContentBlocks: []*einoschema.ContentBlock{
			{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: "x"}, StreamingMeta: &einoschema.StreamingMeta{Index: 0}},
		}}
		if _, _, err := ContentFromAgenticMessage(msg, sequentialIDs("b")); !errors.Is(err, ErrContentUnsupported) {
			t.Fatalf("err = %v, want ErrContentUnsupported", err)
		}
	})
	t.Run("block extra", func(t *testing.T) {
		msg := &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ContentBlocks: []*einoschema.ContentBlock{
			{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: "x"}, Extra: map[string]any{"k": "v"}},
		}}
		if _, _, err := ContentFromAgenticMessage(msg, sequentialIDs("b")); !errors.Is(err, ErrContentUnsupported) {
			t.Fatalf("err = %v, want ErrContentUnsupported", err)
		}
	})
	t.Run("message extra", func(t *testing.T) {
		msg := &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, Extra: map[string]any{"k": "v"}}
		if _, _, err := ContentFromAgenticMessage(msg, sequentialIDs("b")); !errors.Is(err, ErrContentUnsupported) {
			t.Fatalf("err = %v, want ErrContentUnsupported", err)
		}
	})
	t.Run("nested function result extra", func(t *testing.T) {
		msg := &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeUser, ContentBlocks: []*einoschema.ContentBlock{
			{Type: einoschema.ContentBlockTypeFunctionToolResult, FunctionToolResult: &einoschema.FunctionToolResult{
				CallID: "c1", Name: "f",
				Content: []*einoschema.FunctionToolResultContentBlock{
					{Type: einoschema.FunctionToolResultContentBlockTypeText, Text: &einoschema.UserInputText{Text: "x"}, Extra: map[string]any{"k": "v"}},
				},
			}},
		}}
		if _, _, err := ContentFromAgenticMessage(msg, sequentialIDs("b")); !errors.Is(err, ErrContentUnsupported) {
			t.Fatalf("err = %v, want ErrContentUnsupported", err)
		}
	})
}

func TestToolSearchNilVsEmptyParamsOneOf(t *testing.T) {
	block := &ToolSearchBlock{CallID: "c1", Name: "search", Tools: []json.RawMessage{}}
	nilParams, err := json.Marshal(&einoschema.ToolInfo{Name: "no_params"})
	if err != nil {
		t.Fatal(err)
	}
	emptyParams, err := json.Marshal(&einoschema.ToolInfo{Name: "empty_params", ParamsOneOf: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{})})
	if err != nil {
		t.Fatal(err)
	}
	block.Tools = append(block.Tools, nilParams, emptyParams)
	infos, err := block.ToolInfos()
	if err != nil {
		t.Fatal(err)
	}
	if infos[0].ParamsOneOf != nil {
		t.Fatal("nil ParamsOneOf became non-nil after round trip")
	}
	if infos[1].ParamsOneOf == nil {
		t.Fatal("empty (non-nil) ParamsOneOf became nil after round trip")
	}
	if !bytes.Equal(block.Tools[0], nilParams) || !bytes.Equal(block.Tools[1], emptyParams) {
		t.Fatal("stored raw tool bytes were mutated")
	}
}

func TestContentResponseMetaRoundTrip(t *testing.T) {
	msg := &einoschema.AgenticMessage{
		Role:          einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: "done"}}},
		ResponseMeta: &einoschema.AgenticResponseMeta{
			TokenUsage: &einoschema.TokenUsage{
				PromptTokens: 10, CompletionTokens: 5, TotalTokens: 17,
				PromptTokenDetails:      einoschema.PromptTokenDetails{CachedTokens: 2},
				CompletionTokensDetails: einoschema.CompletionTokensDetails{ReasoningTokens: 1},
			},
			OpenAIExtension: &openai.ResponseMetaExtension{
				Status: openai.ResponseStatusCompleted, ServiceTier: openai.ServiceTierDefault,
				Reasoning: &openai.Reasoning{Effort: openai.ReasoningEffortHigh, Summary: openai.ReasoningSummaryConcise},
			},
			ClaudeExtension: &claude.ResponseMetaExtension{StopReason: "end_turn", StopDetails: &claude.StopDetails{Category: "natural", Explanation: "done"}},
			GeminiExtension: &gemini.ResponseMetaExtension{
				FinishReason: "STOP",
				GroundingMeta: &gemini.GroundingMetadata{
					GroundingChunks: []*gemini.GroundingChunk{{Web: &gemini.GroundingChunkWeb{Domain: "example.com", Title: "t", URI: "https://example.com"}}},
					GroundingSupports: []*gemini.GroundingSupport{{
						ConfidenceScores: []float32{0.9}, GroundingChunkIndices: []int{0},
						Segment: &gemini.Segment{StartIndex: 0, EndIndex: 4, PartIndex: 0, Text: "done"},
					}},
					SearchEntryPoint: &gemini.SearchEntryPoint{RenderedContent: "rendered"},
					WebSearchQueries: []string{"query one"},
				},
			},
		},
	}
	_, _, rebuilt := roundTripMessage(t, RoleAssistant, msg)
	expected := *msg
	// Private fields stripped: ids/previous id/created-at and the SDK blob.
	expectedMeta := *msg.ResponseMeta
	expectedMeta.OpenAIExtension = &openai.ResponseMetaExtension{
		Status: openai.ResponseStatusCompleted, ServiceTier: openai.ServiceTierDefault,
		Reasoning: &openai.Reasoning{Effort: openai.ReasoningEffortHigh, Summary: openai.ReasoningSummaryConcise},
	}
	expected.ResponseMeta = &expectedMeta
	assertJSONEqual(t, rebuilt.ResponseMeta, expected.ResponseMeta)
}

func TestContentClone(t *testing.T) {
	original := Content{
		Role: RoleUser,
		Blocks: []ContentBlock{
			{ID: "b1", Kind: BlockKindUserInputText, Text: &TextBlock{Text: "hello", Annotations: []TextAnnotation{{Type: "x"}}}},
			{ID: "b2", Kind: BlockKindUserInputFile, Media: &MediaBlock{URL: "https://example.com/a.pdf", Name: "a.pdf"}},
		},
	}
	clone, err := original.Clone()
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	assertJSONEqual(t, clone, original)

	clone.Blocks[0].Text.Text = "mutated"
	clone.Blocks[0].Text.Annotations[0].Type = "mutated"
	clone.Blocks[1].Media.URL = "https://mutated"
	clone.Blocks[0].ID = "mutated-id"

	if original.Blocks[0].Text.Text != "hello" {
		t.Fatal("clone mutation leaked into original text")
	}
	if original.Blocks[0].Text.Annotations[0].Type != "x" {
		t.Fatal("clone mutation leaked into original annotation")
	}
	if original.Blocks[1].Media.URL != "https://example.com/a.pdf" {
		t.Fatal("clone mutation leaked into original media")
	}
	if original.Blocks[0].ID != "b1" {
		t.Fatal("clone mutation leaked into original block ID")
	}
}

// TestContentCloneMarshalFailureReturnsError pins the fix for I6: Clone must
// report an error, never silently return an empty or partial copy, when the
// content cannot round trip through JSON (here, a json.RawMessage field
// holding non-JSON bytes, which json.Marshal refuses to compact).
func TestContentCloneMarshalFailureReturnsError(t *testing.T) {
	original := Content{
		Role: RoleAssistant,
		Blocks: []ContentBlock{
			{ID: "b1", Kind: BlockKindServerToolCall, ServerCall: &ServerCallBlock{
				Name: "search", CallID: "call-1", Arguments: json.RawMessage("not-json"),
			}},
		},
	}
	clone, err := original.Clone()
	if err == nil {
		t.Fatalf("Clone succeeded on unmarshalable content, want error; got %d blocks", len(clone.Blocks))
	}
	if !errors.Is(err, ErrContentInvalid) {
		t.Fatalf("Clone err = %v, want ErrContentInvalid", err)
	}
	if len(clone.Blocks) != 0 || clone.Role != "" {
		t.Fatalf("Clone returned a non-empty partial value on error: %+v", clone)
	}
}

func TestBlockKindMatchesEinoContentBlockType(t *testing.T) {
	want := map[BlockKind]einoschema.ContentBlockType{
		BlockKindReasoning:               einoschema.ContentBlockTypeReasoning,
		BlockKindUserInputText:           einoschema.ContentBlockTypeUserInputText,
		BlockKindUserInputImage:          einoschema.ContentBlockTypeUserInputImage,
		BlockKindUserInputAudio:          einoschema.ContentBlockTypeUserInputAudio,
		BlockKindUserInputVideo:          einoschema.ContentBlockTypeUserInputVideo,
		BlockKindUserInputFile:           einoschema.ContentBlockTypeUserInputFile,
		BlockKindToolSearchResult:        einoschema.ContentBlockTypeToolSearchResult,
		BlockKindAssistantGenText:        einoschema.ContentBlockTypeAssistantGenText,
		BlockKindAssistantGenImage:       einoschema.ContentBlockTypeAssistantGenImage,
		BlockKindAssistantGenAudio:       einoschema.ContentBlockTypeAssistantGenAudio,
		BlockKindAssistantGenVideo:       einoschema.ContentBlockTypeAssistantGenVideo,
		BlockKindFunctionToolCall:        einoschema.ContentBlockTypeFunctionToolCall,
		BlockKindFunctionToolResult:      einoschema.ContentBlockTypeFunctionToolResult,
		BlockKindServerToolCall:          einoschema.ContentBlockTypeServerToolCall,
		BlockKindServerToolResult:        einoschema.ContentBlockTypeServerToolResult,
		BlockKindMCPToolCall:             einoschema.ContentBlockTypeMCPToolCall,
		BlockKindMCPToolResult:           einoschema.ContentBlockTypeMCPToolResult,
		BlockKindMCPListToolsResult:      einoschema.ContentBlockTypeMCPListToolsResult,
		BlockKindMCPToolApprovalRequest:  einoschema.ContentBlockTypeMCPToolApprovalRequest,
		BlockKindMCPToolApprovalResponse: einoschema.ContentBlockTypeMCPToolApprovalResponse,
	}
	if len(want) != 20 {
		t.Fatalf("fixture covers %d kinds, want 20", len(want))
	}
	all := AllBlockKinds()
	if len(all) != 20 {
		t.Fatalf("AllBlockKinds() returned %d kinds, want 20", len(all))
	}
	seen := make(map[BlockKind]bool, len(all))
	for _, k := range all {
		if seen[k] {
			t.Fatalf("AllBlockKinds() duplicate kind %s", k)
		}
		seen[k] = true
		einoType, ok := want[k]
		if !ok {
			t.Fatalf("AllBlockKinds() returned unexpected kind %s", k)
		}
		if string(k) != string(einoType) {
			t.Fatalf("BlockKind %s != einoschema.ContentBlockType %s", k, einoType)
		}
	}
}

func TestPartKindBlockKindBijection(t *testing.T) {
	for _, k := range AllBlockKinds() {
		part := PartKindForBlock(k)
		if part == "" {
			t.Fatalf("PartKindForBlock(%s) = empty", k)
		}
		back, ok := BlockKindForPart(part)
		if !ok || back != k {
			t.Fatalf("BlockKindForPart(%s) = (%s, %v), want (%s, true)", part, back, ok, k)
		}
	}
	if _, ok := BlockKindForPart(PartCompaction); ok {
		t.Fatal("BlockKindForPart(PartCompaction) should be false (not block-shaped)")
	}
	if _, ok := BlockKindForPart(PartProviderState); ok {
		t.Fatal("BlockKindForPart(PartProviderState) should be false")
	}
	if _, ok := BlockKindForPart(PartResponseMeta); ok {
		t.Fatal("BlockKindForPart(PartResponseMeta) should be false (not block-shaped)")
	}
}
