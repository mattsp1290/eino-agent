package sqlstore

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func encodedContentPart(t *testing.T, content session.Content, limits session.ContentLimits, messageID session.MessageID, partID session.PartID) session.Part {
	t.Helper()
	used := false
	parts, err := session.EncodeContentParts(content, func() session.PartID {
		if used {
			return ""
		}
		used = true
		return partID
	}, messageID, "session-1", "run-1", time.Now().UTC(), limits)
	if err != nil {
		t.Fatalf("EncodeContentParts: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("parts = %#v, want 1", parts)
	}
	return parts[0]
}

// TestValidToolRequestEnvelopeAcceptsContentAdmittedUnderRaisedLimits guards
// against runtime-persistence-reviewer I2: ValidToolRequestEnvelope must
// decode with limits at least as wide as whatever the content was admitted
// under (runtime.WithContentLimits may raise ContentLimits above
// session.DefaultContentLimits), not the hard-coded production defaults —
// otherwise a request part legitimately written under raised limits decodes
// as invalid here and the caller sees an opaque session.ErrConflict.
func TestValidToolRequestEnvelopeAcceptsContentAdmittedUnderRaisedLimits(t *testing.T) {
	raised := session.ContentLimits{MaxMessageBytes: 32 << 20, MaxBlocks: 1024, MaxBlockBytes: 8 << 20}
	arguments := `{"data":"` + strings.Repeat("a", 2<<20) + `"}` // 2 MiB, over the 1 MiB default MaxBlockBytes

	content := session.Content{
		Role: session.RoleAssistant,
		Blocks: []session.ContentBlock{{
			ID: "block-1", Kind: session.BlockKindFunctionToolCall,
			FunctionCall: &session.FunctionCallBlock{CallID: "call-1", Name: "search", Arguments: arguments},
		}},
	}
	part := encodedContentPart(t, content, raised, "message-1", "part-1")

	call := session.ToolCall{
		ID: "call-1", SessionID: "session-1", RunID: "run-1", MessageID: "message-1",
		RequestPartID: "part-1", Name: "search", Input: json.RawMessage(arguments),
	}
	if !ValidToolRequestEnvelope(call, part) {
		t.Fatal("ValidToolRequestEnvelope rejected a request part legitimately admitted under raised content limits")
	}
}

// TestValidToolResultEnvelopeAcceptsContentAdmittedUnderRaisedLimits is the
// ValidToolResultEnvelope counterpart of the above.
func TestValidToolResultEnvelopeAcceptsContentAdmittedUnderRaisedLimits(t *testing.T) {
	raised := session.ContentLimits{MaxMessageBytes: 32 << 20, MaxBlocks: 1024, MaxBlockBytes: 8 << 20}
	// The scalar shape's recorded Output is always valid JSON in production
	// (runtime.encodeToolOutput marshals a ToolOutput struct); build a
	// minimal object of the same shape here, oversized via its content
	// field, over the 1 MiB default MaxBlockBytes.
	output := `{"tool_call_id":"call-1","status":"completed","content":"` + strings.Repeat("b", 2<<20) + `"}`

	content := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{{
			ID: "block-1", Kind: session.BlockKindFunctionToolResult,
			FunctionResult: &session.FunctionResultBlock{
				CallID: "call-1", Name: "search",
				Content: []session.ResultContent{{Type: session.ResultContentText, Text: output}},
			},
		}},
	}
	part := encodedContentPart(t, content, raised, "result-message-1", "result-part-1")

	call := session.ToolCall{
		ID: "call-1", SessionID: "session-1", RunID: "run-1", MessageID: "message-1",
		ResultMessageID: "result-message-1", ResultPartID: "result-part-1", Name: "search",
	}
	settlement := session.ToolSettlement{
		ID: "call-1", Output: json.RawMessage(output),
		ResultMessage: session.Message{
			ID: "result-message-1", SessionID: "session-1", RunID: "run-1", ParentID: "message-1", Role: session.RoleUser,
		},
		ResultPart: part,
	}
	if !ValidToolResultEnvelope(call, settlement) {
		t.Fatal("ValidToolResultEnvelope rejected a result part legitimately admitted under raised content limits")
	}
}

// TestValidToolResultEnvelopeRejectsTamperedScalarResult guards against
// tool-identity-reviewer I1: a scalar (non-enhanced) settlement whose durable
// content was tampered with (extra item, different text, a media item in
// place of the recorded text) must be rejected, not fall through to an
// unconditional accept.
func TestValidToolResultEnvelopeRejectsTamperedScalarResult(t *testing.T) {
	output := `{"tool_call_id":"call-1","status":"completed","content":"sun"}`
	call := session.ToolCall{
		ID: "call-1", SessionID: "session-1", RunID: "run-1", MessageID: "message-1",
		ResultMessageID: "result-message-1", ResultPartID: "result-part-1", Name: "search",
	}
	baseSettlement := session.ToolSettlement{
		ID: "call-1", Output: json.RawMessage(output),
		ResultMessage: session.Message{
			ID: "result-message-1", SessionID: "session-1", RunID: "run-1", ParentID: "message-1", Role: session.RoleUser,
		},
	}

	cases := map[string]session.Content{
		"different text": {
			Role: session.RoleUser,
			Blocks: []session.ContentBlock{{
				ID: "block-1", Kind: session.BlockKindFunctionToolResult,
				FunctionResult: &session.FunctionResultBlock{
					CallID: "call-1", Name: "search",
					Content: []session.ResultContent{{Type: session.ResultContentText, Text: "tampered"}},
				},
			}},
		},
		"extra text item": {
			Role: session.RoleUser,
			Blocks: []session.ContentBlock{{
				ID: "block-1", Kind: session.BlockKindFunctionToolResult,
				FunctionResult: &session.FunctionResultBlock{
					CallID: "call-1", Name: "search",
					Content: []session.ResultContent{
						{Type: session.ResultContentText, Text: output},
						{Type: session.ResultContentText, Text: "extra"},
					},
				},
			}},
		},
		"media instead of text": {
			Role: session.RoleUser,
			Blocks: []session.ContentBlock{{
				ID: "block-1", Kind: session.BlockKindFunctionToolResult,
				FunctionResult: &session.FunctionResultBlock{
					CallID: "call-1", Name: "search",
					Content: []session.ResultContent{
						{Type: session.ResultContentImage, Media: &session.MediaBlock{URL: "https://example.com/a.png", MIMEType: "image/png"}},
					},
				},
			}},
		},
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			part := encodedContentPart(t, content, session.DefaultContentLimits(), "result-message-1", "result-part-1")
			settlement := baseSettlement
			settlement.ResultPart = part
			if ValidToolResultEnvelope(call, settlement) {
				t.Fatalf("ValidToolResultEnvelope accepted a tampered scalar result (%s)", name)
			}
		})
	}
}

// TestValidToolResultEnvelopeAcceptsCorrectEnhancedResult asserts an
// enhanced (multi-part) function_tool_result whose content items agree in
// count, order, and type with the shape recorded in settlement.Output is
// accepted -- text, media, and an omitted part all present.
func TestValidToolResultEnvelopeAcceptsCorrectEnhancedResult(t *testing.T) {
	content := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{{
			ID: "block-1", Kind: session.BlockKindFunctionToolResult,
			FunctionResult: &session.FunctionResultBlock{
				CallID: "call-1", Name: "search",
				Content: []session.ResultContent{
					{Type: session.ResultContentText, Text: "sun"},
					{Type: session.ResultContentImage, Media: &session.MediaBlock{URL: "https://example.com/a.png", MIMEType: "image/png"}},
					{Type: session.ResultContentText, Text: `{"type":"file","omitted":true,"original_size":9999}`},
				},
			},
		}},
	}
	part := encodedContentPart(t, content, session.DefaultContentLimits(), "result-message-1", "result-part-1")

	call := session.ToolCall{
		ID: "call-1", SessionID: "session-1", RunID: "run-1", MessageID: "message-1",
		ResultMessageID: "result-message-1", ResultPartID: "result-part-1", Name: "search",
	}
	settlement := session.ToolSettlement{
		ID: "call-1",
		Output: json.RawMessage(`{"tool_call_id":"call-1","status":"completed","parts":[` +
			`{"type":"text","text":"sun"},` +
			`{"type":"image"},` +
			`{"type":"file","omitted":true,"original_size":9999}` +
			`]}`),
		ResultMessage: session.Message{
			ID: "result-message-1", SessionID: "session-1", RunID: "run-1", ParentID: "message-1", Role: session.RoleUser,
		},
		ResultPart: part,
	}
	if !ValidToolResultEnvelope(call, settlement) {
		t.Fatal("ValidToolResultEnvelope rejected a correct enhanced multi-part result")
	}

	otherCall := call
	otherCall.ID = "call-2"
	if ValidToolResultEnvelope(otherCall, settlement) {
		t.Fatal("ValidToolResultEnvelope accepted a result bound to a different call")
	}
}

// TestValidToolResultEnvelopeRejectsTamperedEnhancedResult guards against
// tool-identity-reviewer I1: an enhanced settlement whose durable content
// diverges in shape from what settlement.Output recorded (extra item, an
// omitted part hydrated back into a media item, wrong content type at an
// index) must be rejected.
func TestValidToolResultEnvelopeRejectsTamperedEnhancedResult(t *testing.T) {
	recordedOutput := json.RawMessage(`{"tool_call_id":"call-1","status":"completed","parts":[` +
		`{"type":"text","text":"sun"},` +
		`{"type":"file","omitted":true,"original_size":9999}` +
		`]}`)
	call := session.ToolCall{
		ID: "call-1", SessionID: "session-1", RunID: "run-1", MessageID: "message-1",
		ResultMessageID: "result-message-1", ResultPartID: "result-part-1", Name: "search",
	}
	baseSettlement := session.ToolSettlement{
		ID: "call-1", Output: recordedOutput,
		ResultMessage: session.Message{
			ID: "result-message-1", SessionID: "session-1", RunID: "run-1", ParentID: "message-1", Role: session.RoleUser,
		},
	}

	cases := map[string]session.Content{
		"omitted part hydrated to media": {
			Role: session.RoleUser,
			Blocks: []session.ContentBlock{{
				ID: "block-1", Kind: session.BlockKindFunctionToolResult,
				FunctionResult: &session.FunctionResultBlock{
					CallID: "call-1", Name: "search",
					Content: []session.ResultContent{
						{Type: session.ResultContentText, Text: "sun"},
						{Type: session.ResultContentFile, Media: &session.MediaBlock{URL: "https://example.com/a.bin", MIMEType: "application/octet-stream", Name: "a.bin"}},
					},
				},
			}},
		},
		"extra item not recorded": {
			Role: session.RoleUser,
			Blocks: []session.ContentBlock{{
				ID: "block-1", Kind: session.BlockKindFunctionToolResult,
				FunctionResult: &session.FunctionResultBlock{
					CallID: "call-1", Name: "search",
					Content: []session.ResultContent{
						{Type: session.ResultContentText, Text: "sun"},
						{Type: session.ResultContentText, Text: `{"type":"file","omitted":true,"original_size":9999}`},
						{Type: session.ResultContentText, Text: "smuggled extra item"},
					},
				},
			}},
		},
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			part := encodedContentPart(t, content, session.DefaultContentLimits(), "result-message-1", "result-part-1")
			settlement := baseSettlement
			settlement.ResultPart = part
			if ValidToolResultEnvelope(call, settlement) {
				t.Fatalf("ValidToolResultEnvelope accepted a tampered enhanced result (%s)", name)
			}
		})
	}
}

// TestValidToolResultEnvelopeChecksToolSearchNameAndDiscoveredNames guards
// against tool-identity-reviewer I6: a tool_search_result envelope whose
// block Name disagrees with the call's name, or whose discovered tool names
// disagree with the recorded discovered_tool_names, must be rejected.
func TestValidToolResultEnvelopeChecksToolSearchNameAndDiscoveredNames(t *testing.T) {
	toolInfoRaw := func(name string) json.RawMessage {
		raw, err := json.Marshal(struct {
			Name string `json:"name"`
			Desc string `json:"desc"`
		}{Name: name, Desc: "desc"})
		if err != nil {
			t.Fatalf("marshal tool info: %v", err)
		}
		return raw
	}

	call := session.ToolCall{
		ID: "call-1", SessionID: "session-1", RunID: "run-1", MessageID: "message-1",
		ResultMessageID: "result-message-1", ResultPartID: "result-part-1", Name: "tool_search",
	}
	baseSettlement := session.ToolSettlement{
		ID:     "call-1",
		Output: json.RawMessage(`{"tool_call_id":"call-1","status":"completed","discovered_tool_names":["weather_tool"]}`),
		ResultMessage: session.Message{
			ID: "result-message-1", SessionID: "session-1", RunID: "run-1", ParentID: "message-1", Role: session.RoleUser,
		},
	}

	correct := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{{
			ID: "block-1", Kind: session.BlockKindToolSearchResult,
			ToolSearch: &session.ToolSearchBlock{CallID: "call-1", Name: "tool_search", Tools: []json.RawMessage{toolInfoRaw("weather_tool")}},
		}},
	}
	part := encodedContentPart(t, correct, session.DefaultContentLimits(), "result-message-1", "result-part-1")
	settlement := baseSettlement
	settlement.ResultPart = part
	if !ValidToolResultEnvelope(call, settlement) {
		t.Fatal("ValidToolResultEnvelope rejected a correct tool_search_result envelope")
	}

	wrongName := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{{
			ID: "block-1", Kind: session.BlockKindToolSearchResult,
			ToolSearch: &session.ToolSearchBlock{CallID: "call-1", Name: "other_search", Tools: []json.RawMessage{toolInfoRaw("weather_tool")}},
		}},
	}
	part = encodedContentPart(t, wrongName, session.DefaultContentLimits(), "result-message-1", "result-part-1")
	settlement = baseSettlement
	settlement.ResultPart = part
	if ValidToolResultEnvelope(call, settlement) {
		t.Fatal("ValidToolResultEnvelope accepted a tool_search_result envelope whose Name disagrees with the call")
	}

	wrongDiscovered := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{{
			ID: "block-1", Kind: session.BlockKindToolSearchResult,
			ToolSearch: &session.ToolSearchBlock{CallID: "call-1", Name: "tool_search", Tools: []json.RawMessage{toolInfoRaw("smuggled_tool")}},
		}},
	}
	part = encodedContentPart(t, wrongDiscovered, session.DefaultContentLimits(), "result-message-1", "result-part-1")
	settlement = baseSettlement
	settlement.ResultPart = part
	if ValidToolResultEnvelope(call, settlement) {
		t.Fatal("ValidToolResultEnvelope accepted a tool_search_result envelope whose discovered tools disagree with the recorded output")
	}
}
