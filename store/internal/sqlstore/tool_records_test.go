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
	output := strings.Repeat("b", 2<<20) // 2 MiB, over the 1 MiB default MaxBlockBytes

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

// TestValidToolResultEnvelopeAllowsNonTextResultContent guards against
// runtime-persistence-reviewer I3: a function_tool_result may carry any of
// the five session.ResultContent variants (text, image, audio, video, file),
// not text alone. The identity rule is call-ID match plus "the concatenation
// of the text items equals the stored settlement.Output", allowing
// additional non-text items alongside it.
func TestValidToolResultEnvelopeAllowsNonTextResultContent(t *testing.T) {
	content := session.Content{
		Role: session.RoleUser,
		Blocks: []session.ContentBlock{{
			ID: "block-1", Kind: session.BlockKindFunctionToolResult,
			FunctionResult: &session.FunctionResultBlock{
				CallID: "call-1", Name: "search",
				Content: []session.ResultContent{
					{Type: session.ResultContentText, Text: "sun"},
					{Type: session.ResultContentImage, Media: &session.MediaBlock{URL: "https://example.com/a.png", MIMEType: "image/png"}},
					{Type: session.ResultContentText, Text: "ny"},
				},
			},
		}},
	}
	part := encodedContentPart(t, content, session.DefaultContentLimits(), "result-message-1", "result-part-1")

	call := session.ToolCall{
		ID: "call-1", SessionID: "session-1", RunID: "run-1", MessageID: "message-1",
		ResultMessageID: "result-message-1", ResultPartID: "result-part-1", Name: "search",
	}
	baseSettlement := session.ToolSettlement{
		ID: "call-1",
		ResultMessage: session.Message{
			ID: "result-message-1", SessionID: "session-1", RunID: "run-1", ParentID: "message-1", Role: session.RoleUser,
		},
		ResultPart: part,
	}

	matching := baseSettlement
	matching.Output = json.RawMessage("sunny") // concatenation of the two text items ("sun" + "ny")
	if !ValidToolResultEnvelope(call, matching) {
		t.Fatal("ValidToolResultEnvelope rejected non-text content alongside a matching text-item concatenation")
	}

	mismatched := baseSettlement
	mismatched.Output = json.RawMessage("sun") // does not equal the full text-item concatenation
	if ValidToolResultEnvelope(call, mismatched) {
		t.Fatal("ValidToolResultEnvelope accepted an Output that does not equal the text-item concatenation")
	}
}

// TestValidToolResultEnvelopeRejectsResultWithNoTextItem asserts a
// function_tool_result with only non-text content items is rejected: the
// identity rule requires at least one text item.
func TestValidToolResultEnvelopeRejectsResultWithNoTextItem(t *testing.T) {
	content := session.Content{
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
	}
	part := encodedContentPart(t, content, session.DefaultContentLimits(), "result-message-1", "result-part-1")

	call := session.ToolCall{
		ID: "call-1", SessionID: "session-1", RunID: "run-1", MessageID: "message-1",
		ResultMessageID: "result-message-1", ResultPartID: "result-part-1", Name: "search",
	}
	settlement := session.ToolSettlement{
		ID: "call-1", Output: json.RawMessage(""),
		ResultMessage: session.Message{
			ID: "result-message-1", SessionID: "session-1", RunID: "run-1", ParentID: "message-1", Role: session.RoleUser,
		},
		ResultPart: part,
	}
	if ValidToolResultEnvelope(call, settlement) {
		t.Fatal("ValidToolResultEnvelope accepted a result with no text item")
	}
}
