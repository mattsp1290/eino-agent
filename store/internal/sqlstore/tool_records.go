package sqlstore

import (
	"encoding/json"
	"strings"

	"github.com/mattsp1290/eino-agent/internal/jsonequal"
	"github.com/mattsp1290/eino-agent/session"
)

// ValidToolRequestEnvelope verifies that a tool call's request part matches it.
// The request part is a durable function_tool_call content-block envelope
// (see session.EncodeContentParts), decoded here through
// session.DecodeContentParts so this check stays in lockstep with the
// durable content contract instead of duplicating its wire shape.
//
// This is an identity check on content already admitted at write time under
// the writer's own configured session.ContentLimits (which
// runtime.WithContentLimits may raise above session.DefaultContentLimits),
// not an admission-time bound -- so it decodes with session.MaxContentLimits,
// the widest the durable contract ever allows, rather than the production
// defaults. Otherwise content legitimately written under raised limits would
// decode as invalid here and surface as an opaque session.ErrConflict.
func ValidToolRequestEnvelope(call session.ToolCall, part session.Part) bool {
	if call.RequestPartID == "" || part.ID != call.RequestPartID || part.MessageID != call.MessageID ||
		part.SessionID != call.SessionID || part.RunID != call.RunID || part.Kind != session.PartFunctionToolCall {
		return false
	}
	content, err := session.DecodeContentParts(session.RoleAssistant, []session.Part{part}, session.MaxContentLimits())
	if err != nil || len(content.Blocks) != 1 || content.Blocks[0].FunctionCall == nil {
		return false
	}
	fc := content.Blocks[0].FunctionCall
	return session.ToolCallID(fc.CallID) == call.ID && fc.Name == call.Name && jsonequal.Equal(json.RawMessage(fc.Arguments), call.Input)
}

// ValidToolResultEnvelope verifies that a settlement's reserved outputs match its call.
// The result part is a durable function_tool_result content-block envelope
// on a user-role result message, decoded the same way (and for the same
// reason -- see ValidToolRequestEnvelope's doc) as ValidToolRequestEnvelope
// above.
//
// The identity check does not require the content to be text-only: a
// function_tool_result may carry any of the five session.ResultContent
// variants (text, image, audio, video, file). It requires call-ID identity
// plus at least one text item whose concatenation equals the stored
// settlement.Output -- the model-visible ToolOutput JSON is always text --
// while allowing additional non-text items alongside it.
func ValidToolResultEnvelope(call session.ToolCall, settlement session.ToolSettlement) bool {
	message := settlement.ResultMessage
	part := settlement.ResultPart
	if call.ResultMessageID == "" || call.ResultPartID == "" ||
		message.ID != call.ResultMessageID || message.SessionID != call.SessionID || message.RunID != call.RunID || message.ParentID != call.MessageID || message.Role != session.RoleUser ||
		part.ID != call.ResultPartID || part.MessageID != call.ResultMessageID || part.SessionID != call.SessionID || part.RunID != call.RunID || part.Kind != session.PartFunctionToolResult {
		return false
	}
	content, err := session.DecodeContentParts(session.RoleUser, []session.Part{part}, session.MaxContentLimits())
	if err != nil || len(content.Blocks) != 1 || content.Blocks[0].FunctionResult == nil {
		return false
	}
	fr := content.Blocks[0].FunctionResult
	if fr.CallID != string(call.ID) || len(fr.Content) == 0 {
		return false
	}
	var text strings.Builder
	hasText := false
	for _, item := range fr.Content {
		if item.Type != session.ResultContentText {
			continue
		}
		hasText = true
		text.WriteString(item.Text)
	}
	return hasText && text.String() == string(settlement.Output)
}
