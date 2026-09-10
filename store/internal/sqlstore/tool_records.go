package sqlstore

import (
	"encoding/json"

	"github.com/mattsp1290/eino-agent/internal/jsonequal"
	"github.com/mattsp1290/eino-agent/session"
)

// ValidToolRequestEnvelope verifies that a tool call's request part matches it.
// The request part is a durable function_tool_call content-block envelope
// (see session.EncodeContentParts), decoded here through
// session.DecodeContentParts so this check stays in lockstep with the
// durable content contract instead of duplicating its wire shape.
func ValidToolRequestEnvelope(call session.ToolCall, part session.Part) bool {
	if call.RequestPartID == "" || part.ID != call.RequestPartID || part.MessageID != call.MessageID ||
		part.SessionID != call.SessionID || part.RunID != call.RunID || part.Kind != session.PartFunctionToolCall {
		return false
	}
	content, err := session.DecodeContentParts(session.RoleAssistant, []session.Part{part}, session.DefaultContentLimits())
	if err != nil || len(content.Blocks) != 1 || content.Blocks[0].FunctionCall == nil {
		return false
	}
	fc := content.Blocks[0].FunctionCall
	return session.ToolCallID(fc.CallID) == call.ID && fc.Name == call.Name && jsonequal.Equal(json.RawMessage(fc.Arguments), call.Input)
}

// ValidToolResultEnvelope verifies that a settlement's reserved outputs match its call.
// The result part is a durable function_tool_result content-block envelope
// on a user-role result message, decoded the same way as
// ValidToolRequestEnvelope above.
func ValidToolResultEnvelope(call session.ToolCall, settlement session.ToolSettlement) bool {
	message := settlement.ResultMessage
	part := settlement.ResultPart
	if call.ResultMessageID == "" || call.ResultPartID == "" ||
		message.ID != call.ResultMessageID || message.SessionID != call.SessionID || message.RunID != call.RunID || message.ParentID != call.MessageID || message.Role != session.RoleUser ||
		part.ID != call.ResultPartID || part.MessageID != call.ResultMessageID || part.SessionID != call.SessionID || part.RunID != call.RunID || part.Kind != session.PartFunctionToolResult {
		return false
	}
	content, err := session.DecodeContentParts(session.RoleUser, []session.Part{part}, session.DefaultContentLimits())
	if err != nil || len(content.Blocks) != 1 || content.Blocks[0].FunctionResult == nil {
		return false
	}
	fr := content.Blocks[0].FunctionResult
	if fr.CallID != string(call.ID) || len(fr.Content) != 1 || fr.Content[0].Type != session.ResultContentText {
		return false
	}
	return fr.Content[0].Text == string(settlement.Output)
}
