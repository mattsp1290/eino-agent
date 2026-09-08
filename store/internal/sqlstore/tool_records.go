package sqlstore

import (
	"encoding/json"

	"github.com/mattsp1290/eino-agent/internal/jsonequal"
	"github.com/mattsp1290/eino-agent/session"
)

// ValidToolRequestEnvelope verifies that a tool call's request part matches it.
func ValidToolRequestEnvelope(call session.ToolCall, part session.Part) bool {
	if call.RequestPartID == "" || part.ID != call.RequestPartID || part.MessageID != call.MessageID ||
		part.SessionID != call.SessionID || part.RunID != call.RunID || part.Kind != session.PartToolCall {
		return false
	}
	var payload struct {
		ID        session.ToolCallID `json:"id"`
		Name      string             `json:"name"`
		Arguments json.RawMessage    `json:"arguments"`
	}
	if err := json.Unmarshal(part.Payload, &payload); err != nil {
		return false
	}
	return payload.ID == call.ID && payload.Name == call.Name && jsonequal.Equal(payload.Arguments, call.Input)
}

// ValidToolResultEnvelope verifies that a settlement's reserved outputs match its call.
func ValidToolResultEnvelope(call session.ToolCall, settlement session.ToolSettlement) bool {
	message := settlement.ResultMessage
	part := settlement.ResultPart
	return call.ResultMessageID != "" && call.ResultPartID != "" &&
		message.ID == call.ResultMessageID && message.SessionID == call.SessionID && message.RunID == call.RunID && message.ParentID == call.MessageID && message.Role == session.RoleTool &&
		part.ID == call.ResultPartID && part.MessageID == call.ResultMessageID && part.SessionID == call.SessionID && part.RunID == call.RunID && part.Kind == session.PartToolResult &&
		jsonequal.Equal(part.Payload, settlement.Output)
}
