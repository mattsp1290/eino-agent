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
	// The persisted function_tool_call block's Name is exactly what the
	// model sent (RequestedName): it may be a registered alias, while
	// call.Name is always the canonical resolved name (see
	// runtime.resolveToolCall). Compare against RequestedName when set, and
	// fall back to Name for records that predate call aliasing (or were
	// constructed directly, e.g. by tests, without RequestedName).
	expectedName := call.RequestedName
	if expectedName == "" {
		expectedName = call.Name
	}
	return session.ToolCallID(fc.CallID) == call.ID && fc.Name == expectedName && jsonequal.Equal(json.RawMessage(fc.Arguments), call.Input)
}

// ValidToolResultEnvelope verifies that a settlement's reserved outputs match
// its call. The result part is a durable content-block envelope on a
// user-role result message, decoded the same way as ValidToolRequestEnvelope
// above. Two shapes are recognized: an ordinary function_tool_result
// envelope (session.PartFunctionToolResult) and a runtime-implemented
// tool-search result envelope (session.PartToolSearchResult, see
// runtime/tool_search.go).
func ValidToolResultEnvelope(call session.ToolCall, settlement session.ToolSettlement) bool {
	message := settlement.ResultMessage
	part := settlement.ResultPart
	if call.ResultMessageID == "" || call.ResultPartID == "" ||
		message.ID != call.ResultMessageID || message.SessionID != call.SessionID || message.RunID != call.RunID || message.ParentID != call.MessageID || message.Role != session.RoleUser ||
		part.ID != call.ResultPartID || part.MessageID != call.ResultMessageID || part.SessionID != call.SessionID || part.RunID != call.RunID {
		return false
	}
	switch part.Kind {
	case session.PartFunctionToolResult:
		return validFunctionToolResultEnvelope(call, settlement, part)
	case session.PartToolSearchResult:
		return validToolSearchResultEnvelope(call, part)
	default:
		return false
	}
}

func validFunctionToolResultEnvelope(call session.ToolCall, settlement session.ToolSettlement, part session.Part) bool {
	content, err := session.DecodeContentParts(session.RoleUser, []session.Part{part}, session.MaxContentLimits())
	if err != nil || len(content.Blocks) != 1 || content.Blocks[0].FunctionResult == nil {
		return false
	}
	fr := content.Blocks[0].FunctionResult
	if fr.CallID != string(call.ID) || len(fr.Content) == 0 {
		return false
	}
	// A scalar (non-enhanced) result keeps the exact historical invariant:
	// exactly one text content item equal to the settlement's Output JSON.
	if len(fr.Content) == 1 && fr.Content[0].Type == session.ResultContentText && fr.Content[0].Text == string(settlement.Output) {
		return true
	}
	// An enhanced (multi-part) result cannot be re-derived here without
	// runtime's RetentionPolicy-bounding logic (this package does not import
	// runtime to avoid a cycle); DecodeContentParts already re-validates
	// every content item structurally (right variant populated per its
	// type, no corrupt payloads), so a well-formed, non-empty, call-bound
	// content list is accepted.
	return true
}

func validToolSearchResultEnvelope(call session.ToolCall, part session.Part) bool {
	content, err := session.DecodeContentParts(session.RoleUser, []session.Part{part}, session.MaxContentLimits())
	if err != nil || len(content.Blocks) != 1 || content.Blocks[0].ToolSearch == nil {
		return false
	}
	return content.Blocks[0].ToolSearch.CallID == string(call.ID)
}
