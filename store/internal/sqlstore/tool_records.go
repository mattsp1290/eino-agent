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
		return validToolSearchResultEnvelope(call, settlement, part)
	default:
		return false
	}
}

// recordedToolOutputShape is the minimal view of the runtime's ToolOutput
// JSON (runtime/tool_settlement.go) this package needs to check that the
// durable result content matches what was recorded, without importing
// runtime (which would create an import cycle) to re-derive the exact
// RetentionPolicy-bounded byte content.
type recordedToolOutputShape struct {
	Parts []struct {
		Type    string `json:"type"`
		Omitted bool   `json:"omitted"`
	} `json:"parts"`
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
	var recorded recordedToolOutputShape
	if err := json.Unmarshal(settlement.Output, &recorded); err != nil {
		return false
	}
	if len(recorded.Parts) == 0 {
		// Scalar (non-enhanced) result: the historical invariant holds
		// exactly -- one text content item equal to the settlement's
		// recorded Output JSON.
		return len(fr.Content) == 1 && fr.Content[0].Type == session.ResultContentText &&
			fr.Content[0].Text == string(settlement.Output)
	}
	// Enhanced (multi-part) result: the durable content cannot be
	// re-derived byte-for-byte here without runtime's RetentionPolicy
	// bounding logic, but its shape is recorded in settlement.Output
	// alongside the content it was derived from (runtime.toolOutputToResultContent),
	// so check that shape agrees: same item count, same content-item type
	// per index (an omitted or text/tool_search part always degrades to a
	// text content item; a non-omitted media part becomes its matching
	// media content type).
	if len(fr.Content) != len(recorded.Parts) {
		return false
	}
	for index, recordedPart := range recorded.Parts {
		want := session.ResultContentText // omitted parts and text/tool_search parts degrade to text
		if !recordedPart.Omitted {
			switch recordedPart.Type {
			case "image":
				want = session.ResultContentImage
			case "audio":
				want = session.ResultContentAudio
			case "video":
				want = session.ResultContentVideo
			case "file":
				want = session.ResultContentFile
			}
		}
		if fr.Content[index].Type != want {
			return false
		}
	}
	return true
}

// recordedToolSearchOutputShape is the minimal view of the runtime's tool
// search Output JSON (runtime/tool_search.go's buildTerminalToolSearchEnvelope)
// this package needs to cross-check the durable tool_search_result block
// against what was recorded.
type recordedToolSearchOutputShape struct {
	DiscoveredToolNames []string `json:"discovered_tool_names"`
}

func validToolSearchResultEnvelope(call session.ToolCall, settlement session.ToolSettlement, part session.Part) bool {
	content, err := session.DecodeContentParts(session.RoleUser, []session.Part{part}, session.MaxContentLimits())
	if err != nil || len(content.Blocks) != 1 || content.Blocks[0].ToolSearch == nil {
		return false
	}
	block := content.Blocks[0].ToolSearch
	if block.CallID != string(call.ID) {
		return false
	}
	// call.Name is the canonical search tool name and the search tool is
	// never aliased (see buildTerminalToolSearchEnvelope), so RequestedName
	// carries no additional information here -- compare against Name
	// directly.
	if block.Name != call.Name {
		return false
	}
	infos, err := block.ToolInfos()
	if err != nil {
		return false
	}
	var recorded recordedToolSearchOutputShape
	if err := json.Unmarshal(settlement.Output, &recorded); err != nil {
		return false
	}
	if len(infos) != len(recorded.DiscoveredToolNames) {
		return false
	}
	for index, info := range infos {
		name := ""
		if info != nil {
			name = info.Name
		}
		if name != recorded.DiscoveredToolNames[index] {
			return false
		}
	}
	return true
}
