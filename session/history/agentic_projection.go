package history

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

// ErrMixedContentKinds reports that one durable message mixes legacy content
// part kinds (text, tool_call, tool_result, compaction, ...) with the new
// BlockKind-backed content part kinds. A single message must use one family
// or the other.
var ErrMixedContentKinds = errors.New("session history: message mixes legacy and durable content block kinds")

// AgenticProjection projects durable session messages into Eino agentic
// messages, preserving durable content block identity.
type AgenticProjection struct {
	Messages         []*einoschema.AgenticMessage
	SourceMessageIDs []session.MessageID
	// BlockIDs is parallel to Messages: BlockIDs[i] is the ordered list of
	// durable content-block identities backing Messages[i].ContentBlocks,
	// i.e. it reflects the projection actually emitted (including any
	// filtering, such as reasoning blocks dropped when
	// !Options.IncludeReasoning). It is nil for a message projected through
	// the legacy pipeline, which carries no durable block identity. Callers
	// that need to bind provider-private state to a specific dispatched
	// block (see runtime/provider_state.go) must use this rather than
	// re-deriving block IDs from the raw stored parts, which would include
	// blocks the projection dropped.
	BlockIDs [][]string
	// PartIDs maps a durable content block's identity, scoped to its owning
	// message, to the durable Part that carries it. Only populated for
	// messages projected through the durable BlockKind pipeline. Unlike
	// BlockIDs, this is populated from every block present in storage
	// before any reasoning filtering is applied.
	//
	// It is keyed by (MessageID, BlockID) rather than bare BlockID because
	// Content.Validate only enforces block-ID uniqueness within a single
	// message: two different messages in the same session batch are free to
	// reuse the same block ID, and a bare-BlockID map would silently
	// collapse one of them.
	PartIDs map[BlockRef]session.PartID
}

// BlockRef identifies one durable content block scoped to its owning
// message.
type BlockRef struct {
	MessageID session.MessageID
	BlockID   string
}

// ProjectAgentic converts durable session messages and parts into Eino
// agentic messages. It never reads runtime events, so live-only deltas are
// excluded by design, and it never reads provider_state parts.
func ProjectAgentic(batch session.ReplayBatch, options Options) (AgenticProjection, error) {
	batch, err := applyEpoch(batch, options.Epoch)
	if err != nil {
		return AgenticProjection{}, err
	}
	partsByMessage := map[session.MessageID][]session.Part{}
	for _, part := range batch.Parts {
		partsByMessage[part.MessageID] = append(partsByMessage[part.MessageID], part)
	}
	result := AgenticProjection{
		Messages:         make([]*einoschema.AgenticMessage, 0, len(batch.Messages)),
		SourceMessageIDs: make([]session.MessageID, 0, len(batch.Messages)),
		BlockIDs:         make([][]string, 0, len(batch.Messages)),
		PartIDs:          map[BlockRef]session.PartID{},
	}
	for _, message := range batch.Messages {
		parts := partsByMessage[message.ID]
		sort.SliceStable(parts, func(i, j int) bool {
			return parts[i].Ordinal < parts[j].Ordinal
		})
		projected, blockIDs, err := projectAgenticMessage(message, parts, options, result.PartIDs)
		if err != nil {
			return AgenticProjection{}, err
		}
		for i, msg := range projected {
			result.Messages = append(result.Messages, msg)
			result.SourceMessageIDs = append(result.SourceMessageIDs, message.ID)
			var ids []string
			if i < len(blockIDs) {
				ids = blockIDs[i]
			}
			result.BlockIDs = append(result.BlockIDs, ids)
		}
	}
	return result, nil
}

// LoadAgentic reads all replayable history for a session and projects it into
// Eino agentic messages.
func LoadAgentic(ctx context.Context, store session.Store, sessionID session.ID, options Options) (AgenticProjection, error) {
	batch, err := LoadBatch(ctx, store, sessionID)
	if err != nil {
		return AgenticProjection{}, err
	}
	return ProjectAgentic(batch, options)
}

func projectAgenticMessage(message session.Message, parts []session.Part, options Options, partIDs map[BlockRef]session.PartID) ([]*einoschema.AgenticMessage, [][]string, error) {
	richCount, legacyCount := 0, 0
	for _, part := range parts {
		// PartProviderState is private provider continuity data, and
		// PartState is a private app-visible state marker (runtime's
		// adkApprovalBinding uses it as a CAS record on the same message as
		// the public content it guards -- see runtime/adk_approval.go).
		// Neither participates in the rich-vs-legacy family decision: both
		// downstream projectors already treat PartState as ignorable by
		// design (projectLegacyAgenticMessage's explicit case;
		// DecodeContentParts's "legacy/unrecognized kinds are ignored"
		// fallthrough, which projectRichAgenticMessage relies on), so
		// counting it here as "legacy" would wrongly reject an otherwise
		// all-rich message as mixed-kind.
		if part.Kind == session.PartProviderState || part.Kind == session.PartState {
			continue
		}
		if isRichContentPart(part) {
			richCount++
		} else {
			legacyCount++
		}
	}
	switch {
	case richCount > 0 && legacyCount > 0:
		return nil, nil, fmt.Errorf("message %s: %w", message.ID, ErrMixedContentKinds)
	case richCount > 0:
		msgs, blockIDs, err := projectRichAgenticMessage(message, parts, options, partIDs)
		if err != nil {
			return nil, nil, err
		}
		return msgs, [][]string{blockIDs}, nil
	default:
		msgs, err := projectLegacyAgenticMessage(message, parts, options)
		if err != nil {
			return nil, nil, err
		}
		return msgs, nil, nil
	}
}

// isRichContentPart reports whether part belongs to the durable BlockKind /
// response_meta content family. PartReasoning is shared between the legacy
// free-text payload and the new BlockKindReasoning envelope, so it is only
// "rich" when its payload actually carries the envelope's schema field.
func isRichContentPart(part session.Part) bool {
	if part.Kind == session.PartResponseMeta {
		return true
	}
	if _, ok := session.BlockKindForPart(part.Kind); ok {
		if part.Kind == session.PartReasoning {
			return hasContentSchemaField(part.Payload)
		}
		return true
	}
	return false
}

func projectRichAgenticMessage(message session.Message, parts []session.Part, options Options, partIDs map[BlockRef]session.PartID) ([]*einoschema.AgenticMessage, []string, error) {
	decodeRole := message.Role
	if decodeRole == session.RoleTool {
		// RoleTool durable messages carry function_tool_result content,
		// which the durable content contract only allows on RoleUser.
		decodeRole = session.RoleUser
	}
	content, err := session.DecodeContentParts(decodeRole, parts, options.contentLimits())
	if err != nil {
		return nil, nil, fmt.Errorf("message %s: %w", message.ID, err)
	}
	for _, part := range parts {
		if part.Kind == session.PartProviderState || part.Kind == session.PartResponseMeta {
			continue
		}
		if _, ok := session.BlockKindForPart(part.Kind); !ok {
			continue
		}
		if id, ok := blockIDFromPayload(part.Payload); ok {
			partIDs[BlockRef{MessageID: message.ID, BlockID: id}] = part.ID
		}
	}
	if !options.IncludeReasoning {
		content.Blocks = withoutReasoningBlocks(content.Blocks)
	}
	// blockIDs reflects the blocks actually emitted below, in the same
	// order as the resulting agentic.ContentBlocks (ContentToAgenticMessage
	// preserves block order 1:1), i.e. after any reasoning filtering above.
	blockIDs := make([]string, len(content.Blocks))
	for i, b := range content.Blocks {
		blockIDs[i] = b.ID
	}
	agentic, err := session.ContentToAgenticMessage(content)
	if err != nil {
		return nil, nil, fmt.Errorf("message %s: %w", message.ID, err)
	}
	return []*einoschema.AgenticMessage{agentic}, blockIDs, nil
}

func withoutReasoningBlocks(blocks []session.ContentBlock) []session.ContentBlock {
	out := make([]session.ContentBlock, 0, len(blocks))
	for _, b := range blocks {
		if b.Kind == session.BlockKindReasoning {
			continue
		}
		out = append(out, b)
	}
	return out
}

type blockEnvelopeIdentityProbe struct {
	BlockID string `json:"block_id"`
}

func blockIDFromPayload(raw json.RawMessage) (string, bool) {
	var probe blockEnvelopeIdentityProbe
	if err := json.Unmarshal(raw, &probe); err != nil || probe.BlockID == "" {
		return "", false
	}
	return probe.BlockID, true
}

func agenticRoleFromSessionRole(role session.Role) (einoschema.AgenticRoleType, error) {
	switch role {
	case session.RoleSystem:
		return einoschema.AgenticRoleTypeSystem, nil
	case session.RoleUser:
		return einoschema.AgenticRoleTypeUser, nil
	case session.RoleAssistant:
		return einoschema.AgenticRoleTypeAssistant, nil
	default:
		return "", fmt.Errorf("unsupported session role %q", role)
	}
}

// legacyTextContentBlock projects a legacy PartText/PartCompaction payload
// into the agentic text block appropriate for role: assistant messages
// become assistant_gen_text, everything else (user, system) becomes
// user_input_text.
func legacyTextContentBlock(role session.Role, text string) *einoschema.ContentBlock {
	if role == session.RoleAssistant {
		return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: text}}
	}
	return &einoschema.ContentBlock{Type: einoschema.ContentBlockTypeUserInputText, UserInputText: &einoschema.UserInputText{Text: text}}
}

func projectLegacyAgenticMessage(message session.Message, parts []session.Part, options Options) ([]*einoschema.AgenticMessage, error) {
	switch message.Role {
	case session.RoleSystem, session.RoleUser, session.RoleAssistant:
		agenticRole, err := agenticRoleFromSessionRole(message.Role)
		if err != nil {
			return nil, err
		}
		primary := &einoschema.AgenticMessage{Role: agenticRole}
		var toolResultParts []session.Part
		for _, part := range parts {
			switch part.Kind {
			case session.PartText, session.PartCompaction:
				text, err := partText(part)
				if err != nil {
					return nil, err
				}
				primary.ContentBlocks = append(primary.ContentBlocks, legacyTextContentBlock(message.Role, text))
			case session.PartReasoning:
				if !options.IncludeReasoning {
					continue
				}
				text, err := partText(part)
				if err != nil {
					return nil, err
				}
				primary.ContentBlocks = append(primary.ContentBlocks, &einoschema.ContentBlock{
					Type:      einoschema.ContentBlockTypeReasoning,
					Reasoning: &einoschema.Reasoning{Text: text},
				})
			case session.PartToolCall:
				toolCall, err := decodeToolCall(part.Payload)
				if err != nil {
					return nil, err
				}
				primary.ContentBlocks = append(primary.ContentBlocks, &einoschema.ContentBlock{
					Type: einoschema.ContentBlockTypeFunctionToolCall,
					FunctionToolCall: &einoschema.FunctionToolCall{
						CallID: toolCall.ID, Name: toolCall.Function.Name, Arguments: toolCall.Function.Arguments,
					},
				})
			case session.PartToolResult:
				toolResultParts = append(toolResultParts, part)
			case session.PartState, session.PartStep, session.PartFile, session.PartProviderState:
				// ignored by design
			}
		}
		result := []*einoschema.AgenticMessage{primary}
		toolMessages, err := legacyToolResultMessages(toolResultParts)
		if err != nil {
			return nil, err
		}
		result = append(result, toolMessages...)
		return result, nil
	case session.RoleTool:
		return legacyToolResultMessages(parts)
	default:
		return nil, fmt.Errorf("unsupported session role %q", message.Role)
	}
}

// legacyToolResultMessages converts every PartToolResult part into its own
// user-role agentic message carrying a single function_tool_result text
// block, rendered exactly as the classic projector's decodeToolResult would.
func legacyToolResultMessages(parts []session.Part) ([]*einoschema.AgenticMessage, error) {
	var result []*einoschema.AgenticMessage
	for _, part := range parts {
		if part.Kind != session.PartToolResult {
			continue
		}
		toolCallID, content, err := decodeToolResult(part.Payload)
		if err != nil {
			return nil, err
		}
		result = append(result, &einoschema.AgenticMessage{
			Role: einoschema.AgenticRoleTypeUser,
			ContentBlocks: []*einoschema.ContentBlock{{
				Type: einoschema.ContentBlockTypeFunctionToolResult,
				FunctionToolResult: &einoschema.FunctionToolResult{
					CallID: toolCallID,
					Content: []*einoschema.FunctionToolResultContentBlock{{
						Type: einoschema.FunctionToolResultContentBlockTypeText,
						Text: &einoschema.UserInputText{Text: content},
					}},
				},
			}},
		})
	}
	return result, nil
}
