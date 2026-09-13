package history

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

// ErrClassicUnsupported reports that a durable part uses one of the new
// BlockKind-backed content kinds that the classic *schema.Message projection
// cannot represent (anything other than reasoning, user_input_text,
// assistant_gen_text, function_tool_call, and function_tool_result). Classic
// callers fail loudly instead of silently flattening rich content.
var ErrClassicUnsupported = errors.New("session history: classic projection does not support this content block kind")

// Options controls durable history projection.
type Options struct {
	IncludeReasoning bool
	Epoch            *session.ContextEpoch
	// ContentLimits bounds durable content decoding for the BlockKind-backed
	// content family. The zero value means session.DefaultContentLimits().
	// It must match the limits content was admitted under (runtime's
	// configured session.ContentLimits, see WithContentLimits), or content
	// legitimately persisted under raised limits becomes unprojectable.
	ContentLimits session.ContentLimits
}

// contentLimits resolves the configured content bounds, defaulting to
// session.DefaultContentLimits() when Options.ContentLimits is unset.
func (o Options) contentLimits() session.ContentLimits {
	if o.ContentLimits == (session.ContentLimits{}) {
		return session.DefaultContentLimits()
	}
	return o.ContentLimits
}

// Project converts durable session messages and parts into provider messages.
// It never reads runtime events, so live-only deltas are excluded by design.
func Project(batch session.ReplayBatch, options Options) ([]*einoschema.Message, error) {
	projection, err := ProjectWithSources(batch, options)
	if err != nil {
		return nil, err
	}
	return projection.Messages, nil
}

// ProjectWithSources projects state-free messages while retaining the durable
// source ID for each output message.
func ProjectWithSources(batch session.ReplayBatch, options Options) (Projection, error) {
	var err error
	batch, err = applyEpoch(batch, options.Epoch)
	if err != nil {
		return Projection{}, err
	}
	partsByMessage := map[session.MessageID][]session.Part{}
	for _, part := range batch.Parts {
		partsByMessage[part.MessageID] = append(partsByMessage[part.MessageID], part)
	}
	result := Projection{Messages: make([]*einoschema.Message, 0, len(batch.Messages)), SourceMessageIDs: make([]session.MessageID, 0, len(batch.Messages))}
	for _, message := range batch.Messages {
		parts := partsByMessage[message.ID]
		sort.SliceStable(parts, func(i, j int) bool {
			return parts[i].Ordinal < parts[j].Ordinal
		})
		projected, err := projectMessage(message, parts, options)
		if err != nil {
			return Projection{}, err
		}
		result.Messages = append(result.Messages, projected...)
		for range projected {
			result.SourceMessageIDs = append(result.SourceMessageIDs, message.ID)
		}
	}
	return result, nil
}

// Load reads all replayable history for a session and projects it.
func Load(ctx context.Context, store session.Store, sessionID session.ID, options Options) ([]*einoschema.Message, error) {
	batch, err := LoadBatch(ctx, store, sessionID)
	if err != nil {
		return nil, err
	}
	return Project(batch, options)
}

func projectMessage(message session.Message, parts []session.Part, options Options) ([]*einoschema.Message, error) {
	limits := options.contentLimits()
	switch message.Role {
	case session.RoleSystem, session.RoleUser, session.RoleAssistant:
		projected := &einoschema.Message{
			Role: role(message.Role),
			Name: message.Agent,
		}
		// A user-role message that carries at least one media block projects
		// all of its ordinary content (text included) as ordered
		// UserInputMultiContent parts instead of the flat Content string, so
		// text/media order relative to each other is preserved. Text-only
		// user messages are unaffected and keep using Content.
		useMultiContent := message.Role == session.RoleUser && hasUserMediaPart(parts)
		var toolResultParts []session.Part
		for _, part := range parts {
			switch part.Kind {
			case session.PartFunctionToolResult:
				toolResultParts = append(toolResultParts, part)
			case session.PartReasoning:
				if !options.IncludeReasoning {
					continue
				}
				if hasContentSchemaField(part.Payload) {
					text, err := classicReasoningEnvelopeText(part, limits)
					if err != nil {
						return nil, err
					}
					projected.ReasoningContent += text
				} else {
					text, err := partText(part)
					if err != nil {
						return nil, err
					}
					projected.Content += text
				}
			case session.PartCompaction:
				text, err := partText(part)
				if err != nil {
					return nil, err
				}
				if useMultiContent {
					projected.UserInputMultiContent = append(projected.UserInputMultiContent, textInputPart(text))
				} else {
					projected.Content += text
				}
			case session.PartProviderState, session.PartApprovalDecision, session.PartResponseMeta:
				// ignored by classic projection
			case session.PartUserInputText, session.PartAssistantGenText:
				text, err := classicTextEnvelopeText(part, limits)
				if err != nil {
					return nil, err
				}
				if useMultiContent && part.Kind == session.PartUserInputText {
					projected.UserInputMultiContent = append(projected.UserInputMultiContent, textInputPart(text))
				} else {
					projected.Content += text
				}
			case session.PartUserInputImage, session.PartUserInputAudio, session.PartUserInputVideo, session.PartUserInputFile:
				if message.Role != session.RoleUser {
					// Assistant-role media stays unsupported; only user-role
					// media has a classic projection.
					return nil, fmt.Errorf("part %s: %w: kind %q", part.ID, ErrClassicUnsupported, part.Kind)
				}
				inputPart, err := classicMediaEnvelopeInputPart(part, limits)
				if err != nil {
					return nil, err
				}
				projected.UserInputMultiContent = append(projected.UserInputMultiContent, inputPart)
			case session.PartFunctionToolCall:
				toolCall, err := classicFunctionToolCallEnvelope(part, limits)
				if err != nil {
					return nil, err
				}
				projected.ToolCalls = append(projected.ToolCalls, toolCall)
			default:
				if _, ok := session.BlockKindForPart(part.Kind); ok {
					return nil, fmt.Errorf("part %s: %w: kind %q", part.ID, ErrClassicUnsupported, part.Kind)
				}
				return nil, fmt.Errorf("part %s: unsupported part kind %q", part.ID, part.Kind)
			}
		}
		// A user-role message whose parts are entirely tool-result parts
		// (the durable shape a settled function_tool_result now uses: role
		// user, one function_tool_result/tool_result part, nothing else)
		// has nothing left to say through the primary projected message
		// once its content is diverted to toolResultParts below; skip
		// emitting that now-empty placeholder so a tool result still
		// projects to exactly one classic message, matching the classic
		// RoleTool shape it replaces. This is scoped to RoleUser
		// specifically so an assistant- or system-role message that mixes
		// (legacy) tool-result parts with otherwise no content keeps its
		// prior placeholder-message behavior.
		result := []*einoschema.Message{}
		if message.Role != session.RoleUser || len(parts) == 0 || len(toolResultParts) != len(parts) {
			result = append(result, projected)
		}
		toolMessages, err := projectToolResults(toolResultParts, limits)
		if err != nil {
			return nil, err
		}
		result = append(result, toolMessages...)
		return result, nil
	case session.RoleTool:
		return projectToolResults(parts, limits)
	default:
		return nil, fmt.Errorf("unsupported session role %q", message.Role)
	}
}

// hasUserMediaPart reports whether parts contains at least one durable
// user-input media block (image, audio, video, or file).
func hasUserMediaPart(parts []session.Part) bool {
	for _, part := range parts {
		switch part.Kind {
		case session.PartUserInputImage, session.PartUserInputAudio, session.PartUserInputVideo, session.PartUserInputFile:
			return true
		}
	}
	return false
}

func textInputPart(text string) einoschema.MessageInputPart {
	return einoschema.MessageInputPart{Type: einoschema.ChatMessagePartTypeText, Text: text}
}

func projectToolResults(parts []session.Part, limits session.ContentLimits) ([]*einoschema.Message, error) {
	result := []*einoschema.Message{}
	for _, part := range parts {
		switch part.Kind {
		case session.PartFunctionToolResult:
			toolCallID, content, err := classicFunctionToolResultEnvelope(part, limits)
			if err != nil {
				return nil, err
			}
			result = append(result, einoschema.ToolMessage(content, toolCallID))
		default:
			if _, ok := session.BlockKindForPart(part.Kind); ok {
				return nil, fmt.Errorf("part %s: %w: kind %q", part.ID, ErrClassicUnsupported, part.Kind)
			}
			// legacy non-result kinds on a tool message remain ignored by design.
		}
	}
	return result, nil
}

// hasContentSchemaField reports whether raw carries the durable content
// envelope's top-level "schema" field. PartReasoning is the one PartKind
// shared between the legacy free-text payload ({"text": "..."}) and the new
// BlockKindReasoning envelope, so callers use this probe to tell them apart
// before decoding.
func hasContentSchemaField(raw json.RawMessage) bool {
	var probe struct {
		Schema *int `json:"schema"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false
	}
	return probe.Schema != nil
}

// decodeSingleBlock decodes one durable content-block Part payload through
// session.DecodeContentParts by presenting it as a synthetic single-part,
// single-block Content of decodeRole. decodeRole only needs to be a role that
// permits kind; it need not match the durable message's actual role, since
// classic projection tolerates rich content on any message role.
func decodeSingleBlock(part session.Part, kind session.BlockKind, decodeRole session.Role, limits session.ContentLimits) (session.ContentBlock, error) {
	synthetic := part
	synthetic.Ordinal = 0
	content, err := session.DecodeContentParts(decodeRole, []session.Part{synthetic}, limits)
	if err != nil {
		return session.ContentBlock{}, fmt.Errorf("part %s payload: %w", part.ID, err)
	}
	if len(content.Blocks) != 1 || content.Blocks[0].Kind != kind {
		return session.ContentBlock{}, fmt.Errorf("part %s: unexpected decoded block", part.ID)
	}
	return content.Blocks[0], nil
}

func classicTextEnvelopeText(part session.Part, limits session.ContentLimits) (string, error) {
	var kind session.BlockKind
	var decodeRole session.Role
	switch part.Kind {
	case session.PartUserInputText:
		kind, decodeRole = session.BlockKindUserInputText, session.RoleUser
	case session.PartAssistantGenText:
		kind, decodeRole = session.BlockKindAssistantGenText, session.RoleAssistant
	default:
		return "", fmt.Errorf("part %s: unsupported text kind %q", part.ID, part.Kind)
	}
	block, err := decodeSingleBlock(part, kind, decodeRole, limits)
	if err != nil {
		return "", err
	}
	if block.Text == nil {
		return "", fmt.Errorf("part %s: text block missing payload", part.ID)
	}
	return block.Text.Text, nil
}

func classicReasoningEnvelopeText(part session.Part, limits session.ContentLimits) (string, error) {
	block, err := decodeSingleBlock(part, session.BlockKindReasoning, session.RoleAssistant, limits)
	if err != nil {
		return "", err
	}
	if block.Reasoning == nil {
		return "", fmt.Errorf("part %s: reasoning block missing payload", part.ID)
	}
	return block.Reasoning.Text, nil
}

func classicFunctionToolCallEnvelope(part session.Part, limits session.ContentLimits) (einoschema.ToolCall, error) {
	block, err := decodeSingleBlock(part, session.BlockKindFunctionToolCall, session.RoleAssistant, limits)
	if err != nil {
		return einoschema.ToolCall{}, err
	}
	if block.FunctionCall == nil {
		return einoschema.ToolCall{}, fmt.Errorf("part %s: function_tool_call block missing payload", part.ID)
	}
	fc := block.FunctionCall
	return einoschema.ToolCall{
		ID:   fc.CallID,
		Type: "function",
		Function: einoschema.FunctionCall{
			Name:      fc.Name,
			Arguments: fc.Arguments,
		},
	}, nil
}

func classicFunctionToolResultEnvelope(part session.Part, limits session.ContentLimits) (string, string, error) {
	block, err := decodeSingleBlock(part, session.BlockKindFunctionToolResult, session.RoleUser, limits)
	if err != nil {
		return "", "", err
	}
	if block.FunctionResult == nil {
		return "", "", fmt.Errorf("part %s: function_tool_result block missing payload", part.ID)
	}
	fr := block.FunctionResult
	var content string
	for _, item := range fr.Content {
		if item.Type != session.ResultContentText {
			return "", "", fmt.Errorf("part %s: %w: non-text function tool result content %q", part.ID, ErrClassicUnsupported, item.Type)
		}
		content += item.Text
	}
	return fr.CallID, content, nil
}

// mediaPartBlockKind maps a durable user-input media PartKind to its
// BlockKind for decoding.
func mediaPartBlockKind(kind session.PartKind) (session.BlockKind, bool) {
	switch kind {
	case session.PartUserInputImage:
		return session.BlockKindUserInputImage, true
	case session.PartUserInputAudio:
		return session.BlockKindUserInputAudio, true
	case session.PartUserInputVideo:
		return session.BlockKindUserInputVideo, true
	case session.PartUserInputFile:
		return session.BlockKindUserInputFile, true
	default:
		return "", false
	}
}

// classicMediaEnvelopeInputPart decodes one durable user-input media Part
// into the schema.MessageInputPart shape schema.Message.UserInputMultiContent
// expects. There is no classic *schema.Message ingest path to mirror here
// (durable content is admitted through the agentic *schema.AgenticMessage
// contract in session/content.go, e.g. mediaToUserInputImage and friends);
// this is the sole *schema.Message-facing projection of a durable media
// block.
func classicMediaEnvelopeInputPart(part session.Part, limits session.ContentLimits) (einoschema.MessageInputPart, error) {
	blockKind, ok := mediaPartBlockKind(part.Kind)
	if !ok {
		return einoschema.MessageInputPart{}, fmt.Errorf("part %s: unsupported media kind %q", part.ID, part.Kind)
	}
	block, err := decodeSingleBlock(part, blockKind, session.RoleUser, limits)
	if err != nil {
		return einoschema.MessageInputPart{}, err
	}
	if block.Media == nil {
		return einoschema.MessageInputPart{}, fmt.Errorf("part %s: media block missing payload", part.ID)
	}
	m := block.Media
	common := einoschema.MessagePartCommon{MIMEType: m.MIMEType}
	if m.URL != "" {
		url := m.URL
		common.URL = &url
	}
	if m.Base64Data != "" {
		data := m.Base64Data
		common.Base64Data = &data
	}
	switch blockKind {
	case session.BlockKindUserInputImage:
		return einoschema.MessageInputPart{
			Type:  einoschema.ChatMessagePartTypeImageURL,
			Image: &einoschema.MessageInputImage{MessagePartCommon: common, Detail: einoschema.ImageURLDetail(m.Detail)},
		}, nil
	case session.BlockKindUserInputAudio:
		return einoschema.MessageInputPart{
			Type:  einoschema.ChatMessagePartTypeAudioURL,
			Audio: &einoschema.MessageInputAudio{MessagePartCommon: common},
		}, nil
	case session.BlockKindUserInputVideo:
		return einoschema.MessageInputPart{
			Type:  einoschema.ChatMessagePartTypeVideoURL,
			Video: &einoschema.MessageInputVideo{MessagePartCommon: common},
		}, nil
	case session.BlockKindUserInputFile:
		return einoschema.MessageInputPart{
			Type: einoschema.ChatMessagePartTypeFileURL,
			File: &einoschema.MessageInputFile{MessagePartCommon: common, Name: m.Name},
		}, nil
	default:
		return einoschema.MessageInputPart{}, fmt.Errorf("part %s: unsupported media kind %q", part.ID, part.Kind)
	}
}

func applyEpoch(batch session.ReplayBatch, epoch *session.ContextEpoch) (session.ReplayBatch, error) {
	if epoch == nil {
		return batch, nil
	}
	if epoch.SummaryMessageID == "" && epoch.TailStartID == "" {
		return batch, nil
	}
	partOwners, err := session.ResolveReplayPartOwners(batch.Parts, batch.PartOwnerMessageIDs)
	if err != nil {
		return session.ReplayBatch{}, err
	}
	include := map[session.MessageID]bool{}
	byID := map[session.MessageID]session.Message{}
	for _, message := range batch.Messages {
		byID[message.ID] = message
	}
	messages := make([]session.Message, 0, len(batch.Messages))
	// An active epoch only ever narrows the conversational body (replacing
	// the summarized range with its summary, retaining the tail); it never
	// drops the session's leading system-message prefix -- host-injected
	// system instructions are not conversational history to compact away.
	for _, message := range batch.Messages {
		if message.Role != session.RoleSystem {
			break
		}
		if !include[message.ID] {
			messages = append(messages, message)
			include[message.ID] = true
		}
	}
	if epoch.SummaryMessageID != "" {
		if summary, ok := byID[epoch.SummaryMessageID]; ok && !include[summary.ID] {
			messages = append(messages, summary)
			include[summary.ID] = true
		}
	}
	// tailAnchor is where the verbatim tail begins: TailStartID when the
	// epoch retained one, or the boundary/summary message's own ID when it
	// did not (retainTail == 0). In the latter case nothing existing at
	// epoch-creation time is retained verbatim, but every message produced
	// AFTER the boundary (a later turn's own conversation, continuing past
	// the compaction point) must still flow through -- an empty TailStartID
	// must never mean "nothing after this epoch is ever included again".
	tailAnchor := epoch.TailStartID
	if tailAnchor == "" {
		tailAnchor = epoch.SummaryMessageID
	}
	tailStarted := false
	for _, message := range batch.Messages {
		if message.ID == tailAnchor {
			tailStarted = true
			if tailAnchor == epoch.SummaryMessageID {
				// Already included above; the tail begins with whatever
				// comes after it, not the boundary message itself again.
				continue
			}
		}
		if tailStarted && !include[message.ID] {
			messages = append(messages, message)
			include[message.ID] = true
		}
	}
	parts := make([]session.Part, 0, len(batch.Parts))
	owners := make([]session.MessageID, 0, len(batch.Parts))
	for index, part := range batch.Parts {
		owner := partOwners[index]
		if include[owner] {
			parts = append(parts, part)
			owners = append(owners, owner)
		}
	}
	batch.Messages = messages
	batch.Parts = parts
	batch.PartOwnerMessageIDs = owners
	return batch, nil
}

func partText(part session.Part) (string, error) {
	switch part.Kind {
	case session.PartReasoning:
		payload, err := decodeCanonical[textPartPayload](part.Payload)
		if err != nil {
			return "", fmt.Errorf("part %s payload: %w", part.ID, err)
		}
		if payload.Text == nil {
			return "", fmt.Errorf("part %s payload: text required", part.ID)
		}
		return *payload.Text, nil
	case session.PartCompaction:
		payload, err := decodeCanonical[compactionPartPayload](part.Payload)
		if err != nil {
			return "", fmt.Errorf("part %s payload: %w", part.ID, err)
		}
		if payload.Text == nil || payload.EpochID == "" || payload.Redacted == nil {
			return "", fmt.Errorf("part %s payload: text, epoch_id, and redacted required", part.ID)
		}
		return *payload.Text, nil
	default:
		return "", fmt.Errorf("part %s: unsupported text payload kind %q", part.ID, part.Kind)
	}
}

type textPartPayload struct {
	Text *string `json:"text"`
}

type compactionPartPayload struct {
	Text             *string           `json:"text"`
	EpochID          session.EpochID   `json:"epoch_id"`
	SummarizedFromID session.MessageID `json:"summarized_from_id,omitempty"`
	SummarizedToID   session.MessageID `json:"summarized_to_id,omitempty"`
	TailStartID      session.MessageID `json:"tail_start_id,omitempty"`
	Redacted         *bool             `json:"redacted"`
}

func decodeCanonical[T any](raw json.RawMessage) (T, error) {
	var value T
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return value, fmt.Errorf("JSON object required")
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return value, fmt.Errorf("exactly one JSON value required")
		}
		return value, err
	}
	return value, nil
}

func role(role session.Role) einoschema.RoleType {
	switch role {
	case session.RoleSystem:
		return einoschema.System
	case session.RoleAssistant:
		return einoschema.Assistant
	default:
		return einoschema.User
	}
}
