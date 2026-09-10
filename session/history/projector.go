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
	IncludeState     bool
	Epoch            *session.ContextEpoch
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
	switch message.Role {
	case session.RoleSystem, session.RoleUser, session.RoleAssistant:
		projected := &einoschema.Message{
			Role: role(message.Role),
			Name: message.Agent,
		}
		var toolResultParts []session.Part
		for _, part := range parts {
			switch part.Kind {
			case session.PartToolCall:
				toolCall, err := decodeToolCall(part.Payload)
				if err != nil {
					return nil, err
				}
				projected.ToolCalls = append(projected.ToolCalls, toolCall)
			case session.PartToolResult, session.PartFunctionToolResult:
				toolResultParts = append(toolResultParts, part)
			case session.PartText:
				text, err := partText(part)
				if err != nil {
					return nil, err
				}
				projected.Content += text
			case session.PartReasoning:
				if !options.IncludeReasoning {
					continue
				}
				if hasContentSchemaField(part.Payload) {
					text, err := classicReasoningEnvelopeText(part)
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
			case session.PartState:
				if !options.IncludeState {
					continue
				}
				text, err := partText(part)
				if err != nil {
					return nil, err
				}
				projected.Content += text
			case session.PartCompaction:
				text, err := partText(part)
				if err != nil {
					return nil, err
				}
				projected.Content += text
			case session.PartFile, session.PartStep, session.PartProviderState, session.PartResponseMeta:
				// ignored by classic projection
			case session.PartUserInputText, session.PartAssistantGenText:
				text, err := classicTextEnvelopeText(part)
				if err != nil {
					return nil, err
				}
				projected.Content += text
			case session.PartFunctionToolCall:
				toolCall, err := classicFunctionToolCallEnvelope(part)
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
		result := []*einoschema.Message{projected}
		toolMessages, err := projectToolResults(toolResultParts)
		if err != nil {
			return nil, err
		}
		result = append(result, toolMessages...)
		return result, nil
	case session.RoleTool:
		return projectToolResults(parts)
	default:
		return nil, fmt.Errorf("unsupported session role %q", message.Role)
	}
}

func projectToolResults(parts []session.Part) ([]*einoschema.Message, error) {
	result := []*einoschema.Message{}
	for _, part := range parts {
		switch part.Kind {
		case session.PartToolResult:
			toolCallID, content, err := decodeToolResult(part.Payload)
			if err != nil {
				return nil, err
			}
			result = append(result, einoschema.ToolMessage(content, toolCallID))
		case session.PartFunctionToolResult:
			toolCallID, content, err := classicFunctionToolResultEnvelope(part)
			if err != nil {
				return nil, err
			}
			result = append(result, einoschema.ToolMessage(content, toolCallID))
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
func decodeSingleBlock(part session.Part, kind session.BlockKind, decodeRole session.Role) (session.ContentBlock, error) {
	synthetic := part
	synthetic.Ordinal = 0
	content, err := session.DecodeContentParts(decodeRole, []session.Part{synthetic}, session.DefaultContentLimits())
	if err != nil {
		return session.ContentBlock{}, fmt.Errorf("part %s payload: %w", part.ID, err)
	}
	if len(content.Blocks) != 1 || content.Blocks[0].Kind != kind {
		return session.ContentBlock{}, fmt.Errorf("part %s: unexpected decoded block", part.ID)
	}
	return content.Blocks[0], nil
}

func classicTextEnvelopeText(part session.Part) (string, error) {
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
	block, err := decodeSingleBlock(part, kind, decodeRole)
	if err != nil {
		return "", err
	}
	if block.Text == nil {
		return "", fmt.Errorf("part %s: text block missing payload", part.ID)
	}
	return block.Text.Text, nil
}

func classicReasoningEnvelopeText(part session.Part) (string, error) {
	block, err := decodeSingleBlock(part, session.BlockKindReasoning, session.RoleAssistant)
	if err != nil {
		return "", err
	}
	if block.Reasoning == nil {
		return "", fmt.Errorf("part %s: reasoning block missing payload", part.ID)
	}
	return block.Reasoning.Text, nil
}

func classicFunctionToolCallEnvelope(part session.Part) (einoschema.ToolCall, error) {
	block, err := decodeSingleBlock(part, session.BlockKindFunctionToolCall, session.RoleAssistant)
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

func classicFunctionToolResultEnvelope(part session.Part) (string, string, error) {
	block, err := decodeSingleBlock(part, session.BlockKindFunctionToolResult, session.RoleUser)
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
	if epoch.SummaryMessageID != "" {
		if summary, ok := byID[epoch.SummaryMessageID]; ok {
			messages = append(messages, summary)
			include[summary.ID] = true
		}
	}
	tailStarted := false
	for _, message := range batch.Messages {
		if message.ID == epoch.TailStartID {
			tailStarted = true
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
	case session.PartText, session.PartReasoning, session.PartState:
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

type toolResultPayload struct {
	ToolCallID   string          `json:"tool_call_id"`
	Status       string          `json:"status"`
	Content      string          `json:"content,omitempty"`
	Structured   json.RawMessage `json:"structured,omitempty"`
	Truncated    bool            `json:"truncated,omitempty"`
	OriginalSize int64           `json:"original_size,omitempty"`
	InlineSize   int64           `json:"inline_size,omitempty"`
	External     bool            `json:"external,omitempty"`
	Redacted     bool            `json:"redacted,omitempty"`
}

func decodeToolResult(raw json.RawMessage) (string, string, error) {
	output, err := decodeCanonical[toolResultPayload](raw)
	if err != nil {
		return "", "", err
	}
	if output.ToolCallID == "" {
		return "", "", fmt.Errorf("tool_call_id required")
	}
	switch output.Status {
	case "completed", "expected_failure", "operational_failure", "interrupted":
	default:
		return "", "", fmt.Errorf("unsupported tool result status %q", output.Status)
	}
	if len(output.Structured) > 0 && !json.Valid(output.Structured) {
		return "", "", fmt.Errorf("structured must contain a JSON value")
	}
	content := output.Content
	if content == "" && len(output.Structured) > 0 {
		content = string(output.Structured)
	}
	if output.Status != "completed" {
		status, _ := json.Marshal(map[string]any{
			"status":    output.Status,
			"content":   content,
			"truncated": output.Truncated,
			"redacted":  output.Redacted,
		})
		content = string(status)
	}
	return output.ToolCallID, content, nil
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

type toolCallPayload struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func decodeToolCall(raw json.RawMessage) (einoschema.ToolCall, error) {
	payload, err := decodeCanonical[toolCallPayload](raw)
	if err != nil {
		return einoschema.ToolCall{}, err
	}
	if payload.ID == "" || payload.Name == "" || len(payload.Arguments) == 0 {
		return einoschema.ToolCall{}, fmt.Errorf("tool call id, name, and arguments required")
	}
	return einoschema.ToolCall{
		ID:   payload.ID,
		Type: "function",
		Function: einoschema.FunctionCall{
			Name:      payload.Name,
			Arguments: string(payload.Arguments),
		},
	}, nil
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
