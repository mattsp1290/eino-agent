package history

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

// Options controls durable history projection.
type Options struct {
	IncludeReasoning bool
	IncludeState     bool
	Epoch            *session.ContextEpoch
}

// Project converts durable session messages and parts into provider messages.
// It never reads runtime events, so live-only deltas are excluded by design.
func Project(batch session.ReplayBatch, options Options) ([]*einoschema.Message, error) {
	batch = applyEpoch(batch, options.Epoch)
	partsByMessage := map[session.MessageID][]session.Part{}
	for _, part := range batch.Parts {
		partsByMessage[part.MessageID] = append(partsByMessage[part.MessageID], part)
	}
	result := make([]*einoschema.Message, 0, len(batch.Messages))
	for _, message := range batch.Messages {
		parts := partsByMessage[message.ID]
		sort.SliceStable(parts, func(i, j int) bool {
			return parts[i].Ordinal < parts[j].Ordinal
		})
		projected, err := projectMessage(message, parts, options)
		if err != nil {
			return nil, err
		}
		result = append(result, projected...)
	}
	return result, nil
}

// Load reads all replayable history for a session and projects it.
func Load(ctx context.Context, store session.Store, sessionID session.ID, options Options) ([]*einoschema.Message, error) {
	cursor := session.ReplayCursor{Limit: 100}
	var messages []session.Message
	var parts []session.Part
	for {
		batch, err := store.ListMessages(ctx, sessionID, cursor)
		if err != nil {
			return nil, err
		}
		messages = append(messages, batch.Messages...)
		parts = append(parts, batch.Parts...)
		if batch.Next == (session.ReplayCursor{}) {
			break
		}
		cursor = batch.Next
	}
	return Project(session.ReplayBatch{Messages: messages, Parts: parts}, options)
}

func projectMessage(message session.Message, parts []session.Part, options Options) ([]*einoschema.Message, error) {
	switch message.Role {
	case session.RoleSystem, session.RoleUser, session.RoleAssistant:
		projected := &einoschema.Message{
			Role: role(message.Role),
			Name: message.Agent,
		}
		content, err := textContent(parts, options)
		if err != nil {
			return nil, err
		}
		projected.Content = content
		result := []*einoschema.Message{projected}
		for _, part := range parts {
			if part.Kind != session.PartToolCall {
				if part.Kind == session.PartToolResult {
					toolMessages, err := projectToolResults([]session.Part{part})
					if err != nil {
						return nil, err
					}
					result = append(result, toolMessages...)
				}
				continue
			}
			toolCall, err := decodeToolCall(part.Payload)
			if err != nil {
				return nil, err
			}
			projected.ToolCalls = append(projected.ToolCalls, toolCall)
		}
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
		if part.Kind != session.PartToolResult {
			continue
		}
		toolCallID, content, err := decodeToolResult(part.Payload)
		if err != nil {
			return nil, err
		}
		result = append(result, einoschema.ToolMessage(content, toolCallID))
	}
	return result, nil
}

func applyEpoch(batch session.ReplayBatch, epoch *session.ContextEpoch) session.ReplayBatch {
	if epoch == nil {
		return batch
	}
	if epoch.SummaryMessageID == "" && epoch.TailStartID == "" {
		return batch
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
	for _, part := range batch.Parts {
		if include[part.MessageID] {
			parts = append(parts, part)
		}
	}
	batch.Messages = messages
	batch.Parts = parts
	return batch
}

func textContent(parts []session.Part, options Options) (string, error) {
	content := ""
	for _, part := range parts {
		switch part.Kind {
		case session.PartText:
			text, err := partText(part)
			if err != nil {
				return "", err
			}
			content += text
		case session.PartReasoning:
			if options.IncludeReasoning {
				text, err := partText(part)
				if err != nil {
					return "", err
				}
				content += text
			}
		case session.PartState:
			if options.IncludeState {
				text, err := partText(part)
				if err != nil {
					return "", err
				}
				content += text
			}
		case session.PartCompaction:
			text, err := partText(part)
			if err != nil {
				return "", err
			}
			content += text
		}
	}
	return content, nil
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
