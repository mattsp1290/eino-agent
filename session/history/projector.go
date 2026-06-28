package history

import (
	"context"
	"encoding/json"
	"fmt"
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
					toolMessages, err := projectToolResults(message, []session.Part{part})
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
		return projectToolResults(message, parts)
	default:
		return nil, fmt.Errorf("unsupported session role %q", message.Role)
	}
}

func projectToolResults(message session.Message, parts []session.Part) ([]*einoschema.Message, error) {
	result := []*einoschema.Message{}
	for _, part := range parts {
		if part.Kind != session.PartToolResult {
			continue
		}
		toolCallID, content, err := decodeToolResult(part.Payload)
		if err != nil {
			return nil, err
		}
		if toolCallID == "" {
			toolCallID = string(message.ID)
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
	payload, err := decodeTextPayload(part.Payload)
	if err != nil {
		return "", fmt.Errorf("part %s payload: %w", part.ID, err)
	}
	return payload.Text, nil
}

type textPayload struct {
	Text       string          `json:"text"`
	Content    string          `json:"content"`
	ToolCallID string          `json:"tool_call_id"`
	Raw        json.RawMessage `json:"raw"`
}

type toolResultPayload struct {
	ToolCallID string          `json:"tool_call_id"`
	Status     string          `json:"status"`
	Content    string          `json:"content"`
	Structured json.RawMessage `json:"structured"`
	Truncated  bool            `json:"truncated"`
	Redacted   bool            `json:"redacted"`
}

func decodeToolResult(raw json.RawMessage) (string, string, error) {
	var output toolResultPayload
	if err := json.Unmarshal(raw, &output); err == nil && (output.ToolCallID != "" || output.Status != "" || len(output.Structured) > 0) {
		content := output.Content
		if content == "" && len(output.Structured) > 0 {
			content = string(output.Structured)
		}
		if output.Status != "" && output.Status != "completed" {
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
	payload, err := decodeTextPayload(raw)
	if err != nil {
		return "", "", err
	}
	return payload.ToolCallID, payload.Text, nil
}

func decodeTextPayload(raw json.RawMessage) (textPayload, error) {
	if len(raw) == 0 {
		return textPayload{}, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return textPayload{Text: text}, nil
	}
	var payload textPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return textPayload{}, err
	}
	if payload.Text == "" {
		payload.Text = payload.Content
	}
	if payload.Text == "" && len(payload.Raw) > 0 {
		payload.Text = string(payload.Raw)
	}
	return payload, nil
}

type toolCallPayload struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func decodeToolCall(raw json.RawMessage) (einoschema.ToolCall, error) {
	var payload toolCallPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return einoschema.ToolCall{}, err
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
