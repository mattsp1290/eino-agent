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
}

// Project converts durable session messages and parts into provider messages.
// It never reads runtime events, so live-only deltas are excluded by design.
func Project(batch session.ReplayBatch, options Options) ([]*einoschema.Message, error) {
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
			Role:    role(message.Role),
			Content: textContent(parts, options),
			Name:    message.Agent,
		}
		for _, part := range parts {
			if part.Kind != session.PartToolCall {
				continue
			}
			toolCall, err := decodeToolCall(part.Payload)
			if err != nil {
				return nil, err
			}
			projected.ToolCalls = append(projected.ToolCalls, toolCall)
		}
		return []*einoschema.Message{projected}, nil
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
		payload, err := decodeTextPayload(part.Payload)
		if err != nil {
			return nil, err
		}
		toolCallID := payload.ToolCallID
		if toolCallID == "" {
			toolCallID = string(message.ID)
		}
		result = append(result, einoschema.ToolMessage(payload.Text, toolCallID))
	}
	return result, nil
}

func textContent(parts []session.Part, options Options) string {
	content := ""
	for _, part := range parts {
		switch part.Kind {
		case session.PartText:
			content += mustText(part.Payload)
		case session.PartReasoning:
			if options.IncludeReasoning {
				content += mustText(part.Payload)
			}
		case session.PartState:
			if options.IncludeState {
				content += mustText(part.Payload)
			}
		case session.PartCompaction:
			content += mustText(part.Payload)
		}
	}
	return content
}

func mustText(raw json.RawMessage) string {
	payload, err := decodeTextPayload(raw)
	if err != nil {
		return ""
	}
	return payload.Text
}

type textPayload struct {
	Text       string          `json:"text"`
	Content    string          `json:"content"`
	ToolCallID string          `json:"tool_call_id"`
	Raw        json.RawMessage `json:"raw"`
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
