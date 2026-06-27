package runtime

import (
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// FreezeTurnSnapshot clones config and message containers at run admission so
// later config reloads or caller mutations cannot affect an in-flight run.
func FreezeTurnSnapshot(
	runID session.RunID,
	sessionID session.ID,
	epochID session.EpochID,
	snapshot config.Snapshot,
	resolved model.Resolved,
	messages []*einoschema.Message,
	systemPrompt string,
	now time.Time,
) TurnSnapshot {
	return TurnSnapshot{
		RunID:        runID,
		SessionID:    sessionID,
		EpochID:      epochID,
		Config:       snapshot.Clone(),
		Model:        resolved,
		Messages:     cloneMessages(messages),
		SystemPrompt: systemPrompt,
		CreatedAt:    now,
	}
}

func cloneMessages(src []*einoschema.Message) []*einoschema.Message {
	if src == nil {
		return nil
	}
	dst := make([]*einoschema.Message, len(src))
	for i, msg := range src {
		if msg == nil {
			continue
		}
		clone := *msg
		clone.MultiContent = cloneSlice(msg.MultiContent)
		clone.UserInputMultiContent = cloneSlice(msg.UserInputMultiContent)
		clone.AssistantGenMultiContent = cloneSlice(msg.AssistantGenMultiContent)
		clone.ToolCalls = cloneSlice(msg.ToolCalls)
		if msg.ResponseMeta != nil {
			responseMeta := *msg.ResponseMeta
			clone.ResponseMeta = &responseMeta
		}
		clone.Extra = cloneAnyMap(msg.Extra)
		dst[i] = &clone
	}
	return dst
}

func cloneAnyMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
