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
) (TurnSnapshot, error) {
	request, err := (model.Request{Messages: messages}).Clone()
	if err != nil {
		return TurnSnapshot{}, err
	}
	return TurnSnapshot{
		RunID:        runID,
		SessionID:    sessionID,
		EpochID:      epochID,
		Config:       snapshot.Clone(),
		Model:        cloneResolved(resolved),
		Messages:     request.Messages,
		SystemPrompt: systemPrompt,
		CreatedAt:    now,
	}, nil
}

func cloneResolved(resolved model.Resolved) model.Resolved {
	next := resolved
	next.Provider.Environment = cloneSlice(resolved.Provider.Environment)
	next.Provider.Options = cloneStringMap(resolved.Provider.Options)
	next.Model.Capabilities = cloneBoolMap(resolved.Model.Capabilities)
	next.Model.Options = cloneStringMap(resolved.Model.Options)
	return next
}

func cloneBoolMap(src map[string]bool) map[string]bool {
	if src == nil {
		return nil
	}
	dst := make(map[string]bool, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
