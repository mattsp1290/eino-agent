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
		Messages:     cloneSlice(messages),
		SystemPrompt: systemPrompt,
		CreatedAt:    now,
	}
}
