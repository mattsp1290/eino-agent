package history

import (
	"context"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

// Projection pairs each provider-neutral message with its durable source ID.
// It never exposes provider-state parts or decoded provider bytes.
type Projection struct {
	Messages         []*einoschema.Message
	SourceMessageIDs []session.MessageID
}

// LoadBatch reads all replayable history pages without projecting content.
func LoadBatch(ctx context.Context, store session.Store, sessionID session.ID) (session.ReplayBatch, error) {
	cursor := session.ReplayCursor{Limit: 100}
	var messages []session.Message
	var parts []session.Part
	var partOwners []session.MessageID
	for {
		batch, err := store.ListMessages(ctx, sessionID, cursor)
		if err != nil {
			return session.ReplayBatch{}, err
		}
		owners, err := session.ResolveReplayPartOwners(batch.Parts, batch.PartOwnerMessageIDs)
		if err != nil {
			return session.ReplayBatch{}, err
		}
		messages = append(messages, batch.Messages...)
		parts = append(parts, batch.Parts...)
		partOwners = append(partOwners, owners...)
		if batch.Next == (session.ReplayCursor{}) {
			break
		}
		cursor = batch.Next
	}
	return session.ReplayBatch{Messages: messages, Parts: parts, PartOwnerMessageIDs: partOwners}, nil
}
