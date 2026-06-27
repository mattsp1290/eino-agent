package runtime

import (
	"context"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

// LoadHistory projects durable session history into provider messages.
func LoadHistory(ctx context.Context, store session.Store, sessionID session.ID, options history.Options) ([]*einoschema.Message, error) {
	return history.Load(ctx, store, sessionID, options)
}
