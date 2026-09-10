package runtime

import (
	"context"

	"github.com/mattsp1290/eino-agent/session"
)

// SessionTitleWriter changes only the current conversation's display title.
// It is bound to the immutable execution fence and host-selected workspace.
type SessionTitleWriter interface {
	SetTitle(context.Context, string) (session.SessionTitleResult, error)
}

type boundSessionTitleWriter struct {
	store       session.ExecutionStore
	sessionID   session.ID
	workspaceID string
}

func (w boundSessionTitleWriter) SetTitle(ctx context.Context, title string) (session.SessionTitleResult, error) {
	if w.store == nil || w.sessionID == "" {
		return session.SessionTitleResult{}, session.ErrConflict
	}
	return w.store.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: w.sessionID, WorkspaceID: w.workspaceID, Title: title})
}
