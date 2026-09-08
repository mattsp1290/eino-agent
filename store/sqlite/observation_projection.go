package sqlite

import (
	"context"

	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) validatePartOwner(ctx context.Context, p session.Part) error {
	var count int
	if err := s.queryRow(ctx, "SELECT COUNT(*) FROM messages WHERE id = ? AND session_id = ? AND run_id = ?", p.MessageID, p.SessionID, p.RunID).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return session.ErrConflict
	}
	return nil
}

func (e *executionStore) FinalizeAssistantMessage(ctx context.Context, id session.MessageID) error {
	return e.withFence(ctx, func(s *Store, run session.Run) error {
		result, err := s.exec(ctx, "UPDATE messages SET finalized = 1 WHERE id = ? AND session_id = ? AND run_id = ? AND role = ?", id, run.SessionID, run.ID, session.RoleAssistant)
		if err != nil {
			return mapErr(err)
		}
		return rowsAffected(result)
	})
}
