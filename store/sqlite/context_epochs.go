package sqlite

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) startContextEpoch(ctx context.Context, record session.ContextEpoch) (session.ContextEpoch, error) {
	var existing session.ContextEpoch
	if err := s.getJSON(ctx, "SELECT record FROM context_epochs WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.ContextEpoch{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.ContextEpoch{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.ContextEpoch{}, err
	}
	_, err = s.exec(ctx, `INSERT INTO context_epochs(id, session_id, record, closed_at) VALUES (?, ?, ?, ?)`, record.ID, record.SessionID, raw, timeText(record.ClosedAt))
	if constraintFailed(err) {
		var reread session.ContextEpoch
		if getErr := s.getJSON(ctx, "SELECT record FROM context_epochs WHERE id = ?", []any{record.ID}, &reread); getErr == nil && sameRecord(reread, record) {
			return reread, nil
		}
	}
	return record, mapErr(err)
}

func (s *Store) finishContextEpoch(ctx context.Context, record session.ContextEpoch) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	result, err := s.exec(ctx, `UPDATE context_epochs SET record = ?, closed_at = ? WHERE id = ?`, raw, timeText(record.ClosedAt), record.ID)
	if err != nil {
		return mapErr(err)
	}
	return rowsAffected(result)
}

func (s *Store) ListContextEpochs(ctx context.Context, sessionID session.ID) ([]session.ContextEpoch, error) {
	epochs, err := listJSON[session.ContextEpoch](ctx, s, `SELECT record FROM context_epochs WHERE session_id = ? ORDER BY json_extract(record, '$.CreatedAt'), id`, sessionID)
	if err != nil {
		return nil, err
	}
	return epochs, nil
}
