package sqlite

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) CreateSession(ctx context.Context, record session.Session) (session.Session, error) {
	var existing session.Session
	if err := s.getJSON(ctx, "SELECT record FROM sessions WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.Session{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.Session{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.Session{}, err
	}
	_, err = s.exec(ctx, `INSERT INTO sessions(id, record, updated_at) VALUES (?, ?, ?)`, record.ID, raw, timeText(record.UpdatedAt))
	return record, mapErr(err)
}

func (s *Store) GetSession(ctx context.Context, id session.ID) (session.Session, error) {
	var record session.Session
	err := s.getJSON(ctx, "SELECT record FROM sessions WHERE id = ?", []any{id}, &record)
	return record, err
}

func (s *Store) UpdateSession(ctx context.Context, record session.Session) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	result, err := s.exec(ctx, `UPDATE sessions SET record = ?, updated_at = ? WHERE id = ?`, raw, timeText(record.UpdatedAt), record.ID)
	if err != nil {
		return mapErr(err)
	}
	return rowsAffected(result)
}
