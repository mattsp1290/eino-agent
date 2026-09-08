package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/internal/sqlstore"
)

func (s *Store) CreateSession(ctx context.Context, record session.Session) (session.Session, error) {
	if !validSessionProjection(record) {
		return session.Session{}, session.ErrConflict
	}
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
	_, err = s.exec(ctx, `INSERT INTO sessions(id, record, workspace_id, title, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`, record.ID, raw, record.WorkspaceID, record.Title, sqlstore.TimeText(record.CreatedAt), sqlstore.TimeText(record.UpdatedAt))
	return record, mapErr(err)
}

func (s *Store) GetSession(ctx context.Context, id session.ID) (session.Session, error) {
	var record session.Session
	err := s.getJSON(ctx, "SELECT record FROM sessions WHERE id = ?", []any{id}, &record)
	return record, err
}

func (s *Store) UpdateSession(ctx context.Context, record session.Session) error {
	if !validSessionProjection(record) {
		return session.ErrConflict
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	result, err := s.exec(ctx, `UPDATE sessions SET record = ?, workspace_id = ?, title = ?, created_at = ?, updated_at = ? WHERE id = ?`, raw, record.WorkspaceID, record.Title, sqlstore.TimeText(record.CreatedAt), sqlstore.TimeText(record.UpdatedAt), record.ID)
	if err != nil {
		return mapErr(err)
	}
	return rowsAffected(result)
}

// Storage accepts large and empty scalars; only discovery imposes byte ceilings.
func validSessionProjection(s session.Session) bool {
	for _, value := range []string{string(s.ID), s.WorkspaceID, s.Title} {
		if !utf8.ValidString(value) {
			return false
		}
	}
	for _, value := range []time.Time{s.CreatedAt, s.UpdatedAt} {
		if !value.IsZero() && (value.UTC().Year() < 0 || value.UTC().Year() > 9999) {
			return false
		}
	}
	return true
}
