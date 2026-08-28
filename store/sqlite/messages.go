package sqlite

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) appendMessage(ctx context.Context, record session.Message) (session.Message, error) {
	var existing session.Message
	if err := s.getJSON(ctx, "SELECT record FROM messages WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.Message{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.Message{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.Message{}, err
	}
	_, err = s.exec(ctx, `INSERT INTO messages(id, session_id, run_id, role, record, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		record.ID, record.SessionID, record.RunID, record.Role, raw, timeText(record.CreatedAt))
	return record, mapErr(err)
}

func (s *Store) appendPart(ctx context.Context, record session.Part) (session.Part, error) {
	var existing session.Part
	if err := s.getJSON(ctx, "SELECT record FROM parts WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.Part{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.Part{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.Part{}, err
	}
	_, err = s.exec(ctx, `INSERT INTO parts(id, message_id, session_id, run_id, ordinal, record, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		record.ID, record.MessageID, record.SessionID, record.RunID, record.Ordinal, raw, timeText(record.CreatedAt))
	return record, mapErr(err)
}

func (s *Store) updatePart(ctx context.Context, record session.Part) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	result, err := s.exec(ctx, `UPDATE parts SET ordinal = ?, record = ? WHERE id = ?`, record.Ordinal, raw, record.ID)
	if err != nil {
		return mapErr(err)
	}
	return rowsAffected(result)
}

func (s *Store) ListMessages(ctx context.Context, sessionID session.ID, cursor session.ReplayCursor) (session.ReplayBatch, error) {
	limit := cursor.Limit
	if limit <= 0 {
		limit = 100
	}
	args := []any{sessionID}
	where := "session_id = ?"
	if cursor.AfterMessageID != "" {
		after, err := s.GetMessage(ctx, cursor.AfterMessageID)
		if err != nil {
			return session.ReplayBatch{}, err
		}
		where += " AND (created_at > ? OR (created_at = ? AND id > ?))"
		args = append(args, timeText(after.CreatedAt), timeText(after.CreatedAt), cursor.AfterMessageID)
	}
	args = append(args, limit+1)
	messages, err := listJSON[session.Message](ctx, s, `SELECT record FROM messages WHERE `+where+` ORDER BY created_at, id LIMIT ?`, args...)
	if err != nil {
		return session.ReplayBatch{}, err
	}
	next := session.ReplayCursor{}
	if len(messages) > limit {
		last := messages[limit-1]
		next = session.ReplayCursor{AfterMessageID: last.ID, Limit: limit}
		messages = messages[:limit]
	}
	parts, err := listJSON[session.Part](ctx, s, `SELECT p.record FROM parts p JOIN messages m ON p.message_id = m.id WHERE p.session_id = ? ORDER BY m.created_at, m.id, p.ordinal, p.id`, sessionID)
	if err != nil {
		return session.ReplayBatch{}, err
	}
	include := make(map[session.MessageID]bool, len(messages))
	for _, msg := range messages {
		include[msg.ID] = true
	}
	filtered := parts[:0]
	for _, part := range parts {
		if include[part.MessageID] {
			filtered = append(filtered, part)
		}
	}
	return session.ReplayBatch{Messages: messages, Parts: filtered, Next: next}, nil
}

func (s *Store) GetMessage(ctx context.Context, id session.MessageID) (session.Message, error) {
	var record session.Message
	err := s.getJSON(ctx, "SELECT record FROM messages WHERE id = ?", []any{id}, &record)
	return record, err
}
