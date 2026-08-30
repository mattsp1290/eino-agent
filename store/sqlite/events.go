package sqlite

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) appendEvent(ctx context.Context, record session.EventRecord) (session.EventRecord, error) {
	var existing session.EventRecord
	if err := s.getJSON(ctx, "SELECT record FROM events WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.EventRecord{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.EventRecord{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.EventRecord{}, err
	}
	var toolCallID any
	var transition any
	if record.ToolTransition != "" {
		toolCallID = record.ToolCallID
		transition = record.ToolTransition
	}
	_, err = s.exec(ctx, `INSERT INTO events(id, session_id, run_id, kind, tool_call_id, tool_transition, record, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID, record.SessionID, record.RunID, record.Kind, toolCallID, transition, raw, timeText(record.CreatedAt))
	return record, mapErr(err)
}

func (s *Store) ListEvents(ctx context.Context, sessionID session.ID, cursor session.EventCursor) (session.EventBatch, error) {
	limit := cursor.Limit
	if limit <= 0 {
		limit = 100
	}
	args := []any{sessionID}
	where := "session_id = ?"
	if cursor.AfterEventID != "" {
		var after session.EventRecord
		if err := s.getJSON(ctx, "SELECT record FROM events WHERE id = ?", []any{cursor.AfterEventID}, &after); err != nil {
			return session.EventBatch{}, err
		}
		where += " AND (created_at > ? OR (created_at = ? AND id > ?))"
		args = append(args, timeText(after.CreatedAt), timeText(after.CreatedAt), cursor.AfterEventID)
	}
	args = append(args, limit+1)
	events, err := listJSON[session.EventRecord](ctx, s, `SELECT record FROM events WHERE `+where+` ORDER BY created_at, id LIMIT ?`, args...)
	if err != nil {
		return session.EventBatch{}, err
	}
	next := session.EventCursor{}
	if len(events) > limit {
		last := events[limit-1]
		next = session.EventCursor{AfterEventID: last.ID, Limit: limit}
		events = events[:limit]
	}
	return session.EventBatch{Events: events, Next: next}, nil
}
