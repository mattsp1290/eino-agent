package sqlstore

import (
	"context"
	"encoding/json"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) appendEvent(ctx context.Context, record session.EventRecord) (session.EventRecord, error) {
	if record.ID == "" || record.SessionID == "" || record.RunID == "" || record.ToolTransition != "" {
		return session.EventRecord{}, session.ErrConflict
	}
	return s.insertEvent(ctx, record, false)
}

// appendCanonicalEvent is reserved for typed lifecycle methods. Arbitrary
// AppendEvent cannot manufacture a run/tool lifecycle event.
func (s *Store) appendCanonicalEvent(ctx context.Context, record session.EventRecord) (session.EventRecord, error) {
	if record.ToolTransition == "" && record.Kind != session.RunSettlementEventKind {
		return session.EventRecord{}, session.ErrConflict
	}
	return s.insertEvent(ctx, record, true)
}

func (s *Store) insertEvent(ctx context.Context, record session.EventRecord, canonical bool) (session.EventRecord, error) {
	if !canonical && (record.Kind == session.RunSettlementEventKind || record.ToolTransition != "") {
		return session.EventRecord{}, session.ErrConflict
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.EventRecord{}, err
	}
	sessionKey, err := s.key(ctx, "sessions", string(record.SessionID))
	if err != nil {
		return session.EventRecord{}, relationError(err)
	}
	runKey, err := s.key(ctx, "runs", string(record.RunID))
	if err != nil {
		return session.EventRecord{}, relationError(err)
	}
	var toolKey int64
	if record.ToolTransition != "" {
		var keyErr error
		toolKey, keyErr = s.key(ctx, "tool_calls", string(record.ToolCallID))
		if keyErr != nil {
			return session.EventRecord{}, relationError(keyErr)
		}
	}
	row := map[string]any{
		"id": []byte(record.ID), "session_key": sessionKey, "run_key": runKey,
		"kind": []byte(record.Kind), "record": raw,
		"created_at": TimeText(record.CreatedAt),
	}
	if record.ToolTransition != "" {
		row["tool_key"] = toolKey
		row["tool_transition"] = string(record.ToolTransition)
	}
	db := s.dbFor(ctx).Table("events").Clauses(clause.OnConflict{DoNothing: true}).Create(row)
	if err := db.Error; err != nil {
		return session.EventRecord{}, s.mapErr(err)
	}
	if db.RowsAffected != 0 {
		return record, nil
	}
	// Conflict-do-nothing keeps the transaction usable. Reread by every
	// possible canonical identity before classifying a retry as a conflict.
	existing, readErr := s.eventByID(ctx, record.ID)
	if readErr == nil {
		if SameRecord(existing, record) {
			return existing, nil
		}
		return session.EventRecord{}, session.ErrConflict
	}
	if !errors.Is(readErr, session.ErrNotFound) {
		return session.EventRecord{}, readErr
	}
	if record.ToolTransition != "" {
		if existing, readErr = s.eventByToolTransition(ctx, toolKey, record.ToolTransition); readErr == nil {
			if SameRecord(existing, record) {
				return existing, nil
			}
			return session.EventRecord{}, session.ErrConflict
		} else if !errors.Is(readErr, session.ErrNotFound) {
			return session.EventRecord{}, readErr
		}
	} else if record.Kind == session.RunSettlementEventKind {
		if existing, readErr = s.eventByRunFinished(ctx, runKey, record.Kind); readErr == nil {
			if SameRecord(existing, record) {
				return existing, nil
			}
			return session.EventRecord{}, session.ErrConflict
		} else if !errors.Is(readErr, session.ErrNotFound) {
			return session.EventRecord{}, readErr
		}
	}
	return session.EventRecord{}, session.ErrConflict
}

func (s *Store) eventQuery(ctx context.Context) *gorm.DB {
	return s.dbFor(ctx).Table("events").Select("events.row_key, events.id, events.session_key, events.run_key, sessions.id AS session_id, runs.id AS run_id, events.tool_key, tool_calls.id AS tool_call_id, tool_calls.session_key AS tool_session_key, tool_calls.run_key AS tool_run_key, events.kind, events.tool_transition, events.record, events.created_at").Joins("JOIN sessions ON sessions.row_key = events.session_key").Joins("JOIN runs ON runs.row_key = events.run_key").Joins("LEFT JOIN tool_calls ON tool_calls.row_key = events.tool_key")
}

func (s *Store) eventByID(ctx context.Context, id session.EventID) (session.EventRecord, error) {
	var row eventRow
	err := s.eventQuery(ctx).Where("events.id = ?", []byte(id)).Take(&row).Error
	if err != nil {
		return session.EventRecord{}, translateReadError(err)
	}
	return decodeEventRow(row)
}

func (s *Store) eventByToolTransition(ctx context.Context, toolKey int64, transition session.ToolTransitionPhase) (session.EventRecord, error) {
	var row eventRow
	err := s.eventQuery(ctx).Where("events.tool_key = ? AND events.tool_transition = ?", toolKey, string(transition)).Take(&row).Error
	if err != nil {
		return session.EventRecord{}, translateReadError(err)
	}
	return decodeEventRow(row)
}

func (s *Store) eventByRunFinished(ctx context.Context, runKey int64, kind string) (session.EventRecord, error) {
	var row eventRow
	err := s.eventQuery(ctx).Where("events.run_key = ? AND events.kind = ?", runKey, []byte(kind)).Take(&row).Error
	if err != nil {
		return session.EventRecord{}, translateReadError(err)
	}
	return decodeEventRow(row)
}

func (s *Store) ListEvents(ctx context.Context, sessionID session.ID, cursor session.EventCursor) (session.EventBatch, error) {
	limit := cursor.Limit
	if limit <= 0 {
		limit = 100
	}
	sessionKey, err := s.key(ctx, "sessions", string(sessionID))
	if err != nil {
		return session.EventBatch{}, err
	}
	query := s.eventQuery(ctx).Where("events.session_key = ?", sessionKey)
	if cursor.AfterEventID != "" {
		after, err := s.eventByID(ctx, cursor.AfterEventID)
		if err != nil {
			return session.EventBatch{}, err
		}
		if after.SessionID != sessionID {
			return session.EventBatch{}, session.ErrConflict
		}
		query = query.Where("(events.created_at > ? OR (events.created_at = ? AND events.id > ?))", TimeText(after.CreatedAt), TimeText(after.CreatedAt), []byte(cursor.AfterEventID))
	}
	var rows []eventRow
	if err := query.Order("events.created_at, events.id").Limit(limit + 1).Find(&rows).Error; err != nil {
		return session.EventBatch{}, translateReadError(err)
	}
	values := make([]session.EventRecord, 0, len(rows))
	for _, row := range rows {
		value, err := decodeEventRow(row)
		if err != nil {
			return session.EventBatch{}, err
		}
		values = append(values, value)
	}
	next := session.EventCursor{}
	if len(values) > limit {
		next = session.EventCursor{AfterEventID: values[limit-1].ID, Limit: limit}
		values = values[:limit]
	}
	return session.EventBatch{Events: values, Next: next}, nil
}
