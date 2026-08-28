package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

const maxModelRequestRecordBytes = 4 << 20

func (s *Store) createModelRequest(ctx context.Context, record session.ModelRequestRecord) (session.ModelRequestRecord, error) {
	if record.ID == "" || record.RunID == "" || record.State != session.ModelRequestPrepared {
		return session.ModelRequestRecord{}, session.ErrConflict
	}
	var existing session.ModelRequestRecord
	if err := s.getJSON(ctx, "SELECT record FROM model_requests WHERE id = ?", []any{record.ID}, &existing); err == nil {
		if !sameRecord(existing, record) {
			return session.ModelRequestRecord{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(err, session.ErrNotFound) {
		return session.ModelRequestRecord{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.ModelRequestRecord{}, err
	}
	if len(raw) > maxModelRequestRecordBytes {
		return session.ModelRequestRecord{}, session.ErrModelRequestTooLarge
	}
	_, err = s.exec(ctx, `INSERT INTO model_requests(id, run_id, state, attempt, step, record, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, record.ID, record.RunID, record.State, record.Attempt, record.Step, raw, timeText(record.CreatedAt))
	return record, mapErr(err)
}

func (s *Store) updateModelRequest(ctx context.Context, record session.ModelRequestRecord) error {
	current, err := s.GetModelRequest(ctx, record.ID)
	if err != nil {
		return err
	}
	if !sameModelRequestIdentity(current, record) {
		return session.ErrConflict
	}
	if current.State == record.State {
		if sameRecord(current, record) {
			return nil
		}
		return session.ErrConflict
	}
	if !session.ValidModelRequestTransition(current.State, record.State) {
		return session.ErrConflict
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(raw) > maxModelRequestRecordBytes {
		return session.ErrModelRequestTooLarge
	}
	result, err := s.exec(ctx, `UPDATE model_requests SET state = ?, record = ? WHERE id = ? AND state = ?`, record.State, raw, record.ID, current.State)
	if err != nil {
		return mapErr(err)
	}
	return rowsAffected(result)
}

func sameModelRequestIdentity(left, right session.ModelRequestRecord) bool {
	left.State, right.State = "", ""
	left.ErrorCode, right.ErrorCode = "", ""
	left.UpdatedAt, right.UpdatedAt = time.Time{}, time.Time{}
	return sameRecord(left, right)
}

func (s *Store) GetModelRequest(ctx context.Context, id session.ModelRequestID) (session.ModelRequestRecord, error) {
	var record session.ModelRequestRecord
	err := s.getJSON(ctx, "SELECT record FROM model_requests WHERE id = ?", []any{id}, &record)
	return record, err
}

func (s *Store) ListModelRequests(ctx context.Context, runID session.RunID, cursor session.ModelRequestCursor) (session.ModelRequestBatch, error) {
	limit := cursor.Limit
	if limit <= 0 {
		limit = 100
	}
	where := "run_id = ?"
	args := []any{runID}
	if cursor.AfterID != "" {
		after, err := s.GetModelRequest(ctx, cursor.AfterID)
		if err != nil {
			return session.ModelRequestBatch{}, err
		}
		where += " AND (created_at > ? OR (created_at = ? AND id > ?))"
		args = append(args, timeText(after.CreatedAt), timeText(after.CreatedAt), cursor.AfterID)
	}
	args = append(args, limit+1)
	records, err := listJSON[session.ModelRequestRecord](ctx, s, `SELECT record FROM model_requests WHERE `+where+` ORDER BY created_at, id LIMIT ?`, args...)
	if err != nil {
		return session.ModelRequestBatch{}, err
	}
	next := session.ModelRequestCursor{}
	if len(records) > limit {
		next = session.ModelRequestCursor{AfterID: records[limit-1].ID, Limit: limit}
		records = records[:limit]
	}
	return session.ModelRequestBatch{Records: records, Next: next}, nil
}
