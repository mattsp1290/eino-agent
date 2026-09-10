package sqlstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

// SetSessionTitle performs a trusted host-authorized title mutation. The
// expected workspace must match exactly, including the empty workspace.
func (s *Store) SetSessionTitle(ctx context.Context, request session.SessionTitleRequest) (session.SessionTitleResult, error) {
	if err := ctx.Err(); err != nil {
		return session.SessionTitleResult{}, err
	}
	if err := request.Validate(); err != nil {
		return session.SessionTitleResult{}, err
	}
	if s == nil || s.db == nil {
		return session.SessionTitleResult{}, session.ErrSessionTitleStore
	}
	var result session.SessionTitleResult
	err := s.atomic(ctx, func(store *Store) error {
		var err error
		result, err = store.setSessionTitleLocked(ctx, request)
		return err
	})
	if err != nil {
		return session.SessionTitleResult{}, titleError(err, true)
	}
	return result, nil
}

func (e *executionStore) SetSessionTitle(ctx context.Context, request session.SessionTitleRequest) (session.SessionTitleResult, error) {
	if err := ctx.Err(); err != nil {
		return session.SessionTitleResult{}, err
	}
	if err := request.Validate(); err != nil {
		return session.SessionTitleResult{}, err
	}
	if e == nil || e.store == nil || e.store.db == nil {
		return session.SessionTitleResult{}, session.ErrSessionTitleStore
	}
	var result session.SessionTitleResult
	err := e.withFence(ctx, func(store *Store, run session.Run) error {
		if run.SessionID != request.SessionID {
			return session.ErrConflict
		}
		var err error
		result, err = store.setSessionTitleLocked(ctx, request)
		return err
	})
	if err != nil {
		return session.SessionTitleResult{}, titleError(err, false)
	}
	return result, nil
}

// setSessionTitleLocked is called only inside a write transaction. The manual
// path obtains the session lock here; fenced execution already holds that lock
// through lockRun and safely reacquires the same row.
func (s *Store) setSessionTitleLocked(ctx context.Context, request session.SessionTitleRequest) (session.SessionTitleResult, error) {
	row, err := s.lockSessionByID(ctx, string(request.SessionID))
	if err != nil {
		return session.SessionTitleResult{}, err
	}
	var record session.Session
	if err := decodeStoredRecord(row.Record, &record); err != nil || !sessionRowMatches(row, record) || record.ID != request.SessionID {
		return session.SessionTitleResult{}, session.ErrSessionTitleStore
	}
	if record.WorkspaceID != request.WorkspaceID {
		return session.SessionTitleResult{}, session.ErrConflict
	}
	if record.Title == request.Title {
		return session.SessionTitleResult{Title: record.Title, UpdatedAt: record.UpdatedAt, Changed: false}, nil
	}
	var micros int64
	if err := s.queryRow(ctx, "SELECT "+s.dialect.ClockSQL()).Scan(&micros); err != nil {
		return session.SessionTitleResult{}, err
	}
	updatedAt := time.UnixMicro(micros).UTC()
	if record.UpdatedAt.After(updatedAt) {
		updatedAt = record.UpdatedAt.UTC()
	}
	if updatedAt.Year() < 0 || updatedAt.Year() > 9999 {
		return session.SessionTitleResult{}, session.ErrSessionTitleStore
	}
	record.Title = request.Title
	record.UpdatedAt = updatedAt
	raw, err := json.Marshal(record)
	if err != nil {
		return session.SessionTitleResult{}, err
	}
	db := s.dbFor(ctx).Table(s.tableName("sessions")).Where("row_key = ?", row.RowKey).Updates(map[string]any{
		"record": raw, "title": []byte(record.Title), "updated_at": TimeText(record.UpdatedAt),
	})
	if err := db.Error; err != nil {
		return session.SessionTitleResult{}, err
	}
	if db.RowsAffected == 0 {
		return session.SessionTitleResult{}, session.ErrSessionTitleStore
	}
	return session.SessionTitleResult{Title: record.Title, UpdatedAt: record.UpdatedAt, Changed: true}, nil
}

func titleError(err error, manual bool) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.Is(err, session.ErrSessionTitleInvalid):
		return session.ErrSessionTitleInvalid
	case errors.Is(err, session.ErrConflict):
		return session.ErrConflict
	case manual && errors.Is(err, session.ErrNotFound):
		return session.ErrNotFound
	default:
		return session.ErrSessionTitleStore
	}
}
