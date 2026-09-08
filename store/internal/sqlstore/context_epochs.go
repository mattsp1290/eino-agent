package sqlstore

import (
	"context"
	"encoding/json"
	"errors"

	"gorm.io/gorm/clause"

	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) startContextEpoch(ctx context.Context, record session.ContextEpoch) (session.ContextEpoch, error) {
	sessionKey, err := s.key(ctx, "sessions", string(record.SessionID))
	if err != nil {
		return session.ContextEpoch{}, err
	}
	db := s.dbFor(ctx)
	row, readErr := s.contextEpochRowByID(ctx, string(record.ID))
	if readErr == nil {
		var existing session.ContextEpoch
		if err := decodeStoredRecord(row.Record, &existing); err != nil {
			return session.ContextEpoch{}, err
		}
		if !contextEpochRowMatches(row, existing) || row.SessionKey != sessionKey {
			return session.ContextEpoch{}, session.ErrConflict
		}
		if !SameRecord(existing, record) {
			return session.ContextEpoch{}, session.ErrConflict
		}
		return existing, nil
	} else if !errors.Is(readErr, session.ErrNotFound) {
		return session.ContextEpoch{}, readErr
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return session.ContextEpoch{}, err
	}
	created := db.Table("context_epochs").Clauses(clause.OnConflict{DoNothing: true}).Create(map[string]any{
		"id": publicID(record.ID), "session_key": sessionKey, "record": raw,
		"created_at": TimeText(record.CreatedAt), "closed_at": TimeText(record.ClosedAt),
	})
	if err := s.mapErr(created.Error); err != nil {
		return session.ContextEpoch{}, err
	}
	if created.RowsAffected != 0 {
		return record, nil
	}
	row, err = s.contextEpochRowByID(ctx, string(record.ID))
	if err != nil {
		return session.ContextEpoch{}, err
	}
	var existing session.ContextEpoch
	if err := decodeStoredRecord(row.Record, &existing); err != nil {
		return session.ContextEpoch{}, err
	}
	if !contextEpochRowMatches(row, existing) || row.SessionKey != sessionKey {
		return session.ContextEpoch{}, session.ErrConflict
	}
	if !SameRecord(existing, record) {
		return session.ContextEpoch{}, session.ErrConflict
	}
	return existing, nil
}

func contextEpochRowMatches(row contextEpochRow, record session.ContextEpoch) bool {
	return string(row.ID) == string(record.ID) && string(row.SessionID) == string(record.SessionID) && TimeText(record.CreatedAt) == row.CreatedAt && TimeText(record.ClosedAt) == row.ClosedAt
}

func (s *Store) finishContextEpoch(ctx context.Context, record session.ContextEpoch) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	sessionKey, err := s.key(ctx, "sessions", string(record.SessionID))
	if err != nil {
		return err
	}
	db := s.dbFor(ctx).Table("context_epochs").Where("id = ? AND session_key = ?", publicID(record.ID), sessionKey).Updates(map[string]any{
		"record": raw, "closed_at": TimeText(record.ClosedAt), "created_at": TimeText(record.CreatedAt),
	})
	if err := s.mapErr(db.Error); err != nil {
		return err
	}
	return rowsAffected(db)
}

func (s *Store) ListContextEpochs(ctx context.Context, sessionID session.ID) ([]session.ContextEpoch, error) {
	sessionKey, err := s.key(ctx, "sessions", string(sessionID))
	if err != nil {
		return nil, err
	}
	var rows []contextEpochRow
	if err := s.contextEpochQuery(ctx).Where("context_epochs.session_key = ?", sessionKey).Order("context_epochs.created_at, context_epochs.id").Find(&rows).Error; err != nil {
		return nil, s.mapErr(err)
	}
	result := make([]session.ContextEpoch, 0, len(rows))
	for _, row := range rows {
		var record session.ContextEpoch
		if err := decodeStoredRecord(row.Record, &record); err != nil {
			return nil, err
		}
		if !contextEpochRowMatches(row, record) {
			return nil, session.ErrConflict
		}
		result = append(result, record)
	}
	return result, nil
}
