package sqlstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	"gorm.io/gorm/clause"

	"github.com/mattsp1290/eino-agent/session"
)

func (s *Store) CreateSession(ctx context.Context, record session.Session) (session.Session, error) {
	if !validSessionProjection(record) {
		return session.Session{}, session.ErrConflict
	}
	var result session.Session
	err := s.atomic(ctx, func(store *Store) error {
		db := store.dbFor(ctx)
		row, readErr := store.sessionRowByID(ctx, string(record.ID))
		if readErr == nil {
			var existing session.Session
			if err := decodeStoredRecord(row.Record, &existing); err != nil {
				return err
			}
			if !sessionRowMatches(row, existing) || string(existing.ID) != string(record.ID) {
				return session.ErrConflict
			}
			if !SameRecord(existing, record) {
				return session.ErrConflict
			}
			result = existing
			return nil
		} else if !errors.Is(readErr, session.ErrNotFound) {
			return readErr
		}
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		created := db.Table("sessions").Clauses(clause.OnConflict{DoNothing: true}).Create(map[string]any{
			"id": publicID(record.ID), "record": raw, "workspace_id": []byte(record.WorkspaceID),
			"title": []byte(record.Title), "created_at": TimeText(record.CreatedAt),
			"updated_at": TimeText(record.UpdatedAt),
		})
		if err := store.mapErr(created.Error); err != nil {
			return err
		}
		if created.RowsAffected == 0 {
			row, err := store.sessionRowByID(ctx, string(record.ID))
			if err != nil {
				return err
			}
			var existing session.Session
			if err := decodeStoredRecord(row.Record, &existing); err != nil {
				return err
			}
			if !sessionRowMatches(row, existing) || string(existing.ID) != string(record.ID) {
				return session.ErrConflict
			}
			if !SameRecord(existing, record) {
				return session.ErrConflict
			}
			result = existing
			return nil
		}
		result = record
		return nil
	})
	return result, err
}

func (s *Store) GetSession(ctx context.Context, id session.ID) (session.Session, error) {
	row, err := s.sessionRowByID(ctx, string(id))
	if err != nil {
		return session.Session{}, err
	}
	var record session.Session
	if err := decodeStoredRecord(row.Record, &record); err != nil {
		return session.Session{}, err
	}
	if !sessionRowMatches(row, record) || string(record.ID) != string(id) {
		return session.Session{}, session.ErrConflict
	}
	return record, nil
}

func sessionRowMatches(row sessionRow, record session.Session) bool {
	return string(row.ID) == string(record.ID) && string(row.Workspace) == record.WorkspaceID &&
		string(row.Title) == record.Title && TimeText(record.CreatedAt) == row.CreatedAt &&
		TimeText(record.UpdatedAt) == row.UpdatedAt
}

func (s *Store) UpdateSession(ctx context.Context, record session.Session) error {
	if !validSessionProjection(record) {
		return session.ErrConflict
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return s.atomic(ctx, func(store *Store) error {
		db := store.dbFor(ctx).Table("sessions").Where("id = ?", publicID(record.ID)).Updates(map[string]any{
			"record": raw, "workspace_id": []byte(record.WorkspaceID), "title": []byte(record.Title),
			"created_at": TimeText(record.CreatedAt), "updated_at": TimeText(record.UpdatedAt),
		})
		if err := store.mapErr(db.Error); err != nil {
			return err
		}
		return rowsAffected(db)
	})
}

func validSessionProjection(record session.Session) bool {
	for _, value := range []string{string(record.ID), record.WorkspaceID, record.Title} {
		if !utf8.ValidString(value) {
			return false
		}
	}
	for _, value := range []time.Time{record.CreatedAt, record.UpdatedAt} {
		if !value.IsZero() && (value.UTC().Year() < 0 || value.UTC().Year() > 9999) {
			return false
		}
	}
	return true
}
