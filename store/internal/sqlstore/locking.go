package sqlstore

import (
	"context"

	"github.com/mattsp1290/eino-agent/session"
)

// lockSessionByID acquires the session row before any related run row. The
// complete projection is returned so callers can validate the authoritative
// record while retaining the lock through their surrounding transaction.
func (s *Store) lockSessionByID(ctx context.Context, id string) (sessionRow, error) {
	var row sessionRow
	err := s.dialect.LockRows(s.dbFor(ctx).Table(s.tableName("sessions")).Select("row_key, id, record, workspace_id, title, created_at, updated_at").Where("id = ?", []byte(id))).Take(&row).Error
	return row, s.mapErr(err)
}

func (s *Store) lockSessionByKey(ctx context.Context, key int64) error {
	var row struct {
		RowKey int64 `gorm:"column:row_key"`
	}
	err := s.dialect.LockRows(s.dbFor(ctx).Table(s.tableName("sessions")).Select("row_key").Where("row_key = ?", key)).Take(&row).Error
	return s.mapErr(err)
}

func (s *Store) lockSessionKeyByID(ctx context.Context, id string) (int64, error) {
	var row struct {
		RowKey int64 `gorm:"column:row_key"`
	}
	err := s.dialect.LockRows(s.dbFor(ctx).Table(s.tableName("sessions")).Select("row_key").Where("id = ?", []byte(id))).Take(&row).Error
	return row.RowKey, s.mapErr(err)
}

// lockRun resolves the owning session without a lock, locks that session,
// then rereads the joined run while locking only the runs relation. The
// session-key check detects a run changing owner between those reads.
func (s *Store) lockRun(ctx context.Context, id session.RunID) (runRow, error) {
	var preliminary struct {
		SessionKey int64 `gorm:"column:session_key"`
	}
	err := s.dbFor(ctx).Table(s.tableName("runs")).Select("session_key").Where("id = ?", []byte(id)).Take(&preliminary).Error
	if err = s.mapErr(err); err != nil {
		return runRow{}, err
	}
	if err := s.lockSessionByKey(ctx, preliminary.SessionKey); err != nil {
		return runRow{}, err
	}
	var row runRow
	err = s.dialect.LockRows(s.runQuery(ctx).Where("runs.id = ?", []byte(id))).Take(&row).Error
	if err = s.mapErr(err); err != nil {
		return runRow{}, err
	}
	if row.SessionKey != preliminary.SessionKey {
		return runRow{}, session.ErrConflict
	}
	return row, nil
}
