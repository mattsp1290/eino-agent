package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/mattsp1290/eino-agent/session"
)

// Store owns persistence behavior; its pool remains owned by the host.
type Store struct {
	db      *gorm.DB
	pool    *sql.DB
	dialect Dialect
	tx      *transactionState
}

func New(db *gorm.DB, pool *sql.DB, dialect Dialect) *Store {
	return &Store{db: db, pool: pool, dialect: dialect}
}

func (s *Store) dbFor(ctx context.Context) *gorm.DB {
	db := s.db.Session(&gorm.Session{NewDB: true, Context: ctx, SkipDefaultTransaction: s.tx != nil})
	if s.tx != nil {
		if err := s.tx.failure(); err != nil {
			_ = db.AddError(err)
		}
	}
	return db
}

func (s *Store) bound(ctx context.Context, conn gorm.ConnPool) *Store {
	db := s.db.Session(&gorm.Session{NewDB: true, Context: ctx, SkipDefaultTransaction: true})
	db.Statement.ConnPool = conn
	db.ConnPool = conn
	return &Store{db: db, dialect: s.dialect}
}

func (s *Store) mapErr(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, sql.ErrNoRows) {
		return session.ErrNotFound
	}
	return s.dialect.MapError(err)
}

func (s *Store) key(ctx context.Context, table, id string) (int64, error) {
	var row struct{ RowKey int64 }
	err := s.dbFor(ctx).Table(s.tableName(table)).Select("row_key").Where("id = ?", []byte(id)).Take(&row).Error
	return row.RowKey, s.mapErr(err)
}

func rowsAffected(result *gorm.DB) error {
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return session.ErrNotFound
	}
	return nil
}

func durationMicros(duration time.Duration) int64 { return max(1, duration.Microseconds()) }

// Both schemas represent boolean projections as checked integer flags.
func flagValue(value bool) int {
	if value {
		return 1
	}
	return 0
}

type rowScanner interface{ Scan(...any) error }

// tableName applies the controlled backend naming strategy to fixed internal names.
// Adapters use SingularTable so singleton names such as observation_store stay exact.
func (s *Store) tableName(name string) string { return s.db.NamingStrategy.TableName(name) }

func (s *Store) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.dbFor(ctx).Raw(query, args...).Rows()
}

func (s *Store) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.dbFor(ctx).Raw(query, args...).Row()
}

func (s *Store) read(ctx context.Context, read func(*Store) error) error {
	return s.dialect.Read(ctx, s.pool, func(conn SQLReader) error {
		return read(s.bound(ctx, conn))
	})
}
