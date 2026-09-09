//go:build postgres_integration

package postgres

import (
	"context"
	"database/sql"

	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"

	"github.com/mattsp1290/eino-agent/store/internal/sqlstore"
)

// NewReaderTestStore instruments queries inside the production read transaction.
// It borrows an already migrated fixture pool; this bridge is never in the library.
func NewReaderTestStore(db *sql.DB, beforeQuery func(context.Context, string) error, trace func(string, []any)) (*Store, error) {
	orm, err := gorm.Open(gormpostgres.New(gormpostgres.Config{DriverName: "pgx", Conn: db}), &gorm.Config{
		DisableAutomaticPing: true,
		NamingStrategy:       schema.NamingStrategy{SingularTable: true, TablePrefix: "public."},
		Logger:               logger.Discard,
	})
	if err != nil {
		return nil, err
	}
	return &Store{Store: sqlstore.New(orm, db, readerTestDialect{beforeQuery: beforeQuery, trace: trace})}, nil
}

type readerTestDialect struct {
	postgresDialect
	beforeQuery func(context.Context, string) error
	trace       func(string, []any)
}

func (d readerTestDialect) Read(ctx context.Context, pool *sql.DB, read func(sqlstore.SQLReader) error) error {
	return d.postgresDialect.Read(ctx, pool, func(reader sqlstore.SQLReader) error {
		return read(&readerTestQueries{SQLReader: reader, beforeQuery: d.beforeQuery, trace: d.trace})
	})
}

type readerTestQueries struct {
	sqlstore.SQLReader
	beforeQuery func(context.Context, string) error
	trace       func(string, []any)
}

func (r *readerTestQueries) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if r.trace != nil {
		r.trace(query, args)
	}
	if r.beforeQuery != nil {
		if err := r.beforeQuery(ctx, query); err != nil {
			return nil, err
		}
	}
	return r.SQLReader.QueryContext(ctx, query, args...)
}

func (r *readerTestQueries) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if r.trace != nil {
		r.trace(query, args)
	}
	return r.SQLReader.QueryRowContext(ctx, query, args...)
}
