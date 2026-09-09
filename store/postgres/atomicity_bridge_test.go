//go:build postgres_integration

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/internal/sqlstore"
)

var ErrAtomicityCleanup = errors.New("injected savepoint cleanup acknowledgement failure")

// AtomicityCleanupFault is used only by synchronous integration callbacks.
// The real command succeeds before its acknowledgement is replaced by an error.
// The server transaction therefore remains usable: poisoning must prevent commit.
type AtomicityCleanupFault struct {
	Command string
	Armed   bool
	Calls   int
	Commits int
}

type atomicityDialect struct {
	postgresDialect
	fault *AtomicityCleanupFault
}

func (d atomicityDialect) Begin(ctx context.Context, pool *sql.DB) (sqlstore.Transaction, error) {
	tx, err := d.postgresDialect.Begin(ctx, pool)
	if err != nil {
		return nil, err
	}
	return &atomicityTransaction{Transaction: tx, fault: d.fault}, nil
}

// NewAtomicityTestStore uses the production PostgreSQL dialect on an already
// migrated fixture pool. Only its transaction cleanup acknowledgement is changed.
// This file is compiled into tests, never into the library (even with the tag).
func NewAtomicityTestStore(db *sql.DB, command string) (session.Store, *AtomicityCleanupFault, error) {
	orm, err := gorm.Open(gormpostgres.New(gormpostgres.Config{DriverName: "pgx", Conn: db}), &gorm.Config{
		DisableAutomaticPing: true,
		NamingStrategy:       schema.NamingStrategy{SingularTable: true, TablePrefix: "public."},
		Logger:               logger.Discard,
	})
	if err != nil {
		return nil, nil, err
	}
	fault := &AtomicityCleanupFault{Command: command}
	return sqlstore.New(orm, db, atomicityDialect{fault: fault}), fault, nil
}

type atomicityTransaction struct {
	sqlstore.Transaction
	fault *AtomicityCleanupFault
}

func (t *atomicityTransaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	result, err := t.Transaction.ExecContext(ctx, query, args...)
	if err == nil && t.fault.Armed && strings.HasPrefix(query, t.fault.Command+" ") {
		t.fault.Armed = false
		t.fault.Calls++
		return result, ErrAtomicityCleanup
	}
	return result, err
}

func (t *atomicityTransaction) Commit(ctx context.Context) error {
	t.fault.Commits++
	return t.Transaction.Commit(ctx)
}
