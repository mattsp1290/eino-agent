package postgres

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/mattsp1290/eino-agent/session"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrate initializes or validates the versioned schema in a dedicated PostgreSQL
// database. It borrows a pgx-backed pool; the caller owns and closes the pool.
// Concurrent calls serialize on a session advisory lock and validate before any
// history or schema writes. Unsupported schemas are rejected without adoption.
func Migrate(ctx context.Context, db *sql.DB) error {
	files, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return err
	}
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, 60), lock.WithUnlockTimeout(1, 5))
	if err != nil {
		return err
	}
	return migrate(ctx, db, files, locker)
}

// Test fixtures may supply an equivalent failing baseline or locker; every path
// still uses the production validation and physical-connection cleanup adapter.
func migrate(ctx context.Context, db *sql.DB, files fs.FS, delegate lock.SessionLocker) error {
	if db == nil {
		return fmt.Errorf("postgres migration: nil pool: %w", session.ErrConflict)
	}
	operation := newMigrationContext(ctx)
	defer operation.stop()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, files,
		goose.WithTableName("public.eino_agent_goose_version"),
		goose.WithDisableGlobalRegistry(true),
		goose.WithSessionLocker(&validatingSessionLocker{delegate: delegate, operation: operation}),
	)
	if err != nil {
		return fmt.Errorf("postgres migration provider: %w", err)
	}
	// ApplyVersion enters the validating locker before history initialization.
	// Up and version/status probes do not provide that ordering in pinned Goose.
	// Never close this provider: Provider.Close would close the borrowed host pool.
	_, err = provider.ApplyVersion(operation.Context, 1, true)
	if err != nil {
		err = errors.Join(err, operation.err())
	}
	if soleAlreadyApplied(err) {
		return nil
	}
	return err
}

// errors.Is alone would hide cleanup errors joined to an already-applied result.
func soleAlreadyApplied(err error) bool {
	if err == goose.ErrAlreadyApplied {
		return true
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		children := wrapped.Unwrap()
		return len(children) == 1 && soleAlreadyApplied(children[0])
	case interface{ Unwrap() error }:
		return soleAlreadyApplied(wrapped.Unwrap())
	default:
		return false
	}
}
