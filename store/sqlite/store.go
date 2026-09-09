package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	gormsqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	modernsqlite "modernc.org/sqlite"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/internal/sqlstore"
)

// Store persists sessions using a host-owned modernc SQLite pool.
type Store struct {
	*sqlstore.Store
	db *sql.DB
}

// New verifies an initialized database and borrows it without changing schema
// or pool settings. The host must first call Migrate, and owns pool shutdown.
func New(ctx context.Context, db *sql.DB) (*Store, error) {
	if err := validatePool(ctx, db); err != nil {
		return nil, sqliteLifecycleError{err}
	}
	state, err := inspectPool(ctx, db)
	if err != nil {
		return nil, sqliteLifecycleError{err}
	}
	if state != migrationSchemaCurrent {
		return nil, fmt.Errorf("%w: sqlite schema is not initialized", session.ErrConflict)
	}
	orm, err := gorm.Open(gormsqlite.New(gormsqlite.Config{DriverName: "sqlite", Conn: db}), &gorm.Config{
		DisableAutomaticPing: true,
		NamingStrategy:       schema.NamingStrategy{SingularTable: true},
		Logger:               logger.Discard,
	})
	if err != nil {
		return nil, sqliteLifecycleError{err}
	}
	return &Store{Store: sqlstore.New(orm, db, sqliteDialect{}), db: db}, nil
}

func validatePool(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("%w: nil sqlite pool", session.ErrConflict)
	}
	if _, ok := db.Driver().(*modernsqlite.Driver); !ok {
		return fmt.Errorf("%w: unsupported sqlite driver", session.ErrConflict)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	var enabled int
	if err := conn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&enabled); err != nil {
		return err
	}
	if enabled != 1 {
		return fmt.Errorf("%w: sqlite foreign keys must be enabled by the host", session.ErrConflict)
	}
	return nil
}

func inspectPool(ctx context.Context, db *sql.DB) (migrationSchemaState, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()
	return inspectMigrationSchema(ctx, conn)
}

// Lifecycle errors retain classification without exposing host connection text.
type sqliteLifecycleError struct{ err error }

func (e sqliteLifecycleError) Error() string { return "sqlite store initialization failed" }
func (e sqliteLifecycleError) Unwrap() error { return e.err }

func (s *Store) ListSessions(ctx context.Context, q session.SessionDiscoveryQuery) (session.SessionDiscoveryPage, error) {
	var store *sqlstore.Store
	if s != nil {
		store = s.Store
	}
	return store.ListSessions(ctx, q)
}
func (s *Store) ReadObservationRevision(ctx context.Context, id session.ID) (session.ObservationWatermark, error) {
	var store *sqlstore.Store
	if s != nil {
		store = s.Store
	}
	return store.ReadObservationRevision(ctx, id)
}
func (s *Store) ReadObservationSnapshot(ctx context.Context, id session.ID, limits session.ObservationLimits) (session.ObservationSnapshot, error) {
	var store *sqlstore.Store
	if s != nil {
		store = s.Store
	}
	return store.ReadObservationSnapshot(ctx, id, limits)
}
