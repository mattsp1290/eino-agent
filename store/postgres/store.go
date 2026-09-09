package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"

	"github.com/jackc/pgx/v5/stdlib"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/internal/sqlstore"
)

// Store persists sessions using a host-owned pgx-backed database/sql pool.
// The pool is borrowed and remains owned by the caller.
type Store struct {
	*sqlstore.Store
}

// New validates an initialized PostgreSQL database and borrows its pool.
// The host must first call Migrate, and owns pool shutdown and configuration.
func New(ctx context.Context, db *sql.DB) (*Store, error) {
	if err := validatePool(db); err != nil {
		return nil, postgresLifecycleError{errors.Join(err, ctx.Err())}
	}
	state, err := inspectPool(ctx, db)
	if err != nil {
		return nil, postgresLifecycleError{errors.Join(err, ctx.Err())}
	}
	if state != schemaCurrent {
		return nil, fmt.Errorf("%w: postgres schema is not initialized", session.ErrConflict)
	}
	orm, err := gorm.Open(gormpostgres.New(gormpostgres.Config{
		DriverName: "pgx",
		Conn:       db,
	}), &gorm.Config{
		DisableAutomaticPing: true,
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
			TablePrefix:   "public.",
		},
		Logger: logger.Discard,
	})
	if err != nil {
		return nil, postgresLifecycleError{errors.Join(err, ctx.Err())}
	}
	return &Store{Store: sqlstore.New(orm, db, postgresDialect{})}, nil
}

func validatePool(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("%w: nil postgres pool", session.ErrConflict)
	}
	if _, ok := db.Driver().(*stdlib.Driver); !ok {
		return fmt.Errorf("%w: unsupported postgres driver", session.ErrConflict)
	}
	return nil
}

func inspectPool(ctx context.Context, db *sql.DB) (schemaState, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()
	return inspectSchema(ctx, conn)
}

// Lifecycle errors deliberately keep the public message content-free while
// preserving errors.Is/errors.As matching for the underlying cause.
type postgresLifecycleError struct{ err error }

func (e postgresLifecycleError) Error() string { return "postgres store initialization failed" }
func (e postgresLifecycleError) Unwrap() error { return e.err }

// Nil receivers preserve the shared reader validation precedence.
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

var _ session.Store = (*Store)(nil)
var _ session.ContextEpochReader = (*Store)(nil)
var _ session.ObservationReader = (*Store)(nil)
var _ session.SessionDiscoveryReader = (*Store)(nil)
