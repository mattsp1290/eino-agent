//go:build postgres_integration

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
)

func testConcurrentMigration(t *testing.T, server *testpostgres.Server) {
	database := server.Database(t)
	first, second, observer := database.Open(t), database.Open(t), database.Open(t)
	entered := make(chan int, 2)
	acquired, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	files := migrationFS(t)
	results := make(chan error, 2)
	go func() {
		results <- migrate(t.Context(), first, files, gateLocker{migrationDelegate(t), entered, acquired, release})
	}()
	firstPID := receiveMigration(t, entered)
	receiveMigration(t, acquired)
	go func() {
		results <- migrate(t.Context(), second, files, gateLocker{migrationDelegate(t), entered, nil, nil})
	}()
	if secondPID := receiveMigration(t, entered); firstPID == secondPID {
		t.Fatal("migrators share a backend")
	}
	if schemaHistory(t, observer) != "absent" {
		t.Fatal("history written before validating lock returned")
	}
	select {
	case err := <-results:
		t.Fatalf("migrator escaped held lock: %v", err)
	default:
	}
	close(release)
	for range 2 {
		if err := receiveMigration(t, results); err != nil {
			t.Fatal(err)
		}
	}
	assertMigrationState(t, observer, schemaCurrent)
}

func testCanceledMigration(t *testing.T, server *testpostgres.Server) {
	database := server.Database(t)
	holderDB, waiterDB := database.Open(t), database.Open(t)
	holder := schemaConn(t, holderDB)
	delegate := migrationDelegate(t)
	if err := delegate.SessionLock(t.Context(), holder); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := delegate.SessionUnlock(context.Background(), holder); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, result := make(chan int, 1), make(chan error, 1)
	files := migrationFS(t)
	go func() { result <- migrate(ctx, waiterDB, files, gateLocker{migrationDelegate(t), entered, nil, nil}) }()
	receiveMigration(t, entered)
	cancel()
	if err := receiveMigration(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter: %v", err)
	}
	if schemaHistory(t, waiterDB) != "absent" {
		t.Fatal("canceled waiter wrote history")
	}
	if err := waiterDB.PingContext(t.Context()); err != nil {
		t.Fatal("waiter pool unusable", err)
	}
}

type failedUnlock struct {
	lock.SessionLocker
	pid    int
	err    error
	cancel context.CancelFunc
	expire bool
}

func (l *failedUnlock) SessionLock(ctx context.Context, conn *sql.Conn) error {
	if err := conn.QueryRowContext(ctx, `SELECT pg_catalog.pg_backend_pid()`).Scan(&l.pid); err != nil {
		return err
	}
	return l.SessionLocker.SessionLock(ctx, conn)
}

func (l *failedUnlock) SessionUnlock(ctx context.Context, conn *sql.Conn) error {
	if l.expire {
		<-ctx.Done()
		var value int
		// Reproduce stdlib's SafeToRetry -> ErrBadConn path on an expired context.
		return errors.Join(l.err, conn.QueryRowContext(ctx, "SELECT 1").Scan(&value))
	}
	l.cancel()
	return l.err // Deliberately leave the real session lock held.
}

func testPhysicalMigrationCleanup(t *testing.T, server *testpostgres.Server) {
	for _, test := range []struct {
		state  schemaState
		expire bool
	}{{schemaEmpty, false}, {schemaCurrent, false}, {schemaCurrent, true}} {
		state := test.state
		database := server.Database(t)
		observer := database.Open(t)
		prepareSchema(t, observer, state)
		pool := database.OpenPGX(t)
		wrapper := stdlib.OpenDBFromPool(pool)
		t.Cleanup(func() {
			if err := wrapper.Close(); err != nil {
				t.Error(err)
			}
		})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		failure := errors.New("injected advisory unlock failure")
		locker := &failedUnlock{SessionLocker: migrationDelegate(t), err: failure, cancel: cancel, expire: test.expire}
		err := migrate(ctx, wrapper, migrationFS(t), locker)
		if !errors.Is(err, failure) {
			t.Fatalf("cleanup failure lost: %v", err)
		}
		if state == schemaCurrent && !errors.Is(err, goose.ErrAlreadyApplied) {
			t.Fatalf("no-op error missing: %v", err)
		}
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			var exists bool
			if err := observer.QueryRowContext(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$1)`, locker.pid).Scan(&exists); err != nil {
				t.Fatal(err)
			}
			if !exists {
				break
			}
			select {
			case <-deadline.C:
				t.Fatal("contaminated physical backend survived cleanup")
			case <-tick.C:
			}
		}
		// An independent pool can acquire the same lock and validate the completed
		// baseline; neither closing the wrapper alone nor releasing to pgxpool suffices.
		if err := Migrate(t.Context(), observer); err != nil {
			t.Fatal("independent migration blocked after cleanup", err)
		}
		if err := pool.Ping(t.Context()); err != nil {
			t.Fatal("host native pool closed", err)
		}
		if err := Migrate(t.Context(), wrapper); err != nil {
			t.Fatal("host wrapper unusable", err)
		}
		assertMigrationState(t, observer, schemaCurrent)
	}
}

func receiveMigration[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatal("migration synchronization timed out")
	}
	var zero T
	return zero
}
