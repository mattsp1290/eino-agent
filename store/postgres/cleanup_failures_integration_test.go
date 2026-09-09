//go:build postgres_integration

package postgres_test

import (
	"context"
	"errors"

	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/postgres"
)

func testCleanupFailures(t *testing.T, server *testpostgres.Server) {
	t.Run("terminated_backend_poison", func(t *testing.T) { testTerminatedBackend(t, server) })
	for _, mode := range []string{"rollback", "release"} {
		t.Run(mode+"_cleanup", func(t *testing.T) { testSavepointCleanup(t, server, mode) })
	}
}

func testTerminatedBackend(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 2)
	f.seed(t)
	baseline := f.snapshot(t)
	mustExec(t, f.observer, `
CREATE FUNCTION public.kill_atomicity_backend() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
	IF NEW.id = 'killed-session'::bytea THEN
		PERFORM pg_catalog.pg_terminate_backend(pg_catalog.pg_backend_pid());
	END IF;
	RETURN NEW;
END
$$;
CREATE TRIGGER kill_atomicity_backend
BEFORE INSERT ON public.sessions
FOR EACH ROW EXECUTE FUNCTION public.kill_atomicity_backend()`)
	cleanupFault := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := f.observer.ExecContext(ctx, "DROP TRIGGER IF EXISTS kill_atomicity_backend ON public.sessions")
		if err != nil {
			return err
		}
		_, err = f.observer.ExecContext(ctx, "DROP FUNCTION IF EXISTS public.kill_atomicity_backend()")
		return err
	}
	removed := false
	t.Cleanup(func() {
		if !removed {
			if err := cleanupFault(); err != nil {
				t.Error(err)
			}
		}
	})

	var pid int
	type outcome struct {
		outer, operation, later error
		calls                   int
	}
	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	var got outcome
	got.outer = f.stores[0].WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
		got.calls++
		if _, err := tx.CreateSession(ctx, session.Session{ID: "before-loss"}); err != nil {
			return err
		}
		var count int
		if err := f.observer.QueryRowContext(ctx, "SELECT count(*), COALESCE(max(pid), 0) FROM pg_catalog.pg_stat_activity WHERE datname = current_database() AND state = 'idle in transaction'").Scan(&count, &pid); err != nil {
			return err
		}
		if count != 1 || pid == 0 {
			t.Fatalf("expected one active writer backend, count=%d", count)
		}
		_, got.operation = tx.CreateSession(ctx, session.Session{ID: "killed-session"})
		_, got.later = tx.CreateSession(ctx, session.Session{ID: "after-loss-in-tx"})
		return nil
	})
	if got.calls != 1 {
		t.Fatalf("callback calls = %d, want 1", got.calls)
	}
	if got.operation == nil || got.later == nil || got.outer == nil {
		t.Fatalf("connection-loss errors operation=%v later=%v outer=%v", got.operation, got.later, got.outer)
	}
	if errors.Is(got.outer, session.ErrConflict) {
		t.Fatalf("connection loss misclassified as conflict: %v", got.outer)
	}
	var pgErr *pgconn.PgError
	if !errors.As(got.operation, &pgErr) || pgErr.Code != "57P01" {
		t.Fatalf("operation did not report backend termination: %v", got.operation)
	}
	waitRace(t, f.ctx, func() (bool, error) {
		var gone bool
		err := f.observer.QueryRowContext(f.ctx, "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$1)", pid).Scan(&gone)
		return gone, err
	})
	if gotSnapshot := f.snapshot(t); gotSnapshot != baseline {
		t.Fatal("connection loss left application rows or revisions")
	}
	if err := cleanupFault(); err != nil {
		t.Fatal(err)
	}
	removed = true
	if _, err := f.stores[0].CreateSession(f.ctx, session.Session{ID: "after-loss"}); err != nil {
		t.Fatalf("writer pool did not recover: %v", err)
	}
}

func testSavepointCleanup(t *testing.T, server *testpostgres.Server, mode string) {
	f := newRaceFixture(t, server, 2)
	f.seed(t)
	baseline := f.snapshot(t)
	command := "ROLLBACK TO SAVEPOINT"
	if mode == "release" {
		command = "RELEASE SAVEPOINT"
	}
	store, fault, err := postgres.NewAtomicityTestStore(f.dbs[0], command)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	var operationErr, laterErr error
	outerErr := store.WithinTx(f.ctx, func(ctx context.Context, tx session.Store) error {
		calls++
		beforeID := session.ID("before-" + mode)
		if _, err := tx.CreateSession(ctx, session.Session{ID: beforeID}); err != nil {
			return err
		}
		fault.Armed = true
		_, operationErr = tx.CreateSession(ctx, session.Session{ID: beforeID, Title: "conflict"})
		_, laterErr = tx.CreateSession(ctx, session.Session{ID: session.ID("after-" + mode)})
		return nil
	})
	if !errors.Is(operationErr, session.ErrConflict) || !errors.Is(operationErr, postgres.ErrAtomicityCleanup) || !errors.Is(laterErr, postgres.ErrAtomicityCleanup) || !errors.Is(outerErr, postgres.ErrAtomicityCleanup) {
		t.Fatalf("%s cleanup errors operation=%v later=%v outer=%v", mode, operationErr, laterErr, outerErr)
	}
	if calls != 1 || f.snapshot(t) != baseline {
		t.Fatalf("%s cleanup changed callback count or durable snapshot", mode)
	}
	if fault.Calls != 1 || fault.Commits != 0 {
		t.Fatalf("%s cleanup calls=%d commits=%d, want 1 and 0", mode, fault.Calls, fault.Commits)
	}
	if _, err := f.stores[0].CreateSession(f.ctx, session.Session{ID: session.ID("after-" + mode)}); err != nil {
		t.Fatalf("writer pool unusable after %s cleanup: %v", mode, err)
	}
}
