//go:build postgres_integration

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/pressly/goose/v3/lock"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
)

func TestPostgresMigration(t *testing.T) {
	server := testpostgres.Start(t)
	t.Run("lifecycle", func(t *testing.T) {
		for _, state := range []schemaState{schemaEmpty, schemaBootstrap, schemaCurrent} {
			db := server.Database(t).Open(t)
			prepareSchema(t, db, state)
			if err := Migrate(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			assertMigrationState(t, db, schemaCurrent)
			before, history := schemaCatalog(t, db), schemaHistory(t, db)
			var identity string
			if err := db.QueryRowContext(t.Context(), `SELECT incarnation FROM public.observation_store WHERE singleton=1`).Scan(&identity); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := Migrate(t.Context(), db); err != nil {
					t.Fatal(err)
				}
			}
			var after string
			if err := db.QueryRowContext(t.Context(), `SELECT incarnation FROM public.observation_store WHERE singleton=1`).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if identity != after || !reflect.DeepEqual(before, schemaCatalog(t, db)) || history != schemaHistory(t, db) {
				t.Fatal("no-op changed identity, catalog or history")
			}
			if err := db.PingContext(t.Context()); err != nil {
				t.Fatal("migration closed host pool", err)
			}
		}
	})
	t.Run("rejection", func(t *testing.T) {
		for _, test := range []struct {
			state schemaState
			sql   string
		}{
			{schemaEmpty, `CREATE TABLE public.foreign_application(id integer)`},
			{schemaEmpty, schemaBaselineSQL},
			{schemaBootstrap, `UPDATE public.eino_agent_goose_version SET version_id=9`},
			{schemaCurrent, `DROP INDEX public.events_run_key_idx`},
		} {
			db := server.Database(t).Open(t)
			prepareSchema(t, db, test.state)
			schemaExec(t, db, test.sql)
			before, history := schemaCatalog(t, db), schemaHistory(t, db)
			if err := Migrate(t.Context(), db); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("unsupported schema: %v", err)
			}
			if !reflect.DeepEqual(before, schemaCatalog(t, db)) || history != schemaHistory(t, db) {
				t.Fatal("rejected migration changed catalog or history")
			}
		}
	})
	t.Run("rollback", func(t *testing.T) {
		db := server.Database(t).Open(t)
		files := fstest.MapFS{"00001_initial.sql": &fstest.MapFile{Data: []byte(schemaBaselineSQL + "\nSELECT 1/0;\n")}}
		if err := migrate(t.Context(), db, files, migrationDelegate(t)); err == nil {
			t.Fatal("injected baseline failure succeeded")
		}
		assertMigrationState(t, db, schemaBootstrap)
		if err := Migrate(t.Context(), db); err != nil {
			t.Fatal(err)
		}
		assertMigrationState(t, db, schemaCurrent)
	})
	t.Run("host_context", func(t *testing.T) {
		db := server.Database(t).Open(t)
		db.SetMaxOpenConns(1)
		schemaExec(t, db, `CREATE SCHEMA host; SET search_path=host,public; SET quote_all_identifiers=on;
   CREATE TEMP TABLE pg_tables(schemaname text, tablename text);
   INSERT INTO pg_tables VALUES('public','eino_agent_goose_version');`)
		for range 2 {
			if err := Migrate(t.Context(), db); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithCancel(t.Context())
		var beforePID int
		if err := db.QueryRowContext(t.Context(), "SELECT pg_catalog.pg_backend_pid()").Scan(&beforePID); err != nil {
			t.Fatal(err)
		}
		if err := Migrate(ctx, db); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		var afterPID int
		if err := db.QueryRowContext(t.Context(), "SELECT pg_catalog.pg_backend_pid()").Scan(&afterPID); err != nil {
			t.Fatal(err)
		}
		if beforePID != afterPID {
			t.Fatal("late cancellation discarded a returned host connection")
		}
		var path, quote string
		if err := db.QueryRowContext(t.Context(), `SELECT pg_catalog.current_setting('search_path'), pg_catalog.current_setting('quote_all_identifiers')`).Scan(&path, &quote); err != nil {
			t.Fatal(err)
		}
		if path != "host, public" || quote != "on" {
			t.Fatalf("host settings changed: %q %q", path, quote)
		}
	})
	t.Run("concurrent", func(t *testing.T) { testConcurrentMigration(t, server) })
	t.Run("canceled_waiter", func(t *testing.T) { testCanceledMigration(t, server) })
	t.Run("physical_cleanup", func(t *testing.T) { testPhysicalMigrationCleanup(t, server) })
}

func assertMigrationState(t *testing.T, db *sql.DB, want schemaState) {
	t.Helper()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got, inspectErr := inspectSchema(t.Context(), conn)
	closeErr := conn.Close()
	if inspectErr != nil || closeErr != nil || got != want {
		t.Fatalf("schema state = %d, want %d: %v / %v", got, want, inspectErr, closeErr)
	}
}

func migrationDelegate(t *testing.T) lock.SessionLocker {
	t.Helper()
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, 60), lock.WithUnlockTimeout(1, 5))
	if err != nil {
		t.Fatal(err)
	}
	return locker
}

func migrationFS(t *testing.T) fs.FS {
	t.Helper()
	files, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// gateLocker signals entry on an actual pinned connection and optionally keeps
// the first migrator inside its acquired lock until the test releases it.
type gateLocker struct {
	lock.SessionLocker
	entered  chan int
	acquired chan struct{}
	release  <-chan struct{}
}

func (l gateLocker) SessionLock(ctx context.Context, conn *sql.Conn) error {
	var pid int
	if err := conn.QueryRowContext(ctx, `SELECT pg_catalog.pg_backend_pid()`).Scan(&pid); err != nil {
		return err
	}
	l.entered <- pid
	if err := l.SessionLocker.SessionLock(ctx, conn); err != nil {
		return err
	}
	if l.acquired != nil {
		close(l.acquired)
	}
	if l.release != nil {
		select {
		case <-l.release:
		case <-ctx.Done():
			return errors.Join(ctx.Err(), l.SessionUnlock(context.WithoutCancel(ctx), conn))
		}
	}
	return nil
}
