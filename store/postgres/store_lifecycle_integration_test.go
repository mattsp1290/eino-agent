//go:build postgres_integration

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/postgres"
)

func TestPostgresStoreLifecycle(t *testing.T) {
	server := testpostgres.Start(t)
	t.Run("readonly_constructor", func(t *testing.T) { testReadonlyConstructor(t, server) })
	t.Run("empty_schema", func(t *testing.T) { testEmptySchema(t, server) })
	t.Run("nil_closed_canceled", func(t *testing.T) { testConstructorInputs(t, server) })
	t.Run("reopen_incarnation", func(t *testing.T) { testReopenIncarnation(t, server) })
	t.Run("host_pool_settings", func(t *testing.T) { testHostStateUnchanged(t, server) })
	t.Run("transaction_visibility", func(t *testing.T) { testTransactionVisibility(t, server) })
}

func publicCatalogMarker(t *testing.T, db *sql.DB) string {
	t.Helper()
	var marker string
	if err := db.QueryRowContext(t.Context(), `SELECT COALESCE(string_agg(c.relname || ':' || c.relkind::text, ',' ORDER BY c.relname, c.relkind), '') FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'`).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	return marker
}

func incarnation(t *testing.T, db *sql.DB) string {
	t.Helper()
	var value string
	if err := db.QueryRowContext(t.Context(), `SELECT incarnation FROM public.observation_store WHERE singleton=1`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func testReadonlyConstructor(t *testing.T, server *testpostgres.Server) {
	db := server.Database(t).Open(t)
	if err := postgres.Migrate(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(t.Context(), `SET search_path = pg_catalog, pg_temp`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(t.Context(), `SET default_transaction_read_only = on`); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	beforeCatalog := publicCatalogMarker(t, db)
	beforeID := incarnation(t, db)
	if _, err := postgres.New(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if got := publicCatalogMarker(t, db); got != beforeCatalog {
		t.Fatalf("constructor changed public catalog: %q -> %q", beforeCatalog, got)
	}
	if got := incarnation(t, db); got != beforeID {
		t.Fatalf("constructor changed incarnation: %q -> %q", beforeID, got)
	}
	if db.Stats().MaxOpenConnections != 1 {
		t.Fatalf("constructor changed max open connections: %d", db.Stats().MaxOpenConnections)
	}
	var path string
	if err := db.QueryRowContext(t.Context(), `SHOW search_path`).Scan(&path); err != nil || path != "pg_catalog, pg_temp" {
		t.Fatalf("constructor changed search_path: %q, %v", path, err)
	}
	var readOnly string
	if err := db.QueryRowContext(t.Context(), `SHOW default_transaction_read_only`).Scan(&readOnly); err != nil || readOnly != "on" {
		t.Fatalf("constructor changed default_transaction_read_only: %q, %v", readOnly, err)
	}
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("constructor closed host pool: %v", err)
	}
}

func testEmptySchema(t *testing.T, server *testpostgres.Server) {
	db := server.Database(t).Open(t)
	before := publicCatalogMarker(t, db)
	if _, err := postgres.New(t.Context(), db); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("empty schema constructor error = %v, want ErrConflict", err)
	}
	if got := publicCatalogMarker(t, db); got != before {
		t.Fatalf("empty schema constructor wrote catalog: %q -> %q", before, got)
	}
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func testConstructorInputs(t *testing.T, server *testpostgres.Server) {
	if _, err := postgres.New(t.Context(), nil); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("nil pool error = %v, want ErrConflict", err)
	}
	db := server.Database(t).Open(t)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := postgres.New(t.Context(), db); err == nil {
		t.Fatal("closed pool constructor succeeded")
	}
	db = server.Database(t).Open(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := postgres.New(ctx, db); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled constructor error = %v, want context.Canceled", err)
	}
}

func testReopenIncarnation(t *testing.T, server *testpostgres.Server) {
	database := server.Database(t)
	firstDB := database.Open(t)
	if err := postgres.Migrate(t.Context(), firstDB); err != nil {
		t.Fatal(err)
	}
	first, err := postgres.New(t.Context(), firstDB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := postgres.New(t.Context(), firstDB); err != nil {
		t.Fatalf("repeated constructor: %v", err)
	}
	want := incarnation(t, firstDB)
	if _, err := first.CreateSession(t.Context(), session.Session{ID: "reopen"}); err != nil {
		t.Fatal(err)
	}
	if err := firstDB.Close(); err != nil {
		t.Fatal(err)
	}
	secondDB := database.Open(t)
	if err := postgres.Migrate(t.Context(), secondDB); err != nil {
		t.Fatal(err)
	}
	second, err := postgres.New(t.Context(), secondDB)
	if err != nil {
		t.Fatal(err)
	}
	if got := incarnation(t, secondDB); got != want {
		t.Fatalf("reopen incarnation = %q, want %q", got, want)
	}
	if _, err := second.GetSession(t.Context(), "reopen"); err != nil {
		t.Fatalf("reopen lost session: %v", err)
	}
}

func testHostStateUnchanged(t *testing.T, server *testpostgres.Server) {
	db := server.Database(t).Open(t)
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(t.Context(), `SET search_path = pg_catalog, pg_temp`); err != nil {
		t.Fatal(err)
	}
	if err := postgres.Migrate(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	before := db.Stats()
	var beforePath string
	if err := db.QueryRowContext(t.Context(), `SHOW search_path`).Scan(&beforePath); err != nil {
		t.Fatal(err)
	}
	st, err := postgres.New(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	record := session.Session{ID: "qualified-host-state"}
	if _, err := st.CreateSession(t.Context(), record); err != nil {
		t.Fatalf("qualified write with public excluded: %v", err)
	}
	if _, err := st.GetSession(t.Context(), record.ID); err != nil {
		t.Fatalf("qualified read with public excluded: %v", err)
	}
	var afterPath string
	if err := db.QueryRowContext(t.Context(), `SHOW search_path`).Scan(&afterPath); err != nil {
		t.Fatal(err)
	}
	if afterPath != beforePath || db.Stats().MaxOpenConnections != before.MaxOpenConnections {
		t.Fatalf("host state changed: path %q -> %q, max open %d -> %d", beforePath, afterPath, before.MaxOpenConnections, db.Stats().MaxOpenConnections)
	}
}

func testTransactionVisibility(t *testing.T, server *testpostgres.Server) {
	database := server.Database(t)
	db, root := migratePostgresDatabase(t, database)
	independentDB := database.Open(t)
	if err := postgres.Migrate(t.Context(), independentDB); err != nil {
		t.Fatal(err)
	}
	independent, err := postgres.New(t.Context(), independentDB)
	if err != nil {
		t.Fatal(err)
	}
	const id = session.ID("transaction-visible")
	err = root.WithinTx(t.Context(), func(ctx context.Context, tx session.Store) error {
		if _, err := tx.CreateSession(ctx, session.Session{ID: id}); err != nil {
			return err
		}
		if _, err := tx.GetSession(ctx, id); err != nil {
			return errors.New("transaction could not read its own write")
		}
		if _, err := independent.GetSession(ctx, id); !errors.Is(err, session.ErrNotFound) {
			return errors.New("independent root saw uncommitted write")
		}
		if reader, ok := tx.(session.SessionDiscoveryReader); ok {
			_, err := reader.ListSessions(ctx, session.SessionDiscoveryQuery{WorkspaceID: "missing"})
			if !errors.Is(err, session.ErrDiscoveryReader) {
				return errors.New("transaction discovery reader was not rejected")
			}
		} else {
			return errors.New("transaction lost discovery capability")
		}
		if reader, ok := tx.(session.ObservationReader); ok {
			_, err := reader.ReadObservationRevision(ctx, id)
			if !errors.Is(err, session.ErrObservationReader) {
				return errors.New("transaction observation reader was not rejected")
			}
		} else {
			return errors.New("transaction lost observation capability")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := independent.GetSession(t.Context(), id); err != nil {
		t.Fatalf("independent root missed committed write: %v", err)
	}
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("host pool unusable after transaction: %v", err)
	}
}
