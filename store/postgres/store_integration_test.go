//go:build postgres_integration

package postgres_test

import (
	"database/sql"
	"testing"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/postgres"
	"github.com/mattsp1290/eino-agent/store/storetest"
)

func TestPostgresStore(t *testing.T) {
	server := testpostgres.Start(t)
	factory := postgresFactory(server)
	t.Run("contract", func(t *testing.T) { storetest.Run(t, factory) })
	t.Run("discovery", func(t *testing.T) { storetest.RunDiscovery(t, factory) })
}

func postgresFactory(server *testpostgres.Server) storetest.Factory {
	return func(tb testing.TB) storetest.Subject {
		t, ok := tb.(*testing.T)
		if !ok {
			tb.Fatal("postgres factory requires *testing.T")
		}
		database := server.Database(t)
		db := database.Open(t)
		if err := postgres.Migrate(t.Context(), db); err != nil {
			t.Fatalf("migrate postgres fixture: %v", err)
		}
		st, err := postgres.New(t.Context(), db)
		if err != nil {
			t.Fatalf("open postgres store: %v", err)
		}
		return storetest.Subject{Store: st, Cleanup: func() { _ = db.Close() }}
	}
}

func migratePostgresDatabase(t *testing.T, database *testpostgres.Database) (*sql.DB, session.Store) {
	t.Helper()
	db := database.Open(t)
	if err := postgres.Migrate(t.Context(), db); err != nil {
		t.Fatalf("migrate postgres fixture: %v", err)
	}
	st, err := postgres.New(t.Context(), db)
	if err != nil {
		t.Fatalf("open postgres store: %v", err)
	}
	return db, st
}
