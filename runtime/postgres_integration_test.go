//go:build postgres_integration

package runtime

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/store/postgres"
)

func TestPostgresRuntime(t *testing.T) {
	server := testpostgres.Start(t)
	t.Run("admission", func(t *testing.T) { testPostgresRuntimeAdmission(t, server) })
	t.Run("admission_rollback", func(t *testing.T) { testPostgresRuntimeAdmissionRollback(t, server) })
	t.Run("keyed_admission_race", func(t *testing.T) { testPostgresKeyedAdmissionRace(t, server) })
	t.Run("pending_resume", func(t *testing.T) { testPostgresRuntimeResume(t, server) })
	t.Run("optional_references", func(t *testing.T) { testPostgresRuntimeOptionalReferences(t, server) })
}

type postgresRuntimeFixture struct {
	ctx      context.Context
	database *testpostgres.Database
	db       *sql.DB
	store    *postgres.Store
}

func newPostgresRuntimeFixture(t *testing.T, server *testpostgres.Server) *postgresRuntimeFixture {
	t.Helper()
	database := server.Database(t)
	db := database.Open(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	if err := postgres.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	store, err := postgres.New(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	return &postgresRuntimeFixture{ctx: ctx, database: database, db: db, store: store}
}

func (f *postgresRuntimeFixture) reopen(t *testing.T) {
	t.Helper()
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	f.db = f.database.Open(t)
	store, err := postgres.New(f.ctx, f.db)
	if err != nil {
		t.Fatal(err)
	}
	f.store = store
}

func awaitPostgresRuntime(t *testing.T, ctx context.Context, handle Handle) Result {
	t.Helper()
	select {
	case result := <-handle.Done():
		return result
	case <-ctx.Done():
		t.Fatalf("runtime did not finish: %v", ctx.Err())
		return Result{}
	}
}
