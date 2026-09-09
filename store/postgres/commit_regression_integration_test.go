//go:build postgres_integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/postgres"
)

// testCommitRollbackTag ensures a caught statement error cannot make
// WithinTx report success when PostgreSQL will turn COMMIT into ROLLBACK.
func testCommitRollbackTag(t *testing.T, server *testpostgres.Server) {
	db := server.Database(t).Open(t)
	defer func() { _ = db.Close() }()
	if err := postgres.Migrate(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	store, err := postgres.New(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(t.Context(), "ALTER TABLE public.runs RENAME TO runs_missing"); err != nil {
		t.Fatal(err)
	}

	record := session.Session{ID: "commit-rollback-tag", WorkspaceID: "workspace"}
	err = store.WithinTx(t.Context(), func(ctx context.Context, tx session.Store) error {
		if _, err := tx.CreateSession(ctx, record); err != nil {
			return err
		}
		if _, err := tx.ListUnfinishedRuns(ctx); err == nil {
			return errors.New("renamed runs relation unexpectedly remained readable")
		} else {
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
				return fmt.Errorf("renamed runs read error = %w", err)
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	})
	if !errors.Is(err, pgx.ErrTxCommitRollback) {
		t.Fatalf("WithinTx error = %v, want pgx.ErrTxCommitRollback", err)
	}

	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM public.sessions WHERE id = $1", []byte(record.ID)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("aborted transaction left %d session rows", count)
	}
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("borrowed pool unusable after rollback-tag commit: %v", err)
	}
}
