package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func TestWriterCommitWaitHonorsCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit.db")
	writer, err := openSQLiteFixture(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.db.Close() }()
	execBaseline(t, writer.db, "PRAGMA journal_mode=DELETE")
	reader, err := reopenSQLiteFixture(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.db.Close() }()
	tx, err := reader.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var count int
	if err := tx.QueryRowContext(t.Context(), "SELECT count(*) FROM sessions").Scan(&count); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	calls := 0
	started := time.Now()
	err = writer.WithinTx(ctx, func(ctx context.Context, store session.Store) error {
		calls++
		_, err := store.CreateSession(ctx, session.Session{ID: "rolled-back"})
		return err
	})
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatalf("commit cancellation=%v callbacks=%d", err, calls)
	}
	if elapsed > 750*time.Millisecond {
		t.Errorf("commit waited in SQLite busy handler for %s", elapsed)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertDiscoveryConnectionSettings(t, writer, 5000)
	if _, err := writer.GetSession(t.Context(), "rolled-back"); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("canceled commit persisted data: %v", err)
	}
	if _, err := writer.CreateSession(t.Context(), session.Session{ID: "after-cancellation"}); err != nil {
		t.Fatal("host pool not reusable", err)
	}
}

func TestNilAdapterPreservesReaderValidation(t *testing.T) {
	for _, store := range []*Store{nil, {}} {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := store.ListSessions(ctx, session.SessionDiscoveryQuery{WorkspaceID: "A"}); err != context.Canceled {
			t.Errorf("canceled discovery: %v", err)
		}
		if _, err := store.ListSessions(t.Context(), session.SessionDiscoveryQuery{}); err != session.ErrDiscoveryQuery {
			t.Errorf("invalid discovery query: %v", err)
		}
		if _, err := store.ReadObservationSnapshot(t.Context(), "s", session.ObservationLimits{}); err != session.ErrObservationLimits {
			t.Errorf("invalid observation limits: %v", err)
		}
	}
}
