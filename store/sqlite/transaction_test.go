package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func TestPublicNestingAndRollback(t *testing.T) {
	st, err := openSQLiteFixture(t.Context(), filepath.Join(t.TempDir(), "tx.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.db.Close() }()
	sentinel := errors.New("callback error")
	calls := 0
	err = st.WithinTx(t.Context(), func(ctx context.Context, tx session.Store) error {
		calls++
		if err := tx.WithinTx(ctx, func(ctx context.Context, nested session.Store) error {
			_, err := nested.CreateSession(ctx, session.Session{ID: "nested"})
			if err != nil {
				return err
			}
			return sentinel
		}); !errors.Is(err, sentinel) {
			t.Fatalf("nested error: %v", err)
		}
		return nil
	})
	if err != nil || calls != 1 {
		t.Fatalf("nesting result=%v calls=%d", err, calls)
	}
	if _, err := st.GetSession(t.Context(), "nested"); err != nil {
		t.Fatal("public nesting unexpectedly rolled back", err)
	}
	for _, mode := range []string{"error", "panic", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			func() {
				defer func() {
					if v := recover(); mode == "panic" && v != sentinel {
						t.Errorf("panic changed: %v", v)
					}
				}()
				err = st.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
					if _, err := tx.CreateSession(ctx, session.Session{ID: session.ID(mode)}); err != nil {
						return err
					}
					switch mode {
					case "panic":
						panic(sentinel)
					case "cancel":
						cancel()
						return nil
					default:
						return sentinel
					}
				})
			}()
			if mode == "error" && !errors.Is(err, sentinel) {
				t.Fatal(err)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if _, err := st.GetSession(t.Context(), session.ID(mode)); !errors.Is(err, session.ErrNotFound) {
				t.Fatalf("transaction survived %s: %v", mode, err)
			}
		})
	}
}

func TestCaughtOperationFailureUsesSavepoint(t *testing.T) {
	st, _, call, now := setupToolTransitionTest(t)
	defer func() { _ = st.db.Close() }()
	execBaseline(t, st.db, "CREATE TRIGGER fail_operation BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'injected operation failure'); END")
	err := st.WithinTx(t.Context(), func(ctx context.Context, tx session.Store) error {
		execution := tx.Execution(session.RunFence{RunID: call.RunID, ClaimToken: "claim-tool-run"})
		if _, err := execution.CreateToolCall(ctx, sqliteCreateRequest(call, "failed-event", now)); err == nil {
			t.Fatal("operation unexpectedly succeeded")
		}
		_, err := tx.CreateSession(ctx, session.Session{ID: "unrelated-success"})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetSession(t.Context(), "unrelated-success"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetToolCall(t.Context(), call.ID); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("failed call survived: %v", err)
	}
	var count int
	if err := st.db.QueryRow("SELECT count(*) FROM parts").Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed request part survived: count=%d err=%v", count, err)
	}
}

func TestWriterWaitHonorsCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "writer.db")
	st, err := openSQLiteFixture(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.db.Close() }()
	other, err := reopenSQLiteFixture(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.db.Close() }()
	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- st.WithinTx(t.Context(), func(context.Context, session.Store) error { close(locked); <-release; return nil })
	}()
	<-locked
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = other.CreateSession(ctx, session.Session{ID: "canceled-writer"})
	close(release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("writer cancellation: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("writer remained in SQLite busy handler")
	}
	if _, err := other.CreateSession(t.Context(), session.Session{ID: "after-cancellation"}); err != nil {
		t.Fatal("pool not reusable", err)
	}
}
