package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func TestDiscoveryCancelsDatabaseLockWait(t *testing.T) {
	for _, mode := range []string{"deadline", "manual"} {
		t.Run(mode, func(t *testing.T) {
			s, locker := lockedDiscoveryStore(t)
			ctx, cancel := context.WithCancel(t.Context())
			want := context.Canceled
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 30*time.Millisecond)
				want = context.DeadlineExceeded
			} else {
				timer := time.AfterFunc(30*time.Millisecond, cancel)
				defer timer.Stop()
			}
			defer cancel()
			started := time.Now()
			requireDiscoveryError(t, s, ctx, session.SessionDiscoveryQuery{WorkspaceID: "A"}, want)
			if elapsed := time.Since(started); elapsed > 750*time.Millisecond {
				t.Fatalf("canceled database lock wait took %s", elapsed)
			}
			if _, err := locker.ExecContext(t.Context(), "ROLLBACK"); err != nil {
				t.Fatal(err)
			}
			assertDiscoveryConnectionSettings(t, s, 5000)
			putDiscovery(t, s, session.Session{ID: "after-cancel", WorkspaceID: "A"})
			page := listDiscovery(t, s, session.SessionDiscoveryQuery{WorkspaceID: "A"})
			if len(page.Sessions) != 1 || page.Sessions[0].ID != "after-cancel" {
				t.Fatal(page)
			}
		})
	}
}

func TestDiscoveryDatabaseLockWaitHasFiniteCeiling(t *testing.T) {
	s, locker := lockedDiscoveryStore(t)
	// A short original timeout proves the retry ceiling without slowing the
	// suite by five seconds. The original value must survive either outcome.
	if _, err := s.db.Exec("PRAGMA busy_timeout=40"); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	requireDiscoveryError(t, s, t.Context(), session.SessionDiscoveryQuery{WorkspaceID: "A"}, session.ErrDiscoveryStore)
	if elapsed := time.Since(started); elapsed > 750*time.Millisecond {
		t.Fatalf("contention ceiling was ignored: %s", elapsed)
	}
	if _, err := locker.ExecContext(t.Context(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	assertDiscoveryConnectionSettings(t, s, 40)
	listDiscovery(t, s, session.SessionDiscoveryQuery{WorkspaceID: "A"})
	assertDiscoveryConnectionSettings(t, s, 40)
}

func TestDiscoveryRetriesUntilDatabaseLockReleased(t *testing.T) {
	s, locker := lockedDiscoveryStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := s.ListSessions(ctx, session.SessionDiscoveryQuery{WorkspaceID: "A"})
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("discovery returned before the exclusive lock was released: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := locker.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertDiscoveryConnectionSettings(t, s, 5000)
}

func lockedDiscoveryStore(t *testing.T) (*Store, *sql.Conn) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "locked.db")
	s := discoveryStore(t, path)
	writer := discoveryStore(t, path)
	locker, err := writer.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = locker.ExecContext(context.Background(), "ROLLBACK")
		_ = locker.Close()
	})
	if _, err := locker.ExecContext(t.Context(), "BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	return s, locker
}

func assertDiscoveryConnectionSettings(t *testing.T, s *Store, timeout int) {
	t.Helper()
	var got, foreignKeys int
	if err := s.db.QueryRow("PRAGMA busy_timeout").Scan(&got); err != nil || got != timeout {
		t.Fatalf("busy timeout = %d, %v; want %d", got, err, timeout)
	}
	if err := s.db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys); err != nil || foreignKeys != 1 {
		t.Fatalf("connection settings lost: foreign_keys=%d, %v", foreignKeys, err)
	}
}
