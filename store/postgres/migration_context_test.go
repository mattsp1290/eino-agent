package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestMigrationContextInterruptCancelsOnlyWhenUnbound is a white-box,
// package-internal proof of the invariant migrationContext.interrupt's doc
// depends on: canceling the pgx-visible context is safe only while nothing
// is bound (bind refuses once a cause is recorded, so no *sql.Conn can
// subsequently observe the canceled context), and interrupt must actually
// do so promptly in that case - that is what lets a parent cancellation
// reach goose's own connection-pool wait before SessionLock has bound
// anything (see TestPostgresMigration/cancel_before_bind_aborts_pool_wait
// for the end-to-end regression this guards: d4f325b's interrupt never
// canceled m.Context at all, so a parent cancellation was invisible to
// goose's p.db.Conn(ctx) pool wait until a connection actually freed up).
//
// This test sets m.sqlConn directly rather than going through bind, so it
// needs no real database connection: interrupt only ever compares
// m.sqlConn against nil to decide whether to cancel, so a non-nil, unused
// *sql.Conn value is enough to exercise the "bound" branch.
func TestMigrationContextInterruptCancelsOnlyWhenUnbound(t *testing.T) {
	t.Run("unbound", func(t *testing.T) {
		m := newMigrationContext(context.Background())
		t.Cleanup(func() { _ = m.stop() })
		cause := errors.New("probe interrupt")

		done := make(chan struct{})
		go func() {
			m.interrupt(cause)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("interrupt did not return promptly while unbound")
		}

		select {
		case <-m.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("interrupt did not cancel m.Context while nothing was bound")
		}
		if got := context.Cause(m.Context); !errors.Is(got, cause) {
			t.Fatalf("m.Context canceled with the wrong cause: %v, want %v", got, cause)
		}
	})

	t.Run("bound", func(t *testing.T) {
		m := newMigrationContext(context.Background())
		// Fake a bound state without a real database connection: interrupt
		// only ever checks m.sqlConn == nil, never dereferences it.
		m.mu.Lock()
		m.sqlConn = new(sql.Conn)
		m.mu.Unlock()
		t.Cleanup(func() {
			m.mu.Lock()
			m.sqlConn = nil // take would otherwise try to discard the fake conn.
			m.mu.Unlock()
			_ = m.stop()
		})
		cause := errors.New("probe interrupt")

		done := make(chan struct{})
		go func() {
			m.interrupt(cause)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("interrupt did not return promptly while bound")
		}

		select {
		case <-m.Done():
			t.Fatal("interrupt canceled m.Context while a *sql.Conn was still bound")
		case <-time.After(50 * time.Millisecond):
		}
		if got := m.err(); !errors.Is(got, cause) {
			t.Fatalf("interrupt did not record the cause while bound: %v, want %v", got, cause)
		}
	})
}

// TestMigrationContextReportsInterruptCauseOnce is a white-box proof that
// take (via stop/discard/err) reports an interrupt cause exactly once, no
// matter how many of SessionLock's/unlockAndRestore's teardown calls run
// afterward - the fix for a d4f325b-era regression where a canceled run's
// cause surfaced two to four times in one returned error (SessionLock's
// deferred stop, migrate's backstop stop, and unlockAndRestore's now-removed
// separate err() call each joined it in again).
func TestMigrationContextReportsInterruptCauseOnce(t *testing.T) {
	m := newMigrationContext(context.Background())
	m.mu.Lock()
	m.sqlConn = new(sql.Conn) // fake bound state; cleared below before discard.
	m.mu.Unlock()
	cause := errors.New("probe interrupt")
	m.interrupt(cause)

	// Clear the fake conn before discard so releaseConn never tries to call
	// discardPGXConnection on an unusable *sql.Conn value: this test is
	// about how many times the cause is reported, not the discard itself.
	m.mu.Lock()
	m.sqlConn = nil
	m.mu.Unlock()

	first := m.discard()
	if !errors.Is(first, cause) {
		t.Fatalf("discard did not report the interrupt cause: %v", first)
	}
	if n := strings.Count(first.Error(), cause.Error()); n != 1 {
		t.Fatalf("discard's error contains the cause %d times, want 1: %v", n, first)
	}
	if second := m.stop(); second != nil {
		t.Fatalf("stop after discard re-reported the cause: %v", second)
	}
	if third := m.err(); third != nil {
		t.Fatalf("err after discard still reports state take already consumed: %v", third)
	}
}
