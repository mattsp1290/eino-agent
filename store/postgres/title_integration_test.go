//go:build postgres_integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
)

func TestPostgresSessionTitle(t *testing.T) {
	server := testpostgres.Start(t)
	t.Run("commit_order", func(t *testing.T) { testPostgresTitleCommitOrder(t, server) })
	t.Run("rollback_order", func(t *testing.T) { testPostgresTitleRollbackOrder(t, server) })
	t.Run("cancellation", func(t *testing.T) { testPostgresTitleCancellation(t, server) })
	t.Run("atomic_projection_revision", func(t *testing.T) { testPostgresTitleAtomicRollback(t, server) })
}

func testPostgresTitleCommitOrder(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 2)
	f.seed(t)
	locked := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- f.stores[0].WithinTx(f.ctx, func(ctx context.Context, tx session.Store) error {
			if _, err := tx.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: "session", Title: "first"}); err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked
	secondDone := make(chan error, 1)
	go func() {
		_, err := f.stores[1].SetSessionTitle(f.ctx, session.SessionTitleRequest{SessionID: "session", Title: "second"})
		secondDone <- err
	}()
	waitForPostgresTitleLock(t, f)
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	got, err := f.stores[0].GetSession(f.ctx, "session")
	if err != nil || got.Title != "second" {
		t.Fatalf("serialized session = %#v, error = %v", got, err)
	}
}

func testPostgresTitleRollbackOrder(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 2)
	f.seed(t)
	locked := make(chan struct{})
	release := make(chan struct{})
	rollback := errors.New("rollback first rename")
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- f.stores[0].WithinTx(f.ctx, func(ctx context.Context, tx session.Store) error {
			if _, err := tx.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: "session", Title: "rolled back"}); err != nil {
				return err
			}
			close(locked)
			<-release
			return rollback
		})
	}()
	<-locked
	secondDone := make(chan error, 1)
	go func() {
		_, err := f.stores[1].SetSessionTitle(f.ctx, session.SessionTitleRequest{SessionID: "session", Title: "committed second"})
		secondDone <- err
	}()
	waitForPostgresTitleLock(t, f)
	close(release)
	if err := <-firstDone; !errors.Is(err, rollback) {
		t.Fatalf("first writer error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	got, err := f.stores[0].GetSession(f.ctx, "session")
	if err != nil || got.Title != "committed second" {
		t.Fatalf("serialized session = %#v, error = %v", got, err)
	}
}

func testPostgresTitleCancellation(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 2)
	f.seed(t)
	locked := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- f.stores[0].WithinTx(f.ctx, func(ctx context.Context, tx session.Store) error {
			if _, err := tx.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: "session", Title: "committed"}); err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked
	waitCtx, cancel := context.WithCancel(f.ctx)
	waiter := make(chan error, 1)
	go func() {
		_, err := f.stores[1].SetSessionTitle(waitCtx, session.SessionTitleRequest{SessionID: "session", Title: "canceled"})
		waiter <- err
	}()
	waitForPostgresTitleLock(t, f)
	cancel()
	if err := <-waiter; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter error = %v, want context.Canceled", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	got, err := f.stores[0].GetSession(f.ctx, "session")
	if err != nil || got.Title != "committed" {
		t.Fatalf("session after cancellation = %#v, error = %v", got, err)
	}
}

func testPostgresTitleAtomicRollback(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 1)
	f.seed(t)
	before := f.snapshot(t)
	rollback := errors.New("rollback title transaction")
	err := f.stores[0].WithinTx(f.ctx, func(ctx context.Context, tx session.Store) error {
		result, err := tx.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: "session", Title: "uncommitted"})
		if err != nil {
			return err
		}
		if !result.Changed {
			t.Fatal("transactional rename reported no change")
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("transaction error = %v", err)
	}
	if after := f.snapshot(t); after != before {
		t.Fatal("rolled-back rename changed record, projection, or observation revision")
	}
}

func waitForPostgresTitleLock(t *testing.T, f *raceFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	waitRace(t, ctx, func() (bool, error) {
		var waiting bool
		err := f.observer.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'
			AND query LIKE '%sessions%'
		)`).Scan(&waiting)
		return waiting, err
	})
}
