//go:build postgres_integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
)

type storeTransactionContextKey struct{}

func testStoreTransactions(t *testing.T, server *testpostgres.Server) {
	for _, outcome := range []string{"commit", "error", "panic", "cancel"} {
		t.Run(outcome, func(t *testing.T) {
			f := newRaceFixture(t, server, 2)
			baseline := f.snapshot(t)
			ctx, cancel := context.WithCancel(context.WithValue(f.ctx, storeTransactionContextKey{}, "store-callback"))
			defer cancel()
			calls := 0
			assertTransactionOutcome(t, outcome, func() error {
				return f.stores[0].WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
					calls++
					if err := writeStoreTransaction(ctx, f, tx, outcome); err != nil {
						return err
					}
					assertStoreUncommitted(t, f, baseline, outcome+"-session")
					return finishTransaction(t, ctx, outcome, cancel)
				})
			})
			if calls != 1 {
				t.Fatalf("callback count = %d", calls)
			}
			if outcome == "commit" {
				assertStoreCommitted(t, f, outcome)
			} else if f.snapshot(t) != baseline {
				t.Fatal("rollback changed durable rows or revisions")
			}
			if _, err := f.stores[0].CreateSession(f.ctx, session.Session{ID: "after-transaction"}); err != nil {
				t.Fatalf("writer unusable after transaction: %v", err)
			}
		})
	}
	t.Run("nested_caught", func(t *testing.T) { testStoreNestedCaught(t, server) })
}

func writeStoreTransaction(ctx context.Context, f *raceFixture, tx session.Store, prefix string) error {
	if tx == nil || ctx == nil || ctx.Value(storeTransactionContextKey{}) != "store-callback" {
		return errors.New("transaction callback handle or context invalid")
	}
	now := f.now
	sid := session.ID(prefix + "-session")
	runID := session.RunID(prefix + "-run")
	if _, err := tx.CreateSession(ctx, session.Session{ID: sid, CreatedAt: now, UpdatedAt: now}); err != nil {
		return err
	}
	run, err := tx.AdmitRun(ctx, session.Run{ID: runID, SessionID: sid, OwnerID: "owner", ClaimToken: prefix + "-token", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		return err
	}
	message := session.Message{ID: session.MessageID(prefix + "-message"), SessionID: sid, RunID: runID, Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}
	if _, err := tx.Execution(session.RunFence{RunID: runID, ClaimToken: run.ClaimToken}).AppendMessage(ctx, message); err != nil {
		return err
	}
	if got, err := tx.GetSession(ctx, sid); err != nil || got.ID != sid {
		return fmt.Errorf("transaction session read mismatch: %v", err)
	}
	if got, err := tx.GetRun(ctx, runID); err != nil || got.ID != runID || got.SessionID != sid {
		return fmt.Errorf("transaction run read mismatch: %v", err)
	}
	batch, err := tx.ListMessages(ctx, sid, session.ReplayCursor{})
	if err != nil || len(batch.Messages) != 1 || batch.Messages[0].ID != message.ID {
		return fmt.Errorf("transaction message read mismatch: %v", err)
	}
	return nil
}

func assertStoreUncommitted(t *testing.T, f *raceFixture, baseline, id string) {
	t.Helper()
	if _, err := f.stores[1].GetSession(f.ctx, session.ID(id)); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("separate store saw uncommitted session: %v", err)
	}
	if got := f.snapshot(t); got != baseline {
		t.Fatal("observer saw uncommitted transaction state")
	}
}

func assertStoreCommitted(t *testing.T, f *raceFixture, prefix string) {
	t.Helper()
	sid := session.ID(prefix + "-session")
	runID := session.RunID(prefix + "-run")
	if got, err := f.stores[1].GetSession(f.ctx, sid); err != nil || got.ID != sid {
		t.Fatalf("committed session mismatch: %v", err)
	}
	if got, err := f.stores[1].GetRun(f.ctx, runID); err != nil || got.ID != runID || got.SessionID != sid {
		t.Fatalf("committed run mismatch: %v", err)
	}
	batch, err := f.stores[1].ListMessages(f.ctx, sid, session.ReplayCursor{})
	if err != nil || len(batch.Messages) != 1 || batch.Messages[0].ID != session.MessageID(prefix+"-message") {
		t.Fatalf("committed messages mismatch: count=%d error=%v", len(batch.Messages), err)
	}
}

func testStoreNestedCaught(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 2)
	baseline := f.snapshot(t)
	ctx := context.WithValue(f.ctx, storeTransactionContextKey{}, "store-callback")
	outerCalls, nestedCalls := 0, 0
	assertTransactionOutcome(t, "commit", func() error {
		return f.stores[0].WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
			outerCalls++
			if tx == nil || ctx == nil || ctx.Value(storeTransactionContextKey{}) != "store-callback" {
				return errors.New("outer callback handle or context invalid")
			}
			err := tx.WithinTx(ctx, func(nestedCtx context.Context, nested session.Store) error {
				nestedCalls++
				if nested == nil || nestedCtx == nil || nestedCtx.Value(storeTransactionContextKey{}) != "store-callback" {
					return errors.New("nested callback handle or context invalid")
				}
				if _, err := nested.CreateSession(nestedCtx, session.Session{ID: "nested-success"}); err != nil {
					return err
				}
				return errTransactionAbort
			})
			if !errors.Is(err, errTransactionAbort) {
				return fmt.Errorf("nested callback lost error: %v", err)
			}
			if _, err := tx.GetSession(ctx, "nested-success"); err != nil {
				return err
			}
			assertStoreUncommitted(t, f, baseline, "nested-success")
			return nil
		})
	})
	if outerCalls != 1 || nestedCalls != 1 {
		t.Fatalf("callback counts outer=%d nested=%d", outerCalls, nestedCalls)
	}
	if _, err := f.stores[1].GetSession(f.ctx, "nested-success"); err != nil {
		t.Fatalf("nested caught write did not commit: %v", err)
	}
}
