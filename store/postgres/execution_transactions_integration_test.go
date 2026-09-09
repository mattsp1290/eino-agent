//go:build postgres_integration

package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
)

type executionContextKey struct{}

func testExecutionTransactions(t *testing.T, server *testpostgres.Server) {
	for _, outcome := range []string{"commit", "error", "panic", "cancel"} {
		t.Run(outcome, func(t *testing.T) {
			f := newRaceFixture(t, server, 2)
			f.seed(t)
			before := f.snapshot(t)
			calls := 0
			ctx, cancel := context.WithCancel(context.WithValue(f.ctx, executionContextKey{}, "execution"))
			defer cancel()
			execution := f.stores[0].Execution(session.RunFence{RunID: "run", ClaimToken: "old"})
			assertTransactionOutcome(t, outcome, func() error {
				return execution.WithinTx(ctx, func(callbackCtx context.Context, tx session.ExecutionStore) error {
					calls++
					if err := checkExecutionContext(callbackCtx, "execution"); err != nil {
						return err
					}
					if err := appendTransactionGraph(callbackCtx, tx, f.now); err != nil {
						return err
					}
					assertExecutionUncommitted(t, f, before)
					return finishTransaction(t, callbackCtx, outcome, cancel)
				})
			})
			if calls != 1 {
				t.Fatalf("%s callback calls = %d, want 1", outcome, calls)
			}
			if outcome == "commit" {
				assertCommittedExecution(t, f, false)
				if f.snapshot(t) == before {
					t.Fatal("committed execution transaction left no durable delta")
				}
			} else {
				assertExecutionUnchanged(t, f, before)
			}
		})
	}

	t.Run("nested_caught", func(t *testing.T) {
		f := newRaceFixture(t, server, 2)
		f.seed(t)
		before := f.snapshot(t)
		outerCalls, nestedCalls := 0, 0
		ctx := context.WithValue(f.ctx, executionContextKey{}, "nested")
		execution := f.stores[0].Execution(session.RunFence{RunID: "run", ClaimToken: "old"})
		assertTransactionOutcome(t, "commit", func() error {
			return execution.WithinTx(ctx, func(outerCtx context.Context, outer session.ExecutionStore) error {
				outerCalls++
				if err := checkExecutionContext(outerCtx, "nested"); err != nil {
					return err
				}
				err := outer.WithinTx(outerCtx, func(nestedCtx context.Context, nested session.ExecutionStore) error {
					nestedCalls++
					if err := checkExecutionContext(nestedCtx, "nested"); err != nil {
						return err
					}
					if err := appendTransactionGraph(nestedCtx, nested, f.now); err != nil {
						return err
					}
					assertExecutionUncommitted(t, f, before)
					return errTransactionAbort
				})
				if !errors.Is(err, errTransactionAbort) {
					return fmt.Errorf("nested callback lost error: %v", err)
				}
				if _, err := outer.AppendEvent(outerCtx, session.EventRecord{
					ID: "tx-outer-event", SessionID: "session", RunID: "run", Kind: "transaction_test",
					Payload: json.RawMessage(`{"ok":true}`), CreatedAt: f.now,
				}); err != nil {
					return err
				}
				assertExecutionUncommitted(t, f, before)
				return nil
			})
		})
		if outerCalls != 1 || nestedCalls != 1 {
			t.Fatalf("outer/nested callback calls = %d/%d, want 1/1", outerCalls, nestedCalls)
		}
		assertCommittedExecution(t, f, true)
		if f.snapshot(t) == before {
			t.Fatal("caught nested transaction left no durable delta")
		}
	})
}

func checkExecutionContext(ctx context.Context, want string) error {
	if ctx == nil || ctx.Value(executionContextKey{}) != want {
		return fmt.Errorf("transaction callback context missing expected value %q", want)
	}
	return nil
}

func appendTransactionGraph(ctx context.Context, tx session.ExecutionStore, now time.Time) error {
	message := session.Message{ID: "tx-assistant", SessionID: "session", RunID: "run", Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}
	if _, err := tx.AppendMessage(ctx, message); err != nil {
		return err
	}
	_, err := tx.AppendPart(ctx, session.Part{
		ID: "tx-reasoning", MessageID: message.ID, SessionID: message.SessionID, RunID: message.RunID,
		Kind: session.PartReasoning, Payload: json.RawMessage(`{"text":"transaction"}`), CreatedAt: now, UpdatedAt: now,
	})
	return err
}

func assertExecutionUncommitted(t *testing.T, f *raceFixture, before string) {
	t.Helper()
	if got := f.snapshot(t); got != before {
		t.Fatal("independent observer saw uncommitted execution rows")
	}
	batch, err := f.stores[1].ListMessages(f.ctx, "session", session.ReplayCursor{})
	if err != nil || len(batch.Messages) != 0 || len(batch.Parts) != 0 {
		t.Fatalf("independent reader saw uncommitted execution graph: messages=%d parts=%d err=%v", len(batch.Messages), len(batch.Parts), err)
	}
}

func assertCommittedExecution(t *testing.T, f *raceFixture, wantEvent bool) {
	t.Helper()
	batch, err := f.stores[1].ListMessages(f.ctx, "session", session.ReplayCursor{})
	if err != nil || len(batch.Messages) != 1 || len(batch.Parts) != 1 {
		t.Fatalf("committed execution graph counts messages=%d parts=%d err=%v", len(batch.Messages), len(batch.Parts), err)
	}
	if batch.Messages[0].ID != "tx-assistant" || batch.Parts[0].ID != "tx-reasoning" || batch.Parts[0].MessageID != batch.Messages[0].ID || !bytes.Equal(batch.Parts[0].Payload, json.RawMessage(`{"text":"transaction"}`)) {
		t.Fatal("committed execution graph does not preserve message, part, and payload relationship")
	}
	events, err := f.stores[1].ListEvents(f.ctx, "session", session.EventCursor{})
	if err != nil || (wantEvent && (len(events.Events) != 1 || events.Events[0].ID != "tx-outer-event")) || (!wantEvent && len(events.Events) != 0) {
		t.Fatalf("committed execution event state mismatch: count=%d err=%v", len(events.Events), err)
	}
	if err := f.dbs[0].PingContext(f.ctx); err != nil {
		t.Fatalf("writer pool unusable after execution transaction: %v", err)
	}
	if err := f.observer.PingContext(f.ctx); err != nil {
		t.Fatalf("observer pool unusable after execution transaction: %v", err)
	}
}

func assertExecutionUnchanged(t *testing.T, f *raceFixture, before string) {
	t.Helper()
	if got := f.snapshot(t); got != before {
		t.Fatal("failed execution transaction changed durable rows or revisions")
	}
	batch, err := f.stores[1].ListMessages(f.ctx, "session", session.ReplayCursor{})
	if err != nil || len(batch.Messages) != 0 || len(batch.Parts) != 0 {
		t.Fatalf("failed execution transaction left graph rows: messages=%d parts=%d err=%v", len(batch.Messages), len(batch.Parts), err)
	}
	if err := f.dbs[0].PingContext(f.ctx); err != nil {
		t.Fatalf("writer pool unusable after failed execution transaction: %v", err)
	}
	if err := f.observer.PingContext(f.ctx); err != nil {
		t.Fatalf("observer pool unusable after failed execution transaction: %v", err)
	}
}
