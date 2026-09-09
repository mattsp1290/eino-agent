//go:build postgres_integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
)

type staleFenceCase struct {
	name    string
	prepare func(*testing.T, *raceFixture, session.Run, session.ExecutionStore) func(context.Context, session.ExecutionStore) error
}

func testStaleMethods(t *testing.T, server *testpostgres.Server) {
	cases := []staleFenceCase{
		{"StartRun", func(t *testing.T, f *raceFixture, run session.Run, _ session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			at := f.now.Add(time.Second)
			return func(ctx context.Context, ex session.ExecutionStore) error { _, err := ex.StartRun(ctx, at); return err }
		}},
		{"RenewRunLease", func(_ *testing.T, _ *raceFixture, _ session.Run, _ session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			return func(ctx context.Context, ex session.ExecutionStore) error {
				_, err := ex.RenewRunLease(ctx, time.Minute)
				return err
			}
		}},
		{"SettleRun", func(t *testing.T, f *raceFixture, run session.Run, old session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			if _, err := old.StartRun(f.ctx, f.now.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			request := session.SettleRunRequest{Settlement: session.RunSettlement{Status: session.RunFailed, FinishedAt: f.now.Add(2 * time.Second), Error: "failed"}, Event: session.RunSettlementEvent{ID: "finished"}}
			return func(ctx context.Context, ex session.ExecutionStore) error {
				_, err := ex.SettleRun(ctx, request)
				return err
			}
		}},
		{"AppendMessage", func(_ *testing.T, f *raceFixture, run session.Run, _ session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			message := fencingAssistantMessage(f, run, "message")
			return func(ctx context.Context, ex session.ExecutionStore) error {
				_, err := ex.AppendMessage(ctx, message)
				return err
			}
		}},
		{"FinalizeAssistantMessage", func(t *testing.T, f *raceFixture, run session.Run, old session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			message := fencingAssistantMessage(f, run, "assistant")
			if _, err := old.AppendMessage(f.ctx, message); err != nil {
				t.Fatal(err)
			}
			return func(ctx context.Context, ex session.ExecutionStore) error {
				return ex.FinalizeAssistantMessage(ctx, message.ID)
			}
		}},
		{"AppendPart", func(t *testing.T, f *raceFixture, run session.Run, old session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			message := fencingAssistantMessage(f, run, "part-message")
			if _, err := old.AppendMessage(f.ctx, message); err != nil {
				t.Fatal(err)
			}
			part := fencingTextPart(f, run, message.ID, "part")
			return func(ctx context.Context, ex session.ExecutionStore) error {
				_, err := ex.AppendPart(ctx, part)
				return err
			}
		}},
		{"UpdatePart", func(t *testing.T, f *raceFixture, run session.Run, old session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			message := fencingAssistantMessage(f, run, "update-message")
			if _, err := old.AppendMessage(f.ctx, message); err != nil {
				t.Fatal(err)
			}
			part := fencingTextPart(f, run, message.ID, "update-part")
			if _, err := old.AppendPart(f.ctx, part); err != nil {
				t.Fatal(err)
			}
			part.Payload = json.RawMessage(`"updated"`)
			return func(ctx context.Context, ex session.ExecutionStore) error { return ex.UpdatePart(ctx, part) }
		}},
		{"AppendEvent", func(_ *testing.T, f *raceFixture, run session.Run, _ session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			event := session.EventRecord{ID: "event", SessionID: run.SessionID, RunID: run.ID, Kind: "custom", CreatedAt: f.now}
			return func(ctx context.Context, ex session.ExecutionStore) error {
				_, err := ex.AppendEvent(ctx, event)
				return err
			}
		}},
		{"CreateToolCall", func(t *testing.T, f *raceFixture, run session.Run, old session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			message := fencingAssistantMessage(f, run, "tool-message")
			if _, err := old.AppendMessage(f.ctx, message); err != nil {
				t.Fatal(err)
			}
			request := fencingToolCreateRequest(run, message.ID, "call", "create", f.now)
			return func(ctx context.Context, ex session.ExecutionStore) error {
				_, err := ex.CreateToolCall(ctx, request)
				return err
			}
		}},
		{"ClaimToolCall", func(t *testing.T, f *raceFixture, run session.Run, old session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			message := fencingAssistantMessage(f, run, "claim-message")
			if _, err := old.AppendMessage(f.ctx, message); err != nil {
				t.Fatal(err)
			}
			request := fencingToolCreateRequest(run, message.ID, "claim-call", "claim-create", f.now)
			if _, err := old.CreateToolCall(f.ctx, request); err != nil {
				t.Fatal(err)
			}
			claim := session.ClaimToolCallRequest{ID: "claim-call", ClaimedBy: "worker", ClaimToken: "tool-token", StartedAt: f.now.Add(time.Second), LeaseDuration: time.Minute, Event: fencingToolEvent("claim-running", f.now.Add(time.Second))}
			return func(ctx context.Context, ex session.ExecutionStore) error {
				_, err := ex.ClaimToolCall(ctx, claim)
				return err
			}
		}},
		{"SettleToolCall", func(t *testing.T, f *raceFixture, run session.Run, old session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			message := fencingAssistantMessage(f, run, "settle-message")
			if _, err := old.AppendMessage(f.ctx, message); err != nil {
				t.Fatal(err)
			}
			create := fencingToolCreateRequest(run, message.ID, "settle-call", "settle-create", f.now)
			if _, err := old.CreateToolCall(f.ctx, create); err != nil {
				t.Fatal(err)
			}
			call := create.Call
			claim := session.ClaimToolCallRequest{ID: "settle-call", ClaimedBy: "worker", ClaimToken: "tool-token", StartedAt: f.now.Add(time.Second), LeaseDuration: time.Minute, Event: fencingToolEvent("settle-running", f.now.Add(time.Second))}
			if _, err := old.ClaimToolCall(f.ctx, claim); err != nil {
				t.Fatal(err)
			}
			at := f.now.Add(2 * time.Second)
			settlement := session.ToolSettlement{ID: "settle-call", ClaimedBy: "worker", ClaimToken: "tool-token", Status: session.ToolCallCompleted, Output: json.RawMessage(`{"ok":true}`), CompletedAt: at, ResultMessage: session.Message{ID: call.ResultMessageID, SessionID: run.SessionID, RunID: run.ID, ParentID: message.ID, Role: session.RoleTool, CreatedAt: at, UpdatedAt: at}, ResultPart: session.Part{ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: run.SessionID, RunID: run.ID, Kind: session.PartToolResult, Payload: json.RawMessage(`{"ok":true}`), CreatedAt: at, UpdatedAt: at}}
			return func(ctx context.Context, ex session.ExecutionStore) error {
				_, err := ex.SettleToolCall(ctx, session.SettleToolCallRequest{Settlement: settlement, Event: fencingToolEvent("settle-terminal", at)})
				return err
			}
		}},
		{"StartContextEpoch", func(_ *testing.T, f *raceFixture, run session.Run, _ session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			epoch := session.ContextEpoch{ID: "epoch", SessionID: run.SessionID, CreatedAt: f.now}
			return func(ctx context.Context, ex session.ExecutionStore) error {
				_, err := ex.StartContextEpoch(ctx, epoch)
				return err
			}
		}},
		{"FinishContextEpoch", func(t *testing.T, f *raceFixture, run session.Run, old session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			epoch := session.ContextEpoch{ID: "finish-epoch", SessionID: run.SessionID, CreatedAt: f.now}
			if _, err := old.StartContextEpoch(f.ctx, epoch); err != nil {
				t.Fatal(err)
			}
			return func(ctx context.Context, ex session.ExecutionStore) error { return ex.FinishContextEpoch(ctx, epoch) }
		}},
		{"CreateModelRequest", func(_ *testing.T, f *raceFixture, run session.Run, _ session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			request := fencingModelRequest(run, "request", f.now, session.ModelRequestPrepared)
			return func(ctx context.Context, ex session.ExecutionStore) error {
				_, err := ex.CreateModelRequest(ctx, request)
				return err
			}
		}},
		{"UpdateModelRequest", func(t *testing.T, f *raceFixture, run session.Run, old session.ExecutionStore) func(context.Context, session.ExecutionStore) error {
			request := fencingModelRequest(run, "update-request", f.now, session.ModelRequestPrepared)
			if _, err := old.CreateModelRequest(f.ctx, request); err != nil {
				t.Fatal(err)
			}
			request.State, request.UpdatedAt = session.ModelRequestDispatchStarted, f.now.Add(time.Second)
			return func(ctx context.Context, ex session.ExecutionStore) error { return ex.UpdateModelRequest(ctx, request) }
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { runStaleFenceCase(t, server, tc) })
	}
}

func runStaleFenceCase(t *testing.T, server *testpostgres.Server, tc staleFenceCase) {
	f := newRaceFixture(t, server, 2)
	run := f.seed(t)
	old := f.stores[0].Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	invoke := tc.prepare(t, f, run, old)
	f.expire(t, run)
	winner, err := f.stores[1].ClaimRun(f.ctx, session.RunClaim{RunID: run.ID, OwnerID: "new", ClaimToken: "new", LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	before := f.snapshot(t)
	if err := invoke(f.ctx, f.stores[0].Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale %s error = %v", tc.name, err)
	}
	if got := f.snapshot(t); got != before {
		t.Fatalf("stale %s changed durable snapshot", tc.name)
	}
	if err := invoke(f.ctx, f.stores[1].Execution(session.RunFence{RunID: run.ID, ClaimToken: winner.ClaimToken})); err != nil {
		t.Fatalf("fresh %s error = %v", tc.name, err)
	}
}

func fencingAssistantMessage(f *raceFixture, run session.Run, id session.MessageID) session.Message {
	return session.Message{ID: id, SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: f.now, UpdatedAt: f.now}
}

func fencingTextPart(f *raceFixture, run session.Run, messageID session.MessageID, id session.PartID) session.Part {
	return session.Part{ID: id, MessageID: messageID, SessionID: run.SessionID, RunID: run.ID, Kind: session.PartText, Payload: json.RawMessage(`"text"`), CreatedAt: f.now, UpdatedAt: f.now}
}

func fencingToolEvent(id session.EventID, at time.Time) session.ToolTransitionEvent {
	return session.ToolTransitionEvent{ID: id, CreatedAt: at}
}

func fencingToolCreateRequest(run session.Run, messageID session.MessageID, id session.ToolCallID, eventID session.EventID, at time.Time) session.CreateToolCallRequest {
	call := session.ToolCall{ID: id, SessionID: run.SessionID, RunID: run.ID, MessageID: messageID, RequestPartID: session.PartID(string(id) + "-part"), ResultMessageID: session.MessageID(string(id) + "-result"), ResultPartID: session.PartID(string(id) + "-result-part"), Name: "tool", Input: json.RawMessage(`{}`), Status: session.ToolCallPending}
	payload := json.RawMessage(`{"id":"` + string(id) + `","name":"tool","arguments":{}}`)
	return session.CreateToolCallRequest{Call: call, RequestPart: session.Part{ID: call.RequestPartID, MessageID: messageID, SessionID: run.SessionID, RunID: run.ID, Kind: session.PartToolCall, Payload: payload, CreatedAt: at, UpdatedAt: at}, Event: fencingToolEvent(eventID, at)}
}

func fencingModelRequest(run session.Run, id session.ModelRequestID, at time.Time, state session.ModelRequestState) session.ModelRequestRecord {
	return session.ModelRequestRecord{ID: id, SessionID: run.SessionID, RunID: run.ID, Attempt: 0, Step: 0, State: state, Messages: json.RawMessage(`[]`), CreatedAt: at, UpdatedAt: at}
}

func testDelayedWriter(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 2)
	run := f.seed(t)
	f.expire(t, run)
	old := f.stores[0].Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	locked, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	workerCtx, cancelWorkers := context.WithCancel(f.ctx)
	var workers sync.WaitGroup
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		cancelWorkers()
		workers.Wait()
	})
	writerResult := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		ctx, cancel := context.WithTimeout(workerCtx, 20*time.Second)
		defer cancel()
		writerResult <- old.WithinTx(ctx, func(ctx context.Context, tx session.ExecutionStore) error {
			if _, err := tx.AppendMessage(ctx, fencingAssistantMessage(f, run, "held-message")); err != nil {
				return err
			}
			close(locked)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	waitRace(t, f.ctx, func() (bool, error) {
		select {
		case <-locked:
			return true, nil
		default:
			return false, nil
		}
	})
	reclaimResult := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		_, err := f.stores[1].ClaimRun(workerCtx, session.RunClaim{RunID: run.ID, OwnerID: "new", ClaimToken: "new", LeaseDuration: time.Minute})
		reclaimResult <- err
	}()
	waitRace(t, f.ctx, func() (bool, error) {
		var blocked bool
		err := f.observer.QueryRowContext(f.ctx, `SELECT $1 = ANY(pg_catalog.pg_blocking_pids($2))`, f.pids[0], f.pids[1]).Scan(&blocked)
		return blocked, err
	})
	select {
	case err := <-reclaimResult:
		t.Fatalf("reclaimer completed before writer release: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-writerResult; err != nil {
		t.Fatal(err)
	}
	if err := <-reclaimResult; err != nil {
		t.Fatal(err)
	}
	if got, err := f.stores[0].GetRun(f.ctx, run.ID); err != nil || got.ClaimToken != "new" {
		t.Fatalf("reclaim did not win after writer commit: token=%q err=%v", got.ClaimToken, err)
	}
	if got, err := f.stores[0].ListMessages(f.ctx, run.SessionID, session.ReplayCursor{}); err != nil || len(got.Messages) != 1 || got.Messages[0].ID != "held-message" {
		t.Fatalf("held writer message missing: messages=%d err=%v", len(got.Messages), err)
	}
	before := f.snapshot(t)
	if _, err := old.AppendMessage(f.ctx, fencingAssistantMessage(f, run, "stale-after-reclaim")); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("delayed stale write = %v", err)
	}
	if got := f.snapshot(t); got != before {
		t.Fatal("delayed stale write changed durable snapshot")
	}
}
