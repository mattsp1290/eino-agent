package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

// Subject is one store implementation under test.
type Subject struct {
	Store   session.Store
	Cleanup func()
}

// Factory creates an isolated store subject for one test. Each call must return
// a fresh store namespace so subtests do not share durable state.
type Factory func(testing.TB) Subject

// Run executes the durable store contract suite against a store implementation.
func Run(t *testing.T, factory Factory) {
	t.Helper()

	t.Run("atomic run ownership", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := createSession(t, ctx, subject.Store, "session-ownership")
		const contenders = 8
		start := make(chan struct{})
		type result struct {
			run session.Run
			err error
		}
		results := make(chan result, contenders)
		var wg sync.WaitGroup
		for i := range contenders {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				r, err := subject.Store.AdmitRun(ctx, run(session.RunID(fmt.Sprintf("run-%02d", i)), s.ID, fmt.Sprintf("owner-%02d", i)), time.Minute)
				results <- result{run: r, err: err}
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)
		var admitted session.Run
		var successes int
		var busy int
		for result := range results {
			switch {
			case result.err == nil:
				successes++
				admitted = result.run
			case errors.Is(result.err, session.ErrSessionBusy):
				busy++
			default:
				t.Fatalf("unexpected concurrent admit err: %v", result.err)
			}
		}
		if successes != 1 || busy != contenders-1 {
			t.Fatalf("successes=%d busy=%d, want 1/%d", successes, busy, contenders-1)
		}
		admitted.Status = session.RunInterrupted
		admitted.FinishedAt = admitted.CreatedAt.Add(time.Minute)
		execution := subject.Store.Execution(session.RunFence{RunID: admitted.ID, ClaimToken: admitted.ClaimToken})
		if err := execution.SettleRun(ctx, admitted, nil); err != nil {
			t.Fatalf("finish first run: %v", err)
		}
		if _, err := subject.Store.AdmitRun(ctx, run("run-3", s.ID, "owner-3"), time.Minute); err != nil {
			t.Fatalf("admit after terminal run: %v", err)
		}
	})

	t.Run("run admission is insert only", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := createSession(t, ctx, subject.Store, "session-insert-only")
		candidate := run("run-insert-only", s.ID, "owner")
		admitted := admitRun(t, ctx, subject.Store, candidate)
		if _, err := subject.Store.AdmitRun(ctx, candidate, time.Minute); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("duplicate active admission err = %v, want ErrConflict", err)
		}
		admitted.Status = session.RunCompleted
		admitted.FinishedAt = admitted.CreatedAt.Add(time.Minute)
		execution := subject.Store.Execution(session.RunFence{RunID: admitted.ID, ClaimToken: admitted.ClaimToken})
		if err := execution.SettleRun(ctx, admitted, nil); err != nil {
			t.Fatalf("settle run: %v", err)
		}
		if _, err := subject.Store.AdmitRun(ctx, candidate, time.Minute); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("duplicate terminal admission err = %v, want ErrConflict", err)
		}
	})

	t.Run("transaction rollback hides writes", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		if err := subject.Store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
			s, err := tx.CreateSession(ctx, sessionRecord("committed"))
			if err != nil {
				return err
			}
			r, err := tx.AdmitRun(ctx, run("committed-run", s.ID, "owner"), time.Minute)
			if err != nil {
				return err
			}
			execution := tx.Execution(session.RunFence{RunID: r.ID, ClaimToken: r.ClaimToken})
			_, err = execution.AppendEvent(ctx, eventRecord("committed-event", s.ID, r.ID, 1))
			return err
		}); err != nil {
			t.Fatalf("commit transaction: %v", err)
		}
		if _, err := subject.Store.GetSession(ctx, "committed"); err != nil {
			t.Fatalf("committed session missing: %v", err)
		}
		errRollback := errors.New("rollback")
		err := subject.Store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
			_, err := tx.CreateSession(ctx, sessionRecord("rolled-back"))
			if err != nil {
				return err
			}
			return errRollback
		})
		if !errors.Is(err, errRollback) {
			t.Fatalf("transaction err = %v, want rollback sentinel", err)
		}
		if _, err := subject.Store.GetSession(ctx, "rolled-back"); !errors.Is(err, session.ErrNotFound) {
			t.Fatalf("rolled back session err = %v, want ErrNotFound", err)
		}
		if err := subject.Store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
			return tx.WithinTx(ctx, func(ctx context.Context, nested session.Store) error {
				_, err := nested.CreateSession(ctx, sessionRecord("nested-committed"))
				return err
			})
		}); err != nil {
			t.Fatalf("nested commit: %v", err)
		}
		if _, err := subject.Store.GetSession(ctx, "nested-committed"); err != nil {
			t.Fatalf("nested committed session missing: %v", err)
		}
		err = subject.Store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
			return tx.WithinTx(ctx, func(ctx context.Context, nested session.Store) error {
				if _, err := nested.CreateSession(ctx, sessionRecord("nested-propagated")); err != nil {
					return err
				}
				return errRollback
			})
		})
		if !errors.Is(err, errRollback) {
			t.Fatalf("propagated nested transaction err = %v", err)
		}
		if _, err := subject.Store.GetSession(ctx, "nested-propagated"); !errors.Is(err, session.ErrNotFound) {
			t.Fatalf("propagated nested write err = %v, want ErrNotFound", err)
		}
		if err := subject.Store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
			_ = tx.WithinTx(ctx, func(ctx context.Context, nested session.Store) error {
				if _, err := nested.CreateSession(ctx, sessionRecord("nested-swallowed")); err != nil {
					return err
				}
				return errRollback
			})
			return nil
		}); err != nil {
			t.Fatalf("swallowed nested transaction: %v", err)
		}
		if _, err := subject.Store.GetSession(ctx, "nested-swallowed"); err != nil {
			t.Fatalf("swallowed nested write did not commit: %v", err)
		}
		assertPanicRollback(t, ctx, subject)
	})

	t.Run("replay ordering is stable by message and part order", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := createSession(t, ctx, subject.Store, "session-replay")
		r := admitRun(t, ctx, subject.Store, run("run-replay", s.ID, "owner"))
		execution := executionFor(subject.Store, r)
		msg1 := appendMessage(t, ctx, execution, message("msg-z", s.ID, r.ID, session.RoleUser))
		msg2 := appendMessage(t, ctx, execution, message("msg-a", s.ID, r.ID, session.RoleAssistant))
		appendPart(t, ctx, execution, part("prt-z", msg2.ID, s.ID, r.ID, 20))
		appendPart(t, ctx, execution, part("prt-m", msg1.ID, s.ID, r.ID, 10))
		appendPart(t, ctx, execution, part("prt-a", msg2.ID, s.ID, r.ID, 30))
		batch, err := subject.Store.ListMessages(ctx, s.ID, session.ReplayCursor{Limit: 10})
		if err != nil {
			t.Fatalf("list messages: %v", err)
		}
		if got := ids(batch.Messages); got != "msg-z,msg-a" {
			t.Fatalf("message order = %s, want msg-z,msg-a", got)
		}
		if got := partIDs(batch.Parts); got != "prt-m,prt-z,prt-a" {
			t.Fatalf("part order = %s, want prt-m,prt-z,prt-a", got)
		}
		first, err := subject.Store.ListMessages(ctx, s.ID, session.ReplayCursor{Limit: 1})
		if err != nil {
			t.Fatalf("list first replay page: %v", err)
		}
		second, err := subject.Store.ListMessages(ctx, s.ID, first.Next)
		if err != nil {
			t.Fatalf("list second replay page: %v", err)
		}
		if got := ids(append(first.Messages, second.Messages...)); got != "msg-z,msg-a" {
			t.Fatalf("paged message order = %s, want msg-z,msg-a", got)
		}
	})

	t.Run("compatible duplicate writes are idempotent and conflicts are rejected", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := sessionRecord("session-idempotent")
		first, err := subject.Store.CreateSession(ctx, s)
		if err != nil {
			t.Fatalf("create session: %v", err)
		}
		again, err := subject.Store.CreateSession(ctx, s)
		if err != nil {
			t.Fatalf("repeat create session: %v", err)
		}
		if again.ID != first.ID {
			t.Fatalf("repeat create returned %s, want %s", again.ID, first.ID)
		}
		conflict := s
		conflict.Title = "different"
		if _, err := subject.Store.CreateSession(ctx, conflict); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("conflicting duplicate session err = %v, want ErrConflict", err)
		}
		r := admitRun(t, ctx, subject.Store, run("run-idempotent", s.ID, "owner"))
		execution := executionFor(subject.Store, r)
		msg := message("msg-idempotent", s.ID, r.ID, session.RoleUser)
		if _, err := execution.AppendMessage(ctx, msg); err != nil {
			t.Fatalf("append message: %v", err)
		}
		if _, err := execution.AppendMessage(ctx, msg); err != nil {
			t.Fatalf("repeat append message: %v", err)
		}
		changed := msg
		changed.Role = session.RoleAssistant
		if _, err := execution.AppendMessage(ctx, changed); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("conflicting duplicate message err = %v, want ErrConflict", err)
		}
	})

	t.Run("model request writes are fenced and reads are top-level", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := createSession(t, ctx, subject.Store, "session-model-requests")
		r := admitRun(t, ctx, subject.Store, run("run-model-requests", s.ID, "owner"))
		execution := executionFor(subject.Store, r)
		first := modelRequest("request-1", s.ID, r.ID, 1)
		created, err := execution.CreateModelRequest(ctx, first)
		if err != nil {
			t.Fatalf("create model request: %v", err)
		}
		created.State = session.ModelRequestDispatchStarted
		created.UpdatedAt = created.UpdatedAt.Add(time.Second)
		if err := execution.UpdateModelRequest(ctx, created); err != nil {
			t.Fatalf("start model dispatch: %v", err)
		}
		created.State = session.ModelRequestCompleted
		created.UpdatedAt = created.UpdatedAt.Add(time.Second)
		if err := execution.UpdateModelRequest(ctx, created); err != nil {
			t.Fatalf("complete model request: %v", err)
		}
		if _, err := execution.CreateModelRequest(ctx, modelRequest("request-2", s.ID, r.ID, 2)); err != nil {
			t.Fatalf("create second model request: %v", err)
		}
		got, err := subject.Store.GetModelRequest(ctx, first.ID)
		if err != nil || got.State != session.ModelRequestCompleted {
			t.Fatalf("get model request = %#v, %v", got, err)
		}
		page, err := subject.Store.ListModelRequests(ctx, r.ID, session.ModelRequestCursor{Limit: 1})
		if err != nil || len(page.Records) != 1 || page.Next.AfterID == "" {
			t.Fatalf("first model request page = %#v, %v", page, err)
		}
		next, err := subject.Store.ListModelRequests(ctx, r.ID, page.Next)
		if err != nil || len(next.Records) != 1 || next.Records[0].ID == page.Records[0].ID {
			t.Fatalf("second model request page = %#v, %v", next, err)
		}
		invalid := created
		invalid.State = session.ModelRequestPrepared
		if err := execution.UpdateModelRequest(ctx, invalid); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("invalid model request transition = %v, want ErrConflict", err)
		}
		stale := subject.Store.Execution(session.RunFence{RunID: r.ID, ClaimToken: "stale"})
		if _, err := stale.CreateModelRequest(ctx, modelRequest("request-stale", s.ID, r.ID, 3)); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("stale model request create = %v, want ErrConflict", err)
		}
		errRollback := errors.New("rollback model request")
		err = subject.Store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
			_, createErr := tx.Execution(session.RunFence{RunID: r.ID, ClaimToken: r.ClaimToken}).CreateModelRequest(ctx, modelRequest("request-rollback", s.ID, r.ID, 4))
			if createErr != nil {
				return createErr
			}
			return errRollback
		})
		if !errors.Is(err, errRollback) {
			t.Fatalf("model request rollback = %v", err)
		}
		if _, err := subject.Store.GetModelRequest(ctx, "request-rollback"); !errors.Is(err, session.ErrNotFound) {
			t.Fatalf("rolled back model request = %v, want ErrNotFound", err)
		}
	})

	t.Run("tool call create claim and settlement are single owner", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := createSession(t, ctx, subject.Store, "session-tools")
		r := admitRun(t, ctx, subject.Store, run("run-tools", s.ID, "owner"))
		execution := executionFor(subject.Store, r)
		msg := appendMessage(t, ctx, execution, message("msg-tools", s.ID, r.ID, session.RoleAssistant))
		call := session.ToolCall{
			ID: "call-1", SessionID: s.ID, RunID: r.ID, MessageID: msg.ID,
			ResultMessageID: "result-message-1", ResultPartID: "result-part-1",
			Name: "file_read", Status: session.ToolCallPending, RetrySafe: true,
		}
		createdAt := time.Now().UTC()
		createRequest := session.CreateToolCallRequest{Call: call, Event: toolEvent("event-create", createdAt)}
		created, err := execution.CreateToolCall(ctx, createRequest)
		if err != nil {
			t.Fatalf("create tool call: %v", err)
		}
		if created.Call.ID != call.ID || created.Event.ID != createRequest.Event.ID || created.Event.ToolTransition != session.ToolTransitionPending {
			t.Fatalf("create transition result = %#v", created)
		}
		createReplay, err := execution.CreateToolCall(ctx, createRequest)
		if err != nil {
			t.Fatalf("idempotent create replay: %v", err)
		}
		if !session.SameToolTransitionState(createReplay.Call, created.Call) || !reflect.DeepEqual(createReplay.Event, created.Event) {
			t.Fatalf("create replay = %#v, want %#v", createReplay, created)
		}
		createRequest.Event.ID = "event-create-new-id"
		if _, err := execution.CreateToolCall(ctx, createRequest); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("new event id for pending phase err = %v, want ErrConflict", err)
		}
		createRequest.Event.ID = "event-create"
		createRequest.Call.Input = json.RawMessage(`{"different":true}`)
		if _, err := execution.CreateToolCall(ctx, createRequest); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("same event id with conflicting pending state err = %v, want ErrConflict", err)
		}
		startedAt := time.Now().UTC()
		claimed, err := execution.ClaimToolCall(ctx, session.ClaimToolCallRequest{ID: call.ID, ClaimedBy: "worker-1", ClaimToken: "claim-1", StartedAt: startedAt, LeaseDuration: time.Minute, Event: toolEvent("event-claim-1", startedAt)})
		if err != nil {
			t.Fatalf("claim tool call: %v", err)
		}
		if claimed.Call.Status != session.ToolCallRunning {
			t.Fatalf("claimed status = %q, want %q", claimed.Call.Status, session.ToolCallRunning)
		}
		if claimed.Event.ID != "event-claim-1" || claimed.Event.ToolTransition != session.ToolTransitionRunning {
			t.Fatalf("claim transition result = %#v", claimed)
		}
		if _, err := execution.ClaimToolCall(ctx, session.ClaimToolCallRequest{ID: call.ID, ClaimedBy: "worker-2", ClaimToken: "claim-2", StartedAt: startedAt, LeaseDuration: time.Minute, Event: toolEvent("event-claim-2", startedAt)}); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("second claim err = %v, want ErrConflict", err)
		}
		claimReplay := session.ClaimToolCallRequest{ID: call.ID, ClaimedBy: "worker-1", ClaimToken: "claim-1", StartedAt: startedAt, LeaseDuration: time.Minute, Event: toolEvent("event-claim-1", startedAt)}
		if _, err := execution.ClaimToolCall(ctx, claimReplay); err != nil {
			t.Fatalf("idempotent claim replay: %v", err)
		}
		claimReplay.Event.ID = "event-claim-new-id"
		if _, err := execution.ClaimToolCall(ctx, claimReplay); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("new event id for running phase err = %v, want ErrConflict", err)
		}
		if _, err := execution.AppendEvent(ctx, session.EventRecord{ID: "tool-bypass", SessionID: s.ID, RunID: r.ID, ToolCallID: call.ID, ToolTransition: session.ToolTransitionRunning, Kind: session.ToolTransitionEventKind, CreatedAt: startedAt}); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("generic tool transition append err = %v, want ErrConflict", err)
		}
		completedAt := startedAt.Add(time.Second)
		output := json.RawMessage(`{"tool_call_id":"call-1","status":"completed","content":"ok"}`)
		settlement := session.ToolSettlement{
			ID: claimed.Call.ID, ClaimedBy: claimed.Call.ClaimedBy, ClaimToken: claimed.Call.ClaimToken, Status: session.ToolCallCompleted, Output: output, CompletedAt: completedAt,
			ResultMessage: session.Message{ID: call.ResultMessageID, SessionID: s.ID, RunID: r.ID, ParentID: msg.ID, Role: session.RoleTool, CreatedAt: completedAt, UpdatedAt: completedAt},
			ResultPart:    session.Part{ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: s.ID, RunID: r.ID, Kind: session.PartToolResult, Payload: output, CreatedAt: completedAt, UpdatedAt: completedAt},
		}
		settleRequest := session.SettleToolCallRequest{Settlement: settlement, Event: toolEvent("event-terminal", completedAt)}
		settled, err := execution.SettleToolCall(ctx, settleRequest)
		if err != nil {
			t.Fatalf("settle tool call: %v", err)
		}
		if settled.Call.Status != session.ToolCallCompleted || settled.Event.ID != "event-terminal" || settled.Event.ToolTransition != session.ToolTransitionTerminal {
			t.Fatalf("settle transition result = %#v", settled)
		}
		settleReplay, err := execution.SettleToolCall(ctx, settleRequest)
		if err != nil {
			t.Fatalf("idempotent settlement replay: %v", err)
		}
		if !reflect.DeepEqual(settleReplay, settled) {
			t.Fatalf("settle replay = %#v, want %#v", settleReplay, settled)
		}
		settleRequest.Event.ID = "event-terminal-new-id"
		if _, err := execution.SettleToolCall(ctx, settleRequest); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("new event id for terminal phase err = %v, want ErrConflict", err)
		}
		events, err := subject.Store.ListEvents(ctx, s.ID, session.EventCursor{Limit: 10})
		if err != nil || len(events.Events) != 3 {
			t.Fatalf("tool transition events = %d, err = %v; want pending/running/terminal", len(events.Events), err)
		}
		if unfinished, err := subject.Store.ListUnfinishedToolCalls(ctx, r.ID); err != nil || len(unfinished) != 0 {
			t.Fatalf("unfinished calls = %d, err = %v; want none after settlement", len(unfinished), err)
		}
	})

	t.Run("unfinished runs and events are discoverable for recovery", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		s := createSession(t, ctx, subject.Store, "session-recovery")
		r := admitRun(t, ctx, subject.Store, run("run-recovery", s.ID, "owner"))
		execution := executionFor(subject.Store, r)
		event := session.EventRecord{
			ID:          "evt-1",
			SessionID:   s.ID,
			RunID:       r.ID,
			ProviderID:  "provider",
			ModelID:     "model",
			Kind:        "run_started",
			Correlation: "corr-1",
			Usage: session.Usage{
				InputTokens: 3,
			},
			Redaction: session.RedactionMetadata,
			CreatedAt: time.Now(),
		}
		if _, err := execution.AppendEvent(ctx, event); err != nil {
			t.Fatalf("append event: %v", err)
		}
		if _, err := execution.AppendEvent(ctx, event); err != nil {
			t.Fatalf("repeat append event: %v", err)
		}
		if _, err := execution.AppendEvent(ctx, eventRecord("evt-2", s.ID, r.ID, 2)); err != nil {
			t.Fatalf("append second event: %v", err)
		}
		runs, err := subject.Store.ListUnfinishedRuns(ctx)
		if err != nil {
			t.Fatalf("list unfinished runs: %v", err)
		}
		if !containsRun(runs, r.ID) {
			t.Fatalf("unfinished runs did not include %s", r.ID)
		}
		events, err := subject.Store.ListEvents(ctx, s.ID, session.EventCursor{Limit: 10})
		if err != nil {
			t.Fatalf("list events: %v", err)
		}
		if got := eventIDs(events.Events); got != "evt-1,evt-2" {
			t.Fatalf("events = %s, want evt-1,evt-2", got)
		}
		if events.Events[0].ProviderID != "provider" || events.Events[0].Usage.InputTokens != 3 || events.Events[0].Redaction != session.RedactionMetadata {
			t.Fatalf("event projection lost first-class fields: %#v", events.Events[0])
		}
		first, err := subject.Store.ListEvents(ctx, s.ID, session.EventCursor{Limit: 1})
		if err != nil {
			t.Fatalf("list first event page: %v", err)
		}
		next, err := subject.Store.ListEvents(ctx, s.ID, first.Next)
		if err != nil {
			t.Fatalf("list next event page: %v", err)
		}
		if len(first.Events) != 1 || first.Events[0].ID != "evt-1" || len(next.Events) != 1 || next.Events[0].ID != "evt-2" {
			t.Fatalf("next events = %#v, want evt-2", next.Events)
		}
		later := time.Now().UTC()
		earlier := later.Add(-time.Minute)
		epoch, err := execution.StartContextEpoch(ctx, session.ContextEpoch{
			ID:               "epoch-2",
			SessionID:        s.ID,
			SummaryMessageID: "summary",
			SummarizedFromID: "msg-1",
			SummarizedToID:   "msg-1",
			TailStartID:      "msg-2",
			ModelID:          "model",
			ProviderID:       "provider",
			Trigger:          "manual",
			Reason:           "contract",
			NextAction:       session.EpochNextStop,
			CreatedAt:        later,
		})
		if err != nil {
			t.Fatalf("start context epoch: %v", err)
		}
		duplicate, err := execution.StartContextEpoch(ctx, epoch)
		if err != nil {
			t.Fatalf("duplicate start context epoch: %v", err)
		}
		if duplicate.ID != epoch.ID {
			t.Fatalf("duplicate epoch = %#v, want %#v", duplicate, epoch)
		}
		if _, err := execution.StartContextEpoch(ctx, session.ContextEpoch{
			ID:        "epoch-1",
			SessionID: s.ID,
			Trigger:   "manual",
			Reason:    "earlier",
			CreatedAt: earlier,
		}); err != nil {
			t.Fatalf("start earlier context epoch: %v", err)
		}
		epoch.ClosedAt = epoch.CreatedAt.Add(time.Minute)
		if err := execution.FinishContextEpoch(ctx, epoch); err != nil {
			t.Fatalf("finish context epoch: %v", err)
		}
		reader, ok := subject.Store.(session.ContextEpochReader)
		if !ok {
			t.Fatal("store does not expose session.ContextEpochReader")
		}
		epochs, err := reader.ListContextEpochs(ctx, s.ID)
		if err != nil {
			t.Fatalf("list context epochs: %v", err)
		}
		if len(epochs) != 2 || epochs[0].ID != "epoch-1" || epochs[1].ID != "epoch-2" || epochs[1].SummaryMessageID != "summary" || epochs[1].ClosedAt.IsZero() {
			t.Fatalf("epochs = %#v, want chronological epoch-1 then finished epoch-2", epochs)
		}
	})
}

func toolEvent(id session.EventID, at time.Time) session.ToolTransitionEvent {
	return session.ToolTransitionEvent{ID: id, CreatedAt: at.UTC()}
}

func setup(t testing.TB, factory Factory) Subject {
	t.Helper()
	subject := factory(t)
	if subject.Store == nil {
		t.Fatal("factory returned nil Store")
	}
	t.Cleanup(func() {
		if subject.Cleanup != nil {
			subject.Cleanup()
		}
	})
	return subject
}

func assertPanicRollback(t testing.TB, ctx context.Context, subject Subject) {
	t.Helper()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatalf("transaction panic was not rethrown")
			}
		}()
		_ = subject.Store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
			return tx.WithinTx(ctx, func(ctx context.Context, nested session.Store) error {
				_, err := nested.CreateSession(ctx, sessionRecord("panic-rolled-back"))
				if err != nil {
					return err
				}
				panic("rollback")
			})
		})
	}()
	if _, err := subject.Store.GetSession(ctx, "panic-rolled-back"); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("panic rolled back session err = %v, want ErrNotFound", err)
	}
}

func createSession(t testing.TB, ctx context.Context, st session.Store, id session.ID) session.Session {
	t.Helper()
	s, err := st.CreateSession(ctx, sessionRecord(id))
	if err != nil {
		t.Fatalf("create session %s: %v", id, err)
	}
	return s
}

func admitRun(t testing.TB, ctx context.Context, st session.Store, r session.Run) session.Run {
	t.Helper()
	admitted, err := st.AdmitRun(ctx, r, time.Minute)
	if err != nil {
		t.Fatalf("admit run %s: %v", r.ID, err)
	}
	return admitted
}

func executionFor(st session.Store, run session.Run) session.ExecutionStore {
	return st.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
}

func appendMessage(t testing.TB, ctx context.Context, st session.ExecutionStore, msg session.Message) session.Message {
	t.Helper()
	got, err := st.AppendMessage(ctx, msg)
	if err != nil {
		t.Fatalf("append message %s: %v", msg.ID, err)
	}
	return got
}

func appendPart(t testing.TB, ctx context.Context, st session.ExecutionStore, p session.Part) session.Part {
	t.Helper()
	got, err := st.AppendPart(ctx, p)
	if err != nil {
		t.Fatalf("append part %s: %v", p.ID, err)
	}
	return got
}

func sessionRecord(id session.ID) session.Session {
	now := time.Now().UTC()
	return session.Session{
		ID:        id,
		Directory: "/workspace",
		Title:     string(id),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func run(id session.RunID, sessionID session.ID, owner string) session.Run {
	now := time.Now().UTC()
	return session.Run{
		ID:         id,
		SessionID:  sessionID,
		OwnerID:    owner,
		ClaimToken: "claim-" + string(id),
		Agent:      "default",
		ProviderID: "provider",
		ModelID:    "model",
		Status:     session.RunPending,
		CreatedAt:  now,
	}
}

func message(id session.MessageID, sessionID session.ID, runID session.RunID, role session.Role) session.Message {
	now := time.Now().UTC()
	return session.Message{
		ID:        id,
		SessionID: sessionID,
		RunID:     runID,
		Role:      role,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func part(id session.PartID, messageID session.MessageID, sessionID session.ID, runID session.RunID, ordinal int64) session.Part {
	now := time.Now().UTC()
	return session.Part{
		ID:        id,
		MessageID: messageID,
		SessionID: sessionID,
		RunID:     runID,
		Kind:      session.PartText,
		Ordinal:   ordinal,
		Payload:   []byte(`{"text":"part"}`),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func ids(messages []session.Message) string {
	values := make([]string, 0, len(messages))
	for _, msg := range messages {
		values = append(values, string(msg.ID))
	}
	return strings.Join(values, ",")
}

func partIDs(parts []session.Part) string {
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		values = append(values, string(part.ID))
	}
	return strings.Join(values, ",")
}

func eventIDs(events []session.EventRecord) string {
	values := make([]string, 0, len(events))
	for _, event := range events {
		values = append(values, string(event.ID))
	}
	return strings.Join(values, ",")
}

func containsRun(runs []session.Run, id session.RunID) bool {
	for _, run := range runs {
		if run.ID == id {
			return true
		}
	}
	return false
}

func eventRecord(id session.EventID, sessionID session.ID, runID session.RunID, offset int) session.EventRecord {
	return session.EventRecord{
		ID:        id,
		SessionID: sessionID,
		RunID:     runID,
		Kind:      "event",
		Payload:   json.RawMessage(`{"ok":true}`),
		CreatedAt: time.Now().UTC().Add(time.Duration(offset) * time.Second),
	}
}

func modelRequest(id session.ModelRequestID, sessionID session.ID, runID session.RunID, attempt int) session.ModelRequestRecord {
	now := time.Now().UTC().Add(time.Duration(attempt) * time.Second)
	return session.ModelRequestRecord{
		ID: id, SessionID: sessionID, RunID: runID, AssistantMessageID: "assistant-model-request",
		Attempt: attempt, Step: 1, State: session.ModelRequestPrepared,
		Messages: json.RawMessage(`[]`), Tools: json.RawMessage(`[]`), SafeCallConfig: json.RawMessage(`{}`),
		ContentSHA256: "hash", CreatedAt: now, UpdatedAt: now,
	}
}
