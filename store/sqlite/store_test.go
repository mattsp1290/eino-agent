package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/storetest"
)

func TestStoreContract(t *testing.T) {
	factory := func(t testing.TB) storetest.Subject {
		t.Helper()
		st, err := Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
		if err != nil {
			t.Fatalf("open sqlite store: %v", err)
		}
		return storetest.Subject{
			Store: st,
			Cleanup: func() {
				_ = st.Close()
			},
		}
	}

	storetest.Run(t, factory)
}

func sqliteToolEvent(id session.EventID, at time.Time) session.ToolTransitionEvent {
	return session.ToolTransitionEvent{ID: id, CreatedAt: at.UTC()}
}

func sqliteCreateRequest(call session.ToolCall, id session.EventID, at time.Time) session.CreateToolCallRequest {
	if call.RequestPartID == "" {
		call.RequestPartID = session.PartID("request-part-" + string(call.ID))
	}
	if len(call.Input) == 0 {
		call.Input = json.RawMessage(`{}`)
	}
	payload, err := json.Marshal(map[string]any{"id": call.ID, "name": call.Name, "arguments": call.Input})
	if err != nil {
		panic(err)
	}
	return session.CreateToolCallRequest{
		Call: call,
		RequestPart: session.Part{
			ID: call.RequestPartID, MessageID: call.MessageID, SessionID: call.SessionID, RunID: call.RunID,
			Kind: session.PartToolCall, Payload: payload, CreatedAt: at.UTC(), UpdatedAt: at.UTC(),
		},
		Event: sqliteToolEvent(id, at),
	}
}

func sqliteSettleRequest(settlement session.ToolSettlement, id session.EventID) session.SettleToolCallRequest {
	return session.SettleToolCallRequest{Settlement: settlement, Event: sqliteToolEvent(id, settlement.CompletedAt)}
}

func sqliteRunSettlementRequest(run session.Run, id session.EventID) session.SettleRunRequest {
	return session.SettleRunRequest{
		Settlement: session.RunSettlement{Status: run.Status, FinishedAt: run.FinishedAt, Error: run.Error},
		Event:      session.RunSettlementEvent{ID: id},
	}
}

func TestConcurrentToolClaimHasSingleOwner(t *testing.T) {
	st, err := Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		_ = st.Close()
	}()

	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := st.CreateSession(ctx, session.Session{ID: "session-claim", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := st.AdmitRun(ctx, session.Run{ID: "run-claim", SessionID: "session-claim", OwnerID: "owner", ClaimToken: "claim-run", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := st.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "msg-claim", SessionID: "session-claim", RunID: "run-claim", Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	call := session.ToolCall{ID: "call-claim", SessionID: "session-claim", RunID: "run-claim", MessageID: "msg-claim", Name: "tool", Status: session.ToolCallPending}
	if _, err := execution.CreateToolCall(ctx, sqliteCreateRequest(call, "event-create-claim", now)); err != nil {
		t.Fatalf("create tool call: %v", err)
	}

	const contenders = 8
	start := make(chan struct{})
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			startedAt := now.Add(time.Duration(i+1) * time.Microsecond)
			_, err := execution.ClaimToolCall(ctx, session.ClaimToolCallRequest{ID: call.ID, ClaimedBy: fmt.Sprintf("worker-%d", i), ClaimToken: fmt.Sprintf("token-%d", i), StartedAt: startedAt, LeaseDuration: time.Minute, Event: sqliteToolEvent(session.EventID(fmt.Sprintf("event-claim-%d", i)), startedAt)})
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)

	var success, conflict int
	for err := range errs {
		switch {
		case err == nil:
			success++
		case errors.Is(err, session.ErrConflict):
			conflict++
		default:
			t.Fatalf("claim err = %v", err)
		}
	}
	if success != 1 || conflict != contenders-1 {
		t.Fatalf("success=%d conflict=%d, want 1/%d", success, conflict, contenders-1)
	}
}

func TestSettleToolCallAtomicallyCreatesReservedResultAndIsIdempotent(t *testing.T) {
	st, execution, call := setupClaimedToolCall(t)
	defer func() { _ = st.Close() }()
	ctx := context.Background()
	now := time.Now().UTC()
	output := json.RawMessage(`{"tool_call_id":"call-tool","status":"completed","content":"ok"}`)
	settlement := session.ToolSettlement{
		ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken, Status: session.ToolCallCompleted, Output: output,
		CompletedAt:   now,
		ResultMessage: session.Message{ID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, ParentID: call.MessageID, Role: session.RoleTool, CreatedAt: now, UpdatedAt: now},
		ResultPart:    session.Part{ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, Kind: session.PartToolResult, Payload: output, CreatedAt: now, UpdatedAt: now},
	}
	if _, err := execution.SettleToolCall(ctx, sqliteSettleRequest(settlement, "event-settle")); err != nil {
		t.Fatal(err)
	}
	if _, err := execution.SettleToolCall(ctx, sqliteSettleRequest(settlement, "event-settle")); err != nil {
		t.Fatalf("idempotent settlement = %v", err)
	}
	settled, err := st.GetToolCall(ctx, call.ID)
	if err != nil || settled.Status != session.ToolCallCompleted {
		t.Fatalf("settled call = %#v, %v", settled, err)
	}
	if _, err := st.GetMessage(ctx, call.ResultMessageID); err != nil {
		t.Fatalf("reserved result message = %v", err)
	}
	conflict := settlement
	conflict.Output = json.RawMessage(`{"different":true}`)
	conflict.ResultPart.Payload = conflict.Output
	if _, err := execution.SettleToolCall(ctx, sqliteSettleRequest(conflict, "event-settle")); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("conflicting settlement = %v", err)
	}
	stale := settlement
	stale.ClaimToken = "stale"
	if _, err := execution.SettleToolCall(ctx, sqliteSettleRequest(stale, "event-settle")); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("terminal stale settlement = %v", err)
	}
}

func TestToolTransitionEventIdentityAndGenericBypass(t *testing.T) {
	st, execution, call := setupClaimedToolCall(t)
	defer func() { _ = st.Close() }()
	ctx := context.Background()

	claimReplay := session.ClaimToolCallRequest{
		ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken, StartedAt: call.StartedAt,
		LeaseDuration: time.Minute, Event: sqliteToolEvent("event-claim-tool", call.StartedAt),
	}
	if _, err := execution.ClaimToolCall(ctx, claimReplay); err != nil {
		t.Fatalf("same claim event replay: %v", err)
	}
	differentID := claimReplay
	differentID.Event.ID = "event-claim-tool-different"
	if _, err := execution.ClaimToolCall(ctx, differentID); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("different claim event id = %v, want ErrConflict", err)
	}
	conflictingSameID := claimReplay
	conflictingSameID.Event.ProviderID = "different"
	if _, err := execution.ClaimToolCall(ctx, conflictingSameID); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("same id conflicting claim event = %v, want ErrConflict", err)
	}
	if _, err := execution.AppendEvent(ctx, session.EventRecord{ID: "bypass-explicit", SessionID: call.SessionID, RunID: call.RunID, MessageID: call.MessageID, ToolCallID: call.ID, ToolTransition: session.ToolTransitionRunning, Kind: session.ToolTransitionEventKind, CreatedAt: call.StartedAt}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("explicit transition bypass = %v, want ErrConflict", err)
	}
	if _, err := execution.AppendEvent(ctx, session.EventRecord{ID: "bypass-kind", SessionID: call.SessionID, RunID: call.RunID, ToolCallID: call.ID, Kind: session.ToolTransitionEventKind, CreatedAt: call.StartedAt}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("kind transition bypass = %v, want ErrConflict", err)
	}

	completedAt := call.StartedAt.Add(time.Second)
	output := json.RawMessage(`{"tool_call_id":"call-tool","status":"completed","content":"ok"}`)
	settlement := session.ToolSettlement{
		ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken, Status: session.ToolCallCompleted, Output: output, CompletedAt: completedAt,
		ResultMessage: session.Message{ID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, ParentID: call.MessageID, Role: session.RoleTool, CreatedAt: completedAt, UpdatedAt: completedAt},
		ResultPart:    session.Part{ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, Kind: session.PartToolResult, Payload: output, CreatedAt: completedAt, UpdatedAt: completedAt},
	}
	request := sqliteSettleRequest(settlement, "event-terminal-tool")
	if _, err := execution.SettleToolCall(ctx, request); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if _, err := execution.SettleToolCall(ctx, request); err != nil {
		t.Fatalf("same terminal event replay: %v", err)
	}
	differentTerminalID := request
	differentTerminalID.Event.ID = "event-terminal-tool-different"
	if _, err := execution.SettleToolCall(ctx, differentTerminalID); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("different terminal event id = %v, want ErrConflict", err)
	}
	batch, err := st.ListEvents(ctx, call.SessionID, session.EventCursor{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Events) != 3 {
		t.Fatalf("events = %d, want pending/running/terminal", len(batch.Events))
	}
}

func TestToolTransitionEventFailureRollsBackCreateAndClaim(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		st, execution, call, now := setupToolTransitionTest(t)
		defer func() { _ = st.Close() }()
		if _, err := st.db.ExecContext(context.Background(), `CREATE TRIGGER fail_tool_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT, 'forced event failure'); END`); err != nil {
			t.Fatal(err)
		}
		if _, err := execution.CreateToolCall(context.Background(), sqliteCreateRequest(call, "event-create-fail", now)); err == nil {
			t.Fatal("create succeeded despite event failure")
		}
		if _, err := st.GetToolCall(context.Background(), call.ID); !errors.Is(err, session.ErrNotFound) {
			t.Fatalf("call survived rollback: %v", err)
		}
	})

	t.Run("claim", func(t *testing.T) {
		st, execution, call, now := setupToolTransitionTest(t)
		defer func() { _ = st.Close() }()
		ctx := context.Background()
		if _, err := execution.CreateToolCall(ctx, sqliteCreateRequest(call, "event-create-ok", now)); err != nil {
			t.Fatal(err)
		}
		before, err := st.GetRun(ctx, call.RunID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.ExecContext(ctx, `CREATE TRIGGER fail_claim_event BEFORE INSERT ON events WHEN NEW.tool_transition = 'running' BEGIN SELECT RAISE(ABORT, 'forced claim event failure'); END`); err != nil {
			t.Fatal(err)
		}
		startedAt := now.Add(time.Second)
		request := session.ClaimToolCallRequest{ID: call.ID, ClaimedBy: "worker", ClaimToken: "claim", StartedAt: startedAt, LeaseDuration: time.Hour, Event: sqliteToolEvent("event-claim-fail", startedAt)}
		if _, err := execution.ClaimToolCall(ctx, request); err == nil {
			t.Fatal("claim succeeded despite event failure")
		}
		after, err := st.GetRun(ctx, call.RunID)
		if err != nil {
			t.Fatal(err)
		}
		current, err := st.GetToolCall(ctx, call.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status != session.ToolCallPending || current.ClaimedBy != "" || current.ClaimToken != "" || !after.LeaseUntil.Equal(before.LeaseUntil) {
			t.Fatalf("claim rollback failed: call=%+v lease before=%s after=%s", current, before.LeaseUntil, after.LeaseUntil)
		}
	})
}

func TestSettleToolCallRollsBackEveryWriteWhenResultPersistenceFails(t *testing.T) {
	for _, table := range []string{"messages", "parts"} {
		t.Run(table, func(t *testing.T) {
			st, execution, call := setupClaimedToolCall(t)
			defer func() { _ = st.Close() }()
			ctx := context.Background()
			now := time.Now().UTC()
			output := json.RawMessage(`{"tool_call_id":"call-tool","status":"completed","content":"ok"}`)
			settlement := session.ToolSettlement{
				ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken, Status: session.ToolCallCompleted, Output: output, CompletedAt: now,
				ResultMessage: session.Message{ID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, ParentID: call.MessageID, Role: session.RoleTool, CreatedAt: now, UpdatedAt: now},
				ResultPart:    session.Part{ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, Kind: session.PartToolResult, Payload: output, CreatedAt: now, UpdatedAt: now},
			}
			call, err := st.GetToolCall(ctx, call.ID)
			if err != nil {
				t.Fatal(err)
			}
			trigger := fmt.Sprintf(`CREATE TRIGGER fail_settlement BEFORE INSERT ON %s BEGIN SELECT RAISE(ABORT, 'forced settlement failure'); END`, table)
			if _, err = st.db.ExecContext(ctx, trigger); err != nil {
				t.Fatal(err)
			}
			if _, err := execution.SettleToolCall(ctx, sqliteSettleRequest(settlement, "event-settle")); err == nil {
				t.Fatal("SettleToolCall succeeded despite injected persistence failure")
			}
			current, err := st.GetToolCall(ctx, call.ID)
			if err != nil || !reflect.DeepEqual(current, call) {
				t.Fatalf("tool call was not rolled back: current=%#v original=%#v err=%v", current, call, err)
			}
			if _, err := st.GetMessage(ctx, call.ResultMessageID); !errors.Is(err, session.ErrNotFound) {
				t.Fatalf("result message survived rollback: %v", err)
			}
			var part session.Part
			if err := st.getJSON(ctx, "SELECT record FROM parts WHERE id = ?", []any{call.ResultPartID}, &part); !errors.Is(err, session.ErrNotFound) {
				t.Fatalf("result part survived rollback: %v", err)
			}
			if _, err := st.db.ExecContext(ctx, `DROP TRIGGER fail_settlement`); err != nil {
				t.Fatal(err)
			}
			if _, err := execution.SettleToolCall(ctx, sqliteSettleRequest(settlement, "event-settle")); err != nil {
				t.Fatalf("retry after rollback = %v", err)
			}
		})
	}
}

func TestSettleToolCallRejectsContradictoryResultEnvelopeWithoutWrites(t *testing.T) {
	tests := map[string]func(*session.ToolSettlement){
		"message id":      func(value *session.ToolSettlement) { value.ResultMessage.ID = "wrong" },
		"message session": func(value *session.ToolSettlement) { value.ResultMessage.SessionID = "wrong" },
		"message run":     func(value *session.ToolSettlement) { value.ResultMessage.RunID = "wrong" },
		"message parent":  func(value *session.ToolSettlement) { value.ResultMessage.ParentID = "wrong" },
		"message role":    func(value *session.ToolSettlement) { value.ResultMessage.Role = session.RoleAssistant },
		"part id":         func(value *session.ToolSettlement) { value.ResultPart.ID = "wrong" },
		"part message":    func(value *session.ToolSettlement) { value.ResultPart.MessageID = "wrong" },
		"part session":    func(value *session.ToolSettlement) { value.ResultPart.SessionID = "wrong" },
		"part run":        func(value *session.ToolSettlement) { value.ResultPart.RunID = "wrong" },
		"part kind":       func(value *session.ToolSettlement) { value.ResultPart.Kind = session.PartText },
		"part payload":    func(value *session.ToolSettlement) { value.ResultPart.Payload = json.RawMessage(`{"different":true}`) },
		"part numeric precision": func(value *session.ToolSettlement) {
			value.Output = json.RawMessage(`9007199254740992`)
			value.ResultPart.Payload = json.RawMessage(`9007199254740993`)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			st, execution, call := setupClaimedToolCall(t)
			defer func() { _ = st.Close() }()
			ctx := context.Background()
			call, err := st.GetToolCall(ctx, call.ID)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			output := json.RawMessage(`{"tool_call_id":"call-tool","status":"completed","content":"ok"}`)
			settlement := session.ToolSettlement{
				ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken, Status: session.ToolCallCompleted, Output: output, CompletedAt: now,
				ResultMessage: session.Message{ID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, ParentID: call.MessageID, Role: session.RoleTool, CreatedAt: now, UpdatedAt: now},
				ResultPart:    session.Part{ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, Kind: session.PartToolResult, Payload: output, CreatedAt: now, UpdatedAt: now},
			}
			mutate(&settlement)
			if _, err := execution.SettleToolCall(ctx, sqliteSettleRequest(settlement, "event-settle")); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("SettleToolCall error = %v, want ErrConflict", err)
			}
			current, err := st.GetToolCall(ctx, call.ID)
			if err != nil || !reflect.DeepEqual(current, call) {
				t.Fatalf("tool call mutated: current=%#v original=%#v err=%v", current, call, err)
			}
			if _, err := st.GetMessage(ctx, call.ResultMessageID); !errors.Is(err, session.ErrNotFound) {
				t.Fatalf("reserved message error = %v, want ErrNotFound", err)
			}
			var part session.Part
			if err := st.getJSON(ctx, "SELECT record FROM parts WHERE id = ?", []any{call.ResultPartID}, &part); !errors.Is(err, session.ErrNotFound) {
				t.Fatalf("reserved part error = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestSettleToolCallRejectsStaleClaimBeforeApplyingResult(t *testing.T) {
	st, execution, call := setupClaimedToolCall(t)
	defer func() { _ = st.Close() }()
	ctx := context.Background()
	stale := session.ToolSettlement{
		ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken, Status: session.ToolCallCompleted, Output: json.RawMessage(`{"content":"stale"}`),
		ResultMessage: session.Message{ID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, ParentID: call.MessageID, Role: session.RoleTool},
		ResultPart:    session.Part{ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, Kind: session.PartToolResult, Payload: json.RawMessage(`{"content":"stale"}`)},
	}

	current := call
	current.ClaimedBy = "new-worker"
	current.ClaimToken = "new-token"
	raw, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.exec(ctx, `UPDATE tool_calls SET claimed_by = ?, claim_token = ?, record = ? WHERE id = ?`, current.ClaimedBy, current.ClaimToken, raw, current.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := execution.SettleToolCall(ctx, sqliteSettleRequest(stale, "event-settle")); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale settlement = %v, want ErrConflict", err)
	}
	if _, err := st.GetMessage(ctx, call.ResultMessageID); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("stale result message error = %v, want ErrNotFound", err)
	}
}

func TestSettleRunIsIdempotentAndRejectsOverwrite(t *testing.T) {
	st, err := Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		_ = st.Close()
	}()

	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := st.CreateSession(ctx, session.Session{ID: "session-run", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := st.AdmitRun(ctx, session.Run{ID: "run-finish", SessionID: "session-run", OwnerID: "owner", ClaimToken: "claim-finish", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	run.Status = session.RunCompleted
	run.FinishedAt = now.Add(time.Second)
	execution := st.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	request := sqliteRunSettlementRequest(run, "event-run-finished")
	committed, err := execution.SettleRun(ctx, request)
	if err != nil {
		t.Fatalf("finish run: %v", err)
	}
	finalEvent, err := session.RunSettlementRecord(committed.Run, request.Event)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(committed.Event, finalEvent) {
		t.Fatalf("committed event = %#v, want %#v", committed.Event, finalEvent)
	}
	if _, err := execution.SettleRun(ctx, request); err != nil {
		t.Fatalf("idempotent finish run: %v", err)
	}
	conflict := request
	conflict.Settlement.Status = session.RunFailed
	conflict.Settlement.Error = "different"
	if _, err := execution.SettleRun(ctx, conflict); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("conflicting finish err = %v, want ErrConflict", err)
	}
	stale := request
	stale.Settlement.Status = session.RunRunning
	if _, err := execution.SettleRun(ctx, stale); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale nonterminal update err = %v, want ErrConflict", err)
	}
}

func TestListMessagesDoesNotDecodePartsOutsideCurrentPage(t *testing.T) {
	st, err := Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := st.CreateSession(ctx, session.Session{ID: "session-page", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	run, err := st.AdmitRun(ctx, session.Run{ID: "run-page", SessionID: "session-page", OwnerID: "owner", ClaimToken: "claim", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	execution := st.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	for index, id := range []session.MessageID{"message-one", "message-two"} {
		at := now.Add(time.Duration(index) * time.Second)
		if _, err := execution.AppendMessage(ctx, session.Message{ID: id, SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: at, UpdatedAt: at}); err != nil {
			t.Fatal(err)
		}
		if _, err := execution.AppendPart(ctx, session.Part{ID: session.PartID("part-" + string(id)), MessageID: id, SessionID: run.SessionID, RunID: run.ID, Kind: session.PartText, Payload: json.RawMessage(`{"text":"ok"}`), CreatedAt: at, UpdatedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.exec(ctx, `UPDATE parts SET record = ? WHERE id = ?`, []byte(`{malformed`), "part-message-two"); err != nil {
		t.Fatal(err)
	}
	first, err := st.ListMessages(ctx, run.SessionID, session.ReplayCursor{Limit: 1})
	if err != nil || len(first.Messages) != 1 || len(first.Parts) != 1 || first.Messages[0].ID != "message-one" {
		t.Fatalf("first page = %#v, %v", first, err)
	}
	if _, err := st.ListMessages(ctx, run.SessionID, first.Next); err == nil {
		t.Fatal("page containing malformed part unexpectedly decoded")
	}
}

func TestSettleRunRollsBackTerminalStateWhenEventInsertFails(t *testing.T) {
	st, err := Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := st.CreateSession(ctx, session.Session{ID: "run-rollback-session", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	run, err := st.AdmitRun(ctx, session.Run{ID: "run-rollback", SessionID: "run-rollback-session", OwnerID: "owner", ClaimToken: "claim", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `CREATE TRIGGER fail_run_finished BEFORE INSERT ON events WHEN NEW.kind = 'run_finished' BEGIN SELECT RAISE(ABORT, 'forced run event failure'); END`); err != nil {
		t.Fatal(err)
	}
	request := session.SettleRunRequest{
		Settlement: session.RunSettlement{Status: session.RunCompleted, FinishedAt: now.Add(time.Second)},
		Event:      session.RunSettlementEvent{ID: "run-rollback-finished"},
	}
	if _, err := st.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken}).SettleRun(ctx, request); err == nil {
		t.Fatal("settlement succeeded despite forced event failure")
	}
	stored, err := st.GetRun(ctx, run.ID)
	if err != nil || stored.Terminal() {
		t.Fatalf("run after rollback = %#v, %v", stored, err)
	}
	batch, err := st.ListEvents(ctx, run.SessionID, session.EventCursor{Limit: 10})
	if err != nil || len(batch.Events) != 0 {
		t.Fatalf("events after rollback = %#v, %v", batch.Events, err)
	}
}

func TestRunClaimIsSingleWinnerAndFencesStaleExecution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	now := time.Now().UTC()
	if _, err := st.CreateSession(ctx, session.Session{ID: "claim-session", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	run, err := st.AdmitRun(ctx, session.Run{ID: "claim-run", SessionID: "claim-session", OwnerID: "old-owner", ClaimToken: "old-token", Status: session.RunPending, CreatedAt: now}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	oldExecution := st.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	time.Sleep(3 * time.Millisecond)

	type claimResult struct {
		run session.Run
		err error
	}
	results := make(chan claimResult, 2)
	for _, token := range []string{"new-token-a", "new-token-b"} {
		token := token
		go func() {
			claimed, claimErr := st.ClaimRun(ctx, session.RunClaim{RunID: run.ID, OwnerID: token, ClaimToken: token, LeaseDuration: time.Minute})
			results <- claimResult{run: claimed, err: claimErr}
		}()
	}
	var winner session.Run
	var losses int
	for range 2 {
		result := <-results
		if result.err == nil {
			winner = result.run
		} else if errors.Is(result.err, session.ErrSessionBusy) || errors.Is(result.err, session.ErrConflict) {
			losses++
		} else {
			t.Fatalf("ClaimRun error = %v", result.err)
		}
	}
	if winner.ClaimToken == "" || winner.ClaimToken == run.ClaimToken || losses != 1 {
		t.Fatalf("winner = %+v, losses = %d", winner, losses)
	}
	if _, err := oldExecution.AppendMessage(ctx, session.Message{ID: "stale-message", SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale AppendMessage error = %v", err)
	}
	stalePart := session.Part{ID: "stale-part", MessageID: "stale-message", SessionID: run.SessionID, RunID: run.ID, Kind: session.PartText, CreatedAt: now, UpdatedAt: now}
	if _, err := oldExecution.AppendPart(ctx, stalePart); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale AppendPart error = %v", err)
	}
	if err := oldExecution.UpdatePart(ctx, stalePart); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale UpdatePart error = %v", err)
	}
	if _, err := oldExecution.AppendEvent(ctx, session.EventRecord{ID: "stale-event", SessionID: run.SessionID, RunID: run.ID, Kind: "stale", CreatedAt: now}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale AppendEvent error = %v", err)
	}
	staleCall := session.ToolCall{ID: "stale-call", SessionID: run.SessionID, RunID: run.ID, MessageID: "stale-message", Status: session.ToolCallPending}
	if _, err := oldExecution.CreateToolCall(ctx, sqliteCreateRequest(staleCall, "stale-tool-create", now)); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale CreateToolCall error = %v", err)
	}
	if _, err := oldExecution.ClaimToolCall(ctx, session.ClaimToolCallRequest{ID: staleCall.ID, ClaimedBy: "stale", ClaimToken: "stale", StartedAt: now, LeaseDuration: time.Minute, Event: sqliteToolEvent("stale-tool-claim", now)}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale ClaimToolCall error = %v", err)
	}
	if _, err := oldExecution.SettleToolCall(ctx, session.SettleToolCallRequest{Settlement: session.ToolSettlement{ID: staleCall.ID}, Event: sqliteToolEvent("stale-tool-settle", now)}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale SettleToolCall error = %v", err)
	}
	staleEpoch := session.ContextEpoch{ID: "stale-epoch", SessionID: run.SessionID, CreatedAt: now}
	if _, err := oldExecution.StartContextEpoch(ctx, staleEpoch); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale StartContextEpoch error = %v", err)
	}
	if err := oldExecution.FinishContextEpoch(ctx, staleEpoch); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale FinishContextEpoch error = %v", err)
	}
	staleRequest := session.ModelRequestRecord{ID: "stale-request", SessionID: run.SessionID, RunID: run.ID, CreatedAt: now, UpdatedAt: now}
	if _, err := oldExecution.CreateModelRequest(ctx, staleRequest); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale CreateModelRequest error = %v", err)
	}
	if err := oldExecution.UpdateModelRequest(ctx, staleRequest); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale UpdateModelRequest error = %v", err)
	}
	if _, err := oldExecution.StartRun(ctx, now); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale StartRun error = %v", err)
	}
	if _, err := oldExecution.RenewRunLease(ctx, time.Minute); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale RenewRunLease error = %v", err)
	}
	staleRun := winner
	staleRun.ClaimToken = run.ClaimToken
	staleRun.Status = session.RunCompleted
	staleRun.FinishedAt = time.Now().UTC()
	if _, err := oldExecution.SettleRun(ctx, sqliteRunSettlementRequest(staleRun, "stale-finished")); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale SettleRun error = %v", err)
	}
	if batch, err := st.ListMessages(ctx, run.SessionID, session.ReplayCursor{}); err != nil || len(batch.Messages) != 0 || len(batch.Parts) != 0 {
		t.Fatalf("stale history persisted: messages=%d parts=%d err=%v", len(batch.Messages), len(batch.Parts), err)
	}
	if batch, err := st.ListEvents(ctx, run.SessionID, session.EventCursor{}); err != nil || len(batch.Events) != 0 {
		t.Fatalf("stale events persisted: events=%d err=%v", len(batch.Events), err)
	}
	if _, err := st.GetToolCall(ctx, staleCall.ID); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("stale tool call persisted: %v", err)
	}
	if _, err := st.GetModelRequest(ctx, staleRequest.ID); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("stale model request persisted: %v", err)
	}
	if epochs, err := st.ListContextEpochs(ctx, run.SessionID); err != nil || len(epochs) != 0 {
		t.Fatalf("stale epoch persisted: epochs=%d err=%v", len(epochs), err)
	}
	currentExecution := st.Execution(session.RunFence{RunID: winner.ID, ClaimToken: winner.ClaimToken})
	if _, err := currentExecution.AppendMessage(ctx, session.Message{ID: "foreign-message", SessionID: "another-session", RunID: winner.ID, Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("cross-session AppendMessage error = %v", err)
	}
	if _, err := currentExecution.StartContextEpoch(ctx, session.ContextEpoch{ID: "foreign-epoch", SessionID: "another-session", CreatedAt: now}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("cross-session StartContextEpoch error = %v", err)
	}
	if _, err := currentExecution.AppendMessage(ctx, session.Message{ID: "winner-message", SessionID: winner.SessionID, RunID: winner.ID, Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("winner AppendMessage: %v", err)
	}
}

func TestCreateToolCallDuplicateRequiresFullRecordMatch(t *testing.T) {
	st, execution, call := setupClaimedToolCall(t)
	defer func() {
		_ = st.Close()
	}()

	ctx := context.Background()
	conflict := call
	conflict.Input = []byte(`{"changed":true}`)
	if _, err := execution.CreateToolCall(ctx, sqliteCreateRequest(conflict, "event-create-tool", time.Now().UTC())); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("duplicate tool input err = %v, want ErrConflict", err)
	}
}

func TestOpenRejectsUnsupportedSchemaVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw sqlite db: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_version(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL); INSERT INTO schema_version(version, applied_at) VALUES (3, 'now');`); err != nil {
		t.Fatalf("seed schema version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw sqlite db: %v", err)
	}

	st, err := Open(context.Background(), path)
	if err == nil {
		_ = st.Close()
		t.Fatal("Open succeeded for unsupported schema version")
	}
	if !errors.Is(err, session.ErrConflict) {
		t.Fatalf("Open err = %v, want ErrConflict", err)
	}
}

func TestOpenRejectsIncompleteVersionOneDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_version(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL); INSERT INTO schema_version(version, applied_at) VALUES (1, 'now');`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	store, err := Open(context.Background(), path)
	if err == nil {
		_ = store.Close()
		t.Fatal("Open succeeded for incomplete schema")
	}
	if !errors.Is(err, session.ErrConflict) {
		t.Fatalf("Open err = %v, want ErrConflict", err)
	}
}

func TestOpenRejectsVersionOneSchemaDrift(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate string
	}{
		{name: "extra column", mutate: `ALTER TABLE sessions ADD COLUMN legacy TEXT`},
		{name: "missing index", mutate: `DROP INDEX events_replay_idx`},
		{name: "extra object", mutate: `CREATE TABLE legacy_state(id TEXT PRIMARY KEY)`},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.db")
			store, err := Open(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(test.mutate); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(context.Background(), path)
			if err == nil {
				_ = store.Close()
				t.Fatal("Open succeeded for drifted current-version schema")
			}
			if !errors.Is(err, session.ErrConflict) {
				t.Fatalf("Open err = %v, want ErrConflict", err)
			}
		})
	}
}

func TestModelRequestLedgerLifecycleAndPagination(t *testing.T) {
	store, err := Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: "ledger-session", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "ledger-run", SessionID: "ledger-session", OwnerID: "owner", ClaimToken: "claim-ledger", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	for index := 1; index <= 2; index++ {
		record := session.ModelRequestRecord{ID: session.ModelRequestID(fmt.Sprintf("request-%d", index)), SessionID: "ledger-session", RunID: "ledger-run", AssistantMessageID: "assistant", Attempt: index, Step: 1, State: session.ModelRequestPrepared, Messages: json.RawMessage(`[]`), Tools: json.RawMessage(`[]`), SafeCallConfig: json.RawMessage(`{}`), ContentSHA256: "hash", CreatedAt: now.Add(time.Duration(index) * time.Second), UpdatedAt: now}
		created, err := execution.CreateModelRequest(ctx, record)
		if err != nil {
			t.Fatal(err)
		}
		created.State = session.ModelRequestDispatchStarted
		created.UpdatedAt = now.Add(time.Minute)
		if err := execution.UpdateModelRequest(ctx, created); err != nil {
			t.Fatal(err)
		}
		created.State = session.ModelRequestCompleted
		created.UpdatedAt = now.Add(2 * time.Minute)
		if err := execution.UpdateModelRequest(ctx, created); err != nil {
			t.Fatal(err)
		}
		if err := execution.UpdateModelRequest(ctx, created); err != nil {
			t.Fatalf("idempotent terminal update: %v", err)
		}
	}
	first, err := store.ListModelRequests(ctx, "ledger-run", session.ModelRequestCursor{Limit: 1})
	if err != nil || len(first.Records) != 1 || first.Next.AfterID == "" {
		t.Fatalf("first page = %#v, %v", first, err)
	}
	second, err := store.ListModelRequests(ctx, "ledger-run", first.Next)
	if err != nil || len(second.Records) != 1 || second.Records[0].State != session.ModelRequestCompleted {
		t.Fatalf("second page = %#v, %v", second, err)
	}
	invalid := second.Records[0]
	invalid.State = session.ModelRequestPrepared
	if err := execution.UpdateModelRequest(ctx, invalid); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("backward transition = %v", err)
	}
}

func setupToolTransitionTest(t testing.TB) (*Store, session.ExecutionStore, session.ToolCall, time.Time) {
	t.Helper()
	st, err := Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := st.CreateSession(ctx, session.Session{ID: "session-tool", CreatedAt: now, UpdatedAt: now}); err != nil {
		_ = st.Close()
		t.Fatalf("create session: %v", err)
	}
	run, err := st.AdmitRun(ctx, session.Run{ID: "run-tool", SessionID: "session-tool", OwnerID: "owner", ClaimToken: "claim-tool-run", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		_ = st.Close()
		t.Fatalf("admit run: %v", err)
	}
	execution := st.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "msg-tool", SessionID: "session-tool", RunID: "run-tool", Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		_ = st.Close()
		t.Fatalf("append message: %v", err)
	}
	call := session.ToolCall{ID: "call-tool", SessionID: "session-tool", RunID: "run-tool", MessageID: "msg-tool", ResultMessageID: "result-tool", ResultPartID: "part-tool", Name: "tool", Pattern: "resource/one", Input: []byte(`{"ok":true}`), Status: session.ToolCallPending}
	return st, execution, call, now
}

func setupClaimedToolCall(t testing.TB) (*Store, session.ExecutionStore, session.ToolCall) {
	t.Helper()
	st, execution, call, now := setupToolTransitionTest(t)
	ctx := context.Background()
	if _, err := execution.CreateToolCall(ctx, sqliteCreateRequest(call, "event-create-tool", now)); err != nil {
		_ = st.Close()
		t.Fatalf("create tool call: %v", err)
	}
	call.ClaimedBy = "worker"
	call.ClaimToken = "token"
	startedAt := now.Add(time.Microsecond)
	claimed, err := execution.ClaimToolCall(ctx, session.ClaimToolCallRequest{ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken, StartedAt: startedAt, LeaseDuration: time.Minute, Event: sqliteToolEvent("event-claim-tool", startedAt)})
	if err != nil {
		_ = st.Close()
		t.Fatalf("claim tool call: %v", err)
	}
	if claimed.Call.Pattern != "resource/one" {
		_ = st.Close()
		t.Fatalf("claimed pattern = %q", claimed.Call.Pattern)
	}
	return st, execution, claimed.Call
}
