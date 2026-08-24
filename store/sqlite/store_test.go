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
			Store:      st,
			Transactor: st,
			Cleanup: func() {
				_ = st.Close()
			},
		}
	}

	storetest.Run(t, factory)
	storetest.RunTransactional(t, factory)
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
	if _, err := st.AdmitRun(ctx, session.Run{ID: "run-claim", SessionID: "session-claim", OwnerID: "owner", Status: session.RunPending, CreatedAt: now}); err != nil {
		t.Fatalf("admit run: %v", err)
	}
	if _, err := st.AppendMessage(ctx, session.Message{ID: "msg-claim", SessionID: "session-claim", RunID: "run-claim", Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	call := session.ToolCall{ID: "call-claim", SessionID: "session-claim", RunID: "run-claim", MessageID: "msg-claim", Name: "tool", Status: session.ToolCallPending}
	if _, err := st.CreateToolCall(ctx, call); err != nil {
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
			claim := call
			claim.ClaimedBy = fmt.Sprintf("worker-%d", i)
			claim.ClaimToken = fmt.Sprintf("token-%d", i)
			_, err := st.ClaimToolCall(ctx, claim)
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

func TestFinishToolCallRejectsConflictingConcurrentSettlement(t *testing.T) {
	st, call := setupClaimedToolCall(t)
	defer func() {
		_ = st.Close()
	}()

	ctx := context.Background()
	const contenders = 8
	start := make(chan struct{})
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			settlement := call
			settlement.Status = session.ToolCallCompleted
			settlement.Output = []byte(fmt.Sprintf(`{"worker":%d}`, i))
			settlement.CompletedAt = time.Now().UTC().Add(time.Duration(i) * time.Nanosecond)
			errs <- st.FinishToolCall(ctx, settlement)
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
			t.Fatalf("finish err = %v", err)
		}
	}
	if success != 1 || conflict != contenders-1 {
		t.Fatalf("success=%d conflict=%d, want 1/%d", success, conflict, contenders-1)
	}
}

func TestSettleToolCallAtomicallyCreatesReservedResultAndIsIdempotent(t *testing.T) {
	st, call := setupClaimedToolCall(t)
	defer func() { _ = st.Close() }()
	ctx := context.Background()
	now := time.Now().UTC()
	output := json.RawMessage(`{"tool_call_id":"call-tool","status":"completed","content":"ok"}`)
	settlement := session.ToolSettlement{
		ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken, Status: session.ToolCallCompleted, Output: output,
		ResultMessage: session.Message{ID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, ParentID: call.MessageID, Role: session.RoleTool, CreatedAt: now, UpdatedAt: now},
		ResultPart:    session.Part{ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, Kind: session.PartToolResult, Payload: output, CreatedAt: now, UpdatedAt: now},
	}
	if err := st.SettleToolCall(ctx, settlement); err != nil {
		t.Fatal(err)
	}
	if err := st.SettleToolCall(ctx, settlement); err != nil {
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
	if err := st.SettleToolCall(ctx, conflict); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("conflicting settlement = %v", err)
	}
	stale := settlement
	stale.ClaimToken = "stale"
	if err := st.SettleToolCall(ctx, stale); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("terminal stale settlement = %v", err)
	}
}

func TestSettleToolCallRollsBackEveryWriteWhenResultPersistenceFails(t *testing.T) {
	for _, table := range []string{"messages", "parts"} {
		t.Run(table, func(t *testing.T) {
			st, call := setupClaimedToolCall(t)
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
			if err := st.SettleToolCall(ctx, settlement); err == nil {
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
			if err := st.SettleToolCall(ctx, settlement); err != nil {
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
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			st, call := setupClaimedToolCall(t)
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
			if err := st.SettleToolCall(ctx, settlement); !errors.Is(err, session.ErrConflict) {
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
	st, call := setupClaimedToolCall(t)
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
	if err := st.SettleToolCall(ctx, stale); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale settlement = %v, want ErrConflict", err)
	}
	if _, err := st.GetMessage(ctx, call.ResultMessageID); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("stale result message error = %v, want ErrNotFound", err)
	}
}

func TestFinishRunIsIdempotentAndRejectsOverwrite(t *testing.T) {
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
	run, err := st.AdmitRun(ctx, session.Run{ID: "run-finish", SessionID: "session-run", OwnerID: "owner", Status: session.RunPending, CreatedAt: now})
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	run.Status = session.RunCompleted
	run.FinishedAt = now.Add(time.Second)
	if err := st.FinishRun(ctx, run); err != nil {
		t.Fatalf("finish run: %v", err)
	}
	if err := st.FinishRun(ctx, run); err != nil {
		t.Fatalf("idempotent finish run: %v", err)
	}
	conflict := run
	conflict.Status = session.RunFailed
	conflict.Error = "different"
	if err := st.FinishRun(ctx, conflict); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("conflicting finish err = %v, want ErrConflict", err)
	}
	stale := run
	stale.Status = session.RunRunning
	if err := st.FinishRun(ctx, stale); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale nonterminal update err = %v, want ErrConflict", err)
	}
}

func TestCreateToolCallDuplicateRequiresFullRecordMatch(t *testing.T) {
	st, call := setupClaimedToolCall(t)
	defer func() {
		_ = st.Close()
	}()

	ctx := context.Background()
	conflict := call
	conflict.Input = []byte(`{"changed":true}`)
	if _, err := st.CreateToolCall(ctx, conflict); !errors.Is(err, session.ErrConflict) {
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

func TestOpenUpgradesVersionOneDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(migration001); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	store, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var version int
	if err := store.db.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&version); err != nil || version != 2 {
		t.Fatalf("version = %d, %v", version, err)
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
	if _, err := store.AdmitRun(ctx, session.Run{ID: "ledger-run", SessionID: "ledger-session", OwnerID: "owner", Status: session.RunPending, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= 2; index++ {
		record := session.ModelRequestRecord{ID: session.ModelRequestID(fmt.Sprintf("request-%d", index)), SessionID: "ledger-session", RunID: "ledger-run", AssistantMessageID: "assistant", Attempt: index, Step: 1, State: session.ModelRequestPrepared, Messages: json.RawMessage(`[]`), Tools: json.RawMessage(`[]`), SafeCallConfig: json.RawMessage(`{}`), ContentSHA256: "hash", CreatedAt: now.Add(time.Duration(index) * time.Second), UpdatedAt: now}
		created, err := store.CreateModelRequest(ctx, record)
		if err != nil {
			t.Fatal(err)
		}
		created.State = session.ModelRequestDispatchStarted
		created.UpdatedAt = now.Add(time.Minute)
		if err := store.UpdateModelRequest(ctx, created); err != nil {
			t.Fatal(err)
		}
		created.State = session.ModelRequestCompleted
		created.UpdatedAt = now.Add(2 * time.Minute)
		if err := store.UpdateModelRequest(ctx, created); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateModelRequest(ctx, created); err != nil {
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
	if err := store.UpdateModelRequest(ctx, invalid); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("backward transition = %v", err)
	}
}

func setupClaimedToolCall(t testing.TB) (*Store, session.ToolCall) {
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
	if _, err := st.AdmitRun(ctx, session.Run{ID: "run-tool", SessionID: "session-tool", OwnerID: "owner", Status: session.RunPending, CreatedAt: now}); err != nil {
		_ = st.Close()
		t.Fatalf("admit run: %v", err)
	}
	if _, err := st.AppendMessage(ctx, session.Message{ID: "msg-tool", SessionID: "session-tool", RunID: "run-tool", Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		_ = st.Close()
		t.Fatalf("append message: %v", err)
	}
	call := session.ToolCall{ID: "call-tool", SessionID: "session-tool", RunID: "run-tool", MessageID: "msg-tool", ResultMessageID: "result-tool", ResultPartID: "part-tool", Name: "tool", Input: []byte(`{"ok":true}`), Status: session.ToolCallPending}
	if _, err := st.CreateToolCall(ctx, call); err != nil {
		_ = st.Close()
		t.Fatalf("create tool call: %v", err)
	}
	call.ClaimedBy = "worker"
	call.ClaimToken = "token"
	claimed, err := st.ClaimToolCall(ctx, call)
	if err != nil {
		_ = st.Close()
		t.Fatalf("claim tool call: %v", err)
	}
	return st, claimed
}
