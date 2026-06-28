package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
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
	if _, err := db.Exec(`CREATE TABLE schema_version(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL); INSERT INTO schema_version(version, applied_at) VALUES (2, 'now');`); err != nil {
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
	call := session.ToolCall{ID: "call-tool", SessionID: "session-tool", RunID: "run-tool", MessageID: "msg-tool", Name: "tool", Input: []byte(`{"ok":true}`), Status: session.ToolCallPending}
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
