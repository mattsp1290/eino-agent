//go:build postgres_integration

package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/postgres"
)

// testPostgresRuntimeResume proves that the runtime can recover a real,
// persisted pending tool call after its original pool is gone. The fixture and
// test entry point live separately so this journey can share the database
// lifecycle with the other PostgreSQL runtime tests.
func testPostgresRuntimeResume(t *testing.T, server *testpostgres.Server) {
	t.Helper()
	f := newPostgresRuntimeFixture(t, server)
	now := time.Now().UTC()
	planDescriptor := testEchoPlanDescriptor()
	if _, err := f.store.CreateSession(f.ctx, session.Session{
		ID: "postgres-resume-session", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := f.store.AdmitRun(f.ctx, session.Run{
		ID: "postgres-resume-run", SessionID: "postgres-resume-session",
		OwnerID: "old-owner", ClaimToken: "old-claim", Agent: "agent",
		ProviderID: "fake", ModelID: "test", Status: session.RunPending,
		Config:        map[string]string{"workspace_id": "workspace", "workspace_root": "/workspace"},
		ExtensionPlan: planDescriptor, CreatedAt: now,
	}, 250*time.Millisecond)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	oldFence := session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken}
	oldExecution := f.store.Execution(oldFence)
	userMessage := session.Message{
		ID: "postgres-resume-user", SessionID: run.SessionID, RunID: run.ID,
		Role: session.RoleUser, CreatedAt: now.Add(time.Nanosecond), UpdatedAt: now.Add(time.Nanosecond),
	}
	assistantMessage := session.Message{
		ID: "postgres-resume-assistant", SessionID: run.SessionID, RunID: run.ID,
		Role: session.RoleAssistant, ParentID: userMessage.ID,
		CreatedAt: now.Add(2 * time.Nanosecond), UpdatedAt: now.Add(2 * time.Nanosecond),
	}
	if _, err := oldExecution.AppendMessage(f.ctx, userMessage); err != nil {
		t.Fatalf("append user message: %v", err)
	}
	if _, err := oldExecution.AppendPart(f.ctx, session.Part{
		ID: "postgres-resume-user-part", MessageID: userMessage.ID,
		SessionID: run.SessionID, RunID: run.ID, Kind: session.PartText, Ordinal: 0,
		Payload:   json.RawMessage(`{"text":"original history"}`),
		CreatedAt: userMessage.CreatedAt, UpdatedAt: userMessage.UpdatedAt,
	}); err != nil {
		t.Fatalf("append user part: %v", err)
	}
	if _, err := oldExecution.AppendMessage(f.ctx, assistantMessage); err != nil {
		t.Fatalf("append assistant message: %v", err)
	}
	if err := oldExecution.FinalizeAssistantMessage(f.ctx, assistantMessage.ID); err != nil {
		t.Fatalf("finalize assistant message: %v", err)
	}
	wantCall := session.ToolCall{
		ID: "postgres-resume-call", SessionID: run.SessionID, RunID: run.ID,
		MessageID: assistantMessage.ID, RequestPartID: "postgres-resume-request-part",
		ResultMessageID: "postgres-resume-result-message", ResultPartID: "postgres-resume-result-part",
		Name: "echo", Pattern: "echo", Input: json.RawMessage(`{"text":"resume me"}`), Status: session.ToolCallPending,
	}
	created, err := oldExecution.CreateToolCall(f.ctx, testCreateToolRequest(wantCall, "postgres-resume-pending", now.Add(3*time.Nanosecond)))
	if err != nil {
		t.Fatalf("create pending tool: %v", err)
	}
	wantCall = created.Call
	assertPostgresRuntimeNoReservedOutputs(t, f.ctx, f.store, wantCall)

	// Reopen the runtime's pool without running migration. The old fence is
	// rebound through an independent pool below so its stale write remains a
	// meaningful cross-connection check after the original pool is closed.
	f.reopen(t)
	reopenedConn, reopenedPID := postgresRuntimeBackend(t, f.ctx, f.db)
	t.Cleanup(func() { _ = reopenedConn.Close() })
	reopenedRun, err := f.store.GetRun(f.ctx, run.ID)
	if err != nil {
		t.Fatalf("get reopened run: %v", err)
	}
	if reopenedRun.OwnerID != "old-owner" || reopenedRun.ClaimToken != oldFence.ClaimToken {
		t.Fatalf("reopened run fence owner=%q token-reused=%t, want original fence", reopenedRun.OwnerID, reopenedRun.ClaimToken == oldFence.ClaimToken)
	}
	reopenedCall, err := f.store.GetToolCall(f.ctx, wantCall.ID)
	if err != nil {
		t.Fatalf("get reopened tool call: %v", err)
	}
	if !reflect.DeepEqual(reopenedRun.ExtensionPlan, planDescriptor) {
		t.Fatal("reopened extension plan differs from the persisted plan")
	}
	if !reflect.DeepEqual(reopenedCall, wantCall) {
		t.Fatal("reopened tool call differs from the persisted call")
	}
	assertPostgresRuntimeNoReservedOutputs(t, f.ctx, f.store, reopenedCall)
	awaitPostgresRuntimeLeaseExpiry(t, f.ctx, f.db, run.ID)

	oldDB := f.database.Open(t)
	oldStore, err := postgres.New(f.ctx, oldDB)
	if err != nil {
		t.Fatalf("open independent old-fence store: %v", err)
	}
	oldConn, oldPID := postgresRuntimeBackend(t, f.ctx, oldDB)
	t.Cleanup(func() { _ = oldConn.Close() })
	if reopenedPID == oldPID {
		t.Fatalf("reopened and old-fence pools share backend PID %d", reopenedPID)
	}
	staleExecution := oldStore.Execution(oldFence)

	entered := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var executions atomic.Int64
	var patternCalls atomic.Int64
	tool := Tool{
		Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Pattern: PermissionPatternResolverFunc(func(context.Context, json.RawMessage) (string, error) {
			patternCalls.Add(1)
			return "", errors.New("persisted pattern must be reused")
		}),
		Executor: orchestratorToolExecutorFunc(func(ctx context.Context, _ ToolCall) (ToolResult, error) {
			executions.Add(1)
			close(entered)
			select {
			case <-release:
				return ToolResult{Output: "resumed output"}, nil
			case <-ctx.Done():
				return ToolResult{}, ctx.Err()
			}
		}),
	}
	resumer, err := NewStreamingOrchestrator(
		WithStore(f.store), WithOwnerID("new-owner"), WithLease(time.Minute),
		WithIDGenerator(&sequenceIDs{}),
		WithModelResolver(resolvedModel{}),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{tools: []Tool{tool}})}),
	)
	if err != nil {
		t.Fatalf("new resumer: %v", err)
	}
	handle, err := resumer.Resume(f.ctx, run.ID)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	select {
	case <-entered:
	case <-f.ctx.Done():
		t.Fatalf("tool did not enter execution: %v", f.ctx.Err())
	}
	claimed, err := f.store.GetToolCall(f.ctx, wantCall.ID)
	if err != nil {
		t.Fatalf("get claimed tool: %v", err)
	}
	if claimed.Status != session.ToolCallRunning || claimed.ClaimedBy != "new-owner" || claimed.ClaimToken == wantCall.ClaimToken || claimed.ClaimToken == "" {
		t.Fatalf("claimed tool status=%s owner=%q token-empty=%t token-reused=%t", claimed.Status, claimed.ClaimedBy, claimed.ClaimToken == "", claimed.ClaimToken == wantCall.ClaimToken)
	}
	resumedRun, err := f.store.GetRun(f.ctx, run.ID)
	if err != nil {
		t.Fatalf("get resumed run: %v", err)
	}
	if resumedRun.OwnerID != "new-owner" || resumedRun.ClaimToken == oldFence.ClaimToken {
		t.Fatalf("resumed run owner=%q token-reused=%t", resumedRun.OwnerID, resumedRun.ClaimToken == oldFence.ClaimToken)
	}
	beforeEvents, err := f.store.ListEvents(f.ctx, run.SessionID, session.EventCursor{Limit: 100})
	if err != nil {
		t.Fatalf("list events before stale write: %v", err)
	}
	_, err = staleExecution.AppendEvent(f.ctx, session.EventRecord{
		ID: "postgres-resume-stale-event", SessionID: run.SessionID, RunID: run.ID,
		Kind: "stale", CreatedAt: time.Now().UTC(),
	})
	if !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale old-fence write = %v, want ErrConflict", err)
	}
	afterEvents, err := f.store.ListEvents(f.ctx, run.SessionID, session.EventCursor{Limit: 100})
	if err != nil {
		t.Fatalf("list events after stale write: %v", err)
	}
	if !reflect.DeepEqual(afterEvents, beforeEvents) {
		t.Fatal("stale old-fence write changed durable events")
	}
	close(release)
	result := awaitPostgresRuntime(t, f.ctx, handle)
	if result.Error != nil || result.Status != session.RunInterrupted || !result.Interrupted {
		t.Fatalf("resume status=%s interrupted=%t err=%v", result.Status, result.Interrupted, result.Error)
	}
	if executions.Load() != 1 {
		t.Fatalf("tool executions = %d, want 1", executions.Load())
	}
	if patternCalls.Load() != 0 {
		t.Fatalf("permission pattern resolver calls = %d, want 0", patternCalls.Load())
	}
	settled, err := f.store.GetToolCall(f.ctx, wantCall.ID)
	if err != nil {
		t.Fatalf("get settled tool: %v", err)
	}
	if settled.Status != session.ToolCallCompleted || settled.ClaimedBy != "new-owner" || string(settled.Output) == "" {
		t.Fatalf("settled tool status=%s owner=%q output-empty=%t", settled.Status, settled.ClaimedBy, len(settled.Output) == 0)
	}
	finishedRun, err := f.store.GetRun(f.ctx, run.ID)
	if err != nil {
		t.Fatalf("get settled run: %v", err)
	}
	if finishedRun.Status != session.RunInterrupted {
		t.Fatalf("settled run status = %s, want interrupted", finishedRun.Status)
	}
	assertPostgresRuntimeHistory(t, f.ctx, f.store, run, wantCall)
	second, err := resumer.Resume(f.ctx, run.ID)
	if err != nil {
		t.Fatalf("second resume: %v", err)
	}
	secondResult := awaitPostgresRuntime(t, f.ctx, second)
	if secondResult.Error != nil || secondResult.Status != session.RunInterrupted || executions.Load() != 1 {
		t.Fatalf("second resume status=%s err=%v executions=%d", secondResult.Status, secondResult.Error, executions.Load())
	}
	assertPostgresRuntimeHistory(t, f.ctx, f.store, run, wantCall)
}

func assertPostgresRuntimeNoReservedOutputs(t *testing.T, ctx context.Context, store session.Store, call session.ToolCall) {
	t.Helper()
	batch, err := store.ListMessages(ctx, call.SessionID, session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	for _, message := range batch.Messages {
		if message.ID == call.ResultMessageID {
			t.Fatalf("reserved result message %q materialized before settlement", call.ResultMessageID)
		}
	}
	for _, part := range batch.Parts {
		if part.ID == call.ResultPartID {
			t.Fatalf("reserved result part %q materialized before settlement", call.ResultPartID)
		}
	}
}

func assertPostgresRuntimeHistory(t *testing.T, ctx context.Context, store session.Store, run session.Run, call session.ToolCall) {
	t.Helper()
	batch, err := store.ListMessages(ctx, run.SessionID, session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatalf("list replay history: %v", err)
	}
	if len(batch.Messages) != 3 || batch.Messages[0].ID != "postgres-resume-user" || batch.Messages[1].ID != "postgres-resume-assistant" || batch.Messages[2].ID != call.ResultMessageID {
		t.Fatal("replay messages do not contain original history plus one tool result")
	}
	if len(batch.Parts) != 3 || batch.Parts[0].ID != "postgres-resume-user-part" || string(batch.Parts[0].Payload) != `{"text":"original history"}` || batch.Parts[1].ID != call.RequestPartID {
		t.Fatal("replayed parts do not contain committed history and request")
	}
	var results []session.Part
	for _, part := range batch.Parts {
		if part.Kind == session.PartToolResult {
			results = append(results, part)
		}
	}
	if len(results) != 1 || results[0].ID != call.ResultPartID || results[0].MessageID != call.ResultMessageID {
		t.Fatal("replay does not contain exactly one materialized reserved result")
	}
	var output ToolOutput
	if err := json.Unmarshal(results[0].Payload, &output); err != nil || output.Content != "resumed output" {
		t.Fatalf("tool result content = %q, want resumed output", output.Content)
	}
}

func postgresRuntimeBackend(t *testing.T, ctx context.Context, db *sql.DB) (*sql.Conn, int) {
	t.Helper()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire postgres backend: %v", err)
	}
	var pid int
	if err := conn.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		_ = conn.Close()
		t.Fatalf("read postgres backend PID: %v", err)
	}
	return conn, pid
}

func awaitPostgresRuntimeLeaseExpiry(t *testing.T, ctx context.Context, db *sql.DB, runID session.RunID) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var expired bool
		if err := db.QueryRowContext(ctx, `SELECT lease_until <= (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint FROM public.runs WHERE id = $1`, []byte(runID)).Scan(&expired); err != nil {
			t.Fatalf("poll lease expiry: %v", err)
		}
		if expired {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("lease did not expire: %v", ctx.Err())
		case <-deadline.C:
			t.Fatal("lease did not expire before deadline")
		case <-ticker.C:
		}
	}
}
