//go:build postgres_integration

package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/postgres"
)

// TestPostgresRestart proves that durable recovery survives both a host pool
// restart and a real PostgreSQL container restart. Each case owns a fresh
// database so that no state is inherited from the other restart boundary.
func TestPostgresRestart(t *testing.T) {
	server := testpostgres.Start(t)
	t.Run("host", func(t *testing.T) { testPostgresRestartCase(t, server, false) })
	t.Run("container", func(t *testing.T) { testPostgresRestartCase(t, server, true) })
}

type restartSnapshot struct {
	run         session.Run
	tool        session.ToolCall
	request     session.ModelRequestRecord
	runs        []session.Run
	tools       []session.ToolCall
	requests    session.ModelRequestBatch
	history     session.ReplayBatch
	events      session.EventBatch
	incarnation string
	historySQL  string
}

func testPostgresRestartCase(t *testing.T, server *testpostgres.Server, containerRestart bool) {
	t.Helper()
	database := server.Database(t)
	db, store := migratePostgresDatabase(t, database)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	now := time.Now().UTC().Truncate(time.Microsecond)
	sessionID := session.ID("restart-session")
	runID := session.RunID("restart-run")
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	plan := restartPlan(t, sessionID)
	run, err := store.AdmitRun(ctx, session.Run{
		ID: runID, SessionID: sessionID, OwnerID: "old-owner", ClaimToken: "old-fence",
		Agent: "restart-agent", ProviderID: "restart-provider", ModelID: "restart-model",
		Status: session.RunPending, ExtensionPlan: plan, CreatedAt: now,
	}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	oldFence := session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken}
	execution := store.Execution(oldFence)
	user := session.Message{ID: "restart-user", SessionID: sessionID, RunID: runID, Role: session.RoleUser, CreatedAt: now.Add(time.Nanosecond), UpdatedAt: now.Add(time.Nanosecond)}
	assistant := session.Message{ID: "restart-assistant", SessionID: sessionID, RunID: runID, ParentID: user.ID, Role: session.RoleAssistant, CreatedAt: now.Add(2 * time.Nanosecond), UpdatedAt: now.Add(2 * time.Nanosecond)}
	for _, message := range []session.Message{user, assistant} {
		if _, err := execution.AppendMessage(ctx, message); err != nil {
			t.Fatalf("append %s message: %v", message.ID, err)
		}
	}
	if _, err := execution.AppendPart(ctx, session.Part{
		ID: "restart-user-part", MessageID: user.ID, SessionID: sessionID, RunID: runID,
		Kind: session.PartText, Ordinal: 0, Payload: json.RawMessage(`{"text":"restart history"}`),
		CreatedAt: user.CreatedAt, UpdatedAt: user.UpdatedAt,
	}); err != nil {
		t.Fatalf("append user part: %v", err)
	}
	if _, err := execution.AppendPart(ctx, session.Part{
		ID: "restart-assistant-part", MessageID: assistant.ID, SessionID: sessionID, RunID: runID,
		Kind: session.PartText, Ordinal: 0, Payload: json.RawMessage(`{"text":"committed answer"}`),
		CreatedAt: assistant.CreatedAt, UpdatedAt: assistant.UpdatedAt,
	}); err != nil {
		t.Fatalf("append assistant part: %v", err)
	}
	if err := execution.FinalizeAssistantMessage(ctx, assistant.ID); err != nil {
		t.Fatalf("finalize assistant: %v", err)
	}
	call := session.ToolCall{
		ID: "restart-tool", SessionID: sessionID, RunID: runID, MessageID: assistant.ID,
		RequestPartID: "restart-tool-request", ResultMessageID: "restart-tool-result",
		ResultPartID: "restart-tool-result-part", Name: "echo", Pattern: "exact",
		Input: json.RawMessage(`{"text":"pending across restart"}`), Status: session.ToolCallPending,
		RetrySafe: true, Metadata: map[string]string{"fixture": "restart"},
	}
	created, err := execution.CreateToolCall(ctx, session.CreateToolCallRequest{
		Call: call,
		RequestPart: session.Part{ID: call.RequestPartID, MessageID: assistant.ID, SessionID: sessionID, RunID: runID, Kind: session.PartToolCall, Ordinal: 1,
			Payload: json.RawMessage(`{"id":"restart-tool","name":"echo","arguments":{"text":"pending across restart"}}`), CreatedAt: now.Add(3 * time.Nanosecond), UpdatedAt: now.Add(3 * time.Nanosecond)},
		Event: session.ToolTransitionEvent{ID: "restart-tool-pending", ProviderID: run.ProviderID, ModelID: run.ModelID, CreatedAt: now.Add(3 * time.Nanosecond)},
	})
	if err != nil {
		t.Fatalf("create pending tool: %v", err)
	}
	call = created.Call
	request := session.ModelRequestRecord{
		ID: "restart-model-request", SessionID: sessionID, RunID: runID, AssistantMessageID: assistant.ID,
		Attempt: 0, Step: 1, ProviderID: run.ProviderID, ModelID: run.ModelID, State: session.ModelRequestPrepared,
		Messages: json.RawMessage(`[{"role":"user","content":"restart history"}]`), System: "restart system",
		Tools: json.RawMessage(`[{"name":"echo"}]`), SafeCallConfig: json.RawMessage(`{"mode":"safe"}`),
		ContentSHA256: "restart-content", ExtensionPlanHash: plan.Fingerprint, CreatedAt: now.Add(4 * time.Nanosecond), UpdatedAt: now.Add(4 * time.Nanosecond),
	}
	if _, err := execution.CreateModelRequest(ctx, request); err != nil {
		t.Fatalf("create prepared model request: %v", err)
	}
	if _, err := execution.AppendEvent(ctx, session.EventRecord{ID: "restart-custom-event", SessionID: sessionID, RunID: runID, Kind: "restart-proof", Payload: json.RawMessage(`{"durable":true}`), CreatedAt: now.Add(5 * time.Nanosecond)}); err != nil {
		t.Fatalf("append committed event: %v", err)
	}
	assertRestartNoReservedOutputs(t, ctx, db, call)
	// Keep the seed lease long enough for setup, then make expiry bounded at the
	// restart boundary using the database clock.
	run, err = execution.RenewRunLease(ctx, 250*time.Millisecond)
	if err != nil {
		t.Fatalf("shorten seed lease: %v", err)
	}
	oldFence = session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken}
	before := restartReadSnapshot(t, ctx, db, store, sessionID, runID, call.ID, request.ID)
	assertRestartSeedSnapshot(t, before, run, call, request)
	assertRestartNoReservedOutputs(t, ctx, db, call)
	beforePostmaster := restartPostmasterTime(t, ctx, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close host pool before boundary: %v", err)
	}
	if containerRestart {
		server.Restart(t)
	}
	db = database.Open(t)
	store, err = postgres.New(ctx, db)
	if err != nil {
		t.Fatalf("reopen postgres store without migration: %v", err)
	}
	if containerRestart && !restartPostmasterTime(t, ctx, db).After(beforePostmaster) {
		t.Fatalf("container restart did not change postmaster start time")
	}
	after := restartReadSnapshot(t, ctx, db, store, sessionID, runID, call.ID, request.ID)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("durable records changed across restart")
	}
	assertRestartNoReservedOutputs(t, ctx, db, call)
	waitRestartLeaseExpiry(t, ctx, db, runID)
	claimed, err := store.ClaimRun(ctx, session.RunClaim{RunID: runID, OwnerID: "new-owner", ClaimToken: "new-fence", LeaseDuration: time.Minute})
	if err != nil {
		t.Fatalf("claim expired run: %v", err)
	}
	if claimed.OwnerID != "new-owner" || claimed.ClaimToken != "new-fence" || !reflect.DeepEqual(claimed.ExtensionPlan, plan) {
		t.Fatal("claimed run does not have the requested new fence and original extension plan")
	}
	eventsBeforeStale := readRestartEvents(t, ctx, store, sessionID)
	if _, err := store.Execution(oldFence).AppendEvent(ctx, session.EventRecord{ID: "restart-stale-event", SessionID: sessionID, RunID: runID, Kind: "stale", CreatedAt: time.Now().UTC()}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale old fence error = %v, want ErrConflict", err)
	}
	if got := readRestartEvents(t, ctx, store, sessionID); !reflect.DeepEqual(eventsBeforeStale, got) {
		t.Fatal("stale old-fence event changed durable history")
	}
	claimedSnapshot := restartReadSnapshot(t, ctx, db, store, sessionID, runID, call.ID, request.ID)
	if !reflect.DeepEqual(claimedSnapshot.tool, before.tool) || !reflect.DeepEqual(claimedSnapshot.request, before.request) || !reflect.DeepEqual(claimedSnapshot.history, before.history) || !reflect.DeepEqual(claimedSnapshot.events, before.events) {
		t.Fatal("claim changed persisted tool, model request, or replay history")
	}
	assertRestartNoReservedOutputs(t, ctx, db, call)
	if got := restartSchemaState(t, ctx, db); got.incarnation != before.incarnation || got.historySQL != before.historySQL {
		t.Fatal("schema incarnation or migration history changed across restart")
	}
}

func restartPlan(t *testing.T, sessionID session.ID) session.ExtensionPlanDescriptor {
	t.Helper()
	descriptor := session.ExtensionPlanDescriptor{Components: []session.ComponentPlan{{
		InstanceID: "restart-tools", Artifact: extension.Artifact{Name: "restart-tools", Version: "1", Hash: "restart-tools-hash", ConfigHash: "restart-tools-config", SourceKind: extension.SourceNative},
		Tools: []session.ToolPlanIdentity{{Name: "echo", RegistrationID: "restart-tool", Scope: extension.GlobalScope(), SchemaHash: "restart-schema", ExecutorHash: "restart-executor", Order: 0}},
	}}}
	sealed, err := session.SealExtensionPlanForSession(sessionID, descriptor)
	if err != nil {
		t.Fatalf("seal restart extension plan: %v", err)
	}
	return sealed.Descriptor()
}

func restartReadSnapshot(t *testing.T, ctx context.Context, db *sql.DB, store session.Store, sessionID session.ID, runID session.RunID, toolID session.ToolCallID, requestID session.ModelRequestID) restartSnapshot {
	t.Helper()
	run, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run snapshot: %v", err)
	}
	tool, err := store.GetToolCall(ctx, toolID)
	if err != nil {
		t.Fatalf("get tool snapshot: %v", err)
	}
	request, err := store.GetModelRequest(ctx, requestID)
	if err != nil {
		t.Fatalf("get model request snapshot: %v", err)
	}
	runs, err := store.ListUnfinishedRuns(ctx)
	if err != nil {
		t.Fatalf("list unfinished runs: %v", err)
	}
	tools, err := store.ListUnfinishedToolCalls(ctx, runID)
	if err != nil {
		t.Fatalf("list unfinished tools: %v", err)
	}
	requests, err := store.ListModelRequests(ctx, runID, session.ModelRequestCursor{Limit: 10})
	if err != nil {
		t.Fatalf("list model requests: %v", err)
	}
	history, err := store.ListMessages(ctx, sessionID, session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatalf("list replay history: %v", err)
	}
	events := readRestartEvents(t, ctx, store, sessionID)
	sch := restartSchemaState(t, ctx, db)
	return restartSnapshot{run: run, tool: tool, request: request, runs: runs, tools: tools, requests: requests, history: history, events: events, incarnation: sch.incarnation, historySQL: sch.historySQL}
}

func readRestartEvents(t *testing.T, ctx context.Context, store session.Store, sessionID session.ID) session.EventBatch {
	t.Helper()
	events, err := store.ListEvents(ctx, sessionID, session.EventCursor{Limit: 100})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	return events
}

func assertRestartSeedSnapshot(t *testing.T, got restartSnapshot, run session.Run, call session.ToolCall, request session.ModelRequestRecord) {
	t.Helper()
	if !reflect.DeepEqual(got.run, run) || len(got.runs) != 1 || !reflect.DeepEqual(got.runs[0], run) {
		t.Fatal("unfinished run snapshot did not retain the original fence and plan")
	}
	if !reflect.DeepEqual(got.tool, call) || len(got.tools) != 1 || !reflect.DeepEqual(got.tools[0], call) {
		t.Fatal("unfinished tool snapshot did not retain the original call")
	}
	if !reflect.DeepEqual(got.request, request) || len(got.requests.Records) != 1 || !reflect.DeepEqual(got.requests.Records[0], request) || got.requests.Next != (session.ModelRequestCursor{}) {
		t.Fatal("model request snapshot did not retain the prepared record")
	}
	if len(got.history.Messages) != 2 || len(got.history.Parts) != 3 || got.history.Next != (session.ReplayCursor{}) {
		t.Fatal("replay snapshot did not contain the committed message and part graph")
	}
	if len(got.events.Events) != 2 || got.events.Next != (session.EventCursor{}) {
		t.Fatal("event snapshot did not contain both committed events")
	}
}

func assertRestartNoReservedOutputs(t *testing.T, ctx context.Context, db *sql.DB, call session.ToolCall) {
	t.Helper()
	for table, id := range map[string][]byte{"messages": []byte(call.ResultMessageID), "parts": []byte(call.ResultPartID)} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM public."+table+" WHERE id=$1", id).Scan(&count); err != nil {
			t.Fatalf("count reserved %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("pending tool result %s count = %d, want zero", table, count)
		}
	}
}

type restartSchema struct{ incarnation, historySQL string }

func restartSchemaState(t *testing.T, ctx context.Context, db *sql.DB) restartSchema {
	t.Helper()
	var result restartSchema
	if err := db.QueryRowContext(ctx, `SELECT incarnation FROM public.observation_store WHERE singleton=1`).Scan(&result.incarnation); err != nil {
		t.Fatalf("read store incarnation: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT coalesce(jsonb_agg(to_jsonb(v) ORDER BY id),'[]'::jsonb)::text FROM public.eino_agent_goose_version v`).Scan(&result.historySQL); err != nil {
		t.Fatalf("read migration history: %v", err)
	}
	return result
}

func restartPostmasterTime(t *testing.T, ctx context.Context, db *sql.DB) time.Time {
	t.Helper()
	var value time.Time
	if err := db.QueryRowContext(ctx, `SELECT pg_postmaster_start_time()`).Scan(&value); err != nil {
		t.Fatalf("read postmaster start time: %v", err)
	}
	return value
}

func waitRestartLeaseExpiry(t *testing.T, ctx context.Context, db *sql.DB, runID session.RunID) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var expired bool
		if err := db.QueryRowContext(ctx, `SELECT lease_until <= (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint FROM public.runs WHERE id=$1`, []byte(runID)).Scan(&expired); err != nil {
			t.Fatalf("poll run lease: %v", err)
		}
		if expired {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("run lease did not expire: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
