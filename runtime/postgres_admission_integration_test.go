//go:build postgres_integration

package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
	"github.com/mattsp1290/eino-agent/store/postgres"
)

const runtimeAdmissionTriggerError = "runtime admission rollback trigger"

func testPostgresRuntimeAdmission(t *testing.T, server *testpostgres.Server) {
	f := newPostgresRuntimeFixture(t, server)
	removeProbe := installRuntimeAdmissionOrderProbe(t, f)

	var calls atomic.Int32
	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		calls.Add(1)
		return []*einoschema.Message{einoschema.AssistantMessage("postgres answer", nil)}, nil
	})
	orchestrator, err := NewStreamingOrchestrator(
		WithStore(f.store), WithModelResolver(resolvedModel{streamer: streamer}),
		WithIDGenerator(&reverseAdmissionIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()),
		WithOwnerID("postgres-admission-test"), WithClock(func() time.Time {
			return time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	const sessionID session.ID = "postgres-admission-success"
	namedAt := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	namedRoot, err := canonicalAdmissionWorkspace(orchestratorConfig().Metadata["workspace_root"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CreateSession(f.ctx, session.Session{
		ID: sessionID, WorkspaceID: "workspace-1", Directory: namedRoot,
		Title: "Postgres named conversation", Metadata: map[string]string{"source": "postgres"}, CreatedAt: namedAt, UpdatedAt: namedAt,
	}); err != nil {
		t.Fatal(err)
	}
	admission, err := orchestrator.Start(f.ctx, Request{
		SessionID: sessionID, Message: UserMessage{Content: "postgres question"},
		Config: orchestratorConfig(), Metadata: map[string]string{"source": "postgres"},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitPostgresRuntime(t, f.ctx, admission.Handle)
	if result.Status != session.RunCompleted || result.Error != nil || calls.Load() != 1 {
		t.Fatalf("result=%+v model calls=%d", result, calls.Load())
	}
	if named, err := f.store.GetSession(f.ctx, sessionID); err != nil || named.Title != "Postgres named conversation" || !named.UpdatedAt.Equal(namedAt) {
		t.Fatalf("named session=%#v err=%v", named, err)
	}
	assertPostgresAdmissionGraph(t, f, sessionID, result.RunID, "postgres question", "postgres answer")

	watermark, err := f.store.ReadObservationRevision(f.ctx, sessionID)
	if err != nil || watermark.Revision <= 0 {
		t.Fatalf("observation revision=%+v err=%v", watermark, err)
	}
	removeProbe()
	f.reopen(t)
	historyMessages, err := history.Load(f.ctx, f.store, sessionID, history.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(historyMessages) != 2 || historyMessages[0].Role != einoschema.User || historyMessages[0].Content != "postgres question" ||
		historyMessages[1].Role != einoschema.Assistant || historyMessages[1].Content != "postgres answer" {
		t.Fatalf("reopened history=%#v", historyMessages)
	}
}

func testPostgresKeyedAdmissionRace(t *testing.T, server *testpostgres.Server) {
	f := newPostgresRuntimeFixture(t, server)
	secondStore, err := postgres.New(f.ctx, f.db)
	if err != nil {
		t.Fatal(err)
	}
	assertConcurrentKeyedAdmission(t, f.ctx, []session.Store{f.store, secondStore}, "postgres-keyed-race")
}

func testPostgresRuntimeAdmissionRollback(t *testing.T, server *testpostgres.Server) {
	f := newPostgresRuntimeFixture(t, server)
	removeProbe := installRuntimeAdmissionRollbackTrigger(t, f)
	var calls atomic.Int32
	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		calls.Add(1)
		return []*einoschema.Message{einoschema.AssistantMessage("retry answer", nil)}, nil
	})
	orchestrator, err := NewStreamingOrchestrator(
		WithStore(f.store), WithModelResolver(resolvedModel{streamer: streamer}),
		WithIDGenerator(&reverseAdmissionIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()),
		WithOwnerID("postgres-admission-rollback-test"),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = orchestrator.Start(f.ctx, Request{
		SessionID: "postgres-admission-rollback", Message: UserMessage{Content: "failed question"},
		Config: orchestratorConfig(),
	})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "P0001" || pgErr.Message != runtimeAdmissionTriggerError {
		t.Fatalf("admission error=%v type=%T, want trigger error", err, err)
	}
	if calls.Load() != 0 {
		t.Fatalf("model calls after failed admission=%d", calls.Load())
	}
	assertPostgresAdmissionCountsZero(t, f)
	removeProbe()

	admission, err := orchestrator.Start(f.ctx, Request{
		SessionID: "postgres-admission-rollback", Message: UserMessage{Content: "retry question"},
		Config: orchestratorConfig(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitPostgresRuntime(t, f.ctx, admission.Handle)
	if result.Status != session.RunCompleted || result.Error != nil || calls.Load() != 1 {
		t.Fatalf("retry result=%+v model calls=%d", result, calls.Load())
	}
}

func assertPostgresAdmissionGraph(t *testing.T, f *postgresRuntimeFixture, sessionID session.ID, runID session.RunID, userText, assistantText string) {
	t.Helper()
	batch, err := f.store.ListMessages(f.ctx, sessionID, session.ReplayCursor{Limit: 100})
	if err != nil || len(batch.Messages) != 2 || len(batch.Parts) != 2 {
		t.Fatalf("messages=%d parts=%d err=%v", len(batch.Messages), len(batch.Parts), err)
	}
	user, assistant := batch.Messages[0], batch.Messages[1]
	if user.ID != "z-user-1" || assistant.ID != "a-assistant-1" || user.Role != session.RoleUser || assistant.Role != session.RoleAssistant ||
		user.RunID != runID || assistant.RunID != runID || assistant.ParentID != user.ID {
		t.Fatalf("message graph ids=%q,%q", batch.Messages[0].ID, batch.Messages[1].ID)
	}
	if run, err := f.store.GetRun(f.ctx, runID); err != nil || run.ParentMsgID != user.ID || run.ContextEpoch != "epoch-1" || run.Status != session.RunCompleted {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	epochs, err := f.store.ListContextEpochs(f.ctx, sessionID)
	if err != nil || len(epochs) != 1 || epochs[0].ID != "epoch-1" || epochs[0].SessionID != sessionID {
		t.Fatalf("epoch count=%d err=%v", len(epochs), err)
	}
	events, err := f.store.ListEvents(f.ctx, sessionID, session.EventCursor{Limit: 100})
	if err != nil || len(events.Events) != 2 || events.Events[0].Kind != EventRunStarted || events.Events[0].EpochID != epochs[0].ID || events.Events[0].MessageID != assistant.ID || events.Events[1].Kind != EventRunFinished {
		t.Fatalf("event count=%d err=%v", len(events.Events), err)
	}
	observation, err := f.store.ReadObservationSnapshot(f.ctx, sessionID, session.ObservationLimits{MaxMessages: 10, MaxTools: 10, MaxParts: 20, MaxSnapshotBytes: 1 << 20, MaxTextBytes: 1 << 20})
	if err != nil || len(observation.Messages) != 2 || observation.Messages[0].Text != userText || observation.Messages[1].Text != assistantText {
		t.Fatalf("observation messages=%d err=%v", len(observation.Messages), err)
	}
}

func installRuntimeAdmissionOrderProbe(t *testing.T, f *postgresRuntimeFixture) func() {
	t.Helper()
	remove := registerRuntimeAdmissionTriggerCleanup(t, f)
	_, err := f.db.ExecContext(f.ctx, `CREATE FUNCTION public.runtime_admission_order_probe() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.context_epochs) OR EXISTS (SELECT 1 FROM public.messages) OR EXISTS (SELECT 1 FROM public.events) THEN
    RAISE EXCEPTION 'runtime admission order witness failed' USING ERRCODE = 'P0001';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_admission_order_probe BEFORE INSERT ON public.runs FOR EACH ROW EXECUTE FUNCTION public.runtime_admission_order_probe()`)
	if err != nil {
		t.Fatal(err)
	}
	return remove
}

func installRuntimeAdmissionRollbackTrigger(t *testing.T, f *postgresRuntimeFixture) func() {
	t.Helper()
	remove := registerRuntimeAdmissionTriggerCleanup(t, f)
	_, err := f.db.ExecContext(f.ctx, `CREATE FUNCTION public.runtime_admission_rollback_probe() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM public.context_epochs WHERE id = convert_to('epoch-1', 'UTF8')) OR
     NOT EXISTS (SELECT 1 FROM public.messages WHERE id = convert_to('z-user-1', 'UTF8') AND role = 'user') OR
     NOT EXISTS (SELECT 1 FROM public.parts WHERE id = convert_to('part-1', 'UTF8') AND kind = 'text') OR
     NOT EXISTS (SELECT 1 FROM public.messages WHERE id = convert_to('a-assistant-1', 'UTF8') AND role = 'assistant') THEN
    RAISE EXCEPTION 'runtime admission order witness missing' USING ERRCODE = 'P0001';
  END IF;
  RAISE EXCEPTION '`+runtimeAdmissionTriggerError+`' USING ERRCODE = 'P0001';
END $$;
CREATE TRIGGER runtime_admission_rollback_probe BEFORE INSERT ON public.events FOR EACH ROW EXECUTE FUNCTION public.runtime_admission_rollback_probe()`)
	if err != nil {
		t.Fatal(err)
	}
	return remove
}

func registerRuntimeAdmissionTriggerCleanup(t *testing.T, f *postgresRuntimeFixture) func() {
	t.Helper()
	var active atomic.Bool
	active.Store(true)
	remove := func() {
		if !active.Swap(false) {
			return
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(f.ctx), 5*time.Second)
		defer cancel()
		if _, err := f.db.ExecContext(ctx, `DROP TRIGGER IF EXISTS runtime_admission_order_probe ON public.runs; DROP TRIGGER IF EXISTS runtime_admission_rollback_probe ON public.events; DROP FUNCTION IF EXISTS public.runtime_admission_order_probe(); DROP FUNCTION IF EXISTS public.runtime_admission_rollback_probe()`); err != nil {
			t.Errorf("remove runtime admission trigger: %v", err)
		}
	}
	t.Cleanup(remove)
	return remove
}

func assertPostgresAdmissionCountsZero(t *testing.T, f *postgresRuntimeFixture) {
	t.Helper()
	var counts [9]int
	err := f.db.QueryRowContext(f.ctx, `SELECT (SELECT count(*) FROM public.sessions), (SELECT count(*) FROM public.runs), (SELECT count(*) FROM public.messages), (SELECT count(*) FROM public.parts), (SELECT count(*) FROM public.tool_calls), (SELECT count(*) FROM public.context_epochs), (SELECT count(*) FROM public.model_requests), (SELECT count(*) FROM public.events), (SELECT count(*) FROM public.observation_revisions)`).Scan(&counts[0], &counts[1], &counts[2], &counts[3], &counts[4], &counts[5], &counts[6], &counts[7], &counts[8])
	if err != nil {
		t.Fatal(err)
	}
	for index, count := range counts {
		if count != 0 {
			t.Fatalf("table index %d retained %d rows after admission rollback", index, count)
		}
	}
}
