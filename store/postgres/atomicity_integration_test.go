//go:build postgres_integration

package postgres_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
)

const unrelatedAtomicityEvent = session.EventID("atomicity-unrelated-event")

func TestPostgresAtomicity(t *testing.T) {
	server := testpostgres.Start(t)
	t.Run("mutation", func(t *testing.T) { testMutationAtomicity(t, server) })
	t.Run("cleanup", func(t *testing.T) { testCleanupFailures(t, server) })
}

func testMutationAtomicity(t *testing.T, server *testpostgres.Server) {
	t.Run("event", func(t *testing.T) { testEventAtomicity(t, server) })
	t.Run("result_part", func(t *testing.T) { testResultPartAtomicity(t, server) })
	t.Run("model_request", func(t *testing.T) { testModelRequestAtomicity(t, server) })
	t.Run("revision", func(t *testing.T) { testRevisionAtomicity(t, server) })
}

func testEventAtomicity(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 2)
	f.seed(t)
	baseline := f.snapshot(t)
	target := session.EventID("atomicity-failed-event")
	installAtomicityTrigger(t, f, "events", "events_atomicity_fault", "NEW.id = "+byteaLiteral(string(target)), "event mutation fault")
	request := session.SettleRunRequest{Settlement: session.RunSettlement{Status: session.RunCompleted, FinishedAt: f.now.Add(time.Minute)}, Event: session.RunSettlementEvent{ID: target}}
	fence := session.RunFence{RunID: "run", ClaimToken: "old"}
	runAtomicityCase(t, f, baseline, fence, func(ctx context.Context, ex session.ExecutionStore) error {
		_, err := ex.SettleRun(ctx, request)
		assertInjectedError(t, err, "event mutation fault")
		return nil
	})
	removeAtomicityTrigger(t, f, "events", "events_atomicity_fault")
	if _, err := f.stores[0].Execution(fence).SettleRun(f.ctx, request); err != nil {
		t.Fatalf("event positive control: %v", err)
	}
	run, err := f.stores[1].GetRun(f.ctx, "run")
	if err != nil || run.Status != session.RunCompleted {
		t.Fatalf("event positive control run status = %q, %v", run.Status, err)
	}
	assertEventIDs(t, f, target, unrelatedAtomicityEvent)
}

func testResultPartAtomicity(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 2)
	f.seed(t)
	call := seedClaimedTool(t, f)
	baseline := f.snapshot(t)
	resultPart := call.ResultPartID
	installAtomicityTrigger(t, f, "parts", "parts_atomicity_fault", "NEW.id = "+byteaLiteral(string(resultPart)), "result part mutation fault")
	request := toolSettlement(call, f.now.Add(time.Minute), session.ToolCallCompleted, json.RawMessage(`{"ok":true}`), "", "atomicity-failed-terminal")
	fence := session.RunFence{RunID: "run", ClaimToken: "old"}
	runAtomicityCase(t, f, baseline, fence, func(ctx context.Context, ex session.ExecutionStore) error {
		_, err := ex.SettleToolCall(ctx, request)
		assertInjectedError(t, err, "result part mutation fault")
		return nil
	})
	removeAtomicityTrigger(t, f, "parts", "parts_atomicity_fault")
	if _, err := f.stores[0].Execution(fence).SettleToolCall(f.ctx, request); err != nil {
		t.Fatalf("result part positive control: %v", err)
	}
	stored, err := f.stores[1].GetToolCall(f.ctx, call.ID)
	if err != nil || stored.Status != session.ToolCallCompleted {
		t.Fatalf("result part positive control status = %q, %v", stored.Status, err)
	}
	batch, err := f.stores[1].ListMessages(f.ctx, "session", session.ReplayCursor{})
	if err != nil || !hasMessagePart(batch, call.ResultMessageID, resultPart) {
		t.Fatalf("result part positive control graph missing: messages=%d parts=%d err=%v", len(batch.Messages), len(batch.Parts), err)
	}
}

func testModelRequestAtomicity(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 2)
	run := f.seed(t)
	baseline := f.snapshot(t)
	record := fencingModelRequest(run, "atomicity-model-request", f.now, session.ModelRequestPrepared)
	installAtomicityTrigger(t, f, "model_requests", "model_requests_atomicity_fault", "NEW.id = "+byteaLiteral(string(record.ID)), "model request mutation fault")
	fence := session.RunFence{RunID: "run", ClaimToken: "old"}
	runAtomicityCase(t, f, baseline, fence, func(ctx context.Context, ex session.ExecutionStore) error {
		_, err := ex.CreateModelRequest(ctx, record)
		assertInjectedError(t, err, "model request mutation fault")
		return nil
	})
	removeAtomicityTrigger(t, f, "model_requests", "model_requests_atomicity_fault")
	if _, err := f.stores[0].Execution(fence).CreateModelRequest(f.ctx, record); err != nil {
		t.Fatalf("model request positive control: %v", err)
	}
	stored, err := f.stores[1].GetModelRequest(f.ctx, record.ID)
	if err != nil || stored.ID != record.ID || stored.State != record.State || string(stored.Messages) != string(record.Messages) {
		t.Fatalf("model request positive control mismatch: id=%q state=%q err=%v", stored.ID, stored.State, err)
	}
}

func testRevisionAtomicity(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 2)
	run := f.seed(t)
	baseline := f.snapshot(t)
	message := session.Message{ID: "atomicity-revision-message", SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: f.now, UpdatedAt: f.now}
	installAtomicityTrigger(t, f, "observation_revisions", "revisions_atomicity_fault", "NEW.session_id = "+byteaLiteral(string(run.SessionID)), "revision mutation fault")
	fence := session.RunFence{RunID: "run", ClaimToken: "old"}
	runAtomicityCase(t, f, baseline, fence, func(ctx context.Context, ex session.ExecutionStore) error {
		_, err := ex.AppendMessage(ctx, message)
		assertInjectedError(t, err, "revision mutation fault")
		return nil
	})
	removeAtomicityTrigger(t, f, "observation_revisions", "revisions_atomicity_fault")
	if _, err := f.stores[0].Execution(fence).AppendMessage(f.ctx, message); err != nil {
		t.Fatalf("revision positive control: %v", err)
	}
	batch, err := f.stores[1].ListMessages(f.ctx, run.SessionID, session.ReplayCursor{})
	if err != nil || !hasMessage(batch, message.ID) {
		t.Fatalf("revision positive control message missing: count=%d err=%v", len(batch.Messages), err)
	}
}

func runAtomicityCase(t *testing.T, f *raceFixture, baseline string, fence session.RunFence, failed func(context.Context, session.ExecutionStore) error) {
	t.Helper()
	err := f.stores[0].WithinTx(f.ctx, func(ctx context.Context, tx session.Store) error {
		if err := failed(ctx, tx.Execution(fence)); err != nil {
			return err
		}
		if got := f.snapshot(t); got != baseline {
			t.Fatal("failed mutation changed observer-visible state before outer commit")
		}
		_, err := tx.Execution(fence).AppendEvent(ctx, session.EventRecord{ID: unrelatedAtomicityEvent, SessionID: "session", RunID: "run", Kind: "atomicity_unrelated", CreatedAt: f.now})
		return err
	})
	if err != nil {
		t.Fatalf("outer transaction after injected mutation error: %v", err)
	}
	if got := f.snapshotExcludingEvent(t, unrelatedAtomicityEvent); got != baseline {
		t.Fatal("failed mutation left durable rows or revisions after unrelated commit")
	}
	assertEventIDs(t, f, unrelatedAtomicityEvent)
}

func installAtomicityTrigger(t *testing.T, f *raceFixture, table, trigger, condition, message string) {
	t.Helper()
	function := trigger + "_fn"
	ctx := f.ctx
	functionSQL := fmt.Sprintf(`CREATE FUNCTION public.%s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF %s THEN RAISE EXCEPTION '%s' USING ERRCODE = 'P0001'; END IF; RETURN NEW; END $$`, function, condition, message)
	triggerSQL := fmt.Sprintf("CREATE TRIGGER %s BEFORE INSERT OR UPDATE ON public.%s FOR EACH ROW EXECUTE FUNCTION public.%s()", trigger, table, function)
	if _, err := f.dbs[0].ExecContext(ctx, functionSQL); err != nil {
		t.Fatalf("install atomicity function: %v", err)
	}
	t.Cleanup(func() {
		if err := dropAtomicityTrigger(f, table, trigger); err != nil {
			t.Errorf("cleanup atomicity trigger: %v", err)
		}
	})
	if _, err := f.dbs[0].ExecContext(ctx, triggerSQL); err != nil {
		t.Fatalf("install atomicity trigger: %v", err)
	}
}

func removeAtomicityTrigger(t *testing.T, f *raceFixture, table, trigger string) {
	t.Helper()
	if err := dropAtomicityTrigger(f, table, trigger); err != nil {
		t.Fatalf("remove atomicity trigger: %v", err)
	}
}

func dropAtomicityTrigger(f *raceFixture, table, trigger string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(f.ctx), 5*time.Second)
	defer cancel()
	function := trigger + "_fn"
	var result error
	if _, err := f.dbs[0].ExecContext(ctx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON public.%s", trigger, table)); err != nil {
		result = errors.Join(result, err)
	}
	if _, err := f.dbs[0].ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS public.%s()", function)); err != nil {
		result = errors.Join(result, err)
	}
	return result
}

func assertInjectedError(t *testing.T, err error, message string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "P0001" || pgErr.Message != message {
		if pgErr == nil {
			t.Fatalf("injected error identity mismatch: type=%T", err)
		}
		t.Fatalf("injected error identity mismatch: code=%q message=%q", pgErr.Code, pgErr.Message)
	}
}

func byteaLiteral(value string) string {
	return "pg_catalog.decode('" + hex.EncodeToString([]byte(value)) + "', 'hex')"
}

func assertEventIDs(t *testing.T, f *raceFixture, want ...session.EventID) {
	t.Helper()
	events, err := f.stores[1].ListEvents(f.ctx, "session", session.EventCursor{})
	if err != nil {
		t.Fatalf("list atomicity events: %v", err)
	}
	seen := make(map[session.EventID]bool, len(events.Events))
	for _, event := range events.Events {
		seen[event.ID] = true
	}
	for _, id := range want {
		if !seen[id] {
			t.Fatalf("atomicity event %q missing", id)
		}
	}
}

func hasMessage(batch session.ReplayBatch, id session.MessageID) bool {
	for _, message := range batch.Messages {
		if message.ID == id {
			return true
		}
	}
	return false
}

func hasMessagePart(batch session.ReplayBatch, messageID session.MessageID, partID session.PartID) bool {
	if !hasMessage(batch, messageID) {
		return false
	}
	for _, part := range batch.Parts {
		if part.ID == partID && part.MessageID == messageID {
			return true
		}
	}
	return false
}
