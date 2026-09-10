package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func TestSQLiteKeyedAdmissionSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "admission.db")
	store, pool, err := openTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	orch := mustConfiguredOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
		})}), WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()),
	)
	request := Request{SessionID: "reopened", AdmissionKey: "event-44", Message: UserMessage{Content: "hello"}, Config: keyedAdmissionConfig(t)}
	first, err := orch.Start(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	<-first.Handle.Done()
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, reopenedPool, err := reopenTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopenedPool.Close() }()
	resolver := &countingResolver{}
	reader := mustConfiguredOrchestrator(WithStore(reopened), WithModelResolver(resolver), WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()))
	duplicate, err := reader.Start(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Disposition != AdmissionExisting || duplicate.Handle != nil || duplicate.Receipt != first.Receipt {
		t.Fatalf("duplicate = %#v, first = %#v", duplicate, first)
	}
	if resolver.calls.Load() != 0 {
		t.Fatalf("duplicate resolved a provider")
	}
	lookup, err := reader.LookupAdmission(ctx, request.SessionID, request.AdmissionKey)
	if err != nil || lookup.Receipt != first.Receipt || lookup.RunStatus != session.RunCompleted {
		t.Fatalf("lookup = %#v, %v", lookup, err)
	}
}

func TestSQLiteConcurrentKeyedAdmissionCommitsOneReceipt(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "concurrent.db")
	store, pool, err := openTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pool.Close() }()
	// Reopen the same database through an independent pool so arbitration is
	// exercised across store instances rather than one shared wrapper.
	secondStore, secondPool, err := reopenTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = secondPool.Close() }()
	assertConcurrentKeyedAdmission(t, ctx, []session.Store{store, secondStore}, "sqlite-race")
}

func TestSQLiteKeyedAdmissionPreservesRenamedSession(t *testing.T) {
	ctx := t.Context()
	store, pool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "renamed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pool.Close() }()
	request := Request{SessionID: "renamed-keyed", AdmissionKey: "renamed-event", Message: UserMessage{Content: "hello"}, Config: keyedAdmissionConfig(t)}
	namedAt := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{
		ID: request.SessionID, WorkspaceID: request.Config.Metadata["workspace_id"], Directory: request.Config.Metadata["workspace_root"],
		Title: "Host-owned title", CreatedAt: namedAt, UpdatedAt: namedAt,
	}); err != nil {
		t.Fatal(err)
	}
	orch := mustConfiguredOrchestrator(
		WithStore(store), WithModelResolver(&countingResolver{}), WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
	)
	admission, err := orch.Start(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if admission.Disposition != AdmissionNew || admission.Handle == nil {
		t.Fatalf("admission=%#v", admission)
	}
	<-admission.Handle.Done()
	stored, err := store.GetSession(ctx, request.SessionID)
	if err != nil || stored.Title != "Host-owned title" || !stored.UpdatedAt.Equal(namedAt) {
		t.Fatalf("stored session=%#v err=%v", stored, err)
	}
}
