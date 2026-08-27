package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func TestStreamingOrchestratorCompletesSuccessfulTurn(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("hel", nil), einoschema.AssistantMessage("lo", nil)}, nil
	}))
	handle, err := orch.Start(context.Background(), Request{
		SessionID: "session-1",
		ParentID:  "user-1",
		Input:     []*einoschema.Message{einoschema.UserMessage("hello")},
		Config:    orchestratorConfig(),
	})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	run, err := store.GetRun(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("GetRun error = %v", err)
	}
	if run.Status != session.RunCompleted {
		t.Fatalf("run status = %s", run.Status)
	}
	var textParts []session.Part
	for _, part := range store.parts {
		if part.Kind == session.PartText {
			textParts = append(textParts, part)
		}
	}
	if len(textParts) != 1 || string(textParts[0].Payload) != `{"text":"hello"}` {
		t.Fatalf("text parts = %#v", textParts)
	}
}

func TestStreamingOrchestratorUsesCanonicalEventSinkForAdmission(t *testing.T) {
	store := newAdmissionStore()
	runtimeSink := &capturingSink{}
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "admission-events", Artifact: extension.Artifact{Name: "admission-events", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var published []EventKind
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, EventPublishedPoint, extension.Registration{ID: "published", Scope: extension.GlobalScope()}, func(_ context.Context, event Event) error {
			published = append(published, event.Kind)
			return nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	orchestrator := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	}), WithEventSink(runtimeSink), WithRunPlanProvider(staticRunPlanProvider{plan: newTestDispatchPlan(dispatch)}))

	result := startAndWait(t, orchestrator)
	if result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	var starts int
	for _, event := range runtimeSink.events {
		if event.Kind == EventRunStarted {
			starts++
		}
	}
	if starts != 1 || len(runtimeSink.events) < 2 {
		t.Fatalf("runtime events = %#v, want one admission start plus execution events", runtimeSink.events)
	}
	var publishedStarts int
	for _, kind := range published {
		if kind == EventRunStarted {
			publishedStarts++
		}
	}
	if publishedStarts != 1 {
		t.Fatalf("published admission starts = %d, all events = %v", publishedStarts, published)
	}
}

func TestStreamingOrchestratorLoadsDurableHistoryBeforeCurrentInput(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	now := time.Date(2026, 6, 27, 11, 0, 0, 0, time.UTC)
	workspaceRoot, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = store.CreateSession(context.Background(), session.Session{
		ID:        "session-1",
		Directory: workspaceRoot,
		Title:     "session-1",
		CreatedAt: now,
		UpdatedAt: now,
	})
	_, _ = store.AppendMessage(context.Background(), session.Message{
		ID:        "prior-assistant",
		SessionID: "session-1",
		Role:      session.RoleAssistant,
		CreatedAt: now,
		UpdatedAt: now,
	})
	_, _ = store.AppendPart(context.Background(), session.Part{
		ID:        "prior-text",
		MessageID: "prior-assistant",
		SessionID: "session-1",
		Kind:      session.PartText,
		Payload:   []byte(`{"text":"previous"}`),
		CreatedAt: now,
		UpdatedAt: now,
	})
	var got []string
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, msg := range request.Messages {
			got = append(got, msg.Content)
		}
		return []*einoschema.Message{einoschema.AssistantMessage("next", nil)}, nil
	}))
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	if len(got) != 2 || got[0] != "previous" || got[1] != "hello" {
		t.Fatalf("provider messages = %#v", got)
	}
}
