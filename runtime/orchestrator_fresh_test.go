package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
)

func TestConcurrentStartsWithSameIDsAdmitAndDispatchOnce(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var dispatches atomic.Int32
	newOrchestrator := func(sink *blockingSink) *StreamingOrchestrator {
		return mustConfiguredOrchestrator(
			WithStore(store),
			WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
				dispatches.Add(1)
				return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
			})}),
			WithIDGenerator(&sequenceIDs{}),
			WithRunPlanProvider(emptyTestRunPlanProvider()),
			WithEventSink(sink),
			WithClock(func() time.Time { return time.Date(2026, 8, 28, 1, 0, 0, 0, time.UTC) }),
		)
	}
	sinks := []*blockingSink{{}, {}}
	orchestrators := []*StreamingOrchestrator{newOrchestrator(sinks[0]), newOrchestrator(sinks[1])}
	type startResult struct {
		handle Handle
		err    error
	}
	results := make(chan startResult, len(orchestrators))
	ready := make(chan struct{})
	var wait sync.WaitGroup
	for _, orchestrator := range orchestrators {
		wait.Add(1)
		go func(orchestrator *StreamingOrchestrator) {
			defer wait.Done()
			<-ready
			handle, err := orchestrator.Start(context.Background(), Request{SessionID: "same-session", ParentID: "user", Input: []*einoschema.Message{einoschema.UserMessage("hello")}, Config: orchestratorConfig()})
			results <- startResult{handle: handle, err: err}
		}(orchestrator)
	}
	close(ready)
	wait.Wait()
	close(results)
	var successes, conflicts int
	for result := range results {
		if result.err == nil {
			successes++
			completed := <-result.handle.Done()
			if completed.Error != nil {
				t.Fatalf("successful run = %+v", completed)
			}
			continue
		}
		if errors.Is(result.err, session.ErrConflict) {
			conflicts++
			continue
		}
		t.Fatalf("Start error = %v, want ErrConflict", result.err)
	}
	if successes != 1 || conflicts != 1 || dispatches.Load() != 1 || sinks[0].count(EventRunStarted)+sinks[1].count(EventRunStarted) != 1 {
		t.Fatalf("successes=%d conflicts=%d dispatches=%d starts=%d", successes, conflicts, dispatches.Load(), sinks[0].count(EventRunStarted)+sinks[1].count(EventRunStarted))
	}
}

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
	store.normalizeEvent = func(event session.EventRecord) session.EventRecord {
		event.Correlation = "stored:" + event.Kind
		return event
	}
	runtimeSink := &capturingSink{}
	registry := newTestExtensionRegistry(nil)
	component := extension.Component{InstanceID: "admission-events", Artifact: extension.Artifact{Name: "admission-events", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var published []string
	var order []string
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.On(registrar, EventPublishedPoint, extension.Registration{ID: "published", Scope: extension.GlobalScope()}, func(_ context.Context, event session.EventRecord) error {
			published = append(published, event.Kind)
			if event.Kind == EventRunStarted {
				order = append(order, "event-published")
			}
			return nil
		}); err != nil {
			return err
		}
		return extension.On(registrar, RunAdmittedPoint, extension.Registration{ID: "admitted", Scope: extension.GlobalScope()}, func(context.Context, RunAdmittedNotice) error {
			order = append(order, "run-admitted")
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
	if !reflect.DeepEqual(order, []string{"event-published", "run-admitted"}) {
		t.Fatalf("admission notification order = %v", order)
	}
	for _, event := range runtimeSink.events {
		if event.Kind == EventRunStarted || event.Kind == EventRunFinished {
			if event.Correlation != "stored:"+string(event.Kind) {
				t.Fatalf("published %s correlation = %q, want store normalization", event.Kind, event.Correlation)
			}
		}
	}
}

func TestAdmissionSinkPanicDoesNotPreventRunOrExtensionNotification(t *testing.T) {
	store := newAdmissionStore()
	registry := newTestExtensionRegistry(nil)
	var published []string
	mount, err := registry.Mount(context.Background(), testExtensionComponent("panicking-admission-sink"), extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, EventPublishedPoint, extension.Registration{ID: "published", Scope: extension.GlobalScope()}, func(_ context.Context, event session.EventRecord) error {
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
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	}), WithEventSink(panickingEventSink{}), WithRunPlanProvider(staticRunPlanProvider{plan: newTestDispatchPlan(dispatch)}))
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	if len(published) < 2 || published[0] != EventRunStarted {
		t.Fatalf("published events = %v", published)
	}
}

type panickingEventSink struct{}

func (panickingEventSink) Emit(context.Context, session.EventRecord) { panic("sink failed") }

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
