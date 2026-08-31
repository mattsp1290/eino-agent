package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func TestRunEventSinkOnlyFansOut(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newAdmissionStore()
	run, err := store.AdmitRun(ctx, session.Run{
		ID: "run-1", SessionID: "session-1", OwnerID: "owner-1", ClaimToken: "claim-1",
		Status: session.RunRunning, CreatedAt: time.Now().UTC(),
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	infrastructure := &capturingSink{}
	execution := newRunExecution(mustConfiguredOrchestrator(WithStore(store), WithEventSink(infrastructure)), mustTestRunPlan(RunPlanSpec{}), run)
	defer execution.release()
	sink := execution.eventSink()

	sink.Emit(ctx, session.EventRecord{Kind: EventMessageDelta, SessionID: run.SessionID, RunID: run.ID, LiveOnly: true})
	if len(store.events) != 0 {
		t.Fatalf("fanout persisted %d events, want none", len(store.events))
	}
	events := infrastructure.waitFor(t, 1)
	if events[0].Kind != EventMessageDelta {
		t.Fatalf("infrastructure events = %#v", events)
	}
}

func TestNewRunExecutionRejectsNilPlan(t *testing.T) {
	store := newAdmissionStore()
	run := session.Run{ID: "run-nil-plan", SessionID: "session-nil-plan", ClaimToken: "claim", Status: session.RunRunning}
	store.runs[run.ID] = run
	defer func() {
		if recover() == nil {
			t.Fatal("newRunExecution accepted nil plan")
		}
	}()
	newRunExecution(mustConfiguredOrchestrator(WithStore(store)), nil, run)
}

func TestFailedToolTransitionPublishesNothing(t *testing.T) {
	transitionErr := errors.New("transition failed")
	store := newAdmissionStore()
	run := session.Run{ID: "run-failed-transition", SessionID: "session-failed-transition", ClaimToken: "claim", Status: session.RunRunning}
	store.runs[run.ID] = run
	store.toolTransitionErr = transitionErr
	registry := newTestExtensionRegistry(nil)
	published := 0
	mount, err := registry.Mount(context.Background(), testExtensionComponent("failed-transition"), extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, EventPublishedPoint, extension.Registration{ID: "published", Scope: extension.GlobalScope()}, func(context.Context, session.EventRecord) error {
			published++
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
	sink := &capturingSink{}
	host := mustConfiguredOrchestrator(WithStore(store), WithEventSink(sink))
	execution := newRunExecution(host, newTestDispatchPlan(dispatch), run)
	defer execution.release()
	_, err = execution.persistToolCreation(context.Background(), session.CreateToolCallRequest{Call: session.ToolCall{ID: "call", RunID: run.ID}})
	if !errors.Is(err, transitionErr) {
		t.Fatalf("persistToolCreation error = %v", err)
	}
	if events := sink.snapshot(); len(events) != 0 || published != 0 {
		t.Fatalf("failed transition published sink=%v notices=%d", events, published)
	}
}

func TestBlockedInfrastructureSinkCannotBlockAdmissionOrHandleDone(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	}), WithEventSink(EventSinkFunc(func(context.Context, session.EventRecord) {
		once.Do(func() { close(started) })
		<-release
	})), WithQueueSize(2))
	handle, err := orch.Start(context.Background(), Request{
		SessionID: "blocked-sink-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig(),
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	select {
	case result := <-handle.Done():
		if result.Status != session.RunCompleted {
			t.Fatalf("result = %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("Handle.Done blocked on infrastructure sink")
	}
	close(release)
}
