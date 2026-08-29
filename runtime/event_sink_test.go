package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/extension"
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
	execution := newRunExecution(mustConfiguredOrchestrator(WithStore(store)), nil)
	execution.bindRun(run)
	sink := execution.eventSink(infrastructure)

	if err := sink.Emit(ctx, Event{Kind: EventMessageDelta, SessionID: run.SessionID, RunID: run.ID, LiveOnly: true}); err != nil {
		t.Fatalf("emit live event: %v", err)
	}
	if len(store.events) != 0 {
		t.Fatalf("fanout persisted %d events, want none", len(store.events))
	}
	if len(infrastructure.events) != 1 || infrastructure.events[0].Kind != EventMessageDelta {
		t.Fatalf("infrastructure events = %#v", infrastructure.events)
	}
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
		return extension.On(registrar, EventPublishedPoint, extension.Registration{ID: "published", Scope: extension.GlobalScope()}, func(context.Context, Event) error {
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
	execution := newRunExecution(host, newTestDispatchPlan(dispatch))
	defer execution.release()
	execution.bindRun(run)
	_, err = execution.persistToolCreation(context.Background(), session.CreateToolCallRequest{Call: session.ToolCall{ID: "call", RunID: run.ID}})
	if !errors.Is(err, transitionErr) {
		t.Fatalf("persistToolCreation error = %v", err)
	}
	if len(sink.events) != 0 || published != 0 {
		t.Fatalf("failed transition published sink=%v notices=%d", sink.events, published)
	}
}
