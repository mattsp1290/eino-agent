package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func TestRunEventSinkPersistsOnlyIntermediateDurableEventsThroughFence(t *testing.T) {
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
	execution := newRunExecution(&StreamingOrchestrator{Store: store, IDs: &sequenceIDs{}}, nil)
	execution.bindRun(run)
	sink := execution.eventSink(infrastructure)

	if err := sink.Emit(ctx, Event{Kind: EventToolCallUpdated, SessionID: run.SessionID, RunID: run.ID}); err != nil {
		t.Fatalf("emit durable event: %v", err)
	}
	if len(store.events) != 1 {
		t.Fatalf("durable events = %d, want 1", len(store.events))
	}
	if len(infrastructure.events) != 1 || infrastructure.events[0].EventID == "" {
		t.Fatalf("infrastructure events = %#v, want generated durable event id", infrastructure.events)
	}

	if err := sink.Emit(ctx, Event{Kind: EventMessageDelta, SessionID: run.SessionID, RunID: run.ID, LiveOnly: true}); err != nil {
		t.Fatalf("emit live event: %v", err)
	}
	if err := sink.Emit(ctx, Event{Kind: EventRunFinished, SessionID: run.SessionID, RunID: run.ID}); err != nil {
		t.Fatalf("emit final notification: %v", err)
	}
	if len(store.events) != 1 {
		t.Fatalf("durable events after transport-only notifications = %d, want 1", len(store.events))
	}
}
