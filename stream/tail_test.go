package stream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func TestTailContinuesLiveEventsForSession(t *testing.T) {
	t.Parallel()

	tail := NewTail(2)
	defer tail.Close()
	events, err := tail.Subscribe(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	tail.Emit(context.Background(), session.EventRecord{Kind: runtime.EventRunStarted, SessionID: "other"})
	want := session.EventRecord{Kind: runtime.EventMessageDelta, SessionID: "session-1", MessageID: "msg-1"}
	tail.Emit(context.Background(), want)
	select {
	case got := <-events:
		if got.Kind != want.Kind || got.SessionID != want.SessionID || got.MessageID != want.MessageID {
			t.Fatalf("event = %+v, want %+v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestTailDisconnectsSlowSubscriberWhenQueueBounded(t *testing.T) {
	t.Parallel()

	tail := NewTail(1)
	defer tail.Close()
	events, err := tail.Subscribe(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	tail.Emit(context.Background(), session.EventRecord{Kind: runtime.EventMessageDelta, SessionID: "session-1"})
	tail.Emit(context.Background(), session.EventRecord{Kind: runtime.EventRunFinished, SessionID: "session-1"})
	if event, ok := <-events; !ok || event.Kind != runtime.EventTailOverflow || !event.LiveOnly {
		t.Fatalf("overflow event = %+v ok=%t, want tail overflow", event, ok)
	}
	if _, ok := <-events; ok {
		t.Fatal("slow subscriber remained connected after queue overflow")
	}
}

func TestTailRejectsCanceledSubscription(t *testing.T) {
	t.Parallel()

	tail := NewTail(2)
	defer tail.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tail.Subscribe(ctx, "session-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe err = %v, want context canceled", err)
	}
}

func TestTailClonesPayloadForEverySubscriber(t *testing.T) {
	tail := NewTail(2)
	defer tail.Close()
	first, err := tail.Subscribe(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := tail.Subscribe(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	tail.Emit(context.Background(), session.EventRecord{Kind: runtime.EventMessageDelta, SessionID: "session-1", Payload: []byte(`{"value":"original"}`)})
	firstEvent := <-first
	secondEvent := <-second
	copy(firstEvent.Payload, []byte(`{"value":"mutated!"}`))
	if string(secondEvent.Payload) != `{"value":"original"}` {
		t.Fatalf("second subscriber payload = %s", secondEvent.Payload)
	}
}

func TestTailSubscriptionClosesOnContextCancellation(t *testing.T) {
	t.Parallel()

	tail := NewTail(2)
	defer tail.Close()
	ctx, cancel := context.WithCancel(context.Background())
	events, err := tail.Subscribe(ctx, "session-1")
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	cancel()
	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("subscription remained open after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscription close")
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("ctx err = %v", ctx.Err())
	}
}

func TestTailImplementsAGUIReplayInterface(t *testing.T) {
	t.Parallel()

	type replayTail interface {
		Subscribe(context.Context, session.ID) (<-chan session.EventRecord, error)
	}
	var _ replayTail = (*Tail)(nil)
}
