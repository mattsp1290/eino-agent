package runtime

import (
	"context"
	"testing"

	"github.com/mattsp1290/eino-agent/session"
)

func TestEventQueueOwnsPayloadAfterEnqueue(t *testing.T) {
	var got string
	queue := newEventQueue(context.Background(), 1, EventSinkFunc(func(_ context.Context, event session.EventRecord) {
		got = string(event.Payload)
	}))
	event := session.EventRecord{Kind: EventMessageDelta, Payload: []byte(`{"value":"original"}`)}
	if err := queue.emit(event); err != nil {
		t.Fatal(err)
	}
	copy(event.Payload, []byte(`{"value":"mutated!"}`))
	queue.close()
	if got != `{"value":"original"}` {
		t.Fatalf("queued payload = %s", got)
	}
}

func TestEventQueueContinuesAfterSinkPanic(t *testing.T) {
	var calls int
	var got string
	queue := newEventQueue(context.Background(), 1, EventSinkFunc(func(_ context.Context, event session.EventRecord) {
		calls++
		if calls == 1 {
			panic("first event failed")
		}
		got = event.Kind
	}))
	if err := queue.emit(session.EventRecord{Kind: EventMessageDelta}); err != nil {
		t.Fatal(err)
	}
	if err := queue.emit(session.EventRecord{Kind: EventRunFinished}); err != nil {
		t.Fatal(err)
	}
	queue.close()
	if calls != 2 || got != EventRunFinished {
		t.Fatalf("calls = %d, final kind = %q", calls, got)
	}
}
