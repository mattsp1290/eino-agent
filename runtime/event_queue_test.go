package runtime

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/mattsp1290/eino-agent/session"
)

func TestEventQueueOwnsPayloadAfterEnqueue(t *testing.T) {
	var got string
	queue := newEventQueue(1, EventSinkFunc(func(_ context.Context, event session.EventRecord) {
		got = string(event.Payload)
	}))
	event := session.EventRecord{Kind: EventMessageDelta, Payload: []byte(`{"value":"original"}`)}
	if !queue.emit(context.Background(), event) {
		t.Fatal("event was not accepted")
	}
	copy(event.Payload, []byte(`{"value":"mutated!"}`))
	queue.close()
	if err := queue.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got != `{"value":"original"}` {
		t.Fatalf("queued payload = %s", got)
	}
}

func TestEventQueueContinuesAfterSinkPanic(t *testing.T) {
	var calls int
	var got string
	queue := newEventQueue(2, EventSinkFunc(func(_ context.Context, event session.EventRecord) {
		calls++
		if calls == 1 {
			panic("first event failed")
		}
		got = event.Kind
	}))
	if !queue.emit(context.Background(), session.EventRecord{Kind: EventMessageDelta}) {
		t.Fatal("first event was not accepted")
	}
	if !queue.emit(context.Background(), session.EventRecord{Kind: EventRunFinished}) {
		t.Fatal("second event was not accepted")
	}
	queue.close()
	if err := queue.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || got != EventRunFinished {
		t.Fatalf("calls = %d, final kind = %q", calls, got)
	}
}

func TestEventQueueDropsNewWorkWhenFullAndCloseDoesNotWait(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var kinds []string
	queue := newEventQueue(1, EventSinkFunc(func(_ context.Context, event session.EventRecord) {
		mu.Lock()
		kinds = append(kinds, event.Kind)
		mu.Unlock()
		if event.Kind == "first" {
			close(started)
			<-release
		}
	}))
	if !queue.emit(context.Background(), session.EventRecord{Kind: "first"}) {
		t.Fatal("first event was not accepted")
	}
	<-started
	if !queue.emit(context.Background(), session.EventRecord{Kind: "second"}) {
		t.Fatal("second event was not accepted")
	}
	if queue.emit(context.Background(), session.EventRecord{Kind: "dropped"}) {
		t.Fatal("full queue accepted a new event")
	}
	queue.close()
	close(release)
	if err := queue.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(kinds, []string{"first", "second"}) {
		t.Fatalf("delivered kinds = %v", kinds)
	}
}
