package runtime

import (
	"context"
	"sync"

	"github.com/mattsp1290/eino-agent/session"
)

type queuedEvent struct {
	ctx   context.Context
	event session.EventRecord
}

// eventQueue is one run-owned, bounded, best-effort infrastructure
// dispatcher. Closing admission never waits for native sink code.
type eventQueue struct {
	mu     sync.Mutex
	events chan queuedEvent
	sink   EventSink
	done   chan struct{}
	closed bool
}

func newEventQueue(size int, sink EventSink) *eventQueue {
	if size <= 0 {
		size = 1
	}
	q := &eventQueue{events: make(chan queuedEvent, size), sink: sink, done: make(chan struct{})}
	go func() {
		defer close(q.done)
		for task := range q.events {
			emitBestEffort(q.sink, task.ctx, task.event)
		}
	}()
	return q
}

func (q *eventQueue) emit(ctx context.Context, event session.EventRecord) bool {
	if q == nil || q.sink == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	select {
	case q.events <- queuedEvent{ctx: context.WithoutCancel(ctx), event: cloneEvent(event)}:
		return true
	default:
		return false
	}
}

func (q *eventQueue) close() {
	if q == nil {
		return
	}
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		close(q.events)
	}
	q.mu.Unlock()
}

func (q *eventQueue) flush(ctx context.Context) error {
	if q == nil {
		return nil
	}
	select {
	case <-q.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
