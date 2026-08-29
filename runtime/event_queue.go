package runtime

import (
	"context"

	"github.com/mattsp1290/eino-agent/session"
)

type eventQueue struct {
	ctx    context.Context
	events chan session.EventRecord
	sink   EventSink
	done   chan struct{}
}

func newEventQueue(ctx context.Context, size int, sink EventSink) *eventQueue {
	if size <= 0 {
		size = 1
	}
	q := &eventQueue{ctx: ctx, events: make(chan session.EventRecord, size), sink: sink, done: make(chan struct{})}
	go func() {
		defer close(q.done)
		for event := range q.events {
			if q.sink != nil {
				emitBestEffort(q.sink, ctx, event)
			}
		}
	}()
	return q
}

func (q *eventQueue) emit(event session.EventRecord) error {
	select {
	case <-q.ctx.Done():
		return q.ctx.Err()
	case q.events <- cloneEvent(event):
		return nil
	}
}

func (q *eventQueue) close() {
	close(q.events)
	<-q.done
}
