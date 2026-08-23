package runtime

import "context"

type eventQueue struct {
	ctx    context.Context
	events chan Event
	sink   EventSink
	done   chan struct{}
}

func newEventQueue(ctx context.Context, size int, sink EventSink) *eventQueue {
	if size <= 0 {
		size = 1
	}
	q := &eventQueue{ctx: ctx, events: make(chan Event, size), sink: sink, done: make(chan struct{})}
	go func() {
		defer close(q.done)
		for event := range q.events {
			if q.sink != nil {
				_ = q.sink.Emit(ctx, event)
			}
		}
	}()
	return q
}

func (q *eventQueue) emit(event Event) error {
	select {
	case <-q.ctx.Done():
		return q.ctx.Err()
	case q.events <- event:
		return nil
	}
}

func (q *eventQueue) close() {
	close(q.events)
	<-q.done
}
