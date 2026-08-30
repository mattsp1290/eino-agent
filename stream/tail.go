package stream

import (
	"context"
	"sync"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

// Tail fans out live runtime events to reconnecting transports.
type Tail struct {
	size int

	mu     sync.Mutex
	next   uint64
	subs   map[uint64]*tailSub
	closed bool
}

type tailSub struct {
	sessionID session.ID
	events    chan session.EventRecord
}

// NewTail creates a bounded in-process event tail.
func NewTail(size int) *Tail {
	if size <= 0 {
		size = 64
	}
	return &Tail{size: size, subs: map[uint64]*tailSub{}}
}

// Subscribe returns a bounded stream of live events for one session. Canceling
// ctx disconnects the subscription.
func (t *Tail) Subscribe(ctx context.Context, sessionID session.ID) (<-chan session.EventRecord, error) {
	if t == nil {
		t = NewTail(0)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		ch := make(chan session.EventRecord)
		close(ch)
		return ch, nil
	}
	t.next++
	id := t.next
	sub := &tailSub{sessionID: sessionID, events: make(chan session.EventRecord, t.size)}
	t.subs[id] = sub
	t.mu.Unlock()

	go func() {
		<-ctx.Done()
		t.remove(id)
	}()
	return sub.events, nil
}

// Emit implements runtime.EventSink. Slow subscribers are disconnected instead
// of applying unbounded memory pressure or blocking the producer.
func (t *Tail) Emit(_ context.Context, event session.EventRecord) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	var drop []uint64
	for id, sub := range t.subs {
		if sub.sessionID != "" && event.SessionID != sub.sessionID {
			continue
		}
		select {
		case sub.events <- cloneEvent(event):
		default:
			select {
			case <-sub.events:
			default:
			}
			sub.events <- session.EventRecord{Kind: runtime.EventTailOverflow, SessionID: sub.sessionID, LiveOnly: true}
			drop = append(drop, id)
		}
	}
	for _, id := range drop {
		sub := t.subs[id]
		delete(t.subs, id)
		close(sub.events)
	}
	t.mu.Unlock()
}

func cloneEvent(event session.EventRecord) session.EventRecord {
	event.Payload = append([]byte(nil), event.Payload...)
	return event
}

// Close disconnects all subscribers.
func (t *Tail) Close() {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	for id, sub := range t.subs {
		delete(t.subs, id)
		close(sub.events)
	}
	t.mu.Unlock()
}

func (t *Tail) remove(id uint64) {
	t.mu.Lock()
	sub, ok := t.subs[id]
	if ok {
		delete(t.subs, id)
		close(sub.events)
	}
	t.mu.Unlock()
}

var _ runtime.EventSink = (*Tail)(nil)
