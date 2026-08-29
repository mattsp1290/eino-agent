package runtime

import (
	"context"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

// runEventSink fans out transport and observability copies. Durable records are
// committed by the state transition that owns them before reaching this sink.
type runEventSink struct {
	infrastructure *eventQueue
	plan           *extension.Plan
}

func (s runEventSink) Emit(ctx context.Context, event session.EventRecord) {
	s.infrastructure.emit(ctx, event)
	extension.Notify(s.plan, ctx, EventPublishedPoint, event)
}

// publishPersisted delivers an event that was already committed as part of a
// state transition. Transport and extension failures cannot roll back durable
// state and are therefore intentionally best-effort.
func (s runEventSink) publishPersisted(infrastructureCtx, notificationCtx context.Context, record session.EventRecord) {
	s.infrastructure.emit(infrastructureCtx, record)
	extension.Notify(s.plan, notificationCtx, EventPublishedPoint, record)
}

func emitBestEffort(sink EventSink, ctx context.Context, event session.EventRecord) {
	if sink == nil {
		return
	}
	defer func() { _ = recover() }()
	sink.Emit(ctx, cloneEvent(event))
}
