package agui

import (
	"context"
	"errors"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

// ErrTailOverflow reports that live-only tail data was dropped.
var ErrTailOverflow = errors.New("agui live tail overflow")

// EventTail provides live runtime events for reconnecting AG-UI transports.
type EventTail interface {
	Subscribe(ctx context.Context, sessionID session.ID) (<-chan session.EventRecord, error)
}

// Replay emits durable events after cursor through bridge. Live-only deltas are
// intentionally skipped because token deltas are transport events, not durable
// conversation facts.
//
// contentLimits must match the session.ContentLimits the orchestrator that
// produced this session's durable content was configured with (see
// runtime.WithContentLimits); the zero value falls back to
// session.DefaultContentLimits(). A mismatch here does not corrupt data, but
// content legitimately admitted under raised limits fails to decode for the
// message snapshot this replay emits first.
func Replay(ctx context.Context, bridge *Bridge, store session.Store, sessionID session.ID, cursor session.EventCursor, contentLimits session.ContentLimits) (session.EventCursor, error) {
	next, _, err := replay(ctx, bridge, store, sessionID, cursor, contentLimits)
	return next, err
}

func replay(ctx context.Context, bridge *Bridge, store session.Store, sessionID session.ID, cursor session.EventCursor, contentLimits session.ContentLimits) (session.EventCursor, map[session.EventID]bool, error) {
	if store == nil {
		return cursor, nil, session.ErrNotFound
	}
	if err := emitMessageSnapshot(ctx, bridge, store, sessionID, contentLimits); err != nil {
		return cursor, nil, err
	}
	next := cursor
	seen := map[session.EventID]bool{}
	for {
		batch, err := store.ListEvents(ctx, sessionID, next)
		if err != nil {
			return next, seen, err
		}
		for _, record := range batch.Events {
			if err := ctx.Err(); err != nil {
				return next, seen, err
			}
			seen[record.ID] = true
			if record.LiveOnly {
				next = session.EventCursor{AfterEventID: record.ID, Limit: cursor.Limit}
				continue
			}
			bridge.Emit(ctx, record)
			if err := bridge.Err(); err != nil {
				return next, seen, err
			}
			next = session.EventCursor{AfterEventID: record.ID, Limit: cursor.Limit}
		}
		if batch.Next.AfterEventID == "" {
			return next, seen, nil
		}
		next = batch.Next
	}
}

// Reconnect subscribes to live tailing, replays durable events, then forwards
// live events until ctx is canceled or the tail disconnects. See Replay for
// the contentLimits contract.
func Reconnect(ctx context.Context, bridge *Bridge, store session.Store, tail EventTail, sessionID session.ID, cursor session.EventCursor, contentLimits session.ContentLimits) (session.EventCursor, error) {
	var live <-chan session.EventRecord
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	if tail != nil {
		var err error
		live, err = tail.Subscribe(subCtx, sessionID)
		if err != nil {
			return cursor, err
		}
	}
	next, seen, err := replay(ctx, bridge, store, sessionID, cursor, contentLimits)
	if err != nil {
		return next, err
	}
	if live == nil {
		<-ctx.Done()
		return next, ctx.Err()
	}
	for {
		select {
		case <-ctx.Done():
			return next, ctx.Err()
		case event, ok := <-live:
			if !ok {
				return next, nil
			}
			if event.SessionID != "" && event.SessionID != sessionID {
				continue
			}
			if event.Kind == runtime.EventTailOverflow {
				return next, ErrTailOverflow
			}
			if event.ID != "" && seen[event.ID] {
				continue
			}
			bridge.Emit(ctx, event)
			if err := bridge.Err(); err != nil {
				return next, err
			}
			if event.ID != "" {
				seen[event.ID] = true
				next = session.EventCursor{AfterEventID: event.ID, Limit: cursor.Limit}
			}
		}
	}
}

func emitMessageSnapshot(ctx context.Context, bridge *Bridge, store session.Store, sessionID session.ID, contentLimits session.ContentLimits) error {
	if bridge == nil {
		return nil
	}
	messages, err := history.Load(ctx, store, sessionID, history.Options{ContentLimits: contentLimits})
	if err != nil {
		return err
	}
	if len(messages) == 0 {
		return nil
	}
	bridge.MessagesSnapshot(messages)
	return nil
}
