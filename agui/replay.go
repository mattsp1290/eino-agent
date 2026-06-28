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
	Subscribe(ctx context.Context, sessionID session.ID) (<-chan runtime.Event, error)
}

// Replay emits durable events after cursor through bridge. Live-only deltas are
// intentionally skipped because token deltas are transport events, not durable
// conversation facts.
func Replay(ctx context.Context, bridge *Bridge, store session.Store, sessionID session.ID, cursor session.EventCursor) (session.EventCursor, error) {
	next, _, err := replay(ctx, bridge, store, sessionID, cursor)
	return next, err
}

func replay(ctx context.Context, bridge *Bridge, store session.Store, sessionID session.ID, cursor session.EventCursor) (session.EventCursor, map[session.EventID]bool, error) {
	if store == nil {
		return cursor, nil, session.ErrNotFound
	}
	if err := emitMessageSnapshot(ctx, bridge, store, sessionID); err != nil {
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
			if err := bridge.Emit(ctx, runtimeEvent(record)); err != nil {
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
// live events until ctx is canceled or the tail disconnects.
func Reconnect(ctx context.Context, bridge *Bridge, store session.Store, tail EventTail, sessionID session.ID, cursor session.EventCursor) (session.EventCursor, error) {
	var live <-chan runtime.Event
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	if tail != nil {
		var err error
		live, err = tail.Subscribe(subCtx, sessionID)
		if err != nil {
			return cursor, err
		}
	}
	next, seen, err := replay(ctx, bridge, store, sessionID, cursor)
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
			if event.EventID != "" && seen[event.EventID] {
				continue
			}
			if err := bridge.Emit(ctx, event); err != nil {
				return next, err
			}
			if event.EventID != "" {
				seen[event.EventID] = true
				next = session.EventCursor{AfterEventID: event.EventID, Limit: cursor.Limit}
			}
		}
	}
}

func emitMessageSnapshot(ctx context.Context, bridge *Bridge, store session.Store, sessionID session.ID) error {
	if bridge == nil {
		return nil
	}
	messages, err := history.Load(ctx, store, sessionID, history.Options{})
	if err != nil {
		return err
	}
	if len(messages) == 0 {
		return nil
	}
	bridge.MessagesSnapshot(messages)
	return nil
}

func runtimeEvent(record session.EventRecord) runtime.Event {
	return runtime.Event{
		Kind:        runtime.EventKind(record.Kind),
		EventID:     record.ID,
		SessionID:   record.SessionID,
		RunID:       record.RunID,
		MessageID:   record.MessageID,
		PartID:      record.PartID,
		ToolCallID:  record.ToolCallID,
		EpochID:     record.EpochID,
		ProviderID:  record.ProviderID,
		ModelID:     record.ModelID,
		ParentID:    record.ParentID,
		Correlation: record.Correlation,
		Usage: runtime.Usage{
			InputTokens:      record.Usage.InputTokens,
			OutputTokens:     record.Usage.OutputTokens,
			ReasoningTokens:  record.Usage.ReasoningTokens,
			CacheReadTokens:  record.Usage.CacheReadTokens,
			CacheWriteTokens: record.Usage.CacheWriteTokens,
			Cost:             record.Usage.Cost,
		},
		Error: runtime.EventError{
			Code:      record.Error.Code,
			Message:   record.Error.Message,
			Retryable: record.Error.Retryable,
		},
		Redaction: runtime.RedactionClass(record.Redaction),
		Payload:   cloneRaw(record.Payload),
		LiveOnly:  record.LiveOnly,
		Time:      record.CreatedAt,
	}
}
