package agui

import (
	"context"
	"errors"
	"fmt"

	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"
	aguiemitter "github.com/mattsp1290/eino-agui/emitter"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

// ErrTailOverflow reports that live-only tail data was dropped.
var ErrTailOverflow = errors.New("agui live tail overflow")

// EventTail provides live runtime events for reconnecting AG-UI transports.
type EventTail interface {
	Subscribe(ctx context.Context, sessionID session.ID) (<-chan session.EventRecord, error)
}

// Replay emits durable events after cursor through bridge. Live-only deltas are
// intentionally skipped because token deltas are transport events, not durable
// conversation facts. session.MessageCommittedEventKind events are also
// skipped here (though they are durable, not live-only): the content they
// notify about was already emitted by this same call's message snapshot
// (see emitMessageSnapshot), so forwarding them to bridge.Emit would
// re-project and re-emit a message this replay already delivered.
//
// contentLimits must match the session.ContentLimits the orchestrator that
// produced this session's durable content was configured with (see
// runtime.WithContentLimits); the zero value falls back to
// session.DefaultContentLimits(). A mismatch here does not corrupt data, but
// content legitimately admitted under raised limits fails to decode for the
// message snapshot this replay emits first.
//
// includeReasoning gates whether durable reasoning content blocks are
// included in the message snapshot (see agui.GateProviderReasoningStorage
// and Bridge.includeReasoning's doc comment): pass true only once the host
// has confirmed that gate is satisfied for this session.
func Replay(ctx context.Context, bridge *Bridge, store session.Store, sessionID session.ID, cursor session.EventCursor, contentLimits session.ContentLimits, includeReasoning bool) (session.EventCursor, error) {
	next, _, err := replay(ctx, bridge, store, sessionID, cursor, contentLimits, includeReasoning)
	return next, err
}

func replay(ctx context.Context, bridge *Bridge, store session.Store, sessionID session.ID, cursor session.EventCursor, contentLimits session.ContentLimits, includeReasoning bool) (session.EventCursor, map[session.EventID]bool, error) {
	if store == nil {
		return cursor, nil, session.ErrNotFound
	}
	if err := emitMessageSnapshot(ctx, bridge, store, sessionID, contentLimits, includeReasoning); err != nil {
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
			if record.LiveOnly || record.Kind == session.MessageCommittedEventKind {
				next = session.EventCursor{AfterEventID: record.ID, Limit: cursor.Limit}
				continue
			}
			bridge.Emit(ctx, record)
			if err := bridge.Err(); err != nil {
				return next, seen, err
			}
			if err := bridge.EncErr(); err != nil {
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
// the contentLimits/includeReasoning contract.
func Reconnect(ctx context.Context, bridge *Bridge, store session.Store, tail EventTail, sessionID session.ID, cursor session.EventCursor, contentLimits session.ContentLimits, includeReasoning bool) (session.EventCursor, error) {
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
	next, seen, err := replay(ctx, bridge, store, sessionID, cursor, contentLimits, includeReasoning)
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
			if err := bridge.EncErr(); err != nil {
				return next, err
			}
			if event.ID != "" {
				seen[event.ID] = true
				next = session.EventCursor{AfterEventID: event.ID, Limit: cursor.Limit}
			}
		}
	}
}

// emitMessageSnapshot projects every durable message in sessionID through
// the agentic pipeline (convert.ToAgenticProjection) and emits each one via
// the observer emitter's committed-projection path with
// DeliveryModeReplay -- native events for every representable content kind
// plus the eino.agentic.v1 custom supplement for every block, so rich kinds
// the classic *schema.Message projection cannot represent (tool_search_result,
// mcp_*, assistant media, ...) no longer make replay fail outright the way
// history.Load's ErrClassicUnsupported did.
//
// Before those per-message projections, it emits one MESSAGES_SNAPSHOT
// built from every projection's NativeMessage (populated by
// convert.ToAgenticProjection for user-role messages precisely so a host
// can reconstruct native-client-visible history from it). Without this, a
// client that does not parse the eino.agentic.v1 custom envelope sees no
// user-role history at all: CommittedNativeEvents legitimately returns no
// native events for user_input_* kinds (there is no AG-UI streaming event
// shape for "the user already sent this"), so user turns would otherwise be
// invisible and the transcript would read as an assistant monologue to any
// native-only AG-UI client.
func emitMessageSnapshot(ctx context.Context, bridge *Bridge, store session.Store, sessionID session.ID, contentLimits session.ContentLimits, includeReasoning bool) error {
	if bridge == nil {
		return nil
	}
	projections, err := loadCommittedProjections(ctx, store, sessionID, contentLimits, includeReasoning)
	if err != nil {
		return err
	}
	natives := make([]aguitypes.Message, 0, len(projections))
	for _, p := range projections {
		if p.Projection.NativeMessage != nil {
			natives = append(natives, *p.Projection.NativeMessage)
		}
	}
	bridge.nativeMessagesSnapshot(natives)
	for _, p := range projections {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !bridge.EmitCommittedProjection(p.Projection, p.Receipt, aguiemitter.DeliveryModeReplay) {
			if encErr := bridge.EncErr(); encErr != nil {
				return encErr
			}
			if transportErr := bridge.Err(); transportErr != nil {
				return transportErr
			}
			return fmt.Errorf("agui: replay projection emission failed for message %s", p.MessageID)
		}
	}
	return nil
}
