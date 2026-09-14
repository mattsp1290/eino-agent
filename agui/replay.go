package agui

import (
	"context"
	"errors"
	"fmt"

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
// conversation facts. session.MessageCommittedEventKind events are durable
// (not live-only) and so are always forwarded to bridge.Emit like any other
// durable event; Bridge itself is what skips a message_committed naming a
// message this same call's snapshot already delivered (see
// Bridge.projectedMessages and emitLiveMessageCommitted's doc comments) --
// this is what lets a message_committed for a message that commits DURING
// this replay window (never covered by the snapshot) reach the client
// instead of being dropped alongside the ones the snapshot already covers.
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
	// ResumedV1 is deliberately stateful: the protocol emitter rejects it
	// unless the paired pause was committed on this connection. A cursor may
	// begin after that pause, so replay the earlier durable pause facts first.
	// This is connection hydration, not an inferred checkpoint projection:
	// Bridge.Emit still validates the immutable payload and skips historical
	// empty pause records.
	if cursor.AfterEventID != "" {
		if err := hydratePausesBeforeCursor(ctx, bridge, store, sessionID, cursor.AfterEventID); err != nil {
			return cursor, nil, err
		}
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
			if record.LiveOnly {
				// This sweep never emits a LiveOnly record (see below), so
				// marking its ID seen here would buy no dedup -- it would
				// only risk suppressing a live-tail copy of the SAME ID the
				// client could otherwise still receive and render. Nothing
				// in this runtime durably persists a LiveOnly record today
				// (see runtime/adk_model.go, stream/tail.go), so this is
				// defensive rather than a fix for an observed failure.
				next = session.EventCursor{AfterEventID: record.ID, Limit: cursor.Limit}
				continue
			}
			seen[record.ID] = true
			bridge.Emit(ctx, record)
			if err := bridge.Err(); err != nil {
				return next, seen, err
			}
			if err := bridge.EncErr(); err != nil {
				return next, seen, err
			}
			if err := bridge.LiveErr(); err != nil {
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

func hydratePausesBeforeCursor(ctx context.Context, bridge *Bridge, store session.Store, sessionID session.ID, through session.EventID) error {
	cursor := session.EventCursor{Limit: 100}
	for {
		batch, err := store.ListEvents(ctx, sessionID, cursor)
		if err != nil {
			return err
		}
		for _, record := range batch.Events {
			if record.Kind == session.RunPausedEventKind && !record.LiveOnly {
				bridge.Emit(ctx, record)
				if err := bridge.LiveErr(); err != nil {
					return err
				}
				if err := bridge.EncErr(); err != nil {
					return err
				}
			}
			if record.ID == through {
				return nil
			}
		}
		if batch.Next.AfterEventID == "" {
			return nil
		}
		cursor = batch.Next
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
			if err := bridge.LiveErr(); err != nil {
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
// This does NOT emit a MESSAGES_SNAPSHOT: convert.ToAgenticProjection only
// populates NativeMessage for user-role messages (eino-agui's
// nativeUserMessage), so a snapshot built from it would carry user turns
// only, ahead of every assistant projection this loop emits after it --
// reordering a U1,A1,U2,A2 transcript into U1,U2,A1,A2 for any client that
// treats MESSAGES_SNAPSHOT as authoritative, and clobbering client state on
// a cursored reconnect since it ignores the cursor entirely (W7 fix-pass
// review finding P0-C). eino-agui's own nativeUserMessage doc comment says
// as much: hosts assemble snapshots from their complete committed
// transcript, so this bridge must not overwrite unrelated history while
// projecting one durable record. A native-only AG-UI client -- one that
// never parses the eino.agentic.v1 custom envelope -- therefore has no
// representation of user-role history at all on this path; see
// docs/architecture/agui-events.md and docs/consumer-guide.md.
func emitMessageSnapshot(ctx context.Context, bridge *Bridge, store session.Store, sessionID session.ID, contentLimits session.ContentLimits, includeReasoning bool) error {
	if bridge == nil {
		return nil
	}
	projections, err := loadCommittedProjections(ctx, store, sessionID, contentLimits, includeReasoning)
	if err != nil {
		return err
	}
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
		bridge.markMessageProjected(p.MessageID)
	}
	return nil
}
