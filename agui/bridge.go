package agui

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	aguievents "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/events"
	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"
	einoschema "github.com/cloudwego/eino/schema"
	"github.com/mattsp1290/eino-agui/convert"
	aguiemitter "github.com/mattsp1290/eino-agui/emitter"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

// Bridge adapts runtime events and durable snapshots to typed AG-UI SSE
// events. One Bridge serves exactly one connection: replay() (its own
// ListEvents sweep) and then Reconnect's live tail loop call Emit
// sequentially from a single goroutine at a time, never concurrently, so
// the unexported fields below need no locking. Bridge is not safe for
// concurrent Emit calls from multiple goroutines.
type Bridge struct {
	emit *aguiemitter.Emitter
	// writer, sseWriter, threadID, and runID are the exact values used to
	// build emit above, retained here (in addition to being bound into it)
	// so Terminate can build a short-lived fallback emitter sharing them on
	// an uncancelable context when emit's own context is already Done --
	// see Terminate's doc comment.
	writer    *bufio.Writer
	sseWriter *sse.SSEWriter
	threadID  string
	runID     string
	textOpen  map[session.MessageID]bool
	reasoning map[session.MessageID]string
	// store backs the agentic committed-projection path: on
	// session.MessageCommittedEventKind, Emit reloads and reprojects the
	// named durable message through convert.ToAgenticProjection and emits
	// it via the observer emitter (see emitLiveMessageCommitted). A nil
	// store disables that path (Emit silently skips the event), which
	// existing classic-only callers/tests may still do.
	store         session.Store
	contentLimits session.ContentLimits
	// includeReasoning gates whether the agentic committed-projection path
	// (both the live path here and agui.Replay's) includes durable
	// reasoning content blocks. It must only be true once the host has
	// confirmed GateProviderReasoningStorage (agui/policy.go) is satisfied
	// for this session -- the default (false) matches the pre-W7 classic
	// history pipeline's default and keeps durable reasoning from
	// streaming to every reconnecting client unless a host explicitly
	// opts in.
	includeReasoning bool
	// liveErr is the first error a live committed-projection attempt
	// (emitLiveMessageCommitted) hit that the runtime.EventSink.Emit
	// signature has no way to return synchronously: a
	// loadCommittedProjections failure. event.MessageID not being found
	// among this session's current projections is deliberately NOT one of
	// these -- see emitLiveMessageCommitted's benign-miss doc comment and
	// BenignCommitMisses -- that case used to be documented here, but is
	// now the opposite: a benign, non-fatal, observable-elsewhere miss,
	// never latched into liveErr. liveErr also does NOT duplicate
	// EmitCommittedProjection's own failures -- those already surface
	// through EncErr()/Err(), since the underlying emitter records them
	// internally regardless of whether this bridge inspects its returned
	// bool.
	liveErr error
	// benignCommitMisses counts every session.MessageCommittedEventKind
	// notification this Bridge has observed naming a message not (yet)
	// visible to a live reload (see emitLiveMessageCommitted's benign-miss
	// doc comment) -- the only host-visible signal for that case, since it
	// is deliberately not latched into liveErr.
	benignCommitMisses int
	// projectedMessages records every message ID this Bridge has already
	// emitted through the agentic committed-projection path -- either
	// emitMessageSnapshot's replay projection or an earlier
	// emitLiveMessageCommitted live continuation -- for the life of this
	// connection. Each durable message is finalized and receipt-stamped
	// exactly once (loadCommittedProjections' revision-folded receipt
	// key), so a repeat session.MessageCommittedEventKind notification
	// naming a message already in this set is always the documented
	// at-least-once publishMessageCommitted choreography (see that
	// function's doc comment), never new content: re-projecting it would
	// mint a receipt that collides with the one already emitted and latch
	// a benign EncErr that kills the stream (see emitLiveMessageCommitted
	// and replay()'s doc comments). This replaces the old, broader
	// "skip every session.MessageCommittedEventKind event during replay"
	// rule, which also silently dropped a message that first committed
	// during the replay window (never in this set) instead of forwarding
	// it.
	projectedMessages map[session.MessageID]bool
	// nativeStreamed records every message ID for which this Bridge has
	// actually emitted at least one native AG-UI event (TEXT_MESSAGE_*,
	// REASONING_*, or TOOL_CALL_*) via emitMessageDelta/emitToolCallUpdated
	// on THIS connection. emitLiveMessageCommitted consults it (not a
	// connection-phase flag -- see the fourth W7 fix-pass review's P0-1
	// finding, reviews/w7-fixes3-2026-09-12/) to decide whether a
	// session.MessageCommittedEventKind notification for a message still
	// needs a native representation or can be a custom-only supplement:
	// the earlier "are we still in replay()'s durable sweep" phase test
	// assumed a message's live deltas are necessarily unseen during that
	// sweep and necessarily already seen once the sweep ends, but
	// Reconnect subscribes to the live tail BEFORE calling replay() (see
	// Reconnect's doc comment), so a delta for a message replay()'s own
	// sweep already found fully committed can still be sitting in that
	// buffered channel, to be drained by the live loop AFTER the sweep
	// already delivered that message's native content. A phase flag can't
	// tell that apart; per-message "did native content actually reach
	// this connection for THIS message" can. See also
	// emitMessageDelta/emitToolCallUpdated's own messageProjected guard,
	// which handles the mirror case (a stale delta arriving for a message
	// already delivered via the committed-projection path).
	nativeStreamed map[session.MessageID]bool
	// terminated is true once this Bridge has written a terminal frame --
	// either Emit's runtime.EventRunFinished case (a durable
	// RUN_FINISHED/RUN_ERROR) or a Terminate call -- for the life of this
	// connection. AG-UI treats RUN_FINISHED and RUN_ERROR as equally
	// terminal for a run: a client that has already seen one must never
	// see a second, and never one after a successful RUN_FINISHED (W7
	// fourth fix-pass review P0-3).
	terminated bool
}

// NewBridge binds an AG-UI emitter to the SDK's concrete SSE writer pair.
// store, contentLimits, and includeReasoning back the agentic
// committed-projection path (see Bridge.store's and
// Bridge.includeReasoning's doc comments); pass a nil store to disable it.
func NewBridge(ctx context.Context, store session.Store, contentLimits session.ContentLimits, includeReasoning bool, writer *bufio.Writer, sseWriter *sse.SSEWriter, threadID, runID string, cancel context.CancelFunc) *Bridge {
	return &Bridge{
		emit:             aguiemitter.NewEmitter(ctx, writer, sseWriter, threadID, runID, cancel),
		writer:           writer,
		sseWriter:        sseWriter,
		threadID:         threadID,
		runID:            runID,
		textOpen:         map[session.MessageID]bool{},
		reasoning:        map[session.MessageID]string{},
		store:            store,
		contentLimits:    contentLimits,
		includeReasoning: includeReasoning,
	}
}

// Err returns the first transport error from the underlying AG-UI emitter.
func (b *Bridge) Err() error {
	if b == nil || b.emit == nil {
		return nil
	}
	return b.emit.Err()
}

// LiveErr returns the first error the live committed-projection path
// (emitLiveMessageCommitted) hit that could not be reported through
// EncErr()/Err() -- see Bridge.liveErr's doc comment. Callers that care
// about a live session.MessageCommittedEventKind failure (as opposed to
// durable replay, which already returns its error synchronously from
// agui.Replay) should poll this after Emit.
func (b *Bridge) LiveErr() error {
	if b == nil {
		return nil
	}
	return b.liveErr
}

// messageProjected reports whether id has already been emitted through the
// agentic committed-projection path on this Bridge (see
// Bridge.projectedMessages's doc comment).
func (b *Bridge) messageProjected(id session.MessageID) bool {
	if b == nil {
		return false
	}
	return b.projectedMessages[id]
}

// markMessageProjected records id as delivered through the agentic
// committed-projection path (see Bridge.projectedMessages's doc comment).
// Callers must only call this after a projection attempt actually
// succeeded.
func (b *Bridge) markMessageProjected(id session.MessageID) {
	if b == nil {
		return
	}
	if b.projectedMessages == nil {
		b.projectedMessages = map[session.MessageID]bool{}
	}
	b.projectedMessages[id] = true
}

// nativeAlreadyStreamed reports whether this Bridge has already emitted at
// least one native AG-UI event for id on this connection (see
// Bridge.nativeStreamed's doc comment).
func (b *Bridge) nativeAlreadyStreamed(id session.MessageID) bool {
	if b == nil {
		return false
	}
	return b.nativeStreamed[id]
}

// markNativeStreamed records id as having had native AG-UI content emitted
// for it on this connection (see Bridge.nativeStreamed's doc comment).
func (b *Bridge) markNativeStreamed(id session.MessageID) {
	if b == nil || id == "" {
		return
	}
	if b.nativeStreamed == nil {
		b.nativeStreamed = map[session.MessageID]bool{}
	}
	b.nativeStreamed[id] = true
}

// BenignCommitMisses returns the number of session.MessageCommittedEventKind
// notifications this Bridge has observed naming a message not (yet) visible
// to a live reload -- see emitLiveMessageCommitted's benign-miss doc
// comment. This is deliberately not surfaced through LiveErr() (it is not a
// failure), but it is the only host-visible signal for that case: a real
// bug producing a persistently non-zero rate is otherwise invisible.
func (b *Bridge) BenignCommitMisses() int {
	if b == nil {
		return 0
	}
	return b.benignCommitMisses
}

// EncErr returns the first event validation/encoding error.
func (b *Bridge) EncErr() error {
	if b == nil || b.emit == nil {
		return nil
	}
	return b.emit.EncErr()
}

// Emit implements runtime.EventSink.
func (b *Bridge) Emit(ctx context.Context, event session.EventRecord) {
	if b == nil || b.emit == nil {
		return
	}
	switch event.Kind {
	case runtime.EventRunStarted:
		b.emit.RunStarted()
	case runtime.EventMessageDelta:
		b.emitMessageDelta(event)
	case runtime.EventToolCallUpdated:
		b.emitToolCallUpdated(event)
	case session.MessageCommittedEventKind:
		b.emitLiveMessageCommitted(ctx, event)
	case runtime.EventRunFinished:
		// terminated guards against writing a second terminal frame: a
		// durable RUN_FINISHED/RUN_ERROR reaching Emit twice (a redundant
		// replay/live delivery of the same settlement record) must not
		// duplicate it, and once this fires, a later out-of-band
		// Terminate call (transport/http.go) must not add a THIRD (W7
		// fourth fix-pass review P0-3).
		if b.terminated {
			return
		}
		b.terminated = true
		b.closeOpen()
		if event.Error.Message != "" {
			b.emit.RunError(event.Error.Message)
			payload := runFinishedPayload{}
			_ = json.Unmarshal(event.Payload, &payload)
			if payload.Interrupted || payload.Status == string(session.RunInterrupted) {
				b.emit.RunFinishedInterrupt(nil)
			}
		} else {
			b.emit.RunFinishedSuccess()
		}
	}
}

// Terminate emits a terminal RUN_ERROR frame reporting that this
// connection's Reconnect/Replay call ended in err, unless:
//
//   - a terminal frame has already reached the wire for this connection
//     (a durable RUN_FINISHED/RUN_ERROR via Emit's runtime.EventRunFinished
//     case, or an earlier Terminate call -- see Bridge.terminated's doc
//     comment: RUN_FINISHED and RUN_ERROR are both terminal for a run, and
//     a client must never see a second one, nor one after a successful
//     RUN_FINISHED), or
//   - err is a benign context.Canceled from a client disconnect or a
//     graceful server shutdown, neither of which is a failure the client
//     needs signalled (nothing is listening on a client disconnect, and a
//     graceful shutdown is not a truncation). context.DeadlineExceeded is
//     deliberately NOT treated as benign here: a host-imposed request
//     deadline leaves the client connected and genuinely needing the
//     truncation signal (W7 fourth fix-pass review P0-2).
//
// err's own text never reaches the wire -- only a fixed, policy-safe
// message does (docs/architecture/agui-events.md's Errors row: "Provider/
// internal details redacted by policy"). err can wrap identifiers (e.g.
// emitLiveMessageCommitted's own wrapped reload-failure errors carry
// session/message IDs) this bridge has no business putting on an
// AG-UI client's wire.
//
// This deliberately does not reuse the primary emitter (b.emit, bound to
// ctx at NewBridge time): by the time Reconnect/Replay has returned a
// non-nil error, ctx is very often already Done (Reconnect returns
// ctx.Err() on both client disconnect and server shutdown, and the SAME
// ctx.Err() propagates through a host's context.DeadlineExceeded too), and
// the AG-UI SDK's JSON encoder refuses to encode anything once ctx.Err() !=
// nil -- checked before encoding, so the failure surfaces as an encoding
// error, never a transport one, and nothing is written (W7 fourth fix-pass
// review P0-2). Instead this builds a short-lived emitter sharing this
// connection's writer/threadID/runID but bound to
// context.WithoutCancel(ctx), so the encoder sees a live (never-canceled,
// never-deadlined) context regardless of why ctx itself ended. For a
// non-context error while ctx is still live (e.g. ErrTailOverflow, a store
// failure) this changes nothing observable: the fallback emitter behaves
// identically to the primary one in that case.
func (b *Bridge) Terminate(ctx context.Context, err error) bool {
	if b == nil || b.emit == nil || err == nil || b.terminated {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	b.terminated = true
	fallback := aguiemitter.NewEmitter(context.WithoutCancel(ctx), b.writer, b.sseWriter, b.threadID, b.runID, nil)
	fallback.RunError(TerminalErrorMessage)
	return fallback.Err() == nil && fallback.EncErr() == nil
}

// TerminalErrorMessage is the fixed, policy-safe text Terminate puts on the
// wire for every non-benign termination it reports -- see Terminate's doc
// comment for why the triggering error's own text never reaches the wire.
// Exported so a host (or a test) that needs to recognize this exact
// terminal frame does not have to hardcode a duplicate string literal.
const TerminalErrorMessage = "stream terminated unexpectedly"

// MessagesSnapshot converts Eino messages with eino-agui/convert before
// emitting MESSAGES_SNAPSHOT.
func (b *Bridge) MessagesSnapshot(messages []*einoschema.Message) {
	if b == nil || b.emit == nil {
		return
	}
	b.emit.MessagesSnapshot(convert.ToAGUIMessages(messages))
}

func (b *Bridge) StateSnapshot(snapshot any) {
	if b == nil || b.emit == nil {
		return
	}
	b.emit.StateSnapshot(snapshot)
}
func (b *Bridge) StateDelta(ops []aguievents.JSONPatchOperation) {
	if b == nil || b.emit == nil {
		return
	}
	b.emit.StateDelta(ops)
}
func (b *Bridge) ActivitySnapshot(messageID, activityType string, content any) {
	if b == nil || b.emit == nil {
		return
	}
	b.emit.ActivitySnapshot(messageID, activityType, content)
}
func (b *Bridge) ActivityDelta(messageID, activityType string, patch []aguievents.JSONPatchOperation) {
	if b == nil || b.emit == nil {
		return
	}
	b.emit.ActivityDelta(messageID, activityType, patch)
}
func (b *Bridge) StepStarted(name string) {
	if b == nil || b.emit == nil {
		return
	}
	b.emit.StepStarted(name)
}
func (b *Bridge) StepFinished(name string) {
	if b == nil || b.emit == nil {
		return
	}
	b.emit.StepFinished(name)
}
func (b *Bridge) Custom(name string, value any) {
	if b == nil || b.emit == nil {
		return
	}
	b.emit.Custom(name, value)
}
func (b *Bridge) Error(message string) {
	if b == nil || b.emit == nil {
		return
	}
	b.emit.RunError(message)
}
func (b *Bridge) ReasoningEncryptedValue(subtype aguievents.ReasoningEncryptedValueSubtype, entityID, encryptedValue string) {
	if b == nil || b.emit == nil {
		return
	}
	b.emit.ReasoningEncryptedValue(subtype, entityID, encryptedValue)
}

func (b *Bridge) emitMessageDelta(event session.EventRecord) {
	messageID := event.MessageID
	// A message already delivered through the agentic committed-projection
	// path (b.projectedMessages, set by emitMessageSnapshot or an earlier
	// emitLiveMessageCommitted) has already reached this connection in
	// full -- natively if nativeStreamed was false at commit time, or as a
	// custom-only supplement otherwise (see emitLiveMessageCommitted's
	// mode-selection doc comment). Any FURTHER delta naming the same
	// messageID on this connection is therefore necessarily stale: content
	// that was sitting buffered on the live tail (Reconnect subscribes
	// before calling replay(), see Reconnect's doc comment) when this
	// connection's own sweep already found the message fully committed, or
	// a duplicate publish. Re-emitting it here would duplicate native
	// content the client already has -- the fourth W7 fix-pass review's
	// P0-1 finding (reviews/w7-fixes3-2026-09-12/). Dropping it is safe
	// because it is transport content, not protocol bookkeeping: nothing
	// downstream depends on having seen it.
	if b.messageProjected(messageID) {
		return
	}
	payload := messageDeltaPayload{}
	_ = json.Unmarshal(event.Payload, &payload)
	// includeReasoning gates this live delta path exactly like it gates
	// the durable committed-projection path (emitLiveMessageCommitted,
	// emitMessageSnapshot): a host that has not attested
	// GateProviderReasoningStorage is satisfied (agui/policy.go) must
	// never see live reasoning deltas either, or the gate is closed on
	// one path of this bridge and open on the other for the same
	// reconnecting client (W7 fix-pass review finding P1-D/I3).
	if b.includeReasoning && payload.Reasoning != "" {
		if b.textOpen[messageID] {
			b.emit.TextEnd(string(messageID))
			delete(b.textOpen, messageID)
		}
		reasoningID := b.reasoning[messageID]
		if reasoningID == "" {
			reasoningID = string(messageID)
			b.reasoning[messageID] = reasoningID
			b.emit.ReasoningStart(reasoningID)
			b.emit.ReasoningMessageStart(reasoningID)
		}
		b.emit.ReasoningContent(reasoningID, payload.Reasoning)
		b.markNativeStreamed(messageID)
	}
	if payload.Content != "" {
		if b.reasoning[messageID] != "" {
			b.emit.ReasoningMessageEnd(b.reasoning[messageID])
			b.emit.ReasoningEnd(b.reasoning[messageID])
			delete(b.reasoning, messageID)
		}
		if !b.textOpen[messageID] {
			b.emit.TextStart(string(messageID))
			b.textOpen[messageID] = true
		}
		b.emit.TextContent(string(messageID), payload.Content)
		b.markNativeStreamed(messageID)
	}
}

func (b *Bridge) emitToolCallUpdated(event session.EventRecord) {
	// See emitMessageDelta's messageProjected guard: the same staleness
	// argument applies to a tool-call update for a message already fully
	// delivered through the committed-projection path.
	if b.messageProjected(event.MessageID) {
		return
	}
	payload := toolPayload{}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		b.emit.RunError(err.Error())
		return
	}
	toolCallID := string(event.ToolCallID)
	if toolCallID == "" {
		toolCallID = payload.ID
	}
	b.closeOpen()
	if payload.Name != "" {
		b.emit.ToolStart(toolCallID, payload.Name, string(event.MessageID))
		b.markNativeStreamed(event.MessageID)
	}
	if payload.Arguments != "" {
		b.emit.ToolArgs(toolCallID, payload.Arguments.String())
		b.markNativeStreamed(event.MessageID)
	}
	switch payload.Status {
	case string(session.ToolCallCompleted), string(session.ToolCallFailed), string(session.ToolCallInterrupted):
		b.emit.ToolEnd(toolCallID)
		b.emit.ToolResult(string(event.MessageID), toolCallID, payload.ResultContent())
		b.markNativeStreamed(event.MessageID)
	}
}

func (b *Bridge) closeOpen() {
	for messageID := range b.textOpen {
		b.emit.TextEnd(string(messageID))
		delete(b.textOpen, messageID)
	}
	for messageID, reasoningID := range b.reasoning {
		b.emit.ReasoningMessageEnd(reasoningID)
		b.emit.ReasoningEnd(reasoningID)
		delete(b.reasoning, messageID)
	}
}

// EmitCommittedProjection forwards a fully-built agentic projection and its
// binding receipt to the underlying emitter's committed-projection path
// (see convert.ToAgenticProjection / emitter.Emitter.EmitCommittedProjection
// and emitter.DeliveryMode). It never touches b.store: callers (this file's
// emitLiveMessageCommitted, and package-level replay code) own loading and
// projecting durable content.
func (b *Bridge) EmitCommittedProjection(projection *convert.AgenticProjection, receipt convert.CommitReceiptV1, mode aguiemitter.DeliveryMode) bool {
	if b == nil || b.emit == nil {
		return false
	}
	return b.emit.EmitCommittedProjection(projection, receipt, mode)
}

// emitLiveMessageCommitted reacts to a durable session.MessageCommittedEventKind
// notification (see that constant's doc comment) by reloading and
// reprojecting event.MessageID's session and emitting it through the
// agentic committed path. The delivery mode depends on whether THIS
// message already had native content streamed to THIS connection -- see
// Bridge.nativeStreamed's doc comment for why a per-message record is used
// instead of a connection-phase flag:
//
//   - If b.nativeStreamed[event.MessageID] is false, this uses
//     DeliveryModeCommittedOnly, which includes a native AG-UI
//     representation (TEXT_MESSAGE_*/TOOL_CALL_*/...) alongside the
//     eino.agentic.v1 custom supplement: this connection has not (yet)
//     natively streamed this message's content, whether because it
//     committed during replay()'s own durable sweep (its live
//     EventMessageDelta records, if any, were never durable -- see
//     runtime/adk_model.go -- so this connection cannot have seen them)
//     or for any other reason.
//   - If b.nativeStreamed[event.MessageID] is true, this uses
//     DeliveryModeLiveContinuation (custom supplement only): representable
//     native content for this message already reached this same
//     connection via emitMessageDelta/emitToolCallUpdated before the
//     message committed, so this must not duplicate it as a second native
//     event.
//
// It reprojects the WHOLE session's durable history rather than loading just
// this one message: session.Store exposes no by-ID single-message read
// (only session-scoped ListMessages), so this reuses the same
// loadCommittedProjections path replay uses for correctness rather than
// adding an unverified narrower one. This is O(session history) per commit,
// a known cost a future single-message store read should remove.
//
// A message already recorded in b.projectedMessages is skipped outright,
// with no reload attempt and no error: publishMessageCommitted's own doc
// comment documents this notification as best-effort and thus
// at-least-once, and a caller (replay() and Reconnect's live loop both
// forward every non-live-only durable event, including this one, to
// Bridge.Emit -- see replay.go) can hand the same messageID to this method
// more than once for the same durable content. Re-attempting the
// projection would mint a receipt colliding with the one already emitted
// at the observation revision the message committed at, and the
// underlying emitter's dedup (agenticReceiptKey) treats that collision as
// a hard encoding error -- exactly the shape that used to abort Reconnect
// on a benign duplicate commit receipt (W7 fix-pass review finding P0-A).
func (b *Bridge) emitLiveMessageCommitted(ctx context.Context, event session.EventRecord) {
	if b.store == nil || event.SessionID == "" || event.MessageID == "" {
		return
	}
	if b.messageProjected(event.MessageID) {
		return
	}
	projections, err := loadCommittedProjections(ctx, b.store, event.SessionID, b.contentLimits, b.includeReasoning)
	if err != nil {
		b.recordLiveErr(fmt.Errorf("agui: live committed-projection reload failed for session %s message %s: %w", event.SessionID, event.MessageID, err))
		return
	}
	mode := aguiemitter.DeliveryModeLiveContinuation
	if !b.nativeAlreadyStreamed(event.MessageID) {
		mode = aguiemitter.DeliveryModeCommittedOnly
	}
	for _, p := range projections {
		if p.MessageID != event.MessageID {
			continue
		}
		// EmitCommittedProjection's own bool return IS consulted here
		// (unlike EncErr()/Err(), which every path that returns false
		// already records on the underlying emitter -- see this bridge's
		// EncErr/Err doc comments): only a successful emission is safe to
		// remember in projectedMessages, so a transient failure here
		// still allows a later retry of the same messageID to attempt
		// the projection again instead of being permanently skipped.
		if b.EmitCommittedProjection(p.Projection, p.Receipt, mode) {
			b.markMessageProjected(p.MessageID)
		}
		return
	}
	// The named message is simply not (yet) present in this reload's
	// projections. Unlike the reload failure above, this is a BENIGN miss,
	// not a hard failure, and must not be latched into liveErr: W7
	// fix-pass review finding P0-2 traced this to
	// runtime.publishMessageCommitted's own documented best-effort
	// choreography, which appends and publishes this notification in a
	// separate call strictly AFTER the message's content transaction
	// commits, read back here through b.store -- possibly a different
	// session.Store handle than the one the content committed through
	// (a read replica, a long-held repeatable-read snapshot, ...). Any
	// such handle that can observe the notification before the content it
	// names produces exactly this branch. (The only other durable route a
	// prior pass suspected -- session.RoleTool always projecting empty --
	// is unreachable: nothing in this module ever constructs a
	// RoleTool message; runtime's two publishMessageCommitted call sites
	// use RoleUser/RoleAssistant, both of which always project at least
	// one AgenticMessage.) Deliberately do NOT call recordLiveErr: turning
	// this transient, read-your-writes race into a fatal LiveErr() would
	// abort Reconnect over a benign gap, exactly the class of stream-kill
	// this bridge exists to avoid (see also emitLiveMessageCommitted's
	// projectedMessages paragraph above, for the sibling P0-A case).
	//
	// The fourth W7 fix-pass review's I2 finding corrected the recovery
	// story an earlier version of this comment told: publishMessageCommitted
	// being at-least-once does NOT mean this exact notification is
	// re-delivered later -- both runtime call sites (tool_preparation.go,
	// tool_execution.go) publish it exactly once per commit, with no retry
	// or re-publish. The real recovery is a FRESH reconnect: its own
	// emitMessageSnapshot reload will see the message once it becomes
	// visible, independent of this notification, which this connection has
	// already consumed (Reconnect's seen map, agui/replay.go). The message
	// is left out of projectedMessages here so that fresh-reconnect path
	// -- or, on this same connection, the extremely unlikely case that this
	// exact messageID commits and re-notifies a second time -- is not
	// mistaken for already-delivered.
	//
	// This miss is deliberately not fatal, but it must not be invisible
	// either: BenignCommitMisses gives a host watching this Bridge (or a
	// metrics scrape built on top of it) a way to see a rate that should
	// normally be zero or near-zero, since nothing else on this path is
	// otherwise observable when it happens.
	b.benignCommitMisses++
}

// recordLiveErr is first-error-wins, matching the underlying emitter's own
// Err()/EncErr() semantics.
func (b *Bridge) recordLiveErr(err error) {
	if b.liveErr == nil {
		b.liveErr = err
	}
}

type messageDeltaPayload struct {
	Content   string `json:"content"`
	Reasoning string `json:"reasoning"`
}

type runFinishedPayload struct {
	Status      string `json:"status"`
	Interrupted bool   `json:"interrupted"`
}

type toolPayload struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	Arguments  rawJSONString  `json:"arguments"`
	Status     string         `json:"status"`
	Content    string         `json:"content"`
	Structured map[string]any `json:"structured"`
	Truncated  bool           `json:"truncated"`
	Redacted   bool           `json:"redacted"`
}

func (p toolPayload) ResultContent() string {
	if p.Content != "" {
		return p.Content
	}
	if len(p.Structured) > 0 {
		raw, err := json.Marshal(p.Structured)
		if err == nil {
			return string(raw)
		}
	}
	if p.Status != "" {
		return fmt.Sprintf(`{"status":%q,"truncated":%t,"redacted":%t}`, p.Status, p.Truncated, p.Redacted)
	}
	return ""
}

type rawJSONString string

func (s *rawJSONString) UnmarshalJSON(raw []byte) error {
	if string(raw) == "null" {
		*s = ""
		return nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		*s = rawJSONString(text)
		return nil
	}
	if !json.Valid(raw) {
		return fmt.Errorf("invalid JSON arguments")
	}
	*s = rawJSONString(raw)
	return nil
}

func (s rawJSONString) String() string {
	return string(s)
}
