package agui

import (
	"bufio"
	"context"
	"encoding/json"
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
// the unexported fields below (including inReplaySweep) need no locking.
// Bridge is not safe for concurrent Emit calls from multiple goroutines.
type Bridge struct {
	emit      *aguiemitter.Emitter
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
	// loadCommittedProjections failure, or event.MessageID not being
	// found among this session's current projections. It does NOT
	// duplicate EmitCommittedProjection's own failures -- those already
	// surface through EncErr()/Err(), since the underlying emitter
	// records them internally regardless of whether this bridge inspects
	// its returned bool.
	liveErr error
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
	// inReplaySweep is true only while replay()'s own ListEvents loop
	// (agui/replay.go) is forwarding a connection's initial catch-up sweep
	// of durable events, and false everywhere else, including while
	// Reconnect's live tail loop runs after replay() has returned. A
	// session.MessageCommittedEventKind event observed while this is true
	// names a message whose EventMessageDelta records were LiveOnly and
	// were therefore SKIPPED by replay() (see replay()'s doc comment):
	// this connection never saw that message's live deltas, so
	// emitLiveMessageCommitted must include native events, not just the
	// eino.agentic.v1 supplement. Once inReplaySweep is false, a
	// message_committed can only arrive after this connection's own live
	// tail already streamed that message's content natively via
	// emitMessageDelta/emitToolCallUpdated, so a custom-only supplement is
	// correct there and does not duplicate content already delivered.
	inReplaySweep bool
}

// NewBridge binds an AG-UI emitter to the SDK's concrete SSE writer pair.
// store, contentLimits, and includeReasoning back the agentic
// committed-projection path (see Bridge.store's and
// Bridge.includeReasoning's doc comments); pass a nil store to disable it.
func NewBridge(ctx context.Context, store session.Store, contentLimits session.ContentLimits, includeReasoning bool, writer *bufio.Writer, sseWriter *sse.SSEWriter, threadID, runID string, cancel context.CancelFunc) *Bridge {
	return &Bridge{
		emit:             aguiemitter.NewEmitter(ctx, writer, sseWriter, threadID, runID, cancel),
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

// beginReplaySweep and endReplaySweep bracket replay()'s own ListEvents
// loop (see Bridge.inReplaySweep's doc comment). Callers must always pair
// beginReplaySweep with a deferred endReplaySweep so the flag cannot leak
// true into Reconnect's live tail loop on any return path.
func (b *Bridge) beginReplaySweep() {
	if b == nil {
		return
	}
	b.inReplaySweep = true
}

func (b *Bridge) endReplaySweep() {
	if b == nil {
		return
	}
	b.inReplaySweep = false
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
	payload := messageDeltaPayload{}
	_ = json.Unmarshal(event.Payload, &payload)
	messageID := event.MessageID
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
	}
}

func (b *Bridge) emitToolCallUpdated(event session.EventRecord) {
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
	}
	if payload.Arguments != "" {
		b.emit.ToolArgs(toolCallID, payload.Arguments.String())
	}
	switch payload.Status {
	case string(session.ToolCallCompleted), string(session.ToolCallFailed), string(session.ToolCallInterrupted):
		b.emit.ToolEnd(toolCallID)
		b.emit.ToolResult(string(event.MessageID), toolCallID, payload.ResultContent())
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
// agentic committed path. The delivery mode depends on which phase of the
// connection observed the notification -- see Bridge.inReplaySweep's doc
// comment for why:
//
//   - While replay()'s own durable ListEvents sweep is running
//     (b.inReplaySweep true), this uses DeliveryModeCommittedOnly, which
//     includes a native AG-UI representation (TEXT_MESSAGE_*/TOOL_CALL_*/...)
//     alongside the eino.agentic.v1 custom supplement, because this
//     connection's live deltas for the message were LiveOnly records that
//     replay() skips (see replay()'s doc comment) -- it never saw them.
//   - Once replay() has returned and Reconnect's live tail loop is running
//     (b.inReplaySweep false), this uses DeliveryModeLiveContinuation
//     (custom supplement only): any representable native content already
//     reached this same connection as transient deltas via
//     emitMessageDelta/emitToolCallUpdated before the message committed, so
//     this must not duplicate it as a second native event.
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
	if b.inReplaySweep {
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
	// one AgenticMessage.) Deliberately do NOT call recordLiveErr: since
	// publishMessageCommitted is at-least-once, a later message_committed
	// for the same messageID -- once its content is actually visible to a
	// reload -- can still succeed, and turning this transient,
	// self-healing miss into a fatal LiveErr() would abort Reconnect over
	// a benign read-your-writes gap, exactly the class of stream-kill this
	// bridge exists to avoid (see also emitLiveMessageCommitted's
	// projectedMessages paragraph above, for the sibling P0-A case). The
	// message is left out of projectedMessages so a later retry is not
	// mistaken for already-delivered.
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
