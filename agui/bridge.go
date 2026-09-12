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

// Bridge adapts runtime events and durable snapshots to typed AG-UI SSE events.
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
}

// NewBridge binds an AG-UI emitter to the SDK's concrete SSE writer pair.
// store and contentLimits back the agentic committed-projection path (see
// Bridge.store's doc comment); pass a nil store to disable it.
func NewBridge(ctx context.Context, store session.Store, contentLimits session.ContentLimits, writer *bufio.Writer, sseWriter *sse.SSEWriter, threadID, runID string, cancel context.CancelFunc) *Bridge {
	return &Bridge{
		emit:          aguiemitter.NewEmitter(ctx, writer, sseWriter, threadID, runID, cancel),
		textOpen:      map[session.MessageID]bool{},
		reasoning:     map[session.MessageID]string{},
		store:         store,
		contentLimits: contentLimits,
	}
}

// Err returns the first transport error from the underlying AG-UI emitter.
func (b *Bridge) Err() error {
	if b == nil || b.emit == nil {
		return nil
	}
	return b.emit.Err()
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
	if payload.Reasoning != "" {
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
// agentic committed path with DeliveryModeLiveContinuation (custom
// eino.agentic.v1 supplements only: any representable native content --
// text, tool calls -- already reached the client as transient deltas during
// the live phase, via emitMessageDelta/emitToolCallUpdated, so this must not
// duplicate it as a second native event).
//
// It reprojects the WHOLE session's durable history rather than loading just
// this one message: session.Store exposes no by-ID single-message read
// (only session-scoped ListMessages), so this reuses the same
// loadCommittedProjections path replay uses for correctness rather than
// adding an unverified narrower one. This is O(session history) per commit,
// a known cost a future single-message store read should remove.
func (b *Bridge) emitLiveMessageCommitted(ctx context.Context, event session.EventRecord) {
	if b.store == nil || event.SessionID == "" || event.MessageID == "" {
		return
	}
	projections, err := loadCommittedProjections(ctx, b.store, event.SessionID, b.contentLimits)
	if err != nil {
		return
	}
	for _, p := range projections {
		if p.MessageID != event.MessageID {
			continue
		}
		b.EmitCommittedProjection(p.Projection, p.Receipt, aguiemitter.DeliveryModeLiveContinuation)
		return
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
