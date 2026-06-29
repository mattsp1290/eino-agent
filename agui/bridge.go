package agui

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"

	aguievents "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/events"
	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"
	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"
	einoschema "github.com/cloudwego/eino/schema"
	"github.com/mattsp1290/eino-agui/convert"
	aguiemitter "github.com/mattsp1290/eino-agui/emitter"
	aguitools "github.com/mattsp1290/eino-agui/tools"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

// Bridge adapts runtime events and durable snapshots to typed AG-UI SSE events.
type Bridge struct {
	emit      *aguiemitter.Emitter
	textOpen  map[session.MessageID]bool
	reasoning map[session.MessageID]string
}

// NewBridge binds an AG-UI emitter to the SDK's concrete SSE writer pair.
func NewBridge(ctx context.Context, writer *bufio.Writer, sseWriter *sse.SSEWriter, threadID, runID string, cancel context.CancelFunc) *Bridge {
	return &Bridge{
		emit:      aguiemitter.NewEmitter(ctx, writer, sseWriter, threadID, runID, cancel),
		textOpen:  map[session.MessageID]bool{},
		reasoning: map[session.MessageID]string{},
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

// Emitter exposes the concrete eino-agui emitter for packages that delegate
// directly to eino-agui stream tapping.
func (b *Bridge) Emitter() *aguiemitter.Emitter {
	if b == nil {
		return nil
	}
	return b.emit
}

// Emit implements runtime.EventSink.
func (b *Bridge) Emit(_ context.Context, event runtime.Event) error {
	if b == nil || b.emit == nil {
		return nil
	}
	switch event.Kind {
	case runtime.EventRunStarted:
		b.emit.RunStarted()
	case runtime.EventMessageDelta:
		b.emitMessageDelta(event)
	case runtime.EventToolCallUpdated:
		b.emitToolCallUpdated(event)
	case runtime.EventContextEpochChanged:
		b.emit.Custom("context_epoch_changed", map[string]any{
			"epoch_id": string(event.EpochID),
		})
	case runtime.EventModelFallbackEngaged:
		// Re-emit the durable ModelFallbackPayload object verbatim so the live
		// AG-UI custom event carries the exact same key set as the persisted
		// payload (omitempty and all) — one wire shape across both surfaces.
		value := map[string]any{}
		if len(event.Payload) > 0 {
			_ = json.Unmarshal(event.Payload, &value)
		}
		b.emit.Custom("model_fallback_engaged", value)
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
	return b.Err()
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

// ClientToolInfos delegates AG-UI client tool binding to eino-agui/tools.
func ClientToolInfos(tools []aguitypes.Tool, opts ...aguitools.SchemaOption) ([]*einoschema.ToolInfo, error) {
	return aguitools.ClientToolInfos(tools, opts...)
}

// ClassifyToolCalls delegates AG-UI client/server tool partitioning to
// eino-agui/tools.
func ClassifyToolCalls(calls []einoschema.ToolCall, clientNames map[string]bool) (server, client []einoschema.ToolCall) {
	return aguitools.ClassifyToolCalls(calls, clientNames)
}

func (b *Bridge) emitMessageDelta(event runtime.Event) {
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

func (b *Bridge) emitToolCallUpdated(event runtime.Event) {
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
		b.emit.ToolStart(toolCallID, payload.Name)
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
