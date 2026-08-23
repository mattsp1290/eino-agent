package runtime

import (
	"context"
	"encoding/json"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// Request admits a user-visible run. Implementations persist the run before
// invoking Eino or emitting live AG-UI events.
type Request struct {
	SessionID session.ID
	ParentID  session.MessageID
	Input     []*einoschema.Message
	Config    config.Snapshot
	Metadata  map[string]string
}

// Handle describes an admitted run and its live control surface.
type Handle interface {
	RunID() session.RunID
	Done() <-chan Result
	Interrupt(ctx context.Context, reason string) error
	FollowUp(ctx context.Context, messages []*einoschema.Message) error
}

// Result is the terminal outcome of a run.
type Result struct {
	RunID       session.RunID
	Status      session.RunStatus
	MessageID   session.MessageID
	Interrupted bool
	Error       error
	// Usage is the run-total provider usage accumulated across every model
	// stream in the run (all turns and retry attempts), mirroring what the
	// observability path sums onto the run span. It is surfaced on the
	// EventRunFinished event so EventSink consumers can persist token totals
	// without reading the OTel span.
	Usage Usage
}

// Orchestrator owns run admission, locking, streaming, interruption, and
// cleanup. Store, model, tool, AG-UI, and observability details are injected.
type Orchestrator interface {
	Start(ctx context.Context, request Request) (Handle, error)
	Resume(ctx context.Context, runID session.RunID) (Handle, error)
	Status(ctx context.Context, sessionID session.ID) (session.Run, error)
}

// TurnSnapshot is the immutable state used for one provider request.
type TurnSnapshot struct {
	RunID        session.RunID
	SessionID    session.ID
	EpochID      session.EpochID
	Config       config.Snapshot
	Model        model.Resolved
	Messages     []*einoschema.Message
	Tools        []Tool
	SystemPrompt string
	CreatedAt    time.Time
}

// Clone returns a defensive copy of the snapshot container fields. Eino
// messages are pointer values, so implementations that mutate message contents
// must still clone the schema.Message values they own before mutation.
func (s TurnSnapshot) Clone() TurnSnapshot {
	next := s
	next.Config = s.Config.Clone()
	next.Messages = cloneSlice(s.Messages)
	next.Tools = cloneSlice(s.Tools)
	return next
}

// Tool describes one runtime-materialized tool available to a turn.
type Tool struct {
	Name         string
	Info         *einoschema.ToolInfo
	Executor     ToolExecutor
	RetrySafe    bool
	Scope        ToolScope
	Concurrency  ToolConcurrency
	InputDecoder InputDecoder
	Retention    RetentionPolicy
	Metadata     map[string]string
}

// ToolExecutor executes one tool call under durable runtime control.
type ToolExecutor interface {
	Execute(ctx context.Context, call ToolCall) (ToolResult, error)
}

// ToolCall is the runtime view of a model-requested tool invocation.
type ToolCall struct {
	ID              session.ToolCallID
	SessionID       session.ID
	RunID           session.RunID
	MessageID       session.MessageID
	ResultMessageID session.MessageID
	ResultPartID    session.PartID
	Name            string
	Scope           ToolScope
	Pattern         string
	Input           json.RawMessage
	Approval        ApprovalRequester
}

// ToolScope describes the authority and serialization scope for a tool.
type ToolScope struct {
	WorkspaceID    string
	Root           string
	ConcurrencyKey string
	Permissions    []string
}

// ToolConcurrency describes whether calls with the same scope may overlap.
type ToolConcurrency string

const (
	// ToolConcurrencyParallel allows concurrent calls.
	ToolConcurrencyParallel ToolConcurrency = "parallel"
	// ToolConcurrencySequential serializes calls sharing the same concurrency key.
	ToolConcurrencySequential ToolConcurrency = "sequential"
)

// InputDecoder validates and normalizes model-provided tool input.
type InputDecoder interface {
	DecodeToolInput(ctx context.Context, raw json.RawMessage) (json.RawMessage, error)
}

// RetentionPolicy describes runtime handling for large tool outputs.
type RetentionPolicy struct {
	MaxInlineBytes int64
	StoreExternal  bool
	Redact         bool
}

// ToolResult is the normalized runtime output of a tool call.
type ToolResult struct {
	Output      string
	Structured  json.RawMessage
	Attachments []Attachment
	Metadata    map[string]string
}

// Attachment is a durable reference to non-text tool output.
type Attachment struct {
	ID       string
	MIMEType string
	Name     string
	URL      string
	Metadata map[string]string
}

// ApprovalRequest asks a host application or user to approve a runtime action.
type ApprovalRequest struct {
	SessionID  session.ID
	RunID      session.RunID
	ToolCallID session.ToolCallID
	Permission string
	Patterns   []string
	Metadata   map[string]string
}

// ApprovalRequester is exposed to runtime tools for dynamic permissions.
type ApprovalRequester interface {
	Ask(ctx context.Context, request ApprovalRequest) error
}

// ToolRegistry materializes Eino-compatible runtime tools for a turn snapshot.
type ToolRegistry interface {
	ResolveTools(ctx context.Context, snapshot TurnSnapshot) ([]Tool, error)
}

// ToolRegistryFunc adapts a function into a ToolRegistry.
type ToolRegistryFunc func(context.Context, TurnSnapshot) ([]Tool, error)

// ResolveTools calls fn.
func (fn ToolRegistryFunc) ResolveTools(ctx context.Context, snapshot TurnSnapshot) ([]Tool, error) {
	return fn(ctx, snapshot)
}

// ContextSource contributes prompt or message context before model conversion.
type ContextSource interface {
	LoadContext(ctx context.Context, snapshot TurnSnapshot) ([]*einoschema.Message, error)
}

// ContextSourceFunc adapts a function into a ContextSource.
type ContextSourceFunc func(context.Context, TurnSnapshot) ([]*einoschema.Message, error)

// LoadContext calls fn.
func (fn ContextSourceFunc) LoadContext(ctx context.Context, snapshot TurnSnapshot) ([]*einoschema.Message, error) {
	return fn(ctx, snapshot)
}

// Hook observes or mutates documented runtime save points.
type Hook interface {
	BeforeRun(ctx context.Context, run session.Run) error
	BeforeTurn(ctx context.Context, snapshot TurnSnapshot) (TurnSnapshot, error)
	AfterTurn(ctx context.Context, snapshot TurnSnapshot, result Result) error
	AfterRun(ctx context.Context, result Result) error
}

// HookFuncs adapts optional functions into a Hook. Nil functions are no-ops;
// a nil BeforeTurn function returns its input snapshot unchanged.
type HookFuncs struct {
	BeforeRunFunc  func(context.Context, session.Run) error
	BeforeTurnFunc func(context.Context, TurnSnapshot) (TurnSnapshot, error)
	AfterTurnFunc  func(context.Context, TurnSnapshot, Result) error
	AfterRunFunc   func(context.Context, Result) error
}

func (h HookFuncs) BeforeRun(ctx context.Context, run session.Run) error {
	if h.BeforeRunFunc == nil {
		return nil
	}
	return h.BeforeRunFunc(ctx, run)
}

func (h HookFuncs) BeforeTurn(ctx context.Context, snapshot TurnSnapshot) (TurnSnapshot, error) {
	if h.BeforeTurnFunc == nil {
		return snapshot, nil
	}
	return h.BeforeTurnFunc(ctx, snapshot)
}

func (h HookFuncs) AfterTurn(ctx context.Context, snapshot TurnSnapshot, result Result) error {
	if h.AfterTurnFunc == nil {
		return nil
	}
	return h.AfterTurnFunc(ctx, snapshot, result)
}

func (h HookFuncs) AfterRun(ctx context.Context, result Result) error {
	if h.AfterRunFunc == nil {
		return nil
	}
	return h.AfterRunFunc(ctx, result)
}

// ToolMiddleware rewrites tool inputs before durable admission and patches
// executed results before durable settlement.
type ToolMiddleware interface {
	BeforeToolCall(ctx context.Context, tool Tool, call ToolCall) (json.RawMessage, error)
	AfterToolCall(ctx context.Context, tool Tool, call ToolCall, result ToolResult, execErr error) (ToolResult, error)
}

// ToolMiddlewareFuncs adapts optional functions into ToolMiddleware. Nil
// functions are identity pass-throughs.
type ToolMiddlewareFuncs struct {
	Before func(context.Context, Tool, ToolCall) (json.RawMessage, error)
	After  func(context.Context, Tool, ToolCall, ToolResult, error) (ToolResult, error)
}

func (m ToolMiddlewareFuncs) BeforeToolCall(ctx context.Context, tool Tool, call ToolCall) (json.RawMessage, error) {
	if m.Before == nil {
		return cloneJSON(call.Input), nil
	}
	return m.Before(ctx, tool, call)
}

func (m ToolMiddlewareFuncs) AfterToolCall(ctx context.Context, tool Tool, call ToolCall, result ToolResult, execErr error) (ToolResult, error) {
	if m.After == nil {
		return result, nil
	}
	return m.After(ctx, tool, call, result, execErr)
}

// EventKind classifies internal runtime events before transport adaptation.
type EventKind string

const (
	// EventRunStarted is emitted after durable run admission.
	EventRunStarted EventKind = "run_started"
	// EventMessageDelta is emitted for live-only model deltas.
	EventMessageDelta EventKind = "message_delta"
	// EventToolCallUpdated is emitted for tool-call state transitions.
	EventToolCallUpdated EventKind = "tool_call_updated"
	// EventContextEpochChanged is emitted when compaction changes context epoch.
	EventContextEpochChanged EventKind = "context_epoch_changed"
	// EventRunFinished is emitted after durable run settlement.
	EventRunFinished EventKind = "run_finished"
	// EventTailOverflow reports that a reconnect live-tail subscriber fell
	// behind a bounded queue and must reconnect or resync.
	EventTailOverflow EventKind = "tail_overflow"
	// EventModelFallbackEngaged reports that the host swapped the primary model
	// for a fallback at a turn boundary (e.g. a circuit-breaker trip). The
	// from→to transition is carried in Payload as ModelFallbackPayload and
	// Event.ModelID is set to the now-active (to) model. Selection of the
	// fallback is owned by the host/consumer, not the eino-agent runtime. The
	// event is durable (LiveOnly=false) and re-emitted on replay/reconnect.
	EventModelFallbackEngaged EventKind = "model_fallback_engaged"
)

// ModelFallbackPayload is the documented JSON shape carried in Event.Payload
// when Kind == EventModelFallbackEngaged. The field names are the stable wire
// contract that adapters (e.g. the ensemble local-symphony projector) target:
// from_model_id → metric label model.from, to_model_id → model.to.
type ModelFallbackPayload struct {
	FromModelID string `json:"from_model_id"`
	ToModelID   string `json:"to_model_id"`
	// FromProviderID/ToProviderID are optional; populate only when the fallback
	// crosses providers. The model-centric NewModelFallbackEvent helper leaves
	// them empty.
	FromProviderID string `json:"from_provider_id,omitempty"`
	ToProviderID   string `json:"to_provider_id,omitempty"`
	// Reason is an optional, host-defined cause (e.g. "circuit_breaker").
	Reason string `json:"reason,omitempty"`
}

// NewModelFallbackEvent builds a model_fallback_engaged Event with ModelID set
// to the to-model and the model transition encoded in Payload. It is
// model-centric: Event.ProviderID and the optional provider payload fields are
// left for the caller to set when the fallback crosses providers. Callers fill
// the remaining envelope fields (IDs, Time) as usual. No error is returned
// because json.Marshal of a struct of strings cannot fail.
func NewModelFallbackEvent(from, to, reason string) Event {
	payload, _ := json.Marshal(ModelFallbackPayload{
		FromModelID: from,
		ToModelID:   to,
		Reason:      reason,
	})
	return Event{
		Kind:    EventModelFallbackEngaged,
		ModelID: to,
		Payload: payload,
	}
}

// Event is the internal event envelope consumed by AG-UI and observability
// adapters. Durable stores remain authoritative for replay.
type Event struct {
	Kind        EventKind
	EventID     session.EventID
	SessionID   session.ID
	RunID       session.RunID
	MessageID   session.MessageID
	PartID      session.PartID
	ToolCallID  session.ToolCallID
	EpochID     session.EpochID
	ProviderID  string
	ModelID     string
	ParentID    string
	Correlation string
	Usage       Usage
	Error       EventError
	Redaction   RedactionClass
	Payload     json.RawMessage
	Time        time.Time
	LiveOnly    bool
}

// EventSink receives internal runtime events.
type EventSink interface {
	Emit(ctx context.Context, event Event) error
}

// EventSinkFunc adapts a function into an EventSink.
type EventSinkFunc func(context.Context, Event) error

// Emit calls fn.
func (fn EventSinkFunc) Emit(ctx context.Context, event Event) error { return fn(ctx, event) }

var (
	_ ToolRegistry   = ToolRegistryFunc(nil)
	_ ContextSource  = ContextSourceFunc(nil)
	_ Hook           = HookFuncs{}
	_ ToolMiddleware = ToolMiddlewareFuncs{}
	_ EventSink      = EventSinkFunc(nil)
)

func cloneSlice[T any](src []T) []T {
	if src == nil {
		return nil
	}
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}

// Usage records provider usage data in a transport-neutral form.
type Usage struct {
	InputTokens      int64
	OutputTokens     int64
	ReasoningTokens  int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	Cost             float64
}

// EventError records stable error classification without requiring adapters to
// parse opaque payload JSON.
type EventError struct {
	Code      string
	Message   string
	Retryable bool
}

// RedactionClass classifies event payload sensitivity for observability sinks.
type RedactionClass string

const (
	// RedactionNone marks payloads safe for direct export.
	RedactionNone RedactionClass = "none"
	// RedactionMetadata marks payloads where metadata can export but content cannot.
	RedactionMetadata RedactionClass = "metadata"
	// RedactionContent marks payloads containing user, model, or tool content.
	RedactionContent RedactionClass = "content"
)
