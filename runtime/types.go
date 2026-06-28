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
	ID        session.ToolCallID
	SessionID session.ID
	RunID     session.RunID
	MessageID session.MessageID
	Name      string
	Scope     ToolScope
	Pattern   string
	Input     json.RawMessage
	Approval  ApprovalRequester
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

// ContextSource contributes prompt or message context before model conversion.
type ContextSource interface {
	LoadContext(ctx context.Context, snapshot TurnSnapshot) ([]*einoschema.Message, error)
}

// Hook observes or mutates documented runtime save points.
type Hook interface {
	BeforeRun(ctx context.Context, run session.Run) error
	BeforeTurn(ctx context.Context, snapshot TurnSnapshot) (TurnSnapshot, error)
	AfterTurn(ctx context.Context, snapshot TurnSnapshot, result Result) error
	AfterRun(ctx context.Context, result Result) error
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
)

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
