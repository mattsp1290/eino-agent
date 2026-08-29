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
	Usage session.Usage
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
	InputDecoder InputDecoder
	Pattern      PermissionPatternResolver
	Retention    RetentionPolicy
	Metadata     map[string]string
}

// ToolScopeContext is the data-only input used while selecting and scoping
// tools. It deliberately excludes messages, model clients, and executors.
type ToolScopeContext struct {
	SessionID     session.ID
	WorkspaceID   string
	WorkspaceRoot string
	EnabledTools  []string
	DisabledTools []string
}

// Clone returns a defensive copy of the context containers.
func (c ToolScopeContext) Clone() ToolScopeContext {
	c.EnabledTools = cloneSlice(c.EnabledTools)
	c.DisabledTools = cloneSlice(c.DisabledTools)
	return c
}

// ToolContext is the bounded data exposed during tool execution.
type ToolContext struct {
	Turn          BoundedTurnMetadata
	WorkspaceID   string
	WorkspaceRoot string
}

// Clone returns a defensive copy of the context containers.
func (c ToolContext) Clone() ToolContext {
	c.Turn = cloneBoundedTurnMetadata(c.Turn)
	return c
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
	Context         ToolContext
}

// ToolScope describes the authority scope for a tool.
type ToolScope struct {
	WorkspaceID string
	Root        string
	Permissions []string
}

// InputDecoder validates and normalizes model-provided tool input.
type InputDecoder interface {
	DecodeToolInput(ctx context.Context, raw json.RawMessage) (json.RawMessage, error)
}

// PermissionPatternResolver derives deterministic permission identity from
// final normalized tool input. Implementations must be side-effect free.
type PermissionPatternResolver interface {
	ResolvePermissionPattern(ctx context.Context, input json.RawMessage) (string, error)
}

type PermissionPatternResolverFunc func(context.Context, json.RawMessage) (string, error)

func (fn PermissionPatternResolverFunc) ResolvePermissionPattern(ctx context.Context, input json.RawMessage) (string, error) {
	return fn(ctx, input)
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

const (
	// EventRunStarted is emitted after durable run admission.
	EventRunStarted = "run_started"
	// EventMessageDelta is emitted for live-only model deltas.
	EventMessageDelta = "message_delta"
	// EventToolCallUpdated is emitted for tool-call state transitions.
	EventToolCallUpdated = "tool_call_updated"
	// EventRunFinished is emitted after durable run settlement.
	EventRunFinished = session.RunSettlementEventKind
	// EventTailOverflow reports that a reconnect live-tail subscriber fell
	// behind a bounded queue and must reconnect or resync.
	EventTailOverflow = "tail_overflow"
)

// EventSink receives transport and observability copies of internal runtime
// events. It has no durable-store authority; runtime persists durable events
// through the current run's fenced ExecutionStore before publication.
type EventSink interface {
	Emit(ctx context.Context, event session.EventRecord)
}

// EventSinkFunc adapts a function into an EventSink.
type EventSinkFunc func(context.Context, session.EventRecord)

// Emit calls fn.
func (fn EventSinkFunc) Emit(ctx context.Context, event session.EventRecord) { fn(ctx, event) }

var _ EventSink = EventSinkFunc(nil)

func cloneSlice[T any](src []T) []T {
	if src == nil {
		return nil
	}
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}
