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

// Tool describes one runtime-materialized tool available to a turn.
type Tool struct {
	Name      string
	Info      *einoschema.ToolInfo
	Executor  ToolExecutor
	RetrySafe bool
	Metadata  map[string]string
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
	Input     json.RawMessage
	Approval  ApprovalRequester
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
)

// Event is the internal event envelope consumed by AG-UI and observability
// adapters. Durable stores remain authoritative for replay.
type Event struct {
	Kind      EventKind
	SessionID session.ID
	RunID     session.RunID
	Payload   json.RawMessage
	Time      time.Time
	LiveOnly  bool
}

// EventSink receives internal runtime events.
type EventSink interface {
	Emit(ctx context.Context, event Event) error
}
