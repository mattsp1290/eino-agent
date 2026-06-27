package session

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ID identifies a durable conversation session.
type ID string

// RunID identifies one admitted execution attempt inside a session.
type RunID string

// MessageID identifies a durable message.
type MessageID string

// PartID identifies one durable message part.
type PartID string

// ToolCallID identifies one model-requested tool call.
type ToolCallID string

// EpochID identifies a context epoch, including compacted epochs.
type EpochID string

var (
	// ErrSessionBusy reports that a session already has a nonterminal owner run.
	ErrSessionBusy = errors.New("session has active run")
	// ErrConflict reports a failed conditional update, claim, or settlement.
	ErrConflict = errors.New("session store conflict")
	// ErrNotFound reports that a durable session record does not exist.
	ErrNotFound = errors.New("session record not found")
)

// Session is durable conversation metadata. Runtime dependencies such as model
// clients, tool implementations, and hooks are intentionally not serialized.
type Session struct {
	ID          ID
	ParentID    ID
	WorkspaceID string
	Directory   string
	Title       string
	Metadata    map[string]string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// RunStatus describes the durable lifecycle of an admitted run.
type RunStatus string

const (
	// RunPending means the run was admitted but execution has not started.
	RunPending RunStatus = "pending"
	// RunRunning means the run owns the session execution lock.
	RunRunning RunStatus = "running"
	// RunInterrupted means execution was canceled or recovery found it unfinished.
	RunInterrupted RunStatus = "interrupted"
	// RunFailed means execution reached a terminal error.
	RunFailed RunStatus = "failed"
	// RunCompleted means execution reached a normal terminal state.
	RunCompleted RunStatus = "completed"
)

// ComponentVersion records the runtime dependency identity captured at run
// admission, for example a plugin, provider adapter, or config source.
type ComponentVersion struct {
	Name    string
	Version string
	Hash    string
	Source  string
}

// Run records one admitted execution attempt before provider streaming starts.
type Run struct {
	ID           RunID
	SessionID    ID
	ParentRunID  RunID
	ParentMsgID  MessageID
	OwnerID      string
	LeaseUntil   time.Time
	Agent        string
	ProviderID   string
	ModelID      string
	ContextEpoch EpochID
	Status       RunStatus
	Config       map[string]string
	Components   []ComponentVersion
	CreatedAt    time.Time
	StartedAt    time.Time
	FinishedAt   time.Time
	Error        string
}

// Terminal reports whether the run no longer owns execution.
func (r Run) Terminal() bool {
	return r.Status == RunInterrupted || r.Status == RunFailed || r.Status == RunCompleted
}

// Role is the durable role of a message in replayable history.
type Role string

const (
	// RoleSystem records system context when a store chooses to persist it.
	RoleSystem Role = "system"
	// RoleUser records user-authored or synthetic user input.
	RoleUser Role = "user"
	// RoleAssistant records model output.
	RoleAssistant Role = "assistant"
	// RoleTool records tool results.
	RoleTool Role = "tool"
)

// Message is the replayable message envelope. Content lives in ordered parts so
// live deltas, tool calls, files, and compaction markers can settle separately.
type Message struct {
	ID        MessageID
	SessionID ID
	RunID     RunID
	ParentID  MessageID
	Role      Role
	Agent     string
	ModelID   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// PartKind classifies replayable message content.
type PartKind string

const (
	// PartText stores text content or a settled text delta.
	PartText PartKind = "text"
	// PartReasoning stores model reasoning content when a provider exposes it.
	PartReasoning PartKind = "reasoning"
	// PartToolCall stores a tool-call state transition.
	PartToolCall PartKind = "tool_call"
	// PartToolResult stores a tool result sent back to the model.
	PartToolResult PartKind = "tool_result"
	// PartFile stores a durable file or media reference.
	PartFile PartKind = "file"
	// PartStep stores provider/runtime step start and finish markers.
	PartStep PartKind = "step"
	// PartCompaction stores compaction request or summary metadata.
	PartCompaction PartKind = "compaction"
	// PartState stores app-visible state snapshots or patches.
	PartState PartKind = "state"
)

// Part is an ordered, replayable fragment of a message. Payload is structured
// JSON owned by the part kind so stores can persist forward-compatible data.
type Part struct {
	ID        PartID
	MessageID MessageID
	SessionID ID
	RunID     RunID
	Kind      PartKind
	Ordinal   int64
	Payload   json.RawMessage
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ToolCallStatus records a durable tool-call settlement state.
type ToolCallStatus string

const (
	// ToolCallPending means the model requested a call but it is not claimed.
	ToolCallPending ToolCallStatus = "pending"
	// ToolCallRunning means a worker has claimed the call.
	ToolCallRunning ToolCallStatus = "running"
	// ToolCallCompleted means the call produced a result.
	ToolCallCompleted ToolCallStatus = "completed"
	// ToolCallFailed means the call produced a terminal error.
	ToolCallFailed ToolCallStatus = "failed"
	// ToolCallInterrupted means the call may have started but did not settle.
	ToolCallInterrupted ToolCallStatus = "interrupted"
)

// ToolCall is the durable execution envelope for one tool invocation.
type ToolCall struct {
	ID          ToolCallID
	SessionID   ID
	RunID       RunID
	MessageID   MessageID
	Name        string
	Input       json.RawMessage
	Output      json.RawMessage
	Status      ToolCallStatus
	RetrySafe   bool
	Metadata    map[string]string
	ClaimedBy   string
	ClaimToken  string
	LeaseUntil  time.Time
	StartedAt   time.Time
	CompletedAt time.Time
	Error       string
}

// ContextEpoch records the history segment used to build provider context.
type ContextEpoch struct {
	ID               EpochID
	SessionID        ID
	ParentEpochID    EpochID
	SummaryMessageID MessageID
	SummarizedFromID MessageID
	SummarizedToID   MessageID
	TailStartID      MessageID
	ModelID          string
	ProviderID       string
	Trigger          string
	Reason           string
	NextAction       EpochNextAction
	CreatedAt        time.Time
	ClosedAt         time.Time
}

// EpochNextAction records what the runtime should do after compaction.
type EpochNextAction string

const (
	// EpochNextStop means compaction ends the active run.
	EpochNextStop EpochNextAction = "stop"
	// EpochNextAutoContinue means runtime may continue with a synthetic prompt.
	EpochNextAutoContinue EpochNextAction = "auto_continue"
	// EpochNextReplay means runtime may replay the triggering user message.
	EpochNextReplay EpochNextAction = "replay"
)

// ReplayCursor selects a stable page of replayable history.
type ReplayCursor struct {
	AfterMessageID MessageID
	AfterPartID    PartID
	Limit          int
}

// ReplayBatch returns messages and their ordered parts for history replay.
type ReplayBatch struct {
	Messages []Message
	Parts    []Part
	Next     ReplayCursor
}

// EventID identifies a durable runtime event record.
type EventID string

// EventRecord is the store-level event shape. Runtime adapters may project
// richer typed events into this durable envelope without making session depend
// on the runtime package.
type EventRecord struct {
	ID          EventID
	SessionID   ID
	RunID       RunID
	MessageID   MessageID
	PartID      PartID
	ToolCallID  ToolCallID
	EpochID     EpochID
	ProviderID  string
	ModelID     string
	ParentID    string
	Kind        string
	Correlation string
	Usage       Usage
	Error       EventError
	Redaction   RedactionClass
	Payload     json.RawMessage
	LiveOnly    bool
	CreatedAt   time.Time
}

// Usage records provider usage data in a store-level event projection.
type Usage struct {
	InputTokens      int64
	OutputTokens     int64
	ReasoningTokens  int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	Cost             float64
}

// EventError records stable error classification in a durable event.
type EventError struct {
	Code      string
	Message   string
	Retryable bool
}

// RedactionClass classifies durable event payload sensitivity.
type RedactionClass string

const (
	// RedactionNone marks payloads safe for direct export.
	RedactionNone RedactionClass = "none"
	// RedactionMetadata marks payloads where metadata can export but content cannot.
	RedactionMetadata RedactionClass = "metadata"
	// RedactionContent marks payloads containing user, model, or tool content.
	RedactionContent RedactionClass = "content"
)

// EventCursor selects a stable page of durable events.
type EventCursor struct {
	AfterEventID EventID
	Limit        int
}

// EventBatch returns ordered durable event records.
type EventBatch struct {
	Events []EventRecord
	Next   EventCursor
}

// Store is the durable boundary used by runtime orchestration. Implementations
// provide locking and transactions; callers should not infer durability from
// live AG-UI transport delivery.
type Store interface {
	CreateSession(ctx context.Context, session Session) (Session, error)
	GetSession(ctx context.Context, id ID) (Session, error)
	UpdateSession(ctx context.Context, session Session) error
	// AdmitRun atomically creates a run and makes it the active owner for its
	// session. Implementations return ErrSessionBusy when another nonterminal
	// run owns the session.
	AdmitRun(ctx context.Context, run Run) (Run, error)
	GetRun(ctx context.Context, id RunID) (Run, error)
	ActiveRun(ctx context.Context, sessionID ID) (Run, error)
	ListUnfinishedRuns(ctx context.Context) ([]Run, error)
	RenewRunLease(ctx context.Context, runID RunID, ownerID string, until time.Time) error
	FinishRun(ctx context.Context, run Run) error
	AppendMessage(ctx context.Context, message Message) (Message, error)
	AppendPart(ctx context.Context, part Part) (Part, error)
	UpdatePart(ctx context.Context, part Part) error
	ListMessages(ctx context.Context, sessionID ID, cursor ReplayCursor) (ReplayBatch, error)
	AppendEvent(ctx context.Context, event EventRecord) (EventRecord, error)
	ListEvents(ctx context.Context, sessionID ID, cursor EventCursor) (EventBatch, error)
	CreateToolCall(ctx context.Context, call ToolCall) (ToolCall, error)
	GetToolCall(ctx context.Context, id ToolCallID) (ToolCall, error)
	ListUnfinishedToolCalls(ctx context.Context, runID RunID) ([]ToolCall, error)
	ClaimToolCall(ctx context.Context, call ToolCall) (ToolCall, error)
	FinishToolCall(ctx context.Context, call ToolCall) error
	StartContextEpoch(ctx context.Context, epoch ContextEpoch) (ContextEpoch, error)
	FinishContextEpoch(ctx context.Context, epoch ContextEpoch) error
}

// Tx is a store view scoped to one implementation-defined transaction.
type Tx interface {
	Store
}

// Transactor runs store operations inside one transaction.
type Transactor interface {
	WithinTx(ctx context.Context, fn func(context.Context, Tx) error) error
}
