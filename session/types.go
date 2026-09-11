package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// TurnID identifies one durable turn admitted inside a run.
type TurnID string

// InboxID identifies one durable queued inbox item.
type InboxID string

var (
	// ErrSessionBusy reports that a session already has a nonterminal owner run.
	ErrSessionBusy = errors.New("session has active run")
	// ErrConflict reports a failed conditional update, claim, or settlement.
	ErrConflict = errors.New("session store conflict")
	// ErrNotFound reports that a durable session record does not exist.
	ErrNotFound = errors.New("session record not found")
	// ErrRunClosed reports that a run-scoped write (e.g. EnqueueInboxForRun)
	// was rejected because the targeted run is already terminal. Distinct
	// from ErrConflict so a caller can tell "the run is closed, do not
	// acknowledge this input" apart from an unrelated write conflict (e.g. a
	// mismatched idempotent retry payload).
	ErrRunClosed = errors.New("session: run is closed")
	// ErrRunHasQueuedInput reports that a SettleRun(RunCompleted) was
	// refused because the run's session has a durably queued inbox item
	// (see SettleRun's terminal-settlement race guard). It wraps
	// ErrConflict (errors.Is(err, ErrConflict) still holds for existing
	// callers), but is distinguished from a generic conflict so a caller
	// like settleRunRetrying can skip retrying a conflict that a bounded
	// retry loop can never clear -- only a FUTURE run's drain does -- and
	// divert immediately instead of burning its full retry budget first
	// (round-three reconciliation item 9, SR-S1/RD-S4).
	ErrRunHasQueuedInput = fmt.Errorf("%w: run has queued input pending drain", ErrConflict)
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
	// RunPaused means execution durably suspended after a promoted
	// checkpoint. Paused runs are nonterminal but hold no live fence: the
	// session remains reserved (see runs_session_active_unique_idx) until a
	// resume claims the run and moves it back to running.
	RunPaused RunStatus = "paused"
)

// Run records one admitted execution attempt before provider streaming starts.
type Run struct {
	ID            RunID
	SessionID     ID
	ParentRunID   RunID
	ParentMsgID   MessageID
	OwnerID       string
	ClaimToken    string
	LeaseUntil    time.Time
	Agent         string
	ProviderID    string
	ModelID       string
	ContextEpoch  EpochID
	Status        RunStatus
	Config        map[string]string
	ExtensionPlan ExtensionPlanDescriptor
	CreatedAt     time.Time
	StartedAt     time.Time
	FinishedAt    time.Time
	Error         string
}

// RunFence identifies the one execution currently authorized to mutate a run.
type RunFence struct {
	RunID      RunID
	ClaimToken string
}

// RunClaim requests atomic ownership of an expired nonterminal run. Stores use
// their own clock to compare expiry and stamp the returned lease deadline.
type RunClaim struct {
	RunID         RunID
	OwnerID       string
	ClaimToken    string
	LeaseDuration time.Duration
}

// Terminal reports whether the run no longer owns execution.
func (r Run) Terminal() bool {
	return r.Status == RunInterrupted || r.Status == RunFailed || r.Status == RunCompleted
}

// Paused reports whether the run is durably suspended awaiting resume. A
// paused run is nonterminal but has no live fence: only ClaimRun can move it
// back to running.
func (r Run) Paused() bool {
	return r.Status == RunPaused
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
	// PartReasoning stores model reasoning content when a provider exposes
	// it. This constant is also the PartKind for BlockKindReasoning content
	// blocks (see PartKindForBlock in content.go): both already share the
	// "reasoning" string, so no separate constant is declared for the block
	// variant.
	PartReasoning PartKind = "reasoning"
	// PartCompaction stores compaction request or summary metadata.
	PartCompaction PartKind = "compaction"
	// PartProviderState stores provider-private continuity data. Public history
	// projection and replay surfaces always omit this kind. Every part of
	// this kind on an active message is decoded as a strict, ordered
	// model.ProviderStateItem envelope by runtime's loadProviderHistory
	// (see runtime/provider_state.go), so no other payload shape may ever
	// share this kind.
	PartProviderState PartKind = "provider_state"
	// PartApprovalDecision stores a runtime-private decision-CAS record
	// guarding a one-time host approval decision (see adkApprovalBinding in
	// runtime/adk_approval.go). Like PartProviderState it is never exposed
	// as public content and is ignored by every history projection, but it
	// deliberately uses its own kind rather than PartProviderState: it can
	// live on the same message as a real PartProviderState continuity part
	// (both may sit alongside the public approval-request content block
	// that message durably commits), and its payload does not conform to
	// the strict ProviderStateItem envelope loadProviderHistory requires of
	// every PartProviderState part on an active message.
	PartApprovalDecision PartKind = "approval_decision"

	// The following PartKind constants persist the 19 non-reasoning
	// BlockKind values declared in content.go, one part kind per block kind,
	// using identical string values (BlockKindReasoning reuses PartReasoning
	// above). See PartKindForBlock / BlockKindForPart in content.go.
	PartUserInputText           PartKind = "user_input_text"
	PartUserInputImage          PartKind = "user_input_image"
	PartUserInputAudio          PartKind = "user_input_audio"
	PartUserInputVideo          PartKind = "user_input_video"
	PartUserInputFile           PartKind = "user_input_file"
	PartToolSearchResult        PartKind = "tool_search_result"
	PartAssistantGenText        PartKind = "assistant_gen_text"
	PartAssistantGenImage       PartKind = "assistant_gen_image"
	PartAssistantGenAudio       PartKind = "assistant_gen_audio"
	PartAssistantGenVideo       PartKind = "assistant_gen_video"
	PartFunctionToolCall        PartKind = "function_tool_call"
	PartFunctionToolResult      PartKind = "function_tool_result"
	PartServerToolCall          PartKind = "server_tool_call"
	PartServerToolResult        PartKind = "server_tool_result"
	PartMCPToolCall             PartKind = "mcp_tool_call"
	PartMCPToolResult           PartKind = "mcp_tool_result"
	PartMCPListToolsResult      PartKind = "mcp_list_tools_result"
	PartMCPToolApprovalRequest  PartKind = "mcp_tool_approval_request"
	PartMCPToolApprovalResponse PartKind = "mcp_tool_approval_response"

	// PartResponseMeta stores one durable ResponseMeta projection per
	// assistant message, ordered after all of that message's block parts.
	PartResponseMeta PartKind = "response_meta"
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

// ToolTransitionPhase is the durable uniqueness domain for tool lifecycle
// events. All terminal statuses intentionally share one phase.
type ToolTransitionPhase string

const (
	ToolTransitionPending  ToolTransitionPhase = "pending"
	ToolTransitionRunning  ToolTransitionPhase = "running"
	ToolTransitionTerminal ToolTransitionPhase = "terminal"
)

// ToolCall is the durable execution envelope for one tool invocation.
type ToolCall struct {
	ID              ToolCallID
	SessionID       ID
	RunID           RunID
	MessageID       MessageID
	RequestPartID   PartID
	ResultMessageID MessageID
	ResultPartID    PartID
	Name            string
	// RequestedName is the model-facing tool name as the model actually
	// called it: equal to Name when the model used the canonical name,
	// or the alias the model used when Name was resolved from an alias.
	// It is persisted so the function_tool_result sent back to the model
	// (and any replay of this call) can correlate on the name the model
	// itself used.
	RequestedName string
	Pattern       string
	Input         json.RawMessage
	Output        json.RawMessage
	Status        ToolCallStatus
	RetrySafe     bool
	Metadata      map[string]string
	ClaimedBy     string
	ClaimToken    string
	LeaseUntil    time.Time
	StartedAt     time.Time
	CompletedAt   time.Time
	Error         string
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
	Limit          int
}

// ReplayBatch returns messages and their ordered parts for history replay.
type ReplayBatch struct {
	Messages []Message
	Parts    []Part
	// PartOwnerMessageIDs contains the store-authoritative owner column for
	// each parallel Parts entry. Stores that cannot supply a separate owner may
	// leave it empty. Consumers must resolve the two valid shapes through
	// ResolveReplayPartOwners.
	PartOwnerMessageIDs []MessageID
	Next                ReplayCursor
}

// EventID identifies a durable runtime event record.
type EventID string

// EventRecord is the store-level event shape. Runtime adapters may project
// richer typed events into this durable envelope without making session depend
// on the runtime package.
type EventRecord struct {
	ID         EventID
	SessionID  ID
	RunID      RunID
	MessageID  MessageID
	PartID     PartID
	ToolCallID ToolCallID
	// ToolTransition identifies a canonical tool state transition. Only the
	// typed tool mutation methods may persist records with this field set.
	ToolTransition ToolTransitionPhase
	EpochID        EpochID
	// TurnID correlates an event to the durable turn that produced it, when
	// applicable. It is a record-JSON-only correlation field: no column or
	// index backs it.
	TurnID TurnID
	// AgentPath is the joined RunPath of the (sub)agent that produced this
	// event, when applicable. Record-JSON-only, like TurnID.
	AgentPath   string
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
	InputTokens int64
	// TotalTokens is the provider-reported total, which is not always
	// InputTokens + OutputTokens (reasoning tokens, cache accounting, or
	// provider-side rounding can make it differ). It is carried verbatim,
	// never derived.
	TotalTokens      int64
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
	ModelRequestReader
	WithinTx(ctx context.Context, fn func(context.Context, Store) error) error
	CreateSession(ctx context.Context, session Session) (Session, error)
	GetSession(ctx context.Context, id ID) (Session, error)
	UpdateSession(ctx context.Context, session Session) error
	SetSessionTitle(ctx context.Context, request SessionTitleRequest) (SessionTitleResult, error)
	// AdmitRun atomically creates a run and makes it the active owner for its
	// session. Every existing run ID returns ErrConflict; implementations return
	// ErrSessionBusy when another nonterminal run owns the session.
	AdmitRun(ctx context.Context, run Run, leaseDuration time.Duration) (Run, error)
	ClaimRun(ctx context.Context, claim RunClaim) (Run, error)
	// Execution returns a non-nil fenced mutation capability. Store failures are
	// reported by capability methods; nil is a store contract violation.
	Execution(fence RunFence) ExecutionStore
	GetRun(ctx context.Context, id RunID) (Run, error)
	ActiveRun(ctx context.Context, sessionID ID) (Run, error)
	ListUnfinishedRuns(ctx context.Context) ([]Run, error)
	ListMessages(ctx context.Context, sessionID ID, cursor ReplayCursor) (ReplayBatch, error)
	ListEvents(ctx context.Context, sessionID ID, cursor EventCursor) (EventBatch, error)
	GetToolCall(ctx context.Context, id ToolCallID) (ToolCall, error)
	ListUnfinishedToolCalls(ctx context.Context, runID RunID) ([]ToolCall, error)
	// EnqueueInbox durably admits one idempotent submission for a session.
	// Replaying the same IdempotencyKey with the same payload returns the
	// existing row; a different payload under the same key is ErrConflict.
	EnqueueInbox(ctx context.Context, item InboxItem, limits ContentLimits) (InboxItem, error)
	// EnqueueInboxForRun is EnqueueInbox, additionally checked -- atomically,
	// under the same session-row lock SettleRun's fenced terminal transition
	// takes -- against runID's current terminal status: if runID is already
	// terminal by the time this call's transaction acquires that lock, the
	// item is never persisted and ErrRunClosed is returned instead of a
	// silent acknowledgement. created reports whether this call durably
	// inserted a new row (false for an idempotent replay of an existing
	// item), so a caller pushing the result into a live loop can skip an
	// already-buffered/already-consumed ID instead of re-delivering it.
	EnqueueInboxForRun(ctx context.Context, runID RunID, item InboxItem, limits ContentLimits) (InboxItem, bool, error)
	ListInbox(ctx context.Context, sessionID ID, states []InboxState) ([]InboxItem, error)
	GetTurn(ctx context.Context, id TurnID) (Turn, error)
	ListTurns(ctx context.Context, runID RunID) ([]Turn, error)
	// ReadPromotedCheckpoint reads the latest promoted checkpoint revision for
	// a run without a fence: promotion is the durability boundary, not the
	// live claim.
	ReadPromotedCheckpoint(ctx context.Context, runID RunID) (Checkpoint, bool, error)
	// RetireRunCheckpoints deletes promoted or staged revisions at or below
	// upToRevision for a terminal run, where no fence is available. It is
	// idempotent and never deletes a revision above upToRevision.
	RetireRunCheckpoints(ctx context.Context, runID RunID, upToRevision int64) error
}

// ExecutionStore is the run-fenced mutation capability used after admission or
// resume. Implementations must verify the fence atomically with every write.
type ExecutionStore interface {
	WithinTx(ctx context.Context, fn func(context.Context, ExecutionStore) error) error
	StartRun(ctx context.Context, startedAt time.Time) (Run, error)
	RenewRunLease(ctx context.Context, leaseDuration time.Duration) (Run, error)
	// SettleRun applies a run's single terminal settlement under the fence.
	// It refuses RunCompleted while the session still has queued inbox
	// input (ErrRunHasQueuedInput) and, in the same transaction, forces
	// every turn still admitted/running/interrupted to TurnFailed with its
	// consumed/interrupted inbox rows carried forward as InboxInterrupted
	// (ApplyFailTurn), so no turn is left non-terminal under a terminal run.
	SettleRun(ctx context.Context, request SettleRunRequest) (RunSettlementResult, error)
	SetSessionTitle(ctx context.Context, request SessionTitleRequest) (SessionTitleResult, error)
	AppendMessage(ctx context.Context, message Message) (Message, error)
	FinalizeAssistantMessage(ctx context.Context, id MessageID) error
	AppendPart(ctx context.Context, part Part) (Part, error)
	UpdatePart(ctx context.Context, part Part) error
	AppendEvent(ctx context.Context, event EventRecord) (EventRecord, error)
	CreateToolCall(ctx context.Context, request CreateToolCallRequest) (ToolTransitionResult, error)
	ClaimToolCall(ctx context.Context, request ClaimToolCallRequest) (ToolTransitionResult, error)
	SettleToolCall(ctx context.Context, request SettleToolCallRequest) (ToolTransitionResult, error)
	StartContextEpoch(ctx context.Context, epoch ContextEpoch) (ContextEpoch, error)
	FinishContextEpoch(ctx context.Context, epoch ContextEpoch) error
	// AdmitTurn atomically admits the next-ordinal turn for the fenced run:
	// it appends the user messages/parts and assistant placeholder, creates
	// the turn row, claims the given queued inbox items into it (queued ->
	// consumed), and appends the canonical turn_started event.
	AdmitTurn(ctx context.Context, request AdmitTurnRequest) (AdmitTurnResult, error)
	// CompleteTurn atomically settles an admitted turn as completed and its
	// claimed inbox items as completed, without touching run status.
	CompleteTurn(ctx context.Context, request CompleteTurnRequest) (CompleteTurnResult, error)
	// InterruptTurn atomically settles an admitted turn as interrupted and
	// its claimed inbox items as interrupted, without touching run status.
	InterruptTurn(ctx context.Context, request InterruptTurnRequest) (InterruptTurnResult, error)
	// ReconcileInterruptedTurn atomically settles a turn a crashed process
	// left admitted/running as interrupted, carrying its consumed inbox
	// items forward as interrupted (see ReconcileInterruptedTurnRequest),
	// without touching run status.
	ReconcileInterruptedTurn(ctx context.Context, request ReconcileInterruptedTurnRequest) (ReconcileInterruptedTurnResult, error)
	// ResumeInterruptedTurn atomically resumes a TurnInterrupted turn under
	// the same TurnID for a fresh redrive (see ResumeInterruptedTurnRequest),
	// without touching run status.
	ResumeInterruptedTurn(ctx context.Context, request ResumeInterruptedTurnRequest) (ResumeInterruptedTurnResult, error)
	// StageCheckpoint inserts one unpromoted checkpoint revision under the
	// fence.
	StageCheckpoint(ctx context.Context, request StageCheckpointRequest) (Checkpoint, error)
	// PromotePause atomically promotes a staged checkpoint revision into the
	// durable pause boundary: it marks the revision promoted, interrupts the
	// given turn and inbox items, sets the run paused with no live lease, and
	// appends the run_paused event. After it commits, this fence can no
	// longer write (loadRunFence rejects paused runs).
	PromotePause(ctx context.Context, request PromotePauseRequest) (PromotePauseResult, error)
	// RetireCheckpoints deletes staged or promoted revisions at or below
	// upToRevision for the fenced (running) run. Idempotent; never deletes a
	// revision above upToRevision.
	RetireCheckpoints(ctx context.Context, upToRevision int64) error
	// RepauseRun atomically reverts this fence's claim back to paused, with
	// no live lease, keeping whatever checkpoint is currently promoted
	// unchanged (see RepauseRunRequest).
	RepauseRun(ctx context.Context, request RepauseRunRequest) (RepauseRunResult, error)
	ModelRequestWriter
}

// ContextEpochReader replays durable context epoch records for audit and
// compaction-aware history projection.
type ContextEpochReader interface {
	ListContextEpochs(ctx context.Context, sessionID ID) ([]ContextEpoch, error)
}
