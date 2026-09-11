package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/eino/adk"
	einoschema "github.com/cloudwego/eino/schema"
	einoobs "github.com/mattsp1290/eino-obs"

	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/internal/jsonobject"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
	"github.com/mattsp1290/eino-agent/watch"
)

var (
	// ErrInvalidOrchestrator reports a missing dependency or invalid request.
	ErrInvalidOrchestrator = errors.New("invalid orchestrator")
)

// IDGenerator creates durable identifiers for records owned by the
// orchestrator. Every minted ID must be unique across the whole durable
// store, including across processes and process restarts: a resumed or
// crash-reconciled run mints new rows into a session a different process
// already wrote (see runtime/interrupt.go's reconcileCrashedRun), and a
// colliding ID fails admission with session.ErrConflict. An implementation
// that restarts a counter per process does not satisfy this contract.
type IDGenerator interface {
	NewRunID() session.RunID
	NewMessageID() session.MessageID
	NewPartID() session.PartID
	// NewToolCallID mints a durable tool-call identity. Besides its normal
	// internal use, this exact value can reach the model provider on the
	// wire: publicizeToolCallIDs sends it verbatim as a call's provider-
	// facing id when (a) the provider left CallID empty, (b) the provider's
	// id was not valid UTF-8 or exceeded session.DiscoveryMaxIdentityBytes
	// (prepareToolCalls/validProviderCallID stores it as absent, the same as
	// case (a)), or (c) sending the provider's id would be ambiguous in this
	// outgoing request: an earlier call in request order already sends
	// that string, or the string is the durable id of any call in the
	// request (see publicizeToolCallIDs's "Duplicate provider ids" doc). An
	// implementation whose minted format would violate a provider's own id
	// rules (for example Mistral, which requires exactly 9 alphanumeric
	// characters) can surface that constraint to such a provider.
	NewToolCallID() session.ToolCallID
	NewEventID() session.EventID
	NewEpochID() session.EpochID
	// NewTurnID mints a durable identity for one admitted turn.
	NewTurnID() session.TurnID
	// NewInboxID mints a durable identity for one queued inbox submission.
	NewInboxID() session.InboxID
	// NewInvocationID mints a unique identity for one physical model
	// dispatch (a fresh generate/stream call, including retries, failover
	// attempts and child-agent dispatches sharing a run).
	NewInvocationID() string
}

// StreamingOrchestrator executes admitted runs against Eino model streams.
type StreamingOrchestrator struct {
	sessionObserver         *watch.Service
	configured              bool
	store                   session.Store
	model                   model.Resolver
	plans                   RunPlanProvider
	events                  EventSink
	ids                     IDGenerator
	clock                   func() time.Time
	ownerIDValue            string
	trace                   agentcontext.TraceContext
	attemptsValue           int
	toolTurnsValue          int
	queueSize               int
	leaseValue              time.Duration
	history                 history.Options
	permissions             permissions.Policy
	observer                *einoobs.Observer
	modelRequestSafeOptions []string
	modelRequestMaxBytes    int
	contentLimits           session.ContentLimits
	streamLimits            StreamLimits

	// loopsMu guards loops, this process's registry of live TurnLoops keyed
	// by run ID (see runtime/turn_loop.go). It lets Enqueue/Stop/Interrupt
	// reach a loop this process owns without a durable round trip.
	loopsMu sync.Mutex
	loops   map[session.RunID]*liveLoop
}

// Start admits and asynchronously executes one streaming turn.
func (o *StreamingOrchestrator) Start(ctx context.Context, request Request) (Handle, error) {
	if err := o.validateConfigured(); err != nil {
		return nil, err
	}
	if err := rejectCallerContentBlockIDs(request.Message.Blocks); err != nil {
		return nil, err
	}
	blocks, err := assignContentBlockIDs(request.Message.Blocks, o.ids)
	if err != nil {
		return nil, err
	}
	request.Message.Blocks = blocks
	if err := o.validate(request); err != nil {
		return nil, err
	}
	plan, err := o.acquireRunPlan(ctx, RunPlanRequest{SessionID: request.SessionID, Config: request.Config})
	if err != nil {
		return nil, err
	}
	ownershipTransferred := false
	defer func() {
		if !ownershipTransferred {
			plan.release()
		}
	}()
	resolved, err := o.model.Resolve(ctx, request.Config.Model, model.Runtime{
		Directory: request.Config.Metadata["workspace_root"],
		Options:   cloneStringMap(request.Config.Agent.Options),
	})
	if err != nil {
		return nil, err
	}
	if err := model.ValidateResolved(request.Config.Model, resolved); err != nil {
		return nil, err
	}
	ids := admissionIDs{
		SessionID:          request.SessionID,
		RunID:              o.ids.NewRunID(),
		UserMessageID:      o.ids.NewMessageID(),
		UserPartIDs:        partIDsFromBlocks(request.Message.Blocks, o.ids),
		AssistantMessageID: o.ids.NewMessageID(),
		ContextEpochID:     o.ids.NewEpochID(),
		EventID:            o.ids.NewEventID(),
		RunClaimToken:      string(o.ids.NewEventID()),
		TurnID:             o.ids.NewTurnID(),
		TurnStartedEventID: o.ids.NewEventID(),
	}
	// history.Options.ContentLimits must track the orchestrator's configured
	// content bounds so decoding never diverges from the bounds admission
	// encoded under (WithContentLimits can raise them above the durable
	// defaults). WithHistory has no visibility into WithContentLimits at
	// option-application time, so the override happens here instead of
	// being baked into o.history.
	historyOptions := o.history
	historyOptions.ContentLimits = o.contentLimits
	admitter := o.admitter()
	admitted, err := admitter.admit(ctx, admissionRequest{
		IDs:           ids,
		UserMessage:   request.Message,
		History:       historyOptions,
		Config:        request.Config,
		Model:         resolved,
		OwnerID:       o.ownerID(),
		LeaseDuration: o.lease(),
		Metadata:      request.Metadata,
		ExtensionPlan: plan.Descriptor(),
		ContentLimits: o.contentLimits,
	})
	if err != nil {
		return nil, err
	}
	execution := newRunExecution(o, plan, admitted.Run)
	execution.seedDurableMessageFloor(admitted.AssistantMessage.CreatedAt)
	execution.publishPersistedWithNotificationContext(ctx, ctx, admitted.Event)
	extension.Notify(plan.dispatch, ctx, RunAdmittedPoint, RunAdmittedNotice{
		SessionID: admitted.Session.ID, RunID: admitted.Run.ID, Plan: plan.Descriptor(),
		Metadata: boundedTurnMetadata(admitted.Snapshot), Time: admitted.Snapshot.CreatedAt,
	})
	runCtx, cancel := context.WithCancel(ctx)
	handle := &turnLoopHandle{runID: admitted.Run.ID, host: o, done: make(chan Result, 1), pause: make(chan PauseInfo, 1), cancel: cancel}
	coordinator := &turnLoopCoordinator{
		host: o, execution: execution, plan: plan, sessionID: admitted.Session.ID, runID: admitted.Run.ID,
		config: request.Config, resolved: resolved, historyOptions: historyOptions, epochID: admitted.Run.ContextEpoch,
		ordinal: admitted.Turn.Ordinal,
	}
	ownershipTransferred = true
	checkpoints := newAdkCheckpointStore(o, execution, plan, admitted.Run.ID)
	coordinator.checkpoints = checkpoints
	// Seed currentTurnID synchronously with the already-admitted first turn,
	// before this run's TurnLoop is even constructed (round-six
	// reconciliation item 8/CP-S1): setEngine otherwise only keeps it in
	// sync starting from the first GenInput call (which consumes
	// firstTurnSentinelID), leaving a window where an upstream Set racing
	// ahead of that first call would see an empty currentTurnID and hard-
	// fail staging, turning a resumable pause into a non-pause.
	checkpoints.setCurrentTurnID(admitted.Turn.ID)
	entry := o.prepareTurnLoop(coordinator, checkpoints)
	// Pushed synchronously, before Run: TurnLoop buffers a Push issued
	// before Run() and processes it in order once Run is called (its
	// "permissive API"), which is the only way to guarantee this sentinel
	// is ordered ahead of any Enqueue a caller races immediately after
	// Start returns -- Run/GenInput itself must stay on the goroutine below
	// since it blocks for the run's lifetime.
	entry.loop.Push(firstTurnSentinelID)
	// A durably queued inbox item accepted by an earlier Enqueue call that
	// found no live loop for this session's (now-terminal) prior run must
	// still be drained into this fresh loop, or it stays queued forever
	// (see Enqueue's doc comment). Pushed after the sentinel so it is
	// admitted as this run's second turn, not confused with the first.
	// Best-effort: a listing failure here must not fail Start (the run is
	// already durably admitted); the item(s) simply remain queued for the
	// next successful Start/ResumeRun to drain.
	if queued, err := o.drainQueuedInbox(ctx, admitted.Session.ID); err == nil {
		for _, id := range queued {
			entry.loop.Push(id)
		}
	}
	go o.runFreshTurnLoop(runCtx, execution, entry, checkpoints, admitted, handle)
	return handle, nil
}

// runFreshTurnLoop prepares the already-admitted first turn's TurnSnapshot
// (extension transforms, tool resolution -- the same pipeline every later
// turn uses) and drives the run's TurnLoop, seeding it with that turn via
// the sentinel-consuming first GenInput call (see firstTurnSentinelID).
func (o *StreamingOrchestrator) runFreshTurnLoop(ctx context.Context, execution *runExecution, entry *liveLoop, checkpoints *adkCheckpointStore, admitted admittedRun, handle *turnLoopHandle) {
	coordinator := entry.coordinator
	defer execution.release()
	// Handle.Interrupt cancels this run's context (and issues an
	// entry.loop.Stop) the instant it is called, unconditionally, so that it
	// wins even when it races ahead of this goroutine's own startup --
	// see turnLoopHandle.Interrupt's doc comment. But TurnLoop.Run(ctx),
	// given an already-canceled ctx before it ever dispatches anything, has
	// nothing "in flight" to interrupt and exits with ExitReason == nil
	// (ordinary empty completion), not an interrupt/cancel error -- which
	// finishTurnLoop's default branch would otherwise settle as a spurious
	// RunCompleted with no assistant output. Catch that race here, before
	// ever handing off to ADK, and settle interrupted directly.
	if err := ctx.Err(); err != nil {
		result := Result{RunID: admitted.Run.ID, MessageID: admitted.AssistantMessage.ID, Status: statusForError(err), Interrupted: errors.Is(err, context.Canceled), Error: err}
		o.observeError(context.WithoutCancel(ctx), admitted.Snapshot, admitted.AssistantMessage.ID, "provider_stream", err)
		o.settleFreshFailure(ctx, execution, result)
		o.unregisterLoop(admitted.Run.ID)
		handle.done <- result
		close(handle.done)
		close(handle.pause)
		return
	}
	runCtx := execution.startLease(ctx, o.lease())
	{
		decision, err := extension.EvaluateGate(execution.dispatch(), runCtx, RunBeforeExecutePoint, RunGateInput{
			SessionID: admitted.Run.SessionID, RunID: admitted.Run.ID, ProviderID: admitted.Run.ProviderID, ModelID: admitted.Run.ModelID,
		})
		if err != nil || decision.Kind == RunReject {
			result := Result{RunID: admitted.Run.ID, MessageID: admitted.AssistantMessage.ID, Status: session.RunFailed, Error: err}
			if err == nil {
				result.Error = model.Error{Code: decision.Code, Message: decision.Message, Cause: model.ErrProviderRejected}
			}
			o.settleFreshFailure(runCtx, execution, result)
			o.unregisterLoop(admitted.Run.ID)
			handle.done <- result
			close(handle.done)
			close(handle.pause)
			return
		}
	}
	startedAt := o.now()
	observed := o.startObservedRun(runCtx, admitted.Run, admitted.AssistantMessage.ID, startedAt)
	started, err := execution.store.StartRun(runCtx, startedAt)
	if err != nil {
		result := Result{RunID: admitted.Run.ID, MessageID: admitted.AssistantMessage.ID, Status: session.RunFailed, Error: err}
		o.settleFreshFailure(runCtx, execution, result)
		o.finishObservedRun(observed, result, o.now())
		o.unregisterLoop(admitted.Run.ID)
		handle.done <- result
		close(handle.done)
		close(handle.pause)
		return
	}
	extension.Notify(execution.dispatch(), runCtx, RunStartedPoint, RunStartedNotice{SessionID: started.SessionID, RunID: started.ID, Time: started.StartedAt})
	baseMessageCount := len(admitted.Snapshot.Messages)
	snapshot, err := o.prepareSnapshot(runCtx, execution, admitted.Snapshot)
	if err != nil {
		result := Result{RunID: admitted.Run.ID, MessageID: admitted.AssistantMessage.ID, Status: statusForError(err), Error: err}
		o.settleFreshFailure(runCtx, execution, result)
		o.finishObservedRun(observed, result, o.now())
		o.unregisterLoop(admitted.Run.ID)
		handle.done <- result
		close(handle.done)
		close(handle.pause)
		return
	}
	execution.seedDiscovered(discoveredToolsFromMessages(snapshot.Messages))
	coordinator.setEngine(nil)
	coordinator.mu.Lock()
	// admitted.HistoryOptions (not coordinator.historyOptions, which stays
	// the unresolved host template so later turns on this run re-resolve
	// fresh -- see resolveTurnHistoryOptions) is what admitted.Snapshot's
	// projection and baseMessageCount were actually computed against.
	coordinator.firstTurnEngine = &adkEngine{host: o, execution: execution, plan: coordinator.plan, snapshot: snapshot, turn: admitted.Turn, assistantMessageID: admitted.AssistantMessage.ID, historyOptions: admitted.HistoryOptions, baseMessageCount: baseMessageCount}
	coordinator.mu.Unlock()
	o.runTurnLoop(runCtx, entry, checkpoints, nil, handle.done, handle.pause, func(result Result) {
		o.finishObservedRun(observed, result, o.now())
	})
}

// settleFreshFailure settles a run that failed before its TurnLoop ever
// started (a rejected pre-execute gate or a StartRun failure).
func (o *StreamingOrchestrator) settleFreshFailure(ctx context.Context, execution *runExecution, result Result) {
	_ = execution.stopLease()
	settlement := session.RunSettlement{Status: result.Status, FinishedAt: o.now()}
	if result.Error != nil {
		settlement.Error = result.Error.Error()
	}
	committed, err := execution.store.SettleRun(context.WithoutCancel(ctx), session.SettleRunRequest{
		Settlement: settlement, Event: session.RunSettlementEvent{ID: o.ids.NewEventID(), MessageID: result.MessageID},
	})
	if err == nil {
		execution.publishPersisted(context.WithoutCancel(ctx), committed.Event)
	}
}

// Status returns the current active run for a session.
func (o *StreamingOrchestrator) Status(ctx context.Context, sessionID session.ID) (session.Run, error) {
	if err := o.validateConfigured(); err != nil {
		return session.Run{}, err
	}
	if sessionID == "" {
		return session.Run{}, fmt.Errorf("%w: session id required", ErrInvalidOrchestrator)
	}
	return o.store.ActiveRun(ctx, sessionID)
}

type runLifecycle struct {
	run         session.Run
	result      Result
	observed    observedRun
	metadata    BoundedTurnMetadata
	startedAt   time.Time
	usage       model.Usage
	panicPrefix string
}

func (o *StreamingOrchestrator) executeLifecycle(ctx context.Context, execution *runExecution, lifecycle *runLifecycle, done chan<- Result, body func(context.Context)) {
	defer close(done)
	defer func() { done <- lifecycle.result }()
	defer execution.release()
	defer func() {
		if recovered := recover(); recovered != nil {
			lifecycle.result.Status = session.RunFailed
			lifecycle.result.Error = fmt.Errorf("%s: %v", lifecycle.panicPrefix, recovered)
		}
		lifecycle.result.Usage = runtimeUsage(lifecycle.usage)
		result, settled := o.settleRun(ctx, execution, lifecycle.run, lifecycle.result)
		lifecycle.result = result
		o.finishObservedRun(lifecycle.observed, result, o.now())
		if settled {
			extension.Notify(execution.dispatch(), context.WithoutCancel(ctx), RunSettledPoint, RunSettledNotice{
				SessionID: lifecycle.run.SessionID,
				Result:    result,
				Metadata:  lifecycle.metadata,
				Duration:  o.now().Sub(lifecycle.startedAt),
				Error:     classifyExtensionError(result.Error),
			})
		}
	}()
	ctx = execution.startLease(ctx, o.lease())
	body(ctx)
}

func (o *StreamingOrchestrator) prepareSnapshot(ctx context.Context, execution *runExecution, snapshot TurnSnapshot) (TurnSnapshot, error) {
	assembly := contextAssembly{SessionID: snapshot.SessionID, RunID: snapshot.RunID, EpochID: snapshot.EpochID, Metadata: boundedTurnMetadata(snapshot), Base: snapshot.Messages}
	assembled, err := extension.ApplyTransforms(execution.dispatch(), ctx, contextAssemblePoint, assembly)
	if err != nil {
		return TurnSnapshot{}, err
	}
	materialized, err := materializeContextAssemblyWithMapping(assembled)
	if err != nil {
		return TurnSnapshot{}, err
	}
	snapshot.Messages = materialized.Messages
	states, err := cloneRuntimeProviderState(snapshot.providerState)
	if err != nil {
		return TurnSnapshot{}, err
	}
	for index := range states {
		baseIndex := states[index].MessageIndex
		if baseIndex < 0 || baseIndex >= len(materialized.BaseToFinal) {
			return TurnSnapshot{}, runtimeProviderStateError(model.ErrProviderStateMismatch)
		}
		finalIndex := materialized.BaseToFinal[baseIndex]
		if finalIndex < 0 || finalIndex >= len(snapshot.Messages) || snapshot.Messages[finalIndex] == nil || snapshot.Messages[finalIndex].Role != einoschema.AgenticRoleTypeAssistant {
			return TurnSnapshot{}, runtimeProviderStateError(model.ErrProviderStateMismatch)
		}
		states[index].MessageIndex = finalIndex
	}
	snapshot.providerState = states
	if len(execution.plan.tools.capabilities) != 0 {
		planned, err := execution.plan.ResolveTools(ctx, NewToolScopeContext(snapshot))
		if err != nil {
			return TurnSnapshot{}, err
		}
		seen := make(map[string]bool, len(snapshot.Tools)+len(planned))
		for _, tool := range snapshot.Tools {
			seen[tool.Name] = true
		}
		for _, tool := range planned {
			if seen[tool.Name] {
				return TurnSnapshot{}, fmt.Errorf("duplicate effective tool %q", tool.Name)
			}
			seen[tool.Name] = true
			snapshot.Tools = append(snapshot.Tools, tool)
		}
	}
	err = extension.RunHooks(execution.dispatch(), ctx, TurnPreparePoint, boundedTurnMetadata(snapshot))
	if err != nil {
		return TurnSnapshot{}, err
	}
	if execution.plan != nil {
		snapshot.ToolSearch = execution.plan.ToolSearch()
	}
	o.observeToolsResolved(ctx, snapshot, snapshot.Tools)
	return snapshot, nil
}

func (o *StreamingOrchestrator) executeToolOutcome(ctx context.Context, execution *runExecution, tool Tool, call ToolCall) toolOutcome {
	guard, guardErr := evaluateToolGuards(ctx, execution.plan, tool, call)
	if guardErr != nil {
		return newToolOutcome(call, ToolResult{}, toolPermissionAllowed, guardErr)
	}
	if guard.Decision == ToolGuardDeny {
		result := modelVisiblePermissionResult("denied", guard.Message)
		return newToolOutcome(call, result, toolPermissionDenied, nil)
	}
	wrapped, cloneErr := cloneToolChecked(tool)
	if cloneErr != nil {
		return newToolOutcome(call, ToolResult{}, toolPermissionAllowed, cloneErr)
	}
	wrapped.Executor = runtimeToolExecutorFunc(func(ctx context.Context, call ToolCall) (ToolResult, error) {
		return extension.InvokeAround(execution.dispatch(), ctx, ToolExecutePoint, ToolExecution{Tool: extensionTool(tool), Call: extensionToolCall(call)}, func(ctx context.Context) (ToolResult, error) {
			concrete := cloneToolCall(call)
			if tool.AllowSessionTitle {
				concrete.SessionTitle = boundSessionTitleWriter{store: execution.store, sessionID: execution.sessionID, workspaceID: execution.workspaceID}
			}
			return tool.Executor.Execute(ctx, concrete)
		})
	})
	permission, permissionErr := executeToolWithPermissions(ctx, wrapped, call, o.permissions)
	return newToolOutcome(call, permission.Result, permission.State, permissionErr)
}

func newToolOutcome(call ToolCall, result ToolResult, permission toolPermissionState, rawErr error) toolOutcome {
	return finalizeToolOutcome(toolOutcome{Call: extensionToolCall(call), Result: result, RawError: rawErr, Permission: permission})
}

func finalizeToolOutcome(outcome toolOutcome) toolOutcome {
	outcome.Call = extensionToolCall(outcome.Call)
	outcome.Result = protectPermissionResult(outcome.Result, outcome.Permission)
	outcome.Disposition = dispositionForOutcome(outcome.Permission, outcome.RawError)
	outcome.Error = classifyExtensionError(outcome.RawError)
	return outcome
}

func (o *StreamingOrchestrator) transformToolOutcome(ctx context.Context, execution *runExecution, outcome toolOutcome) toolOutcome {
	input := ToolResultTransform{ToolName: outcome.Call.Name, Call: extensionToolCall(outcome.Call), Result: cloneRuntimeToolResult(outcome.Result)}
	transformed, err := extension.ApplyTransforms(execution.dispatch(), ctx, ToolResultTransformPoint, input)
	if err != nil {
		outcome.RawError = errors.Join(outcome.RawError, err)
		return finalizeToolOutcome(outcome)
	}
	outcome.Result = transformed.Result
	return finalizeToolOutcome(outcome)
}

func dispositionForOutcome(permission toolPermissionState, err error) ToolDisposition {
	if err != nil {
		return dispositionForError(err)
	}
	switch permission {
	case toolPermissionDenied:
		return ToolDenied
	case toolPermissionApprovalRequired:
		return ToolApprovalRequired
	case toolPermissionInterrupted:
		return ToolInterrupted
	default:
		return ToolExecuted
	}
}

func dispositionForError(err error) ToolDisposition {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ToolInterrupted
	}
	return ToolFailed
}

type runtimeToolExecutorFunc func(context.Context, ToolCall) (ToolResult, error)

func (f runtimeToolExecutorFunc) Execute(ctx context.Context, call ToolCall) (ToolResult, error) {
	return f(ctx, call)
}

func (o *StreamingOrchestrator) settleRun(ctx context.Context, execution *runExecution, run session.Run, result Result) (Result, bool) {
	if leaseErr := execution.stopLease(); leaseErr != nil {
		result.Status = session.RunFailed
		result.Error = errors.Join(result.Error, leaseErr)
	}
	if result.Status == "" {
		result.Status = session.RunCompleted
	}
	settlement := session.RunSettlement{Status: result.Status, FinishedAt: o.now()}
	if result.Error != nil {
		settlement.Error = result.Error.Error()
	}
	settled := true
	committed, err := execution.store.SettleRun(context.WithoutCancel(ctx), session.SettleRunRequest{Settlement: settlement, Event: o.finalRunEvent(result)})
	if err != nil {
		settled = false
		result.Status = session.RunFailed
		result.Error = errors.Join(result.Error, err)
	} else {
		o.publishRunFinished(ctx, execution, committed.Event)
	}
	return result, settled
}

func (o *StreamingOrchestrator) finalRunEvent(result Result) session.RunSettlementEvent {
	eventErr := eventError(result.Error)
	return session.RunSettlementEvent{
		ID: o.ids.NewEventID(), MessageID: result.MessageID,
		Usage: session.Usage{
			InputTokens: result.Usage.InputTokens, TotalTokens: result.Usage.TotalTokens, OutputTokens: result.Usage.OutputTokens,
			ReasoningTokens: result.Usage.ReasoningTokens, CacheReadTokens: result.Usage.CacheReadTokens, CacheWriteTokens: result.Usage.CacheWriteTokens, Cost: result.Usage.Cost,
		},
		ErrorCode: eventErr.Code, Retryable: eventErr.Retryable,
	}
}

func (o *StreamingOrchestrator) publishRunFinished(ctx context.Context, execution *runExecution, event session.EventRecord) {
	execution.publishPersisted(context.WithoutCancel(ctx), event)
}

func (o *StreamingOrchestrator) validate(request Request) error {
	if err := o.validateConfigured(); err != nil {
		return err
	}
	if request.SessionID == "" {
		return fmt.Errorf("%w: session id required", ErrInvalidOrchestrator)
	}
	if len(request.Message.Blocks) == 0 {
		return fmt.Errorf("%w: message blocks required", ErrInvalidOrchestrator)
	}
	content := session.Content{Role: session.RoleUser, Blocks: request.Message.Blocks}
	if err := content.Validate(o.contentLimits); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidOrchestrator, err)
	}
	if !hasNonEmptyTextOrMediaBlock(request.Message.Blocks) {
		return fmt.Errorf("%w: message requires non-empty text or media content", ErrInvalidOrchestrator)
	}
	if err := rejectNonCallerBlocks(request.Message.Blocks); err != nil {
		return err
	}
	return nil
}

// rejectNonCallerBlocks enforces that a caller's user submission carries only
// block kinds a real caller can legitimately author. session.Content.Validate
// permits BlockKindFunctionToolResult and BlockKindToolSearchResult under
// RoleUser because both are valid durable content on a user-role message,
// but their only legitimate writer is this runtime's own tool settlement
// path (persistToolSettlement / settleToolCall), never the public Start
// API. Without this check, a caller could hand-author a
// tool_search_result block and seed execution.discovered
// (discoveredToolsFromMessages/discoveredToolsFromHistory) with no claim,
// no guard evaluation, and no durable tool-call record behind it.
func rejectNonCallerBlocks(blocks []session.ContentBlock) error {
	for _, b := range blocks {
		switch b.Kind {
		case session.BlockKindUserInputText, session.BlockKindUserInputImage, session.BlockKindUserInputAudio,
			session.BlockKindUserInputVideo, session.BlockKindUserInputFile, session.BlockKindMCPToolApprovalResponse:
		default:
			return fmt.Errorf("%w: user submission may not carry a %q block", ErrInvalidOrchestrator, b.Kind)
		}
	}
	return nil
}

// rejectCallerContentBlockIDs enforces that content block identity is
// entirely runtime-owned (see UserMessage's doc comment): a caller must not
// supply its own ContentBlock.ID. It runs on the caller's original blocks,
// before assignContentBlockIDs clones and overwrites them, so it fails
// loudly instead of silently discarding caller-supplied identity.
func rejectCallerContentBlockIDs(blocks []session.ContentBlock) error {
	for _, b := range blocks {
		if b.ID != "" {
			return fmt.Errorf("%w: content block ID is runtime-assigned; callers must leave ContentBlock.ID empty", ErrInvalidOrchestrator)
		}
	}
	return nil
}

// assignContentBlockIDs returns a deep copy of blocks with a fresh durable
// block ID minted for every block, unconditionally. It never mutates the
// caller's slice or its elements: the copy is a full session.Content.Clone,
// not just a shallow slice copy, so the caller's *TextBlock, *MediaBlock, and
// json.RawMessage pointers never reach validation or the admission
// transaction. ids may be nil, in which case block IDs are left empty
// (validate then rejects them).
func assignContentBlockIDs(blocks []session.ContentBlock, ids IDGenerator) ([]session.ContentBlock, error) {
	if len(blocks) == 0 {
		return blocks, nil
	}
	cloned, err := session.Content{Role: session.RoleUser, Blocks: blocks}.Clone()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidOrchestrator, err)
	}
	out := cloned.Blocks
	if ids == nil {
		return out, nil
	}
	for i := range out {
		out[i].ID = string(ids.NewPartID())
	}
	return out, nil
}

// partIDsFromBlocks mints one durable Part ID per block, independent of the
// block's own ContentBlock.ID. Block identity and Part identity are separate
// concerns: parts.id is a store-wide UNIQUE primary key, so reusing a block
// ID as a Part ID would let caller-influenced values collide with unrelated
// parts. Both identifiers are minted from the same IDGenerator, but
// independently.
func partIDsFromBlocks(blocks []session.ContentBlock, ids IDGenerator) []session.PartID {
	if len(blocks) == 0 {
		return nil
	}
	out := make([]session.PartID, len(blocks))
	for i := range blocks {
		out[i] = ids.NewPartID()
	}
	return out
}

// hasNonEmptyTextOrMediaBlock reports whether blocks contains at least one
// non-blank user_input_text block or one media block (image/audio/video/
// file). A submission with only blank text is rejected, matching prior
// plain-text behavior.
func hasNonEmptyTextOrMediaBlock(blocks []session.ContentBlock) bool {
	for _, b := range blocks {
		switch b.Kind {
		case session.BlockKindUserInputText:
			if b.Text != nil && strings.TrimSpace(b.Text.Text) != "" {
				return true
			}
		case session.BlockKindUserInputImage, session.BlockKindUserInputAudio, session.BlockKindUserInputVideo, session.BlockKindUserInputFile:
			return true
		}
	}
	return false
}

func (o *StreamingOrchestrator) validateConfigured() error {
	if o == nil || !o.configured {
		return fmt.Errorf("%w: use NewStreamingOrchestrator", ErrInvalidOrchestrator)
	}
	return nil
}

func (o *StreamingOrchestrator) admitter() admitter {
	return admitter{Store: o.store, Clock: o.clock}
}

func (o *StreamingOrchestrator) now() time.Time {
	return o.clock().UTC()
}

func (o *StreamingOrchestrator) ownerID() string {
	return o.ownerIDValue
}

func (o *StreamingOrchestrator) attempts() int {
	return o.attemptsValue
}

func (o *StreamingOrchestrator) toolTurns() int {
	return o.toolTurnsValue
}

func (o *StreamingOrchestrator) lease() time.Duration {
	return o.leaseValue
}

// unwrapRetryExhausted returns err's original per-attempt cause when err is
// (or wraps, e.g. inside ADK's compose.internalError node-path envelope) an
// *adk.RetryExhaustedError. That type's own Unwrap() deliberately returns
// the fixed adk.ErrExceedMaxRetries sentinel, not LastErr, so a standard
// errors.Is/errors.As walk down a retry-exhausted terminal error never
// reaches the original provider error (e.g. a model.Error carrying a
// provider error code) -- callers that need to classify/report on the
// underlying cause (not just "retries were exhausted") must unwrap here
// first. Returns err unchanged when it is not a RetryExhaustedError.
func unwrapRetryExhausted(err error) error {
	var exhausted *adk.RetryExhaustedError
	if errors.As(err, &exhausted) && exhausted.LastErr != nil {
		return exhausted.LastErr
	}
	return err
}

func retryable(err error) bool {
	var providerErr model.Error
	return errors.As(unwrapRetryExhausted(err), &providerErr) && providerErr.Retryable
}

func statusForError(err error) session.RunStatus {
	if errors.Is(err, context.Canceled) {
		return session.RunInterrupted
	}
	return session.RunFailed
}

func eventError(err error) session.EventError {
	if err == nil {
		return session.EventError{}
	}
	var providerErr model.Error
	if errors.As(err, &providerErr) {
		return session.EventError{Code: providerErr.Code, Message: providerErr.Message, Retryable: providerErr.Retryable}
	}
	return session.EventError{Message: err.Error()}
}

func normalizedToolArguments(arguments string) (json.RawMessage, error) {
	if arguments == "" {
		return json.RawMessage(`{}`), nil
	}
	raw, err := canonicalToolObject(json.RawMessage(arguments))
	if err != nil {
		return nil, model.Error{
			Code:    "malformed_provider_tool_call",
			Message: "provider returned tool call arguments that are not a JSON object",
			Cause:   model.ErrProviderRejected,
		}
	}
	return raw, nil
}

func canonicalToolObject(raw json.RawMessage) (json.RawMessage, error) {
	object, err := jsonobject.Decode(raw)
	if err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

// errToolCallIDUnresolved wraps a publicizeToolCallIDs failure that cannot
// proceed for a deterministic, durable-consistency reason: either the id
// lookup itself returned session.ErrNotFound (a function_tool_call/
// function_tool_result/tool_search_result block's durable CallID has no
// tool_calls row) or session.ErrConflict (the row exists but under a
// conflicting identity), or -- a separate case, not a lookup error -- the
// resolved row belongs to a different session than the one being dispatched
// for (the cross-session check). It deliberately does NOT wrap any other
// store.GetToolCall error (a transient read failure, for example): those
// are returned without the sentinel (still wrapped with %w for the cause)
// and stay retryable/failover-eligible under the run's normal policy,
// exactly like every other store error in begin. Only
// ErrNotFound/ErrConflict are worth failing closed over: through the public
// API they can only happen from direct store writes or imported history --
// rejectNonCallerBlocks refuses caller-authored tool blocks, and context
// contributions are text-only -- so they are genuinely durable-consistency
// failures, not transient ones. Failing the dispatch closed for those is
// deliberate; defaultShouldRetry/defaultShouldFailover both refuse to retry
// or fail over an error matching this sentinel (see their doc comments), so a
// deterministic not-found does not burn every retry/failover attempt.
//
// One ErrNotFound case is NOT failed closed: a call ID this cycle's
// authorizedRewriteSet already vouches for (patchtoolcalls patching a
// genuinely dangling, never-admitted call -- see
// publicizeToolCallIDs's authorized parameter and
// NewPatchToolCallsHandlerFactory's doc comment) is a sanctioned, expected
// missing row, not corrupt history, so it falls back to the durable id on
// the wire instead of reaching this sentinel at all.
var errToolCallIDUnresolved = errors.New("tool call id could not be resolved to a provider-facing identity")

// toolCallIDCache is a per-turn cache of the durable-id -> provider-facing-id
// mapping publicizeToolCallIDs resolves via store.GetToolCall.
// session.ToolCall.ProviderCallID never changes once CreateToolCall commits,
// so this mapping is immutable for the lifetime of the cache; sharing one
// across every dispatch in a turn (adkEngine is turn-scoped -- see its doc
// comment) removes the repeated point-query cost a long ReAct tool loop
// would otherwise pay resolving the same, already-resolved calls again on
// every physical dispatch as the projected history grows (see
// publicizeToolCallIDs's doc comment). Safe for concurrent use: a turn's
// engine can be shared by more than one *adkModel (a retry/failover attempt
// via resolvedOverride).
type toolCallIDCache struct {
	mu       sync.Mutex
	resolved map[session.ToolCallID]string
}

func (c *toolCallIDCache) get(id session.ToolCallID) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	public, ok := c.resolved[id]
	return public, ok
}

func (c *toolCallIDCache) put(id session.ToolCallID, public string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.resolved == nil {
		c.resolved = make(map[session.ToolCallID]string)
	}
	c.resolved[id] = public
}

// publicizeToolCallIDs returns a shallow copy of messages with every
// function_tool_call/function_tool_result/tool_search_result block's CallID
// rewritten from its durable, store-wide-unique identity
// (session.ToolCall.ID, minted by prepareToolCalls) to the provider-facing
// identity that call was originally dispatched under
// (session.ToolCall.ProviderCallID) -- except when sending that string would
// collide with another call's wire id in this same outgoing request (see
// "Duplicate provider ids" below), in which case that call keeps its durable
// id instead.
//
// This is the ONLY point in the pipeline where that substitution happens,
// applied exactly once per physical dispatch (adkModel.begin, before any
// other side effect -- so the audited/ledgered request and the wire request
// are the same bytes; see adkModel.begin's doc comment). Everywhere else --
// ADK's own tool dispatch via compose.GetToolCallID, this package's
// store.GetToolCall lookups (adk_execution.go, adk_approval.go,
// interrupt.go's resume/crash-reconciliation replay), tool search --
// keys off the durable, always-unique ID uniformly, exactly as it did before
// this call ID could diverge from what the provider sent. Only the literal
// wire payload handed to the model provider must show the provider back the
// id it minted (or, when the provider left CallID empty, the same durable ID
// it was assigned, since there is no separate provider id to preserve) so a
// provider that pairs its own call/result ids by value can still recognize
// its own history -- including a provider that reissues the same indexed id
// (e.g. "call_0") across unrelated turns, which is exactly what makes the
// durable ID unsafe to reuse verbatim (see session.ToolCall.ProviderCallID).
//
// Duplicate provider ids: every distinct durable id referenced anywhere in
// this outgoing request (across every message, not just the current turn's
// own calls) is processed in request order -- the order its first block
// appears in messages. A call sends its resolved provider id on the wire
// only if (a) no earlier call in this same request has already claimed that
// exact string, and (b) the string is not the durable id of any call
// referenced in this request (every durable id is reserved for its own call
// from the start, so no provider id can ever be mistaken for another call's
// fallback); otherwise the call sends its own durable id -- reserved by rule
// (b) above, so it can never in turn be claimed as some other call's
// provider id either. This makes the wire unique by construction -- a
// provider that reissues the same indexed id
// across unrelated turns (or, rarer, emits two calls sharing one id in a
// single response) would otherwise put two ambiguous call/result pairs on
// the wire under the same id, which a provider that pairs them by value
// cannot tell apart, and which providers that require unique ids (e.g.
// Anthropic's Messages API) reject outright. Processing in request order
// also means the earliest call with a given provider id keeps it when a
// later call reuses that id, so history already sent normally keeps its
// wire ids as the conversation grows (provider-side prompt-prefix caches --
// llama.cpp, vLLM, Ollama -- stay valid). One exception, from rule (b): if
// an earlier call's provider id equals the durable id of a call added to
// history later (possible only when a provider emits ids in the
// IDGenerator's minted format), the earlier call sends its own durable id
// from that request on. Every request is still unique and internally
// consistent; only the prefix cache for that history is lost.
//
// sessionID fences every resolved session.ToolCall against the turn's own
// session: GetToolCall is store-global, so without this check a durable id
// that happens to collide with another session's row (impossible in
// practice given IDGenerator's store-wide-unique contract, but not provable
// from this function's own inputs) would publish that other session's
// provider id onto this session's wire request instead of failing closed.
//
// cache, when non-nil, is consulted before any store read and updated after
// a fresh resolution (see toolCallIDCache's doc comment); pass nil to always
// read through to the store.
//
// authorized, when non-nil, is consulted ONLY when a durable id's
// store.GetToolCall lookup returns session.ErrNotFound: if that id is
// currently authorized (authorizedRewriteSet.authorizedRewrite, this
// cycle's patchtoolcalls/reduction rewrites -- see
// wrapAuthorizedContentRewrites), the missing row is treated as an
// intentionally dangling, never-settled call rather than corrupt history,
// and the durable id itself goes on the wire (same as an empty
// ProviderCallID) instead of failing the dispatch closed. Pass nil to
// always fail closed on a missing row, exactly as before this exception
// existed.
//
// messages is never mutated in place: the original objects remain exactly
// what ADK itself is tracking (durable-ID-keyed), so this package's own
// bookkeeping (registerToolBatch, ADK's own tracked conversation, a later
// dispatch's own prepareDispatchInput/begin re-projection) is unaffected by
// the substitution performed here for the wire call alone.
func publicizeToolCallIDs(ctx context.Context, store session.Store, sessionID session.ID, cache *toolCallIDCache, authorized *authorizedRewriteSet, messages []*einoschema.AgenticMessage) ([]*einoschema.AgenticMessage, error) {
	resolved := make(map[string]string)
	resolve := func(durableID string) (string, error) {
		if durableID == "" {
			return "", nil
		}
		if public, ok := resolved[durableID]; ok {
			return public, nil
		}
		if cache != nil {
			if public, ok := cache.get(session.ToolCallID(durableID)); ok {
				resolved[durableID] = public
				return public, nil
			}
		}
		call, err := store.GetToolCall(ctx, session.ToolCallID(durableID))
		if err != nil {
			// A missing row is expected, not corrupt history, for exactly
			// one case: patchtoolcalls has authorized THIS cycle's content
			// for a call ID with no durable ToolCall row at all, by design
			// -- patchtoolcalls patches a genuinely dangling call without
			// ever fabricating a settlement row (see
			// NewPatchToolCallsHandlerFactory's doc comment). This fallback
			// is deliberately restricted to a patchtoolcalls-kind
			// authorization (round-two W6 review I1): reduction is never
			// legitimately authorized to rewrite a call with NO baseline
			// occurrence at all (authorityBindsToBaseline requires the
			// OPPOSITE -- a baseline occurrence must already exist for
			// reduction), so a reduction-kind authorization reaching this
			// branch means the call ID has no durable ToolCall row AND is
			// not one reduction may legitimately touch -- almost certainly
			// a fabricated call with no settlement at all, which must fail
			// closed exactly like an unauthorized one, not be waved onto
			// the wire under its own durable id. There is no provider id to
			// preserve for a call that was never durably admitted, so a
			// genuinely authorized patchtoolcalls fill sends the wire the
			// durable id back, exactly like an empty ProviderCallID -- and
			// this resolution is deliberately NOT cached, since it depends
			// on this cycle's authorization, not a stable durable row.
			if errors.Is(err, session.ErrNotFound) {
				if _, kind, ok := authorized.authorizedRewrite(durableID); ok && kind == HandlerKindPatchToolCalls {
					resolved[durableID] = durableID
					return durableID, nil
				}
			}
			// Only a deterministic durable-consistency failure -- the row
			// genuinely does not exist and is not an authorized dangling
			// patch, or exists under a conflicting identity -- is wrapped
			// in the non-retryable sentinel. Any other store error (a
			// transient read failure, for example) stays retryable/
			// failover-eligible under the run's normal policy: see
			// errToolCallIDUnresolved's doc comment.
			if errors.Is(err, session.ErrNotFound) || errors.Is(err, session.ErrConflict) {
				return "", fmt.Errorf("%w: resolve provider-facing tool call id for %s: %w", errToolCallIDUnresolved, durableID, err)
			}
			return "", fmt.Errorf("resolve provider-facing tool call id for %s: %w", durableID, err)
		}
		if sessionID != "" && call.SessionID != sessionID {
			return "", fmt.Errorf("%w: tool call %s belongs to session %s, not %s", errToolCallIDUnresolved, durableID, call.SessionID, sessionID)
		}
		public := call.ProviderCallID
		if public == "" {
			public = string(call.ID)
		}
		resolved[durableID] = public
		if cache != nil {
			cache.put(session.ToolCallID(durableID), public)
		}
		return public, nil
	}
	blockDurableID := func(block *einoschema.ContentBlock) string {
		if block == nil {
			return ""
		}
		switch block.Type {
		case einoschema.ContentBlockTypeFunctionToolCall:
			if block.FunctionToolCall != nil {
				return block.FunctionToolCall.CallID
			}
		case einoschema.ContentBlockTypeFunctionToolResult:
			if block.FunctionToolResult != nil {
				return block.FunctionToolResult.CallID
			}
		case einoschema.ContentBlockTypeToolSearchResult:
			if block.ToolSearchFunctionToolResult != nil {
				return block.ToolSearchFunctionToolResult.CallID
			}
		}
		return ""
	}

	// First pass: resolve every distinct durable id referenced anywhere in
	// messages, in the order its first block appears (request order). This
	// must see the whole request before any block is rewritten -- a
	// collision can span two calls in different messages (different turns),
	// not just two calls in the same response.
	var durableOrder []string
	orderSeen := make(map[string]bool)
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			durableID := blockDurableID(block)
			if durableID == "" {
				continue
			}
			if _, err := resolve(durableID); err != nil {
				return nil, err
			}
			if !orderSeen[durableID] {
				orderSeen[durableID] = true
				durableOrder = append(durableOrder, durableID)
			}
		}
	}
	// Second pass: assign each distinct durable id its wire value, in
	// request order. durableOrder (equivalently orderSeen) is also every
	// durable id's own reservation -- rule (b) above -- since a call is
	// always entitled to send its own durable id as a fallback.
	wireFor := make(map[string]string, len(durableOrder))
	claimed := make(map[string]bool, len(durableOrder))
	for _, durableID := range durableOrder {
		candidate := resolved[durableID]
		if candidate != durableID && (claimed[candidate] || orderSeen[candidate]) {
			// Either an earlier call in this request already sent this
			// exact string (rule a), or it is another call's durable id
			// (rule b): both make it ambiguous on the wire, so fall back to
			// this call's own durable id, which is unique store-wide.
			candidate = durableID
		}
		wireFor[durableID] = candidate
		claimed[candidate] = true
	}
	publicFor := func(durableID string) string {
		if public, ok := wireFor[durableID]; ok {
			return public
		}
		return durableID
	}

	out := make([]*einoschema.AgenticMessage, len(messages))
	for i, msg := range messages {
		if msg == nil {
			continue
		}
		var rewritten []*einoschema.ContentBlock
		for blockIndex, block := range msg.ContentBlocks {
			durableID := blockDurableID(block)
			if durableID == "" {
				continue
			}
			public := publicFor(durableID)
			if public == durableID {
				continue
			}
			if rewritten == nil {
				rewritten = append([]*einoschema.ContentBlock(nil), msg.ContentBlocks...)
			}
			cloned := *block
			switch block.Type {
			case einoschema.ContentBlockTypeFunctionToolCall:
				call := *block.FunctionToolCall
				call.CallID = public
				cloned.FunctionToolCall = &call
			case einoschema.ContentBlockTypeFunctionToolResult:
				result := *block.FunctionToolResult
				result.CallID = public
				cloned.FunctionToolResult = &result
			case einoschema.ContentBlockTypeToolSearchResult:
				search := *block.ToolSearchFunctionToolResult
				search.CallID = public
				cloned.ToolSearchFunctionToolResult = &search
			}
			rewritten[blockIndex] = &cloned
		}
		if rewritten == nil {
			out[i] = msg
			continue
		}
		clonedMsg := *msg
		clonedMsg.ContentBlocks = rewritten
		out[i] = &clonedMsg
	}
	return out, nil
}

func mustJSON(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Errorf("encode internal JSON value: %w", err))
	}
	return raw
}

type streamingHandle struct {
	runID       session.RunID
	host        *StreamingOrchestrator
	cancel      context.CancelFunc
	done        chan Result
	once        sync.Once
	onInterrupt func(reason string)
}

func (h *streamingHandle) RunID() session.RunID { return h.runID }
func (h *streamingHandle) Done() <-chan Result  { return h.done }
func (h *streamingHandle) Interrupt(_ context.Context, reason string) error {
	h.once.Do(func() {
		if h.onInterrupt != nil {
			h.onInterrupt(reason)
		}
		h.cancel()
	})
	return nil
}

// AwaitPause is not produced by the legacy tool-only resume path: it never
// delivers.
func (h *streamingHandle) AwaitPause() <-chan PauseInfo { return nil }

func (h *streamingHandle) Status(ctx context.Context) (session.Run, error) {
	if h.host == nil {
		return session.Run{}, fmt.Errorf("%w: handle has no host", ErrInvalidOrchestrator)
	}
	return h.host.store.GetRun(ctx, h.runID)
}
