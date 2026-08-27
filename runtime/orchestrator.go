package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	einoschema "github.com/cloudwego/eino/schema"
	einoobs "github.com/mattsp1290/eino-obs"

	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/internal/jsonobject"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

var (
	// ErrInvalidOrchestrator reports a missing dependency or invalid request.
	ErrInvalidOrchestrator = errors.New("invalid orchestrator")
)

// IDGenerator creates durable identifiers for records owned by the
// orchestrator.
type IDGenerator interface {
	NewRunID() session.RunID
	NewMessageID() session.MessageID
	NewPartID() session.PartID
	NewToolCallID() session.ToolCallID
	NewEventID() session.EventID
	NewEpochID() session.EpochID
}

// StreamingOrchestrator executes admitted runs against Eino model streams.
type StreamingOrchestrator struct {
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
}

// Start admits and asynchronously executes one streaming turn.
func (o *StreamingOrchestrator) Start(ctx context.Context, request Request) (Handle, error) {
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
	input, err := o.providerInput(ctx, request)
	if err != nil {
		return nil, err
	}
	execution := newRunExecution(o, plan)
	ids := AdmissionIDs{
		SessionID:          request.SessionID,
		RunID:              o.ids.NewRunID(),
		AssistantMessageID: o.ids.NewMessageID(),
		ContextEpochID:     o.ids.NewEpochID(),
		EventID:            o.ids.NewEventID(),
		RunClaimToken:      string(o.ids.NewEventID()),
	}
	admitter := o.admitter()
	admitter.Events = execution.eventSink(admitter.Events)
	admitter.Extensions = execution.dispatch()
	admitted, err := admitter.Admit(ctx, AdmissionRequest{
		IDs:             ids,
		ParentMessageID: request.ParentID,
		Config:          request.Config,
		Model:           resolved,
		Input:           input,
		OwnerID:         o.ownerID(),
		LeaseDuration:   o.lease(),
		Metadata:        request.Metadata,
		ExtensionPlan:   plan.Descriptor(),
	})
	if err != nil {
		return nil, err
	}
	execution.bindRun(admitted.Run)
	runCtx, cancel := context.WithCancel(ctx)
	handle := &streamingHandle{
		runID:  admitted.Run.ID,
		cancel: cancel,
		done:   make(chan Result, 1),
		onInterrupt: func(reason string) {
			o.observeInterrupt(context.WithoutCancel(ctx), admitted.Run, admitted.AssistantMessage.ID, reason)
		},
	}
	ownershipTransferred = true
	go o.execute(runCtx, execution, admitted, handle.done)
	return handle, nil
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

func (o *StreamingOrchestrator) execute(ctx context.Context, execution *runExecution, admitted AdmittedRun, done chan<- Result) {
	defer close(done)
	defer execution.release()
	result, settled := o.run(ctx, execution, admitted)
	if settled {
		extension.Notify(execution.dispatch(), context.WithoutCancel(ctx), RunSettledPoint, RunSettledNotice{SessionID: admitted.Run.SessionID, Result: result, Duration: o.now().Sub(admitted.Run.CreatedAt), Error: classifyExtensionError(result.Error)})
	}
	done <- result
}

func (o *StreamingOrchestrator) run(ctx context.Context, execution *runExecution, admitted AdmittedRun) (result Result, settled bool) {
	run := admitted.Run
	ctx = execution.startLease(ctx, o.lease())
	result = Result{RunID: admitted.Run.ID, MessageID: admitted.AssistantMessage.ID}
	var observed observedRun
	// runUsage accumulates provider usage across every model stream in the run
	// (all turns and retry attempts), mirroring the per-stream usage reported to
	// the observability path. It is surfaced on result.Usage in the defer below
	// so finish() can carry the run total on the EventRunFinished event.
	var runUsage model.Usage
	defer func() {
		if recovered := recover(); recovered != nil {
			result.Status = session.RunFailed
			result.Error = fmt.Errorf("provider stream panic: %v", recovered)
		}
		result.Usage = runtimeUsage(runUsage)
		result, settled = o.finish(ctx, execution, run, result)
		o.finishObservedRun(observed, result, o.now())
	}()
	{
		decision, err := extension.Invoke(execution.dispatch(), ctx, RunBeforeExecutePoint, RunGateInput{SessionID: run.SessionID, RunID: run.ID, ProviderID: run.ProviderID, ModelID: run.ModelID}, func(context.Context, RunGateInput) (RunDecision, error) {
			return RunDecision{Kind: RunContinue}, nil
		})
		if err != nil {
			result.Status = session.RunFailed
			result.Error = err
			return result, false
		}
		if decision.Kind == RunReject {
			result.Status = session.RunFailed
			result.Error = model.Error{Code: decision.Code, Message: decision.Message, Cause: model.ErrProviderRejected}
			return result, false
		}
	}
	run.StartedAt = o.now()
	observed = o.startObservedRun(ctx, run, admitted.AssistantMessage.ID, run.StartedAt)
	started, err := execution.store.StartRun(ctx, run.StartedAt)
	if err != nil {
		result.Status = session.RunFailed
		result.Error = err
		return result, false
	}
	run = started
	extension.Notify(execution.dispatch(), ctx, RunStartedPoint, RunStartedNotice{SessionID: run.SessionID, RunID: run.ID, Time: run.StartedAt})
	snapshot, err := o.prepareSnapshot(ctx, execution, admitted.Snapshot, admitted.AssistantMessage.ID)
	if err != nil {
		result.Status = statusForError(err)
		result.Error = err
		return result, false
	}
	result = o.executeAttempts(ctx, execution, snapshot, admitted.AssistantMessage.ID, &runUsage)
	return result, false
}

func (o *StreamingOrchestrator) prepareSnapshot(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID) (TurnSnapshot, error) {
	assembly := ContextAssembly{SessionID: snapshot.SessionID, RunID: snapshot.RunID, EpochID: snapshot.EpochID, Metadata: boundedTurnMetadata(snapshot), Base: cloneMessages(snapshot.Messages)}
	assembled, err := extension.Invoke(execution.dispatch(), ctx, ContextAssemblePoint, assembly, func(_ context.Context, value ContextAssembly) (ContextAssembly, error) { return value, nil })
	if err != nil {
		return TurnSnapshot{}, err
	}
	snapshot.Messages, err = materializeContextAssembly(assembled)
	if err != nil {
		return TurnSnapshot{}, err
	}
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
	_, err = extension.Invoke(execution.dispatch(), ctx, TurnPreparePoint, boundedTurnMetadata(snapshot), func(_ context.Context, value BoundedTurnMetadata) (BoundedTurnMetadata, error) { return value, nil })
	if err != nil {
		return TurnSnapshot{}, err
	}
	o.observeToolsResolved(ctx, snapshot.Clone(), snapshot.Tools)
	_ = messageID
	return snapshot, nil
}

func (o *StreamingOrchestrator) executeAttempts(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, usage *model.Usage) Result {
	attempts := o.attempts()
	var last error
	for attempt := 1; attempt <= attempts; attempt++ {
		result, err := o.executeOne(ctx, execution, snapshot, messageID, attempt, usage)
		if err == nil {
			return result
		}
		if ctx.Err() != nil {
			o.observeError(ctx, snapshot, messageID, "provider_stream", err)
			return Result{RunID: snapshot.RunID, MessageID: messageID, Status: session.RunInterrupted, Interrupted: true, Error: ctx.Err()}
		}
		last = err
		if !retryable(err) || attempt == attempts {
			break
		}
		o.observeRetry(ctx, snapshot, messageID, attempt, attempts, err)
	}
	if errors.Is(last, context.Canceled) {
		o.observeError(ctx, snapshot, messageID, "provider_stream", last)
		return Result{RunID: snapshot.RunID, MessageID: messageID, Status: session.RunInterrupted, Interrupted: true, Error: last}
	}
	o.observeError(ctx, snapshot, messageID, "provider_stream", last)
	return Result{RunID: snapshot.RunID, MessageID: messageID, Status: session.RunFailed, Error: last}
}

func (o *StreamingOrchestrator) executeOne(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, attempt int, usage *model.Usage) (Result, error) {
	messages := cloneMessages(snapshot.Messages)
	currentMessageID := messageID
	for turn := 0; ; turn++ {
		msg, err := o.streamModel(ctx, execution, snapshot, currentMessageID, messages, attempt, turn+1, usage)
		if err != nil {
			return Result{}, err
		}
		normalizeToolCallIDs(msg, o.ids)
		preparedCalls, err := o.prepareToolCalls(ctx, execution, snapshot, currentMessageID, msg.ToolCalls)
		if err != nil {
			return Result{}, err
		}
		for index := range preparedCalls {
			msg.ToolCalls[index] = preparedCalls[index].schemaCall
		}
		if err := o.persistAssistant(ctx, execution, snapshot, currentMessageID, msg); err != nil {
			return Result{}, err
		}
		if len(msg.ToolCalls) == 0 {
			return Result{RunID: snapshot.RunID, MessageID: currentMessageID, Status: session.RunCompleted}, nil
		}
		if turn >= o.toolTurns() {
			return Result{}, model.Error{Code: "tool_turn_limit_exceeded", Message: "model exceeded tool turn limit", Cause: model.ErrProviderRejected}
		}
		messages = append(messages, msg)
		toolMessages, err := o.executePreparedTools(ctx, execution, snapshot, currentMessageID, preparedCalls)
		if err != nil {
			return Result{}, err
		}
		messages = append(messages, toolMessages...)
		currentMessageID = o.ids.NewMessageID()
		if _, err := execution.store.AppendMessage(ctx, session.Message{
			ID:        currentMessageID,
			SessionID: snapshot.SessionID,
			RunID:     snapshot.RunID,
			ParentID:  messageID,
			Role:      session.RoleAssistant,
			Agent:     snapshot.Config.Agent.Name,
			ModelID:   string(snapshot.Model.Model.ID),
			CreatedAt: o.now(),
			UpdatedAt: o.now(),
		}); err != nil {
			return Result{}, err
		}
	}
}

func (o *StreamingOrchestrator) executeToolOutcome(ctx context.Context, execution *runExecution, tool Tool, call ToolCall) ToolOutcome {
	guard, guardErr := evaluateToolGuards(ctx, execution.plan, tool, call)
	if guardErr != nil {
		return ToolOutcome{Call: cloneToolCall(call), Disposition: dispositionForError(guardErr), RawError: guardErr, Error: classifyExtensionError(guardErr)}
	}
	if guard.Decision == ToolGuardDeny {
		result := modelVisiblePermissionResult("denied", guard.Message)
		return ToolOutcome{Call: cloneToolCall(call), Disposition: ToolDenied, Result: result, PermissionMetadata: cloneStringMap(result.Metadata)}
	}
	wrapped, cloneErr := cloneToolChecked(tool)
	if cloneErr != nil {
		return ToolOutcome{Call: cloneToolCall(call), Disposition: ToolFailed, RawError: cloneErr, Error: classifyExtensionError(cloneErr)}
	}
	wrapped.Executor = runtimeToolExecutorFunc(func(ctx context.Context, call ToolCall) (ToolResult, error) {
		outcome, err := extension.Invoke(execution.dispatch(), ctx, ToolExecutePoint, ToolExecution{Tool: extensionTool(tool), Call: extensionToolCall(call)}, func(ctx context.Context, _ ToolExecution) (ToolOutcome, error) {
			result, execErr := tool.Executor.Execute(ctx, cloneToolCall(call))
			disposition := ToolExecuted
			if execErr != nil {
				disposition = dispositionForError(execErr)
			}
			return sealToolOutcome(ToolOutcome{Call: extensionToolCall(call), Disposition: disposition, Result: result, RawError: execErr, Error: classifyExtensionError(execErr)}), nil
		})
		if err != nil {
			return ToolResult{}, err
		}
		return outcome.Result, outcome.RawError
	})
	var result ToolResult
	var err error
	if o.permissions == nil {
		result, err = wrapped.Executor.Execute(ctx, call)
	} else {
		result, err = ExecuteToolWithPermissions(ctx, wrapped, call, o.permissions)
	}
	disposition := dispositionForResult(result, err)
	permissionMetadata := map[string]string(nil)
	if result.Metadata["permission_status"] != "" {
		permissionMetadata = cloneStringMap(result.Metadata)
	}
	return ToolOutcome{Call: cloneToolCall(call), Disposition: disposition, Result: cloneRuntimeToolResult(result), RawError: err, Error: classifyExtensionError(err), PermissionMetadata: permissionMetadata}
}

func (o *StreamingOrchestrator) afterToolOutcome(ctx context.Context, tool Tool, outcome ToolOutcome) ToolOutcome {
	result := outcome.Result
	if len(outcome.PermissionMetadata) != 0 {
		if result.Metadata == nil {
			result.Metadata = make(map[string]string)
		}
		for key, value := range outcome.PermissionMetadata {
			result.Metadata[key] = value
		}
	}
	outcome.Result = cloneRuntimeToolResult(result)
	return outcome
}

func (o *StreamingOrchestrator) transformToolOutcome(ctx context.Context, execution *runExecution, outcome ToolOutcome) ToolOutcome {
	outcome = sealToolOutcome(outcome)
	transformed, err := extension.Invoke(execution.dispatch(), ctx, ToolResultTransformPoint, outcome, func(_ context.Context, value ToolOutcome) (ToolOutcome, error) { return value, nil })
	if err != nil {
		outcome.RawError = errors.Join(outcome.RawError, err)
		outcome.Error = classifyExtensionError(outcome.RawError)
		outcome.Disposition = dispositionForError(outcome.RawError)
		return outcome
	}
	outcome = transformed
	outcome.Result = cloneRuntimeToolResult(outcome.Result)
	if len(outcome.PermissionMetadata) != 0 {
		if outcome.Result.Metadata == nil {
			outcome.Result.Metadata = make(map[string]string)
		}
		for key, value := range outcome.PermissionMetadata {
			outcome.Result.Metadata[key] = value
		}
	}
	return outcome
}

func dispositionForResult(result ToolResult, err error) ToolDisposition {
	if err != nil {
		return dispositionForError(err)
	}
	switch result.Metadata["permission_status"] {
	case "denied":
		return ToolDenied
	case "approval_required":
		return ToolApprovalRequired
	case "interrupted":
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

func (o *StreamingOrchestrator) finish(ctx context.Context, execution *runExecution, run session.Run, result Result) (Result, bool) {
	if leaseErr := execution.stopLease(); leaseErr != nil && result.Error == nil {
		result.Status = session.RunFailed
		result.Error = leaseErr
	}
	if result.Status == "" {
		result.Status = session.RunCompleted
	}
	run.Status = result.Status
	run.FinishedAt = o.now()
	if result.Error != nil {
		run.Error = result.Error.Error()
	}
	finalEvent := o.finalRunEvent(run, result)
	settled := true
	if err := execution.store.SettleRun(context.WithoutCancel(ctx), run, finalEvent); err != nil {
		settled = false
		if result.Error == nil {
			result.Status = session.RunFailed
			result.Error = err
		}
	} else {
		o.publishRunFinished(ctx, execution, finalEvent, result)
	}
	return result, settled
}

func (o *StreamingOrchestrator) finalRunEvent(run session.Run, result Result) *session.EventRecord {
	if o == nil || o.ids == nil {
		return nil
	}
	eventErr := eventError(result.Error)
	return &session.EventRecord{
		ID: o.ids.NewEventID(), SessionID: run.SessionID, RunID: run.ID, MessageID: result.MessageID,
		ProviderID: run.ProviderID, ModelID: run.ModelID, Kind: string(EventRunFinished),
		Usage:     session.Usage{InputTokens: result.Usage.InputTokens, OutputTokens: result.Usage.OutputTokens, ReasoningTokens: result.Usage.ReasoningTokens, CacheReadTokens: result.Usage.CacheReadTokens, CacheWriteTokens: result.Usage.CacheWriteTokens, Cost: result.Usage.Cost},
		Error:     session.EventError{Code: eventErr.Code, Message: eventErr.Message, Retryable: eventErr.Retryable},
		Payload:   mustJSON(map[string]any{"status": string(result.Status), "interrupted": result.Interrupted}),
		Redaction: session.RedactionMetadata, CreatedAt: o.now(),
	}
}

func (o *StreamingOrchestrator) publishRunFinished(ctx context.Context, execution *runExecution, event *session.EventRecord, result Result) {
	if event == nil {
		return
	}
	if sink := execution.eventSink(o.events); sink != nil {
		_ = sink.Emit(context.WithoutCancel(ctx), Event{
			Kind: EventRunFinished, EventID: event.ID, SessionID: event.SessionID, RunID: event.RunID,
			MessageID: event.MessageID, ProviderID: event.ProviderID, ModelID: event.ModelID,
			Usage: result.Usage, Error: eventError(result.Error), Redaction: RedactionClass(event.Redaction),
			Payload: cloneJSON(event.Payload), Time: event.CreatedAt,
		})
	}
}

func toolCallEventPayload(output json.RawMessage, name string, input json.RawMessage) map[string]any {
	payload := map[string]any{}
	_ = json.Unmarshal(output, &payload)
	payload["name"] = name
	payload["arguments"] = cloneJSON(input)
	return payload
}

func withToolStatus(payload any, status session.ToolCallStatus) (map[string]any, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode tool event payload: %w", err)
	}
	result := map[string]any{}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("decode tool event payload: %w", err)
	}
	result["status"] = string(status)
	return result, nil
}

func (o *StreamingOrchestrator) validate(request Request) error {
	if err := o.validateConfigured(); err != nil {
		return err
	}
	if request.SessionID == "" {
		return fmt.Errorf("%w: session id required", ErrInvalidOrchestrator)
	}
	return nil
}

func (o *StreamingOrchestrator) validateConfigured() error {
	if o == nil || !o.configured {
		return fmt.Errorf("%w: use NewStreamingOrchestrator", ErrInvalidOrchestrator)
	}
	return nil
}

func (o *StreamingOrchestrator) providerInput(ctx context.Context, request Request) ([]*einoschema.Message, error) {
	historyMessages, err := LoadHistory(ctx, o.store, request.SessionID, o.history)
	if err != nil && !errors.Is(err, session.ErrNotFound) {
		return nil, err
	}
	messages := cloneMessages(historyMessages)
	messages = append(messages, cloneMessages(request.Input)...)
	return messages, nil
}

func (o *StreamingOrchestrator) admitter() Admitter {
	return Admitter{Store: o.store, Events: o.events, Clock: o.clock}
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

func retryable(err error) bool {
	var providerErr model.Error
	return errors.As(err, &providerErr) && providerErr.Retryable
}

func statusForError(err error) session.RunStatus {
	if errors.Is(err, context.Canceled) {
		return session.RunInterrupted
	}
	return session.RunFailed
}

func eventError(err error) EventError {
	if err == nil {
		return EventError{}
	}
	var providerErr model.Error
	if errors.As(err, &providerErr) {
		return EventError{Code: providerErr.Code, Message: providerErr.Message, Retryable: providerErr.Retryable}
	}
	return EventError{Message: err.Error()}
}

type toolCallPayload struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
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

func normalizeToolCallIDs(msg *einoschema.Message, ids IDGenerator) {
	if msg == nil {
		return
	}
	for i := range msg.ToolCalls {
		if msg.ToolCalls[i].ID == "" {
			msg.ToolCalls[i].ID = string(ids.NewToolCallID())
		}
		if msg.ToolCalls[i].Type == "" {
			msg.ToolCalls[i].Type = "function"
		}
	}
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
