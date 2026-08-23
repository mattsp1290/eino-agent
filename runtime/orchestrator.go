package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"
	einoobs "github.com/mattsp1290/eino-obs"

	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

var (
	// ErrInvalidOrchestrator reports a missing dependency or invalid request.
	ErrInvalidOrchestrator = errors.New("invalid orchestrator")
	// ErrUnsupportedOperation reports lifecycle behavior reserved for a later
	// runtime bead.
	ErrUnsupportedOperation = errors.New("unsupported orchestrator operation")
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
	Store                       session.Store
	Model                       model.Resolver
	Tools                       ToolRegistry
	Plans                       RunPlanProvider
	Context                     []ContextSource
	Events                      EventSink
	Hooks                       []Hook
	IDs                         IDGenerator
	Clock                       func() time.Time
	OwnerID                     string
	Trace                       agentcontext.TraceContext
	Attempts                    int
	ToolTurns                   int
	QueueSize                   int
	Lease                       time.Duration
	History                     history.Options
	Permissions                 permissions.Policy
	Middleware                  []ToolMiddleware
	Admit                       *Admitter
	Transactor                  session.Transactor
	Observer                    *einoobs.Observer
	SystemPromptMaterialization bool
	ModelRequestLedger          bool
	ModelRequestSafeOptions     []string
	ModelRequestMaxBytes        int
}

// Start admits and asynchronously executes one streaming turn.
func (o *StreamingOrchestrator) Start(ctx context.Context, request Request) (Handle, error) {
	if err := o.validate(request); err != nil {
		return nil, err
	}
	resolved, err := o.Model.Resolve(ctx, request.Config.Model, model.Runtime{
		Directory: request.Config.Metadata["workspace_root"],
		Options:   cloneStringMap(request.Config.Agent.Options),
	})
	if err != nil {
		return nil, err
	}
	input, err := o.providerInput(ctx, request)
	if err != nil {
		return nil, err
	}
	plan, err := o.acquireRunPlan(ctx, RunPlanRequest{SessionID: request.SessionID, Config: request.Config})
	if err != nil {
		return nil, err
	}
	ctx = withRunPlan(ctx, plan)
	now := o.now()
	ids := AdmissionIDs{
		SessionID:          request.SessionID,
		RunID:              o.IDs.NewRunID(),
		AssistantMessageID: o.IDs.NewMessageID(),
		ContextEpochID:     o.IDs.NewEpochID(),
		EventID:            o.IDs.NewEventID(),
	}
	admitter := o.admitter()
	admitter.Events = o.eventSinkFor(ctx, admitter.Events)
	admitted, err := admitter.Admit(ctx, AdmissionRequest{
		IDs:             ids,
		ParentMessageID: request.ParentID,
		Config:          request.Config,
		Model:           resolved,
		Input:           input,
		OwnerID:         o.ownerID(),
		LeaseUntil:      now.Add(o.lease()),
		Metadata:        request.Metadata,
		ExtensionPlan:   plan.Descriptor,
	})
	if err != nil {
		plan.release()
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	handle := &streamingHandle{
		runID:  admitted.Run.ID,
		cancel: cancel,
		done:   make(chan Result, 1),
		onInterrupt: func(reason string) {
			o.observeInterrupt(context.WithoutCancel(ctx), admitted.Run, admitted.AssistantMessage.ID, reason)
		},
	}
	go o.execute(withRunPlan(runCtx, plan), admitted, handle.done)
	return handle, nil
}

// Status returns the current active run for a session.
func (o *StreamingOrchestrator) Status(ctx context.Context, sessionID session.ID) (session.Run, error) {
	if o == nil || o.Store == nil {
		return session.Run{}, fmt.Errorf("%w: store required", ErrInvalidOrchestrator)
	}
	return o.Store.ActiveRun(ctx, sessionID)
}

func (o *StreamingOrchestrator) execute(ctx context.Context, admitted AdmittedRun, done chan<- Result) {
	defer close(done)
	plan := runPlanFromContext(ctx)
	if plan != nil {
		defer plan.release()
	}
	result, settled := o.run(ctx, admitted)
	if settled && plan != nil && plan.Dispatch != nil {
		_ = extension.Notify(plan.Dispatch, context.WithoutCancel(ctx), RunSettledPoint, RunSettledNotice{SessionID: admitted.Run.SessionID, Result: result, Duration: o.now().Sub(admitted.Run.CreatedAt), Error: classifyExtensionError(result.Error)})
	}
	done <- result
}

func (o *StreamingOrchestrator) run(ctx context.Context, admitted AdmittedRun) (result Result, settled bool) {
	run := admitted.Run
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
		result, settled = o.finish(ctx, run, result)
		o.finishObservedRun(observed, result, o.now())
	}()
	if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
		decision, err := extension.Invoke(plan.Dispatch, ctx, RunBeforeExecutePoint, RunGateInput{SessionID: run.SessionID, RunID: run.ID, ProviderID: run.ProviderID, ModelID: run.ModelID}, func(context.Context, RunGateInput) (RunDecision, error) {
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
	run.Status = session.RunRunning
	run.StartedAt = o.now()
	observed = o.startObservedRun(ctx, run, admitted.AssistantMessage.ID, run.StartedAt)
	if err := o.Store.FinishRun(ctx, run); err != nil {
		result.Status = session.RunFailed
		result.Error = err
		return result, false
	}
	if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
		_ = extension.Notify(plan.Dispatch, ctx, RunStartedPoint, RunStartedNotice{SessionID: run.SessionID, RunID: run.ID, Time: run.StartedAt})
	}
	snapshot, err := o.prepareSnapshot(ctx, admitted.Snapshot, admitted.AssistantMessage.ID)
	if err != nil {
		result.Status = statusForError(err)
		result.Error = err
		return result, false
	}
	result = o.executeAttempts(ctx, snapshot, admitted.AssistantMessage.ID, &runUsage)
	for _, hook := range o.Hooks {
		_ = hook.AfterTurn(ctx, snapshot.Clone(), result)
	}
	return result, false
}

func (o *StreamingOrchestrator) prepareSnapshot(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID) (TurnSnapshot, error) {
	for _, source := range o.Context {
		if source == nil {
			continue
		}
		messages, err := source.LoadContext(ctx, snapshot.Clone())
		if err != nil {
			return TurnSnapshot{}, err
		}
		snapshot.Messages = append(snapshot.Messages, cloneMessages(messages)...)
	}
	if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
		assembly := ContextAssembly{SessionID: snapshot.SessionID, RunID: snapshot.RunID, EpochID: snapshot.EpochID, Metadata: boundedTurnMetadata(snapshot), Base: cloneMessages(snapshot.Messages)}
		assembled, err := extension.Invoke(plan.Dispatch, ctx, ContextAssemblePoint, assembly, func(_ context.Context, value ContextAssembly) (ContextAssembly, error) { return value, nil })
		if err != nil {
			return TurnSnapshot{}, err
		}
		snapshot.Messages = materializeContextAssembly(assembled)
	}
	if o.Tools != nil {
		tools, err := o.Tools.ResolveTools(ctx, snapshot.Clone())
		if err != nil {
			return TurnSnapshot{}, err
		}
		snapshot.Tools = cloneSlice(tools)
	}
	if plan := runPlanFromContext(ctx); plan != nil && plan.Tools != nil {
		planned, err := plan.Tools.ResolveTools(ctx, snapshot.Clone())
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
		sort.Slice(snapshot.Tools, func(i, j int) bool { return snapshot.Tools[i].Name < snapshot.Tools[j].Name })
	}
	if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
		_, err := extension.Invoke(plan.Dispatch, ctx, TurnPreparePoint, boundedTurnMetadata(snapshot), func(_ context.Context, value BoundedTurnMetadata) (BoundedTurnMetadata, error) { return value, nil })
		if err != nil {
			return TurnSnapshot{}, err
		}
	}
	for _, hook := range o.Hooks {
		var err error
		snapshot, err = hook.BeforeTurn(ctx, snapshot.Clone())
		if err != nil {
			return TurnSnapshot{}, err
		}
	}
	o.observeToolsResolved(ctx, snapshot.Clone(), snapshot.Tools)
	_ = messageID
	return snapshot, nil
}

func (o *StreamingOrchestrator) executeAttempts(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, usage *model.Usage) Result {
	attempts := o.attempts()
	var last error
	for attempt := 1; attempt <= attempts; attempt++ {
		result, err := o.executeOne(ctx, snapshot, messageID, attempt, usage)
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

func (o *StreamingOrchestrator) executeOne(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, attempt int, usage *model.Usage) (Result, error) {
	messages := cloneMessages(snapshot.Messages)
	currentMessageID := messageID
	for turn := 0; ; turn++ {
		msg, err := o.streamModel(ctx, snapshot, currentMessageID, messages, attempt, turn+1, usage)
		if err != nil {
			return Result{}, err
		}
		normalizeToolCallIDs(msg, o.IDs)
		preparedCalls, err := o.prepareToolCalls(ctx, snapshot, currentMessageID, msg.ToolCalls)
		if err != nil {
			return Result{}, err
		}
		for index := range preparedCalls {
			msg.ToolCalls[index] = preparedCalls[index].schemaCall
		}
		if err := o.persistAssistant(ctx, snapshot, currentMessageID, msg); err != nil {
			return Result{}, err
		}
		if len(msg.ToolCalls) == 0 {
			return Result{RunID: snapshot.RunID, MessageID: currentMessageID, Status: session.RunCompleted}, nil
		}
		if turn >= o.toolTurns() {
			return Result{}, model.Error{Code: "tool_turn_limit_exceeded", Message: "model exceeded tool turn limit", Cause: model.ErrProviderRejected}
		}
		messages = append(messages, msg)
		toolMessages, err := o.executePreparedTools(ctx, snapshot, currentMessageID, preparedCalls)
		if err != nil {
			return Result{}, err
		}
		messages = append(messages, toolMessages...)
		currentMessageID = o.IDs.NewMessageID()
		if _, err := o.Store.AppendMessage(ctx, session.Message{
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

func (o *StreamingOrchestrator) streamModel(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, messages []*einoschema.Message, attempt, step int, usage *model.Usage) (message *einoschema.Message, err error) {
	queue := newEventQueue(ctx, o.QueueSize, o.eventSink(ctx))
	defer queue.close()
	obsStream := o.startObservedStream(ctx, snapshot, messageID, attempt)
	var streamUsage model.Usage
	var streamErr error
	var requestRecord *session.ModelRequestRecord
	var requestStore session.ModelRequestStore
	defer func() {
		if recovered := recover(); recovered != nil {
			streamErr = fmt.Errorf("provider stream panic: %v", recovered)
			err = streamErr
			message = nil
		}
		// Accumulate this stream's usage into the run total in the same defer
		// that reports it to observability, so the persisted run total stays
		// exactly consistent with the run span's summed usage (both include
		// partial usage from a failed/canceled stream).
		if usage != nil {
			*usage = addUsage(*usage, streamUsage)
		}
		ledgerTransitionOK := true
		if requestRecord != nil {
			state := session.ModelRequestCompleted
			if streamErr != nil {
				state = session.ModelRequestFailed
			}
			if transitionErr := updateModelRequest(ctx, requestStore, requestRecord, state, streamErr, o.now()); transitionErr != nil {
				streamErr = transitionErr
				err = transitionErr
				message = nil
				ledgerTransitionOK = false
			}
		}
		if streamErr != nil {
			o.errorObservedStream(obsStream, streamErr, streamUsage)
		} else {
			o.endObservedStream(obsStream, streamUsage)
		}
		if plan := runPlanFromContext(ctx); ledgerTransitionOK && plan != nil && plan.Dispatch != nil {
			_ = extension.Notify(plan.Dispatch, context.WithoutCancel(ctx), ModelCompletedPoint, ModelCompletedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Attempt: attempt, Step: step, Usage: runtimeUsage(streamUsage), Error: classifyExtensionError(streamErr)})
		}
	}()
	observer := &streamObserver{queue: queue, base: snapshot, messageID: messageID, now: o.now}
	request := snapshot.ProviderRequest(messageID, o.Trace, observer)
	request.Messages = cloneMessages(messages)
	request.System, err = o.renderSystemPrompt(ctx, snapshot, attempt, step)
	if err != nil {
		streamErr = err
		return nil, err
	}
	requestRecord, requestStore, err = o.prepareModelRequest(ctx, snapshot, request, messageID, attempt, step)
	if err != nil {
		streamErr = err
		return nil, err
	}
	if requestRecord != nil {
		request.IdempotencyKey = string(requestRecord.ID)
		if err = updateModelRequest(ctx, requestStore, requestRecord, session.ModelRequestDispatchStarted, nil, o.now()); err != nil {
			streamErr = err
			return nil, err
		}
	}
	if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
		requestRecordID := session.ModelRequestID("")
		if requestRecord != nil {
			requestRecordID = requestRecord.ID
		}
		contentHash := modelRequestContentHash(request)
		if requestRecord != nil {
			contentHash = requestRecord.ContentSHA256
		}
		_ = extension.Notify(plan.Dispatch, ctx, ModelRequestedPoint, ModelRequestedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Attempt: attempt, Step: step, ProviderID: string(request.Identity.ProviderID), ModelID: string(request.Identity.ModelID), RequestRecordID: requestRecordID, MessageCount: len(request.Messages), ToolCount: len(request.Tools), ContentHash: contentHash})
	}
	var reader *einoschema.StreamReader[*einoschema.Message]
	if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
		reader, err = extension.Invoke(plan.Dispatch, ctx, ModelStreamPoint, ModelStreamInput{Resolved: snapshot.Model, Request: request}, func(ctx context.Context, input ModelStreamInput) (*einoschema.StreamReader[*einoschema.Message], error) {
			return openStream(ctx, input.Resolved, input.Request)
		})
	} else {
		reader, err = openStream(ctx, snapshot.Model, request)
	}
	if err != nil {
		streamErr = err
		return nil, err
	}
	if reader == nil {
		streamErr = model.Error{Code: "nil_provider_stream", Message: "provider returned nil stream"}
		return nil, streamErr
	}
	defer reader.Close()
	var chunks []*einoschema.Message
	for {
		if err := ctx.Err(); err != nil {
			streamErr = err
			return nil, err
		}
		chunk, err := reader.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			streamErr = err
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			streamErr = err
			return nil, err
		}
		if chunk == nil {
			streamErr = model.Error{Code: "malformed_provider_stream", Message: "provider returned nil message chunk"}
			return nil, streamErr
		}
		o.observeStreamChunk(obsStream, int64(len(chunks)))
		chunks = append(chunks, chunk)
		if err := queue.emit(Event{
			Kind:       EventMessageDelta,
			SessionID:  snapshot.SessionID,
			RunID:      snapshot.RunID,
			MessageID:  messageID,
			EpochID:    snapshot.EpochID,
			ProviderID: string(request.Identity.ProviderID),
			ModelID:    string(request.Identity.ModelID),
			Payload:    mustJSON(map[string]string{"content": chunk.Content, "reasoning": chunk.ReasoningContent}),
			LiveOnly:   true,
			Time:       o.now(),
		}); err != nil {
			streamErr = err
			return nil, err
		}
	}
	if len(chunks) == 0 {
		streamUsage = observer.usageSnapshot()
		return einoschema.AssistantMessage("", nil), nil
	}
	msg, err := einoschema.ConcatMessages(chunks)
	if err != nil {
		streamErr = model.Error{Code: "malformed_provider_stream", Message: err.Error(), Cause: err}
		return nil, streamErr
	}
	streamUsage = resolveStreamUsage(observer.usageSnapshot(), msg)
	return msg, nil
}

func openStream(ctx context.Context, resolved model.Resolved, request model.Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	if resolved.Streamer != nil {
		if request.IdempotencyKey != "" {
			if streamer, ok := resolved.Streamer.(model.IdempotentStreamer); ok {
				return streamer.StreamProviderWithIdempotencyKey(ctx, request, request.IdempotencyKey)
			}
		}
		return resolved.Streamer.StreamProvider(ctx, request)
	}
	client := resolved.Client
	if client == nil {
		return nil, model.Error{Code: "model_client_missing", Message: "resolved model has no client", Cause: model.ErrProviderUnavailable}
	}
	if len(request.Tools) > 0 {
		withTools, err := client.WithTools(request.Tools)
		if err != nil {
			return nil, err
		}
		client = withTools
	}
	messages := cloneMessages(request.Messages)
	if request.System != "" {
		messages = append([]*einoschema.Message{einoschema.SystemMessage(request.System)}, messages...)
	}
	return client.Stream(ctx, messages, einomodel.WithTools(request.Tools))
}

func (o *StreamingOrchestrator) persistAssistant(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, msg *einoschema.Message) error {
	calls, err := normalizeToolCalls(msg.ToolCalls)
	if err != nil {
		return err
	}
	ordinal := int64(0)
	if msg.Content != "" {
		if err := o.appendPart(ctx, session.Part{
			ID:        o.IDs.NewPartID(),
			MessageID: messageID,
			SessionID: snapshot.SessionID,
			RunID:     snapshot.RunID,
			Kind:      session.PartText,
			Ordinal:   ordinal,
			Payload:   mustJSON(map[string]string{"text": msg.Content}),
			CreatedAt: o.now(),
			UpdatedAt: o.now(),
		}); err != nil {
			return err
		}
		ordinal++
	}
	if msg.ReasoningContent != "" {
		if err := o.appendPart(ctx, session.Part{
			ID:        o.IDs.NewPartID(),
			MessageID: messageID,
			SessionID: snapshot.SessionID,
			RunID:     snapshot.RunID,
			Kind:      session.PartReasoning,
			Ordinal:   ordinal,
			Payload:   mustJSON(map[string]string{"text": msg.ReasoningContent}),
			CreatedAt: o.now(),
			UpdatedAt: o.now(),
		}); err != nil {
			return err
		}
		ordinal++
	}
	for _, call := range calls {
		payload := toolCallPayload{
			ID:        call.call.ID,
			Name:      call.call.Function.Name,
			Arguments: call.arguments,
		}
		if err := o.appendPart(ctx, session.Part{
			ID:        o.IDs.NewPartID(),
			MessageID: messageID,
			SessionID: snapshot.SessionID,
			RunID:     snapshot.RunID,
			Kind:      session.PartToolCall,
			Ordinal:   ordinal,
			Payload:   mustJSON(payload),
			CreatedAt: o.now(),
			UpdatedAt: o.now(),
		}); err != nil {
			return err
		}
		ordinal++
	}
	return nil
}

type normalizedToolCall struct {
	call      einoschema.ToolCall
	arguments json.RawMessage
}

func normalizeToolCalls(calls []einoschema.ToolCall) ([]normalizedToolCall, error) {
	normalized := make([]normalizedToolCall, 0, len(calls))
	for _, call := range calls {
		arguments, err := normalizedToolArguments(call.Function.Arguments)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, normalizedToolCall{call: call, arguments: arguments})
	}
	return normalized, nil
}

type preparedToolCall struct {
	schemaCall    einoschema.ToolCall
	tool          Tool
	call          ToolCall
	middlewareErr error
}

func (o *StreamingOrchestrator) prepareToolCalls(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, calls []einoschema.ToolCall) ([]preparedToolCall, error) {
	byName := map[string]Tool{}
	for _, tool := range snapshot.Tools {
		byName[tool.Name] = tool
	}
	prepared := make([]preparedToolCall, 0, len(calls))
	for _, schemaCall := range calls {
		callID := session.ToolCallID(schemaCall.ID)
		if callID == "" {
			callID = o.IDs.NewToolCallID()
		}
		tool, ok := byName[schemaCall.Function.Name]
		if !ok || tool.Executor == nil {
			err := fmt.Errorf("tool %q unavailable", schemaCall.Function.Name)
			o.observeToolSettled(ctx, snapshot, Tool{Name: schemaCall.Function.Name}, ToolCall{
				ID:        callID,
				SessionID: snapshot.SessionID,
				RunID:     snapshot.RunID,
				MessageID: messageID,
				Name:      schemaCall.Function.Name,
			}, session.ToolCallFailed, 0, err, nil)
			return nil, err
		}
		input, err := normalizedToolArguments(schemaCall.Function.Arguments)
		if err != nil {
			return nil, err
		}
		if tool.InputDecoder != nil {
			decoded, err := tool.InputDecoder.DecodeToolInput(ctx, input)
			if err != nil {
				return nil, err
			}
			input = decoded
		}
		call := ToolCall{
			ID:        callID,
			SessionID: snapshot.SessionID,
			RunID:     snapshot.RunID,
			MessageID: messageID,
			Name:      schemaCall.Function.Name,
			Scope:     tool.Scope,
			Input:     cloneJSON(input),
		}
		input, middlewareErr := o.beforeToolCall(ctx, tool, call)
		call.Input = cloneJSON(input)
		if middlewareErr == nil {
			if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
				prepared, err := extension.Invoke(plan.Dispatch, ctx, ToolPreparePoint, PreparedToolCall{Tool: tool, Call: call}, func(_ context.Context, value PreparedToolCall) (PreparedToolCall, error) { return value, nil })
				if err != nil {
					middlewareErr = err
				} else {
					tool, call, input = prepared.Tool, prepared.Call, cloneJSON(prepared.Call.Input)
				}
			}
		}
		call.Pattern = toolPattern(input, schemaCall.Function.Name)
		schemaCall.Function.Arguments = string(input)
		if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
			_ = extension.Notify(plan.Dispatch, ctx, ToolPreparedPoint, ToolPreparedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, ToolCallID: call.ID, ToolName: call.Name, Input: call.Input, Component: cloneStringMap(tool.Metadata)})
		}
		prepared = append(prepared, preparedToolCall{schemaCall: schemaCall, tool: tool, call: call, middlewareErr: middlewareErr})
	}
	return prepared, nil
}

func (o *StreamingOrchestrator) executePreparedTools(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, calls []preparedToolCall) ([]*einoschema.Message, error) {
	messages := make([]*einoschema.Message, 0, len(calls))
	var settlementStore session.ToolSettlementStore
	if plan := runPlanFromContext(ctx); plan != nil && plan.Descriptor.Mode == session.PlanStrict {
		var ok bool
		settlementStore, ok = o.Store.(session.ToolSettlementStore)
		if !ok {
			return nil, fmt.Errorf("%w: strict tool plan requires ToolSettlementStore", ErrInvalidOrchestrator)
		}
	}
	for _, prepared := range calls {
		schemaCall := prepared.schemaCall
		tool := prepared.tool
		call := prepared.call
		callID := call.ID
		input := call.Input
		var resultMessageID session.MessageID
		var resultPartID session.PartID
		if settlementStore != nil {
			resultMessageID = o.IDs.NewMessageID()
			resultPartID = o.IDs.NewPartID()
		}
		record, err := o.Store.CreateToolCall(ctx, session.ToolCall{
			ID:              callID,
			SessionID:       snapshot.SessionID,
			RunID:           snapshot.RunID,
			MessageID:       messageID,
			ResultMessageID: resultMessageID,
			ResultPartID:    resultPartID,
			Name:            call.Name,
			Input:           cloneJSON(input),
			Status:          session.ToolCallPending,
			RetrySafe:       tool.RetrySafe,
			Metadata:        cloneStringMap(tool.Metadata),
		})
		if err != nil {
			return nil, err
		}
		call.ResultMessageID = record.ResultMessageID
		call.ResultPartID = record.ResultPartID
		_ = o.emitToolCall(ctx, snapshot, messageID, callID, session.ToolCallPending, toolCallPayload{
			ID:        string(callID),
			Name:      call.Name,
			Arguments: cloneJSON(input),
		})
		record.Status = session.ToolCallRunning
		record.ClaimedBy = o.ownerID()
		record.ClaimToken = string(o.IDs.NewEventID())
		record.LeaseUntil = o.now().Add(o.lease())
		record.StartedAt = o.now()
		if _, err := o.Store.ClaimToolCall(ctx, record); err != nil {
			return nil, err
		}
		if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
			_ = extension.Notify(plan.Dispatch, ctx, ToolStartedPoint, ToolStartedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, ToolCallID: callID, ToolName: call.Name, Time: record.StartedAt})
		}
		_ = o.emitToolCall(ctx, snapshot, messageID, callID, session.ToolCallRunning, toolCallPayload{
			ID:        string(callID),
			Name:      call.Name,
			Arguments: cloneJSON(input),
		})
		o.observeToolMaterialized(ctx, snapshot, tool, call)
		observedTool := o.startObservedToolCall(ctx, snapshot, tool, call)
		outcome := ToolOutcome{Call: cloneToolCall(call), Disposition: ToolFailed, RawError: prepared.middlewareErr, Error: classifyExtensionError(prepared.middlewareErr)}
		if prepared.middlewareErr == nil {
			outcome = o.executeToolOutcome(ctx, tool, call)
			outcome = o.afterToolOutcome(ctx, tool, outcome)
		}
		outcome = o.transformToolOutcome(ctx, outcome)
		result, execErr := outcome.Result, outcome.RawError
		output, status, errText := encodeToolOutput(callID, result, tool.Retention, outcome.Disposition, execErr)
		record.Status = status
		record.Output = cloneJSON(output)
		record.CompletedAt = o.now()
		record.Error = errText
		toolMessageID := resultMessageID
		if toolMessageID == "" {
			toolMessageID = o.IDs.NewMessageID()
		}
		toolPartID := resultPartID
		if toolPartID == "" {
			toolPartID = o.IDs.NewPartID()
		}
		resultMessage := session.Message{
			ID: toolMessageID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, ParentID: messageID,
			Role: session.RoleTool, ModelID: string(snapshot.Model.Model.ID), CreatedAt: o.now(), UpdatedAt: o.now(),
		}
		resultPart := session.Part{
			ID: toolPartID, MessageID: toolMessageID, SessionID: snapshot.SessionID, RunID: snapshot.RunID,
			Kind: session.PartToolResult, Payload: cloneJSON(output), CreatedAt: o.now(), UpdatedAt: o.now(),
		}
		if settlementStore != nil {
			err = settlementStore.SettleToolCall(context.WithoutCancel(ctx), session.ToolSettlement{ID: callID, ClaimedBy: record.ClaimedBy, ClaimToken: record.ClaimToken, Status: status, Output: cloneJSON(output), Error: errText, Metadata: cloneStringMap(record.Metadata), CompletedAt: record.CompletedAt, ResultMessage: resultMessage, ResultPart: resultPart})
		} else {
			err = o.Store.FinishToolCall(ctx, record)
			if err == nil {
				_, err = o.Store.AppendMessage(ctx, resultMessage)
			}
			if err == nil {
				err = o.appendPart(ctx, resultPart)
			}
		}
		if err != nil {
			o.finishObservedToolCall(observedTool, session.ToolCallFailed, err, nil)
			o.observeToolSettled(ctx, snapshot, tool, call, session.ToolCallFailed, o.now().Sub(record.StartedAt), err, nil)
			return nil, err
		}
		if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
			_ = extension.Notify(plan.Dispatch, context.WithoutCancel(ctx), ToolSettledPoint, ToolSettledNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, ToolCallID: callID, ToolName: call.Name, Status: status, Result: result, Error: classifyExtensionError(execErr)})
		}
		o.finishObservedToolCall(observedTool, status, execErr, result.Metadata)
		o.observeToolSettled(ctx, snapshot, tool, call, status, record.CompletedAt.Sub(record.StartedAt), execErr, result.Metadata)
		_ = o.emitToolCall(ctx, snapshot, messageID, callID, status, toolCallEventPayload(output, schemaCall.Function.Name, input))
		messages = append(messages, einoschema.ToolMessage(string(output), string(callID), einoschema.WithToolName(schemaCall.Function.Name)))
		if errors.Is(execErr, context.Canceled) {
			return messages, execErr
		}
	}
	return messages, nil
}

func (o *StreamingOrchestrator) appendPart(ctx context.Context, part session.Part) error {
	_, err := o.Store.AppendPart(ctx, part)
	return err
}

func (o *StreamingOrchestrator) executeToolOutcome(ctx context.Context, tool Tool, call ToolCall) ToolOutcome {
	guard, guardErr := evaluateToolGuards(ctx, runPlanFromContext(ctx), tool, call)
	if guardErr != nil {
		return ToolOutcome{Call: cloneToolCall(call), Disposition: dispositionForError(guardErr), RawError: guardErr, Error: classifyExtensionError(guardErr)}
	}
	if guard.Decision == ToolGuardDeny {
		result := modelVisiblePermissionResult("denied", guard.Message)
		return ToolOutcome{Call: cloneToolCall(call), Disposition: ToolDenied, Result: result, PermissionMetadata: cloneStringMap(result.Metadata)}
	}
	wrapped := cloneTool(tool)
	wrapped.Executor = runtimeToolExecutorFunc(func(ctx context.Context, call ToolCall) (ToolResult, error) {
		plan := runPlanFromContext(ctx)
		if plan == nil || plan.Dispatch == nil {
			return tool.Executor.Execute(ctx, call)
		}
		outcome, err := extension.Invoke(plan.Dispatch, ctx, ToolExecutePoint, ToolExecution{Tool: tool, Call: call}, func(ctx context.Context, input ToolExecution) (ToolOutcome, error) {
			result, execErr := input.Tool.Executor.Execute(ctx, input.Call)
			disposition := ToolExecuted
			if execErr != nil {
				disposition = dispositionForError(execErr)
			}
			return sealToolOutcome(ToolOutcome{Call: cloneToolCall(input.Call), Disposition: disposition, Result: result, RawError: execErr, Error: classifyExtensionError(execErr)}), nil
		})
		if err != nil {
			return ToolResult{}, err
		}
		return outcome.Result, outcome.RawError
	})
	var result ToolResult
	var err error
	if o.Permissions == nil {
		result, err = wrapped.Executor.Execute(ctx, call)
	} else {
		result, err = ExecuteToolWithPermissions(ctx, wrapped, call, o.Permissions)
	}
	disposition := dispositionForResult(result, err)
	permissionMetadata := map[string]string(nil)
	if result.Metadata["permission_status"] != "" {
		permissionMetadata = cloneStringMap(result.Metadata)
	}
	return ToolOutcome{Call: cloneToolCall(call), Disposition: disposition, Result: cloneRuntimeToolResult(result), RawError: err, Error: classifyExtensionError(err), PermissionMetadata: permissionMetadata}
}

func (o *StreamingOrchestrator) afterToolOutcome(ctx context.Context, tool Tool, outcome ToolOutcome) ToolOutcome {
	result, middlewareErr := o.afterToolCall(ctx, tool, outcome.Call, outcome.Result, outcome.RawError)
	if middlewareErr != nil {
		outcome.RawError = errors.Join(outcome.RawError, middlewareErr)
		outcome.Error = classifyExtensionError(outcome.RawError)
		outcome.Disposition = dispositionForError(outcome.RawError)
	}
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

func (o *StreamingOrchestrator) transformToolOutcome(ctx context.Context, outcome ToolOutcome) ToolOutcome {
	outcome = sealToolOutcome(outcome)
	if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
		transformed, err := extension.Invoke(plan.Dispatch, ctx, ToolResultTransformPoint, outcome, func(_ context.Context, value ToolOutcome) (ToolOutcome, error) { return value, nil })
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

func (o *StreamingOrchestrator) beforeToolCall(ctx context.Context, tool Tool, call ToolCall) (json.RawMessage, error) {
	input := cloneJSON(call.Input)
	for _, middleware := range o.Middleware {
		if middleware == nil {
			continue
		}
		call.Input = cloneJSON(input)
		next, err := middleware.BeforeToolCall(ctx, tool, call)
		if err != nil {
			return input, err
		}
		if !json.Valid(next) {
			return input, fmt.Errorf("tool middleware returned malformed JSON input")
		}
		input = cloneJSON(next)
	}
	return input, nil
}

func (o *StreamingOrchestrator) afterToolCall(ctx context.Context, tool Tool, call ToolCall, result ToolResult, execErr error) (ToolResult, error) {
	for index := len(o.Middleware) - 1; index >= 0; index-- {
		middleware := o.Middleware[index]
		if middleware == nil {
			continue
		}
		next, err := middleware.AfterToolCall(ctx, tool, call, result, execErr)
		if err != nil {
			return result, err
		}
		result = next
	}
	return result, nil
}

func (o *StreamingOrchestrator) emitToolCall(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, callID session.ToolCallID, status session.ToolCallStatus, payload any) error {
	sink := o.eventSink(ctx)
	if sink == nil {
		return nil
	}
	return sink.Emit(ctx, Event{
		Kind:       EventToolCallUpdated,
		SessionID:  snapshot.SessionID,
		RunID:      snapshot.RunID,
		MessageID:  messageID,
		ToolCallID: callID,
		EpochID:    snapshot.EpochID,
		ProviderID: string(snapshot.Model.Provider.ID),
		ModelID:    string(snapshot.Model.Model.ID),
		Payload:    mustJSON(withToolStatus(payload, status)),
		Time:       o.now(),
	})
}

func (o *StreamingOrchestrator) finish(ctx context.Context, run session.Run, result Result) (Result, bool) {
	if result.Status == "" {
		result.Status = session.RunCompleted
	}
	run.Status = result.Status
	run.FinishedAt = o.now()
	if result.Error != nil {
		run.Error = result.Error.Error()
	}
	settled := true
	if err := o.Store.FinishRun(context.WithoutCancel(ctx), run); err != nil {
		settled = false
		if result.Error == nil {
			result.Status = session.RunFailed
			result.Error = err
		}
	}
	for _, hook := range o.Hooks {
		_ = hook.AfterRun(context.WithoutCancel(ctx), result)
	}
	if sink := o.eventSink(ctx); sink != nil {
		_ = sink.Emit(context.WithoutCancel(ctx), Event{
			Kind:      EventRunFinished,
			SessionID: run.SessionID,
			RunID:     run.ID,
			MessageID: result.MessageID,
			Usage:     result.Usage,
			Error:     eventError(result.Error),
			Payload: mustJSON(map[string]any{
				"status":      string(result.Status),
				"interrupted": result.Interrupted,
			}),
			Time: o.now(),
		})
	}
	return result, settled
}

func toolCallEventPayload(output json.RawMessage, name string, input json.RawMessage) map[string]any {
	payload := map[string]any{}
	_ = json.Unmarshal(output, &payload)
	payload["name"] = name
	payload["arguments"] = cloneJSON(input)
	return payload
}

func withToolStatus(payload any, status session.ToolCallStatus) map[string]any {
	raw, _ := json.Marshal(payload)
	result := map[string]any{}
	_ = json.Unmarshal(raw, &result)
	result["status"] = string(status)
	return result
}

func toolPattern(input json.RawMessage, fallback string) string {
	var payload struct {
		PermissionPattern string `json:"permission_pattern"`
		Pattern           string `json:"pattern"`
	}
	if json.Unmarshal(input, &payload) == nil {
		if payload.PermissionPattern != "" {
			return payload.PermissionPattern
		}
		if payload.Pattern != "" {
			return payload.Pattern
		}
	}
	return fallback
}

func (o *StreamingOrchestrator) validate(request Request) error {
	switch {
	case o == nil:
		return fmt.Errorf("%w: orchestrator required", ErrInvalidOrchestrator)
	case o.Store == nil:
		return fmt.Errorf("%w: store required", ErrInvalidOrchestrator)
	case o.Model == nil:
		return fmt.Errorf("%w: model resolver required", ErrInvalidOrchestrator)
	case o.IDs == nil:
		return fmt.Errorf("%w: id generator required", ErrInvalidOrchestrator)
	case request.SessionID == "":
		return fmt.Errorf("%w: session id required", ErrInvalidOrchestrator)
	case o.ModelRequestLedger:
		if _, ok := o.Store.(session.ModelRequestStore); !ok {
			return fmt.Errorf("%w: model request ledger requires session.ModelRequestStore", ErrInvalidOrchestrator)
		}
		return nil
	default:
		return nil
	}
}

func (o *StreamingOrchestrator) providerInput(ctx context.Context, request Request) ([]*einoschema.Message, error) {
	historyMessages, err := LoadHistory(ctx, o.Store, request.SessionID, o.History)
	if err != nil && !errors.Is(err, session.ErrNotFound) {
		return nil, err
	}
	messages := cloneMessages(historyMessages)
	messages = append(messages, cloneMessages(request.Input)...)
	return messages, nil
}

func (o *StreamingOrchestrator) admitter() Admitter {
	if o.Admit != nil {
		admitter := *o.Admit
		if admitter.Store == nil {
			admitter.Store = o.Store
		}
		if admitter.Events == nil {
			admitter.Events = o.Events
		}
		if admitter.Hooks == nil {
			admitter.Hooks = o.Hooks
		}
		if admitter.Clock == nil {
			admitter.Clock = o.Clock
		}
		if admitter.Transactor == nil {
			admitter.Transactor = o.Transactor
		}
		return admitter
	}
	return Admitter{Store: o.Store, Transactor: o.Transactor, Events: o.Events, Hooks: o.Hooks, Clock: o.Clock}
}

func (o *StreamingOrchestrator) now() time.Time {
	if o.Clock != nil {
		return o.Clock().UTC()
	}
	return time.Now().UTC()
}

func (o *StreamingOrchestrator) ownerID() string {
	if o.OwnerID != "" {
		return o.OwnerID
	}
	return "runtime"
}

func (o *StreamingOrchestrator) attempts() int {
	if o.Attempts > 0 {
		return o.Attempts
	}
	return 1
}

func (o *StreamingOrchestrator) toolTurns() int {
	if o.ToolTurns > 0 {
		return o.ToolTurns
	}
	return 8
}

func (o *StreamingOrchestrator) lease() time.Duration {
	if o.Lease > 0 {
		return o.Lease
	}
	return time.Minute
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

func encodeToolOutput(callID session.ToolCallID, result ToolResult, policy RetentionPolicy, disposition ToolDisposition, err error) (json.RawMessage, session.ToolCallStatus, string) {
	payload := toolOutputPayload{
		ToolCallID: string(callID),
		Status:     "completed",
	}
	switch disposition {
	case ToolDenied, ToolApprovalRequired:
		payload.Status = "expected_failure"
		applyToolOutputBounds(&payload, result, policy)
		return mustJSON(payload), session.ToolCallFailed, ""
	case ToolInterrupted:
		payload.Status = "interrupted"
		if err == nil {
			applyToolOutputBounds(&payload, result, policy)
			return mustJSON(payload), session.ToolCallInterrupted, ""
		}
		payload.Content = "tool execution failed"
		return mustJSON(payload), session.ToolCallInterrupted, err.Error()
	case ToolFailed:
		payload.Status = "operational_failure"
		payload.Content = "tool execution failed"
		if err == nil {
			return mustJSON(payload), session.ToolCallFailed, ""
		}
		return mustJSON(payload), session.ToolCallFailed, err.Error()
	case ToolExecuted:
		if err != nil {
			payload.Status = "operational_failure"
			payload.Content = "tool execution failed"
			return mustJSON(payload), session.ToolCallFailed, err.Error()
		}
	default:
		payload.Status = "operational_failure"
		payload.Content = "tool execution failed"
		return mustJSON(payload), session.ToolCallFailed, "invalid tool disposition"
	}
	applyToolOutputBounds(&payload, result, policy)
	return mustJSON(payload), session.ToolCallCompleted, ""
}

type toolOutputPayload struct {
	ToolCallID   string          `json:"tool_call_id"`
	Status       string          `json:"status"`
	Content      string          `json:"content,omitempty"`
	Structured   json.RawMessage `json:"structured,omitempty"`
	Truncated    bool            `json:"truncated,omitempty"`
	OriginalSize int64           `json:"original_size,omitempty"`
	InlineSize   int64           `json:"inline_size,omitempty"`
	External     bool            `json:"external,omitempty"`
	Redacted     bool            `json:"redacted,omitempty"`
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
	raw := json.RawMessage(arguments)
	if !json.Valid(raw) {
		return nil, model.Error{
			Code:    "malformed_provider_tool_call",
			Message: "provider returned invalid tool call arguments",
			Cause:   model.ErrProviderRejected,
		}
	}
	return cloneJSON(raw), nil
}

func applyToolOutputBounds(output *toolOutputPayload, result ToolResult, policy RetentionPolicy) {
	output.OriginalSize = int64(len(result.Output))
	if policy.Redact {
		output.Redacted = true
		output.External = policy.StoreExternal && (result.Output != "" || len(result.Structured) > 0)
		return
	}
	content := result.Output
	if policy.MaxInlineBytes >= 0 && int64(len(content)) > policy.MaxInlineBytes {
		content = validUTF8Prefix(content, int(policy.MaxInlineBytes))
		output.Truncated = true
		output.External = policy.StoreExternal
	}
	output.Content = content
	output.InlineSize = int64(len(content))
	if len(result.Structured) == 0 {
		return
	}
	output.OriginalSize += int64(len(result.Structured))
	if policy.MaxInlineBytes >= 0 {
		remaining := policy.MaxInlineBytes - output.InlineSize
		if remaining < int64(len(result.Structured)) {
			output.Truncated = true
			output.External = policy.StoreExternal
			return
		}
	}
	output.Structured = cloneJSON(result.Structured)
	output.InlineSize += int64(len(result.Structured))
}

func validUTF8Prefix(content string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if limit > len(content) {
		limit = len(content)
	}
	for limit > 0 && !utf8.ValidString(content[:limit]) {
		limit--
	}
	return content[:limit]
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
		return []byte(`{}`)
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
func (h *streamingHandle) FollowUp(context.Context, []*einoschema.Message) error {
	return ErrUnsupportedOperation
}

type streamObserver struct {
	queue     *eventQueue
	base      TurnSnapshot
	messageID session.MessageID
	now       func() time.Time
	mu        sync.Mutex
	usage     model.Usage
}

func (o *streamObserver) OnProviderStart(context.Context, model.Request) {}
func (o *streamObserver) OnProviderDelta(_ context.Context, delta model.StreamDelta) {
	o.setUsage(delta.Usage)
}
func (o *streamObserver) OnProviderError(context.Context, model.Error) {}
func (o *streamObserver) OnProviderEnd(_ context.Context, response model.Response) {
	o.setUsage(response.Usage)
}

func (o *streamObserver) setUsage(usage model.Usage) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.usage = mergeUsage(o.usage, usage)
	o.mu.Unlock()
}

func (o *streamObserver) usageSnapshot() model.Usage {
	if o == nil {
		return model.Usage{}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.usage
}

// resolveStreamUsage picks the token usage for a completed model stream. A
// runtime Streamer adapter reports usage through the ProviderObserver (the
// `observed` snapshot); the default client-streaming path (resolved.Client, no
// Streamer) has no such adapter, so the provider's usage instead rides on the
// concatenated message's ResponseMeta.Usage — the standard Eino usage channel.
// Prefer the observed usage when present, else fall back to the message's
// ResponseMeta.Usage so token totals flow on the client path too (without this,
// run consumers on the client path see zero tokens even when the model reports
// them).
func resolveStreamUsage(observed model.Usage, msg *einoschema.Message) model.Usage {
	if observed.InputTokens != 0 || observed.OutputTokens != 0 {
		return observed
	}
	if msg != nil && msg.ResponseMeta != nil && msg.ResponseMeta.Usage != nil {
		u := msg.ResponseMeta.Usage
		return model.Usage{
			InputTokens:  int64(u.PromptTokens),
			OutputTokens: int64(u.CompletionTokens),
		}
	}
	return observed
}

func mergeUsage(current model.Usage, next model.Usage) model.Usage {
	if next.InputTokens != 0 {
		current.InputTokens = next.InputTokens
	}
	if next.OutputTokens != 0 {
		current.OutputTokens = next.OutputTokens
	}
	if next.ReasoningTokens != 0 {
		current.ReasoningTokens = next.ReasoningTokens
	}
	if next.CacheReadTokens != 0 {
		current.CacheReadTokens = next.CacheReadTokens
	}
	if next.CacheWriteTokens != 0 {
		current.CacheWriteTokens = next.CacheWriteTokens
	}
	if next.Cost != 0 {
		current.Cost = next.Cost
	}
	return current
}

type eventQueue struct {
	ctx    context.Context
	events chan Event
	sink   EventSink
	done   chan struct{}
}

func newEventQueue(ctx context.Context, size int, sink EventSink) *eventQueue {
	if size <= 0 {
		size = 1
	}
	q := &eventQueue{ctx: ctx, events: make(chan Event, size), sink: sink, done: make(chan struct{})}
	go func() {
		defer close(q.done)
		for event := range q.events {
			if q.sink != nil {
				_ = q.sink.Emit(ctx, event)
			}
		}
	}()
	return q
}

func (q *eventQueue) emit(event Event) error {
	select {
	case <-q.ctx.Done():
		return q.ctx.Err()
	case q.events <- event:
		return nil
	}
}

func (q *eventQueue) close() {
	close(q.events)
	<-q.done
}
