package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
	"unicode/utf8"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/model"
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
	Store      session.Store
	Model      model.Resolver
	Tools      ToolRegistry
	Context    []ContextSource
	Events     EventSink
	Hooks      []Hook
	IDs        IDGenerator
	Clock      func() time.Time
	OwnerID    string
	Trace      agentcontext.TraceContext
	Attempts   int
	ToolTurns  int
	QueueSize  int
	Lease      time.Duration
	History    history.Options
	Admit      *Admitter
	Transactor session.Transactor
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
	now := o.now()
	ids := AdmissionIDs{
		SessionID:          request.SessionID,
		RunID:              o.IDs.NewRunID(),
		AssistantMessageID: o.IDs.NewMessageID(),
		ContextEpochID:     o.IDs.NewEpochID(),
		EventID:            o.IDs.NewEventID(),
	}
	admitter := o.admitter()
	admitted, err := admitter.Admit(ctx, AdmissionRequest{
		IDs:             ids,
		ParentMessageID: request.ParentID,
		Config:          request.Config,
		Model:           resolved,
		Input:           input,
		OwnerID:         o.ownerID(),
		LeaseUntil:      now.Add(o.lease()),
		Metadata:        request.Metadata,
	})
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	handle := &streamingHandle{
		runID:  admitted.Run.ID,
		cancel: cancel,
		done:   make(chan Result, 1),
	}
	go o.execute(runCtx, admitted, handle.done)
	return handle, nil
}

// Resume is reserved for the interrupt/resume bead.
func (o *StreamingOrchestrator) Resume(context.Context, session.RunID) (Handle, error) {
	return nil, ErrUnsupportedOperation
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
	result := o.run(ctx, admitted)
	done <- result
}

func (o *StreamingOrchestrator) run(ctx context.Context, admitted AdmittedRun) (result Result) {
	run := admitted.Run
	result = Result{RunID: admitted.Run.ID, MessageID: admitted.AssistantMessage.ID}
	defer func() {
		if recovered := recover(); recovered != nil {
			result.Status = session.RunFailed
			result.Error = fmt.Errorf("provider stream panic: %v", recovered)
		}
		result = o.finish(ctx, run, result)
	}()
	run.Status = session.RunRunning
	run.StartedAt = o.now()
	if err := o.Store.FinishRun(ctx, run); err != nil {
		result.Status = session.RunFailed
		result.Error = err
		return result
	}
	snapshot, err := o.prepareSnapshot(ctx, admitted.Snapshot, admitted.AssistantMessage.ID)
	if err != nil {
		result.Status = statusForError(err)
		result.Error = err
		return result
	}
	result = o.executeAttempts(ctx, snapshot, admitted.AssistantMessage.ID)
	for _, hook := range o.Hooks {
		_ = hook.AfterTurn(ctx, snapshot.Clone(), result)
	}
	return result
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
	if o.Tools != nil {
		tools, err := o.Tools.ResolveTools(ctx, snapshot.Clone())
		if err != nil {
			return TurnSnapshot{}, err
		}
		snapshot.Tools = cloneSlice(tools)
	}
	for _, hook := range o.Hooks {
		var err error
		snapshot, err = hook.BeforeTurn(ctx, snapshot.Clone())
		if err != nil {
			return TurnSnapshot{}, err
		}
	}
	_ = messageID
	return snapshot, nil
}

func (o *StreamingOrchestrator) executeAttempts(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID) Result {
	attempts := o.attempts()
	var last error
	for attempt := 1; attempt <= attempts; attempt++ {
		result, err := o.executeOne(ctx, snapshot, messageID)
		if err == nil {
			return result
		}
		if ctx.Err() != nil {
			return Result{RunID: snapshot.RunID, MessageID: messageID, Status: session.RunInterrupted, Interrupted: true, Error: ctx.Err()}
		}
		last = err
		if !retryable(err) || attempt == attempts {
			break
		}
	}
	if errors.Is(last, context.Canceled) {
		return Result{RunID: snapshot.RunID, MessageID: messageID, Status: session.RunInterrupted, Interrupted: true, Error: last}
	}
	return Result{RunID: snapshot.RunID, MessageID: messageID, Status: session.RunFailed, Error: last}
}

func (o *StreamingOrchestrator) executeOne(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID) (Result, error) {
	messages := cloneMessages(snapshot.Messages)
	currentMessageID := messageID
	for turn := 0; ; turn++ {
		msg, err := o.streamModel(ctx, snapshot, currentMessageID, messages)
		if err != nil {
			return Result{}, err
		}
		normalizeToolCallIDs(msg, o.IDs)
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
		toolMessages, err := o.executeTools(ctx, snapshot, currentMessageID, msg.ToolCalls)
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

func (o *StreamingOrchestrator) streamModel(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, messages []*einoschema.Message) (*einoschema.Message, error) {
	queue := newEventQueue(ctx, o.QueueSize, o.Events)
	defer queue.close()
	observer := streamObserver{queue: queue, base: snapshot, messageID: messageID, now: o.now}
	request := snapshot.ProviderRequest(messageID, o.Trace, observer)
	request.Messages = cloneMessages(messages)
	reader, err := openStream(ctx, snapshot.Model, request)
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, model.Error{Code: "nil_provider_stream", Message: "provider returned nil stream"}
	}
	defer reader.Close()
	var chunks []*einoschema.Message
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		chunk, err := reader.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if chunk == nil {
			return nil, model.Error{Code: "malformed_provider_stream", Message: "provider returned nil message chunk"}
		}
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
			return nil, err
		}
	}
	if len(chunks) == 0 {
		return einoschema.AssistantMessage("", nil), nil
	}
	msg, err := einoschema.ConcatMessages(chunks)
	if err != nil {
		return nil, model.Error{Code: "malformed_provider_stream", Message: err.Error(), Cause: err}
	}
	return msg, nil
}

func openStream(ctx context.Context, resolved model.Resolved, request model.Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	if resolved.Streamer != nil {
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
	return client.Stream(ctx, request.Messages, einomodel.WithTools(request.Tools))
}

func (o *StreamingOrchestrator) persistAssistant(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, msg *einoschema.Message) error {
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
	for _, call := range msg.ToolCalls {
		payload := toolCallPayload{
			ID:        call.ID,
			Name:      call.Function.Name,
			Arguments: json.RawMessage(call.Function.Arguments),
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

func (o *StreamingOrchestrator) executeTools(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, calls []einoschema.ToolCall) ([]*einoschema.Message, error) {
	byName := map[string]Tool{}
	for _, tool := range snapshot.Tools {
		byName[tool.Name] = tool
	}
	messages := make([]*einoschema.Message, 0, len(calls))
	for _, schemaCall := range calls {
		tool, ok := byName[schemaCall.Function.Name]
		if !ok || tool.Executor == nil {
			return nil, fmt.Errorf("tool %q unavailable", schemaCall.Function.Name)
		}
		callID := session.ToolCallID(schemaCall.ID)
		if callID == "" {
			callID = o.IDs.NewToolCallID()
		}
		input := json.RawMessage(schemaCall.Function.Arguments)
		if tool.InputDecoder != nil {
			decoded, err := tool.InputDecoder.DecodeToolInput(ctx, input)
			if err != nil {
				return nil, err
			}
			input = decoded
		}
		record, err := o.Store.CreateToolCall(ctx, session.ToolCall{
			ID:        callID,
			SessionID: snapshot.SessionID,
			RunID:     snapshot.RunID,
			MessageID: messageID,
			Name:      schemaCall.Function.Name,
			Input:     cloneJSON(input),
			Status:    session.ToolCallPending,
			RetrySafe: tool.RetrySafe,
			Metadata:  cloneStringMap(tool.Metadata),
		})
		if err != nil {
			return nil, err
		}
		_ = o.emitToolCall(ctx, snapshot, messageID, callID, session.ToolCallPending, toolCallPayload{
			ID:        string(callID),
			Name:      schemaCall.Function.Name,
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
		_ = o.emitToolCall(ctx, snapshot, messageID, callID, session.ToolCallRunning, toolCallPayload{
			ID:        string(callID),
			Name:      schemaCall.Function.Name,
			Arguments: cloneJSON(input),
		})
		result, execErr := tool.Executor.Execute(ctx, ToolCall{
			ID:        callID,
			SessionID: snapshot.SessionID,
			RunID:     snapshot.RunID,
			MessageID: messageID,
			Name:      schemaCall.Function.Name,
			Scope:     tool.Scope,
			Input:     cloneJSON(input),
		})
		output, status, errText := encodeToolOutput(callID, result, tool.Retention, execErr)
		record.Status = status
		record.Output = cloneJSON(output)
		record.CompletedAt = o.now()
		record.Error = errText
		if err := o.Store.FinishToolCall(ctx, record); err != nil {
			return nil, err
		}
		_ = o.emitToolCall(ctx, snapshot, messageID, callID, status, toolCallEventPayload(output, schemaCall.Function.Name, input))
		toolMessageID := o.IDs.NewMessageID()
		if _, err := o.Store.AppendMessage(ctx, session.Message{
			ID:        toolMessageID,
			SessionID: snapshot.SessionID,
			RunID:     snapshot.RunID,
			ParentID:  messageID,
			Role:      session.RoleTool,
			ModelID:   string(snapshot.Model.Model.ID),
			CreatedAt: o.now(),
			UpdatedAt: o.now(),
		}); err != nil {
			return nil, err
		}
		if err := o.appendPart(ctx, session.Part{
			ID:        o.IDs.NewPartID(),
			MessageID: toolMessageID,
			SessionID: snapshot.SessionID,
			RunID:     snapshot.RunID,
			Kind:      session.PartToolResult,
			Payload:   cloneJSON(output),
			CreatedAt: o.now(),
			UpdatedAt: o.now(),
		}); err != nil {
			return nil, err
		}
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

func (o *StreamingOrchestrator) emitToolCall(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, callID session.ToolCallID, status session.ToolCallStatus, payload any) error {
	if o.Events == nil {
		return nil
	}
	return o.Events.Emit(ctx, Event{
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

func (o *StreamingOrchestrator) finish(ctx context.Context, run session.Run, result Result) Result {
	if result.Status == "" {
		result.Status = session.RunCompleted
	}
	run.Status = result.Status
	run.FinishedAt = o.now()
	if result.Error != nil {
		run.Error = result.Error.Error()
	}
	if err := o.Store.FinishRun(context.WithoutCancel(ctx), run); err != nil && result.Error == nil {
		result.Status = session.RunFailed
		result.Error = err
	}
	for _, hook := range o.Hooks {
		_ = hook.AfterRun(context.WithoutCancel(ctx), result)
	}
	if o.Events != nil {
		_ = o.Events.Emit(context.WithoutCancel(ctx), Event{
			Kind:      EventRunFinished,
			SessionID: run.SessionID,
			RunID:     run.ID,
			MessageID: result.MessageID,
			Error:     eventError(result.Error),
			Payload: mustJSON(map[string]any{
				"status":      string(result.Status),
				"interrupted": result.Interrupted,
			}),
			Time: o.now(),
		})
	}
	return result
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

func encodeToolOutput(callID session.ToolCallID, result ToolResult, policy RetentionPolicy, err error) (json.RawMessage, session.ToolCallStatus, string) {
	payload := toolOutputPayload{
		ToolCallID: string(callID),
		Status:     "completed",
	}
	if err != nil {
		payload.Status = "operational_failure"
		payload.Content = "tool execution failed"
		if errors.Is(err, context.Canceled) {
			payload.Status = "interrupted"
			return mustJSON(payload), session.ToolCallInterrupted, err.Error()
		}
		return mustJSON(payload), session.ToolCallFailed, err.Error()
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
	runID  session.RunID
	cancel context.CancelFunc
	done   chan Result
	once   sync.Once
}

func (h *streamingHandle) RunID() session.RunID { return h.runID }
func (h *streamingHandle) Done() <-chan Result  { return h.done }
func (h *streamingHandle) Interrupt(context.Context, string) error {
	h.once.Do(h.cancel)
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
}

func (o streamObserver) OnProviderStart(context.Context, model.Request)     {}
func (o streamObserver) OnProviderDelta(context.Context, model.StreamDelta) {}
func (o streamObserver) OnProviderError(context.Context, model.Error)       {}
func (o streamObserver) OnProviderEnd(context.Context, model.Response)      {}

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
