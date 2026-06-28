package runtime

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func TestStreamingOrchestratorCompletesSuccessfulTurn(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("hel", nil), einoschema.AssistantMessage("lo", nil)}, nil
	}))
	handle, err := orch.Start(context.Background(), Request{
		SessionID: "session-1",
		ParentID:  "user-1",
		Input:     []*einoschema.Message{einoschema.UserMessage("hello")},
		Config:    orchestratorConfig(),
	})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	run, err := store.GetRun(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("GetRun error = %v", err)
	}
	if run.Status != session.RunCompleted {
		t.Fatalf("run status = %s", run.Status)
	}
	var textParts []session.Part
	for _, part := range store.parts {
		if part.Kind == session.PartText {
			textParts = append(textParts, part)
		}
	}
	if len(textParts) != 1 || string(textParts[0].Payload) != `{"text":"hello"}` {
		t.Fatalf("text parts = %#v", textParts)
	}
}

func TestStreamingOrchestratorLoadsDurableHistoryBeforeCurrentInput(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	now := time.Date(2026, 6, 27, 11, 0, 0, 0, time.UTC)
	_, _ = store.CreateSession(context.Background(), session.Session{
		ID:        "session-1",
		Directory: "/workspace",
		Title:     "session-1",
		CreatedAt: now,
		UpdatedAt: now,
	})
	_, _ = store.AppendMessage(context.Background(), session.Message{
		ID:        "prior-assistant",
		SessionID: "session-1",
		Role:      session.RoleAssistant,
		CreatedAt: now,
		UpdatedAt: now,
	})
	_, _ = store.AppendPart(context.Background(), session.Part{
		ID:        "prior-text",
		MessageID: "prior-assistant",
		SessionID: "session-1",
		Kind:      session.PartText,
		Payload:   []byte(`{"text":"previous"}`),
		CreatedAt: now,
		UpdatedAt: now,
	})
	var got []string
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, msg := range request.Messages {
			got = append(got, msg.Content)
		}
		return []*einoschema.Message{einoschema.AssistantMessage("next", nil)}, nil
	}))
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	if len(got) != 2 || got[0] != "previous" || got[1] != "hello" {
		t.Fatalf("provider messages = %#v", got)
	}
}

func TestStreamingOrchestratorFailsProviderErrors(t *testing.T) {
	t.Parallel()

	providerErr := model.Error{Code: "provider_rejected", Message: "bad request"}
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return nil, providerErr
	}))
	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || !errors.Is(result.Error, providerErr) {
		t.Fatalf("result = %+v", result)
	}
}

func TestStreamingOrchestratorMarksCanceledRunsInterrupted(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	started := make(chan struct{})
	orch := newTestOrchestrator(store, scriptedStreamer(func(ctx context.Context, _ model.Request) ([]*einoschema.Message, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	handle, err := orch.Start(context.Background(), Request{
		SessionID: "session-1",
		ParentID:  "user-1",
		Input:     []*einoschema.Message{einoschema.UserMessage("hello")},
		Config:    orchestratorConfig(),
	})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	<-started
	if err := handle.Interrupt(context.Background(), "test"); err != nil {
		t.Fatalf("Interrupt error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunInterrupted || !result.Interrupted {
		t.Fatalf("result = %+v", result)
	}
}

func TestStreamingOrchestratorHonorsCancellationDuringDeltaBackpressure(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	started := make(chan struct{})
	var once sync.Once
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{
			einoschema.AssistantMessage("a", nil),
			einoschema.AssistantMessage("b", nil),
			einoschema.AssistantMessage("c", nil),
		}, nil
	}))
	orch.Events = blockingSinkFunc(func(ctx context.Context, event Event) error {
		if event.Kind == EventMessageDelta {
			once.Do(func() { close(started) })
			<-ctx.Done()
		}
		return nil
	})
	orch.QueueSize = 1
	handle, err := orch.Start(context.Background(), Request{
		SessionID: "session-1",
		ParentID:  "user-1",
		Input:     []*einoschema.Message{einoschema.UserMessage("hello")},
		Config:    orchestratorConfig(),
	})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	<-started
	if err := handle.Interrupt(context.Background(), "test"); err != nil {
		t.Fatalf("Interrupt error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunInterrupted {
		t.Fatalf("result = %+v", result)
	}
}

func TestStreamingOrchestratorRetriesRetryableProviderErrors(t *testing.T) {
	t.Parallel()

	var attempts int
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		attempts++
		if attempts == 1 {
			return nil, model.Error{Code: "rate_limited", Message: "retry", Retryable: true}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("ok", nil)}, nil
	}))
	orch.Attempts = 2
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted || attempts != 2 {
		t.Fatalf("result = %+v attempts=%d", result, attempts)
	}
}

func TestStreamingOrchestratorBoundedQueueAppliesBackpressure(t *testing.T) {
	t.Parallel()

	sink := &blockingSink{delay: time.Millisecond}
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{
			einoschema.AssistantMessage("a", nil),
			einoschema.AssistantMessage("b", nil),
			einoschema.AssistantMessage("c", nil),
		}, nil
	}))
	orch.Events = sink
	orch.QueueSize = 1
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	if sink.count(EventMessageDelta) != 3 {
		t.Fatalf("delta events = %d, want 3", sink.count(EventMessageDelta))
	}
}

func TestStreamingOrchestratorFailsMalformedStreamWithoutPanic(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{nil}, nil
	}))
	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || result.Error == nil {
		t.Fatalf("result = %+v", result)
	}
}

func TestStreamingOrchestratorExecutesToolCallLoop(t *testing.T) {
	t.Parallel()

	var calls int
	store := newAdmissionStore()
	sink := &capturingSink{}
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		calls++
		for _, msg := range request.Messages {
			if msg.Role == einoschema.Tool {
				return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
			}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:   "call-1",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "echo",
				Arguments: `{"text":"hi"}`,
			},
		}})}, nil
	}))
	orch.Tools = staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "hi"}, nil
		}),
	}}}
	orch.Events = sink
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted || calls != 2 {
		t.Fatalf("result = %+v calls=%d", result, calls)
	}
	call, err := store.GetToolCall(context.Background(), "call-1")
	if err != nil {
		t.Fatalf("GetToolCall error = %v", err)
	}
	if call.Status != session.ToolCallCompleted {
		t.Fatalf("tool call = %+v", call)
	}
	var assistantMessages int
	for _, message := range store.messages {
		if message.Role == session.RoleAssistant {
			assistantMessages++
		}
	}
	if assistantMessages != 2 {
		t.Fatalf("assistant messages = %d, want 2", assistantMessages)
	}
	var toolCallParts []session.Part
	for _, part := range store.parts {
		if part.Kind == session.PartToolCall {
			toolCallParts = append(toolCallParts, part)
		}
	}
	if len(toolCallParts) != 1 || string(toolCallParts[0].Payload) != `{"id":"call-1","name":"echo","arguments":{"text":"hi"}}` {
		t.Fatalf("tool call parts = %#v", toolCallParts)
	}
	var toolEvents []Event
	for _, event := range sink.events {
		if event.Kind == EventToolCallUpdated {
			toolEvents = append(toolEvents, event)
		}
	}
	if len(toolEvents) != 3 {
		t.Fatalf("tool events = %#v, want pending/running/completed", toolEvents)
	}
	if !strings.Contains(string(toolEvents[0].Payload), `"arguments":{"text":"hi"}`) {
		t.Fatalf("tool event payload = %s", toolEvents[0].Payload)
	}
}

func TestStreamingOrchestratorGeneratesMissingToolCallIDsConsistently(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, msg := range request.Messages {
			if msg.Role == einoschema.Tool {
				return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
			}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "echo",
				Arguments: `{}`,
			},
		}})}, nil
	}))
	orch.Tools = staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "ok"}, nil
		}),
	}}}
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	var callID session.ToolCallID
	for id := range store.toolCalls {
		callID = id
	}
	if callID == "" {
		t.Fatal("tool call was not created")
	}
	for _, part := range store.parts {
		if part.Kind == session.PartToolCall && !strings.Contains(string(part.Payload), `"id":"`+string(callID)+`"`) {
			t.Fatalf("tool call payload %s does not contain generated id %s", part.Payload, callID)
		}
	}
}

func TestStreamingOrchestratorBoundsToolOutput(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, msg := range request.Messages {
			if msg.Role == einoschema.Tool {
				return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
			}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:   "call-1",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "echo",
				Arguments: `{}`,
			},
		}})}, nil
	}))
	orch.Tools = staticToolRegistry{tools: []Tool{{
		Name:      "echo",
		Retention: RetentionPolicy{MaxInlineBytes: 2, StoreExternal: true},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "hello"}, nil
		}),
	}}}
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	call, err := store.GetToolCall(context.Background(), "call-1")
	if err != nil {
		t.Fatalf("GetToolCall error = %v", err)
	}
	if string(call.Output) != `{"tool_call_id":"call-1","status":"completed","content":"he","truncated":true,"original_size":5,"inline_size":2,"external":true}` {
		t.Fatalf("tool output = %s", call.Output)
	}
}

func TestStreamingOrchestratorContinuesAfterToolFailurePayload(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, msg := range request.Messages {
			if msg.Role == einoschema.Tool && strings.Contains(msg.Content, "operational_failure") {
				return []*einoschema.Message{einoschema.AssistantMessage("recovered", nil)}, nil
			}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:   "call-1",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "echo",
				Arguments: `{}`,
			},
		}})}, nil
	}))
	orch.Tools = staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{}, errors.New("tool failed")
		}),
	}}}
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	call, err := store.GetToolCall(context.Background(), "call-1")
	if err != nil {
		t.Fatalf("GetToolCall error = %v", err)
	}
	if call.Status != session.ToolCallFailed {
		t.Fatalf("tool call = %+v", call)
	}
}

func TestStreamingOrchestratorMarksCanceledToolInterrupted(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:   "call-1",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "echo",
				Arguments: `{}`,
			},
		}})}, nil
	}))
	orch.Tools = staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{}, context.Canceled
		}),
	}}}
	result := startAndWait(t, orch)
	if result.Status != session.RunInterrupted {
		t.Fatalf("result = %+v", result)
	}
	call, err := store.GetToolCall(context.Background(), "call-1")
	if err != nil {
		t.Fatalf("GetToolCall error = %v", err)
	}
	if call.Status != session.ToolCallInterrupted {
		t.Fatalf("tool call = %+v", call)
	}
}

func TestStreamingOrchestratorFailsWhenToolLoopExceedsLimit(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:   "call-loop",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "echo",
				Arguments: `{}`,
			},
		}})}, nil
	}))
	orch.Tools = staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "again"}, nil
		}),
	}}}
	orch.ToolTurns = 1
	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || result.Error == nil {
		t.Fatalf("result = %+v", result)
	}
}

func startAndWait(t *testing.T, orch *StreamingOrchestrator) Result {
	t.Helper()
	handle, err := orch.Start(context.Background(), Request{
		SessionID: "session-1",
		ParentID:  "user-1",
		Input:     []*einoschema.Message{einoschema.UserMessage("hello")},
		Config:    orchestratorConfig(),
	})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	return <-handle.Done()
}

func newTestOrchestrator(store *admissionStore, streamer model.Streamer) *StreamingOrchestrator {
	return &StreamingOrchestrator{
		Store:     store,
		Model:     resolvedModel{streamer: streamer},
		IDs:       &sequenceIDs{},
		Clock:     func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) },
		OwnerID:   "owner-1",
		QueueSize: 2,
	}
}

func orchestratorConfig() config.Snapshot {
	selection := model.Selection{ProviderID: "fake", ModelID: "test"}
	return config.Snapshot{
		Agent: config.Agent{
			Name:    "agent",
			Model:   selection,
			Options: map[string]string{"temperature": "0"},
		},
		Model: selection,
		Metadata: map[string]string{
			"workspace_id":   "workspace-1",
			"workspace_root": "/workspace",
		},
	}
}

type resolvedModel struct {
	streamer model.Streamer
}

func (r resolvedModel) Resolve(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
	return model.Resolved{
		Provider: model.Provider{ID: "fake"},
		Model:    model.Descriptor{ID: "test", ProviderID: "fake"},
		Streamer: r.streamer,
	}, nil
}

type scriptedStreamer func(context.Context, model.Request) ([]*einoschema.Message, error)

func (s scriptedStreamer) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	messages, err := s(ctx, request)
	if err != nil {
		return nil, err
	}
	reader, writer := einoschema.Pipe[*einoschema.Message](len(messages))
	go func() {
		defer writer.Close()
		for _, msg := range messages {
			if writer.Send(msg, nil) {
				return
			}
		}
	}()
	return reader, nil
}

type sequenceIDs struct {
	mu sync.Mutex
	n  int
}

func (s *sequenceIDs) next(prefix string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return prefix + "-" + strconv.Itoa(s.n)
}

func (s *sequenceIDs) NewRunID() session.RunID         { return session.RunID(s.next("run")) }
func (s *sequenceIDs) NewMessageID() session.MessageID { return session.MessageID(s.next("message")) }
func (s *sequenceIDs) NewPartID() session.PartID       { return session.PartID(s.next("part")) }
func (s *sequenceIDs) NewToolCallID() session.ToolCallID {
	return session.ToolCallID(s.next("tool-call"))
}
func (s *sequenceIDs) NewEventID() session.EventID { return session.EventID(s.next("event")) }
func (s *sequenceIDs) NewEpochID() session.EpochID { return session.EpochID(s.next("epoch")) }

type blockingSink struct {
	mu     sync.Mutex
	events []Event
	delay  time.Duration
}

func (s *blockingSink) Emit(_ context.Context, event Event) error {
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *blockingSink) count(kind EventKind) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int
	for _, event := range s.events {
		if event.Kind == kind {
			count++
		}
	}
	return count
}

type blockingSinkFunc func(context.Context, Event) error

func (f blockingSinkFunc) Emit(ctx context.Context, event Event) error {
	return f(ctx, event)
}

type staticToolRegistry struct {
	tools []Tool
}

func (r staticToolRegistry) ResolveTools(context.Context, TurnSnapshot) ([]Tool, error) {
	return r.tools, nil
}

type orchestratorToolExecutorFunc func(context.Context, ToolCall) (ToolResult, error)

func (f orchestratorToolExecutorFunc) Execute(ctx context.Context, call ToolCall) (ToolResult, error) {
	return f(ctx, call)
}
