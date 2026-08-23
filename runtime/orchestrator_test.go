package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
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

func TestStreamingOrchestratorPreservesConfiguredAdmissionEventSink(t *testing.T) {
	store := newAdmissionStore()
	admissionSink := &capturingSink{}
	runtimeSink := &capturingSink{}
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "admission-events", Artifact: extension.Artifact{Name: "admission-events", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var published []EventKind
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, EventPublishedPoint, extension.Registration{ID: "published", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(_ context.Context, event Event) error {
			published = append(published, event.Kind)
			return nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanStrict}
	descriptor.Fingerprint, _ = session.FingerprintExtensionPlan(descriptor)
	orchestrator := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	}))
	orchestrator.Admit = &Admitter{Events: admissionSink}
	orchestrator.Events = runtimeSink
	orchestrator.Plans = staticRunPlanProvider{plan: &RunPlan{Descriptor: descriptor, Dispatch: dispatch}}

	result := startAndWait(t, orchestrator)
	if result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	if len(admissionSink.events) != 1 || admissionSink.events[0].Kind != EventRunStarted {
		t.Fatalf("admission events = %#v", admissionSink.events)
	}
	for _, event := range runtimeSink.events {
		if event.Kind == EventRunStarted {
			t.Fatalf("orchestrator sink received custom admission event: %#v", runtimeSink.events)
		}
	}
	if len(runtimeSink.events) == 0 {
		t.Fatal("orchestrator sink did not receive execution events")
	}
	var publishedStarts int
	for _, kind := range published {
		if kind == EventRunStarted {
			publishedStarts++
		}
	}
	if publishedStarts != 1 {
		t.Fatalf("published admission starts = %d, all events = %v", publishedStarts, published)
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

func TestStreamingOrchestratorFailsMalformedToolArgumentsWithoutPanic(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		msg := einoschema.AssistantMessage("partial text", []einoschema.ToolCall{{
			ID:   "call-bad-json",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "echo",
				Arguments: `{"text":`,
			},
		}})
		msg.ReasoningContent = "partial reasoning"
		return []*einoschema.Message{msg}, nil
	}))
	orch.Tools = staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			t.Fatal("executor should not run for malformed tool arguments")
			return ToolResult{}, nil
		}),
	}}}
	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || result.Error == nil {
		t.Fatalf("result = %+v", result)
	}
	var providerErr model.Error
	if !errors.As(result.Error, &providerErr) || providerErr.Code != "malformed_provider_tool_call" {
		t.Fatalf("result error = %#v, want malformed_provider_tool_call", result.Error)
	}
	if _, err := store.GetToolCall(context.Background(), "call-bad-json"); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("tool call persisted despite malformed arguments: %v", err)
	}
	for _, part := range store.parts {
		switch part.Kind {
		case session.PartText, session.PartReasoning, session.PartToolCall:
			t.Fatalf("assistant part persisted despite malformed arguments: kind=%s payload=%s", part.Kind, part.Payload)
		}
	}
}

func TestStreamingOrchestratorNormalizesEmptyToolArguments(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, msg := range request.Messages {
			if msg.Role == einoschema.Tool {
				return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
			}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:   "call-empty-args",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name: "echo",
			},
		}})}, nil
	}))
	orch.Tools = staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			if string(call.Input) != `{}` {
				t.Fatalf("tool input = %s, want {}", call.Input)
			}
			return ToolResult{Output: "ok"}, nil
		}),
	}}}
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	call, err := store.GetToolCall(context.Background(), "call-empty-args")
	if err != nil {
		t.Fatalf("GetToolCall error = %v", err)
	}
	if string(call.Input) != `{}` {
		t.Fatalf("persisted input = %s, want {}", call.Input)
	}
}

func TestNormalizedToolArgumentsCompatibilityShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "empty", input: "", want: `{}`},
		{name: "object", input: `{"text":"hi"}`, want: `{"text":"hi"}`},
		{name: "null", input: `null`, want: `null`},
		{name: "array", input: `[]`, want: `[]`},
		{name: "string", input: `"value"`, want: `"value"`},
		{name: "malformed", input: `{"text":`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizedToolArguments(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizedToolArguments error = %v", err)
			}
			if string(got) != tt.want {
				t.Fatalf("normalizedToolArguments(%q) = %s, want %s", tt.input, got, tt.want)
			}
		})
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

// TestStreamingOrchestratorRunFinishedCarriesRunTotalUsage pins that the
// EventRunFinished event (and Result.Usage) carries the run-total provider
// usage summed across every model stream in the run — here a two-stream
// tool-call loop. This is the EventSink-side token total that lets consumers
// persist run_attempts.tokens without reading the OTel span; the sum matches
// what the observability path aggregates onto the run span.
func TestStreamingOrchestratorRunFinishedCarriesRunTotalUsage(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	sink := &capturingSink{}
	var calls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(ctx context.Context, request model.Request) ([]*einoschema.Message, error) {
		calls++
		for _, msg := range request.Messages {
			if msg.Role == einoschema.Tool {
				// Second stream (after the tool result): report usage, finish.
				request.Observer.OnProviderEnd(ctx, model.Response{Usage: model.Usage{InputTokens: 7, OutputTokens: 3}})
				return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
			}
		}
		// First stream: report usage, emit a tool call to force a second turn.
		request.Observer.OnProviderEnd(ctx, model.Response{Usage: model.Usage{InputTokens: 10, OutputTokens: 5}})
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:       "call-1",
			Type:     "function",
			Function: einoschema.FunctionCall{Name: "echo", Arguments: `{"text":"hi"}`},
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

	// Run total = sum across both streams: input 10+7=17, output 5+3=8.
	const wantInput, wantOutput = int64(17), int64(8)
	if result.Usage.InputTokens != wantInput || result.Usage.OutputTokens != wantOutput {
		t.Fatalf("result.Usage = %+v, want input=%d output=%d", result.Usage, wantInput, wantOutput)
	}

	var finished []Event
	for _, event := range sink.events {
		if event.Kind == EventRunFinished {
			finished = append(finished, event)
		}
	}
	if len(finished) != 1 {
		t.Fatalf("run_finished events = %d, want exactly 1", len(finished))
	}
	if finished[0].Usage.InputTokens != wantInput || finished[0].Usage.OutputTokens != wantOutput {
		t.Fatalf("run_finished Usage = %+v, want input=%d output=%d", finished[0].Usage, wantInput, wantOutput)
	}
}

// TestResolveStreamUsage covers the client-path usage bridge: when no Streamer
// adapter reported usage through the observer (the default resolved.Client
// path), the token usage must be taken from the concatenated message's
// ResponseMeta.Usage so run consumers still see non-zero tokens. When the
// observer DID report usage (a Streamer adapter), that wins.
func TestResolveStreamUsage(t *testing.T) {
	t.Parallel()

	observed := model.Usage{InputTokens: 11, OutputTokens: 7}
	msgWithUsage := &einoschema.Message{ResponseMeta: &einoschema.ResponseMeta{
		Usage: &einoschema.TokenUsage{PromptTokens: 23, CompletionTokens: 18},
	}}
	msgNoMeta := &einoschema.Message{}

	// Observer reported usage (Streamer path) → observer wins, message ignored.
	if got := resolveStreamUsage(observed, msgWithUsage); got != observed {
		t.Errorf("observer-reported usage should win: got %+v, want %+v", got, observed)
	}
	// Client path: observer empty, message carries ResponseMeta.Usage → use it.
	got := resolveStreamUsage(model.Usage{}, msgWithUsage)
	if got.InputTokens != 23 || got.OutputTokens != 18 {
		t.Errorf("client-path usage from ResponseMeta: got %+v, want input=23 output=18", got)
	}
	// Empty observer + no usable message metadata → zero.
	if got := resolveStreamUsage(model.Usage{}, msgNoMeta); got != (model.Usage{}) {
		t.Errorf("no usage anywhere should be zero: got %+v", got)
	}
	if got := resolveStreamUsage(model.Usage{}, nil); got != (model.Usage{}) {
		t.Errorf("nil message should be zero: got %+v", got)
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

func TestStreamingOrchestratorEnforcesToolPermissionPolicy(t *testing.T) {
	t.Parallel()

	var executed bool
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, msg := range request.Messages {
			if msg.Role == einoschema.Tool {
				return []*einoschema.Message{einoschema.AssistantMessage("handled", nil)}, nil
			}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:   "call-1",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "echo",
				Arguments: `{"permission_pattern":"go"}`,
			},
		}})}, nil
	}))
	orch.Tools = staticToolRegistry{tools: []Tool{{
		Name:      "echo",
		Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Scope: ToolScope{
			Permissions: []string{"agui.client_tool"},
		},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			executed = true
			return ToolResult{Output: "executed"}, nil
		}),
	}}}
	orch.Permissions = permissions.PolicyFunc(func(_ context.Context, request permissions.Request) (permissions.Decision, error) {
		if request.Pattern != "go" {
			t.Fatalf("permission pattern = %q, want go", request.Pattern)
		}
		return permissions.Decision{Action: permissions.ActionDeny, Message: "blocked"}, nil
	})
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	if executed {
		t.Fatal("tool executor ran despite denial")
	}
	call, err := store.GetToolCall(context.Background(), "call-1")
	if err != nil {
		t.Fatalf("GetToolCall error = %v", err)
	}
	if !strings.Contains(string(call.Output), "blocked") {
		t.Fatalf("tool output = %s, want denied payload", call.Output)
	}
}

func TestStreamingOrchestratorToolMiddlewareRewritesAndPatchesEverySettlementView(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	var mu sync.Mutex
	var order []string
	var events []Event
	var permissionRequest permissions.Request
	var executedCall ToolCall
	var modelVisible string
	appendOrder := func(value string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, value)
	}

	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, msg := range request.Messages {
			if msg.Role == einoschema.Tool {
				modelVisible = msg.Content
				return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
			}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:   "call-middleware",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "echo",
				Arguments: `{"permission_pattern":"original","value":0}`,
			},
		}})}, nil
	}))
	orch.Tools = staticToolRegistry{tools: []Tool{{
		Name:      "echo",
		Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Scope:     ToolScope{Permissions: []string{"tool.echo"}},
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			executedCall = call
			return ToolResult{Output: "executed"}, nil
		}),
	}}}
	orch.Middleware = []ToolMiddleware{
		ToolMiddlewareFuncs{
			Before: func(_ context.Context, _ Tool, call ToolCall) (json.RawMessage, error) {
				appendOrder("before-1")
				var input map[string]any
				_ = json.Unmarshal(call.Input, &input)
				input["permission_pattern"] = "rewritten"
				input["value"] = 1
				return json.Marshal(input)
			},
			After: func(_ context.Context, _ Tool, _ ToolCall, result ToolResult, _ error) (ToolResult, error) {
				appendOrder("after-1")
				result.Output += ":after-1"
				return result, nil
			},
		},
		ToolMiddlewareFuncs{
			Before: func(_ context.Context, _ Tool, call ToolCall) (json.RawMessage, error) {
				appendOrder("before-2")
				var input map[string]any
				_ = json.Unmarshal(call.Input, &input)
				input["value"] = 2
				return json.Marshal(input)
			},
			After: func(_ context.Context, _ Tool, _ ToolCall, result ToolResult, _ error) (ToolResult, error) {
				appendOrder("after-2")
				result.Output += ":after-2"
				return result, nil
			},
		},
	}
	orch.Permissions = permissions.PolicyFunc(func(_ context.Context, request permissions.Request) (permissions.Decision, error) {
		permissionRequest = request
		return permissions.Decision{Action: permissions.ActionAllow}, nil
	})
	orch.Events = EventSinkFunc(func(_ context.Context, event Event) error {
		if event.Kind == EventToolCallUpdated {
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		}
		return nil
	})

	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	mu.Lock()
	gotOrder := append([]string(nil), order...)
	gotEvents := append([]Event(nil), events...)
	mu.Unlock()
	if strings.Join(gotOrder, ",") != "before-1,before-2,after-2,after-1" {
		t.Fatalf("middleware order = %v", gotOrder)
	}
	if string(executedCall.Input) != `{"permission_pattern":"rewritten","value":2}` || executedCall.Pattern != "rewritten" {
		t.Fatalf("executed call = %+v", executedCall)
	}
	if permissionRequest.Pattern != "rewritten" {
		t.Fatalf("permission request = %+v", permissionRequest)
	}
	call, err := store.GetToolCall(context.Background(), "call-middleware")
	if err != nil {
		t.Fatalf("GetToolCall error = %v", err)
	}
	if string(call.Input) != string(executedCall.Input) || !strings.Contains(string(call.Output), "executed:after-2:after-1") {
		t.Fatalf("durable call = %+v", call)
	}
	if len(gotEvents) != 3 {
		t.Fatalf("tool events = %d, want 3", len(gotEvents))
	}
	for _, event := range gotEvents {
		if !strings.Contains(string(event.Payload), `"permission_pattern":"rewritten"`) || !strings.Contains(string(event.Payload), `"value":2`) {
			t.Errorf("event payload did not use rewritten input: %s", event.Payload)
		}
	}
	if !strings.Contains(string(gotEvents[2].Payload), "executed:after-2:after-1") {
		t.Errorf("settled event payload = %s", gotEvents[2].Payload)
	}
	var callPartRewritten, resultPartFound bool
	for _, part := range store.parts {
		if part.Kind == session.PartToolCall {
			callPartRewritten = strings.Contains(string(part.Payload), `"permission_pattern":"rewritten"`) && strings.Contains(string(part.Payload), `"value":2`)
		}
		if part.Kind == session.PartToolResult {
			resultPartFound = strings.Contains(string(part.Payload), "executed:after-2:after-1")
		}
	}
	if !callPartRewritten || !resultPartFound || !strings.Contains(modelVisible, "executed:after-2:after-1") {
		t.Fatalf("rewritten call part=%v patched result part=%v model-visible=%s", callPartRewritten, resultPartFound, modelVisible)
	}
}

func TestStreamingOrchestratorToolMiddlewareErrorSettlesFailedAndContinues(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	var executed []string
	var afterCalls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, msg := range request.Messages {
			if msg.Role == einoschema.Tool {
				return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
			}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{
			{ID: "call-failed", Type: "function", Function: einoschema.FunctionCall{Name: "fail", Arguments: `{}`}},
			{ID: "call-sibling", Type: "function", Function: einoschema.FunctionCall{Name: "ok", Arguments: `{}`}},
		})}, nil
	}))
	orch.Tools = staticToolRegistry{tools: []Tool{
		{Name: "fail", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			executed = append(executed, "fail")
			return ToolResult{}, nil
		})},
		{Name: "ok", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			executed = append(executed, "ok")
			return ToolResult{Output: "ok"}, nil
		})},
	}}
	orch.Middleware = []ToolMiddleware{ToolMiddlewareFuncs{
		Before: func(_ context.Context, _ Tool, call ToolCall) (json.RawMessage, error) {
			if call.Name == "fail" {
				return call.Input, errors.New("middleware rejected input")
			}
			return call.Input, nil
		},
		After: func(_ context.Context, _ Tool, call ToolCall, result ToolResult, _ error) (ToolResult, error) {
			if call.Name == "fail" {
				afterCalls++
			}
			return result, nil
		},
	}}
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted || strings.Join(executed, ",") != "ok" {
		t.Fatalf("result = %+v, executed = %v", result, executed)
	}
	failed, _ := store.GetToolCall(context.Background(), "call-failed")
	sibling, _ := store.GetToolCall(context.Background(), "call-sibling")
	if failed.Status != session.ToolCallFailed || !strings.Contains(failed.Error, "middleware rejected") || sibling.Status != session.ToolCallCompleted {
		t.Fatalf("failed=%+v sibling=%+v", failed, sibling)
	}
	if afterCalls != 0 {
		t.Fatalf("AfterToolCall invoked %d time(s) for preparation failure", afterCalls)
	}
}

func TestToolPrepareExtensionFailureSkipsLegacyAfterAndTransformsResult(t *testing.T) {
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "prepare-failure", Artifact: extension.Artifact{Name: "prepare-failure", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var transformCalls int
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.Use(registrar, ToolPreparePoint, extension.Registration{ID: "reject", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(context.Context, PreparedToolCall, extension.Next[PreparedToolCall, PreparedToolCall]) (PreparedToolCall, error) {
			return PreparedToolCall{}, errors.New("extension rejected preparation")
		}); err != nil {
			return err
		}
		return extension.Use(registrar, ToolResultTransformPoint, extension.Registration{ID: "transform", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(_ context.Context, outcome ToolOutcome, _ extension.Next[ToolOutcome, ToolOutcome]) (ToolOutcome, error) {
			transformCalls++
			outcome.Result.Output = "transformed preparation failure"
			return outcome, nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}

	store := newAdmissionStore()
	var executorCalls, afterCalls int
	var modelVisible string
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, message := range request.Messages {
			if message.Role == einoschema.Tool {
				modelVisible = message.Content
				return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
			}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "call-prepare-failure", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{}`}}})}, nil
	}))
	orch.Tools = staticToolRegistry{tools: []Tool{{Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096}, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		executorCalls++
		return ToolResult{Output: "executed"}, nil
	})}}}
	orch.Middleware = []ToolMiddleware{ToolMiddlewareFuncs{After: func(_ context.Context, _ Tool, _ ToolCall, result ToolResult, _ error) (ToolResult, error) {
		afterCalls++
		return result, nil
	}}}
	orch.Plans = staticRunPlanProvider{plan: &RunPlan{Dispatch: dispatch}}

	result := startAndWait(t, orch)
	if result.Error != nil || result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	call, err := store.GetToolCall(context.Background(), "call-prepare-failure")
	if err != nil {
		t.Fatal(err)
	}
	if executorCalls != 0 || afterCalls != 0 || transformCalls != 1 {
		t.Fatalf("executor=%d after=%d transform=%d", executorCalls, afterCalls, transformCalls)
	}
	if call.Status != session.ToolCallFailed || !strings.Contains(string(call.Output), `"status":"operational_failure"`) || !strings.Contains(modelVisible, "tool execution failed") {
		t.Fatalf("call=%+v model-visible=%q", call, modelVisible)
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

func TestEncodeToolOutputUsesProtectedDisposition(t *testing.T) {
	for _, test := range []struct {
		name        string
		disposition ToolDisposition
		result      ToolResult
		wantStatus  session.ToolCallStatus
		wantPayload string
	}{
		{name: "denied", disposition: ToolDenied, result: ToolResult{Output: "blocked"}, wantStatus: session.ToolCallFailed, wantPayload: "expected_failure"},
		{name: "approval required", disposition: ToolApprovalRequired, result: ToolResult{Output: "approve"}, wantStatus: session.ToolCallFailed, wantPayload: "expected_failure"},
		{name: "model visible interruption", disposition: ToolInterrupted, result: ToolResult{Output: "interrupted"}, wantStatus: session.ToolCallInterrupted, wantPayload: "interrupted"},
	} {
		t.Run(test.name, func(t *testing.T) {
			output, status, errText := encodeToolOutput("call", test.result, RetentionPolicy{MaxInlineBytes: 4096}, test.disposition, nil)
			if status != test.wantStatus || errText != "" || !strings.Contains(string(output), `"status":"`+test.wantPayload+`"`) || !strings.Contains(string(output), test.result.Output) {
				t.Fatalf("output=%s status=%s error=%q", output, status, errText)
			}
		})
	}
}

func TestStreamingOrchestratorStrictSettlementSurvivesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() { _ = store.Close() }()
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanStrict, Entries: []session.ExtensionPlanEntry{{InstanceID: "tools", Kind: session.ExtensionTool, Required: true, CapabilityID: "echo/tool"}}}
	descriptor.Fingerprint, _ = session.FingerprintExtensionPlan(descriptor)
	var executedCall ToolCall
	toolRegistry := staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			executedCall = call
			cancel()
			return ToolResult{Output: "ok"}, nil
		}),
	}}}
	orch := &StreamingOrchestrator{
		Store: store,
		Model: resolvedModel{streamer: scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
			for _, message := range request.Messages {
				if message.Role == einoschema.Tool {
					return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
				}
			}
			return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "call-cancel", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{}`}}})}, nil
		})},
		Plans:   staticRunPlanProvider{plan: &RunPlan{Tools: toolRegistry, Descriptor: descriptor}},
		IDs:     &sequenceIDs{},
		Clock:   func() time.Time { return time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC) },
		OwnerID: "owner-1",
	}
	handle, err := orch.Start(ctx, Request{SessionID: "session-cancel", ParentID: "user-1", Input: []*einoschema.Message{einoschema.UserMessage("hello")}, Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	<-handle.Done()
	call, err := store.GetToolCall(context.Background(), "call-cancel")
	if err != nil {
		t.Fatalf("GetToolCall error = %v", err)
	}
	if call.Status != session.ToolCallCompleted {
		t.Fatalf("tool call status = %s, want completed", call.Status)
	}
	if executedCall.ResultMessageID != call.ResultMessageID || executedCall.ResultPartID != call.ResultPartID || executedCall.ResultMessageID == "" || executedCall.ResultPartID == "" {
		t.Fatalf("runtime call reservations = %+v, durable call = %+v", executedCall, call)
	}
}

func TestStreamingOrchestratorPreservesDeniedDispositionAfterFreshResultTransform(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var notices []ToolSettledNotice
	var executed atomic.Bool
	toolRegistry := staticToolRegistry{tools: []Tool{{
		Name:      "echo",
		Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			executed.Store(true)
			return ToolResult{}, nil
		}),
	}}}
	plan := transformedPermissionRunPlan(t, toolRegistry, &notices)
	var modelVisible string
	orchestrator := &StreamingOrchestrator{
		Store: store,
		Model: resolvedModel{streamer: scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
			for _, message := range request.Messages {
				if message.Role == einoschema.Tool {
					modelVisible = message.Content
					return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
				}
			}
			return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "call-denied", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{}`}}})}, nil
		})},
		Plans: staticRunPlanProvider{plan: plan},
		Permissions: permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
			return permissions.Decision{Action: permissions.ActionDeny, Message: "blocked"}, nil
		}),
		IDs: &sequenceIDs{}, Clock: func() time.Time { return time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC) }, OwnerID: "owner-1",
	}
	handle, err := orchestrator.Start(ctx, Request{SessionID: "session-denied", ParentID: "user", Input: []*einoschema.Message{einoschema.UserMessage("hello")}, Config: orchestratorConfig()})
	if err != nil {
		t.Fatal(err)
	}
	result := <-handle.Done()
	if result.Error != nil || result.Status != session.RunCompleted || executed.Load() {
		t.Fatalf("run result = %+v", result)
	}
	call, err := store.GetToolCall(ctx, "call-denied")
	if err != nil {
		t.Fatal(err)
	}
	if call.Status != session.ToolCallFailed || call.Error != "" || !strings.Contains(string(call.Output), `"status":"expected_failure"`) || !strings.Contains(string(call.Output), "transformed denial") {
		t.Fatalf("durable denied call = %+v", call)
	}
	if !strings.Contains(modelVisible, `"status":"expected_failure"`) || !strings.Contains(modelVisible, "transformed denial") {
		t.Fatalf("model-visible denied result = %s", modelVisible)
	}
	if len(notices) != 1 || notices[0].Status != session.ToolCallFailed || notices[0].Result.Metadata["permission_status"] != "denied" {
		t.Fatalf("settled notices = %#v", notices)
	}
}

func TestResumePreservesDeniedDispositionAfterFreshResultTransform(t *testing.T) {
	ctx := context.Background()
	store, run := resumeStoreWithTool(t, "dead-owner", session.ToolCallPending)
	var notices []ToolSettledNotice
	var executed atomic.Bool
	toolRegistry := staticToolRegistry{tools: []Tool{{
		Name:      "echo",
		Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			executed.Store(true)
			return ToolResult{}, nil
		}),
	}}}
	plan := transformedPermissionRunPlan(t, toolRegistry, &notices)
	orchestrator := &StreamingOrchestrator{
		Store: store,
		Permissions: permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
			return permissions.Decision{Action: permissions.ActionDeny, Message: "blocked"}, nil
		}),
		IDs: &sequenceIDs{}, Clock: func() time.Time { return time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC) }, OwnerID: "owner-1",
	}
	result := orchestrator.resumeRun(withRunPlan(ctx, plan), run)
	if result.Error != nil || executed.Load() {
		t.Fatalf("resume result = %+v", result)
	}
	call, err := store.GetToolCall(ctx, "call-resume")
	if err != nil {
		t.Fatal(err)
	}
	if call.Status != session.ToolCallFailed || call.Error != "" || !strings.Contains(string(call.Output), `"status":"expected_failure"`) || !strings.Contains(string(call.Output), "transformed denial") {
		t.Fatalf("resumed denied call = %+v", call)
	}
	if len(notices) != 1 || notices[0].Status != session.ToolCallFailed || notices[0].Result.Metadata["permission_status"] != "denied" {
		t.Fatalf("settled notices = %#v", notices)
	}
}

func transformedPermissionRunPlan(t *testing.T, tools ToolRegistry, notices *[]ToolSettledNotice) *RunPlan {
	t.Helper()
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "permission-transform", Artifact: extension.Artifact{Name: "permission-transform", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.Use(registrar, ToolResultTransformPoint, extension.Registration{ID: "transform", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(ctx context.Context, input ToolOutcome, next extension.Next[ToolOutcome, ToolOutcome]) (ToolOutcome, error) {
			outcome, err := next(ctx, input)
			outcome.Result = ToolResult{Output: "transformed denial"}
			return outcome, err
		}); err != nil {
			return err
		}
		return extension.On(registrar, ToolSettledPoint, extension.Registration{ID: "settled", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(_ context.Context, notice ToolSettledNotice) error {
			*notices = append(*notices, notice)
			return nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	descriptor := strictToolDescriptor(t)
	plan := &RunPlan{Descriptor: descriptor, Dispatch: dispatch, Tools: tools}
	t.Cleanup(func() { _ = mount.Close(context.Background()) })
	t.Cleanup(func() { plan.release() })
	return plan
}

func TestResumeToolLifecycleNotificationsFollowDurableClaim(t *testing.T) {
	for _, test := range []struct {
		name string
		call session.ToolCallStatus
		want []string
	}{
		{name: "pending", call: session.ToolCallPending, want: []string{"started", "settled"}},
		{name: "running", call: session.ToolCallRunning, want: []string{"settled"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := extension.NewRegistry(nil)
			component := extension.Component{InstanceID: "resume-lifecycle", Artifact: extension.Artifact{Name: "resume-lifecycle", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
			var notices []string
			mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
				if err := extension.On(registrar, ToolStartedPoint, extension.Registration{ID: "started", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(context.Context, ToolStartedNotice) error {
					notices = append(notices, "started")
					return nil
				}); err != nil {
					return err
				}
				return extension.On(registrar, ToolSettledPoint, extension.Registration{ID: "settled", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(context.Context, ToolSettledNotice) error {
					notices = append(notices, "settled")
					return nil
				})
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = mount.Close(context.Background()) }()
			dispatch, err := registry.Snapshot(extension.GlobalScope())
			if err != nil {
				t.Fatal(err)
			}
			defer dispatch.Release()

			store, run := resumeStoreWithTool(t, "old-owner", test.call)
			orch := &StreamingOrchestrator{
				Store: store,
				Tools: staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
					return ToolResult{Output: "ok"}, nil
				})}}},
				IDs:     &sequenceIDs{},
				Clock:   func() time.Time { return time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC) },
				OwnerID: "new-owner",
			}
			result := orch.resumeRun(withRunPlan(context.Background(), &RunPlan{Dispatch: dispatch}), run)
			if result.Error != nil {
				t.Fatalf("resumeRun result = %+v", result)
			}
			if strings.Join(notices, ",") != strings.Join(test.want, ",") {
				t.Fatalf("notices = %v, want %v", notices, test.want)
			}
		})
	}
}

func TestResumeReconciliationNotifiesAfterDurableSettlement(t *testing.T) {
	store, run, settlement := resumeStoreWithUnreconciledSettlement(t)
	reconciling := &reconciliationStore{Store: store, settlements: []session.ToolSettlement{settlement}}
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "resume-reconciliation", Artifact: extension.Artifact{Name: "resume-reconciliation", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var notices []ToolSettledNotice
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, ToolSettledPoint, extension.Registration{ID: "settled", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(_ context.Context, notice ToolSettledNotice) error {
			notices = append(notices, notice)
			return nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer dispatch.Release()
	descriptor := strictToolDescriptor(t)
	orchestrator := &StreamingOrchestrator{Store: reconciling, IDs: &sequenceIDs{}, OwnerID: "new-owner"}

	result := orchestrator.resumeRun(withRunPlan(context.Background(), &RunPlan{Descriptor: descriptor, Dispatch: dispatch}), run)
	if result.Error != nil {
		t.Fatalf("resume result = %+v", result)
	}
	if reconciling.settleCalls != 1 || len(notices) != 1 {
		t.Fatalf("settle calls=%d notices=%#v", reconciling.settleCalls, notices)
	}
	notice := notices[0]
	if notice.SessionID != run.SessionID || notice.RunID != run.ID || notice.ToolCallID != settlement.ID || notice.ToolName != "echo" || notice.Status != session.ToolCallCompleted || notice.Result.Output != "recovered" || string(notice.Result.Structured) != `{"ok":true}` || notice.Error != (ClassifiedError{}) {
		t.Fatalf("reconciled notice = %#v", notice)
	}
	if _, err := store.GetMessage(context.Background(), settlement.ResultMessage.ID); err != nil {
		t.Fatalf("notification preceded durable settlement: %v", err)
	}
}

func TestResumeReconciliationFailureDoesNotRefreshRunLease(t *testing.T) {
	listErr := errors.New("list reconciliation failed")
	settleErr := errors.New("apply reconciliation failed")
	for _, test := range []struct {
		name      string
		listErr   error
		settleErr error
		wantErr   error
	}{
		{name: "list", listErr: listErr, wantErr: listErr},
		{name: "settle", settleErr: settleErr, wantErr: settleErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, run, settlement := resumeStoreWithUnreconciledSettlement(t)
			reconciling := &reconciliationStore{Store: store, settlements: []session.ToolSettlement{settlement}, listErr: test.listErr, settleErr: test.settleErr}
			orchestrator := &StreamingOrchestrator{Store: reconciling, IDs: &sequenceIDs{}, OwnerID: "new-owner", Clock: func() time.Time { return run.LeaseUntil.Add(time.Hour) }}

			result := orchestrator.resumeRun(withRunPlan(context.Background(), &RunPlan{Descriptor: strictToolDescriptor(t)}), run)
			if !errors.Is(result.Error, test.wantErr) || result.Status != session.RunFailed {
				t.Fatalf("resume result = %+v", result)
			}
			persisted, err := store.GetRun(context.Background(), run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if persisted.Status != run.Status || persisted.OwnerID != run.OwnerID || !persisted.LeaseUntil.Equal(run.LeaseUntil) {
				t.Fatalf("reconciliation failure refreshed durable run: before=%+v after=%+v", run, persisted)
			}
		})
	}
}

type reconciliationStore struct {
	*sqlitestore.Store
	settlements []session.ToolSettlement
	listErr     error
	settleErr   error
	settleCalls int
}

func (s *reconciliationStore) ListUnreconciledToolSettlements(context.Context, session.RunID) ([]session.ToolSettlement, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return append([]session.ToolSettlement(nil), s.settlements...), nil
}

func (s *reconciliationStore) SettleToolCall(ctx context.Context, settlement session.ToolSettlement) error {
	s.settleCalls++
	if s.settleErr != nil {
		return s.settleErr
	}
	return s.Store.SettleToolCall(ctx, settlement)
}

func strictToolDescriptor(t *testing.T) session.ExtensionPlanDescriptor {
	t.Helper()
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanStrict, Entries: []session.ExtensionPlanEntry{{InstanceID: "tools", Kind: session.ExtensionTool, Required: true, CapabilityID: "echo/tool"}}}
	var err error
	descriptor.Fingerprint, err = session.FingerprintExtensionPlan(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func resumeStoreWithUnreconciledSettlement(t *testing.T) (*sqlitestore.Store, session.Run, session.ToolSettlement) {
	t.Helper()
	store, run := resumeStoreWithTool(t, "old-owner", session.ToolCallPending)
	call, err := store.GetToolCall(context.Background(), "call-resume")
	if err != nil {
		t.Fatal(err)
	}
	call.Status = session.ToolCallRunning
	call.ClaimedBy = "old-owner"
	call.ClaimToken = "reconcile-token"
	call.ResultMessageID = "reconciled-message"
	call.ResultPartID = "reconciled-part"
	call.StartedAt = run.CreatedAt
	call, err = store.ClaimToolCall(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}
	completedAt := run.CreatedAt.Add(time.Second)
	output := mustJSON(toolOutputPayload{ToolCallID: string(call.ID), Status: "completed", Content: "recovered", Structured: json.RawMessage(`{"ok":true}`)})
	settlement := session.ToolSettlement{
		ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken, Status: session.ToolCallCompleted, Output: output, CompletedAt: completedAt,
		ResultMessage: session.Message{ID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, ParentID: call.MessageID, Role: session.RoleTool, CreatedAt: completedAt, UpdatedAt: completedAt},
		ResultPart:    session.Part{ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, Kind: session.PartToolResult, Payload: output, CreatedAt: completedAt, UpdatedAt: completedAt},
	}
	return store, run, settlement
}

func TestStreamingOrchestratorResumeClaimsPendingToolOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()
	now := time.Date(2026, 6, 28, 14, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: "session-resume", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{
		ID:         "run-resume",
		SessionID:  "session-resume",
		OwnerID:    "owner-1",
		LeaseUntil: now.Add(-time.Minute),
		Agent:      "agent",
		ProviderID: "fake",
		ModelID:    "test",
		Status:     session.RunPending,
		Config:     map[string]string{"workspace_id": "workspace-1", "workspace_root": "/workspace"},
		CreatedAt:  now,
	})
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	if _, err := store.AppendMessage(ctx, session.Message{ID: "assistant-resume", SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	if _, err := store.CreateToolCall(ctx, session.ToolCall{
		ID:        "call-resume",
		SessionID: run.SessionID,
		RunID:     run.ID,
		MessageID: "assistant-resume",
		Name:      "echo",
		Input:     []byte(`{"text":"hi"}`),
		Status:    session.ToolCallPending,
	}); err != nil {
		t.Fatalf("create tool call: %v", err)
	}
	var executions atomic.Int64
	orch := &StreamingOrchestrator{
		Store: store,
		Tools: staticToolRegistry{tools: []Tool{{Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096}, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			executions.Add(1)
			time.Sleep(10 * time.Millisecond)
			return ToolResult{Output: "ok"}, nil
		})}}},
		IDs:     &sequenceIDs{},
		Clock:   func() time.Time { return now },
		OwnerID: "owner-1",
	}

	start := make(chan struct{})
	results := make(chan Result, 2)
	for range 2 {
		go func() {
			<-start
			handle, err := orch.Resume(context.Background(), run.ID)
			if err != nil {
				results <- Result{RunID: run.ID, Status: session.RunFailed, Error: err}
				return
			}
			results <- <-handle.Done()
		}()
	}
	close(start)
	first := <-results
	second := <-results
	if first.Error != nil && second.Error != nil {
		t.Fatalf("resume results = %+v / %+v", first, second)
	}
	if executions.Load() != 1 {
		t.Fatalf("tool executions = %d, want 1", executions.Load())
	}
	call, err := store.GetToolCall(ctx, "call-resume")
	if err != nil {
		t.Fatalf("get tool call: %v", err)
	}
	if call.Status != session.ToolCallCompleted {
		t.Fatalf("tool call status = %s, want completed", call.Status)
	}
	finished, err := store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if finished.Status != session.RunInterrupted {
		t.Fatalf("run status = %s, want interrupted", finished.Status)
	}
	if _, err := store.ActiveRun(ctx, run.SessionID); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("active run err = %v, want ErrNotFound", err)
	}
	batch, err := store.ListMessages(ctx, run.SessionID, session.ReplayCursor{Limit: 10})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	var toolResults int
	for _, part := range batch.Parts {
		if part.Kind == session.PartToolResult {
			toolResults++
		}
	}
	if toolResults != 1 {
		t.Fatalf("tool result parts = %d, want 1", toolResults)
	}
}

func TestStreamingOrchestratorResumeTakesStaleRunOwnership(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, run := resumeStoreWithTool(t, "dead-owner", session.ToolCallPending)
	now := time.Date(2026, 6, 28, 14, 0, 0, 0, time.UTC)
	var executions atomic.Int64
	orch := &StreamingOrchestrator{
		Store: store,
		Tools: staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			executions.Add(1)
			return ToolResult{Output: "ok"}, nil
		})}}},
		IDs:     &sequenceIDs{},
		Clock:   func() time.Time { return now },
		OwnerID: "owner-1",
	}
	handle, err := orch.Resume(ctx, run.ID)
	if err != nil {
		t.Fatalf("Resume error: %v", err)
	}
	result := <-handle.Done()
	if result.Error != nil {
		t.Fatalf("resume result = %+v", result)
	}
	finished, err := store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if finished.OwnerID != "owner-1" || finished.Status != session.RunInterrupted {
		t.Fatalf("finished run = %+v, want owner-1 interrupted", finished)
	}
	if executions.Load() != 1 {
		t.Fatalf("tool executions = %d, want 1", executions.Load())
	}
}

func TestStreamingOrchestratorResumeSkipsBeforeAndRunsAfterForPendingExecution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, run := resumeStoreWithTool(t, "dead-owner", session.ToolCallPending)
	defer func() { _ = store.Close() }()
	var before, after atomic.Int64
	orch := &StreamingOrchestrator{
		Store: store,
		Tools: staticToolRegistry{tools: []Tool{{Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096}, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "executed"}, nil
		})}}},
		Middleware: []ToolMiddleware{ToolMiddlewareFuncs{
			Before: func(_ context.Context, _ Tool, call ToolCall) (json.RawMessage, error) {
				before.Add(1)
				return call.Input, nil
			},
			After: func(_ context.Context, _ Tool, _ ToolCall, result ToolResult, _ error) (ToolResult, error) {
				after.Add(1)
				result.Output = "patched"
				return result, nil
			},
		}},
		IDs: &sequenceIDs{}, OwnerID: "owner-1",
	}
	handle, err := orch.Resume(ctx, run.ID)
	if err != nil {
		t.Fatalf("Resume error = %v", err)
	}
	result := <-handle.Done()
	if result.Error != nil || before.Load() != 0 || after.Load() != 1 {
		t.Fatalf("result=%+v before=%d after=%d", result, before.Load(), after.Load())
	}
	call, err := store.GetToolCall(ctx, "call-resume")
	if err != nil || !strings.Contains(string(call.Output), "patched") {
		t.Fatalf("call=%+v error=%v", call, err)
	}
}

func TestStreamingOrchestratorResumeInterruptedRunningCallSkipsMiddleware(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, run := resumeStoreWithTool(t, "owner-1", session.ToolCallRunning)
	defer func() { _ = store.Close() }()
	var before, after atomic.Int64
	orch := &StreamingOrchestrator{
		Store: store,
		Tools: staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "must not execute"}, nil
		})}}},
		Middleware: []ToolMiddleware{ToolMiddlewareFuncs{
			Before: func(_ context.Context, _ Tool, call ToolCall) (json.RawMessage, error) {
				before.Add(1)
				return call.Input, nil
			},
			After: func(_ context.Context, _ Tool, _ ToolCall, result ToolResult, _ error) (ToolResult, error) {
				after.Add(1)
				return result, nil
			},
		}},
		IDs: &sequenceIDs{}, OwnerID: "owner-1",
	}
	handle, err := orch.Resume(ctx, run.ID)
	if err != nil {
		t.Fatalf("Resume error = %v", err)
	}
	result := <-handle.Done()
	if result.Error != nil || before.Load() != 0 || after.Load() != 0 {
		t.Fatalf("result=%+v before=%d after=%d", result, before.Load(), after.Load())
	}
	call, err := store.GetToolCall(ctx, "call-resume")
	if err != nil || call.Status != session.ToolCallInterrupted {
		t.Fatalf("call=%+v error=%v", call, err)
	}
}

func TestStreamingOrchestratorResumeDoesNotReexecuteRunningTool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, run := resumeStoreWithTool(t, "owner-1", session.ToolCallRunning)
	now := time.Date(2026, 6, 28, 14, 0, 0, 0, time.UTC)
	var executions atomic.Int64
	orch := &StreamingOrchestrator{
		Store: store,
		Tools: staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			executions.Add(1)
			return ToolResult{Output: "ok"}, nil
		})}}},
		IDs:     &sequenceIDs{},
		Clock:   func() time.Time { return now },
		OwnerID: "owner-1",
	}
	handle, err := orch.Resume(ctx, run.ID)
	if err != nil {
		t.Fatalf("Resume error: %v", err)
	}
	result := <-handle.Done()
	if result.Error != nil {
		t.Fatalf("resume result = %+v", result)
	}
	if executions.Load() != 0 {
		t.Fatalf("tool executions = %d, want 0", executions.Load())
	}
	call, err := store.GetToolCall(ctx, "call-resume")
	if err != nil {
		t.Fatalf("get tool call: %v", err)
	}
	if call.Status != session.ToolCallInterrupted {
		t.Fatalf("tool status = %s, want interrupted", call.Status)
	}
}

func resumeStoreWithTool(t *testing.T, owner string, status session.ToolCallStatus) (*sqlitestore.Store, session.Run) {
	t.Helper()
	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	now := time.Date(2026, 6, 28, 14, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: "session-resume", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{
		ID:         "run-resume",
		SessionID:  "session-resume",
		OwnerID:    owner,
		LeaseUntil: now.Add(-time.Minute),
		Agent:      "agent",
		ProviderID: "fake",
		ModelID:    "test",
		Status:     session.RunPending,
		Config:     map[string]string{"workspace_id": "workspace-1", "workspace_root": "/workspace"},
		CreatedAt:  now,
	})
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	if _, err := store.AppendMessage(ctx, session.Message{ID: "assistant-resume", SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	call := session.ToolCall{
		ID:              "call-resume",
		SessionID:       run.SessionID,
		RunID:           run.ID,
		MessageID:       "assistant-resume",
		ResultMessageID: "result-resume",
		ResultPartID:    "part-resume",
		Name:            "echo",
		Input:           []byte(`{"text":"hi"}`),
		Status:          session.ToolCallPending,
	}
	created, err := store.CreateToolCall(ctx, call)
	if err != nil {
		t.Fatalf("create tool call: %v", err)
	}
	if status == session.ToolCallRunning {
		created.Status = session.ToolCallRunning
		created.ClaimedBy = owner
		created.ClaimToken = "claim-resume"
		created.StartedAt = now
		if _, err := store.ClaimToolCall(ctx, created); err != nil {
			t.Fatalf("claim tool call: %v", err)
		}
	}
	return store, run
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
