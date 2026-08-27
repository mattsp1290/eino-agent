package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

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
	orch.events = blockingSinkFunc(func(ctx context.Context, event Event) error {
		if event.Kind == EventMessageDelta {
			once.Do(func() { close(started) })
			<-ctx.Done()
		}
		return nil
	})
	orch.queueSize = 1
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
	orch.attemptsValue = 2
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
	orch.events = sink
	orch.queueSize = 1
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
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			t.Fatal("executor should not run for malformed tool arguments")
			return ToolResult{}, nil
		}),
	}}})
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
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			if string(call.Input) != `{}` {
				t.Fatalf("tool input = %s, want {}", call.Input)
			}
			return ToolResult{Output: "ok"}, nil
		}),
	}}})
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

func TestNormalizedToolArgumentsRequiresObjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "empty", input: "", want: `{}`},
		{name: "object", input: `{"text":"hi"}`, want: `{"text":"hi"}`},
		{name: "canonical object", input: `{ "z": 1, "a": 2 }`, want: `{"a":2,"z":1}`},
		{name: "duplicate top-level key", input: `{"text":"a","text":"b"}`, wantErr: true},
		{name: "null", input: `null`, wantErr: true},
		{name: "array", input: `[]`, wantErr: true},
		{name: "string", input: `"value"`, wantErr: true},
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
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "hi"}, nil
		}),
	}}})
	orch.events = sink
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

// TestResolveStreamUsage covers the Eino-streamer usage bridge: when no observer
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

func TestStartRejectsInvalidResolvedModelBeforeHistoryReads(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	resolver := model.ResolverFunc(func(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
		return model.Resolved{
			Provider: model.Provider{ID: "wrong-provider"},
			Model:    model.Descriptor{ID: "test", ProviderID: "wrong-provider"},
			Streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) { return nil, nil }),
		}, nil
	})
	orchestrator := mustConfiguredOrchestrator(WithStore(store), WithModelResolver(resolver))
	_, err := orchestrator.Start(context.Background(), Request{SessionID: "session-1", Config: orchestratorConfig(), Input: []*einoschema.Message{einoschema.UserMessage("hello")}})
	if !errors.Is(err, model.ErrInvalidResolution) {
		t.Fatalf("Start error = %v, want ErrInvalidResolution", err)
	}
	if store.listMessagesCalls.Load() != 0 {
		t.Fatalf("invalid resolver output triggered %d history reads", store.listMessagesCalls.Load())
	}
	if len(store.sessions) != 0 || len(store.runs) != 0 || len(store.events) != 0 {
		t.Fatal("invalid resolver output caused admission side effects")
	}
}
