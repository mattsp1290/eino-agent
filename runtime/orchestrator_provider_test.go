package runtime

import (
	"context"
	"errors"
	"path/filepath"
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
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return nil, providerErr
	}))
	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || !errors.Is(result.Error, providerErr) {
		t.Fatalf("result = %+v", result)
	}
}

func TestFailedExecutionDoesNotEraseAdmittedHistory(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	admitted, err := (admitter{Store: store, Clock: func() time.Time { return time.Unix(1, 0) }}).admit(context.Background(), testRunAdmission())
	if err != nil {
		t.Fatalf("Admit error = %v", err)
	}
	failed := admitted.Run
	failed.Status = session.RunFailed
	failed.Error = "provider failed"
	failed.FinishedAt = failed.CreatedAt.Add(time.Second)
	if err := store.FinishRun(context.Background(), failed); err != nil {
		t.Fatalf("FinishRun error = %v", err)
	}
	batch, err := store.ListMessages(context.Background(), admitted.Session.ID, session.ReplayCursor{Limit: 10})
	if err != nil {
		t.Fatalf("ListMessages error = %v", err)
	}
	if len(batch.Messages) != 2 || batch.Messages[0].ID != admitted.UserMessage.ID || batch.Messages[1].ID != admitted.AssistantMessage.ID {
		t.Fatalf("history after failure = %#v", batch.Messages)
	}
	gotRun, err := store.GetRun(context.Background(), admitted.Run.ID)
	if err != nil {
		t.Fatalf("GetRun error = %v", err)
	}
	if gotRun.Status != session.RunFailed || gotRun.Error != "provider failed" {
		t.Fatalf("run after failure = %+v", gotRun)
	}
}

func TestStreamingOrchestratorMarksCanceledRunsInterrupted(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	started := make(chan struct{})
	orch := newTestOrchestrator(store, scriptedStreamer(func(ctx context.Context, _ model.Request) ([]*einoschema.AgenticMessage, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	handle, err := orch.Start(context.Background(), Request{
		SessionID: "session-1",
		Message:   TextUserMessage("hello"),
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

func TestStreamingOrchestratorCompletesWithBlockedInfrastructureSink(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{
			agenticTextChunk(0, "a"),
			agenticTextChunk(0, "b"),
			agenticTextChunk(0, "c"),
		}, nil
	}))
	orch.events = blockingSinkFunc(func(_ context.Context, event session.EventRecord) {
		if event.Kind == EventMessageDelta {
			once.Do(func() { close(started) })
			<-release
		}
	})
	orch.queueSize = 1
	handle, err := orch.Start(context.Background(), Request{
		SessionID: "session-1",
		Message:   TextUserMessage("hello"),
		Config:    orchestratorConfig(),
	})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	select {
	case <-started:
		close(release)
	default:
		// The bounded queue may drop every delta behind the admission event.
	}
}

func TestStreamingOrchestratorRetriesRetryableProviderErrors(t *testing.T) {
	t.Parallel()

	var attempts int
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		attempts++
		if attempts == 1 {
			return nil, model.Error{Code: "rate_limited", Message: "retry", Retryable: true}
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("ok")}, nil
	}))
	orch.attemptsValue = 2
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted || attempts != 2 {
		t.Fatalf("result = %+v attempts=%d", result, attempts)
	}
}

func TestStreamingOrchestratorRollsBackIncompleteAssistantParts(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	store.appendPartErrAt = 2
	var toolCalls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		msg := &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ContentBlocks: []*einoschema.ContentBlock{
			{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: "answer"}},
			{Type: einoschema.ContentBlockTypeReasoning, Reasoning: &einoschema.Reasoning{Text: "reasoning"}},
			{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &einoschema.FunctionToolCall{CallID: "call-atomic", Name: "echo", Arguments: `{}`}},
		}}
		return []*einoschema.AgenticMessage{msg}, nil
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		toolCalls++
		return ToolResult{Output: "unexpected"}, nil
	})}}})

	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || result.Error == nil {
		t.Fatalf("result = %+v, want failed persistence", result)
	}
	assertOnlyAdmittedUserPart(t, store.parts)
	if toolCalls != 0 {
		t.Fatalf("tool calls = %d, want 0", toolCalls)
	}
}

func TestStreamingOrchestratorRollsBackWholeTurnWhenSecondToolCreationFails(t *testing.T) {
	store := newAdmissionStore()
	store.createToolErrAt = 2
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(
			agenticToolCall("call-one", "one", `{}`),
			agenticToolCall("call-two", "two", `{}`),
		)}, nil
	}))
	var executions int
	configureTestTools(orch, staticToolRegistry{tools: []Tool{
		{Name: "one", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { executions++; return ToolResult{}, nil })},
		{Name: "two", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { executions++; return ToolResult{}, nil })},
	}})

	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || result.Error == nil {
		t.Fatalf("result = %+v, want failed persistence", result)
	}
	if len(store.toolCalls) != 0 || executions != 0 {
		t.Fatalf("rolled back turn: parts=%#v calls=%#v executions=%d", store.parts, store.toolCalls, executions)
	}
	assertOnlyAdmittedUserPart(t, store.parts)
	for _, event := range store.events {
		if event.ToolTransition == session.ToolTransitionPending {
			t.Fatalf("pending event survived rollback: %#v", event)
		}
	}
}

func TestStreamingOrchestratorBoundedQueueDropsWithoutBackpressure(t *testing.T) {
	t.Parallel()

	sink := &blockingSink{delay: time.Millisecond}
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{
			agenticTextChunk(0, "a"),
			agenticTextChunk(0, "b"),
			agenticTextChunk(0, "c"),
		}, nil
	}))
	orch.events = sink
	orch.queueSize = 1
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	if got := sink.count(EventMessageDelta); got > 3 {
		t.Fatalf("delta events = %d, want at most 3", got)
	}
}

func TestStreamingOrchestratorFailsMalformedStreamWithoutPanic(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{nil}, nil
	}))
	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || result.Error == nil {
		t.Fatalf("result = %+v", result)
	}
}

func TestStreamingOrchestratorFailsMalformedToolArgumentsWithoutPanic(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		msg := &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant, ContentBlocks: []*einoschema.ContentBlock{
			{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: "partial text"}},
			{Type: einoschema.ContentBlockTypeReasoning, Reasoning: &einoschema.Reasoning{Text: "partial reasoning"}},
			{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &einoschema.FunctionToolCall{CallID: "call-bad-json", Name: "echo", Arguments: `{"text":`}},
		}}
		return []*einoschema.AgenticMessage{msg}, nil
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
		if part.MessageID == "message-3" {
			continue
		}
		switch part.Kind {
		case session.PartAssistantGenText, session.PartReasoning, session.PartFunctionToolCall:
			t.Fatalf("assistant part persisted despite malformed arguments: kind=%s payload=%s", part.Kind, part.Payload)
		}
	}
}

func assertOnlyAdmittedUserPart(t *testing.T, parts map[session.PartID]session.Part) {
	t.Helper()
	if len(parts) != 1 {
		t.Fatalf("parts = %#v, want only admitted user part", parts)
	}
	for _, part := range parts {
		if part.MessageID != "message-3" || part.Kind != session.PartUserInputText {
			t.Fatalf("admitted user part = %#v", part)
		}
		decoded, err := session.DecodeContentParts(session.RoleUser, []session.Part{part}, session.DefaultContentLimits())
		if err != nil || len(decoded.Blocks) != 1 || decoded.Blocks[0].Text == nil || decoded.Blocks[0].Text.Text != "hello" {
			t.Fatalf("admitted user part decoded = %#v, error = %v", decoded, err)
		}
	}
}

func TestStreamingOrchestratorNormalizesEmptyToolArguments(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		for _, msg := range request.Messages {
			if msg.Role == einoschema.AgenticRoleTypeUser && isFunctionToolResultMessage(msg) {
				return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
			}
		}
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-empty-args", "echo", ""))}, nil
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
	orch := newTestOrchestrator(store, scriptedStreamer(func(ctx context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		_ = ctx
		calls++
		for _, msg := range request.Messages {
			if msg.Role == einoschema.AgenticRoleTypeUser && isFunctionToolResultMessage(msg) {
				// Second stream (after the tool result): report usage, finish.
				response := agenticAssistantText("done")
				response.ResponseMeta = &einoschema.AgenticResponseMeta{TokenUsage: &einoschema.TokenUsage{PromptTokens: 7, CompletionTokens: 3}}
				return []*einoschema.AgenticMessage{response}, nil
			}
		}
		// First stream: report usage, emit a tool call to force a second turn.
		response := agenticAssistantToolCalls(agenticToolCall("call-1", "echo", `{"text":"hi"}`))
		response.ResponseMeta = &einoschema.AgenticResponseMeta{TokenUsage: &einoschema.TokenUsage{PromptTokens: 10, CompletionTokens: 5}}
		return []*einoschema.AgenticMessage{response}, nil
	}), WithQueueSize(16))
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

	var finished []session.EventRecord
	for _, event := range sink.waitForKind(t, EventRunFinished, 1) {
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

// TestResolveStreamUsage covers the Eino-streamer usage bridge. Delta usage
// wins field by field, while the concatenated message's ResponseMeta fills
// fields that the provider delta did not report.
func TestResolveStreamUsage(t *testing.T) {
	t.Parallel()

	observed := model.Usage{InputTokens: 11, OutputTokens: 7}
	msgWithUsage := &einoschema.AgenticMessage{ResponseMeta: &einoschema.AgenticResponseMeta{
		TokenUsage: &einoschema.TokenUsage{PromptTokens: 23, CompletionTokens: 18},
	}}
	msgNoMeta := &einoschema.AgenticMessage{}

	// Delta usage wins over message metadata for fields it reports.
	if got := resolveStreamUsage(observed, msgWithUsage); got != observed {
		t.Errorf("delta-reported usage should win: got %+v, want %+v", got, observed)
	}
	// Empty delta usage falls back to message ResponseMeta.Usage.
	got := resolveStreamUsage(model.Usage{}, msgWithUsage)
	if got.InputTokens != 23 || got.OutputTokens != 18 {
		t.Errorf("client-path usage from ResponseMeta: got %+v, want input=23 output=18", got)
	}
	// No delta or message usage yields zero.
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
			Streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) { return nil, nil }),
		}, nil
	})
	orchestrator := mustConfiguredOrchestrator(WithStore(store), WithModelResolver(resolver))
	_, err := orchestrator.Start(context.Background(), Request{SessionID: "session-1", Config: orchestratorConfig(), Message: TextUserMessage("hello")})
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

// TestClassicAdapterCapabilityRejectionSurfacesAsTypedRunFailure proves the
// real model.NewClassicStreamer adapter (not a scripted fake) rejecting a
// non-representable agentic message before dispatch surfaces, end to end,
// as a failed run carrying the adapter's typed capability_unsupported error
// code and ErrCapabilityUnsupported cause -- not a generic or swallowed
// failure. The first turn persists a real assistant_gen_image content block
// (via a native agentic streamer, the only way such content legitimately
// enters durable history); the second turn switches to the classic adapter,
// which must reject that durable history block before it can dispatch.
func TestClassicAdapterCapabilityRejectionSurfacesAsTypedRunFailure(t *testing.T) {
	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "capability.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storePool.Close() }()
	ids := &sequenceIDs{}

	imageStreamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{{
			Role: einoschema.AgenticRoleTypeAssistant,
			ContentBlocks: []*einoschema.ContentBlock{{
				Type:              einoschema.ContentBlockTypeAssistantGenImage,
				AssistantGenImage: &einoschema.AssistantGenImage{URL: "https://example.test/gen.png", MIMEType: "image/png"},
			}},
		}}, nil
	})
	first := mustConfiguredOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: imageStreamer}), WithIDGenerator(ids),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
	)
	firstResult := startAndWaitRequest(t, first, Request{SessionID: "capability-session", Message: TextUserMessage("draw something"), Config: orchestratorConfig()})
	if firstResult.Status != session.RunCompleted || firstResult.Error != nil {
		t.Fatalf("first result = %+v", firstResult)
	}

	classicStreamer := model.NewClassicStreamer(&capturingChatModel{})
	second := mustConfiguredOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: classicStreamer}), WithIDGenerator(ids),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
	)
	secondResult := startAndWaitRequest(t, second, Request{SessionID: "capability-session", Message: TextUserMessage("draw another"), Config: orchestratorConfig()})
	if secondResult.Status != session.RunFailed || secondResult.Error == nil {
		t.Fatalf("second result = %+v", secondResult)
	}
	var providerErr model.Error
	if !errors.As(secondResult.Error, &providerErr) || providerErr.Code != "capability_unsupported" || !errors.Is(secondResult.Error, model.ErrCapabilityUnsupported) {
		t.Fatalf("error = %v, want capability_unsupported/ErrCapabilityUnsupported", secondResult.Error)
	}
}
