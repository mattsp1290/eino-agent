package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
)

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

func TestToolTransitionTransportFailureIsPostCommitBestEffort(t *testing.T) {
	store := newAdmissionStore()
	sink := &selectiveToolFailingSink{err: errors.New("transport unavailable")}
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, msg := range request.Messages {
			if msg.Role == einoschema.Tool {
				return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
			}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "call-transport", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{}`}}})}, nil
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		return ToolResult{Output: "ok"}, nil
	})}}})
	orch.events = sink
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v, want completed despite transport failure", result)
	}
	call, err := store.GetToolCall(context.Background(), "call-transport")
	if err != nil || call.Status != session.ToolCallCompleted {
		t.Fatalf("durable call = %+v, %v", call, err)
	}
	if sink.toolEvents != 3 {
		t.Fatalf("tool event delivery attempts = %d, want 3", sink.toolEvents)
	}
}

func TestToolTransitionPersistenceFailureFailsMutation(t *testing.T) {
	store := newAdmissionStore()
	store.toolTransitionErr = errors.New("persistent tool event failed")
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "call-persist", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{}`}}})}, nil
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		return ToolResult{Output: "should not run"}, nil
	})}}})
	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || result.Error == nil {
		t.Fatalf("result = %+v, want failed persistence", result)
	}
	if _, err := store.GetToolCall(context.Background(), "call-persist"); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("tool call survived failed atomic create: %v", err)
	}
}

type selectiveToolFailingSink struct {
	err        error
	toolEvents int
}

func (s *selectiveToolFailingSink) Emit(_ context.Context, event Event) error {
	if event.Kind == EventToolCallUpdated {
		s.toolEvents++
		return s.err
	}
	return nil
}

// TestStreamingOrchestratorRunFinishedCarriesRunTotalUsage pins that the
// EventRunFinished event (and Result.Usage) carries the run-total provider
// usage summed across every model stream in the run — here a two-stream
// tool-call loop. This is the EventSink-side token total that lets consumers
// persist run_attempts.tokens without reading the OTel span; the sum matches
// what the observability path aggregates onto the run span.
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
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "ok"}, nil
		}),
	}}})
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
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name:      "echo",
		Retention: RetentionPolicy{MaxInlineBytes: 2, StoreExternal: true},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "hello"}, nil
		}),
	}}})
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
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{}, errors.New("tool failed")
		}),
	}}})
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
				Arguments: `{"target":"go"}`,
			},
		}})}, nil
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name:      "echo",
		Pattern:   permissionPatternField("target"),
		Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Scope: ToolScope{
			Permissions: []string{"agui.client_tool"},
		},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			executed = true
			return ToolResult{Output: "executed"}, nil
		}),
	}}})
	orch.permissions = permissions.PolicyFunc(func(_ context.Context, request permissions.Request) (permissions.Decision, error) {
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

func TestStreamingOrchestratorPatternFailurePrecedesPolicyAndPersistence(t *testing.T) {
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID: "call-pattern-failure", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{"value":1}`},
		}})}, nil
	}))
	patternErr := errors.New("pattern failed")
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Pattern: PermissionPatternResolverFunc(func(context.Context, json.RawMessage) (string, error) {
			return "", patternErr
		}),
		Scope: ToolScope{Permissions: []string{"tool.execute"}},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			t.Fatal("executor ran after pattern failure")
			return ToolResult{}, nil
		}),
	}}})
	var policyCalls atomic.Int32
	orch.permissions = permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
		policyCalls.Add(1)
		return permissions.Decision{Action: permissions.ActionAllow}, nil
	})
	result := startAndWait(t, orch)
	if !errors.Is(result.Error, patternErr) {
		t.Fatalf("result = %+v, want pattern failure", result)
	}
	if policyCalls.Load() != 0 || len(store.toolCalls) != 0 {
		t.Fatalf("policy calls = %d, durable calls = %d", policyCalls.Load(), len(store.toolCalls))
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
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{}, context.Canceled
		}),
	}}})
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

func TestToolOutputUsesProtectedDisposition(t *testing.T) {
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
			output, _, status, errText := encodeToolOutput("call", test.result, RetentionPolicy{MaxInlineBytes: 4096}, test.disposition, nil)
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
	var executedCall ToolCall
	toolRegistry := staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			executedCall = call
			cancel()
			return ToolResult{Output: "ok"}, nil
		}),
	}}}
	orch := mustConfiguredOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
			for _, message := range request.Messages {
				if message.Role == einoschema.Tool {
					return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
				}
			}
			return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "call-cancel", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{}`}}})}, nil
		})}),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(toolRegistry)}),
		WithClock(func() time.Time { return time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC) }),
		WithOwnerID("owner-1"),
	)
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
	orchestrator := mustConfiguredOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
			for _, message := range request.Messages {
				if message.Role == einoschema.Tool {
					modelVisible = message.Content
					return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
				}
			}
			return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "call-denied", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{}`}}})}, nil
		})}),
		WithRunPlanProvider(staticRunPlanProvider{plan: plan}),
		WithPermissions(permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
			return permissions.Decision{Action: permissions.ActionDeny, Message: "blocked"}, nil
		})),
		WithClock(func() time.Time { return time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC) }), WithOwnerID("owner-1"),
	)
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
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "again"}, nil
		}),
	}}})
	orch.toolTurnsValue = 1
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
