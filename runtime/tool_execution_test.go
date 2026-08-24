package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
)

func TestAtomicSettlementSurvivesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, run := resumeStoreWithTool(t, "old-owner", session.ToolCallPending)
	claimed, err := store.GetToolCall(context.Background(), "call-resume")
	if err != nil {
		t.Fatal(err)
	}
	claimed.Status = session.ToolCallRunning
	claimed.ClaimedBy = "owner-1"
	claimed.ClaimToken = "atomic-claim"
	claimed.StartedAt = run.CreatedAt
	claimed, err = store.ClaimToolCall(context.Background(), claimed)
	if err != nil {
		t.Fatal(err)
	}
	tool := Tool{Name: claimed.Name, Retention: RetentionPolicy{MaxInlineBytes: 4096}, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		cancel()
		return ToolResult{Output: "committed"}, nil
	})}
	orchestrator := &StreamingOrchestrator{
		Store: store, IDs: &sequenceIDs{}, OwnerID: "owner-1",
		Clock: func() time.Time { return time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC) },
	}
	plan := newTestToolPlan(staticToolRegistry{tools: []Tool{tool}})
	call := runtimeCallFromClaim(tool, claimed)
	settled, err := newRunExecution(orchestrator, plan).executeAndSettleClaimedTool(ctx, orchestrator.resumeSnapshot(run), tool, call, claimed, nil)
	if err != nil {
		t.Fatalf("execute and settle: %v", err)
	}
	if settled.Settlement.Status != session.ToolCallCompleted {
		t.Fatalf("settlement = %+v", settled.Settlement)
	}
	assertDurableToolResult(t, store, claimed.SessionID, claimed.ID, session.ToolCallCompleted, "committed")
}

func TestFinalToolContextIsSortedBoundedAndIsolated(t *testing.T) {
	var received ToolContext
	tools := staticToolRegistry{tools: []Tool{
		{Name: "zeta", Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			received = call.Context.Clone()
			call.Context.Turn.ToolNames[0] = "mutated"
			return ToolResult{Output: "ok"}, nil
		})},
		{Name: "alpha", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{}, nil })},
	}}
	plan := newTestToolPlan(tools)
	host := &StreamingOrchestrator{}
	snapshot := TurnSnapshot{
		RunID: "run", SessionID: "session", EpochID: "epoch",
		Config:   config.Snapshot{Agent: config.Agent{Name: "agent", Mode: "primary"}, Metadata: map[string]string{"workspace_id": "workspace", "workspace_root": "/workspace"}},
		Model:    model.Resolved{Provider: model.Provider{ID: "provider"}, Model: model.Descriptor{ID: "model"}},
		Messages: []*einoschema.Message{einoschema.UserMessage("secret")},
	}
	preparedSnapshot, err := host.prepareSnapshot(context.Background(), newRunExecution(host, plan), snapshot, "message")
	if err != nil {
		t.Fatal(err)
	}
	calls, err := host.prepareToolCalls(context.Background(), newRunExecution(host, plan), preparedSnapshot, "message", []einoschema.ToolCall{{ID: "call", Function: einoschema.FunctionCall{Name: "zeta", Arguments: `{}`}}})
	if err != nil || len(calls) != 1 {
		t.Fatalf("prepared calls = %#v, %v", calls, err)
	}
	outcome := host.executeToolOutcome(context.Background(), newRunExecution(host, plan), calls[0].tool, calls[0].call)
	if outcome.RawError != nil {
		t.Fatal(outcome.RawError)
	}
	wantNames := []string{"alpha", "zeta"}
	if !reflect.DeepEqual(received.Turn.ToolNames, wantNames) || received.Turn.SessionID != "session" || received.Turn.RunID != "run" || received.WorkspaceID != "workspace" || received.WorkspaceRoot != "/workspace" {
		t.Fatalf("tool context = %#v", received)
	}
	if !reflect.DeepEqual(calls[0].call.Context.Turn.ToolNames, wantNames) || len(calls[0].call.Context.Turn.ToolNames) != 2 {
		t.Fatalf("executor mutation leaked into runtime context: %#v", calls[0].call.Context)
	}
}

func TestFreshToolPanicSettlesBeforeFailingRun(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), t.TempDir()+"/store.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var releases atomic.Int64
	toolRegistry := staticToolRegistry{tools: []Tool{{Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096}, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		panic("executor secret")
	})}}}
	plan := newTestToolPlanWithDispatch(toolRegistry, nil, func() { releases.Add(1) })
	sink := &capturingSink{}
	orchestrator := &StreamingOrchestrator{
		Store: store,
		Model: resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "call-panic", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{}`}}})}, nil
		})},
		Plans: staticRunPlanProvider{plan: plan}, IDs: &sequenceIDs{}, Events: sink, OwnerID: "owner-1",
		Clock: func() time.Time { return time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC) },
	}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "panic-session", ParentID: "user-1", Input: []*einoschema.Message{einoschema.UserMessage("hello")}, Config: orchestratorConfig()})
	if result.Status != session.RunFailed || !errors.Is(result.Error, errToolExecutionPanic) {
		t.Fatalf("result = %+v", result)
	}
	assertDurableToolResult(t, store, "panic-session", "call-panic", session.ToolCallFailed, "operational_failure")
	if releases.Load() != 1 {
		t.Fatalf("plan releases = %d, want 1", releases.Load())
	}
	var toolEvents []Event
	for _, event := range sink.events {
		if event.Kind == EventToolCallUpdated && event.ToolCallID == "call-panic" {
			toolEvents = append(toolEvents, event)
		}
	}
	if len(toolEvents) != 3 || !strings.Contains(string(toolEvents[2].Payload), string(session.ToolCallFailed)) {
		t.Fatalf("tool events = %#v, want pending/running/failed", toolEvents)
	}
}

func TestPendingResumeToolPanicSettlesWithoutTransportEvent(t *testing.T) {
	store, run := resumeStoreWithTool(t, "old-owner", session.ToolCallPending)
	var releases atomic.Int64
	sink := &capturingSink{}
	toolRegistry := staticToolRegistry{tools: []Tool{{Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096}, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		panic("resume executor secret")
	})}}}
	plan := newTestToolPlanWithDispatch(toolRegistry, nil, func() { releases.Add(1) })
	orchestrator := &StreamingOrchestrator{
		Store: store, IDs: &sequenceIDs{}, Events: sink, OwnerID: "owner-1",
		Clock: func() time.Time { return time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC) },
	}
	done := make(chan Result, 1)
	orchestrator.executeResume(context.Background(), newRunExecution(orchestrator, plan), run, done)
	result := <-done
	if result.Status != session.RunFailed || !errors.Is(result.Error, errToolExecutionPanic) {
		t.Fatalf("result = %+v", result)
	}
	assertDurableToolResult(t, store, run.SessionID, "call-resume", session.ToolCallFailed, "operational_failure")
	if releases.Load() != 1 {
		t.Fatalf("plan releases = %d, want 1", releases.Load())
	}
	for _, event := range sink.events {
		if event.Kind == EventToolCallUpdated {
			t.Fatalf("resume emitted fresh-only tool transport event: %+v", event)
		}
	}
}

func runtimeCallFromClaim(tool Tool, claimed session.ToolCall) ToolCall {
	return ToolCall{
		ID: claimed.ID, SessionID: claimed.SessionID, RunID: claimed.RunID, MessageID: claimed.MessageID,
		ResultMessageID: claimed.ResultMessageID, ResultPartID: claimed.ResultPartID,
		Name: claimed.Name, Scope: tool.Scope, Pattern: toolPattern(claimed.Input, claimed.Name), Input: cloneJSON(claimed.Input),
	}
}

func assertDurableToolResult(t *testing.T, store *sqlitestore.Store, sessionID session.ID, callID session.ToolCallID, wantStatus session.ToolCallStatus, payloadFragment string) {
	t.Helper()
	call, err := store.GetToolCall(context.Background(), callID)
	if err != nil {
		t.Fatal(err)
	}
	if call.Status != wantStatus || !strings.Contains(string(call.Output), payloadFragment) {
		t.Fatalf("tool call = %+v", call)
	}
	if _, err := store.GetMessage(context.Background(), call.ResultMessageID); err != nil {
		t.Fatalf("result message: %v", err)
	}
	batch, err := store.ListMessages(context.Background(), sessionID, session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range batch.Parts {
		if part.ID == call.ResultPartID && part.MessageID == call.ResultMessageID && part.Kind == session.PartToolResult && strings.Contains(string(part.Payload), payloadFragment) {
			return
		}
	}
	t.Fatalf("result part %q missing from %#v", call.ResultPartID, batch.Parts)
}
