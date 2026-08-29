package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
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
	claimed.ClaimedBy = "owner-1"
	claimed.ClaimToken = "atomic-claim"
	claimed.StartedAt = run.CreatedAt
	claimResult, err := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken}).ClaimToolCall(context.Background(), testClaimToolRequest(claimed, "event-atomic-claim", time.Minute, claimed.StartedAt))
	if err != nil {
		t.Fatal(err)
	}
	claimed = claimResult.Call
	tool := Tool{Name: claimed.Name, Retention: RetentionPolicy{MaxInlineBytes: 4096}, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		cancel()
		return ToolResult{Output: "committed"}, nil
	})}
	orchestrator := mustConfiguredOrchestrator(
		WithStore(store), WithOwnerID("owner-1"),
		WithClock(func() time.Time { return time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC) }),
	)
	plan := newTestToolPlan(staticToolRegistry{tools: []Tool{tool}})
	call := runtimeCallFromClaim(tool, claimed)
	settled, err := newRunExecution(orchestrator, plan, run).executeAndSettleClaimedTool(ctx, orchestrator.resumeSnapshot(run), tool, call, claimed, nil)
	if err != nil {
		t.Fatalf("execute and settle: %v", err)
	}
	if settled.Settlement.Status != session.ToolCallCompleted {
		t.Fatalf("settlement = %+v", settled.Settlement)
	}
	assertDurableToolResult(t, store, claimed.SessionID, claimed.ID, session.ToolCallCompleted, "committed")
}

func TestFinalToolContextPreservesPlanOrderAndIsIsolated(t *testing.T) {
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
	host := mustConfiguredOrchestrator()
	snapshot := TurnSnapshot{
		RunID: "run", SessionID: "session", EpochID: "epoch",
		Config:   config.Snapshot{Agent: config.Agent{Name: "agent", Mode: "primary"}, Metadata: map[string]string{"workspace_id": "workspace", "workspace_root": "/workspace"}},
		Model:    model.Resolved{Provider: model.Provider{ID: "provider"}, Model: model.Descriptor{ID: "model"}},
		Messages: []*einoschema.Message{einoschema.UserMessage("secret")},
	}
	preparedSnapshot, err := host.prepareSnapshot(context.Background(), newTestRunExecution(host, plan), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	calls, err := host.prepareToolCalls(context.Background(), newTestRunExecution(host, plan), preparedSnapshot, "message", []einoschema.ToolCall{{ID: "call", Function: einoschema.FunctionCall{Name: "zeta", Arguments: `{}`}}})
	if err != nil || len(calls) != 1 {
		t.Fatalf("prepared calls = %#v, %v", calls, err)
	}
	outcome := host.executeToolOutcome(context.Background(), newTestRunExecution(host, plan), calls[0].tool, calls[0].call)
	if outcome.RawError != nil {
		t.Fatal(outcome.RawError)
	}
	wantNames := []string{"zeta", "alpha"}
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
	toolRegistry := staticToolRegistry{tools: []Tool{{Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096}, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		panic("executor secret")
	})}}}
	registry := newTestExtensionRegistry(nil)
	var order []string
	var publishedIDs []session.EventID
	mount, err := registry.Mount(context.Background(), testExtensionComponent("transition-order"), extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.On(registrar, EventPublishedPoint, extension.Registration{ID: "published", Scope: extension.GlobalScope()}, func(_ context.Context, event Event) error {
			if event.Kind == EventToolCallUpdated && event.ToolCallID == "call-panic" {
				order = append(order, "published:"+toolEventStatus(event))
				publishedIDs = append(publishedIDs, event.EventID)
			}
			return nil
		}); err != nil {
			return err
		}
		if err := extension.On(registrar, ToolStartedPoint, extension.Registration{ID: "started", Scope: extension.GlobalScope()}, func(context.Context, ToolStartedNotice) error {
			order = append(order, "started")
			return nil
		}); err != nil {
			return err
		}
		return extension.On(registrar, ToolSettledPoint, extension.Registration{ID: "settled", Scope: extension.GlobalScope()}, func(context.Context, ToolSettledNotice) error {
			order = append(order, "settled")
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
	plan := newTestToolPlanWithDispatch(toolRegistry, dispatch)
	var sinkEvents []Event
	sink := EventSinkFunc(func(_ context.Context, event Event) error {
		if event.Kind == EventToolCallUpdated && event.ToolCallID == "call-panic" {
			order = append(order, "sink:"+toolEventStatus(event))
			sinkEvents = append(sinkEvents, event)
			return errors.New("transport unavailable")
		}
		return nil
	})
	orchestrator := mustConfiguredOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "call-panic", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{}`}}})}, nil
		})}),
		WithRunPlanProvider(staticRunPlanProvider{plan: plan}), WithEventSink(sink), WithOwnerID("owner-1"),
		WithClock(func() time.Time { return time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC) }),
	)
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "panic-session", ParentID: "user-1", Input: []*einoschema.Message{einoschema.UserMessage("hello")}, Config: orchestratorConfig()})
	if result.Status != session.RunFailed || !errors.Is(result.Error, errToolExecutionPanic) {
		t.Fatalf("result = %+v", result)
	}
	assertDurableToolResult(t, store, "panic-session", "call-panic", session.ToolCallFailed, "operational_failure")
	wantOrder := []string{"sink:pending", "published:pending", "sink:running", "published:running", "started", "sink:failed", "published:failed", "settled"}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("transition observer order = %v, want %v", order, wantOrder)
	}
	if len(sinkEvents) != 3 || len(publishedIDs) != 3 {
		t.Fatalf("sink events = %#v published IDs = %#v", sinkEvents, publishedIDs)
	}
	batch, err := store.ListEvents(context.Background(), "panic-session", session.EventCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var durableIDs []session.EventID
	durableIDSet := make(map[session.EventID]bool)
	for _, event := range batch.Events {
		if event.ToolCallID == "call-panic" && event.Kind == string(EventToolCallUpdated) {
			durableIDs = append(durableIDs, event.ID)
			durableIDSet[event.ID] = true
		}
	}
	for index := range sinkEvents {
		if sinkEvents[index].EventID != publishedIDs[index] || !durableIDSet[sinkEvents[index].EventID] {
			t.Fatalf("transition IDs differ: sink=%v published=%v durable=%v", sinkEvents, publishedIDs, durableIDs)
		}
	}
}

func toolEventStatus(event Event) string {
	for _, status := range []session.ToolCallStatus{session.ToolCallPending, session.ToolCallRunning, session.ToolCallCompleted, session.ToolCallFailed, session.ToolCallInterrupted} {
		if strings.Contains(string(event.Payload), `"status":"`+string(status)+`"`) {
			return string(status)
		}
	}
	return "unknown"
}

func TestPendingResumeToolPanicPublishesClaimAndSettlement(t *testing.T) {
	store, run := resumeStoreWithTool(t, "old-owner", session.ToolCallPending)
	sink := &capturingSink{}
	toolRegistry := staticToolRegistry{tools: []Tool{{Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096}, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		panic("resume executor secret")
	})}}}
	plan := newTestToolPlanWithDispatch(toolRegistry, nil)
	orchestrator := mustConfiguredOrchestrator(
		WithStore(store), WithEventSink(sink), WithOwnerID("owner-1"),
		WithClock(func() time.Time { return time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC) }),
	)
	done := make(chan Result, 1)
	orchestrator.executeResume(context.Background(), newRunExecution(orchestrator, plan, run), run, done)
	result := <-done
	if result.Status != session.RunFailed || !errors.Is(result.Error, errToolExecutionPanic) {
		t.Fatalf("result = %+v", result)
	}
	assertDurableToolResult(t, store, run.SessionID, "call-resume", session.ToolCallFailed, "operational_failure")
	var toolEvents []Event
	for _, event := range sink.events {
		if event.Kind == EventToolCallUpdated {
			toolEvents = append(toolEvents, event)
		}
	}
	if len(toolEvents) != 2 || !strings.Contains(string(toolEvents[0].Payload), string(session.ToolCallRunning)) || !strings.Contains(string(toolEvents[1].Payload), string(session.ToolCallFailed)) {
		t.Fatalf("resume tool events = %#v, want running/failed", toolEvents)
	}
}

func runtimeCallFromClaim(tool Tool, claimed session.ToolCall) ToolCall {
	return ToolCall{
		ID: claimed.ID, SessionID: claimed.SessionID, RunID: claimed.RunID, MessageID: claimed.MessageID,
		ResultMessageID: claimed.ResultMessageID, ResultPartID: claimed.ResultPartID,
		Name: claimed.Name, Scope: tool.Scope, Pattern: claimed.Pattern, Input: cloneJSON(claimed.Input),
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
