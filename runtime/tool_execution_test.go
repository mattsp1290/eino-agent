package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
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
		Messages: []*einoschema.AgenticMessage{agenticUserText("secret")},
	}
	preparedSnapshot, err := host.prepareSnapshot(context.Background(), newTestRunExecution(host, plan), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	calls, err := host.prepareToolCalls(context.Background(), newTestRunExecution(host, plan), preparedSnapshot, "message", []*einoschema.FunctionToolCall{agenticToolCall("call", "zeta", `{}`)})
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
	store, storePool, err := openTestSQLite(context.Background(), t.TempDir()+"/store.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storePool.Close() }()
	toolRegistry := staticToolRegistry{tools: []Tool{{Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096}, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		panic("executor secret")
	})}}}
	registry := newTestExtensionRegistry(nil)
	var notificationOrder []string
	// notificationIDs is parallel to notificationOrder: every extension
	// point captured here (EventPublishedPoint, ToolStartedPoint,
	// ToolSettledPoint) carries a ToolCallID, so nothing needs to be
	// filtered by content up front the way the published hook used to
	// filter by EventToolCallUpdated alone. This run creates exactly one
	// tool call, so every notification observed here belongs to it, but
	// notificationOrder/sinkEvents are still filtered down to that one id
	// (onlyToolCallID-style) below before comparing them -- capturing
	// everything unfiltered and filtering by id afterward, rather than
	// relying on "there's only ever one call", is what keeps this
	// assertion correct if a later edit adds a second call.
	var notificationIDs []session.ToolCallID
	var publishedIDs []session.EventID
	var publishedToolCallIDs []session.ToolCallID
	// The scripted provider CallID ("call-panic") is preserved separately as
	// ProviderCallID; the durable session.ToolCall.ID (and every event's
	// ToolCallID) is always a fresh mint now, so there's nothing to filter
	// events by up front -- capture the minted id from the first one seen.
	var toolCallID session.ToolCallID
	mount, err := registry.Mount(context.Background(), testExtensionComponent("transition-order"), extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.On(registrar, EventPublishedPoint, extension.Registration{ID: "published", Scope: extension.GlobalScope()}, func(_ context.Context, event session.EventRecord) error {
			if event.Kind == EventToolCallUpdated {
				if toolCallID == "" {
					toolCallID = event.ToolCallID
				}
				notificationOrder = append(notificationOrder, "published:"+toolEventStatus(event))
				notificationIDs = append(notificationIDs, event.ToolCallID)
				publishedIDs = append(publishedIDs, event.ID)
				publishedToolCallIDs = append(publishedToolCallIDs, event.ToolCallID)
			}
			return nil
		}); err != nil {
			return err
		}
		if err := extension.On(registrar, ToolStartedPoint, extension.Registration{ID: "started", Scope: extension.GlobalScope()}, func(_ context.Context, notice ToolStartedNotice) error {
			notificationOrder = append(notificationOrder, "started")
			notificationIDs = append(notificationIDs, notice.ToolCallID)
			return nil
		}); err != nil {
			return err
		}
		return extension.On(registrar, ToolSettledPoint, extension.Registration{ID: "settled", Scope: extension.GlobalScope()}, func(_ context.Context, notice ToolSettledNotice) error {
			notificationOrder = append(notificationOrder, "settled")
			notificationIDs = append(notificationIDs, notice.ToolCallID)
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
	var sinkMu sync.Mutex
	var sinkEvents []session.EventRecord
	sink := EventSinkFunc(func(_ context.Context, event session.EventRecord) {
		if event.Kind == EventToolCallUpdated {
			sinkMu.Lock()
			defer sinkMu.Unlock()
			sinkEvents = append(sinkEvents, event)
		}
	})
	orchestrator := mustConfiguredOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-panic", "echo", `{}`))}, nil
		})}),
		WithRunPlanProvider(staticRunPlanProvider{plan: plan}), WithEventSink(sink), WithOwnerID("owner-1"),
		WithClock(func() time.Time { return time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC) }),
	)
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "panic-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if result.Status != session.RunFailed || !errors.Is(result.Error, errToolExecutionPanic) {
		t.Fatalf("result = %+v", result)
	}
	if err := plan.FlushNotifications(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		sinkMu.Lock()
		count := len(sinkEvents)
		sinkMu.Unlock()
		if count == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sink event count = %d, want 3", count)
		}
		time.Sleep(time.Millisecond)
	}
	if toolCallID == "" {
		t.Fatal("tool call id was never observed via published events")
	}
	assertDurableToolResult(t, store, "panic-session", toolCallID, session.ToolCallFailed, "operational_failure")
	// Filter every captured notification/sink/published slice down to
	// toolCallID before comparing: this run creates exactly one tool call
	// today, so the filter is a no-op now, but it is what actually enforces
	// that invariant (onlyToolCallID-style) instead of silently mixing
	// events from a second call into these comparisons if one is ever added.
	var filteredOrder []string
	for index, id := range notificationIDs {
		if id == toolCallID {
			filteredOrder = append(filteredOrder, notificationOrder[index])
		}
	}
	wantOrder := []string{"published:pending", "published:running", "started", "published:failed", "settled"}
	if !reflect.DeepEqual(filteredOrder, wantOrder) {
		t.Fatalf("notification order = %v, want %v (unfiltered = %v)", filteredOrder, wantOrder, notificationOrder)
	}
	var filteredPublishedIDs []session.EventID
	for index, id := range publishedToolCallIDs {
		if id == toolCallID {
			filteredPublishedIDs = append(filteredPublishedIDs, publishedIDs[index])
		}
	}
	sinkMu.Lock()
	defer sinkMu.Unlock()
	var filteredSinkEvents []session.EventRecord
	for _, event := range sinkEvents {
		if event.ToolCallID == toolCallID {
			filteredSinkEvents = append(filteredSinkEvents, event)
		}
	}
	if len(filteredSinkEvents) != 3 || len(filteredPublishedIDs) != 3 {
		t.Fatalf("sink events = %#v published IDs = %#v (unfiltered sink = %#v, unfiltered published = %#v)", filteredSinkEvents, filteredPublishedIDs, sinkEvents, publishedIDs)
	}
	sinkEvents = filteredSinkEvents
	publishedIDs = filteredPublishedIDs
	batch, err := store.ListEvents(context.Background(), "panic-session", session.EventCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var durableIDs []session.EventID
	durableIDSet := make(map[session.EventID]bool)
	for _, event := range batch.Events {
		if event.ToolCallID == toolCallID && event.Kind == string(EventToolCallUpdated) {
			durableIDs = append(durableIDs, event.ID)
			durableIDSet[event.ID] = true
		}
	}
	for index := range sinkEvents {
		if sinkEvents[index].ID != publishedIDs[index] || !durableIDSet[sinkEvents[index].ID] {
			t.Fatalf("transition IDs differ: sink=%v published=%v durable=%v", sinkEvents, publishedIDs, durableIDs)
		}
	}
}

func TestFreshToolPanicInterruptsEveryRemainingCommittedCall(t *testing.T) {
	store := newAdmissionStore()
	var secondExecutions int
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(
			agenticToolCall("call-panic-first", "panic", `{}`),
			agenticToolCall("call-skipped-second", "second", `{}`),
		)}, nil
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{
		{Name: "panic", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { panic("boom") })},
		{Name: "second", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { secondExecutions++; return ToolResult{}, nil })},
	}})

	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || !errors.Is(result.Error, errToolExecutionPanic) {
		t.Fatalf("result = %+v", result)
	}
	if secondExecutions != 0 {
		t.Fatalf("second executor ran %d times", secondExecutions)
	}
	// The scripted provider CallIDs ("call-panic-first"/"call-skipped-second")
	// are preserved separately as ProviderCallID; the durable ID is always a
	// fresh mint, so discover each call's minted id by its tool name instead.
	first, err := store.GetToolCall(context.Background(), toolCallIDByName(t, store, "panic"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.GetToolCall(context.Background(), toolCallIDByName(t, store, "second"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != session.ToolCallFailed || second.Status != session.ToolCallInterrupted {
		t.Fatalf("terminal calls = first:%s second:%s", first.Status, second.Status)
	}
	if _, ok := store.parts[first.ResultPartID]; !ok {
		t.Fatalf("first result part %q missing", first.ResultPartID)
	}
	if _, ok := store.parts[second.ResultPartID]; !ok {
		t.Fatalf("second result part %q missing", second.ResultPartID)
	}
	run, err := store.GetRun(context.Background(), result.RunID)
	if err != nil || !run.Terminal() {
		t.Fatalf("run = %#v, %v", run, err)
	}
}

func toolEventStatus(event session.EventRecord) string {
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
	execution := newRunExecution(orchestrator, plan, run)
	orchestrator.executeResume(context.Background(), execution, run, done)
	result := <-done
	if err := execution.events.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if result.Status != session.RunFailed || !errors.Is(result.Error, errToolExecutionPanic) {
		t.Fatalf("result = %+v", result)
	}
	assertDurableToolResult(t, store, run.SessionID, "call-resume", session.ToolCallFailed, "operational_failure")
	var toolEvents []session.EventRecord
	for _, event := range sink.snapshot() {
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
		if part.ID == call.ResultPartID && part.MessageID == call.ResultMessageID && part.Kind == session.PartFunctionToolResult && strings.Contains(string(part.Payload), payloadFragment) {
			return
		}
	}
	t.Fatalf("result part %q missing from %#v", call.ResultPartID, batch.Parts)
}
