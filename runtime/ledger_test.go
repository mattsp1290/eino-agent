package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
)

func TestLedgerProjectionEqualsSubmittedRequestAndExcludesCredentials(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var submitted model.Request
	streamer := scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		var cloneErr error
		submitted, cloneErr = request.Clone()
		if cloneErr != nil {
			return nil, cloneErr
		}
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	})
	orchestrator, err := NewStreamingOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
		WithModelRequestSafeOptions("temperature"),
		WithClock(func() time.Time { return time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	config := orchestratorConfig()
	config.Agent.SystemPrompt = "audited system"
	config.Agent.Options["SECRET_TOKEN"] = "credential-sentinel"
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "ledger-session", Message: UserMessage{Content: "hello"}, Config: config})
	if result.Error != nil {
		t.Fatalf("result = %#v", result)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 1 {
		t.Fatalf("records = %#v, %v", batch, err)
	}
	record := batch.Records[0]
	if record.State != session.ModelRequestCompleted || record.System != "audited system" {
		t.Fatalf("record = %#v", record)
	}
	_, audited, hash, err := auditModelRequest(submitted, []string{"temperature"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantMessages, _ := json.Marshal(audited.Messages)
	wantTools, _ := json.Marshal(audited.Tools)
	wantConfig, _ := json.Marshal(audited.SafeCallConfig)
	if !bytes.Equal(record.Messages, wantMessages) || !bytes.Equal(record.Tools, wantTools) || !bytes.Equal(record.SafeCallConfig, wantConfig) || record.ContentSHA256 != hash {
		t.Fatalf("record projection does not equal submitted request: %#v", record)
	}
	raw, _ := json.Marshal(record)
	if bytes.Contains(raw, []byte("credential-sentinel")) || bytes.Contains(raw, []byte("SECRET_TOKEN")) {
		t.Fatalf("credential leaked into ledger: %s", raw)
	}
}

func TestModelRequestLedgerPersistsAndSetsIdempotencyKeyByDefault(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var submitted model.Request
	streamer := scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		submitted = request
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	})
	orchestrator, err := NewStreamingOrchestrator(WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()))
	if err != nil {
		t.Fatal(err)
	}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "ledger-default-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	if result.Error != nil || submitted.IdempotencyKey == "" {
		t.Fatalf("result=%#v idempotency_key=%q", result, submitted.IdempotencyKey)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 1 || batch.Records[0].ID != session.ModelRequestID(submitted.IdempotencyKey) || batch.Records[0].State != session.ModelRequestCompleted {
		t.Fatalf("records=%#v error=%v", batch.Records, err)
	}
}

func TestLedgerRecordsRetryAttemptsAndTerminalFailure(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var mu sync.Mutex
	calls := 0
	streamer := scriptedStreamer(func(_ context.Context, _ model.Request) ([]*einoschema.Message, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return nil, model.Error{Code: "temporary", Message: "temporary", Retryable: true}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	})
	orchestrator, err := NewStreamingOrchestrator(WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()), WithAttempts(2))
	if err != nil {
		t.Fatal(err)
	}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "retry-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	if result.Error != nil {
		t.Fatalf("result = %#v", result)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 2 || batch.Records[0].State != session.ModelRequestFailed || batch.Records[1].State != session.ModelRequestCompleted || batch.Records[0].Attempt != 1 || batch.Records[1].Attempt != 2 {
		t.Fatalf("retry records = %#v, %v", batch.Records, err)
	}
}

func TestLedgerRetriesOnlyFailedProviderStepAfterSettledTool(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	providerCalls := 0
	toolExecutions := 0
	streamer := scriptedStreamer(func(_ context.Context, _ model.Request) ([]*einoschema.Message, error) {
		providerCalls++
		switch providerCalls {
		case 1:
			return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
				ID: "call-once", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{}`},
			}})}, nil
		case 2:
			return nil, model.Error{Code: "temporary", Message: "retry second step", Retryable: true}
		default:
			return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
		}
	})
	orchestrator, err := NewStreamingOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(emptyTestRunPlanProvider()), WithAttempts(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	configureTestTools(orchestrator, staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		toolExecutions++
		return ToolResult{Output: "ok"}, nil
	})}}})

	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "tool-retry-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	if result.Error != nil || result.Status != session.RunCompleted || providerCalls != 3 || toolExecutions != 1 {
		t.Fatalf("result=%#v provider calls=%d tool executions=%d", result, providerCalls, toolExecutions)
	}
	call, err := store.GetToolCall(context.Background(), "call-once")
	if err != nil || call.Status != session.ToolCallCompleted {
		t.Fatalf("tool call=%#v error=%v", call, err)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 3 {
		t.Fatalf("records=%#v error=%v", batch.Records, err)
	}
	want := map[[2]int]session.ModelRequestState{
		{1, 1}: session.ModelRequestCompleted,
		{2, 1}: session.ModelRequestFailed,
		{2, 2}: session.ModelRequestCompleted,
	}
	for _, record := range batch.Records {
		key := [2]int{record.Step, record.Attempt}
		if want[key] != record.State {
			t.Fatalf("record=%#v want state=%q", record, want[key])
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Fatalf("missing ledger records: %#v", want)
	}
}

func TestLedgerDoesNotRetryAfterLiveDeltas(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var sequence []string
	var completed []ModelCompletedNotice
	plan, cleanup := modelLifecycleNoticePlan(t, &sequence, &completed)
	defer cleanup()
	var attempts int
	streamer := deltaStreamerFunc(func(context.Context, model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
		attempts++
		reader, writer := einoschema.Pipe[model.StreamDelta](3)
		_ = writer.Send(model.StreamDelta{Message: einoschema.AssistantMessage("partial-a", nil), Usage: model.Usage{InputTokens: 3}}, nil)
		_ = writer.Send(model.StreamDelta{Message: einoschema.AssistantMessage("partial-b", nil), Usage: model.Usage{InputTokens: 3, OutputTokens: 2}}, nil)
		_ = writer.Send(model.StreamDelta{Message: einoschema.AssistantMessage("ignored", nil), Usage: model.Usage{InputTokens: 3, OutputTokens: 4, ReasoningTokens: 1}}, model.Error{Code: "temporary", Message: "do not retry", Retryable: true})
		writer.Close()
		return reader, nil
	})
	orchestrator, err := NewStreamingOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(staticRunPlanProvider{plan: plan}), WithAttempts(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "partial-usage-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	cleanup()
	want := session.Usage{InputTokens: 3, OutputTokens: 4, ReasoningTokens: 1}
	if result.Status != session.RunFailed || result.Error == nil || result.Usage != want || attempts != 1 {
		t.Fatalf("result=%#v attempts=%d want usage=%#v", result, attempts, want)
	}
	if len(completed) != 1 || completed[0].Usage != want || completed[0].Error.Code == "" {
		t.Fatalf("completed notices = %#v", completed)
	}
	batch, err := store.ListEvents(context.Background(), "partial-usage-session", session.EventCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var finished []session.EventRecord
	for _, event := range batch.Events {
		if event.Kind == session.RunSettlementEventKind {
			finished = append(finished, event)
		}
	}
	if len(finished) != 1 || finished[0].Usage != (session.Usage{InputTokens: 3, OutputTokens: 4, ReasoningTokens: 1}) {
		t.Fatalf("run-finished events = %#v", finished)
	}
}

func TestLedgerCancellationAfterDispatchSettlesFailed(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	started := make(chan struct{})
	streamer := deltaStreamerFunc(func(ctx context.Context, _ model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
		reader, writer := einoschema.Pipe[model.StreamDelta](1)
		close(started)
		go func() {
			defer writer.Close()
			<-ctx.Done()
			writer.Send(model.StreamDelta{}, ctx.Err())
		}()
		return reader, nil
	})
	orchestrator, err := NewStreamingOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
	)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := orchestrator.Start(context.Background(), Request{SessionID: "cancel-ledger-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := handle.Interrupt(context.Background(), "test cancellation"); err != nil {
		t.Fatal(err)
	}
	result := <-handle.Done()
	if result.Status != session.RunInterrupted {
		t.Fatalf("result = %#v", result)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 1 || batch.Records[0].State != session.ModelRequestFailed {
		t.Fatalf("canceled ledger records = %#v, %v", batch.Records, err)
	}
}

func TestTerminalLedgerFailureOverridesProviderResultAndRetainsUsage(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	updateErr := errors.New("terminal ledger update failed")
	failingStore := &terminalUpdateFailingStore{Store: store, err: updateErr}
	streamer := deltaStreamerFunc(func(context.Context, model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
		reader, writer := einoschema.Pipe[model.StreamDelta](1)
		_ = writer.Send(model.StreamDelta{Message: einoschema.AssistantMessage("done", nil), Usage: model.Usage{InputTokens: 4, OutputTokens: 2}}, nil)
		writer.Close()
		return reader, nil
	})
	orchestrator, err := NewStreamingOrchestrator(
		WithStore(failingStore), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
	)
	if err != nil {
		t.Fatal(err)
	}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "terminal-ledger-failure-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	if !errors.Is(result.Error, updateErr) || result.Usage != (session.Usage{InputTokens: 4, OutputTokens: 2}) {
		t.Fatalf("result = %#v", result)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 1 || batch.Records[0].State != session.ModelRequestDispatchStarted {
		t.Fatalf("ledger records = %#v, %v", batch.Records, err)
	}
}

func TestLedgerMarksPanickingDispatchedRequestFailed(t *testing.T) {
	const secret = "provider-secret-invoke-value"
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var sequence []string
	var completed []ModelCompletedNotice
	plan, cleanup := modelLifecycleNoticePlan(t, &sequence, &completed)
	defer cleanup()

	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		panic(secret)
	})
	orchestrator, err := NewStreamingOrchestrator(WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()))
	if err != nil {
		t.Fatal(err)
	}
	orchestrator.plans = staticRunPlanProvider{plan: plan}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "panic-ledger-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	cleanup()
	if result.Status != session.RunFailed || result.Error == nil || result.Error.Error() != providerStreamPanicMessage || strings.Contains(result.Error.Error(), secret) {
		t.Fatalf("result = %#v", result)
	}
	run, err := store.GetRun(context.Background(), result.RunID)
	if err != nil || run.Error != providerStreamPanicMessage || strings.Contains(run.Error, secret) {
		t.Fatalf("durable run = %#v, %v", run, err)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 1 || batch.Records[0].State != session.ModelRequestFailed || batch.Records[0].ErrorCode != "operation_failed" {
		t.Fatalf("records = %#v, %v", batch.Records, err)
	}
	if strings.Join(sequence, ",") != "requested,completed" || len(completed) != 1 || completed[0].Error.Code == "" {
		t.Fatalf("model lifecycle sequence = %#v, completed = %#v", sequence, completed)
	}
}

func TestLedgerRetainsPartialStateAfterReceivePanic(t *testing.T) {
	const secret = "provider-secret-second-receive"
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var sequence []string
	var completed []ModelCompletedNotice
	plan, cleanup := modelLifecycleNoticePlan(t, &sequence, &completed)
	defer cleanup()
	var attempts int
	streamer := deltaStreamerFunc(func(context.Context, model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
		attempts++
		origin := einoschema.StreamReaderFromArray([]model.StreamDelta{
			{Message: einoschema.AssistantMessage("partial", nil), Usage: model.Usage{InputTokens: 5, OutputTokens: 2}},
			{Message: einoschema.AssistantMessage("panic", nil), Usage: model.Usage{InputTokens: 5, OutputTokens: 4}},
		})
		converted := 0
		return einoschema.StreamReaderWithConvert(origin, func(delta model.StreamDelta) (model.StreamDelta, error) {
			converted++
			if converted == 2 {
				panic(secret)
			}
			return delta, nil
		}), nil
	})
	orchestrator, err := NewStreamingOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(staticRunPlanProvider{plan: plan}), WithAttempts(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "receive-panic-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	cleanup()
	wantUsage := session.Usage{InputTokens: 5, OutputTokens: 2}
	if result.Status != session.RunFailed || result.Error == nil || result.Error.Error() != providerStreamPanicMessage || strings.Contains(result.Error.Error(), secret) || result.Usage != wantUsage || attempts != 1 {
		t.Fatalf("result=%#v attempts=%d", result, attempts)
	}
	run, err := store.GetRun(context.Background(), result.RunID)
	if err != nil || run.Error != providerStreamPanicMessage || strings.Contains(run.Error, secret) {
		t.Fatalf("durable run=%#v error=%v", run, err)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 1 || batch.Records[0].State != session.ModelRequestFailed || batch.Records[0].ErrorCode != "operation_failed" {
		t.Fatalf("records=%#v error=%v", batch.Records, err)
	}
	if strings.Join(sequence, ",") != "requested,completed" || len(completed) != 1 || completed[0].Usage != wantUsage || completed[0].Error.Code == "" {
		t.Fatalf("sequence=%#v completed=%#v", sequence, completed)
	}
}

func TestModelLifecycleNotificationsSkipDispatchStartFailure(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	updateErr := errors.New("dispatch start update failed")
	failingStore := &dispatchStartFailingStore{Store: store, err: updateErr}
	var sequence []string
	var completed []ModelCompletedNotice
	plan, cleanup := modelLifecycleNoticePlan(t, &sequence, &completed)
	defer cleanup()
	called := false
	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		called = true
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	})
	orchestrator, err := NewStreamingOrchestrator(
		WithStore(failingStore), WithModelResolver(resolvedModel{streamer: streamer}),
		WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()))
	if err != nil {
		t.Fatal(err)
	}
	orchestrator.plans = staticRunPlanProvider{plan: plan}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "dispatch-start-failure-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	cleanup()
	if !errors.Is(result.Error, updateErr) || called || len(sequence) != 0 || len(completed) != 0 {
		t.Fatalf("result=%#v adapter_called=%t sequence=%#v completed=%#v", result, called, sequence, completed)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 1 || batch.Records[0].State != session.ModelRequestFailed {
		t.Fatalf("failed dispatch-start records=%#v error=%v", batch.Records, err)
	}
}

func TestModelLifecycleNotificationsPairOnSuccess(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var sequence []string
	var completed []ModelCompletedNotice
	plan, cleanup := modelLifecycleNoticePlan(t, &sequence, &completed)
	defer cleanup()
	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	})
	orchestrator, err := NewStreamingOrchestrator(WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()))
	if err != nil {
		t.Fatal(err)
	}
	orchestrator.plans = staticRunPlanProvider{plan: plan}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "lifecycle-success-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	cleanup()
	if result.Error != nil || strings.Join(sequence, ",") != "requested,completed" || len(completed) != 1 || completed[0].Error.Code != "" {
		t.Fatalf("result=%#v sequence=%#v completed=%#v", result, sequence, completed)
	}
}

func TestLedgerRecordsToolFollowUpAsNextStep(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var calls int
	streamer := scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		calls++
		if calls == 1 {
			return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "ledger-tool-call", Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{"text":"hello"}`}}})}, nil
		}
		foundResult := false
		for _, message := range request.Messages {
			foundResult = foundResult || message.Role == einoschema.Tool
		}
		if !foundResult {
			return nil, errors.New("tool result missing from follow-up request")
		}
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	})
	tool := Tool{Name: "echo", Info: &einoschema.ToolInfo{Name: "echo", Desc: "echo"}, Retention: RetentionPolicy{MaxInlineBytes: 4096}, Executor: runtimeToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
		return ToolResult{Output: string(call.Input)}, nil
	})}
	orchestrator, err := NewStreamingOrchestrator(WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{tools: []Tool{tool}})}), WithIDGenerator(&sequenceIDs{}))
	if err != nil {
		t.Fatal(err)
	}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "tool-follow-up-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	if result.Error != nil {
		t.Fatalf("result = %#v", result)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 2 || batch.Records[0].Step != 1 || batch.Records[1].Step != 2 || batch.Records[0].Attempt != 1 || batch.Records[1].Attempt != 1 {
		t.Fatalf("tool follow-up records = %#v, %v", batch.Records, err)
	}
}

func TestUnsafeProviderOutputFailsBeforeSecondRequest(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*einoschema.Message)
	}{
		{name: "extra", mutate: func(message *einoschema.Message) {
			message.Extra = map[string]any{"credential": "sentinel"}
		}},
		{name: "deprecated multi content", mutate: func(message *einoschema.Message) {
			//nolint:staticcheck // The ownership boundary must reject this field.
			message.MultiContent = []einoschema.ChatMessagePart{{Type: einoschema.ChatMessagePartTypeText, Text: "legacy"}}
		}},
		{name: "streaming metadata", mutate: func(message *einoschema.Message) {
			message.AssistantGenMultiContent = []einoschema.MessageOutputPart{{
				Type: einoschema.ChatMessagePartTypeText, Text: "partial",
				StreamingMeta: &einoschema.MessageStreamingMeta{Index: 0},
			}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			calls := 0
			streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
				calls++
				message := einoschema.AssistantMessage("", []einoschema.ToolCall{{
					ID: "unsafe-call", Type: "function",
					Function: einoschema.FunctionCall{Name: "echo", Arguments: `{"text":"hello"}`},
				}})
				test.mutate(message)
				return []*einoschema.Message{message}, nil
			})
			tool := Tool{Name: "echo", Info: &einoschema.ToolInfo{Name: "echo"}, Executor: runtimeToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
				return ToolResult{Output: "hello"}, nil
			})}
			orchestrator, err := NewStreamingOrchestrator(
				WithStore(store),
				WithModelResolver(resolvedModel{streamer: streamer}),
				WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{tools: []Tool{tool}})}),
				WithIDGenerator(&sequenceIDs{}),
			)
			if err != nil {
				t.Fatal(err)
			}
			result := startAndWaitRequest(t, orchestrator, Request{SessionID: session.ID("unsafe-output-" + strings.ReplaceAll(test.name, " ", "-")), Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
			if result.Status != session.RunFailed || result.Error == nil || calls != 1 {
				t.Fatalf("result=%#v adapter calls=%d", result, calls)
			}
			run, err := store.GetRun(context.Background(), result.RunID)
			if err != nil || run.Status != session.RunFailed {
				t.Fatalf("durable run=%#v error=%v", run, err)
			}
			batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
			if err != nil || len(batch.Records) != 1 || batch.Records[0].Step != 1 || batch.Records[0].State != session.ModelRequestCompleted {
				t.Fatalf("model request records=%#v error=%v", batch.Records, err)
			}
		})
	}
}

func TestLedgerAuditFailureAfterAdmissionSettlesRunWithoutDispatch(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	called := false
	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		called = true
		return []*einoschema.Message{einoschema.AssistantMessage("unexpected", nil)}, nil
	})
	orchestrator, err := NewStreamingOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
		WithModelRequestMaxBytes(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "audit-failure-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	if !errors.Is(result.Error, session.ErrModelRequestTooLarge) || result.Status != session.RunFailed || called {
		t.Fatalf("result=%#v provider_called=%t", result, called)
	}
	run, err := store.GetRun(context.Background(), result.RunID)
	if err != nil || run.Status != session.RunFailed {
		t.Fatalf("durable run=%#v error=%v", run, err)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 0 {
		t.Fatalf("model request records=%#v error=%v", batch.Records, err)
	}
}

type dispatchStartFailingStore struct {
	session.Store
	err error
}

type terminalUpdateFailingStore struct {
	session.Store
	err error
}

func (s *terminalUpdateFailingStore) WithinTx(ctx context.Context, fn func(context.Context, session.Store) error) error {
	return s.Store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
		return fn(ctx, &terminalUpdateFailingStore{Store: tx, err: s.err})
	})
}

func (s *terminalUpdateFailingStore) Execution(fence session.RunFence) session.ExecutionStore {
	return &terminalUpdateFailingExecution{ExecutionStore: s.Store.Execution(fence), err: s.err}
}

type terminalUpdateFailingExecution struct {
	session.ExecutionStore
	err error
}

func (s *terminalUpdateFailingExecution) UpdateModelRequest(ctx context.Context, record session.ModelRequestRecord) error {
	if record.State == session.ModelRequestCompleted || record.State == session.ModelRequestFailed {
		return s.err
	}
	return s.ExecutionStore.UpdateModelRequest(ctx, record)
}

func (s *dispatchStartFailingStore) WithinTx(ctx context.Context, fn func(context.Context, session.Store) error) error {
	return s.Store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
		return fn(ctx, &dispatchStartFailingStore{Store: tx, err: s.err})
	})
}

func (s *dispatchStartFailingStore) Execution(fence session.RunFence) session.ExecutionStore {
	return &dispatchStartFailingExecution{ExecutionStore: s.Store.Execution(fence), err: s.err}
}

type dispatchStartFailingExecution struct {
	session.ExecutionStore
	err error
}

func (s *dispatchStartFailingExecution) UpdateModelRequest(ctx context.Context, record session.ModelRequestRecord) error {
	if record.State == session.ModelRequestDispatchStarted {
		return s.err
	}
	return s.ExecutionStore.UpdateModelRequest(ctx, record)
}

func modelLifecycleNoticePlan(t *testing.T, sequence *[]string, completed *[]ModelCompletedNotice) (*RunPlan, func()) {
	t.Helper()
	registry := newTestExtensionRegistry(nil)
	component := extension.Component{InstanceID: "model-lifecycle", Artifact: extension.Artifact{Name: "model-lifecycle", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.On(registrar, ModelRequestedPoint, extension.Registration{ID: "requested", Scope: extension.GlobalScope()}, func(context.Context, ModelRequestedNotice) error {
			*sequence = append(*sequence, "requested")
			return nil
		}); err != nil {
			return err
		}
		return extension.On(registrar, ModelCompletedPoint, extension.Registration{ID: "completed", Scope: extension.GlobalScope()}, func(_ context.Context, notice ModelCompletedNotice) error {
			*sequence = append(*sequence, "completed")
			*completed = append(*completed, notice)
			return nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		_ = mount.Close(context.Background())
		t.Fatal(err)
	}
	var once sync.Once
	return newTestDispatchPlan(dispatch), func() {
		once.Do(func() {
			if err := mount.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAuditModelRequestRejectsUnsafeAndDeprecatedMessageShapes(t *testing.T) {
	unsafe := map[string]any{"credential": "sentinel"}
	tests := []struct {
		name   string
		mutate func(*einoschema.Message)
	}{
		{name: "tool call", mutate: func(message *einoschema.Message) {
			message.ToolCalls = []einoschema.ToolCall{{Extra: unsafe}}
		}},
		{name: "deprecated MultiContent", mutate: func(message *einoschema.Message) {
			//nolint:staticcheck // The audit boundary must reject the deprecated field.
			message.MultiContent = []einoschema.ChatMessagePart{{Type: einoschema.ChatMessagePartTypeText, Text: "legacy"}}
		}},
		{name: "input part", mutate: func(message *einoschema.Message) {
			message.UserInputMultiContent = []einoschema.MessageInputPart{{Extra: unsafe}}
		}},
		{name: "input media", mutate: func(message *einoschema.Message) {
			message.UserInputMultiContent = []einoschema.MessageInputPart{{Image: &einoschema.MessageInputImage{MessagePartCommon: einoschema.MessagePartCommon{Extra: unsafe}}}}
		}},
		{name: "output part", mutate: func(message *einoschema.Message) {
			message.AssistantGenMultiContent = []einoschema.MessageOutputPart{{Extra: unsafe}}
		}},
		{name: "output media", mutate: func(message *einoschema.Message) {
			message.AssistantGenMultiContent = []einoschema.MessageOutputPart{{Audio: &einoschema.MessageOutputAudio{MessagePartCommon: einoschema.MessagePartCommon{Extra: unsafe}}}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message := einoschema.UserMessage("hello")
			test.mutate(message)
			if _, _, _, err := auditModelRequest(model.Request{Messages: []*einoschema.Message{message}}, nil, 0); err == nil {
				t.Fatal("unsafe nested Extra was accepted")
			}
		})
	}
}

func TestLedgerUsesExecutionScopedWriterCapability(t *testing.T) {
	_, err := NewStreamingOrchestrator(WithStore(newAdmissionStore()), WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) { return nil, errors.New("unused") })}), WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()))
	if err != nil {
		t.Fatalf("construction error = %v", err)
	}
}

func TestLedgerPassesDurableRecordIDThroughRequest(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	streamer := &recordingRequestStreamer{}
	orchestrator, err := NewStreamingOrchestrator(WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()))
	if err != nil {
		t.Fatal(err)
	}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "idempotency-session", Message: UserMessage{Content: "hello"}, Config: orchestratorConfig()})
	if result.Error != nil {
		t.Fatalf("result = %#v", result)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 1 {
		t.Fatalf("records = %#v, %v", batch.Records, err)
	}
	if streamer.request.IdempotencyKey != string(batch.Records[0].ID) {
		t.Fatalf("request key=%q record=%q", streamer.request.IdempotencyKey, batch.Records[0].ID)
	}
}

type recordingRequestStreamer struct {
	request model.Request
}

func (s *recordingRequestStreamer) StreamProvider(_ context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	var err error
	s.request, err = request.Clone()
	if err != nil {
		return nil, err
	}
	reader, writer := einoschema.Pipe[model.StreamDelta](1)
	_ = writer.Send(model.StreamDelta{Message: einoschema.AssistantMessage("done", nil)}, nil)
	writer.Close()
	return reader, nil
}

func startAndWaitRequest(t *testing.T, orchestrator *StreamingOrchestrator, request Request) Result {
	t.Helper()
	handle, err := orchestrator.Start(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return <-handle.Done()
}
