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
		submitted = request.Clone()
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	})
	orchestrator, err := NewStreamingOrchestrator(
		WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}),
		WithModelRequestLedger(true), WithModelRequestSafeOptions("temperature"), WithSystemPromptMaterialization(true),
		WithClock(func() time.Time { return time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	config := orchestratorConfig()
	config.Agent.SystemPrompt = "audited system"
	config.Agent.Options["SECRET_TOKEN"] = "credential-sentinel"
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "ledger-session", Input: []*einoschema.Message{einoschema.UserMessage("hello")}, Config: config})
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
	audited, hash, err := AuditModelRequest(submitted, []string{"temperature"}, 0)
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
	orchestrator, err := NewStreamingOrchestrator(WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}), WithModelRequestLedger(true), WithAttempts(2))
	if err != nil {
		t.Fatal(err)
	}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "retry-session", Input: []*einoschema.Message{einoschema.UserMessage("hello")}, Config: orchestratorConfig()})
	if result.Error != nil {
		t.Fatalf("result = %#v", result)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 2 || batch.Records[0].State != session.ModelRequestFailed || batch.Records[1].State != session.ModelRequestCompleted || batch.Records[0].Attempt != 1 || batch.Records[1].Attempt != 2 {
		t.Fatalf("retry records = %#v, %v", batch.Records, err)
	}
}

func TestLedgerMarksPanickingDispatchedRequestFailed(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "panic-ledger", Artifact: extension.Artifact{Name: "panic-ledger", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var completed []ModelCompletedNotice
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, ModelCompletedPoint, extension.Registration{ID: "completed", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(_ context.Context, notice ModelCompletedNotice) error {
			completed = append(completed, notice)
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

	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		panic("provider panic")
	})
	orchestrator, err := NewStreamingOrchestrator(WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}), WithModelRequestLedger(true))
	if err != nil {
		t.Fatal(err)
	}
	orchestrator.Plans = staticRunPlanProvider{plan: &RunPlan{Dispatch: dispatch}}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "panic-ledger-session", Input: []*einoschema.Message{einoschema.UserMessage("hello")}, Config: orchestratorConfig()})
	if result.Status != session.RunFailed || result.Error == nil || !strings.Contains(result.Error.Error(), "provider stream panic: provider panic") {
		t.Fatalf("result = %#v", result)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 1 || batch.Records[0].State != session.ModelRequestFailed {
		t.Fatalf("records = %#v, %v", batch.Records, err)
	}
	if len(completed) != 1 || completed[0].Error.Code == "" {
		t.Fatalf("model-completed notices = %#v", completed)
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
	orchestrator, err := NewStreamingOrchestrator(WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithToolRegistry(staticToolRegistry{tools: []Tool{tool}}), WithRunPlanProvider(legacyRunTestPlanProvider()), WithIDGenerator(&sequenceIDs{}), WithModelRequestLedger(true))
	if err != nil {
		t.Fatal(err)
	}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "tool-follow-up-session", Input: []*einoschema.Message{einoschema.UserMessage("hello")}, Config: orchestratorConfig()})
	if result.Error != nil {
		t.Fatalf("result = %#v", result)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 2 || batch.Records[0].Step != 1 || batch.Records[1].Step != 2 || batch.Records[0].Attempt != 1 || batch.Records[1].Attempt != 1 {
		t.Fatalf("tool follow-up records = %#v, %v", batch.Records, err)
	}
}

func TestLedgerRejectsUnsafeExtraBeforeAdapterCall(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	called := false
	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		called = true
		return nil, nil
	})
	orchestrator, err := NewStreamingOrchestrator(WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}), WithModelRequestLedger(true))
	if err != nil {
		t.Fatal(err)
	}
	message := einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "unsafe-call", Extra: map[string]any{"credential": "sentinel"}}})
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "unsafe-session", Input: []*einoschema.Message{message}, Config: orchestratorConfig()})
	if result.Error == nil || called {
		t.Fatalf("result=%#v adapter_called=%t", result, called)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 0 {
		t.Fatalf("unsafe request records=%#v error=%v", batch.Records, err)
	}
}

func TestAuditModelRequestRejectsEveryNestedExtraCategory(t *testing.T) {
	unsafe := map[string]any{"credential": "sentinel"}
	tests := []struct {
		name   string
		mutate func(*einoschema.Message)
	}{
		{name: "tool call", mutate: func(message *einoschema.Message) {
			message.ToolCalls = []einoschema.ToolCall{{Extra: unsafe}}
		}},
		{name: "legacy multimodal media", mutate: func(message *einoschema.Message) {
			//nolint:staticcheck // The audit boundary must remain safe for legacy persisted message shapes.
			message.MultiContent = []einoschema.ChatMessagePart{{ImageURL: &einoschema.ChatMessageImageURL{Extra: unsafe}}}
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
			if _, _, err := AuditModelRequest(model.Request{Messages: []*einoschema.Message{message}}, nil, 0); err == nil {
				t.Fatal("unsafe nested Extra was accepted")
			}
		})
	}
}

func TestLedgerOptionRequiresCapability(t *testing.T) {
	_, err := NewStreamingOrchestrator(WithStore(newAdmissionStore()), WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) { return nil, errors.New("unused") })}), WithIDGenerator(&sequenceIDs{}), WithModelRequestLedger(true))
	if !errors.Is(err, ErrInvalidOrchestrator) {
		t.Fatalf("construction error = %v", err)
	}
}

func TestLedgerPassesDurableRecordIDToIdempotentStreamer(t *testing.T) {
	store, err := sqlitestore.Open(context.Background(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	streamer := &recordingIdempotentStreamer{}
	orchestrator, err := NewStreamingOrchestrator(WithStore(store), WithModelResolver(resolvedModel{streamer: streamer}), WithIDGenerator(&sequenceIDs{}), WithModelRequestLedger(true))
	if err != nil {
		t.Fatal(err)
	}
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "idempotency-session", Input: []*einoschema.Message{einoschema.UserMessage("hello")}, Config: orchestratorConfig()})
	if result.Error != nil {
		t.Fatalf("result = %#v", result)
	}
	batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
	if err != nil || len(batch.Records) != 1 {
		t.Fatalf("records = %#v, %v", batch.Records, err)
	}
	if streamer.key != string(batch.Records[0].ID) || streamer.request.IdempotencyKey != streamer.key || streamer.fallbackCalled {
		t.Fatalf("idempotency key=%q request=%q fallback=%t record=%q", streamer.key, streamer.request.IdempotencyKey, streamer.fallbackCalled, batch.Records[0].ID)
	}
}

type recordingIdempotentStreamer struct {
	key            string
	request        model.Request
	fallbackCalled bool
}

func (s *recordingIdempotentStreamer) StreamProvider(context.Context, model.Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	s.fallbackCalled = true
	return nil, errors.New("idempotent path not used")
}

func (s *recordingIdempotentStreamer) StreamProviderWithIdempotencyKey(_ context.Context, request model.Request, key string) (*einoschema.StreamReader[*einoschema.Message], error) {
	s.key = key
	s.request = request.Clone()
	reader, writer := einoschema.Pipe[*einoschema.Message](1)
	_ = writer.Send(einoschema.AssistantMessage("done", nil), nil)
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
