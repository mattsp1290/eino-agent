package wasmext

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/types"
)

func TestContextSourceMapsOnlyBoundedPlainText(t *testing.T) {
	component := &fakeComponent{}
	component.call = func(_ context.Context, operation string, input, output any) error {
		if operation != "context-source.load-context" {
			t.Fatalf("operation = %q", operation)
		}
		turn := input.(wittypes.TurnMetadata)
		if turn.RunID != "run-1" || turn.MessageCount != 1 || strings.Contains(turn.AgentName, "SECRET") {
			t.Fatalf("bounded turn = %#v", turn)
		}
		*output.(*[]wittypes.TextMessage) = []wittypes.TextMessage{
			{Role: wittypes.TextRoleSystem, Text: "policy"},
			{Role: wittypes.TextRoleUser, Text: "context"},
		}
		return nil
	}
	module, err := loadModule(context.Background(), fixtureConfig(t, []byte("context")), contextSourceContract, fakeFactory(component))
	if err != nil {
		t.Fatal(err)
	}
	source := &LoadedContextSource{module: module}
	defer func() { _ = source.Close() }()
	messages, err := source.LoadContext(context.Background(), runtime.TurnSnapshot{
		RunID: "run-1", SessionID: "session-1", Messages: []*einoschema.Message{einoschema.UserMessage("SECRET content")},
	})
	if err != nil || len(messages) != 2 || messages[0].Role != einoschema.System || messages[1].Content != "context" {
		t.Fatalf("LoadContext = %#v, %v", messages, err)
	}
}

func TestEventSinkUsesContentFreeSummary(t *testing.T) {
	component := &fakeComponent{}
	component.call = func(_ context.Context, _ string, input, _ any) error {
		event := input.(wittypes.BoundedEvent)
		if strings.Contains(event.PayloadSummary, "SECRET") || !strings.Contains(event.PayloadSummary, "payload_bytes=") {
			t.Fatalf("summary = %q", event.PayloadSummary)
		}
		return nil
	}
	module, err := loadModule(context.Background(), fixtureConfig(t, []byte("event")), eventSinkContract, fakeFactory(component))
	if err != nil {
		t.Fatal(err)
	}
	sink := &LoadedEventSink{module: module}
	defer func() { _ = sink.Close() }()
	if err := sink.Emit(context.Background(), runtime.Event{Kind: runtime.EventMessageDelta, Payload: json.RawMessage(`{"content":"SECRET"}`), Time: time.Unix(1, 0)}); err != nil {
		t.Fatal(err)
	}
}

func TestHookCachesFullMetadataUntilAfterRun(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]wittypes.TurnMetadata{}
	component := &fakeComponent{}
	component.call = func(_ context.Context, operation string, input, _ any) error {
		mu.Lock()
		seen[operation] = input.(wittypes.TurnMetadata)
		mu.Unlock()
		return nil
	}
	module, err := loadModule(context.Background(), fixtureConfig(t, []byte("hook")), hookContract, fakeFactory(component))
	if err != nil {
		t.Fatal(err)
	}
	hook := &LoadedHook{module: module, turns: make(map[session.RunID]wittypes.TurnMetadata)}
	defer func() { _ = hook.Close() }()
	if err := hook.BeforeRun(context.Background(), session.Run{ID: "run-1", SessionID: "session-1", Agent: "agent"}); err != nil {
		t.Fatal(err)
	}
	snapshot := runtime.TurnSnapshot{RunID: "run-1", SessionID: "session-1", Tools: []runtime.Tool{{Name: "echo"}}, Messages: []*einoschema.Message{einoschema.UserMessage("hidden")}}
	if _, err := hook.BeforeTurn(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if err := hook.AfterTurn(context.Background(), runtime.TurnSnapshot{RunID: "run-1"}, runtime.Result{}); err != nil {
		t.Fatal(err)
	}
	if err := hook.AfterRun(context.Background(), runtime.Result{RunID: "run-1"}); err != nil {
		t.Fatal(err)
	}
	if seen["hook.before-run"].MessageCount != 0 || seen["hook.after-turn"].MessageCount != 1 || seen["hook.after-run"].ToolNames.Len() != 1 {
		t.Fatalf("hook metadata = %#v", seen)
	}
}

func TestToolMiddlewareJSONMappingPreservesProtectedContainers(t *testing.T) {
	component := &fakeComponent{}
	component.call = func(_ context.Context, operation string, input, output any) error {
		switch operation {
		case "tool-middleware.before-tool-call":
			*output.(*wittypes.Replacement) = wittypes.ReplacementJSON(`{"normalized":true}`)
		case "tool-middleware.after-tool-call":
			request := input.(toolMiddlewareAfterRequest)
			if request.OutputJSON != `{"original":true}` {
				t.Fatalf("preferred output = %q", request.OutputJSON)
			}
			*output.(*wittypes.Replacement) = wittypes.ReplacementJSON(`{"changed":true}`)
		}
		return nil
	}
	module, err := loadModule(context.Background(), fixtureConfig(t, []byte("middleware")), toolMiddlewareContract, fakeFactory(component))
	if err != nil {
		t.Fatal(err)
	}
	middleware := &LoadedToolMiddleware{module: module}
	defer func() { _ = middleware.Close() }()
	call := runtime.ToolCall{ID: "call-1", Input: json.RawMessage(`{"raw":true}`)}
	input, err := middleware.BeforeToolCall(context.Background(), runtime.Tool{Name: "echo"}, call)
	if err != nil || string(input) != `{"normalized":true}` {
		t.Fatalf("BeforeToolCall = %s, %v", input, err)
	}
	result, err := middleware.AfterToolCall(context.Background(), runtime.Tool{Name: "echo"}, call, runtime.ToolResult{
		Output: "fallback", Structured: json.RawMessage(`{"original":true}`),
		Attachments: []runtime.Attachment{{ID: "attachment-1"}}, Metadata: map[string]string{"protected": "yes"},
	}, nil)
	if err != nil || string(result.Structured) != `{"changed":true}` || len(result.Attachments) != 1 || result.Metadata["protected"] != "yes" {
		t.Fatalf("AfterToolCall = %#v, %v", result, err)
	}
}

func TestPhaseBContractsAndLoaderClose(t *testing.T) {
	component := &fakeComponent{call: func(_ context.Context, operation string, _ any, output any) error {
		switch operation {
		case "context-source.load-context":
			*output.(*[]wittypes.TextMessage) = nil
		case "tool-middleware.before-tool-call", "tool-middleware.after-tool-call":
			*output.(*wittypes.Replacement) = wittypes.ReplacementUnchanged()
		}
		return nil
	}}
	loader := NewLoader()
	loader.factory = fakeFactory(component)
	cfg := fixtureConfig(t, []byte("all phase b"))
	if _, err := loader.LoadContextSource(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := loader.LoadEventSink(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := loader.LoadHook(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := loader.LoadToolMiddleware(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := loader.Close(context.Background()); err != nil || !component.closed.Load() {
		t.Fatalf("Close = %v, closed=%t", err, component.closed.Load())
	}
	if len(contextSourceContract.functions) != 1 || len(eventSinkContract.functions) != 1 || len(hookContract.functions) != 4 || len(toolMiddlewareContract.functions) != 2 {
		t.Fatal("phase B contract declarations incomplete")
	}
}

func TestPhaseBWrappersUseNativeRuntimePoints(t *testing.T) {
	contextComponent := &fakeComponent{call: func(_ context.Context, _ string, _ any, output any) error {
		*output.(*[]wittypes.TextMessage) = []wittypes.TextMessage{{Role: wittypes.TextRoleUser, Text: "from-wasm"}}
		return nil
	}}
	contextModule, err := loadModule(context.Background(), fixtureConfig(t, []byte("point-context")), contextSourceContract, fakeFactory(contextComponent))
	if err != nil {
		t.Fatal(err)
	}
	source := &LoadedContextSource{module: contextModule}
	defer func() { _ = source.Close() }()
	eventCalls := 0
	eventComponent := &fakeComponent{call: func(_ context.Context, _ string, _ any, _ any) error { eventCalls++; return nil }}
	eventModule, err := loadModule(context.Background(), fixtureConfig(t, []byte("point-event")), eventSinkContract, fakeFactory(eventComponent))
	if err != nil {
		t.Fatal(err)
	}
	sink := &LoadedEventSink{module: eventModule}
	defer func() { _ = sink.Close() }()

	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "wasm-points", Artifact: extension.Artifact{Name: "wasm-points", Version: "0.1.0", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := RegisterContextSource(registrar, extension.Registration{ID: "context", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, source); err != nil {
			return err
		}
		return RegisterEventSink(registrar, extension.Registration{ID: "events", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, sink)
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	assembly := runtime.ContextAssembly{RunID: "run", SessionID: "session", Base: []*einoschema.Message{einoschema.UserMessage("base")}}
	assembly, err = extension.Invoke(plan, context.Background(), runtime.ContextAssemblePoint, assembly, func(_ context.Context, value runtime.ContextAssembly) (runtime.ContextAssembly, error) {
		return value, nil
	})
	if err != nil || len(assembly.Contributions) != 1 || assembly.Contributions[0].Message.Content != "from-wasm" {
		t.Fatalf("assembly = %#v, %v", assembly, err)
	}
	_ = extension.Notify(plan, context.Background(), runtime.EventPublishedPoint, runtime.Event{Kind: runtime.EventRunStarted})
	if eventCalls != 1 {
		t.Fatalf("event calls = %d", eventCalls)
	}
	plan.Release()
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
