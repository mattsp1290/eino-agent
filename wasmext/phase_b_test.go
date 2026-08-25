package wasmext

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
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
	source := &LoadedContextSource{module: module, component: component}
	defer func() { _ = source.close() }()
	messages, err := source.loadContext(context.Background(), runtime.TurnSnapshot{
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
	sink := &LoadedEventSink{module: module, component: component}
	defer func() { _ = sink.close() }()
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
	hook := &LoadedHook{module: module, component: component, turns: make(map[session.RunID]wittypes.TurnMetadata)}
	defer func() { _ = hook.close() }()
	if err := hook.beforeRun(context.Background(), session.Run{ID: "run-1", SessionID: "session-1", Agent: "agent"}); err != nil {
		t.Fatal(err)
	}
	snapshot := runtime.TurnSnapshot{RunID: "run-1", SessionID: "session-1", Tools: []runtime.Tool{{Name: "echo"}}, Messages: []*einoschema.Message{einoschema.UserMessage("hidden")}}
	if _, err := hook.beforeTurn(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if err := hook.afterTurn(context.Background(), runtime.TurnSnapshot{RunID: "run-1"}, runtime.Result{}); err != nil {
		t.Fatal(err)
	}
	if err := hook.afterRun(context.Background(), runtime.Result{RunID: "run-1"}); err != nil {
		t.Fatal(err)
	}
	if seen["hook.before-run"].MessageCount != 0 || seen["hook.after-turn"].MessageCount != 1 || seen["hook.after-run"].ToolNames.Len() != 1 {
		t.Fatalf("hook metadata = %#v", seen)
	}
}

func TestRegisteredHookReceivesBoundedMetadataAcrossAllPhases(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]wittypes.TurnMetadata{}
	component := &fakeComponent{call: func(_ context.Context, operation string, input, _ any) error {
		mu.Lock()
		seen[operation] = input.(wittypes.TurnMetadata)
		mu.Unlock()
		return nil
	}}
	module, err := loadModule(context.Background(), fixtureConfig(t, []byte("registered-hook")), hookContract, fakeFactory(component))
	if err != nil {
		t.Fatal(err)
	}
	hook := &LoadedHook{module: module, component: component, turns: make(map[session.RunID]wittypes.TurnMetadata)}
	defer func() { _ = hook.close() }()

	registry := extension.NewRegistry(nil)
	extensionComponent := extension.Component{InstanceID: "registered-hook", Artifact: extension.Artifact{Name: "registered-hook", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}
	_, err = registry.Mount(context.Background(), extensionComponent, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return RegisterHook(registrar, extension.Registration{ID: "hook", InstanceID: extensionComponent.InstanceID, Scope: extension.GlobalScope()}, hook)
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()

	metadata := runtime.BoundedTurnMetadata{
		RunID: "run-1", SessionID: "session-1", EpochID: "epoch-1", AgentName: "agent", AgentMode: "primary",
		ProviderID: "provider", ModelID: "model", ToolNames: []string{"echo"}, MessageCount: 2,
		RoleCounts: runtime.MessageRoleCounts{System: 1, User: 1}, HasSystemPrompt: true,
	}
	_ = extension.Notify(plan, context.Background(), runtime.RunAdmittedPoint, runtime.RunAdmittedNotice{SessionID: metadata.SessionID, RunID: metadata.RunID, Metadata: metadata})
	if _, err := extension.Invoke(plan, context.Background(), runtime.TurnPreparePoint, metadata, func(_ context.Context, value runtime.BoundedTurnMetadata) (runtime.BoundedTurnMetadata, error) {
		return value, nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = extension.Notify(plan, context.Background(), runtime.RunSettledPoint, runtime.RunSettledNotice{SessionID: metadata.SessionID, Result: runtime.Result{RunID: metadata.RunID}})

	for _, operation := range []string{"hook.before-run", "hook.before-turn", "hook.after-turn", "hook.after-run"} {
		turn := seen[operation]
		if turn.EpochID != "epoch-1" || turn.AgentName != "agent" || turn.AgentMode != "primary" || turn.ProviderID != "provider" || turn.ModelID != "model" {
			t.Fatalf("%s metadata = %#v", operation, turn)
		}
	}
	if seen["hook.before-turn"].ToolNames.Len() != 1 || seen["hook.after-run"].ToolNames.Len() != 1 || seen["hook.before-turn"].MessageCount != 2 || !seen["hook.before-turn"].HasSystemPrompt {
		t.Fatalf("turn metadata = %#v", seen)
	}
}

func TestRegisteredHookUsesAdmissionMetadataWhenRunSettlesBeforeTurn(t *testing.T) {
	seen := map[string]wittypes.TurnMetadata{}
	component := &fakeComponent{call: func(_ context.Context, operation string, input, _ any) error {
		seen[operation] = input.(wittypes.TurnMetadata)
		return nil
	}}
	module, err := loadModule(context.Background(), fixtureConfig(t, []byte("early-settlement-hook")), hookContract, fakeFactory(component))
	if err != nil {
		t.Fatal(err)
	}
	hook := &LoadedHook{module: module, component: component, turns: make(map[session.RunID]wittypes.TurnMetadata)}
	defer func() { _ = hook.close() }()
	registry := extension.NewRegistry(nil)
	extensionComponent := extension.Component{InstanceID: "early-hook", Artifact: extension.Artifact{Name: "early-hook", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}
	_, err = registry.Mount(context.Background(), extensionComponent, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return RegisterHook(registrar, extension.Registration{ID: "hook", InstanceID: extensionComponent.InstanceID, Scope: extension.GlobalScope()}, hook)
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	metadata := runtime.BoundedTurnMetadata{RunID: "run-early", SessionID: "session-early", EpochID: "epoch-early", AgentName: "agent", ProviderID: "provider", ModelID: "model", MessageCount: 1, RoleCounts: runtime.MessageRoleCounts{User: 1}, HasSystemPrompt: true}
	_ = extension.Notify(plan, context.Background(), runtime.RunAdmittedPoint, runtime.RunAdmittedNotice{SessionID: metadata.SessionID, RunID: metadata.RunID, Metadata: metadata})
	_ = extension.Notify(plan, context.Background(), runtime.RunSettledPoint, runtime.RunSettledNotice{SessionID: metadata.SessionID, Result: runtime.Result{RunID: metadata.RunID}})
	for _, operation := range []string{"hook.after-turn", "hook.after-run"} {
		turn := seen[operation]
		if turn.SessionID != "session-early" || turn.EpochID != "epoch-early" || turn.AgentName != "agent" || turn.ProviderID != "provider" || turn.ModelID != "model" || turn.MessageCount != 1 || !turn.HasSystemPrompt {
			t.Fatalf("%s metadata = %#v", operation, turn)
		}
	}
	if len(hook.turns) != 0 {
		t.Fatalf("cached turns leaked after settlement: %#v", hook.turns)
	}
}

func TestFinishRegisteredHookRunsCleanupAndJoinsErrors(t *testing.T) {
	afterTurnErr := errors.New("after turn")
	afterRunErr := errors.New("after run")
	var operations []string
	component := &fakeComponent{call: func(_ context.Context, operation string, _ any, _ any) error {
		operations = append(operations, operation)
		switch operation {
		case "hook.after-turn":
			return afterTurnErr
		case "hook.after-run":
			return afterRunErr
		default:
			return nil
		}
	}}
	module, err := loadModule(context.Background(), fixtureConfig(t, []byte("hook-cleanup")), hookContract, fakeFactory(component))
	if err != nil {
		t.Fatal(err)
	}
	hook := &LoadedHook{module: module, component: component, turns: map[session.RunID]wittypes.TurnMetadata{"run-1": {RunID: "run-1"}}}
	defer func() { _ = hook.close() }()
	err = finishRegisteredHook(context.Background(), hook, runtime.RunSettledNotice{SessionID: "session-1", Result: runtime.Result{RunID: "run-1"}})
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok || len(joined.Unwrap()) != 2 || !reflect.DeepEqual(operations, []string{"hook.after-turn", "hook.after-run"}) {
		t.Fatalf("finishRegisteredHook = %v operations=%v", err, operations)
	}
	for index, operation := range []string{"hook.after-turn", "hook.after-run"} {
		var extensionErr *Error
		if !errors.As(joined.Unwrap()[index], &extensionErr) || extensionErr.Operation != operation {
			t.Fatalf("joined error %d = %#v", index, joined.Unwrap()[index])
		}
	}
	hook.mu.RLock()
	_, cached := hook.turns["run-1"]
	hook.mu.RUnlock()
	if cached {
		t.Fatal("after-run did not remove cached metadata")
	}
}

func TestContextContributionSourceIsUnambiguousAcrossInstancesAndScopes(t *testing.T) {
	specs := []extension.Registration{
		{ID: "c", InstanceID: "a/b", Scope: extension.GlobalScope()},
		{ID: "b/c", InstanceID: "a", Scope: extension.GlobalScope()},
		{ID: "c", InstanceID: "a/b", Scope: extension.SessionScope("session")},
		{ID: "c", InstanceID: "other", Scope: extension.GlobalScope()},
	}
	seen := map[string]bool{}
	for _, spec := range specs {
		source := contextContributionSource(spec, 0)
		if seen[source] {
			t.Fatalf("duplicate source %q for %#v", source, spec)
		}
		seen[source] = true
	}
}

func TestRegisteredContextSourcesNamespaceContributionsByInstance(t *testing.T) {
	registry := extension.NewRegistry(nil)
	for _, instanceID := range []string{"context-one", "context-two"} {
		instanceID := instanceID
		component := &fakeComponent{call: func(_ context.Context, _ string, _ any, output any) error {
			*output.(*[]wittypes.TextMessage) = []wittypes.TextMessage{{Role: wittypes.TextRoleUser, Text: instanceID}}
			return nil
		}}
		module, err := loadModule(context.Background(), fixtureConfig(t, []byte(instanceID)), contextSourceContract, fakeFactory(component))
		if err != nil {
			t.Fatal(err)
		}
		source := &LoadedContextSource{module: module, component: component}
		t.Cleanup(func() { _ = source.close() })
		extensionComponent := extension.Component{InstanceID: instanceID, Artifact: extension.Artifact{Name: instanceID, Version: "1", Hash: instanceID + "-artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}
		_, err = registry.Mount(context.Background(), extensionComponent, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
			return RegisterContextSource(registrar, extension.Registration{ID: "context", InstanceID: instanceID, Scope: extension.GlobalScope()}, source)
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	plan, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	assembly := runtime.ContextAssembly{Metadata: runtime.BoundedTurnMetadata{RunID: "run", SessionID: "session"}}
	assembled, err := extension.Invoke(plan, context.Background(), runtime.ContextAssemblePoint, assembly, func(_ context.Context, value runtime.ContextAssembly) (runtime.ContextAssembly, error) {
		return value, nil
	})
	if err != nil || len(assembled.Contributions) != 2 || assembled.Contributions[0].Source == assembled.Contributions[1].Source {
		t.Fatalf("assembled contributions = %#v, %v", assembled.Contributions, err)
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
	middleware := &LoadedToolMiddleware{module: module, component: component}
	defer func() { _ = middleware.close() }()
	call := runtime.ToolCall{ID: "call-1", Input: json.RawMessage(`{"raw":true}`)}
	input, err := middleware.beforeToolCall(context.Background(), runtime.Tool{Name: "echo"}, call)
	if err != nil || string(input) != `{"normalized":true}` {
		t.Fatalf("BeforeToolCall = %s, %v", input, err)
	}
	result, err := middleware.afterToolCall(context.Background(), runtime.Tool{Name: "echo"}, call, runtime.ToolResult{
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
	var contextTurn wittypes.TurnMetadata
	contextComponent := &fakeComponent{call: func(_ context.Context, _ string, input any, output any) error {
		contextTurn = input.(wittypes.TurnMetadata)
		*output.(*[]wittypes.TextMessage) = []wittypes.TextMessage{{Role: wittypes.TextRoleUser, Text: "from-wasm"}}
		return nil
	}}
	contextModule, err := loadModule(context.Background(), fixtureConfig(t, []byte("point-context")), contextSourceContract, fakeFactory(contextComponent))
	if err != nil {
		t.Fatal(err)
	}
	source := &LoadedContextSource{module: contextModule, component: contextComponent}
	defer func() { _ = source.close() }()
	eventCalls := 0
	eventComponent := &fakeComponent{call: func(_ context.Context, _ string, _ any, _ any) error { eventCalls++; return nil }}
	eventModule, err := loadModule(context.Background(), fixtureConfig(t, []byte("point-event")), eventSinkContract, fakeFactory(eventComponent))
	if err != nil {
		t.Fatal(err)
	}
	sink := &LoadedEventSink{module: eventModule, component: eventComponent}
	defer func() { _ = sink.close() }()

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
	assembly := runtime.ContextAssembly{
		RunID: "run", SessionID: "session", Base: []*einoschema.Message{einoschema.UserMessage("base")},
		Metadata: runtime.BoundedTurnMetadata{RunID: "run", SessionID: "session", EpochID: "epoch", AgentName: "agent", AgentMode: "primary", ProviderID: "provider", ModelID: "model", MessageCount: 1, RoleCounts: runtime.MessageRoleCounts{User: 1}, HasSystemPrompt: true},
	}
	assembly, err = extension.Invoke(plan, context.Background(), runtime.ContextAssemblePoint, assembly, func(_ context.Context, value runtime.ContextAssembly) (runtime.ContextAssembly, error) {
		return value, nil
	})
	if err != nil || len(assembly.Contributions) != 1 || assembly.Contributions[0].Message.Content != "from-wasm" {
		t.Fatalf("assembly = %#v, %v", assembly, err)
	}
	if contextTurn.AgentName != "agent" || contextTurn.AgentMode != "primary" || contextTurn.ProviderID != "provider" || contextTurn.ModelID != "model" || !contextTurn.HasSystemPrompt || contextTurn.RoleCounts.User != 1 {
		t.Fatalf("registered context metadata = %#v", contextTurn)
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
