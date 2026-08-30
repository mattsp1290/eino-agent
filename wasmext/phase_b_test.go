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

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
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
	source := &loadedContextSource{module: module, component: component}
	defer func() { _ = source.close() }()
	messages, err := source.loadBoundedContext(context.Background(), runtime.BoundedTurnMetadata{
		RunID: "run-1", SessionID: "session-1", MessageCount: 1, RoleCounts: runtime.MessageRoleCounts{User: 1},
	})
	if err != nil || len(messages) != 2 || messages[0].Role != einoschema.System || messages[1].Content != "context" {
		t.Fatalf("LoadContext = %#v, %v", messages, err)
	}
}

func TestWasmContextSourceReachesProviderInCanonicalOrder(t *testing.T) {
	component := &fakeComponent{call: func(_ context.Context, _ string, _ any, output any) error {
		*output.(*[]wittypes.TextMessage) = []wittypes.TextMessage{
			{Role: wittypes.TextRoleUser, Text: "wasm-user"},
			{Role: wittypes.TextRoleSystem, Text: "wasm-system"},
		}
		return nil
	}}
	loader := NewLoader()
	loader.factory = fakeFactory(component)
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.Mount(context.Background(), extension.Component{InstanceID: "wasm-context-provider", Artifact: extension.Artifact{Name: "wasm-context-provider", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}, composition.InstallerFunc(func(ctx context.Context, registrar *composition.Registrar) error {
		return loader.RegisterContextSource(ctx, registrar, extension.Registration{ID: "context", Scope: extension.GlobalScope()}, fixtureConfig(t, []byte("provider-order")))
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	store, err := sqlitestore.Open(context.Background(), t.TempDir()+"/wasm-context.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var messages []string
	streamer := wasmScriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, message := range request.Messages {
			messages = append(messages, message.Content)
		}
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	})
	selection := model.Selection{ProviderID: "fake", ModelID: "test"}
	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(store), runtime.WithRunPlanProvider(registry), runtime.WithIDGenerator(&wasmTestIDs{}),
		runtime.WithOwnerID("wasm-test"), runtime.WithModelResolver(model.ResolverFunc(func(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
			return model.Resolved{Provider: model.Provider{ID: "fake"}, Model: model.Descriptor{ID: "test", ProviderID: "fake"}, Streamer: streamer}, nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := orchestrator.Start(context.Background(), runtime.Request{
		SessionID: "session-a", Input: []*einoschema.Message{einoschema.UserMessage("base-user")},
		Config: config.Snapshot{Agent: config.Agent{Name: "agent", Model: selection, Options: map[string]string{}}, Model: selection, Metadata: map[string]string{"workspace_root": t.TempDir()}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result := <-handle.Done(); result.Error != nil {
		t.Fatal(result.Error)
	}
	want := []string{"wasm-system", "base-user", "wasm-user"}
	if !reflect.DeepEqual(messages, want) {
		t.Fatalf("provider messages = %v, want %v", messages, want)
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
	sink := &loadedEventSink{module: module, component: component}
	defer func() { _ = sink.close() }()
	if err := sink.Emit(context.Background(), session.EventRecord{Kind: runtime.EventMessageDelta, Payload: json.RawMessage(`{"content":"SECRET"}`), CreatedAt: time.Unix(1, 0)}); err != nil {
		t.Fatal(err)
	}
}

func TestRegisteredHookReceivesBoundedMetadataAtRunBoundaries(t *testing.T) {
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
	hook := &loadedHook{module: module, component: component}
	defer func() { _ = hook.close() }()

	registry := newTestExtensionRegistry(nil)
	extensionComponent := extension.Component{InstanceID: "registered-hook", Artifact: extension.Artifact{Name: "registered-hook", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}
	_, err = registry.Mount(context.Background(), extensionComponent, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return registerHook(registrar, extension.Registration{ID: "hook", Scope: extension.GlobalScope()}, hook)
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
	extension.Notify(plan, context.Background(), runtime.RunAdmittedPoint, runtime.RunAdmittedNotice{SessionID: metadata.SessionID, RunID: metadata.RunID, Metadata: metadata})
	extension.Notify(plan, context.Background(), runtime.RunSettledPoint, runtime.RunSettledNotice{SessionID: metadata.SessionID, Result: runtime.Result{RunID: metadata.RunID}, Metadata: metadata})
	if err := plan.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	for _, operation := range []string{"hook.before-run", "hook.after-run"} {
		turn := seen[operation]
		if turn.EpochID != "epoch-1" || turn.AgentName != "agent" || turn.AgentMode != "primary" || turn.ProviderID != "provider" || turn.ModelID != "model" {
			t.Fatalf("%s metadata = %#v", operation, turn)
		}
	}
	if len(seen) != 2 || seen["hook.before-run"].ToolNames.Len() != 1 || seen["hook.after-run"].ToolNames.Len() != 1 || seen["hook.after-run"].MessageCount != 2 || !seen["hook.after-run"].HasSystemPrompt {
		t.Fatalf("turn metadata = %#v", seen)
	}
}

func TestRegisteredHookRetainedOperationsAreExactlyOnceAndContained(t *testing.T) {
	var operations []string
	component := &fakeComponent{call: func(_ context.Context, operation string, input, _ any) error {
		operations = append(operations, operation+":"+input.(wittypes.TurnMetadata).RunID)
		if operation == "hook.after-run" {
			return errors.New("contained after-run failure")
		}
		return nil
	}}
	module, err := loadModule(context.Background(), fixtureConfig(t, []byte("hook-settlements")), hookContract, fakeFactory(component))
	if err != nil {
		t.Fatal(err)
	}
	hook := &loadedHook{module: module, component: component}
	defer func() { _ = hook.close() }()
	registry := newTestExtensionRegistry(nil)
	registered := extension.Component{InstanceID: "settlement-hook", Artifact: extension.Artifact{Name: "settlement-hook", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}
	_, err = registry.Mount(context.Background(), registered, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return registerHook(registrar, extension.Registration{ID: "hook", Scope: extension.GlobalScope()}, hook)
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()

	settlements := []struct {
		runID  session.RunID
		status session.RunStatus
		err    runtime.ClassifiedError
	}{
		{runID: "success", status: session.RunCompleted},
		{runID: "failure", status: session.RunFailed, err: runtime.ClassifiedError{Code: "operation_failed"}},
		{runID: "rejection", status: session.RunFailed, err: runtime.ClassifiedError{Code: "provider_rejected"}},
		{runID: "cancellation", status: session.RunInterrupted, err: runtime.ClassifiedError{Code: "interrupted"}},
	}
	for _, settlement := range settlements {
		metadata := runtime.BoundedTurnMetadata{SessionID: "session", RunID: settlement.runID}
		extension.Notify(plan, context.Background(), runtime.RunAdmittedPoint, runtime.RunAdmittedNotice{SessionID: "session", RunID: settlement.runID, Metadata: metadata})
		// Notifications contain callback errors, including after-run failures.
		extension.Notify(plan, context.Background(), runtime.RunSettledPoint, runtime.RunSettledNotice{SessionID: "session", Result: runtime.Result{RunID: settlement.runID, Status: settlement.status}, Metadata: metadata, Error: settlement.err})
	}
	// A run that never reaches durable settlement invokes no after-run callback;
	// loadedHook retains no metadata that needs cleanup.
	extension.Notify(plan, context.Background(), runtime.RunAdmittedPoint, runtime.RunAdmittedNotice{SessionID: "session", RunID: "abandoned", Metadata: runtime.BoundedTurnMetadata{SessionID: "session", RunID: "abandoned"}})
	if err := plan.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"hook.before-run:success", "hook.after-run:success",
		"hook.before-run:failure", "hook.after-run:failure",
		"hook.before-run:rejection", "hook.after-run:rejection",
		"hook.before-run:cancellation", "hook.after-run:cancellation",
		"hook.before-run:abandoned",
	}
	if !reflect.DeepEqual(operations, want) {
		t.Fatalf("hook operations = %v, want %v", operations, want)
	}
}

func TestRegisteredHookUsesSettlementMetadataWithoutAdmissionState(t *testing.T) {
	seen := map[string]wittypes.TurnMetadata{}
	component := &fakeComponent{call: func(_ context.Context, operation string, input, _ any) error {
		seen[operation] = input.(wittypes.TurnMetadata)
		return nil
	}}
	module, err := loadModule(context.Background(), fixtureConfig(t, []byte("early-settlement-hook")), hookContract, fakeFactory(component))
	if err != nil {
		t.Fatal(err)
	}
	hook := &loadedHook{module: module, component: component}
	defer func() { _ = hook.close() }()
	registry := newTestExtensionRegistry(nil)
	extensionComponent := extension.Component{InstanceID: "early-hook", Artifact: extension.Artifact{Name: "early-hook", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}
	_, err = registry.Mount(context.Background(), extensionComponent, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return registerHook(registrar, extension.Registration{ID: "hook", Scope: extension.GlobalScope()}, hook)
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
	extension.Notify(plan, context.Background(), runtime.RunSettledPoint, runtime.RunSettledNotice{SessionID: metadata.SessionID, Result: runtime.Result{RunID: metadata.RunID}, Metadata: metadata})
	if err := plan.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	turn := seen["hook.after-run"]
	if turn.SessionID != "session-early" || turn.EpochID != "epoch-early" || turn.AgentName != "agent" || turn.ProviderID != "provider" || turn.ModelID != "model" || turn.MessageCount != 1 || !turn.HasSystemPrompt {
		t.Fatalf("after-run metadata = %#v", turn)
	}
	if len(seen) != 1 {
		t.Fatalf("hook operations = %#v", seen)
	}
}

func TestFinishRegisteredHookReturnsAfterRunError(t *testing.T) {
	afterRunErr := errors.New("after run")
	var operations []string
	component := &fakeComponent{call: func(_ context.Context, operation string, _ any, _ any) error {
		operations = append(operations, operation)
		if operation == "hook.after-run" {
			return afterRunErr
		}
		return nil
	}}
	module, err := loadModule(context.Background(), fixtureConfig(t, []byte("hook-cleanup")), hookContract, fakeFactory(component))
	if err != nil {
		t.Fatal(err)
	}
	hook := &loadedHook{module: module, component: component}
	defer func() { _ = hook.close() }()
	err = hook.finish(context.Background(), runtime.BoundedTurnMetadata{SessionID: "session-1", RunID: "run-1"})
	if !reflect.DeepEqual(operations, []string{"hook.after-run"}) {
		t.Fatalf("hook.finish = %v operations=%v", err, operations)
	}
	var extensionErr *Error
	if !errors.As(err, &extensionErr) || extensionErr.Operation != "hook.after-run" {
		t.Fatalf("after-run error = %#v", err)
	}
}

func TestRegisteredContextSourcesRetainComponentOwnership(t *testing.T) {
	registry := newTestExtensionRegistry(nil)
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
		source := &loadedContextSource{module: module, component: component}
		t.Cleanup(func() { _ = source.close() })
		extensionComponent := extension.Component{InstanceID: instanceID, Artifact: extension.Artifact{Name: instanceID, Version: "1", Hash: instanceID + "-artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}
		_, err = registry.Mount(context.Background(), extensionComponent, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
			return registerContextSource(registrar, extension.Registration{ID: "context", Scope: extension.GlobalScope()}, source)
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
	handlers := plan.HandlerComponents()
	if len(handlers) != 2 || handlers[0].Component.InstanceID == handlers[1].Component.InstanceID {
		t.Fatalf("context handlers = %#v", handlers)
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
	middleware := &loadedToolMiddleware{module: module, component: component}
	defer func() { _ = middleware.close() }()
	call := runtime.ToolCall{ID: "call-1", Input: json.RawMessage(`{"raw":true}`)}
	input, err := middleware.beforeToolCall(context.Background(), runtime.Tool{Name: "echo"}, call)
	if err != nil || string(input) != `{"normalized":true}` {
		t.Fatalf("BeforeToolCall = %s, %v", input, err)
	}
	result, err := middleware.afterToolCall(context.Background(), runtime.Tool{Name: "echo"}, call, runtime.ToolResult{
		Output: "fallback", Structured: json.RawMessage(`{"original":true}`),
		Attachments: []runtime.Attachment{{ID: "attachment-1"}}, Metadata: map[string]string{"protected": "yes"},
	})
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
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.Mount(context.Background(), extension.Component{InstanceID: "phase-b", Artifact: extension.Artifact{Name: "phase-b", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}, composition.InstallerFunc(func(ctx context.Context, registrar *composition.Registrar) error {
		if err := loader.RegisterEventSink(ctx, registrar, extension.Registration{ID: "events", Scope: extension.GlobalScope()}, cfg); err != nil {
			return err
		}
		if err := loader.RegisterContextSource(ctx, registrar, extension.Registration{ID: "context", Scope: extension.GlobalScope()}, cfg); err != nil {
			return err
		}
		if err := loader.RegisterHook(ctx, registrar, extension.Registration{ID: "hook", Scope: extension.GlobalScope()}, cfg); err != nil {
			return err
		}
		return loader.RegisterToolMiddleware(ctx, registrar, extension.Registration{ID: "middleware", Scope: extension.GlobalScope()}, cfg)
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(loader.modules) != 0 {
		t.Fatalf("loader modules after mount close = %d, want 0", len(loader.modules))
	}
	if err := loader.Close(context.Background()); err != nil || !component.closed.Load() {
		t.Fatalf("Close = %v, closed=%t", err, component.closed.Load())
	}
	if len(contextSourceContract.functions) != 1 || len(eventSinkContract.functions) != 1 || len(hookContract.functions) != 2 || len(toolMiddlewareContract.functions) != 2 {
		t.Fatal("phase B contract declarations incomplete")
	}
}

func TestDirectRegistrationRollsBackOnCompositionInstallerFailure(t *testing.T) {
	component := contextSourceFakeComponent()
	loader := NewLoader()
	loader.factory = fakeFactory(component)
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := fixtureConfig(t, []byte("composition rollback"))
	mountedComponent := extension.Component{InstanceID: "wasm-rollback", Artifact: extension.Artifact{Name: "wasm-rollback", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}
	_, err = registry.Mount(context.Background(), mountedComponent, composition.InstallerFunc(func(ctx context.Context, registrar *composition.Registrar) error {
		if err := loader.RegisterContextSource(ctx, registrar, extension.Registration{ID: "context", Scope: extension.GlobalScope()}, cfg); err != nil {
			return err
		}
		return registrar.Guard(composition.GuardRegistration{ID: "invalid guard id", Scope: extension.GlobalScope()})
	}))
	if err == nil {
		t.Fatal("Mount succeeded after invalid later capability")
	}
	if len(loader.modules) != 0 || !component.closed.Load() {
		t.Fatalf("failed composition mount retained module: modules=%d closed=%t", len(loader.modules), component.closed.Load())
	}
}

func TestDirectRegistrationRollsBackOnCommitFailure(t *testing.T) {
	component := contextSourceFakeComponent()
	loader := NewLoader()
	loader.factory = fakeFactory(component)
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	mountedComponent := extension.Component{InstanceID: "duplicate", Artifact: extension.Artifact{Name: "duplicate", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}
	first, err := registry.Mount(context.Background(), mountedComponent, composition.InstallerFunc(func(context.Context, *composition.Registrar) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close(context.Background()) }()
	cfg := fixtureConfig(t, []byte("commit rollback"))
	_, err = registry.Mount(context.Background(), mountedComponent, composition.InstallerFunc(func(ctx context.Context, registrar *composition.Registrar) error {
		return loader.RegisterContextSource(ctx, registrar, extension.Registration{ID: "context", Scope: extension.GlobalScope()}, cfg)
	}))
	if !errors.Is(err, extension.ErrDuplicateInstance) {
		t.Fatalf("Mount error = %v, want duplicate instance", err)
	}
	if len(loader.modules) != 0 || !component.closed.Load() {
		t.Fatalf("failed commit retained module: modules=%d closed=%t", len(loader.modules), component.closed.Load())
	}
}

func TestDirectRegistrationMountCloseRacesLoaderClose(t *testing.T) {
	component := contextSourceFakeComponent()
	loader := NewLoader()
	loader.factory = fakeFactory(component)
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := fixtureConfig(t, []byte("close race"))
	mount, err := registry.Mount(context.Background(), extension.Component{InstanceID: "close-race", Artifact: extension.Artifact{Name: "close-race", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}, composition.InstallerFunc(func(ctx context.Context, registrar *composition.Registrar) error {
		return loader.RegisterContextSource(ctx, registrar, extension.Registration{ID: "context", Scope: extension.GlobalScope()}, cfg)
	}))
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() { <-start; errs <- loader.Close(context.Background()) }()
	go func() { <-start; errs <- mount.Close(context.Background()) }()
	close(start)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if len(loader.modules) != 0 || component.closeCalls.Load() != 1 {
		t.Fatalf("close race ownership: modules=%d close calls=%d", len(loader.modules), component.closeCalls.Load())
	}
}

func TestRegisteredEventObserverReportsModuleFailure(t *testing.T) {
	want := errors.New("guest event failure")
	component := &fakeComponent{call: func(context.Context, string, any, any) error { return want }}
	module, err := loadModule(context.Background(), fixtureConfig(t, []byte("failing-event")), eventSinkContract, fakeFactory(component))
	if err != nil {
		t.Fatal(err)
	}
	sink := &loadedEventSink{module: module, component: component}
	defer func() { _ = sink.close() }()
	diagnostics := make(chan extension.Diagnostic, 1)
	registry := newTestExtensionRegistry(extension.ReporterFunc(func(_ context.Context, diagnostic extension.Diagnostic) {
		diagnostics <- diagnostic
	}))
	mount, err := registry.Mount(context.Background(), extension.Component{InstanceID: "failing-event", Artifact: extension.Artifact{Name: "failing-event", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return registerEventSink(registrar, extension.Registration{ID: "events", Scope: extension.GlobalScope()}, sink)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	plan, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	extension.Notify(plan, context.Background(), runtime.EventPublishedPoint, session.EventRecord{Kind: runtime.EventRunStarted})
	if err := plan.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case diagnostic := <-diagnostics:
		if diagnostic.HandlerID != "events" || diagnostic.Cause == nil {
			t.Fatalf("diagnostic = %#v", diagnostic)
		}
	case <-time.After(time.Second):
		t.Fatal("event observer failure was not reported")
	}
}

func contextSourceFakeComponent() *fakeComponent {
	return &fakeComponent{call: func(_ context.Context, operation string, _ any, output any) error {
		if operation == "context-source.load-context" {
			*output.(*[]wittypes.TextMessage) = nil
		}
		return nil
	}}
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
	source := &loadedContextSource{module: contextModule, component: contextComponent}
	defer func() { _ = source.close() }()
	eventCalls := 0
	eventComponent := &fakeComponent{call: func(_ context.Context, _ string, _ any, _ any) error { eventCalls++; return nil }}
	eventModule, err := loadModule(context.Background(), fixtureConfig(t, []byte("point-event")), eventSinkContract, fakeFactory(eventComponent))
	if err != nil {
		t.Fatal(err)
	}
	sink := &loadedEventSink{module: eventModule, component: eventComponent}
	defer func() { _ = sink.close() }()

	registry := newTestExtensionRegistry(nil)
	component := extension.Component{InstanceID: "wasm-points", Artifact: extension.Artifact{Name: "wasm-points", Version: "0.1.0", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceWasm}}
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := registerContextSource(registrar, extension.Registration{ID: "context", Scope: extension.GlobalScope()}, source); err != nil {
			return err
		}
		return registerEventSink(registrar, extension.Registration{ID: "events", Scope: extension.GlobalScope()}, sink)
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	metadata := runtime.BoundedTurnMetadata{RunID: "run", SessionID: "session", EpochID: "epoch", AgentName: "agent", AgentMode: "primary", ProviderID: "provider", ModelID: "model", MessageCount: 1, RoleCounts: runtime.MessageRoleCounts{User: 1}, HasSystemPrompt: true}
	messages, err := source.loadBoundedContext(context.Background(), metadata)
	if err != nil || len(messages) != 1 || messages[0].Content != "from-wasm" {
		t.Fatalf("context messages = %#v, %v", messages, err)
	}
	if contextTurn.AgentName != "agent" || contextTurn.AgentMode != "primary" || contextTurn.ProviderID != "provider" || contextTurn.ModelID != "model" || !contextTurn.HasSystemPrompt || contextTurn.RoleCounts.User != 1 {
		t.Fatalf("registered context metadata = %#v", contextTurn)
	}
	extension.Notify(plan, context.Background(), runtime.EventPublishedPoint, session.EventRecord{Kind: runtime.EventRunStarted})
	if err := plan.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCalls != 1 {
		t.Fatalf("event calls = %d", eventCalls)
	}
	plan.Release()
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
