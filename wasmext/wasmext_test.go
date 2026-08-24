package wasmext

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"
	einoobs "github.com/mattsp1290/eino-obs"
	"go.bytecodealliance.org/cm"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
	"github.com/mattsp1290/eino-agent/tools"
	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/types"
)

func TestToolWrapperRoundTripAndBoundedSnapshot(t *testing.T) {
	t.Parallel()
	component := &fakeComponent{}
	component.call = func(_ context.Context, operation string, input, output any) error {
		switch operation {
		case "tool.metadata":
			permissions := []string{"workspace.read"}
			*output.(*wittypes.ToolMetadata) = wittypes.ToolMetadata{
				Name: "wasm-echo", Description: "echo JSON", ParametersJSONSchema: `{"type":"object"}`,
				RetrySafe: true, RequiredPermissions: cm.ToList(permissions),
			}
		case "tool.execute":
			request := input.(toolExecuteRequest)
			component.mu.Lock()
			component.lastInput = request
			component.mu.Unlock()
			*output.(*string) = `{"echo":` + request.InputJSON + `}`
		}
		return nil
	}
	loaded, err := openTool(context.Background(), fixtureConfig(t, []byte("tool component")), fakeFactory(component))
	if err != nil {
		t.Fatalf("openTool error = %v", err)
	}
	defer func() { _ = loaded.Close() }()
	definition := loaded.Definition()
	registry := tools.NewRegistry()
	if _, err := registry.Register(definition); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	snapshot := runtime.TurnSnapshot{
		RunID: "run-1", SessionID: "session-1", EpochID: "epoch-1",
		Config: config.Snapshot{
			Agent:     config.Agent{Name: "agent", Mode: "primary", Options: map[string]string{"SECRET": "agent-secret"}},
			Providers: []config.ProviderConfig{{Options: map[string]string{"TOKEN": "provider-secret"}}},
		},
		Model: runtimeResolvedWithSecret(), Messages: []*einoschema.Message{einoschema.SystemMessage("secret conversation"), einoschema.UserMessage("secret user")},
		SystemPrompt: "secret system prompt",
	}
	materialized, err := registry.ResolveTools(context.Background(), snapshot)
	if err != nil || len(materialized) != 1 {
		t.Fatalf("ResolveTools = %v, %v", materialized, err)
	}
	decoded, err := materialized[0].InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"value":1}`))
	if err != nil {
		t.Fatalf("DecodeToolInput error = %v", err)
	}
	result, err := materialized[0].Executor.Execute(context.Background(), runtime.ToolCall{ID: "call-1", Input: decoded})
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}
	if result.Output != `{"echo":{"value":1}}` {
		t.Fatalf("result = %+v", result)
	}
	component.mu.Lock()
	request := component.lastInput.(toolExecuteRequest)
	component.mu.Unlock()
	if request.Turn.AgentName != "agent" || request.Turn.MessageCount != 2 || request.Turn.RoleCounts.System != 1 || request.Turn.RoleCounts.User != 1 {
		t.Fatalf("turn metadata = %+v", request.Turn)
	}
	visible := strings.Join([]string{request.ToolCallID, request.InputJSON, request.Turn.RunID, request.Turn.SessionID, request.Turn.AgentName, request.Turn.AgentMode, request.Turn.ProviderID, request.Turn.ModelID}, " ")
	for _, secret := range []string{"agent-secret", "provider-secret", "resolved-secret", "secret conversation", "secret user", "secret system prompt"} {
		if strings.Contains(visible, secret) {
			t.Fatalf("secret %q reached guest input", secret)
		}
	}
}

func TestPermissionsPolicyWrapperAllDecisions(t *testing.T) {
	t.Parallel()
	component := &fakeComponent{call: func(_ context.Context, _ string, input, output any) error {
		request := input.(wittypes.PermissionRequest)
		decision := output.(*wittypes.PermissionDecision)
		switch request.Permission {
		case "allow":
			decision.Action = wittypes.PermissionActionAllow
		case "deny":
			decision.Action = wittypes.PermissionActionDeny
		case "ask":
			decision.Action = wittypes.PermissionActionAsk
		}
		decision.Reason = request.ArgumentsSummary
		return nil
	}}
	policy, err := loadPermissionsPolicy(context.Background(), fixtureConfig(t, []byte("policy component")), fakeFactory(component))
	if err != nil {
		t.Fatalf("loadPermissionsPolicy error = %v", err)
	}
	defer func() { _ = policy.Close() }()
	for permission, action := range map[string]permissions.Action{"allow": permissions.ActionAllow, "deny": permissions.ActionDeny, "ask": permissions.ActionAsk} {
		decision, err := policy.Decide(context.Background(), permissions.Request{Permission: permission, Pattern: "rewritten"})
		if err != nil || decision.Action != action || decision.Reason != "rewritten" {
			t.Errorf("Decide(%s) = %+v, %v", permission, decision, err)
		}
	}
	if err := policy.Close(); err != nil {
		t.Fatalf("second Close error = %v", err)
	}
	_, err = policy.Decide(context.Background(), permissions.Request{Permission: "allow"})
	if !IsKind(err, ErrorClosed) {
		t.Fatalf("call after Close error = %v", err)
	}
}

func TestModuleTimeoutActivelyInterruptsGuest(t *testing.T) {
	t.Parallel()
	component := newBlockingComponent()
	cfg := fixtureConfig(t, []byte("hung component"))
	cfg.Limits.Timeout = 20 * time.Millisecond
	cfg.Limits.CloseDrain = 100 * time.Millisecond
	policy, err := loadPermissionsPolicy(context.Background(), cfg, fakeFactory(component))
	if err != nil {
		t.Fatalf("loadPermissionsPolicy error = %v", err)
	}
	defer func() { _ = policy.Close() }()
	_, err = policy.Decide(context.Background(), permissions.Request{})
	if !IsKind(err, ErrorTimeout) || component.interrupts.Load() == 0 {
		t.Fatalf("timeout error = %v, interrupts = %d", err, component.interrupts.Load())
	}
}

func TestSecureModuleLoadingRejectsHashSizeAndEscapingSymlink(t *testing.T) {
	t.Parallel()
	component := &fakeComponent{}
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.wasm")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "escape.wasm")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	cfg := fixtureConfigAt(t, root, "escape.wasm", []byte("outside"))
	if _, err := loadPermissionsPolicy(context.Background(), cfg, fakeFactory(component)); !IsKind(err, ErrorPath) {
		t.Fatalf("symlink error = %v", err)
	}

	cfg = fixtureConfig(t, []byte("hash"))
	cfg.ExpectedSHA256 = strings.Repeat("0", 64)
	if _, err := loadPermissionsPolicy(context.Background(), cfg, fakeFactory(component)); !IsKind(err, ErrorHash) {
		t.Fatalf("hash error = %v", err)
	}

	cfg = fixtureConfig(t, []byte("oversized"))
	cfg.Limits.MaxModuleBytes = 2
	if _, err := loadPermissionsPolicy(context.Background(), cfg, fakeFactory(component)); !IsKind(err, ErrorSize) {
		t.Fatalf("size error = %v", err)
	}

	cfg = fixtureConfig(t, []byte("url"))
	cfg.Path = "https://example.test/guest.wasm"
	if _, err := loadPermissionsPolicy(context.Background(), cfg, fakeFactory(component)); !IsKind(err, ErrorPath) {
		t.Fatalf("URL error = %v", err)
	}
}

func TestContractAndPayloadViolationsAreClassifiedAndBounded(t *testing.T) {
	t.Parallel()
	cfg := fixtureConfig(t, []byte("wrong world"))
	factory := func(Limits) (engine, error) {
		return &fakeEngine{compileErr: errors.New("guest supplied very secret diagnostic")}, nil
	}
	_, err := loadPermissionsPolicy(context.Background(), cfg, factory)
	if !IsKind(err, ErrorContract) || strings.Contains(err.Error(), "very secret") || strings.Contains(err.Error(), cfg.Path) {
		t.Fatalf("contract error = %v", err)
	}

	component := &fakeComponent{call: func(_ context.Context, operation string, _, output any) error {
		if operation == "tool.metadata" {
			*output.(*wittypes.ToolMetadata) = wittypes.ToolMetadata{Name: "bad", ParametersJSONSchema: `{broken`}
		}
		return nil
	}}
	_, err = openTool(context.Background(), fixtureConfig(t, []byte("bad payload")), fakeFactory(component))
	if !IsKind(err, ErrorPayload) {
		t.Fatalf("payload error = %v", err)
	}
}

func TestCheckedInComponentsCompileAndExposeExpectedWorlds(t *testing.T) {
	root := filepath.Join("..", "examples", "wasm-extensions", "fixtures")
	for _, test := range []struct {
		name     string
		file     string
		contract worldContract
	}{
		{name: "tool", file: "tool.wasm", contract: toolContract},
		{name: "permissions-policy", file: "permissions-policy.wasm", contract: permissionsPolicyContract},
		{name: "context-source", file: "context-source.wasm", contract: contextSourceContract},
		{name: "event-sink", file: "event-sink.wasm", contract: eventSinkContract},
		{name: "hook", file: "hook.wasm", contract: hookContract},
		{name: "tool-middleware", file: "tool-middleware.wasm", contract: toolMiddlewareContract},
	} {
		t.Run(test.name, func(t *testing.T) {
			bytes, err := os.ReadFile(filepath.Join(root, test.file))
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(bytes)
			module, err := loadModule(context.Background(), ModuleConfig{
				Name: test.name, AllowedRoot: root, Path: test.file, ExpectedSHA256: hex.EncodeToString(hash[:]),
			}, test.contract, newWasmtimeEngine)
			if err != nil {
				t.Fatalf("loadModule error = %v", err)
			}
			if err := module.Close(); err != nil {
				t.Fatalf("Close error = %v", err)
			}
		})
	}
	bytes, err := os.ReadFile(filepath.Join(root, "permissions-policy.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(bytes)
	_, err = loadModule(context.Background(), ModuleConfig{
		Name: "wrong-world", AllowedRoot: root, Path: "permissions-policy.wasm", ExpectedSHA256: hex.EncodeToString(hash[:]),
	}, toolContract, newWasmtimeEngine)
	if !IsKind(err, ErrorContract) {
		t.Fatalf("wrong world error = %v", err)
	}
}

func TestCheckedInPhaseBComponentsRoundTrip(t *testing.T) {
	root := filepath.Join("..", "examples", "wasm-extensions", "fixtures")
	ctx := context.Background()

	source, err := OpenContextSource(ctx, checkedInFixtureConfig(t, root, "context-source.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	messages, err := source.loadContext(ctx, runtime.TurnSnapshot{RunID: "run", SessionID: "session"})
	if err != nil || len(messages) != 1 || messages[0].Content != "wasm context" {
		t.Fatalf("context source = %#v, %v", messages, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	sink, err := OpenEventSink(ctx, checkedInFixtureConfig(t, root, "event-sink.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit(ctx, runtime.Event{Kind: runtime.EventRunStarted, SessionID: "session", RunID: "run", Payload: json.RawMessage(`{"secret":"credential-sentinel"}`), Time: time.Unix(1, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}

	hook, err := OpenHook(ctx, checkedInFixtureConfig(t, root, "hook.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.beforeRun(ctx, session.Run{ID: "run", SessionID: "session"}); err != nil {
		t.Fatal(err)
	}
	snapshot := runtime.TurnSnapshot{RunID: "run", SessionID: "session", Messages: []*einoschema.Message{einoschema.UserMessage("hidden")}}
	if _, err := hook.beforeTurn(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := hook.afterTurn(ctx, snapshot, runtime.Result{RunID: "run"}); err != nil {
		t.Fatal(err)
	}
	if err := hook.afterRun(ctx, runtime.Result{RunID: "run"}); err != nil {
		t.Fatal(err)
	}
	if err := hook.Close(); err != nil {
		t.Fatal(err)
	}

	middleware, err := OpenToolMiddleware(ctx, checkedInFixtureConfig(t, root, "tool-middleware.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	call := runtime.ToolCall{ID: "call", RunID: "run", SessionID: "session", Input: json.RawMessage(`{"replace":true}`)}
	input, err := middleware.beforeToolCall(ctx, runtime.Tool{Name: "echo"}, call)
	if err != nil || string(input) != `{"from":"wasm"}` {
		var extensionErr *Error
		if errors.As(err, &extensionErr) {
			t.Fatalf("middleware input = %s, %v (cause: %v)", input, err, extensionErr.cause)
		}
		t.Fatalf("middleware input = %s, %v", input, err)
	}
	result, err := middleware.afterToolCall(ctx, runtime.Tool{Name: "echo"}, call, runtime.ToolResult{Structured: json.RawMessage(`{"replace":true}`), Metadata: map[string]string{"protected": "yes"}}, nil)
	if err != nil || string(result.Structured) != `{"result":"wasm"}` || result.Metadata["protected"] != "yes" {
		t.Fatalf("middleware result = %#v, %v", result, err)
	}
	if err := middleware.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckedInPhaseAComponentsRoundTrip(t *testing.T) {
	root := filepath.Join("..", "examples", "wasm-extensions", "fixtures")
	toolConfig := checkedInFixtureConfig(t, root, "tool.wasm")
	observer := einoobs.New(einoobs.Config{})
	toolConfig.Observer = observer
	loadedTool, err := OpenTool(context.Background(), toolConfig)
	if err != nil {
		var extensionErr *Error
		if errors.As(err, &extensionErr) {
			t.Fatalf("OpenTool error = %v (cause: %v)", err, extensionErr.cause)
		}
		t.Fatalf("OpenTool error = %v", err)
	}
	defer func() { _ = loadedTool.Close() }()
	definition := loadedTool.Definition()
	decoded, err := definition.Decode(context.Background(), json.RawMessage(`{"value":1}`))
	if err != nil {
		t.Fatalf("Decode error = %v", err)
	}
	output, err := definition.Execute(context.Background(), tools.Execution{
		Call: runtime.ToolCall{ID: "call-1"}, Input: decoded,
	})
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}
	raw, err := definition.Encode(context.Background(), output)
	if err != nil || string(raw) != `{"echo":{"value":1}}` {
		t.Fatalf("Encode = %s, %v", raw, err)
	}
	observations := observer.Snapshot().Observations
	if len(observations) != 1 || observations[0].Attributes["wasm.module.name"] != "tool.wasm" || observations[0].Attributes["log.level"] != "info" {
		t.Fatalf("guest log observations = %#v", observations)
	}

	policyConfig := checkedInFixtureConfig(t, root, "permissions-policy.wasm")
	policy, err := loadPermissionsPolicy(context.Background(), policyConfig, newWasmtimeEngine)
	if err != nil {
		t.Fatalf("LoadPermissionsPolicy error = %v", err)
	}
	defer func() { _ = policy.Close() }()
	for pattern, want := range map[string]permissions.Action{
		"allow": permissions.ActionAllow,
		"deny":  permissions.ActionDeny,
		"ask":   permissions.ActionAsk,
	} {
		decision, err := policy.Decide(context.Background(), permissions.Request{Pattern: pattern})
		if err != nil || decision.Action != want {
			t.Errorf("Decide(%q) = %+v, %v", pattern, decision, err)
		}
	}
}

func TestCheckedInToolFailuresAreBoundedAndClassified(t *testing.T) {
	root := filepath.Join("..", "examples", "wasm-extensions", "fixtures")
	for _, test := range []struct {
		name  string
		input string
		kind  ErrorKind
	}{
		{name: "trap", input: `{"mode":"trap"}`, kind: ErrorTrap},
		{name: "malformed", input: `{"mode":"malformed"}`, kind: ErrorPayload},
		{name: "oversized", input: `{"mode":"oversized"}`, kind: ErrorSize},
	} {
		t.Run(test.name, func(t *testing.T) {
			loaded, err := OpenTool(context.Background(), checkedInFixtureConfig(t, root, "tool.wasm"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = loaded.Close() }()
			_, err = executeLoadedDefinition(context.Background(), loaded.Definition(), test.input)
			if !IsKind(err, test.kind) {
				t.Fatalf("Execute error = %v, want %s", err, test.kind)
			}
		})
	}

	t.Run("active timeout", func(t *testing.T) {
		cfg := checkedInFixtureConfig(t, root, "tool.wasm")
		cfg.Limits.Timeout = 25 * time.Millisecond
		cfg.Limits.CloseDrain = time.Second
		loaded, err := OpenTool(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = loaded.Close() }()
		started := time.Now()
		_, err = executeLoadedDefinition(context.Background(), loaded.Definition(), `{"mode":"hang"}`)
		if !IsKind(err, ErrorTimeout) || time.Since(started) > time.Second {
			t.Fatalf("Execute error = %v after %s", err, time.Since(started))
		}
		if _, err := executeLoadedDefinition(context.Background(), loaded.Definition(), `{"value":1}`); err != nil {
			t.Fatalf("call after interrupted guest = %v", err)
		}
	})
}

func TestCheckedInToolCloseInterruptsInflightAndRejectsFurtherCalls(t *testing.T) {
	root := filepath.Join("..", "examples", "wasm-extensions", "fixtures")
	exporter := &signalExporter{entered: make(chan struct{})}
	cfg := checkedInFixtureConfig(t, root, "tool.wasm")
	cfg.Limits.Timeout = 5 * time.Second
	cfg.Limits.CloseDrain = time.Second
	cfg.Observer = einoobs.New(einoobs.Config{Exporter: exporter})
	loaded, err := OpenTool(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	definition := loaded.Definition()
	callError := make(chan error, 1)
	go func() {
		_, callErr := executeLoadedDefinition(context.Background(), definition, `{"mode":"hang"}`)
		callError <- callErr
	}()
	select {
	case <-exporter.entered:
	case <-time.After(time.Second):
		t.Fatal("guest did not enter execute")
	}
	if err := loaded.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
	select {
	case err := <-callError:
		if !IsKind(err, ErrorTrap) {
			t.Fatalf("in-flight call error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight call did not drain")
	}
	_, err = executeLoadedDefinition(context.Background(), definition, `{"value":1}`)
	if !IsKind(err, ErrorClosed) {
		t.Fatalf("call after Close error = %v", err)
	}
}

func TestCheckedInToolConcurrentUse(t *testing.T) {
	root := filepath.Join("..", "examples", "wasm-extensions", "fixtures")
	loaded, err := OpenTool(context.Background(), checkedInFixtureConfig(t, root, "tool.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = loaded.Close() }()
	definition := loaded.Definition()
	const calls = 12
	errorsChannel := make(chan error, calls)
	var wait sync.WaitGroup
	for index := range calls {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			output, callErr := executeLoadedDefinition(context.Background(), definition, `{"value":1}`)
			if callErr != nil {
				errorsChannel <- callErr
				return
			}
			if !strings.Contains(string(output.(json.RawMessage)), `"echo"`) {
				errorsChannel <- errors.New("missing echo output for call " + strconv.Itoa(index))
			}
		}(index)
	}
	wait.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		t.Error(err)
	}
}

func TestOrchestratorMixesNativeRuntimeWithWasmToolAndPolicy(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join("..", "examples", "wasm-extensions", "fixtures")
	loader := NewLoader()
	defer func() { _ = loader.Close(context.Background()) }()
	definition, err := loader.LoadTool(ctx, checkedInFixtureConfig(t, root, "tool.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	policy, err := loader.LoadPermissionsPolicy(ctx, checkedInFixtureConfig(t, root, "permissions-policy.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	registry := tools.NewRegistry()
	if _, err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var modelTurns atomic.Int64
	streamer := wasmScriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		if modelTurns.Add(1) == 1 {
			return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
				ID: "wasm-call", Type: "function", Function: einoschema.FunctionCall{
					Name: "wasm_echo", Arguments: `{"permission_pattern":"allow","value":1}`,
				},
			}})}, nil
		}
		for _, message := range request.Messages {
			if message.Role == einoschema.Tool && strings.Contains(message.Content, `"echo"`) {
				return []*einoschema.Message{einoschema.AssistantMessage("complete", nil)}, nil
			}
		}
		return nil, errors.New("Wasm tool result was not model-visible")
	})
	selection := model.Selection{ProviderID: "native", ModelID: "scripted"}
	orch, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(store),
		runtime.WithModelResolver(model.ResolverFunc(func(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
			return model.Resolved{
				Provider: model.Provider{ID: "native"},
				Model:    model.Descriptor{ID: "scripted", ProviderID: "native"},
				Streamer: streamer,
			}, nil
		})),
		runtime.WithIDGenerator(&wasmTestIDs{}),
		runtime.WithRunPlanProvider(wasmTestPlanProvider{registry: registry}),
		runtime.WithPermissions(policy),
		runtime.WithOwnerID("wasm-blackbox"),
	)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := orch.Start(ctx, runtime.Request{
		SessionID: "wasm-session",
		Input:     []*einoschema.Message{einoschema.UserMessage("run the Wasm tool")},
		Config: config.Snapshot{
			Agent: config.Agent{Name: "agent", Model: selection}, Model: selection,
			Metadata: map[string]string{"workspace_id": "workspace", "workspace_root": t.TempDir()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted || result.Error != nil || modelTurns.Load() != 2 {
		t.Fatalf("result = %+v, model turns = %d", result, modelTurns.Load())
	}
	toolCall, err := store.GetToolCall(ctx, "wasm-call")
	if err != nil {
		t.Fatal(err)
	}
	if toolCall.Status != session.ToolCallCompleted || !strings.Contains(string(toolCall.Input), `"permission_pattern":"allow"`) || !strings.Contains(string(toolCall.Output), `"echo"`) {
		t.Fatalf("durable tool call = %+v", toolCall)
	}
}

type wasmTestPlanProvider struct{ registry runtime.ToolRegistry }

func (p wasmTestPlanProvider) AcquireRunPlan(context.Context, runtime.RunPlanRequest) (*runtime.RunPlan, error) {
	return runtime.NewRunPlan(runtime.RunPlanSpec{Tools: []runtime.PlanTool{{
		Identity: session.ExtensionPlanEntry{InstanceID: "wasm-test", Kind: session.ExtensionTool, Artifact: session.ArtifactIdentity{Name: "wasm-test", Version: "1", Hash: "hash", SourceKind: string(extension.SourceNative)}, Required: true, Scope: session.ExtensionScope{Kind: string(extension.ScopeGlobal)}, CapabilityID: "wasm_echo/tool"},
		Resolve: func(ctx context.Context, snapshot runtime.TurnSnapshot) (runtime.Tool, error) {
			resolved, err := p.registry.ResolveTools(ctx, snapshot)
			if err != nil {
				return runtime.Tool{}, err
			}
			if len(resolved) != 1 {
				return runtime.Tool{}, fmt.Errorf("resolved %d tools", len(resolved))
			}
			return resolved[0], nil
		},
	}}})
}

func (p wasmTestPlanProvider) AcquireResumePlan(ctx context.Context, _ session.ExtensionPlanDescriptor) (*runtime.RunPlan, error) {
	return p.AcquireRunPlan(ctx, runtime.RunPlanRequest{})
}

func executeLoadedDefinition(ctx context.Context, definition tools.Definition, input string) (any, error) {
	decoded, err := definition.Decode(ctx, json.RawMessage(input))
	if err != nil {
		return nil, err
	}
	return definition.Execute(ctx, tools.Execution{Call: runtime.ToolCall{ID: "fixture-call"}, Input: decoded})
}

type signalExporter struct {
	once    sync.Once
	entered chan struct{}
}

func (e *signalExporter) Export(context.Context, []einoobs.Observation) error {
	e.once.Do(func() { close(e.entered) })
	return nil
}

func (*signalExporter) Flush(context.Context) error    { return nil }
func (*signalExporter) Shutdown(context.Context) error { return nil }

type wasmScriptedStreamer func(context.Context, model.Request) ([]*einoschema.Message, error)

func (s wasmScriptedStreamer) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	messages, err := s(ctx, request)
	if err != nil {
		return nil, err
	}
	reader, writer := einoschema.Pipe[*einoschema.Message](len(messages))
	go func() {
		defer writer.Close()
		for _, message := range messages {
			if writer.Send(message, nil) {
				return
			}
		}
	}()
	return reader, nil
}

type wasmTestIDs struct{ next atomic.Uint64 }

func (i *wasmTestIDs) id(prefix string) string {
	return prefix + "-" + strconv.FormatUint(i.next.Add(1), 10)
}

func (i *wasmTestIDs) NewRunID() session.RunID         { return session.RunID(i.id("run")) }
func (i *wasmTestIDs) NewMessageID() session.MessageID { return session.MessageID(i.id("message")) }
func (i *wasmTestIDs) NewPartID() session.PartID       { return session.PartID(i.id("part")) }
func (i *wasmTestIDs) NewToolCallID() session.ToolCallID {
	return session.ToolCallID(i.id("tool-call"))
}
func (i *wasmTestIDs) NewEventID() session.EventID { return session.EventID(i.id("event")) }
func (i *wasmTestIDs) NewEpochID() session.EpochID { return session.EpochID(i.id("epoch")) }

func checkedInFixtureConfig(t *testing.T, root, name string) ModuleConfig {
	t.Helper()
	bytes, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(bytes)
	return ModuleConfig{
		Name: name, AllowedRoot: root, Path: name, ExpectedSHA256: hex.EncodeToString(hash[:]),
	}
}

func runtimeResolvedWithSecret() model.Resolved {
	return model.Resolved{Provider: model.Provider{ID: "provider", Options: map[string]string{"secret": "resolved-secret"}}, Model: model.Descriptor{ID: "model", ProviderID: "provider", Options: map[string]string{"secret": "resolved-secret"}}}
}

func fixtureConfig(t *testing.T, bytes []byte) ModuleConfig {
	t.Helper()
	root := t.TempDir()
	return fixtureConfigAt(t, root, "component.wasm", bytes)
}

func fixtureConfigAt(t *testing.T, root, name string, bytes []byte) ModuleConfig {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, bytes, 0o600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(bytes)
	return ModuleConfig{Name: "fixture", AllowedRoot: root, Path: name, ExpectedSHA256: hex.EncodeToString(hash[:])}
}

type fakeEngine struct {
	component  compiledComponent
	compileErr error
	closed     atomic.Bool
}

func (e *fakeEngine) Compile(context.Context, []byte, worldContract) (compiledComponent, error) {
	if e.compileErr != nil {
		return nil, e.compileErr
	}
	return e.component, nil
}
func (e *fakeEngine) Close() error { e.closed.Store(true); return nil }

func fakeFactory(component compiledComponent) engineFactory {
	return func(Limits) (engine, error) { return &fakeEngine{component: component}, nil }
}

type fakeComponent struct {
	mu         sync.Mutex
	call       func(context.Context, string, any, any) error
	lastInput  any
	closed     atomic.Bool
	interrupts atomic.Int64
}

func (c *fakeComponent) Call(ctx context.Context, operation string, input, output any) error {
	if c.call == nil {
		return nil
	}
	return c.call(ctx, operation, input, output)
}
func (c *fakeComponent) Interrupt()   { c.interrupts.Add(1) }
func (c *fakeComponent) Close() error { c.closed.Store(true); return nil }

type blockingComponent struct {
	fakeComponent
	release chan struct{}
	once    sync.Once
}

func newBlockingComponent() *blockingComponent {
	return &blockingComponent{release: make(chan struct{})}
}
func (c *blockingComponent) Call(context.Context, string, any, any) error { <-c.release; return nil }
func (c *blockingComponent) Interrupt()                                   { c.interrupts.Add(1); c.once.Do(func() { close(c.release) }) }
