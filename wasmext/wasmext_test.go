package wasmext

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"
	einoobs "github.com/mattsp1290/eino-obs"
	"go.bytecodealliance.org/cm"

	"github.com/mattsp1290/eino-agent/composition"
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
		case "tool.permission-pattern":
			component.mu.Lock()
			component.lastInput = input
			component.mu.Unlock()
			*output.(*string) = "workspace.read:value-1"
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
	defer func() { _ = loaded.close() }()
	definition, err := loaded.definition.Clone()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := runtime.TurnSnapshot{
		RunID: "run-1", SessionID: "session-1", EpochID: "epoch-1",
		Config: config.Snapshot{
			Agent: config.Agent{Name: "agent", Mode: "primary", Options: map[string]string{"SECRET": "agent-secret"}},
		},
		Model: runtimeResolvedWithSecret(), Messages: []*einoschema.Message{einoschema.SystemMessage("secret conversation"), einoschema.UserMessage("secret user")},
		SystemPrompt: "secret system prompt",
	}
	materialized, err := tools.Materialize(context.Background(), definition, runtime.NewToolScopeContext(snapshot))
	if err != nil {
		t.Fatalf("Materialize = %v", err)
	}
	pattern, err := materialized.Pattern.ResolvePermissionPattern(context.Background(), json.RawMessage(`{"value":1}`))
	if err != nil || pattern != "workspace.read:value-1" {
		t.Fatalf("permission pattern = %q, %v", pattern, err)
	}
	component.mu.Lock()
	patternInput := component.lastInput
	component.mu.Unlock()
	if patternInput != `{"value":1}` {
		t.Fatalf("permission input = %#v", patternInput)
	}
	decoded, err := materialized.InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"value":1}`))
	if err != nil {
		t.Fatalf("DecodeToolInput error = %v", err)
	}
	result, err := materialized.Executor.Execute(context.Background(), runtime.ToolCall{ID: "call-1", Input: decoded, Context: runtime.ToolContext{Turn: runtime.BoundedTurnMetadata{
		RunID: "run-1", SessionID: "session-1", EpochID: "epoch-1", AgentName: "agent", AgentMode: "primary",
		ToolNames: []string{"wasm_echo"}, MessageCount: 2, RoleCounts: runtime.MessageRoleCounts{System: 1, User: 1}, HasSystemPrompt: true,
	}}})
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}
	if result.Output != `{"echo":{"value":1}}` {
		t.Fatalf("result = %+v", result)
	}
	component.mu.Lock()
	request := component.lastInput.(toolExecuteRequest)
	component.mu.Unlock()
	if request.Turn.AgentName != "agent" || request.Turn.MessageCount != 2 || request.Turn.RoleCounts.System != 1 || request.Turn.RoleCounts.User != 1 || !reflect.DeepEqual(request.Turn.ToolNames.Slice(), []string{"wasm_echo"}) {
		t.Fatalf("turn metadata = %+v", request.Turn)
	}
	visible := strings.Join([]string{request.ToolCallID, request.InputJSON, request.Turn.RunID, request.Turn.SessionID, request.Turn.AgentName, request.Turn.AgentMode, request.Turn.ProviderID, request.Turn.ModelID}, " ")
	for _, secret := range []string{"agent-secret", "provider-secret", "resolved-secret", "secret conversation", "secret user", "secret system prompt"} {
		if strings.Contains(visible, secret) {
			t.Fatalf("secret %q reached guest input", secret)
		}
	}
}

func TestToolPermissionPatternBounds(t *testing.T) {
	tests := []struct {
		name      string
		output    string
		maxOutput int64
		wantKind  ErrorKind
	}{
		{name: "runtime maximum", output: strings.Repeat("x", 4096)},
		{name: "over runtime maximum", output: strings.Repeat("x", 4097), wantKind: ErrorSize},
		{name: "tighter module maximum", output: strings.Repeat("x", 33), maxOutput: 32, wantKind: ErrorSize},
		{name: "empty", wantKind: ErrorContract},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			component := &fakeComponent{call: func(_ context.Context, operation string, _ any, output any) error {
				switch operation {
				case "tool.metadata":
					*output.(*wittypes.ToolMetadata) = wittypes.ToolMetadata{Name: "bounded", ParametersJSONSchema: `{"type":"object"}`}
				case "tool.permission-pattern":
					*output.(*string) = test.output
				}
				return nil
			}}
			cfg := fixtureConfig(t, []byte(test.name))
			cfg.Limits.MaxOutputBytes = test.maxOutput
			loaded, err := openTool(context.Background(), cfg, fakeFactory(component))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = loaded.close() }()
			materialized, err := tools.Materialize(context.Background(), loaded.definition, runtime.ToolScopeContext{})
			if err != nil {
				t.Fatal(err)
			}
			pattern, err := materialized.Pattern.ResolvePermissionPattern(context.Background(), json.RawMessage(`{"value":1}`))
			if test.wantKind != "" {
				if !IsKind(err, test.wantKind) {
					t.Fatalf("pattern error = %v, want %s", err, test.wantKind)
				}
				return
			}
			if err != nil || pattern != test.output {
				t.Fatalf("pattern bytes = %d, error = %v", len(pattern), err)
			}
		})
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
	defer func() { _ = policy.close() }()
	for permission, action := range map[string]permissions.Action{"allow": permissions.ActionAllow, "deny": permissions.ActionDeny, "ask": permissions.ActionAsk} {
		decision, err := policy.Decide(context.Background(), permissions.Request{Permission: permission, Pattern: "rewritten"})
		if err != nil || decision.Action != action || decision.Reason != "rewritten" {
			t.Errorf("Decide(%s) = %+v, %v", permission, decision, err)
		}
	}
	if err := policy.close(); err != nil {
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
	defer func() { _ = policy.close() }()
	_, err = policy.Decide(context.Background(), permissions.Request{})
	if !IsKind(err, ErrorTimeout) || component.interrupts.Load() == 0 {
		t.Fatalf("timeout error = %v, interrupts = %d", err, component.interrupts.Load())
	}
}

func TestModuleInvocationPanicIsClassifiedAndReleasesGate(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	component := &fakeComponent{call: func(_ context.Context, _ string, _ any, output any) error {
		if calls.Add(1) == 1 {
			panic("component panic")
		}
		output.(*wittypes.PermissionDecision).Action = wittypes.PermissionActionAllow
		return nil
	}}
	policy, err := loadPermissionsPolicy(context.Background(), fixtureConfig(t, []byte("panicking component")), fakeFactory(component))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := policy.Decide(context.Background(), permissions.Request{}); !IsKind(err, ErrorTrap) {
		t.Fatalf("panic error = %v, want trap", err)
	}
	decision, err := policy.Decide(context.Background(), permissions.Request{})
	if err != nil || decision.Action != permissions.ActionAllow {
		t.Fatalf("call after panic = %+v, %v", decision, err)
	}
	if err := policy.close(); err != nil {
		t.Fatalf("Close after panic = %v", err)
	}
	if !component.closed.Load() {
		t.Fatal("component was not closed after recovered panic")
	}
}

func TestModuleTimeoutQuarantinesStubbornWorkerUntilItExits(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	component := &fakeComponent{}
	component.call = func(context.Context, string, any, any) error {
		calls.Add(1)
		close(started)
		<-release
		return nil
	}
	engineInstance := &fakeEngine{component: component}
	cfg := fixtureConfig(t, []byte("stubborn component"))
	cfg.Limits.Timeout = 20 * time.Millisecond
	cfg.Limits.CloseDrain = 20 * time.Millisecond
	policy, err := loadPermissionsPolicy(context.Background(), cfg, func(Limits) (engine, error) { return engineInstance, nil })
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() {
		_, callErr := policy.Decide(context.Background(), permissions.Request{})
		first <- callErr
	}()
	<-started
	queued := make(chan error, 1)
	go func() {
		_, callErr := policy.Decide(context.Background(), permissions.Request{})
		queued <- callErr
	}()
	if err := <-first; !IsKind(err, ErrorTimeout) {
		t.Fatalf("first call error = %v, want timeout", err)
	}
	select {
	case err := <-queued:
		if !IsKind(err, ErrorClosed) {
			t.Fatalf("queued call error = %v, want closed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued background call did not wake on quarantine")
	}
	if calls.Load() != 1 || component.closed.Load() || engineInstance.closed.Load() {
		t.Fatalf("calls=%d component_closed=%t engine_closed=%t before worker exit", calls.Load(), component.closed.Load(), engineInstance.closed.Load())
	}
	if err := policy.close(); !IsKind(err, ErrorTimeout) {
		t.Fatalf("Close before worker exit = %v, want timeout", err)
	}
	close(release)
	select {
	case <-policy.module.finalized:
	case <-time.After(time.Second):
		t.Fatal("deferred module finalization did not complete")
	}
	if err := policy.close(); err != nil {
		t.Fatalf("Close after worker exit = %v", err)
	}
	if !component.closed.Load() || !engineInstance.closed.Load() {
		t.Fatalf("component_closed=%t engine_closed=%t after worker exit", component.closed.Load(), engineInstance.closed.Load())
	}
}

func TestLoaderCloseCanObserveDeferredModuleFinalization(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	component := &fakeComponent{}
	component.call = func(context.Context, string, any, any) error {
		close(started)
		<-release
		return nil
	}
	engineInstance := &fakeEngine{component: component}
	loader := NewLoader()
	loader.factory = func(Limits) (engine, error) { return engineInstance, nil }
	cfg := fixtureConfig(t, []byte("loader stubborn component"))
	cfg.Limits.Timeout = time.Second
	cfg.Limits.CloseDrain = 20 * time.Millisecond
	policy, err := loader.LoadHostPermissionsPolicy(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	callDone := make(chan error, 1)
	go func() {
		_, callErr := policy.Decide(context.Background(), permissions.Request{})
		callDone <- callErr
	}()
	<-started
	closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := loader.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Loader.Close = %v, want context deadline", err)
	}
	if component.closed.Load() || engineInstance.closed.Load() {
		t.Fatal("Loader.Close destroyed resources while worker remained active")
	}
	close(release)
	<-callDone
	if err := loader.Close(context.Background()); err != nil {
		t.Fatalf("second Loader.Close = %v", err)
	}
	if !component.closed.Load() || !engineInstance.closed.Load() {
		t.Fatal("Loader.Close did not report actual deferred finalization")
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
	requireCGO(t)
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
	requireCGO(t)
	root := filepath.Join("..", "examples", "wasm-extensions", "fixtures")
	ctx := context.Background()
	loader := NewLoader()
	defer func() { _ = loader.Close(context.Background()) }()

	source, err := openContextSource(ctx, checkedInFixtureConfig(t, root, "context-source.wasm"), loader.engineFactory())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.close() }()
	metadata := runtime.BoundedTurnMetadata{RunID: "run", SessionID: "session", MessageCount: 1, RoleCounts: runtime.MessageRoleCounts{User: 1}}
	messages, err := source.loadBoundedContext(ctx, metadata)
	if err != nil || len(messages) != 1 || messages[0].Content != "wasm context" {
		t.Fatalf("context source = %#v, %v", messages, err)
	}
	sink, err := loadEventSinkForTest(loader, ctx, checkedInFixtureConfig(t, root, "event-sink.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit(ctx, session.EventRecord{Kind: runtime.EventRunStarted, SessionID: "session", RunID: "run", Payload: json.RawMessage(`{"secret":"credential-sentinel"}`), CreatedAt: time.Unix(1, 0)}); err != nil {
		t.Fatal(err)
	}
	hook, err := openHook(ctx, checkedInFixtureConfig(t, root, "hook.wasm"), loader.engineFactory())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hook.close() }()
	if err := hook.beforeRunBounded(ctx, metadata); err != nil {
		t.Fatal(err)
	}
	if err := hook.finish(ctx, metadata); err != nil {
		t.Fatal(err)
	}

	middleware, err := openToolMiddleware(ctx, checkedInFixtureConfig(t, root, "tool-middleware.wasm"), loader.engineFactory())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = middleware.close() }()
	call := runtime.ToolCall{ID: "call", RunID: "run", SessionID: "session", Input: json.RawMessage(`{"replace":true}`)}
	input, err := middleware.beforeToolCall(ctx, runtime.Tool{Name: "echo"}, call)
	if err != nil || string(input) != `{"from":"wasm"}` {
		var extensionErr *Error
		if errors.As(err, &extensionErr) {
			t.Fatalf("middleware input = %s, %v (cause: %v)", input, err, extensionErr.cause)
		}
		t.Fatalf("middleware input = %s, %v", input, err)
	}
	result, err := middleware.afterToolCall(ctx, runtime.Tool{Name: "echo"}, call, runtime.ToolResult{Structured: json.RawMessage(`{"replace":true}`), Metadata: map[string]string{"protected": "yes"}})
	if err != nil || string(result.Structured) != `{"result":"wasm"}` || result.Metadata["protected"] != "yes" {
		t.Fatalf("middleware result = %#v, %v", result, err)
	}
}

func TestCheckedInPhaseAComponentsRoundTrip(t *testing.T) {
	requireCGO(t)
	root := filepath.Join("..", "examples", "wasm-extensions", "fixtures")
	toolConfig := checkedInFixtureConfig(t, root, "tool.wasm")
	observer := einoobs.New(einoobs.Config{})
	toolConfig.Observer = observer
	loader := NewLoader()
	defer func() { _ = loader.Close(context.Background()) }()
	definition, err := loadToolForTest(loader, context.Background(), toolConfig)
	if err != nil {
		var extensionErr *Error
		if errors.As(err, &extensionErr) {
			t.Fatalf("OpenTool error = %v (cause: %v)", err, extensionErr.cause)
		}
		t.Fatalf("OpenTool error = %v", err)
	}
	materialized, err := tools.Materialize(context.Background(), definition, runtime.ToolScopeContext{})
	if err != nil {
		t.Fatalf("Materialize error = %v", err)
	}
	decoded, err := materialized.InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"value":1}`))
	if err != nil {
		t.Fatalf("Decode error = %v", err)
	}
	output, err := materialized.Executor.Execute(context.Background(), runtime.ToolCall{ID: "call-1", Input: decoded})
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}
	if string(output.Structured) != `{"echo":{"value":1}}` {
		t.Fatalf("Execute = %s", output.Structured)
	}
	observations := observer.Snapshot().Observations
	if len(observations) != 1 || observations[0].Attributes["wasm.module.name"] != "tool.wasm" || observations[0].Attributes["log.level"] != "info" {
		t.Fatalf("guest log observations = %#v", observations)
	}

	policyConfig := checkedInFixtureConfig(t, root, "permissions-policy.wasm")
	policy, err := loader.LoadHostPermissionsPolicy(context.Background(), policyConfig)
	if err != nil {
		t.Fatalf("LoadPermissionsPolicy error = %v", err)
	}
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

func TestCheckedInGuestLogExporterPanicIsContained(t *testing.T) {
	requireCGO(t)
	root := filepath.Join("..", "examples", "wasm-extensions", "fixtures")
	cfg := checkedInFixtureConfig(t, root, "tool.wasm")
	cfg.Observer = einoobs.New(einoobs.Config{Exporter: panickingExporter{}})
	loader := NewLoader()
	defer func() { _ = loader.Close(context.Background()) }()
	definition, err := loadToolForTest(loader, context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	output, err := executeLoadedDefinition(context.Background(), definition, `{"value":1}`)
	if err != nil || !strings.Contains(string(output.Structured), `"echo"`) {
		t.Fatalf("Execute after exporter panic = %s, %v", output.Structured, err)
	}
}

func TestCheckedInToolFailuresAreBoundedAndClassified(t *testing.T) {
	requireCGO(t)
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
			loader := NewLoader()
			defer func() { _ = loader.Close(context.Background()) }()
			definition, err := loadToolForTest(loader, context.Background(), checkedInFixtureConfig(t, root, "tool.wasm"))
			if err != nil {
				t.Fatal(err)
			}
			_, err = executeLoadedDefinition(context.Background(), definition, test.input)
			if !IsKind(err, test.kind) {
				t.Fatalf("Execute error = %v, want %s", err, test.kind)
			}
		})
	}

	t.Run("active timeout", func(t *testing.T) {
		cfg := checkedInFixtureConfig(t, root, "tool.wasm")
		cfg.Limits.Timeout = 25 * time.Millisecond
		cfg.Limits.CloseDrain = time.Second
		loader := NewLoader()
		defer func() { _ = loader.Close(context.Background()) }()
		definition, err := loadToolForTest(loader, context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		_, err = executeLoadedDefinition(context.Background(), definition, `{"mode":"hang"}`)
		if !IsKind(err, ErrorTimeout) || time.Since(started) > time.Second {
			t.Fatalf("Execute error = %v after %s", err, time.Since(started))
		}
		if _, err := executeLoadedDefinition(context.Background(), definition, `{"value":1}`); err != nil {
			t.Fatalf("call after interrupted guest = %v", err)
		}
	})
}

func TestCheckedInToolCloseInterruptsInflightAndRejectsFurtherCalls(t *testing.T) {
	requireCGO(t)
	root := filepath.Join("..", "examples", "wasm-extensions", "fixtures")
	exporter := &signalExporter{entered: make(chan struct{})}
	cfg := checkedInFixtureConfig(t, root, "tool.wasm")
	cfg.Limits.Timeout = 5 * time.Second
	cfg.Limits.CloseDrain = time.Second
	cfg.Observer = einoobs.New(einoobs.Config{Exporter: exporter})
	loader := NewLoader()
	definition, err := loadToolForTest(loader, context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
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
	if err := loader.Close(context.Background()); err != nil {
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
	requireCGO(t)
	root := filepath.Join("..", "examples", "wasm-extensions", "fixtures")
	loader := NewLoader()
	defer func() { _ = loader.Close(context.Background()) }()
	definition, err := loadToolForTest(loader, context.Background(), checkedInFixtureConfig(t, root, "tool.wasm"))
	if err != nil {
		t.Fatal(err)
	}
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
			if !strings.Contains(string(output.Structured), `"echo"`) {
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
	requireCGO(t)
	ctx := context.Background()
	root := filepath.Join("..", "examples", "wasm-extensions", "fixtures")
	loader := NewLoader()
	defer func() { _ = loader.Close(context.Background()) }()
	policy, err := loader.LoadHostPermissionsPolicy(ctx, checkedInFixtureConfig(t, root, "permissions-policy.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	component := extension.Component{InstanceID: "wasm-test", Artifact: extension.Artifact{Name: "wasm-test", Version: "1", Hash: "wasm-test-artifact", ConfigHash: "wasm-test-config", SourceKind: extension.SourceNative}}
	mount, err := registry.Mount(ctx, component, composition.InstallerFunc(func(ctx context.Context, registrar *composition.Registrar) error {
		return loader.RegisterTool(ctx, registrar, composition.ToolRegistration{ID: "tool", Scope: extension.GlobalScope()}, checkedInFixtureConfig(t, root, "tool.wasm"))
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
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
					Name: "wasm_echo", Arguments: `{"value":1}`,
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
		runtime.WithRunPlanProvider(registry),
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
	if toolCall.Status != session.ToolCallCompleted || toolCall.Pattern != "allow" || string(toolCall.Input) != `{"value":1}` || !strings.Contains(string(toolCall.Output), `"echo"`) {
		t.Fatalf("durable tool call = %+v", toolCall)
	}
}

func executeLoadedDefinition(ctx context.Context, definition tools.Definition, input string) (runtime.ToolResult, error) {
	materialized, err := tools.Materialize(ctx, definition, runtime.ToolScopeContext{})
	if err != nil {
		return runtime.ToolResult{}, err
	}
	decoded, err := materialized.InputDecoder.DecodeToolInput(ctx, json.RawMessage(input))
	if err != nil {
		return runtime.ToolResult{}, err
	}
	return materialized.Executor.Execute(ctx, runtime.ToolCall{ID: "fixture-call", Input: decoded})
}

func loadToolForTest(loader *Loader, ctx context.Context, cfg ModuleConfig) (tools.Definition, error) {
	loaded, err := openTool(ctx, cfg, loader.engineFactory())
	if err != nil {
		return tools.Definition{}, err
	}
	definition, err := loaded.definition.Clone()
	if err != nil {
		_ = loaded.close()
		return tools.Definition{}, err
	}
	if err := loader.track(loaded.module, nil); err != nil {
		_ = loaded.close()
		return tools.Definition{}, err
	}
	return definition, nil
}

func loadEventSinkForTest(loader *Loader, ctx context.Context, cfg ModuleConfig) (*loadedEventSink, error) {
	loaded, err := openEventSink(ctx, cfg, loader.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := loader.track(loaded.module, nil); err != nil {
		_ = loaded.close()
		return nil, err
	}
	return loaded, nil
}

type signalExporter struct {
	once    sync.Once
	entered chan struct{}
}

type panickingExporter struct{}

func (panickingExporter) Export(context.Context, []einoobs.Observation) error {
	panic("exporter panic")
}

func (panickingExporter) Flush(context.Context) error    { return nil }
func (panickingExporter) Shutdown(context.Context) error { return nil }

func (e *signalExporter) Export(context.Context, []einoobs.Observation) error {
	e.once.Do(func() { close(e.entered) })
	return nil
}

func (*signalExporter) Flush(context.Context) error    { return nil }
func (*signalExporter) Shutdown(context.Context) error { return nil }

type wasmScriptedStreamer func(context.Context, model.Request) ([]*einoschema.Message, error)

func (s wasmScriptedStreamer) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	messages, err := s(ctx, request)
	if err != nil {
		return nil, err
	}
	reader, writer := einoschema.Pipe[model.StreamDelta](len(messages))
	go func() {
		defer writer.Close()
		for _, message := range messages {
			if writer.Send(model.StreamDelta{Message: message, Usage: model.UsageFromMessage(message)}, nil) {
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

func requireCGO(t *testing.T) {
	t.Helper()
	if !cgoEnabled {
		t.Skip("Wasmtime component integration requires cgo")
	}
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
	closeCalls atomic.Int32
	interrupts atomic.Int64
}

func (c *fakeComponent) invoke(ctx context.Context, operation string, input, output any) error {
	if c.call == nil {
		return nil
	}
	return c.call(ctx, operation, input, output)
}
func (c *fakeComponent) ToolMetadata(ctx context.Context) (output wittypes.ToolMetadata, err error) {
	err = c.invoke(ctx, "tool.metadata", nil, &output)
	return
}
func (c *fakeComponent) ToolPermissionPattern(ctx context.Context, input string) (output string, err error) {
	err = c.invoke(ctx, "tool.permission-pattern", input, &output)
	return
}
func (c *fakeComponent) ExecuteTool(ctx context.Context, input toolExecuteRequest) (output string, err error) {
	err = c.invoke(ctx, "tool.execute", input, &output)
	return
}
func (c *fakeComponent) DecidePermissions(ctx context.Context, input wittypes.PermissionRequest) (output wittypes.PermissionDecision, err error) {
	err = c.invoke(ctx, "permissions-policy.decide", input, &output)
	return
}
func (c *fakeComponent) LoadContext(ctx context.Context, input wittypes.TurnMetadata) (output []wittypes.TextMessage, err error) {
	err = c.invoke(ctx, "context-source.load-context", input, &output)
	return
}
func (c *fakeComponent) EmitEvent(ctx context.Context, input wittypes.BoundedEvent) error {
	return c.invoke(ctx, "event-sink.emit", input, nil)
}
func (c *fakeComponent) BeforeRun(ctx context.Context, input wittypes.TurnMetadata) error {
	return c.invoke(ctx, "hook.before-run", input, nil)
}
func (c *fakeComponent) AfterRun(ctx context.Context, input wittypes.TurnMetadata) error {
	return c.invoke(ctx, "hook.after-run", input, nil)
}
func (c *fakeComponent) BeforeToolCall(ctx context.Context, input toolMiddlewareBeforeRequest) (output wittypes.Replacement, err error) {
	err = c.invoke(ctx, "tool-middleware.before-tool-call", input, &output)
	return
}
func (c *fakeComponent) AfterToolCall(ctx context.Context, input toolMiddlewareAfterRequest) (output wittypes.Replacement, err error) {
	err = c.invoke(ctx, "tool-middleware.after-tool-call", input, &output)
	return
}
func (c *fakeComponent) Interrupt() { c.interrupts.Add(1) }
func (c *fakeComponent) Close() error {
	c.closeCalls.Add(1)
	c.closed.Store(true)
	return nil
}

type blockingComponent struct {
	fakeComponent
	release chan struct{}
	once    sync.Once
}

func newBlockingComponent() *blockingComponent {
	component := &blockingComponent{release: make(chan struct{})}
	component.call = func(context.Context, string, any, any) error { <-component.release; return nil }
	return component
}
func (c *blockingComponent) Interrupt() { c.interrupts.Add(1); c.once.Do(func() { close(c.release) }) }
