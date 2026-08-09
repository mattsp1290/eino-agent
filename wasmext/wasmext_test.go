package wasmext

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"
	"go.bytecodealliance.org/cm"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/runtime"
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
