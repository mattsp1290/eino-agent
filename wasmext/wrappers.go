package wasmext

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"
	"go.bytecodealliance.org/cm"

	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/tools"
	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/types"
)

// LoadedTool owns one Wasm-backed tool definition and implements io.Closer.
// Register Definition through the ordinary tools.Registry path.
type LoadedTool struct {
	module     *module
	definition tools.Definition
}

// Definition returns the native tool definition backed by this component.
func (t *LoadedTool) Definition() tools.Definition { return t.definition.Clone() }

// Close releases the compiled component and stops accepting calls.
func (t *LoadedTool) Close() error { return t.module.Close() }

// OpenTool loads a tool component while retaining an explicit close handle.
func OpenTool(ctx context.Context, cfg ModuleConfig) (*LoadedTool, error) {
	return openTool(ctx, cfg, newEngine)
}

func openTool(ctx context.Context, cfg ModuleConfig, factory engineFactory) (*LoadedTool, error) {
	module, err := loadModule(ctx, cfg, toolContract, factory)
	if err != nil {
		return nil, err
	}
	var metadata wittypes.ToolMetadata
	if err := module.call(ctx, "tool.metadata", 0, nil, &metadata); err != nil {
		_ = module.Close()
		return nil, err
	}
	definition, err := toolDefinition(module, metadata)
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedTool{module: module, definition: definition}, nil
}

// LoadTool returns a native tools.Definition. Embedders that need a direct
// close handle should use OpenTool or Loader.LoadTool.
func LoadTool(ctx context.Context, cfg ModuleConfig) (tools.Definition, error) {
	loaded, err := OpenTool(ctx, cfg)
	if err != nil {
		return tools.Definition{}, err
	}
	return loaded.Definition(), nil
}

func toolDefinition(module *module, metadata wittypes.ToolMetadata) (tools.Definition, error) {
	if strings.TrimSpace(metadata.Name) == "" || len(metadata.Name) > 128 || int64(len(metadata.Description)) > module.limits.MaxOutputBytes {
		return tools.Definition{}, extensionError(ErrorContract, module.identity, "tool.metadata", errors.New("invalid tool metadata"))
	}
	var parameters *einoschema.ParamsOneOf
	if metadata.ParametersJSONSchema != "" {
		if int64(len(metadata.ParametersJSONSchema)) > module.limits.MaxOutputBytes {
			return tools.Definition{}, extensionError(ErrorSize, module.identity, "tool.metadata", nil)
		}
		var schema jsonschema.Schema
		if err := json.Unmarshal([]byte(metadata.ParametersJSONSchema), &schema); err != nil {
			return tools.Definition{}, extensionError(ErrorPayload, module.identity, "tool.metadata", err)
		}
		parameters = einoschema.NewParamsOneOfByJSONSchema(&schema)
	}
	permissionsList := append([]string(nil), metadata.RequiredPermissions.Slice()...)
	for _, permission := range permissionsList {
		if permission == "" || len(permission) > 256 {
			return tools.Definition{}, extensionError(ErrorContract, module.identity, "tool.metadata", nil)
		}
	}
	definition := tools.Definition{
		Name: metadata.Name, Description: metadata.Description, Parameters: parameters,
		RetrySafe: metadata.RetrySafe, Permissions: permissionsList,
		Metadata: map[string]string{"wasm_module": module.identity.name, "wasm_sha256": module.identity.hash},
	}
	definition.Decode = func(_ context.Context, raw json.RawMessage) (any, error) {
		if err := validateBoundedJSON(raw, module.limits.MaxInputBytes); err != nil {
			return nil, extensionError(payloadErrorKind(err), module.identity, "tool.decode", err)
		}
		return append(json.RawMessage(nil), raw...), nil
	}
	definition.Execute = func(ctx context.Context, execution tools.Execution) (any, error) {
		raw, ok := execution.Input.(json.RawMessage)
		if !ok {
			return nil, extensionError(ErrorPayload, module.identity, "tool.execute", errors.New("decoded input is not JSON"))
		}
		request := toolExecuteRequest{
			ToolCallID: string(execution.Call.ID), InputJSON: string(raw), Turn: turnMetadata(execution.Snapshot),
		}
		var output string
		if err := module.call(ctx, "tool.execute", len(raw), request, &output); err != nil {
			return nil, err
		}
		if err := validateBoundedJSON([]byte(output), module.limits.MaxOutputBytes); err != nil {
			return nil, extensionError(payloadErrorKind(err), module.identity, "tool.execute", err)
		}
		return json.RawMessage(output), nil
	}
	definition.Encode = func(_ context.Context, value any) (json.RawMessage, error) {
		raw, ok := value.(json.RawMessage)
		if !ok {
			return nil, extensionError(ErrorPayload, module.identity, "tool.encode", errors.New("guest output is not JSON"))
		}
		if err := validateBoundedJSON(raw, module.limits.MaxOutputBytes); err != nil {
			return nil, extensionError(payloadErrorKind(err), module.identity, "tool.encode", err)
		}
		return append(json.RawMessage(nil), raw...), nil
	}
	return definition, nil
}

type toolExecuteRequest struct {
	ToolCallID string
	InputJSON  string
	Turn       wittypes.TurnMetadata
}

type permissionsPolicy struct{ module *module }

// Close releases the compiled policy component.
func (p *permissionsPolicy) Close() error { return p.module.Close() }

func (p *permissionsPolicy) Decide(ctx context.Context, request permissions.Request) (permissions.Decision, error) {
	input := wittypes.PermissionRequest{
		ToolName: request.ToolName, ToolCallID: request.ToolCallID, Permission: request.Permission,
		ArgumentsSummary: request.Pattern, SessionID: request.SessionID, RunID: request.RunID,
	}
	inputSize := len(input.ToolName) + len(input.ToolCallID) + len(input.Permission) + len(input.ArgumentsSummary) + len(input.SessionID) + len(input.RunID)
	var output wittypes.PermissionDecision
	if err := p.module.call(ctx, "permissions-policy.decide", inputSize, input, &output); err != nil {
		return permissions.Decision{}, err
	}
	if int64(len(output.Reason)) > p.module.limits.MaxOutputBytes {
		return permissions.Decision{}, extensionError(ErrorSize, p.module.identity, "permissions-policy.decide", nil)
	}
	decision := permissions.Decision{Reason: output.Reason, Message: output.Reason}
	switch output.Action {
	case wittypes.PermissionActionAllow:
		decision.Action = permissions.ActionAllow
	case wittypes.PermissionActionDeny:
		decision.Action = permissions.ActionDeny
	case wittypes.PermissionActionAsk:
		decision.Action = permissions.ActionAsk
	default:
		return permissions.Decision{}, extensionError(ErrorContract, p.module.identity, "permissions-policy.decide", fmt.Errorf("invalid action"))
	}
	return decision, nil
}

// LoadPermissionsPolicy loads a component as the native permissions.Policy
// interface. The returned concrete value also implements io.Closer.
func LoadPermissionsPolicy(ctx context.Context, cfg ModuleConfig) (permissions.Policy, error) {
	return loadPermissionsPolicy(ctx, cfg, newEngine)
}

func loadPermissionsPolicy(ctx context.Context, cfg ModuleConfig, factory engineFactory) (*permissionsPolicy, error) {
	module, err := loadModule(ctx, cfg, permissionsPolicyContract, factory)
	if err != nil {
		return nil, err
	}
	return &permissionsPolicy{module: module}, nil
}

func turnMetadata(snapshot runtime.TurnSnapshot) wittypes.TurnMetadata {
	toolNames := make([]string, 0, len(snapshot.Tools))
	for _, tool := range snapshot.Tools {
		toolNames = append(toolNames, tool.Name)
	}
	counts := wittypes.RoleCounts{}
	for _, message := range snapshot.Messages {
		if message == nil {
			continue
		}
		switch message.Role {
		case einoschema.System:
			counts.System = saturatingIncrement(counts.System)
		case einoschema.User:
			counts.User = saturatingIncrement(counts.User)
		case einoschema.Assistant:
			counts.Assistant = saturatingIncrement(counts.Assistant)
		case einoschema.Tool:
			counts.Tool = saturatingIncrement(counts.Tool)
		}
	}
	messageCount := len(snapshot.Messages)
	if messageCount > math.MaxUint32 {
		messageCount = math.MaxUint32
	}
	return wittypes.TurnMetadata{
		RunID: string(snapshot.RunID), SessionID: string(snapshot.SessionID), EpochID: string(snapshot.EpochID),
		AgentName: snapshot.Config.Agent.Name, AgentMode: snapshot.Config.Agent.Mode,
		ProviderID: string(snapshot.Model.Provider.ID), ModelID: string(snapshot.Model.Model.ID),
		ToolNames: cm.ToList(toolNames), MessageCount: uint32(messageCount), RoleCounts: counts,
		HasSystemPrompt: snapshot.SystemPrompt != "" || snapshot.Config.Agent.SystemPrompt != "",
	}
}

func saturatingIncrement(value uint32) uint32 {
	if value == math.MaxUint32 {
		return value
	}
	return value + 1
}

func payloadErrorKind(err error) ErrorKind {
	if errors.Is(err, errModuleTooLarge) {
		return ErrorSize
	}
	return ErrorPayload
}

var (
	_ permissions.Policy         = (*permissionsPolicy)(nil)
	_ interface{ Close() error } = (*permissionsPolicy)(nil)
	_ interface{ Close() error } = (*LoadedTool)(nil)
)

// Loader owns all modules opened through it and provides one-call shutdown.
type Loader struct {
	mu      sync.Mutex
	closed  bool
	modules []*module
	factory engineFactory
}

// NewLoader returns an empty module owner.
func NewLoader() *Loader { return &Loader{factory: newEngine} }

// LoadTool loads and tracks a Wasm-backed native tool definition.
func (l *Loader) LoadTool(ctx context.Context, cfg ModuleConfig) (tools.Definition, error) {
	loaded, err := openTool(ctx, cfg, l.engineFactory())
	if err != nil {
		return tools.Definition{}, err
	}
	if err := l.track(loaded.module); err != nil {
		_ = loaded.Close()
		return tools.Definition{}, err
	}
	return loaded.Definition(), nil
}

// LoadPermissionsPolicy loads and tracks a Wasm-backed native policy.
func (l *Loader) LoadPermissionsPolicy(ctx context.Context, cfg ModuleConfig) (permissions.Policy, error) {
	policy, err := loadPermissionsPolicy(ctx, cfg, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(policy.module); err != nil {
		_ = policy.Close()
		return nil, err
	}
	return policy, nil
}

func (l *Loader) engineFactory() engineFactory {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.factory == nil {
		l.factory = newEngine
	}
	return l.factory
}

func (l *Loader) track(module *module) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return extensionError(ErrorClosed, module.identity, "load", nil)
	}
	l.modules = append(l.modules, module)
	return nil
}

// Close stops new loads, interrupts in-flight calls, and closes every tracked
// module exactly once. The supplied context bounds the aggregate shutdown.
func (l *Loader) Close(ctx context.Context) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	modules := append([]*module(nil), l.modules...)
	l.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		var errs []error
		for index := len(modules) - 1; index >= 0; index-- {
			errs = append(errs, modules[index].Close())
		}
		done <- errors.Join(errs...)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
