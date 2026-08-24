package wasmext

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"

	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/tools"
	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/types"
)

// LoadedTool owns one Wasm-backed tool definition and implements io.Closer.
// Register Definition through the ordinary tools.Registry path.
type LoadedTool struct {
	module     *module
	component  toolComponent
	definition tools.Definition
}

// Definition returns the native tool definition backed by this component.
func (t *LoadedTool) Definition() (tools.Definition, error) { return t.definition.Clone() }

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
	component, err := componentAs[toolComponent](module, "tool.metadata")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	var metadata wittypes.ToolMetadata
	if err := module.call(ctx, "tool.metadata", 0, func(callCtx context.Context) error {
		var callErr error
		metadata, callErr = component.ToolMetadata(callCtx)
		return callErr
	}); err != nil {
		_ = module.Close()
		return nil, err
	}
	definition, err := toolDefinition(module, component, metadata)
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedTool{module: module, component: component, definition: definition}, nil
}

// LoadTool returns a native tools.Definition. Embedders that need a direct
// close handle should use OpenTool or Loader.LoadTool.
func LoadTool(ctx context.Context, cfg ModuleConfig) (tools.Definition, error) {
	loaded, err := OpenTool(ctx, cfg)
	if err != nil {
		return tools.Definition{}, err
	}
	return loaded.Definition()
}

func toolDefinition(module *module, component toolComponent, metadata wittypes.ToolMetadata) (tools.Definition, error) {
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
		Metadata:  map[string]string{"wasm_module": module.identity.name, "wasm_sha256": module.identity.hash},
		Retention: runtime.RetentionPolicy{MaxInlineBytes: module.limits.MaxOutputBytes},
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
			ToolCallID: string(execution.Call.ID), InputJSON: string(raw), Turn: turnMetadataFromBounded(execution.Context.Turn),
		}
		var output string
		if err := module.call(ctx, "tool.execute", len(raw), func(callCtx context.Context) error {
			var callErr error
			output, callErr = component.ExecuteTool(callCtx, request)
			return callErr
		}); err != nil {
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

type permissionsPolicy struct {
	module    *module
	component permissionsComponent
}

// Close releases the compiled policy component.
func (p *permissionsPolicy) Close() error { return p.module.Close() }

func (p *permissionsPolicy) Decide(ctx context.Context, request permissions.Request) (permissions.Decision, error) {
	input := wittypes.PermissionRequest{
		ToolName: request.ToolName, ToolCallID: request.ToolCallID, Permission: request.Permission,
		ArgumentsSummary: request.Pattern, SessionID: request.SessionID, RunID: request.RunID,
	}
	inputSize := len(input.ToolName) + len(input.ToolCallID) + len(input.Permission) + len(input.ArgumentsSummary) + len(input.SessionID) + len(input.RunID)
	var output wittypes.PermissionDecision
	if err := p.module.call(ctx, "permissions-policy.decide", inputSize, func(callCtx context.Context) error {
		var callErr error
		output, callErr = p.component.DecidePermissions(callCtx, input)
		return callErr
	}); err != nil {
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
	component, err := componentAs[permissionsComponent](module, "permissions-policy.decide")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &permissionsPolicy{module: module, component: component}, nil
}

// LoadedContextSource adapts context-source@0.1.0 and owns its component.
type LoadedContextSource struct {
	module    *module
	component contextComponent
}

func (s *LoadedContextSource) Close() error { return s.module.Close() }

func (s *LoadedContextSource) loadContext(ctx context.Context, snapshot runtime.TurnSnapshot) ([]*einoschema.Message, error) {
	return s.loadContextMetadata(ctx, turnMetadata(snapshot))
}

func (s *LoadedContextSource) loadBoundedContext(ctx context.Context, metadata runtime.BoundedTurnMetadata) ([]*einoschema.Message, error) {
	return s.loadContextMetadata(ctx, turnMetadataFromBounded(metadata))
}

func (s *LoadedContextSource) loadContextMetadata(ctx context.Context, turn wittypes.TurnMetadata) ([]*einoschema.Message, error) {
	var output []wittypes.TextMessage
	if err := s.module.call(ctx, "context-source.load-context", turnMetadataSize(turn), func(callCtx context.Context) error {
		var callErr error
		output, callErr = s.component.LoadContext(callCtx, turn)
		return callErr
	}); err != nil {
		return nil, err
	}
	messages := make([]*einoschema.Message, 0, len(output))
	var total int64
	for _, message := range output {
		total += int64(len(message.Text))
		if total > s.module.limits.MaxOutputBytes {
			return nil, extensionError(ErrorSize, s.module.identity, "context-source.load-context", nil)
		}
		switch message.Role {
		case wittypes.TextRoleSystem:
			messages = append(messages, einoschema.SystemMessage(message.Text))
		case wittypes.TextRoleUser:
			messages = append(messages, einoschema.UserMessage(message.Text))
		case wittypes.TextRoleAssistant:
			messages = append(messages, einoschema.AssistantMessage(message.Text, nil))
		default:
			return nil, extensionError(ErrorContract, s.module.identity, "context-source.load-context", nil)
		}
	}
	return messages, nil
}

func OpenContextSource(ctx context.Context, cfg ModuleConfig) (*LoadedContextSource, error) {
	module, err := loadModule(ctx, cfg, contextSourceContract, newEngine)
	if err != nil {
		return nil, err
	}
	component, err := componentAs[contextComponent](module, "context-source.load-context")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedContextSource{module: module, component: component}, nil
}

// LoadedEventSink emits only a bounded, content-free projection.
type LoadedEventSink struct {
	module    *module
	component eventComponent
}

func (s *LoadedEventSink) Close() error { return s.module.Close() }

func (s *LoadedEventSink) Emit(ctx context.Context, event runtime.Event) error {
	input := wittypes.BoundedEvent{
		Kind: string(event.Kind), SessionID: string(event.SessionID), RunID: string(event.RunID),
		MessageID: string(event.MessageID), ToolCallID: string(event.ToolCallID), EpochID: string(event.EpochID),
		TimestampUnixMillis: event.Time.UTC().UnixMilli(), PayloadSummary: boundedEventSummary(event, s.module.limits.MaxOutputBytes),
	}
	return s.module.call(ctx, "event-sink.emit", boundedEventSize(input), func(callCtx context.Context) error {
		return s.component.EmitEvent(callCtx, input)
	})
}

func OpenEventSink(ctx context.Context, cfg ModuleConfig) (*LoadedEventSink, error) {
	module, err := loadModule(ctx, cfg, eventSinkContract, newEngine)
	if err != nil {
		return nil, err
	}
	component, err := componentAs[eventComponent](module, "event-sink.emit")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedEventSink{module: module, component: component}, nil
}

func LoadEventSink(ctx context.Context, cfg ModuleConfig) (runtime.EventSink, error) {
	return OpenEventSink(ctx, cfg)
}

// LoadedHook caches full turn metadata by run for deterministic after hooks.
type LoadedHook struct {
	module    *module
	component hookComponent
	mu        sync.RWMutex
	turns     map[session.RunID]wittypes.TurnMetadata
}

func (h *LoadedHook) Close() error {
	h.mu.Lock()
	h.turns = nil
	h.mu.Unlock()
	return h.module.Close()
}

func (h *LoadedHook) beforeRun(ctx context.Context, run session.Run) error {
	return h.beforeRunMetadata(ctx, partialTurnMetadata(run))
}

func (h *LoadedHook) beforeTurn(ctx context.Context, snapshot runtime.TurnSnapshot) (runtime.TurnSnapshot, error) {
	if err := h.beforeTurnMetadata(ctx, turnMetadata(snapshot)); err != nil {
		return runtime.TurnSnapshot{}, err
	}
	return snapshot.Clone(), nil
}

func (h *LoadedHook) beforeRunBounded(ctx context.Context, metadata runtime.BoundedTurnMetadata) error {
	turn := turnMetadataFromBounded(metadata)
	h.cacheTurn(turn)
	return h.beforeRunMetadata(ctx, turn)
}

func (h *LoadedHook) beforeRunMetadata(ctx context.Context, turn wittypes.TurnMetadata) error {
	return h.module.call(ctx, "hook.before-run", turnMetadataSize(turn), func(callCtx context.Context) error { return h.component.BeforeRun(callCtx, turn) })
}

func (h *LoadedHook) beforeTurnBounded(ctx context.Context, metadata runtime.BoundedTurnMetadata) error {
	return h.beforeTurnMetadata(ctx, turnMetadataFromBounded(metadata))
}

func (h *LoadedHook) beforeTurnMetadata(ctx context.Context, turn wittypes.TurnMetadata) error {
	if err := h.module.call(ctx, "hook.before-turn", turnMetadataSize(turn), func(callCtx context.Context) error { return h.component.BeforeTurn(callCtx, turn) }); err != nil {
		return err
	}
	h.cacheTurn(turn)
	return nil
}

func (h *LoadedHook) cacheTurn(turn wittypes.TurnMetadata) {
	h.mu.Lock()
	if h.turns == nil {
		h.turns = make(map[session.RunID]wittypes.TurnMetadata)
	}
	h.turns[session.RunID(turn.RunID)] = turn
	h.mu.Unlock()
}

func (h *LoadedHook) afterTurn(ctx context.Context, snapshot runtime.TurnSnapshot, _ runtime.Result) error {
	turn := h.cachedTurn(snapshot.RunID, turnMetadata(snapshot))
	return h.module.call(ctx, "hook.after-turn", turnMetadataSize(turn), func(callCtx context.Context) error { return h.component.AfterTurn(callCtx, turn) })
}

func (h *LoadedHook) afterRun(ctx context.Context, result runtime.Result) error {
	h.mu.Lock()
	turn, ok := h.turns[result.RunID]
	delete(h.turns, result.RunID)
	h.mu.Unlock()
	if !ok {
		turn = wittypes.TurnMetadata{RunID: string(result.RunID)}
	}
	return h.module.call(ctx, "hook.after-run", turnMetadataSize(turn), func(callCtx context.Context) error { return h.component.AfterRun(callCtx, turn) })
}

func (h *LoadedHook) cachedTurn(runID session.RunID, fallback wittypes.TurnMetadata) wittypes.TurnMetadata {
	h.mu.RLock()
	turn, ok := h.turns[runID]
	h.mu.RUnlock()
	if ok {
		return turn
	}
	return fallback
}

func OpenHook(ctx context.Context, cfg ModuleConfig) (*LoadedHook, error) {
	module, err := loadModule(ctx, cfg, hookContract, newEngine)
	if err != nil {
		return nil, err
	}
	component, err := componentAs[hookComponent](module, "hook.before-run")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedHook{module: module, component: component, turns: make(map[session.RunID]wittypes.TurnMetadata)}, nil
}

// LoadedToolMiddleware preserves attachments and metadata on replacement.
type LoadedToolMiddleware struct {
	module    *module
	component middlewareComponent
}

func (m *LoadedToolMiddleware) Close() error { return m.module.Close() }

func (m *LoadedToolMiddleware) beforeToolCall(ctx context.Context, tool runtime.Tool, call runtime.ToolCall) (json.RawMessage, error) {
	turn := middlewareTurn(call, tool)
	request := toolMiddlewareBeforeRequest{ToolName: tool.Name, ToolCallID: string(call.ID), InputJSON: string(call.Input), Turn: turn}
	var replacement wittypes.Replacement
	if err := m.module.call(ctx, "tool-middleware.before-tool-call", len(request.ToolName)+len(request.ToolCallID)+len(request.InputJSON)+turnMetadataSize(turn), func(callCtx context.Context) error {
		var callErr error
		replacement, callErr = m.component.BeforeToolCall(callCtx, request)
		return callErr
	}); err != nil {
		return nil, err
	}
	return applyInputReplacement(m.module, "tool-middleware.before-tool-call", call.Input, replacement)
}

func (m *LoadedToolMiddleware) afterToolCall(ctx context.Context, tool runtime.Tool, call runtime.ToolCall, result runtime.ToolResult, _ error) (runtime.ToolResult, error) {
	encoded, err := toolResultJSON(result)
	if err != nil {
		return runtime.ToolResult{}, extensionError(ErrorPayload, m.module.identity, "tool-middleware.after-tool-call", err)
	}
	turn := middlewareTurn(call, tool)
	request := toolMiddlewareAfterRequest{ToolName: tool.Name, ToolCallID: string(call.ID), InputJSON: string(call.Input), OutputJSON: string(encoded), Turn: turn}
	var replacement wittypes.Replacement
	inputSize := len(request.ToolName) + len(request.ToolCallID) + len(request.InputJSON) + len(request.OutputJSON) + turnMetadataSize(turn)
	if err := m.module.call(ctx, "tool-middleware.after-tool-call", inputSize, func(callCtx context.Context) error {
		var callErr error
		replacement, callErr = m.component.AfterToolCall(callCtx, request)
		return callErr
	}); err != nil {
		return runtime.ToolResult{}, err
	}
	return applyResultReplacement(m.module, result, replacement)
}

type toolMiddlewareBeforeRequest struct {
	ToolName, ToolCallID, InputJSON string
	Turn                            wittypes.TurnMetadata
}

type toolMiddlewareAfterRequest struct {
	ToolName, ToolCallID, InputJSON, OutputJSON string
	Turn                                        wittypes.TurnMetadata
}

func OpenToolMiddleware(ctx context.Context, cfg ModuleConfig) (*LoadedToolMiddleware, error) {
	module, err := loadModule(ctx, cfg, toolMiddlewareContract, newEngine)
	if err != nil {
		return nil, err
	}
	component, err := componentAs[middlewareComponent](module, "tool-middleware.before-tool-call")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedToolMiddleware{module: module, component: component}, nil
}
