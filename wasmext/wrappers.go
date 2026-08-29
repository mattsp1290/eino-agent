package wasmext

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"

	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/tools"
	wittypes "github.com/mattsp1290/eino-agent/wasmext/gen/eino-agent/extensions/v0.1.0/types"
)

type loadedTool struct {
	module     *module
	component  toolComponent
	definition tools.Definition
}

func (t *loadedTool) close() error { return t.module.Close() }

func openTool(ctx context.Context, cfg ModuleConfig, factory engineFactory) (*loadedTool, error) {
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
	return &loadedTool{module: module, component: component, definition: definition}, nil
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
	definition.Normalize = func(_ context.Context, raw json.RawMessage) (json.RawMessage, error) {
		if err := validateBoundedJSON(raw, module.limits.MaxInputBytes); err != nil {
			return nil, extensionError(payloadErrorKind(err), module.identity, "tool.normalize", err)
		}
		return append(json.RawMessage(nil), raw...), nil
	}
	definition.Pattern = func(_ context.Context, raw json.RawMessage) (string, error) {
		var value struct {
			PermissionPattern string `json:"permission_pattern"`
		}
		if err := json.Unmarshal(raw, &value); err != nil {
			return "", extensionError(ErrorPayload, module.identity, "tool.permission-pattern", err)
		}
		if value.PermissionPattern == "" {
			return metadata.Name, nil
		}
		return value.PermissionPattern, nil
	}
	definition.Execute = func(ctx context.Context, execution tools.Execution) (json.RawMessage, error) {
		raw := execution.Input
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
	return definition, nil
}

type toolExecuteRequest struct {
	ToolCallID string
	InputJSON  string
	Turn       wittypes.TurnMetadata
}

type loadedPermissionsPolicy struct {
	module    *module
	component permissionsComponent
}

func (p *loadedPermissionsPolicy) close() error { return p.module.Close() }

func (p *loadedPermissionsPolicy) Decide(ctx context.Context, request permissions.Request) (permissions.Decision, error) {
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

func loadPermissionsPolicy(ctx context.Context, cfg ModuleConfig, factory engineFactory) (*loadedPermissionsPolicy, error) {
	module, err := loadModule(ctx, cfg, permissionsPolicyContract, factory)
	if err != nil {
		return nil, err
	}
	component, err := componentAs[permissionsComponent](module, "permissions-policy.decide")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &loadedPermissionsPolicy{module: module, component: component}, nil
}

type loadedContextSource struct {
	module    *module
	component contextComponent
}

func (s *loadedContextSource) close() error { return s.module.Close() }

func (s *loadedContextSource) loadBoundedContext(ctx context.Context, metadata runtime.BoundedTurnMetadata) ([]*einoschema.Message, error) {
	return s.loadContextMetadata(ctx, turnMetadataFromBounded(metadata))
}

func (s *loadedContextSource) loadContextMetadata(ctx context.Context, turn wittypes.TurnMetadata) ([]*einoschema.Message, error) {
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

func openContextSource(ctx context.Context, cfg ModuleConfig, factory engineFactory) (*loadedContextSource, error) {
	module, err := loadModule(ctx, cfg, contextSourceContract, factory)
	if err != nil {
		return nil, err
	}
	component, err := componentAs[contextComponent](module, "context-source.load-context")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &loadedContextSource{module: module, component: component}, nil
}

type loadedEventSink struct {
	module    *module
	component eventComponent
}

func (s *loadedEventSink) close() error { return s.module.Close() }

func (s *loadedEventSink) Emit(ctx context.Context, event runtime.Event) error {
	input := wittypes.BoundedEvent{
		Kind: string(event.Kind), SessionID: string(event.SessionID), RunID: string(event.RunID),
		MessageID: string(event.MessageID), ToolCallID: string(event.ToolCallID), EpochID: string(event.EpochID),
		TimestampUnixMillis: event.Time.UTC().UnixMilli(), PayloadSummary: boundedEventSummary(event, s.module.limits.MaxOutputBytes),
	}
	return s.module.call(ctx, "event-sink.emit", boundedEventSize(input), func(callCtx context.Context) error {
		return s.component.EmitEvent(callCtx, input)
	})
}

func openEventSink(ctx context.Context, cfg ModuleConfig, factory engineFactory) (*loadedEventSink, error) {
	module, err := loadModule(ctx, cfg, eventSinkContract, factory)
	if err != nil {
		return nil, err
	}
	component, err := componentAs[eventComponent](module, "event-sink.emit")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &loadedEventSink{module: module, component: component}, nil
}

type loadedHook struct {
	module    *module
	component hookComponent
}

func (h *loadedHook) close() error { return h.module.Close() }

func (h *loadedHook) beforeRunBounded(ctx context.Context, metadata runtime.BoundedTurnMetadata) error {
	return h.beforeRunMetadata(ctx, turnMetadataFromBounded(metadata))
}

func (h *loadedHook) beforeRunMetadata(ctx context.Context, turn wittypes.TurnMetadata) error {
	return h.module.call(ctx, "hook.before-run", turnMetadataSize(turn), func(callCtx context.Context) error { return h.component.BeforeRun(callCtx, turn) })
}

func (h *loadedHook) finish(ctx context.Context, metadata runtime.BoundedTurnMetadata) error {
	turn := turnMetadataFromBounded(metadata)
	return h.module.call(ctx, "hook.after-run", turnMetadataSize(turn), func(callCtx context.Context) error { return h.component.AfterRun(callCtx, turn) })
}

func openHook(ctx context.Context, cfg ModuleConfig, factory engineFactory) (*loadedHook, error) {
	module, err := loadModule(ctx, cfg, hookContract, factory)
	if err != nil {
		return nil, err
	}
	component, err := componentAs[hookComponent](module, "hook.before-run")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &loadedHook{module: module, component: component}, nil
}

// loadedToolMiddleware preserves attachments and metadata on replacement.
type loadedToolMiddleware struct {
	module    *module
	component middlewareComponent
}

func (m *loadedToolMiddleware) close() error { return m.module.Close() }

func (m *loadedToolMiddleware) beforeToolCall(ctx context.Context, tool runtime.Tool, call runtime.ToolCall) (json.RawMessage, error) {
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

func (m *loadedToolMiddleware) afterToolCall(ctx context.Context, tool runtime.Tool, call runtime.ToolCall, result runtime.ToolResult) (runtime.ToolResult, error) {
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

func openToolMiddleware(ctx context.Context, cfg ModuleConfig, factory engineFactory) (*loadedToolMiddleware, error) {
	module, err := loadModule(ctx, cfg, toolMiddlewareContract, factory)
	if err != nil {
		return nil, err
	}
	component, err := componentAs[middlewareComponent](module, "tool-middleware.before-tool-call")
	if err != nil {
		_ = module.Close()
		return nil, err
	}
	return &loadedToolMiddleware{module: module, component: component}, nil
}
