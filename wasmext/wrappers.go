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

	"github.com/mattsp1290/eino-agent/model"
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

// LoadedContextSource adapts context-source@0.1.0 and owns its component.
type LoadedContextSource struct{ module *module }

func (s *LoadedContextSource) Close() error { return s.module.Close() }

func (s *LoadedContextSource) LoadContext(ctx context.Context, snapshot runtime.TurnSnapshot) ([]*einoschema.Message, error) {
	return s.loadContextMetadata(ctx, turnMetadata(snapshot))
}

func (s *LoadedContextSource) loadBoundedContext(ctx context.Context, metadata runtime.BoundedTurnMetadata) ([]*einoschema.Message, error) {
	return s.loadContextMetadata(ctx, turnMetadataFromBounded(metadata))
}

func (s *LoadedContextSource) loadContextMetadata(ctx context.Context, turn wittypes.TurnMetadata) ([]*einoschema.Message, error) {
	var output []wittypes.TextMessage
	if err := s.module.call(ctx, "context-source.load-context", turnMetadataSize(turn), turn, &output); err != nil {
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
	return &LoadedContextSource{module: module}, nil
}

func LoadContextSource(ctx context.Context, cfg ModuleConfig) (runtime.ContextSource, error) {
	return OpenContextSource(ctx, cfg)
}

// LoadedEventSink emits only a bounded, content-free projection.
type LoadedEventSink struct{ module *module }

func (s *LoadedEventSink) Close() error { return s.module.Close() }

func (s *LoadedEventSink) Emit(ctx context.Context, event runtime.Event) error {
	input := wittypes.BoundedEvent{
		Kind: string(event.Kind), SessionID: string(event.SessionID), RunID: string(event.RunID),
		MessageID: string(event.MessageID), ToolCallID: string(event.ToolCallID), EpochID: string(event.EpochID),
		TimestampUnixMillis: event.Time.UTC().UnixMilli(), PayloadSummary: boundedEventSummary(event, s.module.limits.MaxOutputBytes),
	}
	return s.module.call(ctx, "event-sink.emit", boundedEventSize(input), input, nil)
}

func OpenEventSink(ctx context.Context, cfg ModuleConfig) (*LoadedEventSink, error) {
	module, err := loadModule(ctx, cfg, eventSinkContract, newEngine)
	if err != nil {
		return nil, err
	}
	return &LoadedEventSink{module: module}, nil
}

func LoadEventSink(ctx context.Context, cfg ModuleConfig) (runtime.EventSink, error) {
	return OpenEventSink(ctx, cfg)
}

// LoadedHook caches full turn metadata by run for deterministic after hooks.
type LoadedHook struct {
	module *module
	mu     sync.RWMutex
	turns  map[session.RunID]wittypes.TurnMetadata
}

func (h *LoadedHook) Close() error {
	h.mu.Lock()
	h.turns = nil
	h.mu.Unlock()
	return h.module.Close()
}

func (h *LoadedHook) BeforeRun(ctx context.Context, run session.Run) error {
	return h.beforeRunMetadata(ctx, partialTurnMetadata(run))
}

func (h *LoadedHook) beforeRunBounded(ctx context.Context, metadata runtime.BoundedTurnMetadata) error {
	return h.beforeRunMetadata(ctx, turnMetadataFromBounded(metadata))
}

func (h *LoadedHook) beforeRunMetadata(ctx context.Context, turn wittypes.TurnMetadata) error {
	return h.module.call(ctx, "hook.before-run", turnMetadataSize(turn), turn, nil)
}

func (h *LoadedHook) BeforeTurn(ctx context.Context, snapshot runtime.TurnSnapshot) (runtime.TurnSnapshot, error) {
	if err := h.beforeTurnMetadata(ctx, turnMetadata(snapshot)); err != nil {
		return runtime.TurnSnapshot{}, err
	}
	return snapshot.Clone(), nil
}

func (h *LoadedHook) beforeTurnBounded(ctx context.Context, metadata runtime.BoundedTurnMetadata) error {
	return h.beforeTurnMetadata(ctx, turnMetadataFromBounded(metadata))
}

func (h *LoadedHook) beforeTurnMetadata(ctx context.Context, turn wittypes.TurnMetadata) error {
	if err := h.module.call(ctx, "hook.before-turn", turnMetadataSize(turn), turn, nil); err != nil {
		return err
	}
	h.mu.Lock()
	if h.turns == nil {
		h.turns = make(map[session.RunID]wittypes.TurnMetadata)
	}
	h.turns[session.RunID(turn.RunID)] = turn
	h.mu.Unlock()
	return nil
}

func (h *LoadedHook) AfterTurn(ctx context.Context, snapshot runtime.TurnSnapshot, _ runtime.Result) error {
	turn := h.cachedTurn(snapshot.RunID, turnMetadata(snapshot))
	return h.module.call(ctx, "hook.after-turn", turnMetadataSize(turn), turn, nil)
}

func (h *LoadedHook) AfterRun(ctx context.Context, result runtime.Result) error {
	h.mu.Lock()
	turn, ok := h.turns[result.RunID]
	delete(h.turns, result.RunID)
	h.mu.Unlock()
	if !ok {
		turn = wittypes.TurnMetadata{RunID: string(result.RunID)}
	}
	return h.module.call(ctx, "hook.after-run", turnMetadataSize(turn), turn, nil)
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
	return &LoadedHook{module: module, turns: make(map[session.RunID]wittypes.TurnMetadata)}, nil
}

func LoadHook(ctx context.Context, cfg ModuleConfig) (runtime.Hook, error) { return OpenHook(ctx, cfg) }

// LoadedToolMiddleware preserves attachments and metadata on replacement.
type LoadedToolMiddleware struct{ module *module }

func (m *LoadedToolMiddleware) Close() error { return m.module.Close() }

func (m *LoadedToolMiddleware) BeforeToolCall(ctx context.Context, tool runtime.Tool, call runtime.ToolCall) (json.RawMessage, error) {
	turn := middlewareTurn(call, tool)
	request := toolMiddlewareBeforeRequest{ToolName: tool.Name, ToolCallID: string(call.ID), InputJSON: string(call.Input), Turn: turn}
	var replacement wittypes.Replacement
	if err := m.module.call(ctx, "tool-middleware.before-tool-call", len(request.ToolName)+len(request.ToolCallID)+len(request.InputJSON)+turnMetadataSize(turn), request, &replacement); err != nil {
		return nil, err
	}
	return applyInputReplacement(m.module, "tool-middleware.before-tool-call", call.Input, replacement)
}

func (m *LoadedToolMiddleware) AfterToolCall(ctx context.Context, tool runtime.Tool, call runtime.ToolCall, result runtime.ToolResult, _ error) (runtime.ToolResult, error) {
	encoded, err := toolResultJSON(result)
	if err != nil {
		return runtime.ToolResult{}, extensionError(ErrorPayload, m.module.identity, "tool-middleware.after-tool-call", err)
	}
	turn := middlewareTurn(call, tool)
	request := toolMiddlewareAfterRequest{ToolName: tool.Name, ToolCallID: string(call.ID), InputJSON: string(call.Input), OutputJSON: string(encoded), Turn: turn}
	var replacement wittypes.Replacement
	inputSize := len(request.ToolName) + len(request.ToolCallID) + len(request.InputJSON) + len(request.OutputJSON) + turnMetadataSize(turn)
	if err := m.module.call(ctx, "tool-middleware.after-tool-call", inputSize, request, &replacement); err != nil {
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
	return &LoadedToolMiddleware{module: module}, nil
}

func LoadToolMiddleware(ctx context.Context, cfg ModuleConfig) (runtime.ToolMiddleware, error) {
	return OpenToolMiddleware(ctx, cfg)
}

func partialTurnMetadata(run session.Run) wittypes.TurnMetadata {
	return wittypes.TurnMetadata{RunID: string(run.ID), SessionID: string(run.SessionID), EpochID: string(run.ContextEpoch), AgentName: run.Agent, ProviderID: run.ProviderID, ModelID: run.ModelID}
}

func middlewareTurn(call runtime.ToolCall, tool runtime.Tool) wittypes.TurnMetadata {
	return wittypes.TurnMetadata{RunID: string(call.RunID), SessionID: string(call.SessionID), ToolNames: cm.ToList([]string{tool.Name})}
}

func turnMetadataSize(turn wittypes.TurnMetadata) int {
	size := len(turn.RunID) + len(turn.SessionID) + len(turn.EpochID) + len(turn.AgentName) + len(turn.AgentMode) + len(turn.ProviderID) + len(turn.ModelID)
	for _, name := range turn.ToolNames.Slice() {
		size += len(name)
	}
	return size
}

func boundedEventSize(event wittypes.BoundedEvent) int {
	return len(event.Kind) + len(event.SessionID) + len(event.RunID) + len(event.MessageID) + len(event.ToolCallID) + len(event.EpochID) + len(event.PayloadSummary)
}

func boundedEventSummary(event runtime.Event, limit int64) string {
	summary := fmt.Sprintf("payload_bytes=%d redaction=%s live_only=%t", len(event.Payload), event.Redaction, event.LiveOnly)
	if int64(len(summary)) > limit {
		return summary[:limit]
	}
	return summary
}

func toolResultJSON(result runtime.ToolResult) (json.RawMessage, error) {
	if len(result.Structured) != 0 && json.Valid(result.Structured) {
		return cloneRawMessage(result.Structured), nil
	}
	return json.Marshal(result.Output)
}

func applyInputReplacement(module *module, operation string, original json.RawMessage, replacement wittypes.Replacement) (json.RawMessage, error) {
	if replacement.Unchanged() {
		return cloneRawMessage(original), nil
	}
	if raw := replacement.JSON(); raw != nil {
		if err := validateBoundedJSON([]byte(*raw), module.limits.MaxOutputBytes); err != nil {
			return nil, extensionError(payloadErrorKind(err), module.identity, operation, err)
		}
		return json.RawMessage(*raw), nil
	}
	if guestErr := replacement.Error(); guestErr != nil {
		return nil, structuredGuestError(*guestErr)
	}
	return nil, extensionError(ErrorContract, module.identity, operation, nil)
}

func applyResultReplacement(module *module, original runtime.ToolResult, replacement wittypes.Replacement) (runtime.ToolResult, error) {
	if replacement.Unchanged() {
		return cloneToolResult(original), nil
	}
	if rawText := replacement.JSON(); rawText != nil {
		raw := json.RawMessage(*rawText)
		if err := validateBoundedJSON(raw, module.limits.MaxOutputBytes); err != nil {
			return runtime.ToolResult{}, extensionError(payloadErrorKind(err), module.identity, "tool-middleware.after-tool-call", err)
		}
		next := cloneToolResult(original)
		var text string
		if json.Unmarshal(raw, &text) == nil {
			next.Output = text
			next.Structured = nil
		} else {
			next.Output = string(raw)
			next.Structured = cloneRawMessage(raw)
		}
		return next, nil
	}
	if guestErr := replacement.Error(); guestErr != nil {
		return runtime.ToolResult{}, structuredGuestError(*guestErr)
	}
	return runtime.ToolResult{}, extensionError(ErrorContract, module.identity, "tool-middleware.after-tool-call", nil)
}

func cloneToolResult(result runtime.ToolResult) runtime.ToolResult {
	next := result
	next.Structured = cloneRawMessage(result.Structured)
	next.Attachments = append([]runtime.Attachment(nil), result.Attachments...)
	next.Metadata = make(map[string]string, len(result.Metadata))
	for key, value := range result.Metadata {
		next.Metadata[key] = value
	}
	return next
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}

func structuredGuestError(input wittypes.StructuredError) error {
	code := strings.TrimSpace(input.Code)
	if code == "" || len(code) > 128 {
		code = "wasm_extension_rejected"
	}
	message := input.Message
	if len(message) > 1024 {
		message = message[:1024]
	}
	return model.Error{Code: code, Message: message, Retryable: input.Retryable}
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

func turnMetadataFromBounded(metadata runtime.BoundedTurnMetadata) wittypes.TurnMetadata {
	return wittypes.TurnMetadata{
		RunID: string(metadata.RunID), SessionID: string(metadata.SessionID), EpochID: string(metadata.EpochID),
		AgentName: metadata.AgentName, AgentMode: metadata.AgentMode,
		ProviderID: metadata.ProviderID, ModelID: metadata.ModelID,
		ToolNames: cm.ToList(append([]string(nil), metadata.ToolNames...)), MessageCount: metadata.MessageCount,
		RoleCounts: wittypes.RoleCounts{
			System: metadata.RoleCounts.System, User: metadata.RoleCounts.User,
			Assistant: metadata.RoleCounts.Assistant, Tool: metadata.RoleCounts.Tool,
		},
		HasSystemPrompt: metadata.HasSystemPrompt,
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

// LoadContextSource loads and tracks a context-source component.
func (l *Loader) LoadContextSource(ctx context.Context, cfg ModuleConfig) (runtime.ContextSource, error) {
	module, err := loadModule(ctx, cfg, contextSourceContract, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(module); err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedContextSource{module: module}, nil
}

// LoadEventSink loads and tracks an event-sink component.
func (l *Loader) LoadEventSink(ctx context.Context, cfg ModuleConfig) (runtime.EventSink, error) {
	module, err := loadModule(ctx, cfg, eventSinkContract, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(module); err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedEventSink{module: module}, nil
}

// LoadHook loads and tracks a hook component.
func (l *Loader) LoadHook(ctx context.Context, cfg ModuleConfig) (runtime.Hook, error) {
	module, err := loadModule(ctx, cfg, hookContract, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(module); err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedHook{module: module, turns: make(map[session.RunID]wittypes.TurnMetadata)}, nil
}

// LoadToolMiddleware loads and tracks a tool-middleware component.
func (l *Loader) LoadToolMiddleware(ctx context.Context, cfg ModuleConfig) (runtime.ToolMiddleware, error) {
	module, err := loadModule(ctx, cfg, toolMiddlewareContract, l.engineFactory())
	if err != nil {
		return nil, err
	}
	if err := l.track(module); err != nil {
		_ = module.Close()
		return nil, err
	}
	return &LoadedToolMiddleware{module: module}, nil
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
