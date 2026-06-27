package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"

	"github.com/mattsp1290/eino-agent/runtime"
)

var (
	// ErrInvalidDefinition reports an incomplete or inconsistent tool definition.
	ErrInvalidDefinition = errors.New("invalid tool definition")
	// ErrDuplicateRegistration reports a second registration for the same tool name.
	ErrDuplicateRegistration = errors.New("duplicate tool registration")
	// ErrStaleRegistration reports an update based on an older registration generation.
	ErrStaleRegistration = errors.New("stale tool registration")
	// ErrMalformedInput reports model tool input that cannot be decoded.
	ErrMalformedInput = errors.New("malformed tool input")
)

// Decoder converts model-provided JSON input into a typed host value.
type Decoder func(ctx context.Context, raw json.RawMessage) (any, error)

// Encoder converts a typed tool result into bounded structured JSON.
type Encoder func(ctx context.Context, value any) (json.RawMessage, error)

// InputNormalizer converts decoded model input into canonical JSON for durable
// storage and the later execution pass. If omitted, json.Marshal is used.
type InputNormalizer func(ctx context.Context, input any) (json.RawMessage, error)

// Executor executes one typed tool invocation.
type Executor func(ctx context.Context, execution Execution) (any, error)

// ScopeResolver returns the runtime scope for one materialized definition.
type ScopeResolver func(snapshot runtime.TurnSnapshot, definition Definition) runtime.ToolScope

// Definition is a typed tool declaration registered by host code or adapters.
type Definition struct {
	Name        string
	Description string
	Parameters  *einoschema.ParamsOneOf
	Decode      Decoder
	Normalize   InputNormalizer
	Encode      Encoder
	Execute     Executor
	RetrySafe   bool
	Scope       ScopeResolver
	Concurrency runtime.ToolConcurrency
	Retention   runtime.RetentionPolicy
	Permissions []string
	Metadata    map[string]string
}

// Execution is the decoded input and durable runtime context for one call.
type Execution struct {
	Input    any
	Call     runtime.ToolCall
	Snapshot runtime.TurnSnapshot
}

// Registration identifies one active tool registration generation.
type Registration struct {
	Name       string
	Generation uint64
}

// Registry stores typed tool definitions and materializes them per snapshot.
type Registry struct {
	mu   sync.RWMutex
	defs map[string]registered
	next uint64
}

type registered struct {
	definition Definition
	generation uint64
}

// NewRegistry returns an empty typed tool registry.
func NewRegistry() *Registry {
	return &Registry{
		defs: make(map[string]registered),
	}
}

// Register adds one new tool definition.
func (r *Registry) Register(definition Definition) (Registration, error) {
	if err := validateDefinition(definition); err != nil {
		return Registration{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.defs == nil {
		r.defs = make(map[string]registered)
	}
	if _, exists := r.defs[definition.Name]; exists {
		return Registration{}, fmt.Errorf("%w: %s", ErrDuplicateRegistration, definition.Name)
	}
	r.next++
	registration := Registration{Name: definition.Name, Generation: r.next}
	r.defs[definition.Name] = registered{definition: definition.Clone(), generation: registration.Generation}
	return registration, nil
}

// Replace updates an existing definition if registration still names the active
// generation. This prevents stale plugin reloads from overwriting newer tools.
func (r *Registry) Replace(registration Registration, definition Definition) (Registration, error) {
	if err := validateDefinition(definition); err != nil {
		return Registration{}, err
	}
	if registration.Name != definition.Name {
		return Registration{}, fmt.Errorf("%w: registration %s cannot replace %s", ErrStaleRegistration, registration.Name, definition.Name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.defs[registration.Name]
	if !ok || current.generation != registration.Generation {
		return Registration{}, fmt.Errorf("%w: %s", ErrStaleRegistration, registration.Name)
	}
	r.next++
	next := Registration{Name: definition.Name, Generation: r.next}
	r.defs[definition.Name] = registered{definition: definition.Clone(), generation: next.Generation}
	return next, nil
}

// ResolveTools materializes enabled tools for a run snapshot.
func (r *Registry) ResolveTools(ctx context.Context, snapshot runtime.TurnSnapshot) ([]runtime.Tool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	enabled := enabledSet(snapshot)
	disabled := disabledSet(snapshot)
	definitions := make([]Definition, 0, len(r.defs))
	for _, entry := range r.defs {
		definition := entry.definition.Clone()
		if !includeTool(definition.Name, enabled, disabled) {
			continue
		}
		definitions = append(definitions, definition)
	}
	r.mu.RUnlock()
	result := make([]runtime.Tool, 0, len(definitions))
	for _, definition := range definitions {
		result = append(result, materialize(definition, snapshot.Clone()))
	}
	return result, nil
}

// Clone returns a defensive copy of definition containers.
func (d Definition) Clone() Definition {
	next := d
	next.Parameters = cloneParamsOneOf(d.Parameters)
	next.Permissions = cloneSlice(d.Permissions)
	next.Metadata = cloneStringMap(d.Metadata)
	return next
}

func validateDefinition(definition Definition) error {
	if strings.TrimSpace(definition.Name) == "" {
		return fmt.Errorf("%w: name required", ErrInvalidDefinition)
	}
	if definition.Decode == nil {
		return fmt.Errorf("%w: decoder required for %s", ErrInvalidDefinition, definition.Name)
	}
	if definition.Encode == nil {
		return fmt.Errorf("%w: encoder required for %s", ErrInvalidDefinition, definition.Name)
	}
	if definition.Execute == nil {
		return fmt.Errorf("%w: executor required for %s", ErrInvalidDefinition, definition.Name)
	}
	if _, err := cloneParamsOneOfChecked(definition.Parameters); err != nil {
		return fmt.Errorf("%w: parameters for %s: %v", ErrInvalidDefinition, definition.Name, err)
	}
	switch definition.Concurrency {
	case "", runtime.ToolConcurrencyParallel, runtime.ToolConcurrencySequential:
	default:
		return fmt.Errorf("%w: unsupported concurrency %q", ErrInvalidDefinition, definition.Concurrency)
	}
	return nil
}

func materialize(definition Definition, snapshot runtime.TurnSnapshot) runtime.Tool {
	scope := runtime.ToolScope{}
	if definition.Scope != nil {
		scope = definition.Scope(snapshot, definition.Clone())
	}
	if scope.WorkspaceID == "" {
		scope.WorkspaceID = snapshot.Config.Metadata["workspace_id"]
	}
	if scope.Root == "" {
		scope.Root = snapshot.Config.Metadata["workspace_root"]
	}
	if scope.ConcurrencyKey == "" {
		scope.ConcurrencyKey = string(snapshot.SessionID) + ":" + definition.Name
	}
	if len(scope.Permissions) == 0 {
		scope.Permissions = cloneSlice(definition.Permissions)
	}
	concurrency := definition.Concurrency
	if concurrency == "" {
		concurrency = runtime.ToolConcurrencyParallel
	}
	return runtime.Tool{
		Name: definition.Name,
		Info: &einoschema.ToolInfo{
			Name:        definition.Name,
			Desc:        definition.Description,
			Extra:       cloneAnyMap(definition.Metadata),
			ParamsOneOf: cloneParamsOneOf(definition.Parameters),
		},
		Executor:     toolExecutor{definition: definition.Clone(), snapshot: snapshot.Clone()},
		RetrySafe:    definition.RetrySafe,
		Scope:        cloneScope(scope),
		Concurrency:  concurrency,
		InputDecoder: toolDecoder{definition: definition.Clone()},
		Retention:    definition.Retention,
		Metadata:     cloneStringMap(definition.Metadata),
	}
}

type toolDecoder struct {
	definition Definition
}

func (d toolDecoder) DecodeToolInput(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	if !json.Valid(raw) {
		return nil, fmt.Errorf("%w: invalid json", ErrMalformedInput)
	}
	decoded, err := d.definition.Decode(ctx, raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedInput, err)
	}
	encoded, err := normalizeInput(ctx, d.definition, decoded)
	if err != nil {
		return nil, err
	}
	return cloneRaw(encoded), nil
}

type toolExecutor struct {
	definition Definition
	snapshot   runtime.TurnSnapshot
}

func (e toolExecutor) Execute(ctx context.Context, call runtime.ToolCall) (runtime.ToolResult, error) {
	decoded, err := e.definition.Decode(ctx, call.Input)
	if err != nil {
		return runtime.ToolResult{}, fmt.Errorf("%w: %v", ErrMalformedInput, err)
	}
	output, err := e.definition.Execute(ctx, Execution{
		Input:    decoded,
		Call:     call,
		Snapshot: e.snapshot.Clone(),
	})
	if err != nil {
		return runtime.ToolResult{}, err
	}
	encoded, err := e.definition.Encode(ctx, output)
	if err != nil {
		return runtime.ToolResult{}, err
	}
	return runtime.ToolResult{
		Output:     string(encoded),
		Structured: cloneRaw(encoded),
		Metadata:   cloneStringMap(e.definition.Metadata),
	}, nil
}

func enabledSet(snapshot runtime.TurnSnapshot) map[string]bool {
	if snapshot.Config.Tools.Enabled == nil {
		return nil
	}
	result := make(map[string]bool, len(snapshot.Config.Tools.Enabled))
	for _, name := range snapshot.Config.Tools.Enabled {
		result[name] = true
	}
	return result
}

func disabledSet(snapshot runtime.TurnSnapshot) map[string]bool {
	if len(snapshot.Config.Tools.Disabled) == 0 {
		return nil
	}
	result := make(map[string]bool, len(snapshot.Config.Tools.Disabled))
	for _, name := range snapshot.Config.Tools.Disabled {
		result[name] = true
	}
	return result
}

func includeTool(name string, enabled map[string]bool, disabled map[string]bool) bool {
	if disabled[name] {
		return false
	}
	if enabled == nil {
		return true
	}
	return enabled[name]
}

func normalizeInput(ctx context.Context, definition Definition, decoded any) (json.RawMessage, error) {
	if definition.Normalize != nil {
		encoded, err := definition.Normalize(ctx, decoded)
		if err != nil {
			return nil, err
		}
		return cloneRaw(encoded), nil
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func cloneParamsOneOf(src *einoschema.ParamsOneOf) *einoschema.ParamsOneOf {
	cloned, err := cloneParamsOneOfChecked(src)
	if err != nil {
		return src
	}
	return cloned
}

func cloneParamsOneOfChecked(src *einoschema.ParamsOneOf) (*einoschema.ParamsOneOf, error) {
	if src == nil {
		return nil, nil
	}
	schema, err := src.ToJSONSchema()
	if err != nil {
		return nil, err
	}
	if schema == nil {
		return nil, nil
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	var cloned jsonschema.Schema
	if err := json.Unmarshal(data, &cloned); err != nil {
		return nil, err
	}
	return einoschema.NewParamsOneOfByJSONSchema(&cloned), nil
}

func cloneScope(src runtime.ToolScope) runtime.ToolScope {
	next := src
	next.Permissions = cloneSlice(src.Permissions)
	return next
}

func cloneRaw(src json.RawMessage) json.RawMessage {
	if src == nil {
		return nil
	}
	dst := make(json.RawMessage, len(src))
	copy(dst, src)
	return dst
}

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneAnyMap(src map[string]string) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneSlice[T any](src []T) []T {
	if src == nil {
		return nil
	}
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}
