package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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

// PermissionPattern derives permission identity from decoded canonical input.
type PermissionPattern func(ctx context.Context, input any) (string, error)

// Executor executes one typed tool invocation.
type Executor func(ctx context.Context, execution Execution) (any, error)

// ScopeResolver returns runtime authority from bounded, data-only scope input.
type ScopeResolver func(runtime.ToolScopeContext) runtime.ToolScope

// Definition is a typed tool declaration registered by host code or adapters.
type Definition struct {
	Name        string
	Description string
	Parameters  *einoschema.ParamsOneOf
	Decode      Decoder
	Normalize   InputNormalizer
	Pattern     PermissionPattern
	Encode      Encoder
	Execute     Executor
	RetrySafe   bool
	Scope       ScopeResolver
	Retention   runtime.RetentionPolicy
	Permissions []string
	Metadata    map[string]string
	// Provenance is the restart-stable identity of the component and executor
	// supplying this definition. It is copied into frozen run plans.
	Provenance Provenance
}

// Provenance identifies the executable artifact behind a tool definition.
type Provenance struct {
	InstanceID      string
	ArtifactName    string
	ArtifactVersion string
	ArtifactHash    string
	ConfigHash      string
	ExecutorHash    string
}

// Execution is the decoded input and durable runtime context for one call.
type Execution struct {
	Input   any
	Call    runtime.ToolCall
	Context runtime.ToolContext
}

// Registration identifies one active tool registration generation.
type Registration struct {
	Name       string
	Generation uint64
}

// Registry stores typed tool definitions and materializes them per bounded
// scope context.
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
	if err := ValidateDefinition(definition); err != nil {
		return Registration{}, err
	}
	frozen, err := definition.Clone()
	if err != nil {
		return Registration{}, fmt.Errorf("%w: freeze %s: %v", ErrInvalidDefinition, definition.Name, err)
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
	r.defs[definition.Name] = registered{definition: frozen, generation: registration.Generation}
	return registration, nil
}

// Replace updates an existing definition if registration still names the active
// generation. This prevents stale plugin reloads from overwriting newer tools.
func (r *Registry) Replace(registration Registration, definition Definition) (Registration, error) {
	if err := ValidateDefinition(definition); err != nil {
		return Registration{}, err
	}
	if registration.Name != definition.Name {
		return Registration{}, fmt.Errorf("%w: registration %s cannot replace %s", ErrStaleRegistration, registration.Name, definition.Name)
	}
	frozen, err := definition.Clone()
	if err != nil {
		return Registration{}, fmt.Errorf("%w: freeze %s: %v", ErrInvalidDefinition, definition.Name, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.defs[registration.Name]
	if !ok || current.generation != registration.Generation {
		return Registration{}, fmt.Errorf("%w: %s", ErrStaleRegistration, registration.Name)
	}
	r.next++
	next := Registration{Name: definition.Name, Generation: r.next}
	r.defs[definition.Name] = registered{definition: frozen, generation: next.Generation}
	return next, nil
}

// Unregister removes only the exact active registration generation.
func (r *Registry) Unregister(registration Registration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.defs[registration.Name]
	if !ok || current.generation != registration.Generation {
		return fmt.Errorf("%w: %s", ErrStaleRegistration, registration.Name)
	}
	delete(r.defs, registration.Name)
	return nil
}

// SnapshotEntry is one exact generation in a deterministic registry snapshot.
type SnapshotEntry struct {
	Registration Registration
	Definition   Definition
}

// Snapshot is an immutable set of exact tool generations. Materialization
// applies run enable/disable selection without consulting the live registry.
type Snapshot struct{ entries []SnapshotEntry }

// Snapshot returns every active definition ordered by registration generation.
func (r *Registry) Snapshot() (Snapshot, error) {
	if r == nil {
		return Snapshot{}, nil
	}
	r.mu.RLock()
	entries := make([]SnapshotEntry, 0, len(r.defs))
	for name, entry := range r.defs {
		definition, err := entry.definition.Clone()
		if err != nil {
			r.mu.RUnlock()
			return Snapshot{}, fmt.Errorf("freeze snapshot tool %q: %w", name, err)
		}
		entries = append(entries, SnapshotEntry{Registration: Registration{Name: name, Generation: entry.generation}, Definition: definition})
	}
	r.mu.RUnlock()
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Registration.Generation != entries[j].Registration.Generation {
			return entries[i].Registration.Generation < entries[j].Registration.Generation
		}
		return entries[i].Registration.Name < entries[j].Registration.Name
	})
	return Snapshot{entries: entries}, nil
}

// Entries returns a defensive copy of the frozen generations.
func (s Snapshot) Entries() ([]SnapshotEntry, error) {
	result := make([]SnapshotEntry, len(s.entries))
	for index, entry := range s.entries {
		definition, err := entry.Definition.Clone()
		if err != nil {
			return nil, fmt.Errorf("clone snapshot tool %q: %w", entry.Registration.Name, err)
		}
		result[index] = SnapshotEntry{Registration: entry.Registration, Definition: definition}
	}
	return result, nil
}

// NewSnapshot builds a frozen snapshot from entries supplied by a composition
// coordinator. Entries are sorted by generation and name.
func NewSnapshot(entries []SnapshotEntry) (Snapshot, error) {
	next := make([]SnapshotEntry, len(entries))
	for index, entry := range entries {
		definition, err := entry.Definition.Clone()
		if err != nil {
			return Snapshot{}, fmt.Errorf("freeze snapshot tool %q: %w", entry.Registration.Name, err)
		}
		next[index] = SnapshotEntry{Registration: entry.Registration, Definition: definition}
	}
	sort.Slice(next, func(i, j int) bool {
		if next[i].Registration.Generation != next[j].Registration.Generation {
			return next[i].Registration.Generation < next[j].Registration.Generation
		}
		return next[i].Registration.Name < next[j].Registration.Name
	})
	return Snapshot{entries: next}, nil
}

// ResolveTools materializes from the immutable snapshot.
func (s Snapshot) ResolveTools(ctx context.Context, scope runtime.ToolScopeContext) ([]runtime.Tool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	enabled := enabledSet(scope)
	disabled := disabledSet(scope)
	result := make([]runtime.Tool, 0, len(s.entries))
	for _, entry := range s.entries {
		if includeTool(entry.Definition.Name, enabled, disabled) {
			definition, err := entry.Definition.Clone()
			if err != nil {
				return nil, fmt.Errorf("clone snapshot tool %q: %w", entry.Definition.Name, err)
			}
			tool, err := materialize(definition, scope.Clone())
			if err != nil {
				return nil, fmt.Errorf("materialize snapshot tool %q: %w", entry.Definition.Name, err)
			}
			result = append(result, tool)
		}
	}
	return result, nil
}

// ResolveTools materializes enabled tools for a bounded scope context.
func (r *Registry) ResolveTools(ctx context.Context, scope runtime.ToolScopeContext) ([]runtime.Tool, error) {
	snapshot, err := r.Snapshot()
	if err != nil {
		return nil, err
	}
	return snapshot.ResolveTools(ctx, scope)
}

// Clone returns a defensive copy of definition containers.
func (d Definition) Clone() (Definition, error) {
	next := d
	parameters, err := cloneParamsOneOfChecked(d.Parameters)
	if err != nil {
		return Definition{}, err
	}
	next.Parameters = parameters
	next.Permissions = cloneSlice(d.Permissions)
	next.Metadata = cloneStringMap(d.Metadata)
	return next, nil
}

// ValidateDefinition reports whether definition can be safely registered and
// materialized by a tool registry.
func ValidateDefinition(definition Definition) error {
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
	if err := validateParameters(definition.Parameters); err != nil {
		return fmt.Errorf("%w: parameters for %s: %v", ErrInvalidDefinition, definition.Name, err)
	}
	return nil
}

func validateParameters(parameters *einoschema.ParamsOneOf) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("invalid parameter schema: %v", recovered)
		}
	}()
	_, err = cloneParamsOneOfChecked(parameters)
	return err
}

func materialize(definition Definition, context runtime.ToolScopeContext) (runtime.Tool, error) {
	scope := runtime.ToolScope{}
	if definition.Scope != nil {
		scope = definition.Scope(context.Clone())
	}
	if scope.WorkspaceID == "" {
		scope.WorkspaceID = context.WorkspaceID
	}
	if scope.Root == "" {
		scope.Root = context.WorkspaceRoot
	}
	if len(scope.Permissions) == 0 {
		scope.Permissions = cloneSlice(definition.Permissions)
	}
	executorDefinition, err := definition.Clone()
	if err != nil {
		return runtime.Tool{}, err
	}
	decoderDefinition, err := definition.Clone()
	if err != nil {
		return runtime.Tool{}, err
	}
	parameters, err := cloneParamsOneOfChecked(definition.Parameters)
	if err != nil {
		return runtime.Tool{}, err
	}
	return runtime.Tool{
		Name: definition.Name,
		Info: &einoschema.ToolInfo{
			Name:        definition.Name,
			Desc:        definition.Description,
			ParamsOneOf: parameters,
		},
		Executor:     &toolExecutor{definition: executorDefinition, scope: context.Clone()},
		RetrySafe:    definition.RetrySafe,
		Scope:        cloneScope(scope),
		InputDecoder: &toolDecoder{definition: decoderDefinition},
		Pattern:      &toolPatternResolver{definition: decoderDefinition},
		Retention:    definition.Retention,
		Metadata:     cloneStringMap(definition.Metadata),
	}, nil
}

type toolPatternResolver struct{ definition Definition }

func (r toolPatternResolver) ResolvePermissionPattern(ctx context.Context, raw json.RawMessage) (string, error) {
	if r.definition.Pattern == nil {
		return r.definition.Name, nil
	}
	decoded, err := r.definition.Decode(ctx, raw)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrMalformedInput, err)
	}
	return r.definition.Pattern(ctx, decoded)
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
	scope      runtime.ToolScopeContext
}

func (e toolExecutor) Execute(ctx context.Context, call runtime.ToolCall) (runtime.ToolResult, error) {
	decoded, err := e.definition.Decode(ctx, call.Input)
	if err != nil {
		return runtime.ToolResult{}, fmt.Errorf("%w: %v", ErrMalformedInput, err)
	}
	executionContext := call.Context.Clone()
	if executionContext.Turn.SessionID == "" {
		executionContext.Turn.SessionID = e.scope.SessionID
	}
	if executionContext.WorkspaceID == "" {
		executionContext.WorkspaceID = e.scope.WorkspaceID
	}
	if executionContext.WorkspaceRoot == "" {
		executionContext.WorkspaceRoot = e.scope.WorkspaceRoot
	}
	output, err := e.definition.Execute(ctx, Execution{
		Input:   decoded,
		Call:    call,
		Context: executionContext,
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

func enabledSet(scope runtime.ToolScopeContext) map[string]bool {
	if scope.EnabledTools == nil {
		return nil
	}
	result := make(map[string]bool, len(scope.EnabledTools))
	for _, name := range scope.EnabledTools {
		result[name] = true
	}
	return result
}

func disabledSet(scope runtime.ToolScopeContext) map[string]bool {
	if len(scope.DisabledTools) == 0 {
		return nil
	}
	result := make(map[string]bool, len(scope.DisabledTools))
	for _, name := range scope.DisabledTools {
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

func cloneSlice[T any](src []T) []T {
	if src == nil {
		return nil
	}
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}
