package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"

	"github.com/mattsp1290/eino-agent/internal/jsonobject"
	"github.com/mattsp1290/eino-agent/runtime"
)

var (
	// ErrInvalidDefinition reports an incomplete or inconsistent tool definition.
	ErrInvalidDefinition = errors.New("invalid tool definition")
	// ErrDuplicateRegistration reports a second registration for the same tool name.
	ErrDuplicateRegistration = errors.New("duplicate tool registration")
	// ErrMalformedInput reports model tool input that cannot be decoded.
	ErrMalformedInput = errors.New("malformed tool input")
)

// InputNormalizer converts model input into canonical JSON for durable storage
// and execution. If omitted, the canonical input is used unchanged.
type InputNormalizer func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

// PermissionPattern derives permission identity from canonical input.
type PermissionPattern func(ctx context.Context, input json.RawMessage) (string, error)

// Executor executes one JSON-native tool invocation.
type Executor func(ctx context.Context, execution Execution) (json.RawMessage, error)

// ScopeResolver returns runtime authority from bounded, data-only scope input.
type ScopeResolver func(runtime.ToolScopeContext) runtime.ToolScope

// Definition is a JSON-native tool declaration registered by host code or adapters.
type Definition struct {
	Name        string
	Description string
	Parameters  *einoschema.ParamsOneOf
	Normalize   InputNormalizer
	Pattern     PermissionPattern
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

// Execution is canonical JSON input and durable runtime context for one call.
type Execution struct {
	Input   json.RawMessage
	Call    runtime.ToolCall
	Context runtime.ToolContext
}

// TypedExecution presents canonical input as a host type while preserving the
// durable call context.
type TypedExecution[I any] struct {
	Input   I
	Call    runtime.ToolCall
	Context runtime.ToolContext
}

// TypedNormalizer adapts a typed input normalizer to the JSON-native boundary.
func TypedNormalizer[I any](normalize func(context.Context, I) (I, error)) InputNormalizer {
	return func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		input, err := decodeTyped[I](raw)
		if err != nil {
			return nil, err
		}
		input, err = normalize(ctx, input)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(input)
		if err != nil {
			return nil, fmt.Errorf("encode normalized tool input: %w", err)
		}
		return encoded, nil
	}
}

// TypedPermissionPattern adapts typed permission identity to canonical JSON.
func TypedPermissionPattern[I any](pattern func(context.Context, I) (string, error)) PermissionPattern {
	return func(ctx context.Context, raw json.RawMessage) (string, error) {
		input, err := decodeTyped[I](raw)
		if err != nil {
			return "", err
		}
		return pattern(ctx, input)
	}
}

// TypedExecutor adapts typed input and output to the JSON-native boundary.
func TypedExecutor[I, O any](execute func(context.Context, TypedExecution[I]) (O, error)) Executor {
	return func(ctx context.Context, execution Execution) (json.RawMessage, error) {
		input, err := decodeTyped[I](execution.Input)
		if err != nil {
			return nil, err
		}
		output, err := execute(ctx, TypedExecution[I]{Input: input, Call: execution.Call, Context: execution.Context})
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(output)
		if err != nil {
			return nil, fmt.Errorf("encode tool result: %w", err)
		}
		return encoded, nil
	}
}

func decodeTyped[T any](raw json.RawMessage) (T, error) {
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		return value, fmt.Errorf("%w: %v", ErrMalformedInput, err)
	}
	return value, nil
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

// ValidateDefinition reports whether definition can be safely composed and
// materialized.
func ValidateDefinition(definition Definition) error {
	if strings.TrimSpace(definition.Name) == "" {
		return fmt.Errorf("%w: name required", ErrInvalidDefinition)
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

// Materialize creates one runtime tool from a validated definition and bounded scope.
func Materialize(ctx context.Context, definition Definition, context runtime.ToolScopeContext) (runtime.Tool, error) {
	if err := ctx.Err(); err != nil {
		return runtime.Tool{}, err
	}
	if err := ValidateDefinition(definition); err != nil {
		return runtime.Tool{}, err
	}
	frozen, err := definition.Clone()
	if err != nil {
		return runtime.Tool{}, fmt.Errorf("freeze tool %q: %w", definition.Name, err)
	}
	return materialize(frozen, context.Clone())
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
	return r.definition.Pattern(ctx, cloneRaw(raw))
}

type toolDecoder struct {
	definition Definition
}

func (d toolDecoder) DecodeToolInput(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	canonical, err := canonicalInput(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedInput, err)
	}
	encoded, err := normalizeInput(ctx, d.definition, canonical)
	if err != nil {
		return nil, err
	}
	canonical, err = canonicalInput(encoded)
	if err != nil {
		return nil, fmt.Errorf("%w: normalized input: %v", ErrMalformedInput, err)
	}
	return canonical, nil
}

type toolExecutor struct {
	definition Definition
	scope      runtime.ToolScopeContext
}

func (e toolExecutor) Execute(ctx context.Context, call runtime.ToolCall) (runtime.ToolResult, error) {
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
		Input:   cloneRaw(call.Input),
		Call:    call,
		Context: executionContext,
	})
	if err != nil {
		return runtime.ToolResult{}, err
	}
	if !json.Valid(output) {
		return runtime.ToolResult{}, fmt.Errorf("%w: executor returned invalid JSON", ErrInvalidDefinition)
	}
	return runtime.ToolResult{
		Output:     string(output),
		Structured: cloneRaw(output),
		Metadata:   cloneStringMap(e.definition.Metadata),
	}, nil
}

func normalizeInput(ctx context.Context, definition Definition, raw json.RawMessage) (json.RawMessage, error) {
	if definition.Normalize != nil {
		encoded, err := definition.Normalize(ctx, cloneRaw(raw))
		if err != nil {
			return nil, err
		}
		return cloneRaw(encoded), nil
	}
	return cloneRaw(raw), nil
}

func canonicalInput(raw json.RawMessage) (json.RawMessage, error) {
	object, err := jsonobject.Decode(raw)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(object)
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
