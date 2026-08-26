package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"

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
