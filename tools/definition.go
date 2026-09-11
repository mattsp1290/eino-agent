package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"

	"github.com/mattsp1290/eino-agent/internal/jsonobject"
	"github.com/mattsp1290/eino-agent/runtime"
)

// toolIdentifierPattern mirrors extension.ValidateIdentifier's stable
// identifier shape so tool and argument aliases are validated the same way
// as every other durable registration identity in this codebase.
var toolIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$`)

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

// RichExecutor executes one call and returns a multi-part enhanced result
// (text, media, and/or tool-search parts) instead of a single JSON payload.
// When a Definition sets both Execute and ExecuteRich, Materialize prefers
// ExecuteRich.
type RichExecutor func(ctx context.Context, execution Execution) (RichResult, error)

// RichResult is the enhanced-tool result returned by a Definition's
// ExecuteRich. Parts mirrors schema.ToolResult.Parts converted to the
// runtime's durable representation.
type RichResult struct {
	Parts []runtime.ToolResultPart
}

// ScopeResolver returns runtime authority from bounded, data-only scope input.
type ScopeResolver func(context.Context, runtime.ToolScopeContext) runtime.ToolScope

// Definition is a JSON-native tool declaration registered by host code or adapters.
type Definition struct {
	Name        string
	Description string
	Parameters  *einoschema.ParamsOneOf
	Normalize   InputNormalizer
	Pattern     PermissionPattern
	Execute     Executor
	// ExecuteRich, if set, is preferred over Execute by Materialize and
	// produces a multi-part enhanced result (see RichResult).
	ExecuteRich       RichExecutor
	RetrySafe         bool
	AllowSessionTitle bool
	Scope             ScopeResolver
	Retention         runtime.RetentionPolicy
	Permissions       []string
	Metadata          map[string]string
	// Aliases are additional model-visible names that resolve to this tool.
	// An alias must not equal Name or any other tool's name or alias within
	// the same run plan (rejected at plan compile time).
	Aliases []string
	// ArgumentAliases maps a canonical parameter name to the alternate
	// argument names a model may use for it. When a call supplies both the
	// canonical key and an alias key, the alias key is left untouched (an
	// unrecognized field, subject to normal schema validation) exactly like
	// Eino's compose.ToolAliasConfig.ArgumentsAliases.
	ArgumentAliases map[string][]string
	// Deferred marks the tool as advertised only through tool search
	// (model.RequestControls.DeferredTools) rather than eagerly bound to
	// every request.
	Deferred bool
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
	next.Aliases = cloneSlice(d.Aliases)
	next.ArgumentAliases = cloneArgumentAliases(d.ArgumentAliases)
	return next, nil
}

func cloneArgumentAliases(src map[string][]string) map[string][]string {
	if src == nil {
		return nil
	}
	dst := make(map[string][]string, len(src))
	for key, value := range src {
		dst[key] = cloneSlice(value)
	}
	return dst
}

// ValidateDefinition reports whether definition can be safely composed and
// materialized.
func ValidateDefinition(definition Definition) error {
	if strings.TrimSpace(definition.Name) == "" {
		return fmt.Errorf("%w: name required", ErrInvalidDefinition)
	}
	if definition.Execute == nil && definition.ExecuteRich == nil {
		return fmt.Errorf("%w: executor required for %s", ErrInvalidDefinition, definition.Name)
	}
	if err := validateParameters(definition.Parameters); err != nil {
		return fmt.Errorf("%w: parameters for %s: %v", ErrInvalidDefinition, definition.Name, err)
	}
	schemaKeys, err := parameterPropertyNames(definition.Parameters)
	if err != nil {
		return fmt.Errorf("%w: parameters for %s: %v", ErrInvalidDefinition, definition.Name, err)
	}
	if err := validateAliases(definition.Name, definition.Aliases, definition.ArgumentAliases, schemaKeys); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDefinition, err)
	}
	return nil
}

// parameterPropertyNames returns the set of top-level JSON schema property
// names declared by parameters, mirroring upstream Eino's
// compose.applyArgsAliases (eino@v0.9.19 compose/tool_node.go:434-450),
// which builds the same set to reject an argument alias that shadows a real
// schema property.
func parameterPropertyNames(parameters *einoschema.ParamsOneOf) (map[string]bool, error) {
	keys := make(map[string]bool)
	if parameters == nil {
		return keys, nil
	}
	schema, err := parameters.ToJSONSchema()
	if err != nil {
		return nil, err
	}
	if schema == nil || schema.Properties == nil {
		return keys, nil
	}
	for pair := schema.Properties.Oldest(); pair != nil; pair = pair.Next() {
		keys[pair.Key] = true
	}
	return keys, nil
}

// validateAliases enforces that tool name aliases and argument aliases are
// valid identifiers, distinct from the canonical name/key they alias, and
// distinct from each other. It additionally mirrors upstream Eino's
// compose.applyArgsAliases (eino@v0.9.19 compose/tool_node.go:434-486):
// an argument alias must not collide with a declared schema property (which
// would silently steal that property's value, see remapToolArguments), and
// a canonical argument key must not contain "." (nested field matching is
// not supported).
func validateAliases(name string, aliases []string, argumentAliases map[string][]string, schemaKeys map[string]bool) error {
	seen := make(map[string]bool, len(aliases))
	for _, alias := range aliases {
		if !toolIdentifierPattern.MatchString(alias) {
			return fmt.Errorf("invalid tool alias %q", alias)
		}
		if alias == name {
			return fmt.Errorf("tool alias %q collides with tool name", alias)
		}
		if seen[alias] {
			return fmt.Errorf("duplicate tool alias %q", alias)
		}
		seen[alias] = true
	}
	seenArgumentAlias := make(map[string]bool, len(argumentAliases))
	for canonical, aliasesForKey := range argumentAliases {
		if strings.TrimSpace(canonical) == "" {
			return errors.New("argument alias canonical key required")
		}
		if strings.Contains(canonical, ".") {
			return fmt.Errorf("unsupported '.' in canonical argument key %q: nested field matching is not supported", canonical)
		}
		if len(aliasesForKey) == 0 {
			return fmt.Errorf("argument alias list for %q is empty", canonical)
		}
		localSeen := make(map[string]bool, len(aliasesForKey))
		for _, alias := range aliasesForKey {
			if strings.TrimSpace(alias) == "" {
				return fmt.Errorf("empty argument alias for %q", canonical)
			}
			if alias == canonical {
				return fmt.Errorf("argument alias %q equals its canonical key %q", alias, canonical)
			}
			if schemaKeys[alias] {
				return fmt.Errorf("argument alias %q conflicts with schema property %q", alias, alias)
			}
			if _, isCanonical := argumentAliases[alias]; isCanonical {
				return fmt.Errorf("argument alias %q conflicts with another canonical argument key", alias)
			}
			if localSeen[alias] {
				return fmt.Errorf("duplicate argument alias %q for %q", alias, canonical)
			}
			localSeen[alias] = true
			if seenArgumentAlias[alias] {
				return fmt.Errorf("argument alias %q reused across canonical keys", alias)
			}
			seenArgumentAlias[alias] = true
		}
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
	return materialize(ctx, frozen, context.Clone())
}

func materialize(ctx context.Context, definition Definition, context runtime.ToolScopeContext) (runtime.Tool, error) {
	scope := runtime.ToolScope{}
	if definition.Scope != nil {
		scope = definition.Scope(ctx, context.Clone())
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
		Executor:          &toolExecutor{definition: executorDefinition, scope: context.Clone()},
		RetrySafe:         definition.RetrySafe,
		AllowSessionTitle: definition.AllowSessionTitle,
		Scope:             cloneScope(scope),
		InputDecoder:      &toolDecoder{definition: decoderDefinition},
		Pattern:           &toolPatternResolver{definition: decoderDefinition},
		Retention:         definition.Retention,
		Metadata:          cloneStringMap(definition.Metadata),
		Aliases:           cloneSlice(definition.Aliases),
		ArgumentAliases:   cloneArgumentAliases(definition.ArgumentAliases),
		Deferred:          definition.Deferred,
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
	execution := Execution{
		Input:   cloneRaw(call.Input),
		Call:    call,
		Context: executionContext,
	}
	if e.definition.ExecuteRich != nil {
		rich, err := e.definition.ExecuteRich(ctx, execution)
		if err != nil {
			return runtime.ToolResult{}, err
		}
		return runtime.ToolResult{
			Parts:    cloneToolResultParts(rich.Parts),
			Metadata: cloneStringMap(e.definition.Metadata),
		}, nil
	}
	output, err := e.definition.Execute(ctx, execution)
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

// cloneToolResultParts returns a defensive deep copy of parts.
func cloneToolResultParts(parts []runtime.ToolResultPart) []runtime.ToolResultPart {
	if parts == nil {
		return nil
	}
	cloned := make([]runtime.ToolResultPart, len(parts))
	for index, part := range parts {
		next := part
		if part.Media != nil {
			media := *part.Media
			next.Media = &media
		}
		next.ToolSearch = cloneRaw(part.ToolSearch)
		cloned[index] = next
	}
	return cloned
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
