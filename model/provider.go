package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"
)

var (
	// ErrProviderUnavailable reports that a provider adapter cannot build a
	// request client in the current runtime environment.
	ErrProviderUnavailable = errors.New("model provider unavailable")
	// ErrProviderRateLimited reports a retryable provider throttle.
	ErrProviderRateLimited = errors.New("model provider rate limited")
	// ErrProviderRejected reports a non-retryable provider request rejection.
	ErrProviderRejected = errors.New("model provider rejected request")
	// ErrInvalidResolution reports an incomplete or inconsistent resolved model.
	ErrInvalidResolution = errors.New("invalid resolved model")
)

// StreamDelta is one normalized chunk from a provider stream. Usage is the
// cumulative attempt-to-date provider usage at this point in the stream.
type StreamDelta struct {
	Message *einoschema.Message
	Usage   Usage
}

// Request is the transport-neutral provider request shape.
type Request struct {
	Identity Identity
	Messages []*einoschema.Message
	System   string
	Tools    []*einoschema.ToolInfo
	Options  map[string]string
	// IdempotencyKey is assigned by a ledger-enabled runtime. It is not part of
	// the model-visible audited projection. Adapters whose provider transport
	// accepts an idempotency key may read it directly.
	IdempotencyKey string
}

// Identity is provider-visible request identity. It intentionally avoids
// importing runtime or session packages so model adapters remain independent.
type Identity struct {
	SessionID          string
	RunID              string
	AgentID            string
	AssistantMessageID string
	ToolCallID         string
	ProviderID         ProviderID
	ModelID            ID
	TraceID            string
	SpanID             string
	ParentSpanID       string
	TraceAttributes    map[string]string
}

// Clone returns a defensive copy of mutable identity fields.
func (i Identity) Clone() Identity {
	next := i
	next.TraceAttributes = cloneMap(i.TraceAttributes)
	return next
}

// Clone returns a defensive copy of the complete mutable request graph.
func (r Request) Clone() (Request, error) {
	next := r
	next.Identity = r.Identity.Clone()
	var err error
	next.Messages, err = cloneMessages(r.Messages)
	if err != nil {
		return Request{}, err
	}
	next.Tools, err = cloneToolInfos(r.Tools)
	if err != nil {
		return Request{}, err
	}
	next.Options = cloneMap(r.Options)
	return next, nil
}

// Usage records provider token and cost data in model-layer terms.
type Usage struct {
	InputTokens      int64
	OutputTokens     int64
	ReasoningTokens  int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	Cost             float64
}

// Error is a normalized provider error. Cause preserves the underlying error
// for logging and errors.Is checks without making runtime parse SDK errors.
type Error struct {
	Code      string
	Message   string
	Retryable bool
	Cause     error
}

func (e Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Code != "" {
		return e.Code
	}
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return "model provider error"
}

// Unwrap returns the underlying provider error.
func (e Error) Unwrap() error {
	return e.Cause
}

// Adapter builds one immutable provider streamer for the selected model.
type Adapter interface {
	Provider() Provider
	Models(ctx context.Context) ([]Descriptor, error)
	Build(ctx context.Context, selection Selection, runtime Runtime) (Streamer, error)
}

// OptionalAdapter describes adapter packages that may be registered by hosts or
// build-tagged packages without adding mandatory dependencies to the core model
// package.
type OptionalAdapter interface {
	Adapter
	Available(ctx context.Context, runtime Runtime) error
}

// Streamer is the sole provider execution surface.
type Streamer interface {
	StreamProvider(ctx context.Context, request Request) (*einoschema.StreamReader[StreamDelta], error)
}

// AdapterResolver resolves model selections through registered adapters.
type AdapterResolver struct {
	Adapters []Adapter
	Catalog  Catalog
}

// ResolverFunc adapts a function into a Resolver.
type ResolverFunc func(context.Context, Selection, Runtime) (Resolved, error)

// Resolve calls fn.
func (fn ResolverFunc) Resolve(ctx context.Context, selection Selection, runtime Runtime) (Resolved, error) {
	return fn(ctx, selection, runtime)
}

var _ Resolver = ResolverFunc(nil)

// ValidateResolved verifies that one resolution completely and consistently
// satisfies the requested provider/model selection.
func ValidateResolved(selection Selection, resolved Resolved) error {
	switch {
	case resolved.Provider.ID == "":
		return fmt.Errorf("%w: provider id required", ErrInvalidResolution)
	case resolved.Model.ID == "":
		return fmt.Errorf("%w: model id required", ErrInvalidResolution)
	case resolved.Model.ProviderID == "":
		return fmt.Errorf("%w: model provider id required", ErrInvalidResolution)
	case resolved.Model.ProviderID != resolved.Provider.ID:
		return fmt.Errorf("%w: model provider %q does not match provider %q", ErrInvalidResolution, resolved.Model.ProviderID, resolved.Provider.ID)
	case selection.ProviderID != "" && resolved.Provider.ID != selection.ProviderID:
		return fmt.Errorf("%w: provider %q does not match selection %q", ErrInvalidResolution, resolved.Provider.ID, selection.ProviderID)
	case selection.ModelID != "" && resolved.Model.ID != selection.ModelID:
		return fmt.Errorf("%w: model %q does not match selection %q", ErrInvalidResolution, resolved.Model.ID, selection.ModelID)
	case resolved.Streamer == nil:
		return fmt.Errorf("%w: streamer required", ErrInvalidResolution)
	default:
		return nil
	}
}

// Resolve builds an immutable model client for the selected provider/model.
func (r AdapterResolver) Resolve(ctx context.Context, selection Selection, runtime Runtime) (Resolved, error) {
	adapter, provider, err := r.adapterFor(ctx, selection.ProviderID, runtime)
	if err != nil {
		return Resolved{}, err
	}
	descriptor, err := r.descriptorFor(ctx, adapter, selection)
	if err != nil {
		return Resolved{}, err
	}
	streamer, err := adapter.Build(ctx, selection, cloneRuntime(runtime))
	if err != nil {
		return Resolved{}, err
	}
	if streamer == nil {
		return Resolved{}, Error{Code: "provider_streamer_missing", Message: "provider adapter returned nil streamer", Cause: ErrProviderUnavailable}
	}
	resolved := Resolved{
		Provider: provider,
		Model:    descriptor,
		Streamer: streamer,
	}
	if err := ValidateResolved(selection, resolved); err != nil {
		return Resolved{}, err
	}
	return resolved, nil
}

func (r AdapterResolver) adapterFor(ctx context.Context, providerID ProviderID, runtime Runtime) (Adapter, Provider, error) {
	for _, adapter := range r.Adapters {
		if adapter == nil {
			continue
		}
		provider := adapter.Provider()
		if provider.ID != providerID {
			continue
		}
		if optional, ok := adapter.(OptionalAdapter); ok {
			if err := optional.Available(ctx, cloneRuntime(runtime)); err != nil {
				return nil, Provider{}, err
			}
		}
		provider.Environment = cloneSlice(provider.Environment)
		provider.Options = cloneMap(provider.Options)
		return adapter, provider, nil
	}
	return nil, Provider{}, Error{
		Code:    "provider_not_found",
		Message: "provider adapter not registered",
		Cause:   ErrProviderUnavailable,
	}
}

func (r AdapterResolver) descriptorFor(ctx context.Context, adapter Adapter, selection Selection) (Descriptor, error) {
	if r.Catalog != nil {
		descriptor, err := r.Catalog.GetModel(ctx, selection.ProviderID, selection.ModelID)
		if err != nil {
			return Descriptor{}, err
		}
		return cloneDescriptor(descriptor), nil
	}
	models, err := adapter.Models(ctx)
	if err != nil {
		return Descriptor{}, err
	}
	for _, descriptor := range models {
		if descriptor.ProviderID == selection.ProviderID && descriptor.ID == selection.ModelID {
			return cloneDescriptor(descriptor), nil
		}
	}
	return Descriptor{}, Error{
		Code:    "model_not_found",
		Message: "model descriptor not found",
		Cause:   ErrProviderRejected,
	}
}

func cloneRuntime(src Runtime) Runtime {
	next := src
	next.Env = cloneMap(src.Env)
	next.Auth = cloneMap(src.Auth)
	next.Options = cloneMap(src.Options)
	return next
}

func cloneDescriptor(src Descriptor) Descriptor {
	next := src
	next.Capabilities = cloneBoolMap(src.Capabilities)
	next.Options = cloneMap(src.Options)
	return next
}

func cloneMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneBoolMap(src map[string]bool) map[string]bool {
	if src == nil {
		return nil
	}
	dst := make(map[string]bool, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneMessages(src []*einoschema.Message) ([]*einoschema.Message, error) {
	if src == nil {
		return nil, nil
	}
	dst := make([]*einoschema.Message, len(src))
	for index, message := range src {
		if message == nil {
			continue
		}
		//nolint:staticcheck // The canonical boundary rejects the deprecated field.
		if len(message.MultiContent) != 0 {
			return nil, fmt.Errorf("clone message %d: deprecated MultiContent is unsupported", index)
		}
		for _, part := range message.AssistantGenMultiContent {
			if part.StreamingMeta != nil {
				return nil, fmt.Errorf("message %d contains non-copyable streaming metadata", index)
			}
		}
		raw, err := json.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("clone message %d: %w", index, err)
		}
		if err := rejectExtraJSON(raw); err != nil {
			return nil, fmt.Errorf("clone message %d: %w", index, err)
		}
		var cloned einoschema.Message
		if err := json.Unmarshal(raw, &cloned); err != nil {
			return nil, fmt.Errorf("clone message %d: %w", index, err)
		}
		dst[index] = &cloned
	}
	return dst, nil
}

func cloneToolInfos(src []*einoschema.ToolInfo) ([]*einoschema.ToolInfo, error) {
	if src == nil {
		return nil, nil
	}
	dst := make([]*einoschema.ToolInfo, len(src))
	for index, tool := range src {
		if tool == nil {
			continue
		}
		if len(tool.Extra) != 0 {
			return nil, fmt.Errorf("tool %d contains unsupported extra metadata", index)
		}
		next := *tool
		var err error
		next.ParamsOneOf, err = cloneParamsOneOf(tool.ParamsOneOf)
		if err != nil {
			return nil, fmt.Errorf("clone tool %d schema: %w", index, err)
		}
		dst[index] = &next
	}
	return dst, nil
}

func cloneParamsOneOf(src *einoschema.ParamsOneOf) (*einoschema.ParamsOneOf, error) {
	if src == nil {
		return nil, nil
	}
	schema, err := src.ToJSONSchema()
	if err != nil || schema == nil {
		if err == nil {
			err = errors.New("nil JSON schema")
		}
		return nil, err
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

func rejectExtraJSON(raw json.RawMessage) error {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	var visit func(any) error
	visit = func(current any) error {
		switch typed := current.(type) {
		case map[string]any:
			for key, child := range typed {
				if key == "extra" {
					return errors.New("unsupported extra metadata")
				}
				if err := visit(child); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range typed {
				if err := visit(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return visit(value)
}

func cloneSlice[T any](src []T) []T {
	if src == nil {
		return nil
	}
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}
