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
)

// StreamDelta is one normalized chunk from a provider stream.
type StreamDelta struct {
	Message *einoschema.Message
	Usage   Usage
	Index   int64
	Done    bool
}

// StreamObserver receives normalized provider stream callbacks. Runtime
// adapters can use these callbacks for usage propagation and lifecycle events
// without parsing provider-specific payloads.
type StreamObserver interface {
	OnProviderStart(ctx context.Context, request Request)
	OnProviderDelta(ctx context.Context, delta StreamDelta)
	OnProviderError(ctx context.Context, err Error)
	OnProviderEnd(ctx context.Context, response Response)
}

// Request is the transport-neutral provider request shape.
type Request struct {
	Identity Identity
	Messages []*einoschema.Message
	System   string
	Tools    []*einoschema.ToolInfo
	Options  map[string]string
	Observer StreamObserver
	// IdempotencyKey is assigned by a ledger-enabled runtime. It is not part of
	// the model-visible audited projection and is used only by adapters that
	// explicitly implement IdempotentStreamer.
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

// Response is the normalized terminal provider response.
type Response struct {
	Message *einoschema.Message
	Usage   Usage
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
	StreamProvider(ctx context.Context, request Request) (*einoschema.StreamReader[*einoschema.Message], error)
}

// IdempotentStreamer is an optional adapter capability. The key identifies the
// durable prepared request record; adapters decide how (or whether) their
// provider transport can honor it and must not imply exactly-once delivery.
type IdempotentStreamer interface {
	Streamer
	StreamProviderWithIdempotencyKey(ctx context.Context, request Request, key string) (*einoschema.StreamReader[*einoschema.Message], error)
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
	return Resolved{
		Provider: provider,
		Model:    descriptor,
		Streamer: streamer,
	}, nil
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
