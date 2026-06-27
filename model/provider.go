package model

import (
	"context"
	"errors"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"
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
	Tools    []*einoschema.ToolInfo
	Options  map[string]string
	Observer StreamObserver
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

// Clone returns a defensive copy of request containers. Message and tool info
// pointers remain provider-owned values and must not be mutated by adapters.
func (r Request) Clone() Request {
	next := r
	next.Identity = r.Identity.Clone()
	next.Messages = cloneMessages(r.Messages)
	next.Tools = cloneToolInfos(r.Tools)
	next.Options = cloneMap(r.Options)
	return next
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

// Adapter builds immutable Eino chat models for provider requests.
type Adapter interface {
	Provider() Provider
	Models(ctx context.Context) ([]Descriptor, error)
	Build(ctx context.Context, selection Selection, runtime Runtime) (einomodel.ToolCallingChatModel, error)
}

// OptionalAdapter describes adapter packages that may be registered by hosts or
// build-tagged packages without adding mandatory dependencies to the core model
// package.
type OptionalAdapter interface {
	Adapter
	Available(ctx context.Context, runtime Runtime) error
}

// Streamer is an optional adapter capability for callers that want normalized
// provider callbacks in addition to the raw Eino stream.
type Streamer interface {
	StreamProvider(ctx context.Context, request Request) (*einoschema.StreamReader[*einoschema.Message], error)
}

// AdapterResolver resolves model selections through registered adapters.
type AdapterResolver struct {
	Adapters []Adapter
	Catalog  Catalog
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
	client, err := adapter.Build(ctx, selection, cloneRuntime(runtime))
	if err != nil {
		return Resolved{}, err
	}
	return Resolved{
		Provider: provider,
		Model:    descriptor,
		Client:   client,
		Streamer: streamerFor(adapter),
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

func streamerFor(adapter Adapter) Streamer {
	streamer, ok := adapter.(Streamer)
	if !ok {
		return nil
	}
	return streamer
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

func cloneMessages(src []*einoschema.Message) []*einoschema.Message {
	if src == nil {
		return nil
	}
	dst := make([]*einoschema.Message, len(src))
	for i, message := range src {
		if message == nil {
			continue
		}
		next := *message
		next.MultiContent = cloneSlice(message.MultiContent)
		next.UserInputMultiContent = cloneSlice(message.UserInputMultiContent)
		next.AssistantGenMultiContent = cloneSlice(message.AssistantGenMultiContent)
		next.ToolCalls = cloneSlice(message.ToolCalls)
		next.Extra = cloneAnyMap(message.Extra)
		dst[i] = &next
	}
	return dst
}

func cloneToolInfos(src []*einoschema.ToolInfo) []*einoschema.ToolInfo {
	if src == nil {
		return nil
	}
	dst := make([]*einoschema.ToolInfo, len(src))
	for i, tool := range src {
		if tool == nil {
			continue
		}
		next := *tool
		next.Extra = cloneAnyMap(tool.Extra)
		dst[i] = &next
	}
	return dst
}

func cloneAnyMap(src map[string]any) map[string]any {
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
