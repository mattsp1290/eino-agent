package model

import (
	"context"

	einomodel "github.com/cloudwego/eino/components/model"
)

// ProviderID identifies a provider adapter.
type ProviderID string

// ID identifies a model within a provider.
type ID string

// Selection is the requested provider/model pair for a run.
type Selection struct {
	ProviderID ProviderID
	ModelID    ID
	Variant    string
}

// Provider describes one available model provider.
type Provider struct {
	ID          ProviderID
	Name        string
	Source      string
	Environment []string
	Options     map[string]string
}

// Descriptor describes one model before a concrete Eino client is built.
type Descriptor struct {
	ID           ID
	ProviderID   ProviderID
	Name         string
	Family       string
	ContextLimit int
	InputLimit   int
	OutputLimit  int
	Capabilities map[string]bool
	Options      map[string]string
}

// Runtime contains per-request data needed while resolving a provider model.
type Runtime struct {
	Directory string
	Env       map[string]string
	Auth      map[string]string
	Options   map[string]string
}

// Catalog lists provider and model metadata without creating provider clients.
type Catalog interface {
	ListProviders(ctx context.Context) ([]Provider, error)
	ListModels(ctx context.Context, providerID ProviderID) ([]Descriptor, error)
	GetModel(ctx context.Context, providerID ProviderID, modelID ID) (Descriptor, error)
	DefaultModel(ctx context.Context) (Selection, error)
}

// Resolver turns a selected model into an immutable Eino chat model for one
// turn snapshot. Implementations may return a model with no tools bound yet.
type Resolver interface {
	Resolve(ctx context.Context, selection Selection, runtime Runtime) (Resolved, error)
}

// Resolved is the concrete model selected for one provider request.
type Resolved struct {
	Provider Provider
	Model    Descriptor
	Client   einomodel.ToolCallingChatModel
}

// AuthResolver resolves provider credentials at request time so expiring
// credentials are not frozen into long-lived runtime objects.
type AuthResolver interface {
	ResolveAuth(ctx context.Context, providerID ProviderID) (map[string]string, error)
}
