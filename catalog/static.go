package catalog

import (
	"context"
	"fmt"

	"github.com/mattsp1290/eino-agent/model"
)

// Static is an in-memory model catalog. It is useful for tests, embedded host
// defaults, and config validation before provider transports are available.
type Static struct {
	Providers []model.Provider
	Models    []model.Descriptor
	Default   model.Selection
}

// ListProviders returns configured provider metadata.
func (c Static) ListProviders(context.Context) ([]model.Provider, error) {
	providers := cloneSlice(c.Providers)
	for i := range providers {
		providers[i].Environment = cloneSlice(c.Providers[i].Environment)
		providers[i].Options = cloneMap(c.Providers[i].Options)
	}
	return providers, nil
}

// ListModels returns configured model metadata for providerID.
func (c Static) ListModels(_ context.Context, providerID model.ProviderID) ([]model.Descriptor, error) {
	var models []model.Descriptor
	for _, descriptor := range c.Models {
		if descriptor.ProviderID == providerID {
			models = append(models, cloneDescriptor(descriptor))
		}
	}
	return models, nil
}

// GetModel returns one configured model descriptor.
func (c Static) GetModel(_ context.Context, providerID model.ProviderID, modelID model.ID) (model.Descriptor, error) {
	for _, descriptor := range c.Models {
		if descriptor.ProviderID == providerID && descriptor.ID == modelID {
			return cloneDescriptor(descriptor), nil
		}
	}
	return model.Descriptor{}, fmt.Errorf("catalog model %s/%s not found", providerID, modelID)
}

// DefaultModel returns the configured default model selection.
func (c Static) DefaultModel(context.Context) (model.Selection, error) {
	if c.Default.ProviderID == "" || c.Default.ModelID == "" {
		return model.Selection{}, fmt.Errorf("catalog default model is not configured")
	}
	return c.Default, nil
}

func cloneDescriptor(src model.Descriptor) model.Descriptor {
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

func cloneSlice[T any](src []T) []T {
	if src == nil {
		return nil
	}
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}
