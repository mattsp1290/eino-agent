package plugins

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/mattsp1290/eino-agent/config"
)

var ErrInvalidPlugin = errors.New("invalid plugin")

// Plugin applies one deterministic config-time extension.
type Plugin interface {
	Name() string
	Apply(ctx context.Context, snapshot *config.Snapshot) error
}

// Registry stores config plugins and applies them in deterministic name order.
type Registry struct {
	plugins map[string]Plugin
}

// NewRegistry returns an empty plugin registry.
func NewRegistry() *Registry {
	return &Registry{plugins: map[string]Plugin{}}
}

// Register adds one plugin. Duplicate or empty names fail before execution.
func (r *Registry) Register(plugin Plugin) error {
	if plugin == nil {
		return fmt.Errorf("%w: nil plugin", ErrInvalidPlugin)
	}
	name := plugin.Name()
	if name == "" {
		return fmt.Errorf("%w: plugin name required", ErrInvalidPlugin)
	}
	if r.plugins == nil {
		r.plugins = map[string]Plugin{}
	}
	if _, exists := r.plugins[name]; exists {
		return fmt.Errorf("%w: duplicate plugin %q", ErrInvalidPlugin, name)
	}
	r.plugins[name] = plugin
	return nil
}

// ApplyAll applies registered plugins in lexical name order and returns the
// applied order for auditing and tests.
func (r *Registry) ApplyAll(ctx context.Context, snapshot *config.Snapshot) ([]string, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("%w: snapshot required", ErrInvalidPlugin)
	}
	names := make([]string, 0, len(r.plugins))
	for name := range r.plugins {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := r.plugins[name].Apply(ctx, snapshot); err != nil {
			return names[:indexOf(names, name)], fmt.Errorf("plugin %q: %w", name, err)
		}
	}
	return names, nil
}

func indexOf(values []string, target string) int {
	for i, value := range values {
		if value == target {
			return i
		}
	}
	return len(values)
}
