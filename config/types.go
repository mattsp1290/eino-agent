package config

import (
	"context"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/obs"
)

// Scope describes where configuration is being loaded.
type Scope struct {
	WorkspaceID string
	Directory   string
	Environment map[string]string
}

// Snapshot is the immutable configuration captured for run admission.
type Snapshot struct {
	Agent          Agent
	Model          model.Selection
	Providers      []ProviderConfig
	Tools          ToolConfig
	ContextSources []ContextSource
	Hooks          []Hook
	Plugins        []Plugin
	Observability  ObservabilityConfig
	Metadata       map[string]string
}

// Clone returns a deep copy of the snapshot maps and slices owned by this
// package. Runtime admission must call Clone, or an equivalent deep-copy path,
// before retaining a snapshot for an in-flight run.
func (s Snapshot) Clone() Snapshot {
	next := s
	next.Providers = cloneSlice(s.Providers)
	next.ContextSources = cloneSlice(s.ContextSources)
	next.Hooks = cloneSlice(s.Hooks)
	next.Plugins = cloneSlice(s.Plugins)
	next.Metadata = cloneMap(s.Metadata)
	next.Agent.Options = cloneMap(s.Agent.Options)
	next.Tools = s.Tools.Clone()
	next.Observability = s.Observability.Clone()
	for i := range next.Providers {
		next.Providers[i].Options = cloneMap(s.Providers[i].Options)
	}
	for i := range next.ContextSources {
		next.ContextSources[i].Options = cloneMap(s.ContextSources[i].Options)
	}
	for i := range next.Hooks {
		next.Hooks[i].Options = cloneMap(s.Hooks[i].Options)
	}
	return next
}

// Agent describes the runtime profile used for a run.
type Agent struct {
	Name         string
	SystemPrompt string
	Mode         string
	Model        model.Selection
	Options      map[string]string
}

// Config is the validated user-facing configuration document. It preserves
// named agents and catalog choices before admission selects one immutable
// Snapshot for a run.
type Config struct {
	DefaultAgent   string
	Agents         map[string]Agent
	Models         []model.Selection
	Providers      []ProviderConfig
	Tools          ToolConfig
	ContextSources []ContextSource
	Hooks          []Hook
	Plugins        []Plugin
	Observability  ObservabilityConfig
	Metadata       map[string]string
}

// ProviderConfig contains provider-specific config before model resolution.
type ProviderConfig struct {
	ProviderID model.ProviderID
	Options    map[string]string
}

// ToolConfig controls tool materialization for a run snapshot.
type ToolConfig struct {
	Enabled     []string
	Disabled    []string
	Permissions []PermissionRule
}

const (
	// PermissionActionAsk requires host approval before a matching operation.
	PermissionActionAsk = "ask"
	// PermissionActionAllow permits a matching operation without prompting.
	PermissionActionAllow = "allow"
	// PermissionActionDeny rejects a matching operation before execution.
	PermissionActionDeny = "deny"
)

// Clone returns a deep copy of the tool config.
func (c ToolConfig) Clone() ToolConfig {
	return ToolConfig{
		Enabled:     cloneSlice(c.Enabled),
		Disabled:    cloneSlice(c.Disabled),
		Permissions: cloneSlice(c.Permissions),
	}
}

// PermissionRule describes a coarse runtime permission rule. Leaf tools may ask
// for narrower approvals through runtime tool contexts.
type PermissionRule struct {
	Permission string
	Pattern    string
	Action     string
}

// ObservabilityConfig contains exporter-safe observation defaults. Raw prompt,
// model output, and tool payload fields stay governed by the redaction policy.
type ObservabilityConfig struct {
	Service     string
	Env         string
	Version     string
	Fields      []obs.FieldPolicy
	Correlation []obs.CorrelationField
	Summary     obs.SummaryPolicy
}

// Clone returns a deep copy of the observability config slices.
func (c ObservabilityConfig) Clone() ObservabilityConfig {
	next := c
	next.Fields = cloneSlice(c.Fields)
	next.Correlation = cloneSlice(c.Correlation)
	next.Summary.AllowedKinds = cloneSlice(c.Summary.AllowedKinds)
	next.Summary.ForbiddenInputs = cloneSlice(c.Summary.ForbiddenInputs)
	return next
}

// ContextSource describes a configured source of prompt/context material.
type ContextSource struct {
	Name     string
	Type     string
	Location string
	Options  map[string]string
}

// Hook describes a deterministic extension registration.
type Hook struct {
	Name    string
	Source  string
	Version string
	Options map[string]string
}

// Plugin describes an extension bundle available to runtime hooks.
type Plugin struct {
	Name    string
	Source  string
	Version string
	Hash    string
	Scope   string
}

// Loader returns a validated configuration snapshot for one runtime scope.
type Loader interface {
	Load(ctx context.Context, scope Scope) (Snapshot, error)
}

// Validator checks a snapshot before run admission.
type Validator interface {
	Validate(ctx context.Context, snapshot Snapshot) error
}

func cloneMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
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
