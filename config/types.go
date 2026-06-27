package config

import (
	"context"

	"github.com/mattsp1290/eino-agent/model"
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
	Metadata       map[string]string
}

// Agent describes the runtime profile used for a run.
type Agent struct {
	Name         string
	SystemPrompt string
	Mode         string
	Options      map[string]string
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

// PermissionRule describes a coarse runtime permission rule. Leaf tools may ask
// for narrower approvals through runtime tool contexts.
type PermissionRule struct {
	Permission string
	Pattern    string
	Action     string
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
