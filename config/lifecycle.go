package config

import (
	"context"
	"fmt"

	"github.com/mattsp1290/eino-agent/model"
)

// SnapshotPlugin mutates a loaded snapshot before final validation. Plugins
// are applied before the returned snapshot is cloned and admitted to a run.
type SnapshotPlugin interface {
	Name() string
	Apply(ctx context.Context, snapshot *Snapshot) error
}

// PluginRegistry is the minimal lifecycle dependency needed by config loading.
type PluginRegistry interface {
	ApplyAll(ctx context.Context, snapshot *Snapshot) ([]string, error)
}

// Lifecycle loads, validates, applies plugins, and freezes run snapshots.
type Lifecycle struct {
	Loader    Loader
	Validator Validator
	Catalog   model.Catalog
	Plugins   PluginRegistry
}

// LoadSnapshot returns a cloned, validated snapshot for one run admission.
func (l Lifecycle) LoadSnapshot(ctx context.Context, scope Scope, agentName string) (Snapshot, error) {
	if l.Loader == nil {
		return Snapshot{}, fmt.Errorf("config lifecycle: loader required")
	}
	loaded, err := l.Loader.Load(ctx, scope)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot := normalizeSnapshot(loaded)
	if l.Plugins != nil {
		if _, err := l.Plugins.ApplyAll(ctx, &snapshot); err != nil {
			return Snapshot{}, err
		}
		snapshot = normalizeSnapshot(snapshot)
	}
	if agentName != "" && snapshot.Agent.Name != "" && snapshot.Agent.Name != agentName {
		return Snapshot{}, validationError(ValidationUnknownAgent, "agent", agentName)
	}
	if l.Validator != nil {
		if err := l.Validator.Validate(ctx, snapshot.Clone()); err != nil {
			return Snapshot{}, err
		}
	}
	if err := validateSnapshot(ctx, snapshot, l.Catalog); err != nil {
		return Snapshot{}, err
	}
	return snapshot.Clone(), nil
}

type snapshotValidatorFunc func(context.Context, Snapshot) error

func (fn snapshotValidatorFunc) Validate(ctx context.Context, snapshot Snapshot) error {
	return fn(ctx, snapshot)
}

func validateSnapshot(ctx context.Context, snapshot Snapshot, catalog model.Catalog) error {
	if snapshot.Agent.Name == "" {
		return validationError(ValidationUnknownAgent, "agent", "must name selected agent")
	}
	if err := validateAgentModel(ctx, snapshot.Agent, []model.Selection{snapshot.Model}, catalog); err != nil {
		return err
	}
	if err := validatePermissions(snapshot.Tools); err != nil {
		return err
	}
	if err := validateObservability(snapshot.Observability); err != nil {
		return err
	}
	return nil
}

func normalizeSnapshot(snapshot Snapshot) Snapshot {
	next := snapshot.Clone()
	next.Tools = next.Tools.WithDefaults()
	next.Observability = next.Observability.WithDefaults()
	return next
}

// SnapshotValidator returns a Validator for callers that already loaded a
// Snapshot and need the same pre-execution validation used by Lifecycle.
func SnapshotValidator(catalog model.Catalog) Validator {
	return snapshotValidatorFunc(func(ctx context.Context, snapshot Snapshot) error {
		return validateSnapshot(ctx, snapshot, catalog)
	})
}
