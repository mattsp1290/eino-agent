package config

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/mattsp1290/eino-agent/model"
)

func TestLifecycleAppliesPluginsDeterministicallyBeforeValidation(t *testing.T) {
	t.Parallel()

	loader := staticLoader{snapshot: validSnapshot()}
	registry := orderedPluginRegistry{}
	order := []string{}
	for _, name := range []string{"z-last", "a-first", "m-middle"} {
		name := name
		registry.plugins = append(registry.plugins, testPlugin{name: name, apply: func(_ context.Context, snapshot *Snapshot) error {
			order = append(order, name)
			if snapshot.Metadata == nil {
				snapshot.Metadata = map[string]string{}
			}
			snapshot.Metadata[name] = "applied"
			return nil
		}})
	}
	lifecycle := Lifecycle{Loader: loader, Plugins: registry}
	snapshot, err := lifecycle.LoadSnapshot(context.Background(), Scope{}, "default")
	if err != nil {
		t.Fatalf("LoadSnapshot error = %v", err)
	}
	wantOrder := []string{"a-first", "m-middle", "z-last"}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("plugin order = %#v, want %#v", order, wantOrder)
	}
	if snapshot.Metadata["a-first"] != "applied" || snapshot.Metadata["z-last"] != "applied" {
		t.Fatalf("snapshot metadata = %#v", snapshot.Metadata)
	}
}

func TestLifecycleRejectsPluginErrorBeforeReturningSnapshot(t *testing.T) {
	t.Parallel()

	errPlugin := errors.New("plugin failed")
	registry := orderedPluginRegistry{plugins: []testPlugin{{name: "plugin", apply: func(context.Context, *Snapshot) error {
		return errPlugin
	}}}}
	lifecycle := Lifecycle{Loader: staticLoader{snapshot: validSnapshot()}, Plugins: registry}
	_, err := lifecycle.LoadSnapshot(context.Background(), Scope{}, "default")
	if !errors.Is(err, errPlugin) {
		t.Fatalf("LoadSnapshot error = %v, want plugin error", err)
	}
}

func TestLifecycleRejectsInvalidSnapshotBeforeExecution(t *testing.T) {
	t.Parallel()

	snapshot := validSnapshot()
	snapshot.Tools.Permissions = []PermissionRule{{Permission: "shell", Action: "invalid"}}
	lifecycle := Lifecycle{Loader: staticLoader{snapshot: snapshot}}
	_, err := lifecycle.LoadSnapshot(context.Background(), Scope{}, "default")
	if !HasValidationCode(err, ValidationInvalidPermission) {
		t.Fatalf("LoadSnapshot error = %v, want %s", err, ValidationInvalidPermission)
	}
}

func TestLifecycleAppliesSnapshotDefaultsBeforeValidation(t *testing.T) {
	t.Parallel()

	snapshot := validSnapshot()
	snapshot.Tools.Permissions = []PermissionRule{{Permission: "shell"}}
	snapshot.Observability = ObservabilityConfig{}
	lifecycle := Lifecycle{Loader: staticLoader{snapshot: snapshot}}
	loaded, err := lifecycle.LoadSnapshot(context.Background(), Scope{}, "default")
	if err != nil {
		t.Fatalf("LoadSnapshot error = %v", err)
	}
	if loaded.Tools.Permissions[0].Action != PermissionActionAsk {
		t.Fatalf("permission action = %q, want %q", loaded.Tools.Permissions[0].Action, PermissionActionAsk)
	}
	fields := map[string]bool{}
	for _, field := range loaded.Observability.Fields {
		fields[field.Name] = true
	}
	if !fields["raw_prompt"] || !fields["raw_tool_payload"] {
		t.Fatalf("observability defaults missing safe fields: %#v", loaded.Observability.Fields)
	}
}

func TestLifecycleAppliesSnapshotDefaultsAfterPlugins(t *testing.T) {
	t.Parallel()

	registry := orderedPluginRegistry{plugins: []testPlugin{{name: "permission", apply: func(context.Context, *Snapshot) error {
		return nil
	}}}}
	registry.plugins[0].apply = func(_ context.Context, snapshot *Snapshot) error {
		snapshot.Tools.Permissions = append(snapshot.Tools.Permissions, PermissionRule{Permission: "apply_patch"})
		snapshot.Observability = ObservabilityConfig{}
		return nil
	}
	lifecycle := Lifecycle{Loader: staticLoader{snapshot: validSnapshot()}, Plugins: registry}
	loaded, err := lifecycle.LoadSnapshot(context.Background(), Scope{}, "default")
	if err != nil {
		t.Fatalf("LoadSnapshot error = %v", err)
	}
	if loaded.Tools.Permissions[0].Action != PermissionActionAsk {
		t.Fatalf("plugin permission action = %q, want %q", loaded.Tools.Permissions[0].Action, PermissionActionAsk)
	}
	if len(loaded.Observability.Fields) == 0 {
		t.Fatal("plugin observability config was not defaulted")
	}
}

func TestLifecycleReturnsImmutableSnapshot(t *testing.T) {
	t.Parallel()

	loaderSnapshot := validSnapshot()
	loader := &mutableLoader{snapshot: loaderSnapshot}
	lifecycle := Lifecycle{Loader: loader}
	snapshot, err := lifecycle.LoadSnapshot(context.Background(), Scope{}, "default")
	if err != nil {
		t.Fatalf("LoadSnapshot error = %v", err)
	}
	loader.snapshot.Agent.Options["temperature"] = "changed"
	loader.snapshot.Tools.Enabled[0] = "changed"
	loader.snapshot.Metadata["workspace_root"] = "changed"
	if snapshot.Agent.Options["temperature"] != "0.2" {
		t.Fatalf("snapshot agent options mutated: %#v", snapshot.Agent.Options)
	}
	if snapshot.Tools.Enabled[0] != "file_read" {
		t.Fatalf("snapshot tools mutated: %#v", snapshot.Tools.Enabled)
	}
	if snapshot.Metadata["workspace_root"] != "/workspace" {
		t.Fatalf("snapshot metadata mutated: %#v", snapshot.Metadata)
	}
}

type staticLoader struct {
	snapshot Snapshot
}

func (l staticLoader) Load(context.Context, Scope) (Snapshot, error) {
	return l.snapshot, nil
}

type mutableLoader struct {
	snapshot Snapshot
}

func (l *mutableLoader) Load(context.Context, Scope) (Snapshot, error) {
	return l.snapshot, nil
}

type testPlugin struct {
	name  string
	apply func(context.Context, *Snapshot) error
}

func (p testPlugin) Name() string {
	return p.name
}

func (p testPlugin) Apply(ctx context.Context, snapshot *Snapshot) error {
	return p.apply(ctx, snapshot)
}

type orderedPluginRegistry struct {
	plugins []testPlugin
}

func (r orderedPluginRegistry) ApplyAll(ctx context.Context, snapshot *Snapshot) ([]string, error) {
	sort.Slice(r.plugins, func(i, j int) bool {
		return r.plugins[i].Name() < r.plugins[j].Name()
	})
	names := make([]string, 0, len(r.plugins))
	for _, plugin := range r.plugins {
		names = append(names, plugin.Name())
		if err := plugin.Apply(ctx, snapshot); err != nil {
			return names, err
		}
	}
	return names, nil
}

func validSnapshot() Snapshot {
	selection := model.Selection{ProviderID: "openai", ModelID: "gpt-4.1"}
	return Snapshot{
		Agent: Agent{
			Name:    "default",
			Model:   selection,
			Options: map[string]string{"temperature": "0.2"},
		},
		Model: selection,
		Tools: ToolConfig{
			Enabled: []string{"file_read"},
		},
		Metadata: map[string]string{
			"workspace_root": "/workspace",
		},
		Observability: ObservabilityConfig{}.WithDefaults(),
	}
}
