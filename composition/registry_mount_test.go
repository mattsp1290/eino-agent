package composition

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/tools"
)

var compositionNotice = extension.NewNotification(extension.Contract{ID: "test/composition-notice", Version: "1"}, func(value string) (string, error) { return value, nil })

func component(id string) extension.Component {
	return extension.Component{InstanceID: id, Artifact: extension.Artifact{Name: id, Version: "1", Hash: id + "-hash", ConfigHash: id + "-config", SourceKind: extension.SourceNative}}
}

func definition(name, marker string) tools.Definition {
	return tools.Definition{
		Name: name, Description: marker,
		Decode: func(_ context.Context, raw json.RawMessage) (any, error) {
			return append(json.RawMessage(nil), raw...), nil
		},
		Encode:  func(_ context.Context, value any) (json.RawMessage, error) { return value.(json.RawMessage), nil },
		Execute: func(context.Context, tools.Execution) (any, error) { return json.RawMessage(`{"ok":true}`), nil },
	}
}

func TestDuplicateGuardAndRestrictionMountsRollbackAtomically(t *testing.T) {
	guard := runtime.ToolGuardFunc(func(context.Context, runtime.ToolGuardRequest) (runtime.ToolGuardResult, error) {
		return runtime.ToolGuardResult{Decision: runtime.ToolGuardAbstain}, nil
	})
	tests := map[string]struct {
		duplicate func(*Registrar) error
		valid     func(*Registrar) error
		count     func(session.ExtensionPlanDescriptor) int
	}{
		"guard": {
			duplicate: func(registrar *Registrar) error {
				if err := registrar.Guard(GuardRegistration{ID: "policy", Order: 1, Scope: extension.GlobalScope(), Guard: guard}); err != nil {
					return err
				}
				return registrar.Guard(GuardRegistration{ID: "policy", Order: 2, Scope: extension.GlobalScope(), Guard: guard})
			},
			valid: func(registrar *Registrar) error {
				return registrar.Guard(GuardRegistration{ID: "policy", Scope: extension.GlobalScope(), Guard: guard})
			},
			count: func(descriptor session.ExtensionPlanDescriptor) int { return len(descriptor.Guards) },
		},
		"restriction": {
			duplicate: func(registrar *Registrar) error {
				if err := registrar.RestrictTools(RestrictionRegistration{ID: "policy", Scope: extension.GlobalScope(), Allowed: []string{"echo"}}); err != nil {
					return err
				}
				return registrar.RestrictTools(RestrictionRegistration{ID: "policy", Scope: extension.GlobalScope(), Denied: []string{"shell"}})
			},
			valid: func(registrar *Registrar) error {
				return registrar.RestrictTools(RestrictionRegistration{ID: "policy", Scope: extension.GlobalScope(), Allowed: []string{"echo"}})
			},
			count: func(descriptor session.ExtensionPlanDescriptor) int { return len(descriptor.Restrictions) },
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			registry := NewRegistry(nil)
			mountedComponent := component("atomic-" + name)
			cleanups := 0
			_, err := registry.Mount(context.Background(), mountedComponent, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
				if err := registrar.Defer(func(context.Context) error { cleanups++; return nil }); err != nil {
					return err
				}
				if err := registrar.Tool(ToolRegistration{ID: "echo", Scope: extension.GlobalScope(), Definition: definition("echo", "staged")}); err != nil {
					return err
				}
				return test.duplicate(registrar)
			}))
			if !errors.Is(err, extension.ErrDuplicateRegistration) || cleanups != 1 {
				t.Fatalf("Mount error=%v cleanups=%d", err, cleanups)
			}
			diagnostics := registry.Diagnostics()
			if len(diagnostics.Components) != 0 || len(diagnostics.Tools) != 0 {
				t.Fatalf("failed mount published diagnostics: %#v", diagnostics)
			}
			plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
			if err != nil {
				t.Fatal(err)
			}
			descriptor := plan.Descriptor()
			plan.Release()
			if len(descriptor.Handlers)+len(descriptor.Tools)+len(descriptor.Prompts)+len(descriptor.Guards)+len(descriptor.Restrictions) != 0 {
				t.Fatalf("failed mount published plan: %#v", descriptor)
			}

			mount, err := registry.Mount(context.Background(), mountedComponent, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
				if err := registrar.Tool(ToolRegistration{ID: "echo", Scope: extension.GlobalScope(), Definition: definition("echo", "valid")}); err != nil {
					return err
				}
				return test.valid(registrar)
			}))
			if err != nil {
				t.Fatalf("component identity was not reusable: %v", err)
			}
			defer func() { _ = mount.Close(context.Background()) }()
			plan, err = registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
			if err != nil {
				t.Fatal(err)
			}
			descriptor = plan.Descriptor()
			plan.Release()
			if len(descriptor.Tools) != 1 || test.count(descriptor) != 1 {
				t.Fatalf("valid remount descriptor = %#v", descriptor)
			}
		})
	}
}

func TestRestrictionRegistrationRejectsInvalidRuleSets(t *testing.T) {
	tests := map[string]RestrictionRegistration{
		"empty":   {ID: "policy", Scope: extension.GlobalScope()},
		"blank":   {ID: "policy", Scope: extension.GlobalScope(), Allowed: []string{" "}},
		"overlap": {ID: "policy", Scope: extension.GlobalScope(), Allowed: []string{"echo"}, Denied: []string{"echo"}},
	}
	for name, registration := range tests {
		t.Run(name, func(t *testing.T) {
			registry := NewRegistry(nil)
			_, err := registry.Mount(context.Background(), component("invalid-restriction-"+name), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
				return registrar.RestrictTools(registration)
			}))
			if !errors.Is(err, extension.ErrInvalidRegistration) {
				t.Fatalf("Mount error = %v, want ErrInvalidRegistration", err)
			}
			if diagnostics := registry.Diagnostics(); len(diagnostics.Components) != 0 || len(diagnostics.Tools) != 0 {
				t.Fatalf("invalid restriction mount published: %#v", diagnostics)
			}
		})
	}
}

func TestMountRejectsInvalidToolDefinitionsAtomically(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*tools.Definition)
	}{
		{name: "missing decoder", mutate: func(definition *tools.Definition) { definition.Decode = nil }},
		{name: "missing encoder", mutate: func(definition *tools.Definition) { definition.Encode = nil }},
		{name: "missing executor", mutate: func(definition *tools.Definition) { definition.Execute = nil }},
		{name: "malformed schema", mutate: func(definition *tools.Definition) {
			definition.Parameters = einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{"broken": nil})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := NewRegistry(nil)
			component := component("invalid-tool")
			invalid := definition("broken", "broken")
			test.mutate(&invalid)
			_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
				return registrar.Tool(ToolRegistration{ID: "broken", Scope: extension.GlobalScope(), Definition: invalid})
			}))
			if !errors.Is(err, tools.ErrInvalidDefinition) {
				t.Fatalf("Mount error = %v, want ErrInvalidDefinition", err)
			}
			if len(registry.tools) != 0 {
				t.Fatalf("mounted tools = %d, want 0", len(registry.tools))
			}
			plan, planErr := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
			if planErr != nil {
				t.Fatalf("AcquireRunPlan error = %v", planErr)
			}
			defer plan.Release()
			descriptor := plan.Descriptor()
			if len(descriptor.Handlers)+len(descriptor.Tools)+len(descriptor.Prompts)+len(descriptor.Guards)+len(descriptor.Restrictions) != 0 {
				t.Fatalf("plan descriptor = %#v, want no identities", descriptor)
			}
		})
	}
}
