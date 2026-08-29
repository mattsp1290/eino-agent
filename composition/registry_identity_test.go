package composition

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func TestCapabilityRegistrarsShareCanonicalIdentifierValidation(t *testing.T) {
	invalidID := "invalid\x00identity"
	guard := runtime.ToolGuardFunc(func(context.Context, runtime.ToolGuardRequest) (runtime.ToolGuardResult, error) {
		return runtime.ToolGuardResult{Decision: runtime.ToolGuardAbstain}, nil
	})
	installers := map[string]InstallerFunc{
		"tool": func(_ context.Context, registrar *Registrar) error {
			return registrar.Tool(ToolRegistration{ID: invalidID, Scope: extension.GlobalScope(), Definition: definition("tool", "tool")})
		},
		"prompt": func(_ context.Context, registrar *Registrar) error {
			return registrar.Prompt(PromptRegistration{ID: invalidID, Name: "prompt", Scope: extension.GlobalScope(), Provider: runtime.PromptProviderFunc(func(context.Context, runtime.PromptContext) (string, error) { return "", nil })})
		},
		"guard": func(_ context.Context, registrar *Registrar) error {
			return registrar.Guard(GuardRegistration{ID: invalidID, Scope: extension.GlobalScope(), Guard: guard})
		},
		"restriction": func(_ context.Context, registrar *Registrar) error {
			return registrar.RestrictTools(RestrictionRegistration{ID: invalidID, Scope: extension.GlobalScope(), Allowed: []string{"tool"}})
		},
	}
	for name, installer := range installers {
		t.Run(name, func(t *testing.T) {
			registry := NewRegistry(nil)
			_, err := registry.Mount(context.Background(), component("invalid-"+name), installer)
			if !errors.Is(err, extension.ErrInvalidRegistration) {
				t.Fatalf("Mount error = %v, want ErrInvalidRegistration", err)
			}
			assertRegistryEmpty(t, registry)
		})
	}
}

func TestToolSchemaHashCanonicalizesMetadataOrder(t *testing.T) {
	first := definition("metadata-tool", "stable")
	first.Metadata = map[string]string{"alpha": "one", "beta": "two"}
	second := definition("metadata-tool", "stable")
	second.Metadata = map[string]string{"beta": "two", "alpha": "one"}
	firstHash, err := toolSchemaHash(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := toolSchemaHash(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("equivalent metadata hashes differ: %s != %s", firstHash, secondHash)
	}
}

func TestComposedToolSchemaIdentityTracksSourceNotOrder(t *testing.T) {
	schemaA, schemaB := strings.Repeat("a", 64), strings.Repeat("b", 64)
	executorA, executorB := strings.Repeat("c", 64), strings.Repeat("d", 64)
	base := ToolRegistration{ID: "standard.echo", Order: 1000, Scope: extension.GlobalScope(), SourceSchemaHash: schemaA, SourceExecutorHash: executorA, Definition: definition("echo", "v1")}
	baseSchema, err := composedToolSchemaHash(base)
	if err != nil {
		t.Fatal(err)
	}
	changedSchema := base
	changedSchema.SourceSchemaHash = schemaB
	changedSchemaHash, _ := composedToolSchemaHash(changedSchema)
	changedOrder := base
	changedOrder.Order++
	changedOrderHash, _ := composedToolSchemaHash(changedOrder)
	if baseSchema == changedSchemaHash || baseSchema != changedOrderHash {
		t.Fatalf("schema identity source/order separation failed: base=%s source=%s order=%s", baseSchema, changedSchemaHash, changedOrderHash)
	}
	componentIdentity := component("tool-order").Artifact
	descriptor := func(order int) session.ExtensionPlanDescriptor {
		value := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Components: []session.ComponentPlan{{
			InstanceID: "tool-order", Artifact: componentIdentity,
			Tools: []session.ToolPlanIdentity{{Name: "echo", RegistrationID: base.ID, Scope: base.Scope, SchemaHash: baseSchema, ExecutorHash: executorA, Order: order}},
		}}}
		sealed, sealErr := session.SealExtensionPlan(value)
		err = sealErr
		if err != nil {
			t.Fatal(err)
		}
		return sealed.Descriptor()
	}
	if descriptor(base.Order).Fingerprint == descriptor(changedOrder.Order).Fingerprint {
		t.Fatal("tool order did not change plan fingerprint")
	}
	baseExecutor, _ := composedToolExecutorHash(executorA, "artifact")
	changedExecutor, _ := composedToolExecutorHash(executorB, "artifact")
	changedArtifact, _ := composedToolExecutorHash(executorA, "artifact-v2")
	if baseExecutor == changedExecutor || baseExecutor == changedArtifact {
		t.Fatal("executor identity ignored source/artifact")
	}
}

func TestToolSourceIdentityValidationIsAtomic(t *testing.T) {
	registry := NewRegistry(nil)
	component := component("source-validation")
	_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "standard.echo", Scope: extension.GlobalScope(), SourceSchemaHash: strings.Repeat("a", 64), Definition: definition("echo", "v1")})
	}))
	if !errors.Is(err, extension.ErrInvalidRegistration) {
		t.Fatalf("partial source identity = %v", err)
	}
	assertRegistryEmpty(t, registry)
}

func TestStrictResumeRejectsSourceIdentityAndOrderDrift(t *testing.T) {
	for _, mutate := range []struct {
		name string
		fn   func(*ToolRegistration)
	}{
		{name: "schema", fn: func(registration *ToolRegistration) { registration.SourceSchemaHash = strings.Repeat("b", 64) }},
		{name: "executor", fn: func(registration *ToolRegistration) { registration.SourceExecutorHash = strings.Repeat("d", 64) }},
		{name: "order", fn: func(registration *ToolRegistration) { registration.Order++ }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			registry := NewRegistry(nil)
			component := component("source-resume")
			registration := ToolRegistration{ID: "standard.echo", Order: 1000, Scope: extension.GlobalScope(), SourceSchemaHash: strings.Repeat("a", 64), SourceExecutorHash: strings.Repeat("c", 64), Definition: definition("echo", "v1")}
			mount := func(value ToolRegistration) *Mount {
				mounted, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar *Registrar) error { return registrar.Tool(value) }))
				if err != nil {
					t.Fatal(err)
				}
				return mounted
			}
			firstMount := mount(registration)
			plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
			if err != nil {
				t.Fatal(err)
			}
			persisted := plan.Descriptor()
			plan.Release()
			if err := firstMount.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			mutate.fn(&registration)
			secondMount := mount(registration)
			defer func() { _ = secondMount.Close(context.Background()) }()
			assertResumePlanDrift(t, registry, "session-a", persisted)
		})
	}
}
