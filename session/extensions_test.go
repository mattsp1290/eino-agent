package session

import (
	"encoding/json"
	"testing"

	"github.com/mattsp1290/eino-agent/extension"
)

func TestExtensionPlanFingerprintCanonicalizesComponentsAndTypedCollections(t *testing.T) {
	descriptor := ExtensionPlanDescriptor{
		SchemaVersion: ExtensionPlanSchemaVersion,
		Components: []ComponentPlan{
			{
				InstanceID: "handlers", Artifact: testArtifact("handlers"),
				Handlers: []RegistrationIdentity{
					{ID: "same", Contract: "contract", Version: "2", Scope: extension.SessionScope("session-b"), Kind: extension.HandlerNotification},
					{ID: "same", Contract: "contract", Version: "1", Scope: extension.GlobalScope(), Kind: extension.HandlerAround},
				},
			},
			{
				InstanceID: "guards", Artifact: testArtifact("guards"),
				Guards: []GuardPlanIdentity{
					{RegistrationID: "session", Scope: extension.SessionScope("session-b"), Order: 20},
					{RegistrationID: "global", Scope: extension.GlobalScope(), Order: 10},
				},
			},
		},
	}
	reordered := descriptor.Clone()
	reordered.Components[0], reordered.Components[1] = reordered.Components[1], reordered.Components[0]
	reordered.Components[1].Handlers[0], reordered.Components[1].Handlers[1] = reordered.Components[1].Handlers[1], reordered.Components[1].Handlers[0]
	reordered.Components[0].Guards[0], reordered.Components[0].Guards[1] = reordered.Components[0].Guards[1], reordered.Components[0].Guards[0]

	first, err := fingerprintExtensionPlan(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	second, err := fingerprintExtensionPlan(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("permuted canonical descriptor fingerprints differ: %s != %s", first, second)
	}
}

func TestExtensionPlanFingerprintCanonicalizesEmptyComponentCollection(t *testing.T) {
	nilCollections := ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion}
	emptyCollections := ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion, Components: []ComponentPlan{}}
	first, err := fingerprintExtensionPlan(nilCollections)
	if err != nil {
		t.Fatal(err)
	}
	second, err := fingerprintExtensionPlan(emptyCollections)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("nil and empty descriptor fingerprints differ: %s != %s", first, second)
	}
}

func TestExtensionPlanSchemaV1JSONAndFingerprintGolden(t *testing.T) {
	descriptor := ExtensionPlanDescriptor{
		SchemaVersion: ExtensionPlanSchemaVersion,
		Components: []ComponentPlan{{
			InstanceID: "component",
			Artifact:   extension.Artifact{Name: "artifact", Version: "1", Hash: "hash", ConfigHash: "config", SourceKind: extension.SourceNative},
			Tools:      []ToolPlanIdentity{{Name: "tool", RegistrationID: "registration", Scope: extension.GlobalScope(), SchemaHash: "schema", ExecutorHash: "executor"}},
		}},
	}
	const expectedJSON = `{"SchemaVersion":1,"Fingerprint":"","Components":[{"InstanceID":"component","Artifact":{"Name":"artifact","Version":"1","Hash":"hash","ConfigHash":"config","SourceKind":"native"},"Handlers":null,"Tools":[{"Name":"tool","RegistrationID":"registration","Scope":{"Kind":"global","Key":""},"SchemaHash":"schema","ExecutorHash":"executor","Order":0}],"Prompts":null,"Guards":null,"Restrictions":null}]}`
	raw, err := json.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != expectedJSON {
		t.Fatalf("schema-v1 JSON changed:\n got: %s\nwant: %s", raw, expectedJSON)
	}
	fingerprint, err := fingerprintExtensionPlan(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	const expectedFingerprint = "3f84a9c0b287ea01d84ae35bf94855fb6757d9412f02c3659ae9249d0b4a4d37"
	if fingerprint != expectedFingerprint {
		t.Fatalf("schema-v1 fingerprint = %s", fingerprint)
	}
}

func TestExtensionPlanRejectsDelimiterBearingIdentifiersButKeepsScopeKeysOpaque(t *testing.T) {
	invalid := ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion, Components: []ComponentPlan{{
		InstanceID: "component", Artifact: testArtifact("delimiter"),
		Tools: []ToolPlanIdentity{{Name: "tool", RegistrationID: "bad\x00registration", Scope: extension.GlobalScope(), SchemaHash: "schema", ExecutorHash: "executor"}},
	}}}
	if _, err := SealExtensionPlan(invalid); err == nil {
		t.Fatal("delimiter-bearing registration id was accepted")
	}

	valid := invalid.Clone()
	valid.Components[0].Tools[0].RegistrationID = "registration"
	valid.Components[0].Tools[0].Scope = extension.SessionScope("tenant\x00workspace")
	if _, err := SealExtensionPlan(valid); err != nil {
		t.Fatalf("opaque scope key rejected: %v", err)
	}
}

func TestExtensionPlanRejectsMalformedComponentAndTypedIdentity(t *testing.T) {
	valid := ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion, Components: []ComponentPlan{{
		InstanceID: "tool", Artifact: testArtifact("tool"),
		Tools: []ToolPlanIdentity{{Name: "tool", RegistrationID: "registration", Scope: extension.GlobalScope(), SchemaHash: "schema", ExecutorHash: "executor"}},
	}}}
	for name, mutate := range map[string]func(*ExtensionPlanDescriptor){
		"missing config hash": func(value *ExtensionPlanDescriptor) { value.Components[0].Artifact.ConfigHash = "" },
		"invalid source":      func(value *ExtensionPlanDescriptor) { value.Components[0].Artifact.SourceKind = "remote" },
		"missing executor":    func(value *ExtensionPlanDescriptor) { value.Components[0].Tools[0].ExecutorHash = "" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid.Clone()
			mutate(&candidate)
			if _, err := SealExtensionPlan(candidate); err == nil {
				t.Fatal("SealExtensionPlan succeeded")
			}
		})
	}
}

func TestExtensionPlanRejectsDuplicateAndEmptyComponents(t *testing.T) {
	component := ComponentPlan{InstanceID: "component", Artifact: testArtifact("component"), Guards: []GuardPlanIdentity{{RegistrationID: "guard", Scope: extension.GlobalScope()}}}
	for name, components := range map[string][]ComponentPlan{
		"duplicate": {component, component},
		"empty":     {{InstanceID: "empty", Artifact: testArtifact("empty")}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := SealExtensionPlan(ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion, Components: components}); err == nil {
				t.Fatal("SealExtensionPlan accepted invalid components")
			}
		})
	}
}

func TestExtensionPlanValidatesSemanticHandlerKinds(t *testing.T) {
	base := ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion, Components: []ComponentPlan{{
		InstanceID: "handlers", Artifact: testArtifact("handlers"),
		Handlers: []RegistrationIdentity{{ID: "handler", Contract: "contract", Version: "1", Scope: extension.GlobalScope()}},
	}}}
	for _, kind := range []extension.HandlerKind{extension.HandlerNotification, extension.HandlerHook, extension.HandlerTransform, extension.HandlerGate, extension.HandlerAround} {
		candidate := base.Clone()
		candidate.Components[0].Handlers[0].Kind = kind
		if _, err := SealExtensionPlan(candidate); err != nil {
			t.Fatalf("semantic kind %q rejected: %v", kind, err)
		}
	}
	invalid := base.Clone()
	invalid.Components[0].Handlers[0].Kind = "interceptor"
	if _, err := SealExtensionPlan(invalid); err == nil {
		t.Fatal("legacy interceptor handler kind was accepted")
	}
}

func TestExtensionPlanRejectsDuplicateLogicalIdentitiesDespiteFingerprintFields(t *testing.T) {
	global := extension.GlobalScope()
	tests := map[string]ComponentPlan{
		"handler order": {
			Handlers: []RegistrationIdentity{
				{ID: "handler", Contract: "contract", Version: "1", Order: 1, Scope: global, Kind: extension.HandlerNotification},
				{ID: "handler", Contract: "contract", Version: "1", Order: 2, Scope: global, Kind: extension.HandlerNotification},
			},
		},
		"tool hashes": {Tools: []ToolPlanIdentity{
			{Name: "tool", RegistrationID: "tool", Scope: global, SchemaHash: "schema-a", ExecutorHash: "executor-a"},
			{Name: "tool", RegistrationID: "tool", Scope: global, SchemaHash: "schema-b", ExecutorHash: "executor-b"},
		}},
		"prompt order": {Prompts: []PromptPlanIdentity{
			{Name: "prompt", RegistrationID: "prompt", Scope: global, Order: 1},
			{Name: "prompt", RegistrationID: "prompt", Scope: global, Order: 2},
		}},
		"guard order": {Guards: []GuardPlanIdentity{
			{RegistrationID: "guard", Scope: global, Order: 1},
			{RegistrationID: "guard", Scope: global, Order: 2},
		}},
		"restriction rules": {Restrictions: []RestrictionPlanIdentity{
			{RegistrationID: "restriction", Scope: global, RulesHash: "rules-a"},
			{RegistrationID: "restriction", Scope: global, RulesHash: "rules-b"},
		}},
	}
	for name, component := range tests {
		t.Run(name, func(t *testing.T) {
			component.InstanceID = "component"
			component.Artifact = testArtifact("duplicates")
			descriptor := ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion, Components: []ComponentPlan{component}}
			if _, err := SealExtensionPlan(descriptor); err == nil {
				t.Fatal("SealExtensionPlan accepted duplicate logical identities")
			}
		})
	}
}

func TestSealedExtensionPlanVerifiesSessionAndOwnsCanonicalDescriptor(t *testing.T) {
	descriptor := ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion, Components: []ComponentPlan{{
		InstanceID: "tool", Artifact: testArtifact("sealed"),
		Tools: []ToolPlanIdentity{{Name: "tool", RegistrationID: "registration", Scope: extension.SessionScope("session-a"), SchemaHash: "schema", ExecutorHash: "executor"}},
	}}}
	sealed, err := SealExtensionPlan(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	persisted := sealed.Descriptor()
	verified, err := VerifyExtensionPlanForSession("session-a", persisted)
	if err != nil || !sealed.Matches(verified) {
		t.Fatalf("verified plan = %#v, %v", verified, err)
	}
	if _, err := VerifyExtensionPlanForSession("session-b", persisted); err == nil {
		t.Fatal("session-scoped plan verified for another session")
	}
	tampered := persisted.Clone()
	tampered.Components[0].Artifact.Hash = "tampered"
	if _, err := VerifyExtensionPlanForSession("session-a", tampered); err == nil {
		t.Fatal("tampered persisted plan verified")
	}
	if _, err := SealExtensionPlan(persisted); err == nil {
		t.Fatal("persisted plan was accepted as newly reconstructed")
	}
	persisted.Components[0].InstanceID = "mutated-copy"
	if sealed.Descriptor().Components[0].InstanceID != "tool" {
		t.Fatal("sealed plan leaked mutable descriptor ownership")
	}
}

func fingerprintExtensionPlan(descriptor ExtensionPlanDescriptor) (string, error) {
	sealed, err := SealExtensionPlan(descriptor)
	if err != nil {
		return "", err
	}
	return sealed.Fingerprint(), nil
}

func testArtifact(suffix string) extension.Artifact {
	return extension.Artifact{Name: "artifact-" + suffix, Version: "1", Hash: "hash-" + suffix, ConfigHash: "config-" + suffix, SourceKind: extension.SourceNative}
}
