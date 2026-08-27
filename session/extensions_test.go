package session

import (
	"encoding/json"
	"testing"

	"github.com/mattsp1290/eino-agent/extension"
)

func TestExtensionPlanFingerprintCanonicalizesTypedCollections(t *testing.T) {
	descriptor := ExtensionPlanDescriptor{
		SchemaVersion: ExtensionPlanSchemaVersion,
		Handlers: []HandlerPlanIdentity{{InstanceID: "component", Artifact: testArtifact("handlers"), Registrations: []RegistrationIdentity{
			{ID: "same", Contract: "contract", Version: "2", Scope: extension.SessionScope("session-b"), Kind: HandlerNotification},
			{ID: "same", Contract: "contract", Version: "1", Scope: extension.GlobalScope(), Kind: HandlerInterceptor},
		}}},
		Guards: []GuardPlanIdentity{
			{InstanceID: "guard-session", Artifact: testArtifact("b"), RegistrationID: "guard", Scope: extension.SessionScope("session-b"), Order: 20},
			{InstanceID: "guard-global", Artifact: testArtifact("a"), RegistrationID: "guard", Scope: extension.GlobalScope(), Order: 10},
		},
	}
	reordered := descriptor.Clone()
	reordered.Guards[0], reordered.Guards[1] = reordered.Guards[1], reordered.Guards[0]
	reordered.Handlers[0].Registrations[0], reordered.Handlers[0].Registrations[1] = reordered.Handlers[0].Registrations[1], reordered.Handlers[0].Registrations[0]

	first, err := FingerprintExtensionPlan(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	second, err := FingerprintExtensionPlan(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("permuted canonical descriptor fingerprints differ: %s != %s", first, second)
	}
}

func TestExtensionPlanFingerprintCanonicalizesEmptyCollections(t *testing.T) {
	nilCollections := ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion}
	emptyCollections := ExtensionPlanDescriptor{
		SchemaVersion: ExtensionPlanSchemaVersion,
		Handlers:      []HandlerPlanIdentity{},
		Tools:         []ToolPlanIdentity{},
		Prompts:       []PromptPlanIdentity{},
		Guards:        []GuardPlanIdentity{},
		Restrictions:  []RestrictionPlanIdentity{},
	}
	first, err := FingerprintExtensionPlan(nilCollections)
	if err != nil {
		t.Fatal(err)
	}
	second, err := FingerprintExtensionPlan(emptyCollections)
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
		Tools: []ToolPlanIdentity{{
			InstanceID: "component",
			Artifact: extension.Artifact{
				Name: "artifact", Version: "1", Hash: "hash", ConfigHash: "config", SourceKind: extension.SourceNative,
			},
			Name: "tool", RegistrationID: "registration", Scope: extension.GlobalScope(), SchemaHash: "schema", ExecutorHash: "executor",
		}},
	}
	const expectedJSON = `{"SchemaVersion":1,"Fingerprint":"","Handlers":null,"Tools":[{"InstanceID":"component","Artifact":{"Name":"artifact","Version":"1","Hash":"hash","ConfigHash":"config","SourceKind":"native"},"Name":"tool","RegistrationID":"registration","Scope":{"Kind":"global","Key":""},"SchemaHash":"schema","ExecutorHash":"executor"}],"Prompts":null,"Guards":null,"Restrictions":null}`
	raw, err := json.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != expectedJSON {
		t.Fatalf("schema-v1 JSON changed:\n got: %s\nwant: %s", raw, expectedJSON)
	}
	fingerprint, err := FingerprintExtensionPlan(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if fingerprint != "08a8b8ee8061dded490f494a81338aa104da43619a88f3c778b1867df0d68df8" {
		t.Fatalf("schema-v1 fingerprint = %s", fingerprint)
	}
}

func TestExtensionPlanRejectsDelimiterBearingIdentifiersButKeepsScopeKeysOpaque(t *testing.T) {
	artifact := testArtifact("delimiter")
	invalid := ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion, Tools: []ToolPlanIdentity{{
		InstanceID: "component", Artifact: artifact, Name: "tool", RegistrationID: "bad\x00registration",
		Scope: extension.GlobalScope(), SchemaHash: "schema", ExecutorHash: "executor",
	}}}
	if err := ValidateExtensionPlan(invalid); err == nil {
		t.Fatal("delimiter-bearing registration id was accepted")
	}

	valid := invalid
	valid.Tools = append([]ToolPlanIdentity(nil), invalid.Tools...)
	valid.Tools[0].RegistrationID = "registration"
	valid.Tools[0].Scope = extension.SessionScope("tenant\x00workspace")
	if err := ValidateExtensionPlan(valid); err != nil {
		t.Fatalf("opaque scope key rejected: %v", err)
	}
}

func TestExtensionPlanRejectsMalformedTypedIdentity(t *testing.T) {
	valid := ToolPlanIdentity{
		InstanceID: "tool", Artifact: testArtifact("tool"), Name: "tool", RegistrationID: "registration",
		Scope: extension.GlobalScope(), SchemaHash: "schema", ExecutorHash: "executor",
	}
	for name, mutate := range map[string]func(*ToolPlanIdentity){
		"missing config hash": func(value *ToolPlanIdentity) { value.Artifact.ConfigHash = "" },
		"invalid source":      func(value *ToolPlanIdentity) { value.Artifact.SourceKind = "remote" },
		"missing executor":    func(value *ToolPlanIdentity) { value.ExecutorHash = "" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if _, err := FingerprintExtensionPlan(ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion, Tools: []ToolPlanIdentity{candidate}}); err == nil {
				t.Fatal("FingerprintExtensionPlan succeeded")
			}
		})
	}
}

func TestExtensionPlanRejectsConflictingComponentArtifacts(t *testing.T) {
	descriptor := ExtensionPlanDescriptor{
		SchemaVersion: ExtensionPlanSchemaVersion,
		Tools: []ToolPlanIdentity{{
			InstanceID: "component", Artifact: testArtifact("first"), Name: "tool", RegistrationID: "tool",
			Scope: extension.GlobalScope(), SchemaHash: "schema", ExecutorHash: "executor",
		}},
		Guards: []GuardPlanIdentity{{
			InstanceID: "component", Artifact: testArtifact("second"), RegistrationID: "guard", Scope: extension.GlobalScope(),
		}},
	}
	if _, err := FingerprintExtensionPlan(descriptor); err == nil {
		t.Fatal("FingerprintExtensionPlan accepted conflicting artifacts")
	}
}

func TestExtensionPlanRejectsMultipleHandlerAggregatesForInstance(t *testing.T) {
	artifact := testArtifact("handlers")
	descriptor := ExtensionPlanDescriptor{
		SchemaVersion: ExtensionPlanSchemaVersion,
		Handlers: []HandlerPlanIdentity{
			{InstanceID: "component", Artifact: artifact, Registrations: []RegistrationIdentity{{ID: "first", Contract: "contract", Version: "1", Scope: extension.GlobalScope(), Kind: HandlerNotification}}},
			{InstanceID: "component", Artifact: artifact, Registrations: []RegistrationIdentity{{ID: "second", Contract: "contract", Version: "1", Scope: extension.GlobalScope(), Kind: HandlerNotification}}},
		},
	}
	if _, err := FingerprintExtensionPlan(descriptor); err == nil {
		t.Fatal("FingerprintExtensionPlan accepted split handler aggregates")
	}
}

func TestExtensionPlanRejectsDuplicateLogicalIdentitiesDespiteFingerprintFields(t *testing.T) {
	artifact := testArtifact("duplicates")
	global := extension.GlobalScope()
	tests := map[string]ExtensionPlanDescriptor{
		"handler order": {
			Handlers: []HandlerPlanIdentity{{InstanceID: "component", Artifact: artifact, Registrations: []RegistrationIdentity{
				{ID: "handler", Contract: "contract", Version: "1", Order: 1, Scope: global, Kind: HandlerNotification},
				{ID: "handler", Contract: "contract", Version: "1", Order: 2, Scope: global, Kind: HandlerNotification},
			}}},
		},
		"tool hashes": {
			Tools: []ToolPlanIdentity{
				{InstanceID: "component", Artifact: artifact, Name: "tool", RegistrationID: "tool", Scope: global, SchemaHash: "schema-a", ExecutorHash: "executor-a"},
				{InstanceID: "component", Artifact: artifact, Name: "tool", RegistrationID: "tool", Scope: global, SchemaHash: "schema-b", ExecutorHash: "executor-b"},
			},
		},
		"prompt order": {
			Prompts: []PromptPlanIdentity{
				{InstanceID: "component", Artifact: artifact, Name: "prompt", RegistrationID: "prompt", Scope: global, Order: 1},
				{InstanceID: "component", Artifact: artifact, Name: "prompt", RegistrationID: "prompt", Scope: global, Order: 2},
			},
		},
		"guard order": {
			Guards: []GuardPlanIdentity{
				{InstanceID: "component", Artifact: artifact, RegistrationID: "guard", Scope: global, Order: 1},
				{InstanceID: "component", Artifact: artifact, RegistrationID: "guard", Scope: global, Order: 2},
			},
		},
		"restriction rules": {
			Restrictions: []RestrictionPlanIdentity{
				{InstanceID: "component", Artifact: artifact, RegistrationID: "restriction", Scope: global, RulesHash: "rules-a"},
				{InstanceID: "component", Artifact: artifact, RegistrationID: "restriction", Scope: global, RulesHash: "rules-b"},
			},
		},
	}
	for name, descriptor := range tests {
		t.Run(name, func(t *testing.T) {
			descriptor.SchemaVersion = ExtensionPlanSchemaVersion
			if _, err := FingerprintExtensionPlan(descriptor); err == nil {
				t.Fatal("FingerprintExtensionPlan accepted duplicate logical identities")
			}
		})
	}
}

func testArtifact(suffix string) extension.Artifact {
	return extension.Artifact{Name: "artifact-" + suffix, Version: "1", Hash: "hash-" + suffix, ConfigHash: "config-" + suffix, SourceKind: extension.SourceNative}
}
