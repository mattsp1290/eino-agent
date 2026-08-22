package session

import "testing"

func TestExtensionPlanFingerprintOrderSchemaCompatibility(t *testing.T) {
	base := ExtensionPlanDescriptor{Mode: PlanStrict, Entries: []ExtensionPlanEntry{{
		InstanceID: "component", Kind: ExtensionHandlers, Required: true, CapabilityID: "callback",
	}}}
	v1 := base.Clone()
	v1.SchemaVersion = 1
	v1WithIgnoredOrder := v1.Clone()
	v1WithIgnoredOrder.Entries[0].Order = 42
	first, err := FingerprintExtensionPlan(v1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := FingerprintExtensionPlan(v1WithIgnoredOrder)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("schema-v1 fingerprint changed for ignored order: %s != %s", first, second)
	}

	v2 := base.Clone()
	v2.SchemaVersion = ExtensionPlanSchemaVersion
	v2WithOrder := v2.Clone()
	v2WithOrder.Entries[0].Order = 42
	first, err = FingerprintExtensionPlan(v2)
	if err != nil {
		t.Fatal(err)
	}
	second, err = FingerprintExtensionPlan(v2WithOrder)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("schema-v2 fingerprint did not include capability order")
	}
}

func TestExtensionPlanFingerprintCanonicalizesFullyTiedEntriesAndRegistrations(t *testing.T) {
	descriptor := ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion, Mode: PlanStrict, Entries: []ExtensionPlanEntry{
		{
			InstanceID: "component", Kind: ExtensionGuard, CapabilityID: "guard", Required: true,
			Scope: ExtensionScope{Kind: "session", Key: "session-b"}, Order: 20,
			Artifact:   ArtifactIdentity{Name: "artifact", Version: "2", Hash: "hash-b", ConfigHash: "config-b", SourceKind: "native"},
			SchemaHash: "schema-b", ExecutorHash: "executor-b",
		},
		{
			InstanceID: "component", Kind: ExtensionGuard, CapabilityID: "guard", Required: true,
			Scope: ExtensionScope{Kind: "global"}, Order: 10,
			Artifact:   ArtifactIdentity{Name: "artifact", Version: "1", Hash: "hash-a", ConfigHash: "config-a", SourceKind: "native"},
			SchemaHash: "schema-a", ExecutorHash: "executor-a",
		},
		{
			InstanceID: "component", Kind: ExtensionHandlers, Required: true,
			Registrations: []RegistrationIdentity{
				{ID: "same", Contract: "contract", Version: "2", Order: 0, Scope: ExtensionScope{Kind: "session", Key: "session-b"}},
				{ID: "same", Contract: "contract", Version: "1", Order: 0, Scope: ExtensionScope{Kind: "global"}},
			},
		},
	}}
	reordered := descriptor.Clone()
	reordered.Entries[0], reordered.Entries[2] = reordered.Entries[2], reordered.Entries[0]
	reordered.Entries[0].Registrations[0], reordered.Entries[0].Registrations[1] = reordered.Entries[0].Registrations[1], reordered.Entries[0].Registrations[0]

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
