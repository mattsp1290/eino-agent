package session

import "testing"

func TestExtensionPlanFingerprintCanonicalizesFullyTiedEntriesAndRegistrations(t *testing.T) {
	descriptor := ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion, Entries: []ExtensionPlanEntry{
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
