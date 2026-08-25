package session

import "testing"

func TestExtensionPlanFingerprintCanonicalizesEntriesAndRegistrations(t *testing.T) {
	descriptor := ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion, Entries: []ExtensionPlanEntry{
		{InstanceID: "guard-session", Artifact: testArtifact("b"), Guard: &GuardPlanIdentity{RegistrationID: "guard", Scope: ExtensionScope{Kind: "session", Key: "session-b"}, Order: 20}},
		{InstanceID: "guard-global", Artifact: testArtifact("a"), Guard: &GuardPlanIdentity{RegistrationID: "guard", Scope: ExtensionScope{Kind: "global"}, Order: 10}},
		{InstanceID: "component", Artifact: testArtifact("handlers"), Handlers: &HandlerPlanIdentity{Registrations: []RegistrationIdentity{
			{ID: "same", Contract: "contract", Version: "2", Scope: ExtensionScope{Kind: "session", Key: "session-b"}, Kind: HandlerNotification},
			{ID: "same", Contract: "contract", Version: "1", Scope: ExtensionScope{Kind: "global"}, Kind: HandlerInterceptor},
		}}},
	}}
	reordered := descriptor.Clone()
	reordered.Entries[0], reordered.Entries[2] = reordered.Entries[2], reordered.Entries[0]
	reordered.Entries[0].Handlers.Registrations[0], reordered.Entries[0].Handlers.Registrations[1] = reordered.Entries[0].Handlers.Registrations[1], reordered.Entries[0].Handlers.Registrations[0]

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

func TestExtensionPlanRejectsMalformedTaggedIdentity(t *testing.T) {
	entry := ExtensionPlanEntry{InstanceID: "tool", Artifact: testArtifact("tool"), Tool: &ToolPlanIdentity{Name: "tool", RegistrationID: "registration", Scope: ExtensionScope{Kind: "global"}, SchemaHash: "schema", ExecutorHash: "executor"}}
	for name, mutate := range map[string]func(*ExtensionPlanEntry){
		"missing config hash": func(value *ExtensionPlanEntry) { value.Artifact.ConfigHash = "" },
		"invalid source":      func(value *ExtensionPlanEntry) { value.Artifact.SourceKind = "remote" },
		"multiple payloads": func(value *ExtensionPlanEntry) {
			value.Guard = &GuardPlanIdentity{RegistrationID: "guard", Scope: ExtensionScope{Kind: "global"}}
		},
		"missing executor": func(value *ExtensionPlanEntry) { value.Tool.ExecutorHash = "" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := entry.Clone()
			mutate(&candidate)
			if _, err := FingerprintExtensionPlan(ExtensionPlanDescriptor{SchemaVersion: ExtensionPlanSchemaVersion, Entries: []ExtensionPlanEntry{candidate}}); err == nil {
				t.Fatal("FingerprintExtensionPlan succeeded")
			}
		})
	}
}

func testArtifact(suffix string) ArtifactIdentity {
	return ArtifactIdentity{Name: "artifact-" + suffix, Version: "1", Hash: "hash-" + suffix, ConfigHash: "config-" + suffix, SourceKind: "native"}
}
