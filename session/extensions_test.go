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
