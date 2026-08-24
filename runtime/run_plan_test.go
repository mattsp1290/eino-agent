package runtime

import (
	"testing"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

// testRunPlanWithTools keeps older orchestration behavior tests focused on
// runtime execution while production plan-construction invariants are tested
// separately through NewRunPlan and composition.Registry.
func testRunPlanWithTools(registry ToolRegistry) *RunPlan {
	plan, err := NewRunPlan(RunPlanSpec{})
	if err != nil {
		panic(err)
	}
	plan.tools = registry
	return plan
}

func testRunPlanWithDispatch(dispatch *extension.Plan) *RunPlan {
	plan, err := NewRunPlan(RunPlanSpec{Dispatch: dispatch})
	if err != nil {
		panic(err)
	}
	return plan
}

func setTestTools(orchestrator *StreamingOrchestrator, registry ToolRegistry) {
	orchestrator.Plans = staticRunPlanProvider{plan: testRunPlanWithTools(registry)}
}

func strictToolDescriptor(t *testing.T) session.ExtensionPlanDescriptor {
	t.Helper()
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Entries: []session.ExtensionPlanEntry{{InstanceID: "tools", Kind: session.ExtensionTool, Required: true, CapabilityID: "echo/tool"}}}
	var err error
	descriptor.Fingerprint, err = session.FingerprintExtensionPlan(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}
