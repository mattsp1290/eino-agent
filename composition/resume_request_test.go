package composition

import (
	"context"
	"testing"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func resumeRequest(sessionID session.ID, descriptor session.ExtensionPlanDescriptor) runtime.ResumePlanRequest {
	return runtime.ResumePlanRequest{SessionID: sessionID, Descriptor: descriptor}
}

func assertRegistryEmpty(t *testing.T, registry *Registry) {
	t.Helper()
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	descriptor := plan.Descriptor()
	if len(descriptor.Handlers)+len(descriptor.Tools)+len(descriptor.Prompts)+len(descriptor.Guards)+len(descriptor.Restrictions) != 0 {
		t.Fatalf("registry published unexpected plan: %#v", descriptor)
	}
}
