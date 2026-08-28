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
	if descriptorCapabilityCount(descriptor) != 0 {
		t.Fatalf("registry published unexpected plan: %#v", descriptor)
	}
}

func descriptorCapabilityCount(descriptor session.ExtensionPlanDescriptor) int {
	count := 0
	for _, component := range descriptor.Components {
		count += len(component.Handlers) + len(component.Tools) + len(component.Prompts) + len(component.Guards) + len(component.Restrictions)
	}
	return count
}

func descriptorComponent(descriptor session.ExtensionPlanDescriptor, instanceID string) *session.ComponentPlan {
	for index := range descriptor.Components {
		if descriptor.Components[index].InstanceID == instanceID {
			return &descriptor.Components[index]
		}
	}
	return nil
}
