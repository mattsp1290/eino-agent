//go:build windows

package einotools

import (
	"context"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-tools/catalog"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
)

func TestMountStandardPreservesUnsupportedPlatformError(t *testing.T) {
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	component := extension.Component{InstanceID: "windows", Artifact: extension.Artifact{
		Name: "eino-tools-standard", Version: "test", Hash: "adapter", ConfigHash: "catalog", SourceKind: extension.SourceNative,
	}}
	_, err = MountStandard(context.Background(), registry, component, Options{Scope: extension.GlobalScope()})
	if !errors.Is(err, catalog.ErrUnsupportedPlatform) {
		t.Fatalf("MountStandard error = %v, want ErrUnsupportedPlatform", err)
	}
	plan, planErr := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if planErr != nil {
		t.Fatal(planErr)
	}
	defer plan.Release()
	descriptor := plan.Descriptor()
	if len(descriptor.Components) != 0 {
		t.Fatalf("unsupported mount published plan: %#v", descriptor)
	}
}
