//go:build windows

package einotools

import (
	"context"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-tools/catalog"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/extension"
)

func TestMountStandardPreservesUnsupportedPlatformError(t *testing.T) {
	registry := composition.NewRegistry(nil)
	component := extension.Component{InstanceID: "windows", Artifact: extension.Artifact{
		Name: "eino-tools-standard", Version: "test", Hash: "adapter", ConfigHash: "catalog", SourceKind: extension.SourceNative,
	}}
	_, err := MountStandard(context.Background(), registry, component, Options{Scope: extension.GlobalScope()})
	if !errors.Is(err, catalog.ErrUnsupportedPlatform) {
		t.Fatalf("MountStandard error = %v, want ErrUnsupportedPlatform", err)
	}
	if diagnostics := registry.Diagnostics(); len(diagnostics.Components) != 0 || len(diagnostics.Tools) != 0 {
		t.Fatalf("unsupported mount published diagnostics: %#v", diagnostics)
	}
}
