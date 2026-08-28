package composition

import (
	"context"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
)

func TestToolScopeResolverReceivesCanonicalCallbackContext(t *testing.T) {
	registry := NewRegistry(nil)
	var mount *Mount
	var closeErr error
	definition := definition("scope-aware", "ok")
	definition.Scope = func(ctx context.Context, scope runtime.ToolScopeContext) runtime.ToolScope {
		closeErr = mount.Close(ctx)
		return runtime.ToolScope{WorkspaceID: scope.WorkspaceID}
	}
	var err error
	mount, err = registry.Mount(context.Background(), component("scope-context-live"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "scope", Scope: extension.GlobalScope(), Definition: definition})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{WorkspaceID: "workspace"})
	if err != nil || len(resolved) != 1 || !errors.Is(closeErr, extension.ErrSelfClose) {
		t.Fatalf("ResolveTools = %#v, %v close=%v", resolved, err, closeErr)
	}
	plan.Release()
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
