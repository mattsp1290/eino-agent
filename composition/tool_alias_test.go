package composition

import (
	"context"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
)

func TestRunPlanCompileRejectsAliasCollidingWithAnotherToolNameThroughRealMount(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	echo := definition("echo", "v1")
	other := definition("other", "v1")
	other.Aliases = []string{"echo"}
	mount, err := registry.Mount(context.Background(), component("alias-collision"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		if err := registrar.Tool(ToolRegistration{ID: "echo", Scope: extension.GlobalScope(), Definition: echo}); err != nil {
			return err
		}
		return registrar.Tool(ToolRegistration{ID: "other", Scope: extension.GlobalScope(), Definition: other})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	_, err = registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if !errors.Is(err, runtime.ErrExtensionPlanMismatch) {
		t.Fatalf("AcquireRunPlan error = %v, want ErrExtensionPlanMismatch", err)
	}
}

func TestRunPlanCompileAcceptsNonCollidingAliasAndResolvesThroughRealMount(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	echo := definition("echo", "v1")
	echo.Aliases = []string{"say"}
	mount, err := registry.Mount(context.Background(), component("alias-ok"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.Tool(ToolRegistration{ID: "echo", Scope: extension.GlobalScope(), Definition: echo})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	if canonical, ok := plan.ResolveToolName("say"); !ok || canonical != "echo" {
		t.Fatalf("ResolveToolName(say) = %q, %v", canonical, ok)
	}
}

func TestRegistrarToolSearchRejectsMoreThanOneActiveRegistration(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	mountA, err := registry.Mount(context.Background(), component("search-a"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.ToolSearch(ToolSearchRegistration{ID: "search", Scope: extension.GlobalScope(), Name: "tool_search"})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mountA.Close(context.Background()) }()
	mountB, err := registry.Mount(context.Background(), component("search-b"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.ToolSearch(ToolSearchRegistration{ID: "search", Scope: extension.GlobalScope(), Name: "tool_search"})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mountB.Close(context.Background()) }()
	_, err = registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if !errors.Is(err, runtime.ErrExtensionPlanMismatch) {
		t.Fatalf("AcquireRunPlan error = %v, want ErrExtensionPlanMismatch", err)
	}
}

func TestRegistrarToolSearchDefaultsNameAndIsExposedOnPlan(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.Mount(context.Background(), component("search-default"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.ToolSearch(ToolSearchRegistration{ID: "search", Scope: extension.GlobalScope()})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	search := plan.ToolSearch()
	if search == nil || search.Name != "tool_search" {
		t.Fatalf("plan.ToolSearch() = %#v", search)
	}
}

// TestStrictResumeRejectsRenamedToolSearchRegistration guards
// composition-search-reviewer I3: the configured tool-search name is sealed
// into ExtensionPlanDescriptor.ToolSearch (session/extensions.go), so a
// resume whose tool-search registration was renamed between the original
// run and the resume must fail with the standard plan-mismatch error
// instead of surfacing later as "tool_search unavailable" deep inside
// resume.
func TestStrictResumeRejectsRenamedToolSearchRegistration(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	firstMount, err := registry.Mount(context.Background(), component("search-rename"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.ToolSearch(ToolSearchRegistration{ID: "search", Scope: extension.GlobalScope(), Name: "tool_search"})
	}))
	if err != nil {
		t.Fatal(err)
	}
	first, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	persisted := first.Descriptor()
	if persisted.ToolSearch != "tool_search" {
		t.Fatalf("persisted.ToolSearch = %q, want %q", persisted.ToolSearch, "tool_search")
	}
	first.Release()
	if err := firstMount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	secondMount, err := registry.Mount(context.Background(), component("search-rename"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.ToolSearch(ToolSearchRegistration{ID: "search", Scope: extension.GlobalScope(), Name: "find_tools"})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = secondMount.Close(context.Background()) }()
	assertResumePlanDrift(t, registry, "session-a", persisted)
}

// TestStrictResumeRejectsRemovedToolSearchRegistration guards the sibling
// drift case: the tool-search registration is removed entirely between the
// original run and the resume (persisted.ToolSearch is non-empty but the
// live plan's is empty), which must also fail as a plan mismatch rather
// than silently resuming with tool search disabled.
func TestStrictResumeRejectsRemovedToolSearchRegistration(t *testing.T) {
	registry, err := NewRegistry(nil, compositionNotice)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.Mount(context.Background(), component("search-removed"), InstallerFunc(func(_ context.Context, registrar *Registrar) error {
		return registrar.ToolSearch(ToolSearchRegistration{ID: "search", Scope: extension.GlobalScope(), Name: "tool_search"})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	persisted := plan.Descriptor()
	if persisted.ToolSearch != "tool_search" {
		t.Fatalf("persisted.ToolSearch = %q, want %q", persisted.ToolSearch, "tool_search")
	}
	plan.Release()
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertResumePlanDrift(t, registry, "session-a", persisted)
}

func TestComposedToolSchemaHashChangesWithAliasesAndDeferred(t *testing.T) {
	base := definition("echo", "v1")
	withAlias := definition("echo", "v1")
	withAlias.Aliases = []string{"say"}
	withDeferred := definition("echo", "v1")
	withDeferred.Deferred = true

	baseHash, err := toolSchemaHash(base)
	if err != nil {
		t.Fatal(err)
	}
	aliasHash, err := toolSchemaHash(withAlias)
	if err != nil {
		t.Fatal(err)
	}
	deferredHash, err := toolSchemaHash(withDeferred)
	if err != nil {
		t.Fatal(err)
	}
	if baseHash == aliasHash {
		t.Fatal("schema hash unchanged when aliases changed")
	}
	if baseHash == deferredHash {
		t.Fatal("schema hash unchanged when deferred flag changed")
	}
}
