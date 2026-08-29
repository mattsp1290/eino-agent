package extension

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRegistrarOwnsMountedIdentity(t *testing.T) {
	registry := newTestRegistry(nil)
	component := testComponent("canonical-instance")
	_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar Registrar) error {
		if got := registrar.InstanceID(); got != component.InstanceID {
			t.Fatalf("Registrar.InstanceID = %q, want %q", got, component.InstanceID)
		}
		return On(registrar, testNotice, Registration{ID: "notice", Scope: GlobalScope()}, func(context.Context, testPayload) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	handlers := plan.HandlerComponents()
	if len(handlers) != 1 || handlers[0].Component.InstanceID != component.InstanceID {
		t.Fatalf("plan handlers = %#v", handlers)
	}
}

func TestMountIsAtomicAndRollsBackEffects(t *testing.T) {
	registry := newTestRegistry(nil)
	var cleanup []int
	_, err := registry.Mount(context.Background(), testComponent("failed"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		_ = registrar.Defer(func(context.Context) error { cleanup = append(cleanup, 1); return nil })
		_ = registrar.Defer(func(context.Context) error { cleanup = append(cleanup, 2); return nil })
		if err := On(registrar, testNotice, spec("failed", "notice", 0, GlobalScope()), func(context.Context, testPayload) error { return nil }); err != nil {
			return err
		}
		return errors.New("install failed")
	}))
	if err == nil || !reflect.DeepEqual(cleanup, []int{2, 1}) || activeMountCount(registry) != 0 {
		t.Fatalf("Mount = %v, cleanup=%v active=%d", err, cleanup, activeMountCount(registry))
	}
}

func TestPreparedMountIsInvisibleUntilCommitAndRollbackCleansOnce(t *testing.T) {
	registry := newTestRegistry(nil)
	cleanups := 0
	prepared, err := registry.PrepareMount(context.Background(), testComponent("prepared"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		if err := registrar.Defer(func(context.Context) error { cleanups++; return nil }); err != nil {
			return err
		}
		return On(registrar, testNotice, spec("prepared", "notice", 0, GlobalScope()), func(context.Context, testPayload) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.entries) != 0 || activeMountCount(registry) != 0 {
		t.Fatalf("prepared mount published early: entries=%d active=%d", len(plan.entries), activeMountCount(registry))
	}
	plan.Release()
	if err := prepared.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Rollback(context.Background()); err != nil || cleanups != 1 {
		t.Fatalf("second rollback = %v cleanups=%d", err, cleanups)
	}
}

func TestPreparedMountCommitTransfersCleanupToMount(t *testing.T) {
	registry := newTestRegistry(nil)
	cleanups := 0
	prepared, err := registry.PrepareMount(context.Background(), testComponent("commit"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		if err := registrar.Defer(func(context.Context) error { cleanups++; return nil }); err != nil {
			return err
		}
		return On(registrar, testNotice, spec("commit", "notice", 0, GlobalScope()), func(context.Context, testPayload) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.CommitMount(prepared)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Rollback(context.Background()); err != nil || cleanups != 0 {
		t.Fatalf("rollback after commit = %v cleanups=%d", err, cleanups)
	}
	if err := mount.Close(context.Background()); err != nil || cleanups != 1 {
		t.Fatalf("close = %v cleanups=%d", err, cleanups)
	}
}

func TestCallbackIdentityDistinguishesReusedInstanceID(t *testing.T) {
	registry := newTestRegistry(nil)
	var replacement *Mount[struct{}]
	old, err := registry.Mount(context.Background(), testComponent("reused"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return On(registrar, testNotice, spec("reused", "old", 0, GlobalScope()), func(ctx context.Context, _ testPayload) error {
			return replacement.Close(ctx)
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	old.Deactivate()
	replacement, err = registry.Mount(context.Background(), testComponent("reused"), InstallerFunc(func(context.Context, Registrar) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	Notify(plan, context.Background(), testNotice, testPayload{})
	if err := plan.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan.Release()
	if err := old.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotAcceptsOpaqueSessionTargetKeys(t *testing.T) {
	registry := newTestRegistry(nil)
	component := testComponent("opaque-target")
	mount, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return On(registrar, testNotice, spec(component.InstanceID, "global", 0, GlobalScope()), func(context.Context, testPayload) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()

	for _, key := range []string{"user@example.com", "dXNlcg=="} {
		plan, err := registry.Snapshot(SessionScope(key))
		if err != nil {
			t.Fatalf("Snapshot(%q) = %v", key, err)
		}
		if len(plan.HandlerComponents()) != 1 {
			t.Fatalf("Snapshot(%q) handlers = %#v", key, plan.HandlerComponents())
		}
		plan.Release()
	}
	if _, err := registry.Snapshot(SessionScope("")); !errors.Is(err, ErrInvalidRegistration) {
		t.Fatalf("empty session target error = %v, want ErrInvalidRegistration", err)
	}
}

func TestRegistrationAcceptsOpaqueSessionScopeKeys(t *testing.T) {
	keys := []string{"user@example.com", "dXNlcg==", "  spaced session  ", strings.Repeat("x", 300) + "=="}
	registry := newTestRegistry(nil)
	counts := make([]int, len(keys))
	for index, key := range keys {
		component := testComponent("opaque-registration-" + strconv.Itoa(index))
		_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar Registrar) error {
			return On(registrar, testNotice, spec(component.InstanceID, "notice", 0, SessionScope(key)), func(context.Context, testPayload) error {
				counts[index]++
				return nil
			})
		}))
		if err != nil {
			t.Fatalf("Mount(%q) = %v", key, err)
		}
	}

	for index, key := range keys {
		plan, err := registry.Snapshot(SessionScope(key))
		if err != nil {
			t.Fatalf("Snapshot(%q) = %v", key, err)
		}
		Notify(plan, context.Background(), testNotice, testPayload{})
		if err := plan.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		plan.Release()
		if counts[index] != 1 {
			t.Fatalf("callback count for %q = %d, want 1", key, counts[index])
		}
	}
}

func TestDeactivateSnapshotIsolationAndCloseDrain(t *testing.T) {
	registry := newTestRegistry(nil)
	cleanup := make(chan struct{})
	mount, err := registry.Mount(context.Background(), testComponent("lease"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		if err := registrar.Defer(func(context.Context) error { close(cleanup); return nil }); err != nil {
			return err
		}
		return On(registrar, testNotice, spec("lease", "notice", 0, GlobalScope()), func(context.Context, testPayload) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := registry.Snapshot(GlobalScope())
	mount.Deactivate()
	newPlan, _ := registry.Snapshot(GlobalScope())
	if len(newPlan.entries) != 0 || len(plan.entries) != 1 {
		t.Fatalf("snapshot isolation old=%d new=%d", len(plan.entries), len(newPlan.entries))
	}
	newPlan.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := mount.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close before release = %v", err)
	}
	plan.Release()
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cleanup:
	default:
		t.Fatal("cleanup did not run")
	}
	if err := mount.Close(context.Background()); err != nil {
		t.Fatalf("idempotent close = %v", err)
	}
}

func TestCapabilitySelectionScopesRespectTargetAndInstanceFilters(t *testing.T) {
	newLeaseMount := func(t *testing.T, registry *testRegistry, instance string) *Mount[struct{}] {
		t.Helper()
		prepared, err := registry.PrepareMount(context.Background(), testComponent(instance), InstallerFunc(func(context.Context, Registrar) error { return nil }))
		if err != nil {
			t.Fatal(err)
		}
		mount, err := registry.Registry.CommitMount(prepared, struct{}{}, []Scope{SessionScope("session-a")}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return mount
	}

	t.Run("included instance drains", func(t *testing.T) {
		registry := newTestRegistry(nil)
		mount := newLeaseMount(t, registry, "included-lease")
		plan, err := registry.SnapshotInstances(SessionScope("session-a"), []string{"included-lease"})
		if err != nil {
			t.Fatal(err)
		}
		mount.Deactivate()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if err := mount.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close before included lease release = %v", err)
		}
		plan.Release()
		if err := mount.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("excluded instance and target do not drain", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			target Scope
			ids    []string
		}{
			{name: "instance", target: SessionScope("session-a"), ids: []string{"some-other-instance"}},
			{name: "target", target: SessionScope("session-b"), ids: []string{"excluded-lease"}},
		} {
			t.Run(test.name, func(t *testing.T) {
				registry := newTestRegistry(nil)
				mount := newLeaseMount(t, registry, "excluded-lease")
				plan, err := registry.SnapshotInstances(test.target, test.ids)
				if err != nil {
					t.Fatal(err)
				}
				defer plan.Release()
				mount.Deactivate()
				if err := mount.Close(context.Background()); err != nil {
					t.Fatalf("unrelated plan retained mount: %v", err)
				}
			})
		}
	})
}
