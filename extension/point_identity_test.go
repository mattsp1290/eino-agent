package extension

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestPointDefinitionCopiesMountAndDispatch(t *testing.T) {
	point := NewNotification[testPayload](Contract{ID: "test/point-copy", Version: "1"}, clonePayload)
	copyOfPoint := point
	registry := newTestRegistry(nil, point)
	called := 0
	for _, id := range []string{"point-copy-a", "point-copy-b"} {
		_, err := registry.Mount(context.Background(), testComponent(id), InstallerFunc(func(_ context.Context, registrar Registrar) error {
			return On(registrar, copyOfPoint, Registration{ID: "handler", Scope: GlobalScope()}, func(context.Context, testPayload) error {
				called++
				return nil
			})
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	plan, err := registry.Snapshot(GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	Notify(plan, context.Background(), point, testPayload{})
	if err := plan.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if called != 2 {
		t.Fatalf("canonical point calls = %d, want 2", called)
	}
}

func TestIndependentPointDefinitionCannotDispatchOrReplaceAuthority(t *testing.T) {
	contract := Contract{ID: "test/point-authority", Version: "1"}
	canonical := NewNotification[testPayload](contract, clonePayload)
	alternate := NewNotification[testPayload](contract, nil)
	registry := newTestRegistry(nil, canonical)
	called := 0
	mount, err := registry.Mount(context.Background(), testComponent("point-authority"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return On(registrar, canonical, Registration{ID: "handler", Scope: GlobalScope()}, func(context.Context, testPayload) error { called++; return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	Notify(plan, context.Background(), alternate, testPayload{})
	if err := plan.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan.Release()
	if called != 0 {
		t.Fatalf("alternate definition dispatched %d callbacks", called)
	}
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err = registry.Mount(context.Background(), testComponent("point-replacement"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return On(registrar, alternate, Registration{ID: "handler", Scope: GlobalScope()}, func(context.Context, testPayload) error { return nil })
	}))
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("alternate remount error = %v, want ErrInvalidContract", err)
	}
}

func TestConflictingPointDefinitionsInCandidateAreRejected(t *testing.T) {
	contract := Contract{ID: "test/candidate-conflict", Version: "1"}
	first := NewNotification[int](contract, nil)
	second := NewNotification[int](contract, nil)
	registry := newTestRegistry(nil, first)
	prepared, err := registry.PrepareMount(context.Background(), testComponent("candidate-conflict"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		if err := On(registrar, first, Registration{ID: "first", Scope: GlobalScope()}, func(context.Context, int) error { return nil }); err != nil {
			return err
		}
		return On(registrar, second, Registration{ID: "second", Scope: GlobalScope()}, func(context.Context, int) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.CommitMount(prepared); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("conflicting candidate error = %v, want ErrInvalidContract", err)
	}
}

func TestRegistryCatalogOwnsPointBeforeCommits(t *testing.T) {
	contract := Contract{ID: "test/rejected-authority", Version: "1"}
	rejected := NewNotification[int](contract, nil)
	accepted := NewNotification[int](contract, nil)
	registry, err := NewRegistry[struct{}](nil, accepted)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := registry.PrepareMount(context.Background(), testComponent("rejected-authority"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return On(registrar, rejected, Registration{ID: "handler", Scope: GlobalScope()}, func(context.Context, int) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.CommitMount(prepared, struct{}{}, nil, nil); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("undeclared point commit error = %v", err)
	}
	if err := prepared.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	prepared, err = registry.PrepareMount(context.Background(), testComponent("accepted-authority"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return On(registrar, accepted, Registration{ID: "handler", Scope: GlobalScope()}, func(context.Context, int) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.CommitMount(prepared, struct{}{}, nil, nil); err != nil {
		t.Fatalf("accepted point could not bind after rejection: %v", err)
	}
}

func TestConcurrentPointDefinitionCommitHasSingleWinner(t *testing.T) {
	contract := Contract{ID: "test/concurrent-authority", Version: "1"}
	definitions := []Notification[int]{NewNotification[int](contract, nil), NewNotification[int](contract, nil)}
	registry, err := NewRegistry[struct{}](nil, definitions[0])
	if err != nil {
		t.Fatal(err)
	}
	prepared := make([]*PreparedMount[struct{}], len(definitions))
	for index, point := range definitions {
		var err error
		prepared[index], err = registry.PrepareMount(context.Background(), testComponent("concurrent-authority-"+string(rune('a'+index))), InstallerFunc(func(_ context.Context, registrar Registrar) error {
			return On(registrar, point, Registration{ID: "handler", Scope: GlobalScope()}, func(context.Context, int) error { return nil })
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	var wait sync.WaitGroup
	errs := make([]error, len(prepared))
	for index := range prepared {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			_, errs[index] = registry.CommitMount(prepared[index], struct{}{}, nil, nil)
		}(index)
	}
	wait.Wait()
	successes := 0
	for _, err := range errs {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrInvalidContract) {
			t.Fatalf("competing commit error = %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful competing commits = %d, want 1", successes)
	}
}

func TestZeroValuePointIsRejectedOrInert(t *testing.T) {
	registry := newTestRegistry(nil)
	_, err := registry.Mount(context.Background(), testComponent("zero-point"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return On(registrar, Notification[int]{}, Registration{ID: "handler", Scope: GlobalScope()}, func(context.Context, int) error { return nil })
	}))
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("zero point mount error = %v, want ErrInvalidContract", err)
	}
	Notify[int](nil, context.Background(), Notification[int]{}, 1)
}

func TestRegistryRejectsConflictingCatalogDefinitions(t *testing.T) {
	contract := Contract{ID: "test/catalog-conflict", Version: "1"}
	first := NewNotification[int](contract, nil)
	second := NewNotification[int](contract, nil)
	if _, err := NewRegistry[struct{}](nil, first, second); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("NewRegistry conflict = %v, want ErrInvalidContract", err)
	}
}
