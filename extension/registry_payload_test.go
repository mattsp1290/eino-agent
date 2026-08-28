package extension

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTypedCommitValidationIsAtomicAndRejectedPreparationRollsBack(t *testing.T) {
	t.Parallel()
	registry := NewRegistry[string](nil)
	first, err := registry.PrepareMount(context.Background(), testComponent("first-payload"), InstallerFunc(func(context.Context, Registrar) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	firstMount, err := registry.CommitMount(first, "first", []Scope{GlobalScope()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = firstMount.Close(context.Background()) }()

	cleaned := false
	second, err := registry.PrepareMount(context.Background(), testComponent("second-payload"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return registrar.Defer(func(ctx context.Context) error {
			if ctx.Err() != nil {
				t.Fatalf("rollback inherited cancellation: %v", ctx.Err())
			}
			cleaned = true
			return nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	rejected := errors.New("rejected payload")
	_, err = registry.CommitMount(second, "second", []Scope{GlobalScope()}, func(active []CommitValue[string], candidate CommitValue[string]) error {
		if len(active) != 1 || active[0].Value() != "first" || candidate.Value() != "second" {
			t.Fatalf("validator values = %#v candidate=%#v", active, candidate)
		}
		return rejected
	})
	if !errors.Is(err, rejected) {
		t.Fatalf("CommitMount error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := second.Rollback(canceled); err != nil || !cleaned {
		t.Fatalf("Rollback = %v cleaned=%t", err, cleaned)
	}

	snapshot, err := registry.Snapshot(GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	values := snapshot.Values()
	if len(values) != 1 || values[0].Component().InstanceID != "first-payload" || values[0].Value() != "first" {
		t.Fatalf("published values = %#v", values)
	}
}

func TestPrepareFailureCleanupIgnoresCancellation(t *testing.T) {
	t.Parallel()
	registry := NewRegistry[struct{}](nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cleaned := false
	_, err := registry.PrepareMount(ctx, testComponent("prepare-canceled"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		if err := registrar.Defer(func(cleanupCtx context.Context) error {
			if cleanupCtx.Err() != nil {
				t.Fatalf("cleanup inherited cancellation: %v", cleanupCtx.Err())
			}
			cleaned = true
			return nil
		}); err != nil {
			return err
		}
		return context.Canceled
	}))
	if !errors.Is(err, context.Canceled) || !cleaned {
		t.Fatalf("PrepareMount = %v cleaned=%t", err, cleaned)
	}
}

func TestCapabilityOnlySnapshotHasOneIdempotentLease(t *testing.T) {
	t.Parallel()
	registry := NewRegistry[string](nil)
	cleanups := 0
	prepared, err := registry.PrepareMount(context.Background(), testComponent("capability-only"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return registrar.Defer(func(context.Context) error { cleanups++; return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	mount, err := registry.CommitMount(prepared, "payload", []Scope{SessionScope("session")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := registry.Snapshot(SessionScope("session"))
	if err != nil {
		t.Fatal(err)
	}
	mount.Deactivate()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := mount.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close before release = %v", err)
	}
	snapshot.Release()
	snapshot.Release()
	snapshot.Dispatch().Release()
	if err := mount.Close(context.Background()); err != nil || cleanups != 1 {
		t.Fatalf("Close after repeated release = %v cleanups=%d", err, cleanups)
	}
}
