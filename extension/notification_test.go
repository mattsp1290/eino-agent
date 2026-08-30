package extension

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestScopedOrderingDefensiveCopyAndContainedFailures(t *testing.T) {
	var diagnostics atomic.Int32
	registry := newTestRegistry(ReporterFunc(func(context.Context, Diagnostic) { diagnostics.Add(1) }))
	var mu sync.Mutex
	var sequence []string
	for _, item := range []struct {
		instance string
		id       string
		order    int
		scope    Scope
		fail     bool
	}{
		{instance: "session", id: "b", order: 0, scope: SessionScope("session-1")},
		{instance: "global-z", id: "z", order: 0, scope: GlobalScope(), fail: true},
		{instance: "global-a", id: "a", order: 0, scope: GlobalScope()},
	} {
		item := item
		_, err := registry.Mount(context.Background(), testComponent(item.instance), InstallerFunc(func(_ context.Context, registrar Registrar) error {
			return On(registrar, testNotice, spec(item.instance, item.id, item.order, item.scope), func(_ context.Context, payload testPayload) error {
				payload.Values[0] = item.id
				mu.Lock()
				sequence = append(sequence, item.id)
				mu.Unlock()
				if item.fail {
					return errors.New("SECRET callback detail")
				}
				return nil
			})
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	plan, err := registry.Snapshot(SessionScope("session-1"))
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	payload := testPayload{Protected: "fixed", Values: []string{"original"}}
	Notify(plan, context.Background(), testNotice, payload)
	if err := plan.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sequence, []string{"a", "z", "b"}) || payload.Values[0] != "original" || diagnostics.Load() != 1 {
		t.Fatalf("sequence=%v payload=%v diagnostics=%d", sequence, payload, diagnostics.Load())
	}
}

func TestNotifyReportsEveryFailureAndContinues(t *testing.T) {
	const failureCount = maxReportedFailures + 5
	var diagnostics atomic.Int32
	var completed atomic.Int32
	registry := newTestRegistry(ReporterFunc(func(context.Context, Diagnostic) { diagnostics.Add(1) }))
	_, err := registry.Mount(context.Background(), testComponent("many-failures"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		for index := 0; index < failureCount; index++ {
			id := fmt.Sprintf("failure-%d", index)
			if err := On(registrar, testNotice, spec("many-failures", id, index, GlobalScope()), func(context.Context, testPayload) error {
				return errors.New("observer failed")
			}); err != nil {
				return err
			}
		}
		return On(registrar, testNotice, spec("many-failures", "tail", failureCount, GlobalScope()), func(context.Context, testPayload) error {
			completed.Add(1)
			return nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()

	Notify(plan, context.Background(), testNotice, testPayload{})
	if err := plan.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := diagnostics.Load(); got != failureCount {
		t.Fatalf("reported failures = %d, want %d", got, failureCount)
	}
	if completed.Load() != 1 {
		t.Fatal("notification stopped before the final observer")
	}
}

func TestBlockedNotificationRetainsOnlyItsOwnMount(t *testing.T) {
	registry := newTestRegistry(nil)
	started := make(chan struct{})
	release := make(chan struct{})
	mountA, err := registry.Mount(context.Background(), testComponent("blocked-observer"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return On(registrar, testNotice, Registration{ID: "blocked", Scope: GlobalScope()}, func(context.Context, testPayload) error {
			close(started)
			<-release
			return nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	mountB, err := registry.Mount(context.Background(), testComponent("unrelated-around"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return OnAround(registrar, testAround, Registration{ID: "around", Scope: GlobalScope()}, func(ctx context.Context, _ testPayload, proceed Proceed) error {
			return proceed(ctx)
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	Notify(plan, context.Background(), testNotice, testPayload{})
	<-started
	plan.Release()
	if err := mountB.Close(context.Background()); err != nil {
		t.Fatalf("unrelated mount close = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := mountA.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked mount close = %v, want deadline", err)
	}
	close(release)
	if err := mountA.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestBlockedReporterCannotBlockNotifyOrPlanRelease(t *testing.T) {
	reporterStarted := make(chan struct{})
	releaseReporter := make(chan struct{})
	registry := newTestRegistry(ReporterFunc(func(context.Context, Diagnostic) {
		close(reporterStarted)
		<-releaseReporter
	}))
	mount, err := registry.Mount(context.Background(), testComponent("blocked-reporter"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return On(registrar, testNotice, Registration{ID: "fails", Scope: GlobalScope()}, func(context.Context, testPayload) error {
			return errors.New("observer failure")
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	Notify(plan, context.Background(), testNotice, testPayload{})
	<-reporterStarted
	released := make(chan struct{})
	go func() {
		plan.Release()
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("Plan.Release blocked on reporter")
	}
	close(releaseReporter)
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
