package extension

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testPayload struct {
	Protected string
	Values    []string
}

var (
	testNotice = NewNotification(Contract{ID: "test/notice", Version: "1"}, NotificationContained, clonePayload)
	testAround = NewRequiredInterceptor(Contract{ID: "test/around", Version: "1"}, clonePayload, func(original, candidate testPayload) error {
		if original.Protected != candidate.Protected {
			return ErrProtectedMutation
		}
		return nil
	}, func(output string) error {
		if output == "" {
			return errors.New("empty output")
		}
		return nil
	})
)

func clonePayload(input testPayload) testPayload {
	input.Values = append([]string(nil), input.Values...)
	return input
}

func testComponent(id string) Component {
	return Component{InstanceID: id, Artifact: Artifact{Name: "tests", Version: "1", Hash: "artifact-hash", ConfigHash: "config-hash", SourceKind: SourceNative}}
}

func spec(instance, id string, order int, scope Scope) Registration {
	return Registration{InstanceID: instance, ID: id, Order: order, Scope: scope}
}

func TestMountIsAtomicAndRollsBackEffects(t *testing.T) {
	registry := NewRegistry(nil)
	var cleanup []int
	_, err := registry.Mount(context.Background(), testComponent("failed"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		_ = registrar.Defer(func(context.Context) error { cleanup = append(cleanup, 1); return nil })
		_ = registrar.Defer(func(context.Context) error { cleanup = append(cleanup, 2); return nil })
		if err := On(registrar, testNotice, spec("failed", "notice", 0, GlobalScope()), func(context.Context, testPayload) error { return nil }); err != nil {
			return err
		}
		return errors.New("install failed")
	}))
	if err == nil || !reflect.DeepEqual(cleanup, []int{2, 1}) || len(registry.Diagnostics()) != 0 {
		t.Fatalf("Mount = %v, cleanup=%v diagnostics=%v", err, cleanup, registry.Diagnostics())
	}
}

func TestPreparedMountIsInvisibleUntilCommitAndRollbackCleansOnce(t *testing.T) {
	registry := NewRegistry(nil)
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
	if len(plan.entries) != 0 || len(registry.Diagnostics()) != 0 {
		t.Fatalf("prepared mount published early: entries=%d diagnostics=%v", len(plan.entries), registry.Diagnostics())
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
	registry := NewRegistry(nil)
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
	registry := NewRegistry(nil)
	var replacement *Mount
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
	if failures := Notify(plan, context.Background(), testNotice, testPayload{}); len(failures) != 0 {
		t.Fatalf("closing replacement from old callback failed: %v", failures)
	}
	plan.Release()
	if err := old.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestScopedOrderingDefensiveCopyAndContainedFailures(t *testing.T) {
	var diagnostics atomic.Int32
	registry := NewRegistry(ReporterFunc(func(context.Context, Diagnostic) { diagnostics.Add(1) }))
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
	if failures := Notify(plan, context.Background(), testNotice, payload); len(failures) != 0 {
		t.Fatalf("contained failures = %#v", failures)
	}
	if !reflect.DeepEqual(sequence, []string{"a", "z", "b"}) || payload.Values[0] != "original" || diagnostics.Load() != 1 {
		t.Fatalf("sequence=%v payload=%v diagnostics=%d", sequence, payload, diagnostics.Load())
	}
}

func TestSnapshotAcceptsOpaqueSessionTargetKeys(t *testing.T) {
	registry := NewRegistry(nil)
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
		if len(plan.Diagnostics()) != 1 {
			t.Fatalf("Snapshot(%q) diagnostics = %#v", key, plan.Diagnostics())
		}
		plan.Release()
	}
	if _, err := registry.Snapshot(SessionScope("")); !errors.Is(err, ErrInvalidRegistration) {
		t.Fatalf("empty session target error = %v, want ErrInvalidRegistration", err)
	}
}

func TestRegistrationAcceptsOpaqueSessionScopeKeys(t *testing.T) {
	keys := []string{"user@example.com", "dXNlcg==", "  spaced session  ", strings.Repeat("x", 300) + "=="}
	registry := NewRegistry(nil)
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
		if failures := Notify(plan, context.Background(), testNotice, testPayload{}); len(failures) != 0 {
			t.Fatalf("Notify(%q) = %v", key, failures)
		}
		plan.Release()
		if counts[index] != 1 {
			t.Fatalf("callback count for %q = %d, want 1", key, counts[index])
		}
	}
}

func TestInterceptorOnionProtectedInputAndNextGuard(t *testing.T) {
	registry := NewRegistry(nil)
	var sequence []string
	for index, id := range []string{"outer", "inner"} {
		id, order := id, index
		_, err := registry.Mount(context.Background(), testComponent(id), InstallerFunc(func(_ context.Context, registrar Registrar) error {
			return Use(registrar, testAround, spec(id, id, order, GlobalScope()), func(ctx context.Context, input testPayload, next Next[testPayload, string]) (string, error) {
				sequence = append(sequence, id+"-before")
				output, err := next(ctx, input)
				sequence = append(sequence, id+"-after")
				return output, err
			})
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	output, err := Invoke(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context, testPayload) (string, error) {
		sequence = append(sequence, "terminal")
		return "ok", nil
	})
	if err != nil || output != "ok" || !reflect.DeepEqual(sequence, []string{"outer-before", "inner-before", "terminal", "inner-after", "outer-after"}) {
		t.Fatalf("Invoke = %q, %v; sequence=%v", output, err, sequence)
	}

	badRegistry := NewRegistry(nil)
	_, _ = badRegistry.Mount(context.Background(), testComponent("bad"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return Use(registrar, testAround, spec("bad", "bad", 0, GlobalScope()), func(ctx context.Context, input testPayload, next Next[testPayload, string]) (string, error) {
			if _, err := next(ctx, input); err != nil {
				return "", err
			}
			return next(ctx, input)
		})
	}))
	badPlan, _ := badRegistry.Snapshot(GlobalScope())
	defer badPlan.Release()
	if _, err := Invoke(badPlan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context, testPayload) (string, error) { return "ok", nil }); !errors.Is(err, ErrNextCalledTwice) {
		t.Fatalf("double next error = %v", err)
	}
}

func TestProtectedMutationAndRequiredDelegation(t *testing.T) {
	for _, test := range []struct {
		name string
		fn   Around[testPayload, string]
		want error
	}{
		{name: "mutation", want: ErrProtectedMutation, fn: func(ctx context.Context, input testPayload, next Next[testPayload, string]) (string, error) {
			input.Protected = "changed"
			return next(ctx, input)
		}},
		{name: "no delegation", want: ErrNextNotCalled, fn: func(context.Context, testPayload, Next[testPayload, string]) (string, error) {
			return "fabricated", nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewRegistry(nil)
			_, _ = registry.Mount(context.Background(), testComponent("instance"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
				return Use(registrar, testAround, spec("instance", "handler", 0, GlobalScope()), test.fn)
			}))
			plan, _ := registry.Snapshot(GlobalScope())
			defer plan.Release()
			_, err := Invoke(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context, testPayload) (string, error) { return "ok", nil })
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestRequiredDelegationCannotSwallowDelegatedFailure(t *testing.T) {
	delegatedErr := errors.New("delegated failure")
	registry := NewRegistry(nil)
	component := testComponent("swallow")
	_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return Use(registrar, testAround, spec(component.InstanceID, "swallow", 0, GlobalScope()), func(ctx context.Context, input testPayload, next Next[testPayload, string]) (string, error) {
			_, _ = next(ctx, input)
			return "fabricated", nil
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
	output, err := Invoke(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context, testPayload) (string, error) {
		return "", delegatedErr
	})
	if output != "" || !errors.Is(err, delegatedErr) {
		t.Fatalf("Invoke = %q, %v; want delegated failure", output, err)
	}
}

func TestInvokeJoinsInFlightDelegation(t *testing.T) {
	for _, test := range []struct {
		name        string
		terminalErr error
		callbackErr error
		wantErr     error
	}{
		{name: "success"},
		{name: "delegated failure", terminalErr: errors.New("delegated failure"), wantErr: errors.New("delegated failure")},
		{name: "callback failure", callbackErr: errors.New("callback failure"), wantErr: errors.New("callback failure")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.terminalErr != nil {
				test.wantErr = test.terminalErr
			}
			if test.callbackErr != nil {
				test.wantErr = test.callbackErr
			}
			registry := NewRegistry(nil)
			component := testComponent("async-" + strings.ReplaceAll(test.name, " ", "-"))
			terminalStarted := make(chan struct{})
			releaseTerminal := make(chan struct{})
			_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar Registrar) error {
				return Use(registrar, testAround, spec(component.InstanceID, "async", 0, GlobalScope()), func(ctx context.Context, input testPayload, next Next[testPayload, string]) (string, error) {
					go func() { _, _ = next(ctx, input) }()
					<-terminalStarted
					if test.callbackErr != nil {
						return "", test.callbackErr
					}
					return "wrapped", nil
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
			returned := make(chan error, 1)
			go func() {
				_, invokeErr := Invoke(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context, testPayload) (string, error) {
					close(terminalStarted)
					<-releaseTerminal
					return "delegated", test.terminalErr
				})
				returned <- invokeErr
			}()
			select {
			case invokeErr := <-returned:
				t.Fatalf("Invoke returned before delegation completed: %v", invokeErr)
			case <-time.After(20 * time.Millisecond):
			}
			close(releaseTerminal)
			invokeErr := <-returned
			if test.wantErr == nil && invokeErr != nil {
				t.Fatalf("Invoke error = %v", invokeErr)
			}
			if test.wantErr != nil && !errors.Is(invokeErr, test.wantErr) {
				t.Fatalf("Invoke error = %v, want %v", invokeErr, test.wantErr)
			}
		})
	}
}

func TestInvokeRejectsDelegationStartedAfterInterceptorReturns(t *testing.T) {
	registry := NewRegistry(nil)
	component := testComponent("late-next")
	savedNext := make(chan Next[testPayload, string], 1)
	_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return Use(registrar, testAround, spec(component.InstanceID, "late", 0, GlobalScope()), func(_ context.Context, _ testPayload, next Next[testPayload, string]) (string, error) {
			savedNext <- next
			return "fabricated", nil
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
	var terminalCalls atomic.Int32
	if _, err := Invoke(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context, testPayload) (string, error) {
		terminalCalls.Add(1)
		return "terminal", nil
	}); !errors.Is(err, ErrNextNotCalled) {
		t.Fatalf("Invoke error = %v, want ErrNextNotCalled", err)
	}
	if _, err := (<-savedNext)(context.Background(), testPayload{Protected: "fixed"}); !errors.Is(err, ErrNextNotCalled) {
		t.Fatalf("late next error = %v, want ErrNextNotCalled", err)
	}
	if calls := terminalCalls.Load(); calls != 0 {
		t.Fatalf("terminal calls = %d, want 0", calls)
	}
}

func TestInvokeConcurrentDuplicateDelegationExecutesTerminalOnce(t *testing.T) {
	registry := NewRegistry(nil)
	component := testComponent("duplicate-next")
	_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return Use(registrar, testAround, spec(component.InstanceID, "duplicate", 0, GlobalScope()), func(ctx context.Context, input testPayload, next Next[testPayload, string]) (string, error) {
			start := make(chan struct{})
			errs := make(chan error, 2)
			for range 2 {
				go func() {
					<-start
					_, nextErr := next(ctx, input)
					errs <- nextErr
				}()
			}
			close(start)
			<-errs
			<-errs
			return "wrapped", nil
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
	var terminalCalls atomic.Int32
	if _, err := Invoke(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context, testPayload) (string, error) {
		terminalCalls.Add(1)
		return "terminal", nil
	}); !errors.Is(err, ErrNextCalledTwice) {
		t.Fatalf("Invoke error = %v, want ErrNextCalledTwice", err)
	}
	if calls := terminalCalls.Load(); calls != 1 {
		t.Fatalf("terminal calls = %d, want 1", calls)
	}
}

func TestInterceptorPropagatesTightenedNextContext(t *testing.T) {
	registry := NewRegistry(nil)
	component := testComponent("context")
	_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return Use(registrar, testAround, spec(component.InstanceID, "deadline", 0, GlobalScope()), func(ctx context.Context, input testPayload, next Next[testPayload, string]) (string, error) {
			tightened, cancel := context.WithCancel(ctx)
			cancel()
			return next(tightened, input)
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	_, err = Invoke(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(ctx context.Context, _ testPayload) (string, error) {
		return "", ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("terminal context error = %v", err)
	}
}

func TestDeactivateSnapshotIsolationAndCloseDrain(t *testing.T) {
	registry := NewRegistry(nil)
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

func TestExplicitLeaseScopesRespectTargetAndInstanceFilters(t *testing.T) {
	newLeaseMount := func(t *testing.T, registry *Registry, instance string) *Mount {
		t.Helper()
		mount, err := registry.Mount(context.Background(), testComponent(instance), InstallerFunc(func(_ context.Context, registrar Registrar) error {
			return registrar.Lease(SessionScope("session-a"))
		}))
		if err != nil {
			t.Fatal(err)
		}
		return mount
	}

	t.Run("included instance drains", func(t *testing.T) {
		registry := NewRegistry(nil)
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
				registry := NewRegistry(nil)
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

func TestInterceptorErrorHasBoundedPublicTextAndLocalCause(t *testing.T) {
	registry := NewRegistry(nil)
	secret := errors.New("credential-sentinel-do-not-persist")
	component := testComponent("bounded-error")
	_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return Use(registrar, testAround, spec(component.InstanceID, "failure", 0, GlobalScope()), func(context.Context, testPayload, Next[testPayload, string]) (string, error) {
			return "", secret
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
	_, err = Invoke(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context, testPayload) (string, error) {
		return "unused", nil
	})
	var callback *CallbackError
	if !errors.As(err, &callback) || !errors.Is(err, secret) {
		t.Fatalf("callback error = %v", err)
	}
	if strings.Contains(err.Error(), "credential-sentinel") {
		t.Fatalf("public callback error leaked cause: %q", err)
	}
}

func BenchmarkSnapshot(b *testing.B) {
	registry := NewRegistry(nil)
	_, _ = registry.Mount(context.Background(), testComponent("bench"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		for index := 0; index < 10; index++ {
			if err := On(registrar, testNotice, spec("bench", string(rune('a'+index)), index, GlobalScope()), func(context.Context, testPayload) error { return nil }); err != nil {
				return err
			}
		}
		return nil
	}))
	b.ResetTimer()
	for range b.N {
		plan, _ := registry.Snapshot(GlobalScope())
		plan.Release()
	}
}

func BenchmarkNotifyZero(b *testing.B) {
	plan := &Plan{}
	for range b.N {
		_ = Notify(plan, context.Background(), testNotice, testPayload{})
	}
}

func BenchmarkNotifyTen(b *testing.B) {
	registry := NewRegistry(nil)
	_, _ = registry.Mount(context.Background(), testComponent("bench"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		for index := 0; index < 10; index++ {
			if err := On(registrar, testNotice, spec("bench", string(rune('a'+index)), index, GlobalScope()), func(context.Context, testPayload) error { return nil }); err != nil {
				return err
			}
		}
		return nil
	}))
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	b.ResetTimer()
	for range b.N {
		_ = Notify(plan, context.Background(), testNotice, testPayload{})
	}
}

func BenchmarkInvokeTen(b *testing.B) {
	registry := NewRegistry(nil)
	for index := 0; index < 10; index++ {
		id := string(rune('a' + index))
		_, _ = registry.Mount(context.Background(), testComponent(id), InstallerFunc(func(_ context.Context, registrar Registrar) error {
			return Use(registrar, testAround, spec(id, id, index, GlobalScope()), func(ctx context.Context, input testPayload, next Next[testPayload, string]) (string, error) {
				return next(ctx, input)
			})
		}))
	}
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	b.ResetTimer()
	for range b.N {
		_, _ = Invoke(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context, testPayload) (string, error) { return "ok", nil })
	}
}

func BenchmarkConcurrentMountSnapshotClose(b *testing.B) {
	registry := NewRegistry(nil)
	b.RunParallel(func(parallel *testing.PB) {
		var sequence atomic.Uint64
		for parallel.Next() {
			id := "bench-" + strconv.FormatUint(sequence.Add(1), 10)
			mount, err := registry.Mount(context.Background(), testComponent(id), InstallerFunc(func(_ context.Context, registrar Registrar) error {
				return On(registrar, testNotice, spec(id, "notice", 0, GlobalScope()), func(context.Context, testPayload) error { return nil })
			}))
			if err != nil {
				continue
			}
			plan, _ := registry.Snapshot(GlobalScope())
			plan.Release()
			_ = mount.Close(context.Background())
		}
	})
}

func FuzzSessionScope(f *testing.F) {
	f.Add("session-1", "session-1")
	f.Add("session-1", "session-2")
	f.Fuzz(func(t *testing.T, registered, target string) {
		if registered == "" || target == "" {
			t.Skip()
		}
		applies := scopeApplies(SessionScope(registered), SessionScope(target))
		if applies != (registered == target) {
			t.Fatalf("scope result = %t", applies)
		}
	})
}
