package extension

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequiredAroundRejectsConcurrentProceed(t *testing.T) {
	registry := newTestRegistry(nil)
	_, err := registry.Mount(context.Background(), testComponent("concurrent-next"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return OnAround(registrar, testAround, spec("concurrent-next", "around", 0, GlobalScope()), func(ctx context.Context, _ testPayload, proceed Proceed) error {
			start := make(chan struct{})
			results := make(chan error, 2)
			for range 2 {
				go func() {
					<-start
					results <- proceed(ctx)
				}()
			}
			close(start)
			first, second := <-results, <-results
			return errors.Join(first, second)
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	_, err = InvokeAround(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context) (string, error) {
		return "ok", nil
	})
	if !errors.Is(err, ErrProceedCalledTwice) {
		t.Fatalf("concurrent proceed error = %v", err)
	}
}

func TestRequiredAroundDrainsAdmittedProceedBeforeReturning(t *testing.T) {
	registry := newTestRegistry(nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	secret := errors.New("delegated secret")
	_, err := registry.Mount(context.Background(), testComponent("outlived-next"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return OnAround(registrar, testAround, spec("outlived-next", "around", 0, GlobalScope()), func(ctx context.Context, _ testPayload, proceed Proceed) error {
			go func() { _ = proceed(ctx) }()
			<-entered
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
	result := make(chan error, 1)
	go func() {
		_, invokeErr := InvokeAround(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context) (string, error) {
			close(entered)
			<-release
			return "", secret
		})
		result <- invokeErr
	}()
	select {
	case err := <-result:
		t.Fatalf("InvokeAround returned before terminal completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	err = <-result
	if !errors.Is(err, ErrProceedOutlivedCallback) || !errors.Is(err, secret) {
		t.Fatalf("InvokeAround error = %v", err)
	}
	if strings.Contains(err.Error(), secret.Error()) {
		t.Fatalf("public lifecycle error leaked delegated text: %q", err)
	}
}

func TestInterceptorOnionAndProceedGuard(t *testing.T) {
	registry := newTestRegistry(nil)
	var sequence []string
	for index, id := range []string{"outer", "inner"} {
		id, order := id, index
		_, err := registry.Mount(context.Background(), testComponent(id), InstallerFunc(func(_ context.Context, registrar Registrar) error {
			return OnAround(registrar, testAround, spec(id, id, order, GlobalScope()), func(ctx context.Context, _ testPayload, proceed Proceed) error {
				sequence = append(sequence, id+"-before")
				err := proceed(ctx)
				sequence = append(sequence, id+"-after")
				return err
			})
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	output, err := InvokeAround(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context) (string, error) {
		sequence = append(sequence, "terminal")
		return "ok", nil
	})
	if err != nil || output != "ok" || !reflect.DeepEqual(sequence, []string{"outer-before", "inner-before", "terminal", "inner-after", "outer-after"}) {
		t.Fatalf("Invoke = %q, %v; sequence=%v", output, err, sequence)
	}

	badRegistry := newTestRegistry(nil)
	_, _ = badRegistry.Mount(context.Background(), testComponent("bad"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return OnAround(registrar, testAround, spec("bad", "bad", 0, GlobalScope()), func(ctx context.Context, _ testPayload, proceed Proceed) error {
			if err := proceed(ctx); err != nil {
				return err
			}
			return proceed(ctx)
		})
	}))
	badPlan, _ := badRegistry.Snapshot(GlobalScope())
	defer badPlan.Release()
	if _, err := InvokeAround(badPlan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context) (string, error) { return "ok", nil }); !errors.Is(err, ErrProceedCalledTwice) {
		t.Fatalf("double proceed error = %v", err)
	}
}

func TestAroundInputMutationIsIsolatedAndDelegationIsRequired(t *testing.T) {
	registry := newTestRegistry(nil)
	seen := make(chan testPayload, 1)
	_, _ = registry.Mount(context.Background(), testComponent("outer"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return OnAround(registrar, testAround, spec("outer", "outer", 0, GlobalScope()), func(ctx context.Context, input testPayload, proceed Proceed) error {
			input.Protected = "changed"
			input.Values = append(input.Values, "outer")
			return proceed(ctx)
		})
	}))
	_, _ = registry.Mount(context.Background(), testComponent("inner"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return OnAround(registrar, testAround, spec("inner", "inner", 1, GlobalScope()), func(ctx context.Context, input testPayload, proceed Proceed) error {
			seen <- input
			return proceed(ctx)
		})
	}))
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	original := testPayload{Protected: "fixed", Values: []string{"original"}}
	if _, err := InvokeAround(plan, context.Background(), testAround, original, func(context.Context) (string, error) { return "ok", nil }); err != nil {
		t.Fatal(err)
	}
	if got := <-seen; !reflect.DeepEqual(got, original) {
		t.Fatalf("inner input = %#v, want %#v", got, original)
	}

	badRegistry := newTestRegistry(nil)
	_, _ = badRegistry.Mount(context.Background(), testComponent("skip"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return OnAround(registrar, testAround, spec("skip", "skip", 0, GlobalScope()), func(context.Context, testPayload, Proceed) error { return nil })
	}))
	badPlan, _ := badRegistry.Snapshot(GlobalScope())
	defer badPlan.Release()
	if _, err := InvokeAround(badPlan, context.Background(), testAround, original, func(context.Context) (string, error) { return "ok", nil }); !errors.Is(err, ErrProceedNotCalled) {
		t.Fatalf("error = %v, want ErrProceedNotCalled", err)
	}
}

func TestRequiredDelegationCannotSwallowDelegatedFailure(t *testing.T) {
	delegatedErr := errors.New("delegated failure")
	registry := newTestRegistry(nil)
	component := testComponent("swallow")
	_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return OnAround(registrar, testAround, spec(component.InstanceID, "swallow", 0, GlobalScope()), func(ctx context.Context, _ testPayload, proceed Proceed) error {
			_ = proceed(ctx)
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
	output, err := InvokeAround(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context) (string, error) {
		return "", delegatedErr
	})
	if output != "" || !errors.Is(err, delegatedErr) {
		t.Fatalf("Invoke = %q, %v; want delegated failure", output, err)
	}
}

func TestRequiredDelegationPreservesCallbackAuthoredErrors(t *testing.T) {
	terminalErr := errors.New("terminal detail")
	innerErr := errors.New("inner detail")
	outerErr := errors.New("outer detail")
	var diagnostics []Diagnostic
	registry := newTestRegistry(ReporterFunc(func(_ context.Context, diagnostic Diagnostic) {
		diagnostics = append(diagnostics, diagnostic)
	}))

	for index, callback := range []struct {
		instance string
		invoke   Around[testPayload]
	}{
		{
			instance: "outer",
			invoke: func(ctx context.Context, _ testPayload, proceed Proceed) error {
				return errors.Join(proceed(ctx), outerErr)
			},
		},
		{
			instance: "inner",
			invoke: func(ctx context.Context, _ testPayload, proceed Proceed) error {
				return fmt.Errorf("inner context: %w", errors.Join(proceed(ctx), innerErr))
			},
		},
	} {
		callback := callback
		_, err := registry.Mount(context.Background(), testComponent(callback.instance), InstallerFunc(func(_ context.Context, registrar Registrar) error {
			return OnAround(registrar, testAround, spec(callback.instance, callback.instance, index, GlobalScope()), callback.invoke)
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
	_, err = InvokeAround(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context) (string, error) {
		return "", terminalErr
	})
	if !errors.Is(err, terminalErr) || !errors.Is(err, innerErr) || !errors.Is(err, outerErr) {
		t.Fatalf("InvokeAround error = %v", err)
	}
	var callbackErr *CallbackError
	if !errors.As(err, &callbackErr) {
		t.Fatalf("InvokeAround error lacks CallbackError: %v", err)
	}
	if len(diagnostics) != 2 || diagnostics[0].InstanceID != "inner" || diagnostics[1].InstanceID != "outer" {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if !strings.Contains(err.Error(), terminalErr.Error()) || strings.Contains(err.Error(), innerErr.Error()) || strings.Contains(err.Error(), outerErr.Error()) || strings.Contains(err.Error(), "inner context") {
		t.Fatalf("InvokeAround error text = %q", err)
	}
}

func TestRequiredDelegationDirectOuterPropagationDoesNotDuplicateDiagnostic(t *testing.T) {
	terminalErr := errors.New("terminal failed")
	innerErr := errors.New("inner failed")
	var diagnostics []Diagnostic
	registry := newTestRegistry(ReporterFunc(func(_ context.Context, diagnostic Diagnostic) {
		diagnostics = append(diagnostics, diagnostic)
	}))
	for index, callback := range []struct {
		instance string
		invoke   Around[testPayload]
	}{
		{instance: "outer", invoke: func(ctx context.Context, _ testPayload, proceed Proceed) error {
			return proceed(ctx)
		}},
		{instance: "inner", invoke: func(ctx context.Context, _ testPayload, proceed Proceed) error {
			return errors.Join(proceed(ctx), innerErr)
		}},
	} {
		callback := callback
		_, err := registry.Mount(context.Background(), testComponent(callback.instance), InstallerFunc(func(_ context.Context, registrar Registrar) error {
			return OnAround(registrar, testAround, spec(callback.instance, callback.instance, index, GlobalScope()), callback.invoke)
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
	_, err = InvokeAround(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context) (string, error) {
		return "", terminalErr
	})
	if !errors.Is(err, terminalErr) || !errors.Is(err, innerErr) || len(diagnostics) != 1 || diagnostics[0].InstanceID != "inner" {
		t.Fatalf("InvokeAround error = %v diagnostics=%#v", err, diagnostics)
	}
}

func TestInvokeRejectsDelegationStartedAfterInterceptorReturns(t *testing.T) {
	registry := newTestRegistry(nil)
	component := testComponent("late-next")
	savedProceed := make(chan Proceed, 1)
	_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return OnAround(registrar, testAround, spec(component.InstanceID, "late", 0, GlobalScope()), func(_ context.Context, _ testPayload, proceed Proceed) error {
			savedProceed <- proceed
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
	var terminalCalls atomic.Int32
	if _, err := InvokeAround(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context) (string, error) {
		terminalCalls.Add(1)
		return "terminal", nil
	}); !errors.Is(err, ErrProceedNotCalled) {
		t.Fatalf("Invoke error = %v, want ErrProceedNotCalled", err)
	}
	if err := (<-savedProceed)(context.Background()); !errors.Is(err, ErrProceedOutlivedCallback) {
		t.Fatalf("late proceed error = %v, want ErrProceedOutlivedCallback", err)
	}
	if calls := terminalCalls.Load(); calls != 0 {
		t.Fatalf("terminal calls = %d, want 0", calls)
	}
}

func TestInterceptorPropagatesTightenedNextContext(t *testing.T) {
	registry := newTestRegistry(nil)
	component := testComponent("context")
	_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return OnAround(registrar, testAround, spec(component.InstanceID, "deadline", 0, GlobalScope()), func(ctx context.Context, _ testPayload, proceed Proceed) error {
			tightened, cancel := context.WithCancel(ctx)
			cancel()
			return proceed(tightened)
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	_, err = InvokeAround(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(ctx context.Context) (string, error) {
		return "", ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("terminal context error = %v", err)
	}
}

func TestInterceptorErrorHasBoundedPublicTextAndLocalCause(t *testing.T) {
	registry := newTestRegistry(nil)
	secret := errors.New("credential-sentinel-do-not-persist")
	component := testComponent("bounded-error")
	_, err := registry.Mount(context.Background(), component, InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return OnAround(registrar, testAround, spec(component.InstanceID, "failure", 0, GlobalScope()), func(context.Context, testPayload, Proceed) error {
			return secret
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
	_, err = InvokeAround(plan, context.Background(), testAround, testPayload{Protected: "fixed"}, func(context.Context) (string, error) {
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
