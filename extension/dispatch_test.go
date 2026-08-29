package extension

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTransformChainFeedsValidatedOutputInOrder(t *testing.T) {
	point := NewTransform(Contract{ID: "test/transform", Version: "1"}, clonePayload, func(original, candidate testPayload) error {
		if original.Protected != candidate.Protected {
			return ErrProtectedMutation
		}
		return nil
	})
	registry := newTestRegistry(nil, point)
	_, err := registry.Mount(context.Background(), testComponent("transforms"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		if err := OnTransform(registrar, point, spec("transforms", "first", 0, GlobalScope()), func(_ context.Context, value testPayload) (testPayload, error) {
			value.Values = append(value.Values, "first")
			return value, nil
		}); err != nil {
			return err
		}
		return OnTransform(registrar, point, spec("transforms", "second", 1, GlobalScope()), func(_ context.Context, value testPayload) (testPayload, error) {
			if !reflect.DeepEqual(value.Values, []string{"first"}) {
				return testPayload{}, errors.New("second transform did not receive first output")
			}
			value.Values = append(value.Values, "second")
			return value, nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	original := testPayload{Protected: "fixed"}
	result, err := ApplyTransforms(plan, context.Background(), point, original)
	if err != nil || !reflect.DeepEqual(result.Values, []string{"first", "second"}) || len(original.Values) != 0 {
		t.Fatalf("ApplyTransforms = %#v, %v; original=%#v", result, err, original)
	}
}

func TestHookChainFailsFast(t *testing.T) {
	point := NewHook(Contract{ID: "test/hook", Version: "1"}, clonePayload, func(original, candidate testPayload) error {
		if !reflect.DeepEqual(original, candidate) {
			return ErrProtectedMutation
		}
		return nil
	})
	registry := newTestRegistry(nil, point)
	var sequence []string
	_, err := registry.Mount(context.Background(), testComponent("hooks"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		if err := OnHook(registrar, point, spec("hooks", "fail", 0, GlobalScope()), func(context.Context, testPayload) error {
			sequence = append(sequence, "fail")
			return errors.New("stop")
		}); err != nil {
			return err
		}
		return OnHook(registrar, point, spec("hooks", "tail", 1, GlobalScope()), func(context.Context, testPayload) error {
			sequence = append(sequence, "tail")
			return nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	err = RunHooks(plan, context.Background(), point, testPayload{Protected: "fixed"})
	var callback *CallbackError
	if !errors.As(err, &callback) || !reflect.DeepEqual(sequence, []string{"fail"}) {
		t.Fatalf("RunHooks error=%v sequence=%v", err, sequence)
	}
}

func TestGateStopsAtFirstRejectAndValidatesDecisions(t *testing.T) {
	type decision string
	const (
		proceed decision = "proceed"
		reject  decision = "reject"
	)
	point := NewGate(Contract{ID: "test/gate", Version: "1"}, clonePayload, func(original, candidate testPayload) error {
		if !reflect.DeepEqual(original, candidate) {
			return ErrProtectedMutation
		}
		return nil
	}, func(value decision) error {
		if value != proceed && value != reject {
			return errors.New("invalid decision")
		}
		return nil
	}, proceed, func(value decision) bool { return value == proceed })
	registry := newTestRegistry(nil, point)
	var sequence []string
	_, err := registry.Mount(context.Background(), testComponent("gates"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		for index, item := range []struct {
			id       string
			decision decision
		}{{"first", proceed}, {"reject", reject}, {"tail", proceed}} {
			item := item
			if err := OnGate(registrar, point, spec("gates", item.id, index, GlobalScope()), func(context.Context, testPayload) (decision, error) {
				sequence = append(sequence, item.id)
				return item.decision, nil
			}); err != nil {
				return err
			}
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	got, err := EvaluateGate(plan, context.Background(), point, testPayload{Protected: "fixed"})
	if err != nil || got != reject || !reflect.DeepEqual(sequence, []string{"first", "reject"}) {
		t.Fatalf("EvaluateGate = %q, %v; sequence=%v", got, err, sequence)
	}

	invalidRegistry := newTestRegistry(nil, point)
	_, err = invalidRegistry.Mount(context.Background(), testComponent("invalid-gate"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		return OnGate(registrar, point, spec("invalid-gate", "invalid", 0, GlobalScope()), func(context.Context, testPayload) (decision, error) {
			return "invalid", nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	invalidPlan, _ := invalidRegistry.Snapshot(GlobalScope())
	defer invalidPlan.Release()
	if _, err := EvaluateGate(invalidPlan, context.Background(), point, testPayload{Protected: "fixed"}); err == nil {
		t.Fatal("invalid gate decision was accepted")
	}
}

func TestGateCallbackFailureIsBoundedAndStopsEvaluation(t *testing.T) {
	type decision bool
	point := NewGate(Contract{ID: "test/failing-gate", Version: "1"}, clonePayload, func(original, candidate testPayload) error {
		if !reflect.DeepEqual(original, candidate) {
			return ErrProtectedMutation
		}
		return nil
	}, func(decision) error { return nil }, decision(true), func(value decision) bool { return bool(value) })
	registry := newTestRegistry(nil, point)
	called := false
	secret := errors.New("secret gate failure")
	_, err := registry.Mount(context.Background(), testComponent("failing-gate"), InstallerFunc(func(_ context.Context, registrar Registrar) error {
		if err := OnGate(registrar, point, spec("failing-gate", "failure", 0, GlobalScope()), func(context.Context, testPayload) (decision, error) {
			return false, secret
		}); err != nil {
			return err
		}
		return OnGate(registrar, point, spec("failing-gate", "tail", 1, GlobalScope()), func(context.Context, testPayload) (decision, error) {
			called = true
			return true, nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := registry.Snapshot(GlobalScope())
	defer plan.Release()
	_, err = EvaluateGate(plan, context.Background(), point, testPayload{})
	var callback *CallbackError
	if !errors.As(err, &callback) || !errors.Is(err, secret) || called {
		t.Fatalf("EvaluateGate error=%v tail_called=%t", err, called)
	}
}
