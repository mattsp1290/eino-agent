package extension

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type delegatedError struct{ cause error }

func (e *delegatedError) Error() string { return e.cause.Error() }
func (e *delegatedError) Unwrap() error { return e.cause }

func Notify[T any](plan *Plan, ctx context.Context, point Notification[T], value T) {
	if plan == nil || point.key == nil {
		return
	}
	for _, entry := range matchingEntries(plan, point.key, entryNotification) {
		observer, ok := entry.callback.(Observer[T])
		if !ok {
			report(plan.reporter, ctx, callbackFailure(entry, "callback_type", errors.New("observer type mismatch")))
			continue
		}
		input, err := cloneInput(point.clone, value)
		if err != nil {
			report(plan.reporter, ctx, callbackFailure(entry, "clone_failed", err))
			continue
		}
		if err := callObserver(callbackContext(ctx, entry.token), observer, input); err != nil {
			report(plan.reporter, ctx, callbackFailure(entry, failureCode(err), err))
		}
	}
}

func RunHooks[T any](plan *Plan, ctx context.Context, point Hook[T], value T) error {
	for _, entry := range matchingEntries(plan, point.key, entryHook) {
		hook, ok := entry.callback.(HookFunc[T])
		if !ok {
			return errors.New("extension hook type mismatch")
		}
		input, err := cloneInput(point.clone, value)
		if err != nil {
			return err
		}
		err = callHook(callbackContext(ctx, entry.token), hook, input)
		if err != nil {
			return propagateCallbackFailure(plan, ctx, entry, err)
		}
		if point.validate != nil {
			if err := point.validate(value, input); err != nil {
				return fmt.Errorf("%w: %v", ErrProtectedMutation, err)
			}
		}
	}
	return nil
}

func ApplyTransforms[T any](plan *Plan, ctx context.Context, point Transform[T], value T) (T, error) {
	original, err := cloneInput(point.clone, value)
	if err != nil {
		return value, err
	}
	current, err := cloneInput(point.clone, value)
	if err != nil {
		return value, err
	}
	if point.validate != nil {
		if validateErr := point.validate(original, current); validateErr != nil {
			return value, validateErr
		}
	}
	for _, entry := range matchingEntries(plan, point.key, entryTransform) {
		transform, ok := entry.callback.(TransformFunc[T])
		if !ok {
			return value, errors.New("extension transform type mismatch")
		}
		callbackInput, cloneErr := cloneInput(point.clone, current)
		if cloneErr != nil {
			return value, cloneErr
		}
		candidate, callbackErr := callTransform(callbackContext(ctx, entry.token), transform, callbackInput)
		if callbackErr != nil {
			return value, propagateCallbackFailure(plan, ctx, entry, callbackErr)
		}
		if point.validate != nil {
			if validateErr := point.validate(original, candidate); validateErr != nil {
				return value, validateErr
			}
		}
		current, err = cloneInput(point.clone, candidate)
		if err != nil {
			return value, err
		}
	}
	return current, nil
}

func EvaluateGate[I, D any](plan *Plan, ctx context.Context, point Gate[I, D], input I) (D, error) {
	decision := point.continueDecision
	if point.validateDecision != nil {
		if err := point.validateDecision(decision); err != nil {
			return decision, err
		}
	}
	if point.shouldContinue == nil {
		return decision, fmt.Errorf("%w: nil gate continuation predicate", ErrInvalidContract)
	}
	for _, entry := range matchingEntries(plan, point.key, entryGate) {
		gate, ok := entry.callback.(GateFunc[I, D])
		if !ok {
			return decision, errors.New("extension gate type mismatch")
		}
		candidate, err := cloneInput(point.clone, input)
		if err != nil {
			return decision, err
		}
		decision, err = callGate(callbackContext(ctx, entry.token), gate, candidate)
		if err != nil {
			return decision, propagateCallbackFailure(plan, ctx, entry, err)
		}
		if point.validateInput != nil {
			if err := point.validateInput(input, candidate); err != nil {
				return decision, fmt.Errorf("%w: %v", ErrProtectedMutation, err)
			}
		}
		if point.validateDecision != nil {
			if err := point.validateDecision(decision); err != nil {
				return decision, err
			}
		}
		if !point.shouldContinue(decision) {
			return decision, nil
		}
	}
	return decision, nil
}

func InvokeAround[I, O any](plan *Plan, ctx context.Context, point RequiredAround[I, O], input I, terminal Next[I, O]) (O, error) {
	var zero O
	if terminal == nil {
		return zero, fmt.Errorf("%w: nil terminal", ErrInvalidRegistration)
	}
	entries := matchingEntries(plan, point.key, entryAround)
	var invoke func(int, context.Context, I) (O, error)
	invoke = func(index int, currentCtx context.Context, candidate I) (O, error) {
		if point.validateInput != nil {
			if err := point.validateInput(input, candidate); err != nil {
				return zero, fmt.Errorf("%w: %v", ErrProtectedMutation, err)
			}
		}
		if index == len(entries) {
			cloned, err := cloneInput(point.clone, candidate)
			if err != nil {
				return zero, err
			}
			out, err := terminal(currentCtx, cloned)
			if err == nil && point.validateOutput != nil {
				err = point.validateOutput(out)
			}
			return out, err
		}
		entry := entries[index]
		around, ok := entry.callback.(Around[I, O])
		if !ok {
			return zero, errors.New("extension around callback type mismatch")
		}
		var nextMu sync.Mutex
		calls := 0
		activeCalls := 0
		callbackOpen := true
		var delegatedOutput O
		var delegatedSucceeded bool
		var delegatedFailure error
		next := func(nextCtx context.Context, nextInput I) (O, error) {
			nextMu.Lock()
			calls++
			if calls != 1 {
				nextMu.Unlock()
				return zero, ErrNextCalledTwice
			}
			if !callbackOpen {
				nextMu.Unlock()
				return zero, ErrNextNotCalled
			}
			activeCalls++
			nextMu.Unlock()
			if nextCtx == nil {
				nextCtx = currentCtx
			}
			cloned, err := cloneInput(point.clone, nextInput)
			if err != nil {
				nextMu.Lock()
				delegatedFailure = err
				activeCalls--
				nextMu.Unlock()
				return zero, &delegatedError{cause: err}
			}
			out, err := invoke(index+1, nextCtx, cloned)
			nextMu.Lock()
			defer nextMu.Unlock()
			activeCalls--
			if err != nil {
				delegatedFailure = err
				return out, &delegatedError{cause: err}
			}
			delegatedOutput = out
			delegatedSucceeded = true
			return out, nil
		}
		cloned, err := cloneInput(point.clone, candidate)
		if err != nil {
			return zero, err
		}
		out, callbackErr := callAround(callbackContext(currentCtx, entry.token), around, cloned, next)
		nextMu.Lock()
		callbackOpen = false
		finalCalls := calls
		pendingCalls := activeCalls
		finalDelegatedOutput := delegatedOutput
		finalDelegatedSucceeded := delegatedSucceeded
		finalDelegatedFailure := delegatedFailure
		nextMu.Unlock()
		if finalCalls > 1 {
			return zero, ErrNextCalledTwice
		}
		if pendingCalls != 0 {
			return zero, ErrNextNotCalled
		}
		if callbackErr == nil {
			if finalCalls != 1 {
				return zero, ErrNextNotCalled
			}
			if !finalDelegatedSucceeded {
				if finalDelegatedFailure != nil {
					return zero, finalDelegatedFailure
				}
				return zero, ErrNextNotCalled
			}
		}
		if callbackErr != nil {
			var delegated *delegatedError
			if errors.As(callbackErr, &delegated) {
				return out, delegated.cause
			}
			return out, propagateCallbackFailure(plan, currentCtx, entry, callbackErr)
		}
		if point.validateOutput != nil {
			if validateErr := point.validateOutput(out); validateErr != nil {
				return zero, validateErr
			}
		}
		if point.validateDelegated != nil {
			if validateErr := point.validateDelegated(finalDelegatedOutput, out); validateErr != nil {
				return zero, validateErr
			}
		}
		return out, nil
	}
	cloned, err := cloneInput(point.clone, input)
	if err != nil {
		return zero, err
	}
	return invoke(0, ctx, cloned)
}

func matchingEntries(plan *Plan, key *pointKey, kind entryKind) []plannedEntry {
	if plan == nil || key == nil {
		return nil
	}
	entries := make([]plannedEntry, 0)
	for _, entry := range plan.entries {
		if entry.point == key && entry.kind == kind {
			entries = append(entries, entry)
		}
	}
	return entries
}

func propagateCallbackFailure(plan *Plan, ctx context.Context, entry plannedEntry, err error) error {
	failure := callbackFailure(entry, failureCode(err), err)
	if plan != nil {
		report(plan.reporter, ctx, failure)
	}
	return &CallbackError{Point: failure.Point, InstanceID: failure.InstanceID, HandlerID: failure.HandlerID, Code: failure.Code, cause: failure.Cause}
}

func callObserver[T any](ctx context.Context, observer Observer[T], value T) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("extension observer panic: %v", recovered)
		}
	}()
	return observer(ctx, value)
}

func callHook[T any](ctx context.Context, hook HookFunc[T], value T) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("extension hook panic: %v", recovered)
		}
	}()
	return hook(ctx, value)
}

func callTransform[T any](ctx context.Context, transform TransformFunc[T], value T) (out T, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("extension transform panic: %v", recovered)
		}
	}()
	return transform(ctx, value)
}

func callGate[I, D any](ctx context.Context, gate GateFunc[I, D], input I) (out D, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("extension gate panic: %v", recovered)
		}
	}()
	return gate(ctx, input)
}

func callAround[I, O any](ctx context.Context, around Around[I, O], input I, next Next[I, O]) (out O, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("extension around callback panic: %v", recovered)
		}
	}()
	return around(ctx, input, next)
}

func cloneInput[T any](clone CloneFunc[T], input T) (T, error) {
	if clone == nil {
		return input, nil
	}
	return clone(input)
}

func callbackFailure(entry plannedEntry, code string, cause error) Diagnostic {
	return Diagnostic{Point: entry.contract, InstanceID: entry.component.InstanceID, HandlerID: entry.spec.ID, Code: code, Cause: cause}
}

func failureCode(err error) string {
	if err == nil {
		return ""
	}
	return "callback_failed"
}

func report(reporter Reporter, ctx context.Context, failure Diagnostic) {
	if reporter == nil {
		return
	}
	defer func() { _ = recover() }()
	reporter.Report(ctx, failure)
}
