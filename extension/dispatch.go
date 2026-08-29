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

type proceedOutlivedError struct{ cause error }

func (e *proceedOutlivedError) Error() string { return ErrProceedOutlivedCallback.Error() }
func (e *proceedOutlivedError) Unwrap() error { return e.cause }
func (e *proceedOutlivedError) Is(target error) bool {
	return target == ErrProceedOutlivedCallback
}

func Notify[T any](plan *Plan, ctx context.Context, point Notification[T], value T) {
	if plan == nil || point.definition == nil {
		return
	}
	for _, entry := range matchingEntries(plan, point.base()) {
		observer := entry.callback.(Observer[T])
		input, err := cloneInput(point.definition.clone, value)
		if err != nil {
			plan.enqueueNotification(entry, func() {
				report(plan.reporter, ctx, callbackFailure(entry, "clone_failed", err))
			})
			continue
		}
		plan.enqueueNotification(entry, func() {
			if err := callObserver(callbackContext(ctx, entry.token), observer, input); err != nil {
				report(plan.reporter, ctx, callbackFailure(entry, failureCode(err), err))
			}
		})
	}
}

func RunHooks[T any](plan *Plan, ctx context.Context, point Hook[T], value T) error {
	if err := validatePoint(point.base()); err != nil {
		return err
	}
	for _, entry := range matchingEntries(plan, point.base()) {
		hook := entry.callback.(HookFunc[T])
		input, err := cloneInput(point.definition.clone, value)
		if err != nil {
			return err
		}
		err = callHook(callbackContext(ctx, entry.token), hook, input)
		if err != nil {
			return propagateCallbackFailure(plan, ctx, entry, err)
		}
		if point.definition.validate != nil {
			if err := point.definition.validate(value, input); err != nil {
				return fmt.Errorf("%w: %v", ErrProtectedMutation, err)
			}
		}
	}
	return nil
}

func ApplyTransforms[T any](plan *Plan, ctx context.Context, point Transform[T], value T) (T, error) {
	if err := validatePoint(point.base()); err != nil {
		return value, err
	}
	original, err := cloneInput(point.definition.clone, value)
	if err != nil {
		return value, err
	}
	current, err := cloneInput(point.definition.clone, value)
	if err != nil {
		return value, err
	}
	if point.definition.validate != nil {
		if validateErr := point.definition.validate(original, current); validateErr != nil {
			return value, validateErr
		}
	}
	for _, entry := range matchingEntries(plan, point.base()) {
		transform := entry.callback.(TransformFunc[T])
		callbackInput, cloneErr := cloneInput(point.definition.clone, current)
		if cloneErr != nil {
			return value, cloneErr
		}
		candidate, callbackErr := callTransform(callbackContext(ctx, entry.token), transform, callbackInput)
		if callbackErr != nil {
			return value, propagateCallbackFailure(plan, ctx, entry, callbackErr)
		}
		if point.definition.validate != nil {
			if validateErr := point.definition.validate(original, candidate); validateErr != nil {
				return value, validateErr
			}
		}
		current, err = cloneInput(point.definition.clone, candidate)
		if err != nil {
			return value, err
		}
	}
	return current, nil
}

func EvaluateGate[I, D any](plan *Plan, ctx context.Context, point Gate[I, D], input I) (D, error) {
	var zero D
	if err := validatePoint(point.base()); err != nil {
		return zero, err
	}
	decision := point.definition.continueDecision
	if point.definition.validateDecision != nil {
		if err := point.definition.validateDecision(decision); err != nil {
			return decision, err
		}
	}
	if point.definition.shouldContinue == nil {
		return decision, fmt.Errorf("%w: nil gate continuation predicate", ErrInvalidContract)
	}
	for _, entry := range matchingEntries(plan, point.base()) {
		gate := entry.callback.(GateFunc[I, D])
		candidate, err := cloneInput(point.definition.clone, input)
		if err != nil {
			return decision, err
		}
		decision, err = callGate(callbackContext(ctx, entry.token), gate, candidate)
		if err != nil {
			return decision, propagateCallbackFailure(plan, ctx, entry, err)
		}
		if point.definition.validateInput != nil {
			if err := point.definition.validateInput(input, candidate); err != nil {
				return decision, fmt.Errorf("%w: %v", ErrProtectedMutation, err)
			}
		}
		if point.definition.validateDecision != nil {
			if err := point.definition.validateDecision(decision); err != nil {
				return decision, err
			}
		}
		if !point.definition.shouldContinue(decision) {
			return decision, nil
		}
	}
	return decision, nil
}

func InvokeAround[I, O any](plan *Plan, ctx context.Context, point RequiredAround[I, O], input I, terminal func(context.Context) (O, error)) (O, error) {
	var zero O
	if terminal == nil {
		return zero, fmt.Errorf("%w: nil terminal", ErrInvalidRegistration)
	}
	if err := validatePoint(point.base()); err != nil {
		return zero, err
	}
	entries := matchingEntries(plan, point.base())
	var invoke func(int, context.Context) (O, error)
	invoke = func(index int, currentCtx context.Context) (O, error) {
		if index == len(entries) {
			out, err := terminal(currentCtx)
			if err == nil && point.definition.validateOutput != nil {
				err = point.definition.validateOutput(out)
			}
			return out, err
		}
		entry := entries[index]
		around := entry.callback.(Around[I])
		var proceedMu sync.Mutex
		calls := 0
		activeCall := false
		callbackOpen := true
		proceedDone := make(chan struct{})
		var delegatedOutput O
		var delegatedSucceeded bool
		var delegatedFailure error
		proceed := func(nextCtx context.Context) error {
			proceedMu.Lock()
			if !callbackOpen {
				proceedMu.Unlock()
				return ErrProceedOutlivedCallback
			}
			calls++
			if calls != 1 {
				proceedMu.Unlock()
				return ErrProceedCalledTwice
			}
			activeCall = true
			proceedMu.Unlock()
			defer func() {
				proceedMu.Lock()
				activeCall = false
				close(proceedDone)
				proceedMu.Unlock()
			}()
			if nextCtx == nil {
				nextCtx = currentCtx
			}
			out, err := invoke(index+1, nextCtx)
			proceedMu.Lock()
			defer proceedMu.Unlock()
			delegatedOutput = out
			if err != nil {
				delegatedFailure = err
				return &delegatedError{cause: err}
			}
			delegatedSucceeded = true
			return nil
		}
		cloned, err := cloneInput(point.definition.clone, input)
		if err != nil {
			return zero, err
		}
		callbackErr := callAround(callbackContext(currentCtx, entry.token), around, cloned, proceed)
		proceedMu.Lock()
		callbackOpen = false
		outlived := activeCall
		proceedMu.Unlock()
		if outlived {
			<-proceedDone
		}
		proceedMu.Lock()
		finalCalls := calls
		finalDelegatedOutput := delegatedOutput
		finalDelegatedSucceeded := delegatedSucceeded
		finalDelegatedFailure := delegatedFailure
		proceedMu.Unlock()
		if finalCalls > 1 {
			return zero, ErrProceedCalledTwice
		}
		if outlived {
			return zero, &proceedOutlivedError{cause: finalDelegatedFailure}
		}
		if callbackErr == nil {
			if finalCalls != 1 {
				return zero, ErrProceedNotCalled
			}
			if !finalDelegatedSucceeded {
				if finalDelegatedFailure != nil {
					return zero, finalDelegatedFailure
				}
				return zero, ErrProceedNotCalled
			}
		}
		if callbackErr != nil {
			var delegated *delegatedError
			if errors.As(callbackErr, &delegated) && callbackErr == delegated {
				return finalDelegatedOutput, delegated.cause
			}
			callbackFailure := propagateCallbackFailure(plan, currentCtx, entry, callbackErr)
			if finalDelegatedFailure != nil {
				return finalDelegatedOutput, errors.Join(finalDelegatedFailure, callbackFailure)
			}
			return finalDelegatedOutput, callbackFailure
		}
		return finalDelegatedOutput, nil
	}
	return invoke(0, ctx)
}

func matchingEntries(plan *Plan, point *pointDefinition) []plannedEntry {
	if plan == nil || point == nil {
		return nil
	}
	entries := make([]plannedEntry, 0)
	for _, entry := range plan.entries {
		if entry.point == point {
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

func callAround[I any](ctx context.Context, around Around[I], input I, proceed Proceed) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("extension around callback panic: %v", recovered)
		}
	}()
	return around(ctx, input, proceed)
}

func cloneInput[T any](clone CloneFunc[T], input T) (T, error) {
	if clone == nil {
		return input, nil
	}
	return clone(input)
}

func callbackFailure(entry plannedEntry, code string, cause error) Diagnostic {
	return Diagnostic{Point: entry.point.contract, InstanceID: entry.component.InstanceID, HandlerID: entry.spec.ID, Code: code, Cause: cause}
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
