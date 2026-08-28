package extension

import (
	"context"
	"errors"
	"fmt"
)

type delegatedError struct{ cause error }

func (e *delegatedError) Error() string { return e.cause.Error() }
func (e *delegatedError) Unwrap() error { return e.cause }

func Notify[T any](plan *Plan, ctx context.Context, point Notification[T], value T) {
	if plan == nil || point.key == nil {
		return
	}
	for _, entry := range plan.entries {
		if entry.point != point.key || entry.kind != entryNotification {
			continue
		}
		observer, ok := entry.callback.(Observer[T])
		if !ok {
			failure := callbackFailure(entry, "callback_type", fmt.Errorf("observer type mismatch"))
			report(plan.reporter, ctx, failure)
			continue
		}
		input := value
		if point.clone != nil {
			var err error
			input, err = point.clone(value)
			if err != nil {
				failure := callbackFailure(entry, "clone_failed", err)
				report(plan.reporter, ctx, failure)
				continue
			}
		}
		err := callObserver(callbackContext(ctx, entry.token), observer, input)
		if err == nil {
			continue
		}
		failure := callbackFailure(entry, failureCode(err), err)
		report(plan.reporter, ctx, failure)
	}
}

func callObserver[T any](ctx context.Context, observer Observer[T], value T) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("extension observer panic: %v", recovered)
		}
	}()
	return observer(ctx, value)
}

func Invoke[I, O any](plan *Plan, ctx context.Context, point Interceptor[I, O], input I, terminal Next[I, O]) (O, error) {
	var zero O
	if terminal == nil {
		return zero, fmt.Errorf("%w: nil terminal", ErrInvalidRegistration)
	}
	if plan == nil || point.key == nil {
		cloned, err := cloneInput(point.clone, input)
		if err != nil {
			return zero, err
		}
		return terminal(ctx, cloned)
	}
	entries := make([]plannedEntry, 0)
	for _, entry := range plan.entries {
		if entry.point == point.key && entry.kind == entryInterceptor {
			entries = append(entries, entry)
		}
	}
	var invoke func(int, context.Context, I) (O, error)
	invoke = func(index int, currentCtx context.Context, candidate I) (O, error) {
		if point.validateIn != nil {
			if err := point.validateIn(input, candidate); err != nil {
				return zero, fmt.Errorf("%w: %v", ErrProtectedMutation, err)
			}
		}
		if index == len(entries) {
			cloned, err := cloneInput(point.clone, candidate)
			if err != nil {
				return zero, err
			}
			out, err := terminal(currentCtx, cloned)
			if err == nil && point.validateOut != nil {
				err = point.validateOut(out)
			}
			if err == nil && point.validateResult != nil {
				err = point.validateResult(input, out)
			}
			return out, err
		}
		entry := entries[index]
		around, ok := entry.callback.(Around[I, O])
		if !ok {
			return zero, fmt.Errorf("extension interceptor type mismatch")
		}
		calls := 0
		callbackOpen := true
		var delegatedOutput O
		var delegatedSucceeded bool
		var delegatedFailure error
		next := func(nextCtx context.Context, nextInput I) (O, error) {
			calls++
			if calls != 1 {
				return zero, ErrNextCalledTwice
			}
			if !callbackOpen {
				return zero, ErrNextNotCalled
			}
			if nextCtx == nil {
				nextCtx = currentCtx
			}
			cloned, err := cloneInput(point.clone, nextInput)
			if err != nil {
				delegatedFailure = err
				return zero, &delegatedError{cause: err}
			}
			out, err := invoke(index+1, nextCtx, cloned)
			if err != nil {
				delegatedFailure = err
				return out, &delegatedError{cause: err}
			}
			delegatedOutput = out
			delegatedSucceeded = true
			return out, nil
		}
		cloned, cloneErr := cloneInput(point.clone, candidate)
		if cloneErr != nil {
			return zero, cloneErr
		}
		out, err := callAround(callbackContext(currentCtx, entry.token), around, cloned, next)
		callbackOpen = false
		if calls > 1 {
			return zero, ErrNextCalledTwice
		}
		if err == nil && (point.requireNext || entry.requireNext) {
			if calls != 1 {
				return zero, ErrNextNotCalled
			}
			if !delegatedSucceeded {
				if delegatedFailure != nil {
					return zero, delegatedFailure
				}
				return zero, ErrNextNotCalled
			}
		}
		if err == nil && point.validateOut != nil {
			if validateErr := point.validateOut(out); validateErr != nil {
				return zero, validateErr
			}
		}
		if err == nil && point.validateResult != nil {
			if validateErr := point.validateResult(input, out); validateErr != nil {
				return zero, validateErr
			}
		}
		if err == nil && point.validateDelegated != nil && delegatedSucceeded {
			if validateErr := point.validateDelegated(delegatedOutput, out); validateErr != nil {
				return zero, validateErr
			}
		}
		if err != nil {
			var delegated *delegatedError
			if errors.As(err, &delegated) {
				return out, delegated.cause
			}
			failure := callbackFailure(entry, failureCode(err), err)
			report(plan.reporter, currentCtx, failure)
			err = &CallbackError{Point: failure.Point, InstanceID: failure.InstanceID, HandlerID: failure.HandlerID, Code: failure.Code, cause: failure.Cause}
		}
		return out, err
	}
	cloned, err := cloneInput(point.clone, input)
	if err != nil {
		return zero, err
	}
	return invoke(0, ctx, cloned)
}

func callAround[I, O any](ctx context.Context, around Around[I, O], input I, next Next[I, O]) (out O, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("extension interceptor panic: %v", recovered)
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
