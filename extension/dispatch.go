package extension

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

type delegatedError struct{ cause error }

func (e *delegatedError) Error() string { return e.cause.Error() }
func (e *delegatedError) Unwrap() error { return e.cause }

func Notify[T any](plan *Plan, ctx context.Context, point Notification[T], value T) Failures {
	if plan == nil || point.key == nil {
		return nil
	}
	var failures Failures
	for _, entry := range plan.entries {
		if entry.point != point.key || entry.kind != entryNotification {
			continue
		}
		observer, ok := entry.callback.(Observer[T])
		if !ok {
			failure := callbackFailure(entry, "callback_type", fmt.Errorf("observer type mismatch"))
			failures = appendBounded(failures, failure)
			report(plan.reporter, ctx, failure)
			continue
		}
		input := value
		if point.clone != nil {
			input = point.clone(value)
		}
		err := callObserver(context.WithValue(ctx, callbackMountKey{}, entry.spec.InstanceID), observer, input)
		if err == nil {
			continue
		}
		failure := callbackFailure(entry, failureCode(err), err)
		report(plan.reporter, ctx, failure)
		if point.policy == NotificationReturnFailures {
			failures = appendBounded(failures, failure)
		}
	}
	return failures
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
		return terminal(ctx, cloneInput(point.clone, input))
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
			out, err := terminal(currentCtx, cloneInput(point.clone, candidate))
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
		var calls atomic.Int32
		var delegatedOutput O
		var delegatedSucceeded bool
		next := func(nextCtx context.Context, nextInput I) (O, error) {
			if calls.Add(1) != 1 {
				return zero, ErrNextCalledTwice
			}
			if nextCtx == nil {
				nextCtx = currentCtx
			}
			out, err := invoke(index+1, nextCtx, cloneInput(point.clone, nextInput))
			if err != nil {
				return out, &delegatedError{cause: err}
			}
			delegatedOutput = out
			delegatedSucceeded = true
			return out, nil
		}
		out, err := callAround(context.WithValue(currentCtx, callbackMountKey{}, entry.spec.InstanceID), around, cloneInput(point.clone, candidate), next)
		count := calls.Load()
		if count > 1 {
			return zero, ErrNextCalledTwice
		}
		if err == nil && (point.requireNext || entry.requireNext) && count != 1 {
			return zero, ErrNextNotCalled
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
	return invoke(0, ctx, cloneInput(point.clone, input))
}

func callAround[I, O any](ctx context.Context, around Around[I, O], input I, next Next[I, O]) (out O, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("extension interceptor panic: %v", recovered)
		}
	}()
	return around(ctx, input, next)
}

func cloneInput[T any](clone CloneFunc[T], input T) T {
	if clone == nil {
		return input
	}
	return clone(input)
}

func callbackFailure(entry plannedEntry, code string, cause error) Failure {
	return Failure{Point: entry.contract, InstanceID: entry.spec.InstanceID, HandlerID: entry.spec.ID, Code: code, Cause: cause}
}

func failureCode(err error) string {
	if err == nil {
		return ""
	}
	return "callback_failed"
}

func appendBounded(failures Failures, failure Failure) Failures {
	if len(failures) >= maxReportedFailures {
		return failures
	}
	return append(failures, failure)
}

func report(reporter Reporter, ctx context.Context, failure Failure) {
	if reporter == nil {
		return
	}
	defer func() { _ = recover() }()
	reporter.Report(ctx, Diagnostic(failure))
}
