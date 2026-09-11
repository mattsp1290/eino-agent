package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cloudwego/eino/adk"
	einomodel "github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
)

// defaultRetryBaseDelay/defaultRetryMaxDelay bound the exponential backoff
// used when a run configures more than one attempt (see
// StreamingOrchestrator.attemptsValue / WithAttempts).
const (
	defaultRetryBaseDelay = 100 * time.Millisecond
	defaultRetryMaxDelay  = 10 * time.Second
)

// defaultRetryConfig maps the orchestrator's legacy WithAttempts/attemptsValue
// setting (total physical attempts per dispatch, including the first) onto
// adk.TypedModelRetryConfig. Every physical attempt still passes through
// adkModel unchanged -- each is its own audited ledger row with its own
// InvocationID -- since ADK's retry wrapper simply calls the wrapped model
// (adkModel) again on a retryable outcome.
func defaultRetryConfig(attempts int) *adk.TypedModelRetryConfig[*einoschema.AgenticMessage] {
	if attempts <= 1 {
		return nil
	}
	return &adk.TypedModelRetryConfig[*einoschema.AgenticMessage]{
		MaxRetries:  attempts - 1,
		ShouldRetry: defaultShouldRetry,
		BackoffFunc: defaultRetryBackoff,
	}
}

// defaultShouldRetry retries any physical-call error except one that is (or
// wraps) a graph-level interrupt-rerun error: upstream's retry wrapper only
// recognizes compose.IsInterruptRerunError-shaped errors as "already
// resumable state", and re-dispatching over one would join partial output
// from an old attempt with a new one, or worse, duplicate a durably paused
// interrupt. It also refuses to retry an error from a dispatch whose durable
// output was already committed (errCommittedDispatchFailed) -- see that
// sentinel's doc comment. A successful result (Err == nil) is never retried.
func defaultShouldRetry(_ context.Context, retryCtx *adk.TypedRetryContext[*einoschema.AgenticMessage]) *adk.TypedRetryDecision[*einoschema.AgenticMessage] {
	if retryCtx == nil || retryCtx.Err == nil {
		return &adk.TypedRetryDecision[*einoschema.AgenticMessage]{Retry: false}
	}
	if _, ok := compose.IsInterruptRerunError(retryCtx.Err); ok {
		return &adk.TypedRetryDecision[*einoschema.AgenticMessage]{Retry: false}
	}
	if errors.Is(retryCtx.Err, errCommittedDispatchFailed) {
		return &adk.TypedRetryDecision[*einoschema.AgenticMessage]{Retry: false}
	}
	if errors.Is(retryCtx.Err, errPartialStreamObserved) {
		return &adk.TypedRetryDecision[*einoschema.AgenticMessage]{Retry: false}
	}
	// A tool-call id that publicizeToolCallIDs could not resolve is a
	// durable-consistency failure, not a transient one (see
	// errToolCallIDUnresolved's doc comment): it fails the same way on
	// every attempt. publicizeToolCallIDs runs at the very top of begin,
	// before prepareModelRequest, so an unrefused retry here would not
	// write an extra ledger row or attempt_replaced event -- it costs
	// nothing durable. What it does cost is real: one wasted store read per
	// attempt, the retry policy's exponential backoff between attempts, and
	// the sentinel itself -- once every attempt is exhausted, ADK's own
	// "exceeds max retries: last error: ..." wrapping around the final
	// attempt's error can leave errors.Is(result.Error,
	// errToolCallIDUnresolved) false by the time the run's caller inspects
	// it, even though this same deterministic failure caused every attempt.
	if errors.Is(retryCtx.Err, errToolCallIDUnresolved) {
		return &adk.TypedRetryDecision[*einoschema.AgenticMessage]{Retry: false}
	}
	return &adk.TypedRetryDecision[*einoschema.AgenticMessage]{Retry: true}
}

func defaultRetryBackoff(_ context.Context, attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	delay := defaultRetryBaseDelay
	for i := 1; i < attempt && delay < defaultRetryMaxDelay; i++ {
		delay *= 2
	}
	if delay > defaultRetryMaxDelay {
		delay = defaultRetryMaxDelay
	}
	return delay
}

// FailoverPolicy is a serializable descriptor for an ordered list of
// alternate model selections to try, in order, when the primary (or a prior
// failover) model call fails. It is frozen on RunPlanSpec.Failover, like
// RunPlanSpec.Agent -- host construction config, not durable capability
// evidence, so it is deliberately not part of the sealed fingerprint.
type FailoverPolicy struct {
	Models []model.Selection
}

// buildFailoverConfig reconstructs adk.ModelFailoverConfig from a
// FailoverPolicy for one turn's engine. Each failover attempt resolves its
// model through the same model.Resolver every primary dispatch uses (never
// a cached/pinned client) and dispatches through a fresh *adkModel sharing
// this turn's engine/execution/approval binding, so it is its own audited
// ledger row (own InvocationID) exactly like a retry attempt; provider
// state restoration for that attempt's own dispatch goes through the same
// durableProjection contract check (loadProviderHistory validates codec
// compatibility against activeModel()), so an incompatible failover target
// fails before dispatch rather than silently dropping or corrupting state.
func buildFailoverConfig(engine *adkEngine, approval *adkApprovalBinding, policy *FailoverPolicy) *adk.ModelFailoverConfig[*einoschema.AgenticMessage] {
	if policy == nil || len(policy.Models) == 0 {
		return nil
	}
	return &adk.ModelFailoverConfig[*einoschema.AgenticMessage]{
		MaxRetries:     uint(len(policy.Models)),
		ShouldFailover: defaultShouldFailover,
		GetFailoverModel: func(ctx context.Context, failoverCtx *adk.FailoverContext[*einoschema.AgenticMessage]) (einomodel.BaseModel[*einoschema.AgenticMessage], []*einoschema.AgenticMessage, error) {
			if failoverCtx.FailoverAttempt == 0 || int(failoverCtx.FailoverAttempt) > len(policy.Models) {
				return nil, nil, fmt.Errorf("%w: failover attempt %d out of range for %d configured models", ErrInvalidOrchestrator, failoverCtx.FailoverAttempt, len(policy.Models))
			}
			selection := policy.Models[failoverCtx.FailoverAttempt-1]
			resolved, err := engine.host.model.Resolve(ctx, selection, model.Runtime{})
			if err != nil {
				return nil, nil, err
			}
			return &adkModel{host: engine.host, execution: engine.execution, engine: engine, approval: approval, resolvedOverride: &resolved}, nil, nil
		},
	}
}

// defaultShouldFailover refuses to fail over on a graph-level interrupt-rerun
// error (see defaultShouldRetry) and, per the same reasoning, on an error
// from a physical dispatch whose durable output was already committed: this
// adapter's model.Generate/Stream only ever return an error before
// adkModel.commit succeeds (commit failures themselves surface as the
// returned error, but by then the assistant message/tool-call records may
// already be durably written), so failing over past that point would risk
// a second physical call whose result this package has no slot left to
// commit against. A successful result (Err == nil) is never failed over.
func defaultShouldFailover(_ context.Context, _ *einoschema.AgenticMessage, outputErr error) bool {
	if outputErr == nil {
		return false
	}
	if _, ok := compose.IsInterruptRerunError(outputErr); ok {
		return false
	}
	if errors.Is(outputErr, errCommittedDispatchFailed) {
		return false
	}
	if errors.Is(outputErr, errPartialStreamObserved) {
		return false
	}
	// See defaultShouldRetry's matching check: a tool-call id
	// publicizeToolCallIDs could not resolve fails closed deterministically,
	// so failing over to another model would not help.
	if errors.Is(outputErr, errToolCallIDUnresolved) {
		return false
	}
	return true
}
