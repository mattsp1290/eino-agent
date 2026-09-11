package runtime

import (
	"context"
	"time"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/compose"
	einoschema "github.com/cloudwego/eino/schema"
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
// interrupt. A successful result (Err == nil) is never retried.
func defaultShouldRetry(_ context.Context, retryCtx *adk.TypedRetryContext[*einoschema.AgenticMessage]) *adk.TypedRetryDecision[*einoschema.AgenticMessage] {
	if retryCtx == nil || retryCtx.Err == nil {
		return &adk.TypedRetryDecision[*einoschema.AgenticMessage]{Retry: false}
	}
	if _, ok := compose.IsInterruptRerunError(retryCtx.Err); ok {
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
