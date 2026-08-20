package runtime

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	einoobs "github.com/mattsp1290/eino-obs"

	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

// Option configures a StreamingOrchestrator. Options are applied in order.
type Option func(*StreamingOrchestrator) error

// NewStreamingOrchestrator constructs and validates an orchestrator. It is the
// preferred construction path; direct struct literals remain supported.
//
// Admit is intentionally not configurable: it is derived from Store,
// Transactor, Events, Hooks, and Clock by the orchestrator.
func NewStreamingOrchestrator(opts ...Option) (*StreamingOrchestrator, error) {
	o := &StreamingOrchestrator{}
	for index, option := range opts {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOrchestrator, index)
		}
		if err := option(o); err != nil {
			return nil, err
		}
	}
	var missing []string
	if nilInterface(o.Store) {
		missing = append(missing, "Store")
	}
	if nilInterface(o.Model) {
		missing = append(missing, "Model")
	}
	if nilInterface(o.IDs) {
		missing = append(missing, "IDs")
	}
	if len(missing) != 0 {
		return nil, fmt.Errorf("%w: missing required dependencies: %s", ErrInvalidOrchestrator, strings.Join(missing, ", "))
	}
	if o.ModelRequestLedger {
		if _, ok := o.Store.(session.ModelRequestStore); !ok {
			return nil, fmt.Errorf("%w: model request ledger requires session.ModelRequestStore", ErrInvalidOrchestrator)
		}
	}
	return o, nil
}

func interfaceOption[T any](name string, value T, apply func(*StreamingOrchestrator, T)) Option {
	return func(o *StreamingOrchestrator) error {
		if nilInterface(value) {
			return fmt.Errorf("%w: %s cannot be nil", ErrInvalidOrchestrator, name)
		}
		apply(o, value)
		return nil
	}
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// WithStore sets the durable session store.
func WithStore(value session.Store) Option {
	return interfaceOption("Store", value, func(o *StreamingOrchestrator, value session.Store) { o.Store = value })
}

// WithTransactor sets the optional transactional session store.
func WithTransactor(value session.Transactor) Option {
	return interfaceOption("Transactor", value, func(o *StreamingOrchestrator, value session.Transactor) { o.Transactor = value })
}

// WithModelResolver sets the native model resolver.
func WithModelResolver(value model.Resolver) Option {
	return interfaceOption("ModelResolver", value, func(o *StreamingOrchestrator, value model.Resolver) { o.Model = value })
}

// WithToolRegistry sets the tool registry.
func WithToolRegistry(value ToolRegistry) Option {
	return interfaceOption("ToolRegistry", value, func(o *StreamingOrchestrator, value ToolRegistry) { o.Tools = value })
}

// WithRunPlanProvider configures immutable per-run extension plans.
func WithRunPlanProvider(value RunPlanProvider) Option {
	return interfaceOption("RunPlanProvider", value, func(o *StreamingOrchestrator, value RunPlanProvider) { o.Plans = value })
}

// WithContextSource appends a context source.
func WithContextSource(value ContextSource) Option {
	return interfaceOption("ContextSource", value, func(o *StreamingOrchestrator, value ContextSource) { o.Context = append(o.Context, value) })
}

// WithEventSink sets the event sink.
func WithEventSink(value EventSink) Option {
	return interfaceOption("EventSink", value, func(o *StreamingOrchestrator, value EventSink) { o.Events = value })
}

// WithHook appends a lifecycle hook.
func WithHook(value Hook) Option {
	return interfaceOption("Hook", value, func(o *StreamingOrchestrator, value Hook) { o.Hooks = append(o.Hooks, value) })
}

// WithPermissions sets the permission policy.
func WithPermissions(value permissions.Policy) Option {
	return interfaceOption("Permissions", value, func(o *StreamingOrchestrator, value permissions.Policy) { o.Permissions = value })
}

// WithToolMiddleware appends tool-call middleware.
func WithToolMiddleware(value ToolMiddleware) Option {
	return interfaceOption("ToolMiddleware", value, func(o *StreamingOrchestrator, value ToolMiddleware) { o.Middleware = append(o.Middleware, value) })
}

// WithIDGenerator sets the durable ID generator.
func WithIDGenerator(value IDGenerator) Option {
	return interfaceOption("IDGenerator", value, func(o *StreamingOrchestrator, value IDGenerator) { o.IDs = value })
}

// WithClock sets the clock. A nil clock retains the runtime time.Now fallback.
func WithClock(value func() time.Time) Option {
	return func(o *StreamingOrchestrator) error { o.Clock = value; return nil }
}

// WithOwnerID sets the durable work owner ID.
func WithOwnerID(value string) Option {
	return func(o *StreamingOrchestrator) error { o.OwnerID = value; return nil }
}

// WithTrace sets trace correlation metadata.
func WithTrace(value agentcontext.TraceContext) Option {
	return func(o *StreamingOrchestrator) error { o.Trace = value; return nil }
}

// WithAttempts sets the maximum provider attempts.
func WithAttempts(value int) Option {
	return func(o *StreamingOrchestrator) error { o.Attempts = value; return nil }
}

// WithToolTurns sets the maximum tool turns.
func WithToolTurns(value int) Option {
	return func(o *StreamingOrchestrator) error { o.ToolTurns = value; return nil }
}

// WithQueueSize sets the event queue size.
func WithQueueSize(value int) Option {
	return func(o *StreamingOrchestrator) error { o.QueueSize = value; return nil }
}

// WithSystemPromptMaterialization explicitly enables configured Agent system
// prompts at the provider boundary. It is disabled by default for compatibility.
func WithSystemPromptMaterialization(enabled bool) Option {
	return func(o *StreamingOrchestrator) error { o.SystemPromptMaterialization = enabled; return nil }
}

// WithModelRequestLedger enables the optional durable provider-attempt ledger.
func WithModelRequestLedger(enabled bool) Option {
	return func(o *StreamingOrchestrator) error { o.ModelRequestLedger = enabled; return nil }
}

// WithModelRequestSafeOptions allowlists model option keys that may be copied
// into audit records. No options are recorded by default.
func WithModelRequestSafeOptions(keys ...string) Option {
	return func(o *StreamingOrchestrator) error {
		o.ModelRequestSafeOptions = append([]string(nil), keys...)
		return nil
	}
}

// WithModelRequestMaxBytes tightens the default canonical ledger content cap.
func WithModelRequestMaxBytes(limit int) Option {
	return func(o *StreamingOrchestrator) error { o.ModelRequestMaxBytes = limit; return nil }
}

// WithLease sets the durable work lease duration.
func WithLease(value time.Duration) Option {
	return func(o *StreamingOrchestrator) error { o.Lease = value; return nil }
}

// WithHistory sets history projection options.
func WithHistory(value history.Options) Option {
	return func(o *StreamingOrchestrator) error { o.History = value; return nil }
}

// WithObserver sets the optional Eino observer. Nil preserves existing
// zero-value observer behavior.
func WithObserver(value *einoobs.Observer) Option {
	return func(o *StreamingOrchestrator) error { o.Observer = value; return nil }
}
