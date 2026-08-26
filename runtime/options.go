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
// only supported construction path.
func NewStreamingOrchestrator(opts ...Option) (*StreamingOrchestrator, error) {
	o := &StreamingOrchestrator{
		clock:                time.Now,
		ownerIDValue:         "runtime",
		attemptsValue:        1,
		toolTurnsValue:       8,
		queueSize:            1,
		leaseValue:           time.Minute,
		modelRequestMaxBytes: defaultModelRequestMaxBytes,
	}
	for index, option := range opts {
		if option == nil {
			return nil, fmt.Errorf("%w: option %d is nil", ErrInvalidOrchestrator, index)
		}
		if err := option(o); err != nil {
			return nil, err
		}
	}
	var missing []string
	if nilInterface(o.store) {
		missing = append(missing, "Store")
	}
	if nilInterface(o.model) {
		missing = append(missing, "Model")
	}
	if nilInterface(o.ids) {
		missing = append(missing, "IDs")
	}
	if len(missing) != 0 {
		return nil, fmt.Errorf("%w: missing required dependencies: %s", ErrInvalidOrchestrator, strings.Join(missing, ", "))
	}
	o.configured = true
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
	return interfaceOption("Store", value, func(o *StreamingOrchestrator, value session.Store) { o.store = value })
}

// WithModelResolver sets the native model resolver.
func WithModelResolver(value model.Resolver) Option {
	return interfaceOption("ModelResolver", value, func(o *StreamingOrchestrator, value model.Resolver) { o.model = value })
}

// WithRunPlanProvider configures immutable per-run extension plans.
func WithRunPlanProvider(value RunPlanProvider) Option {
	return interfaceOption("RunPlanProvider", value, func(o *StreamingOrchestrator, value RunPlanProvider) { o.plans = value })
}

// WithEventSink sets the event sink.
func WithEventSink(value EventSink) Option {
	return interfaceOption("EventSink", value, func(o *StreamingOrchestrator, value EventSink) { o.events = value })
}

// WithPermissions sets the permission policy.
func WithPermissions(value permissions.Policy) Option {
	return interfaceOption("Permissions", value, func(o *StreamingOrchestrator, value permissions.Policy) { o.permissions = value })
}

// WithIDGenerator sets the durable ID generator.
func WithIDGenerator(value IDGenerator) Option {
	return interfaceOption("IDGenerator", value, func(o *StreamingOrchestrator, value IDGenerator) { o.ids = value })
}

// WithClock sets the clock.
func WithClock(value func() time.Time) Option {
	return interfaceOption("Clock", value, func(o *StreamingOrchestrator, value func() time.Time) { o.clock = value })
}

// WithOwnerID sets the durable work owner ID.
func WithOwnerID(value string) Option {
	return func(o *StreamingOrchestrator) error {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%w: OwnerID cannot be empty", ErrInvalidOrchestrator)
		}
		o.ownerIDValue = value
		return nil
	}
}

// WithTrace sets trace correlation metadata.
func WithTrace(value agentcontext.TraceContext) Option {
	return func(o *StreamingOrchestrator) error {
		value.Attributes = cloneStringMap(value.Attributes)
		o.trace = value
		return nil
	}
}

// WithAttempts sets the maximum provider attempts.
func WithAttempts(value int) Option {
	return positiveIntOption("Attempts", value, func(o *StreamingOrchestrator, value int) { o.attemptsValue = value })
}

// WithToolTurns sets the maximum tool turns.
func WithToolTurns(value int) Option {
	return positiveIntOption("ToolTurns", value, func(o *StreamingOrchestrator, value int) { o.toolTurnsValue = value })
}

// WithQueueSize sets the event queue size.
func WithQueueSize(value int) Option {
	return positiveIntOption("QueueSize", value, func(o *StreamingOrchestrator, value int) { o.queueSize = value })
}

// WithModelRequestLedger enables the optional durable provider-attempt ledger.
func WithModelRequestLedger(enabled bool) Option {
	return func(o *StreamingOrchestrator) error { o.modelRequestLedger = enabled; return nil }
}

// WithModelRequestSafeOptions allowlists model option keys that may be copied
// into audit records. No options are recorded by default.
func WithModelRequestSafeOptions(keys ...string) Option {
	return func(o *StreamingOrchestrator) error {
		o.modelRequestSafeOptions = append([]string(nil), keys...)
		return nil
	}
}

// WithModelRequestMaxBytes tightens the default canonical ledger content cap.
func WithModelRequestMaxBytes(limit int) Option {
	return positiveIntOption("ModelRequestMaxBytes", limit, func(o *StreamingOrchestrator, value int) { o.modelRequestMaxBytes = value })
}

// WithLease sets the durable work lease duration.
func WithLease(value time.Duration) Option {
	return func(o *StreamingOrchestrator) error {
		if value <= 0 {
			return fmt.Errorf("%w: Lease must be positive", ErrInvalidOrchestrator)
		}
		o.leaseValue = value
		return nil
	}
}

// WithHistory sets history projection options.
func WithHistory(value history.Options) Option {
	return func(o *StreamingOrchestrator) error {
		if value.Epoch != nil {
			epoch := *value.Epoch
			value.Epoch = &epoch
		}
		o.history = value
		return nil
	}
}

// WithObserver sets the optional Eino observer. Nil preserves existing
// zero-value observer behavior.
func WithObserver(value *einoobs.Observer) Option {
	return func(o *StreamingOrchestrator) error { o.observer = value; return nil }
}

func positiveIntOption(name string, value int, apply func(*StreamingOrchestrator, int)) Option {
	return func(o *StreamingOrchestrator) error {
		if value <= 0 {
			return fmt.Errorf("%w: %s must be positive", ErrInvalidOrchestrator, name)
		}
		apply(o, value)
		return nil
	}
}
