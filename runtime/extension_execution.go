package runtime

import (
	"context"
	"fmt"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

// runExecution owns the frozen extension plan for one fresh or resumed run.
// Request contexts carry cancellation and request values, never plan identity.
type runExecution struct {
	host   *StreamingOrchestrator
	plan   *RunPlan
	store  session.ExecutionStore
	lease  *runLeaseHeartbeat
	events *eventQueue
}

func newRunExecution(host *StreamingOrchestrator, plan *RunPlan, run session.Run) *runExecution {
	if host == nil || host.store == nil || plan == nil || run.ID == "" || run.ClaimToken == "" {
		panic(fmt.Sprintf("invalid run execution fence for run %q", run.ID))
	}
	store := host.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	if store == nil {
		panic(fmt.Sprintf("nil run execution store for run %q", run.ID))
	}
	return &runExecution{host: host, plan: plan, store: store, events: newEventQueue(host.queueSize, host.events)}
}

func (e *runExecution) dispatch() *extension.Plan {
	if e == nil || e.plan == nil {
		return nil
	}
	return e.plan.dispatch
}

func (e *runExecution) release() {
	if e != nil {
		e.events.close()
		if e.plan != nil {
			e.plan.release()
		}
	}
}

func (e *runExecution) eventSink() EventSink {
	if e == nil {
		return nil
	}
	return runEventSink{infrastructure: e.events, plan: e.dispatch()}
}

func (e *runExecution) publishPersisted(ctx context.Context, record session.EventRecord) {
	if e == nil {
		return
	}
	e.publishPersistedWithNotificationContext(ctx, context.WithoutCancel(ctx), record)
}

func (e *runExecution) publishPersistedWithNotificationContext(infrastructureCtx, notificationCtx context.Context, record session.EventRecord) {
	if e == nil {
		return
	}
	runEventSink{infrastructure: e.events, plan: e.dispatch()}.publishPersisted(infrastructureCtx, notificationCtx, record)
}
