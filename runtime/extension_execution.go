package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

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

	durableMessageMu          sync.Mutex
	durableMessageFloor       time.Time
	durableMessageInitialized bool
}

func (e *runExecution) seedDurableMessageFloor(at time.Time) {
	if e == nil || at.IsZero() {
		return
	}
	e.durableMessageMu.Lock()
	defer e.durableMessageMu.Unlock()
	at = at.UTC()
	if !e.durableMessageInitialized || at.After(e.durableMessageFloor) {
		e.durableMessageFloor = at
	}
	e.durableMessageInitialized = true
}

func (e *runExecution) nextDurableMessageTime(ctx context.Context, sessionID session.ID, observed time.Time) (time.Time, error) {
	e.durableMessageMu.Lock()
	defer e.durableMessageMu.Unlock()
	if !e.durableMessageInitialized {
		latest, err := latestAdmissionMessageTime(context.WithoutCancel(ctx), e.host.store, sessionID)
		if err != nil {
			return time.Time{}, err
		}
		e.durableMessageFloor = latest
		e.durableMessageInitialized = true
	}
	observed = observed.UTC()
	if !observed.After(e.durableMessageFloor) {
		observed = e.durableMessageFloor.Add(time.Nanosecond)
	}
	e.durableMessageFloor = observed
	return observed, nil
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
