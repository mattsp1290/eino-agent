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
	sessionID   session.ID
	runID       session.RunID
	workspaceID string
	host        *StreamingOrchestrator
	plan        *RunPlan
	store       session.ExecutionStore
	lease       *runLeaseHeartbeat
	events      *eventQueue

	durableMessageMu          sync.Mutex
	durableMessageFloor       time.Time
	durableMessageInitialized bool

	// discoveredMu guards discovered, the per-execution advertised set of
	// deferred tool names the model has discovered via the tool-search tool
	// (see runtime/tool_search.go). It starts empty for a fresh run;
	// StreamingOrchestrator seeds it from durable history on resume via
	// discoveredToolsFromHistory.
	discoveredMu sync.Mutex
	discovered   map[string]bool
}

// markDiscovered adds names to the per-execution advertised set.
func (e *runExecution) markDiscovered(names ...string) {
	if len(names) == 0 {
		return
	}
	e.discoveredMu.Lock()
	defer e.discoveredMu.Unlock()
	if e.discovered == nil {
		e.discovered = make(map[string]bool, len(names))
	}
	for _, name := range names {
		e.discovered[name] = true
	}
}

// seedDiscovered replaces the per-execution advertised set, used to restore
// it from durable history on resume.
func (e *runExecution) seedDiscovered(names []string) {
	if len(names) == 0 {
		return
	}
	e.markDiscovered(names...)
}

// discoveredSnapshot returns a defensive copy of the current advertised set.
func (e *runExecution) discoveredSnapshot() map[string]bool {
	e.discoveredMu.Lock()
	defer e.discoveredMu.Unlock()
	if len(e.discovered) == 0 {
		return nil
	}
	snapshot := make(map[string]bool, len(e.discovered))
	for name := range e.discovered {
		snapshot[name] = true
	}
	return snapshot
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
	// Allocation is monotonic per execution. Persistence failures may leave
	// gaps and must never roll the floor backward.
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
	host.sessionObserver.Hint(run.SessionID)
	return &runExecution{sessionID: run.SessionID, runID: run.ID, workspaceID: run.Config["workspace_id"], host: host, plan: plan, store: store, events: newEventQueue(host.queueSize, host.events)}
}

func (e *runExecution) dispatch() *extension.Plan {
	if e == nil || e.plan == nil {
		return nil
	}
	return e.plan.dispatch
}

func (e *runExecution) release() {
	if e != nil {
		e.host.sessionObserver.ReleaseRun(e.sessionID, e.runID)
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
	e.host.sessionObserver.Hint(record.SessionID)
	runEventSink{infrastructure: e.events, plan: e.dispatch()}.publishPersisted(infrastructureCtx, notificationCtx, record)
}
