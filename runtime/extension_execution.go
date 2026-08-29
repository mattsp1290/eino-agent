package runtime

import (
	"context"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

// runExecution owns the frozen extension plan for one fresh or resumed run.
// Request contexts carry cancellation and request values, never plan identity.
type runExecution struct {
	host  *StreamingOrchestrator
	plan  *RunPlan
	store session.ExecutionStore
	lease *runLeaseHeartbeat
}

func (e *runExecution) bindRun(run session.Run) {
	if e == nil || e.host == nil || e.host.store == nil {
		return
	}
	e.store = e.host.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
}

func (e *runExecution) ensureStore(ctx context.Context, runID session.RunID) error {
	if e.store != nil {
		return nil
	}
	if e == nil || e.host == nil || e.host.store == nil {
		return session.ErrConflict
	}
	run, err := e.host.store.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	e.bindRun(run)
	if e.store == nil {
		return session.ErrConflict
	}
	return nil
}

func newRunExecution(host *StreamingOrchestrator, plan *RunPlan) *runExecution {
	if plan == nil {
		plan = &RunPlan{}
	}
	return &runExecution{host: host, plan: plan}
}

func (e *runExecution) dispatch() *extension.Plan {
	if e == nil || e.plan == nil {
		return nil
	}
	return e.plan.dispatch
}

func (e *runExecution) release() {
	if e != nil && e.plan != nil {
		e.plan.release()
	}
}

func (e *runExecution) eventSink(infrastructure EventSink) EventSink {
	if e == nil {
		return infrastructure
	}
	if e.store == nil && infrastructure == nil && e.dispatch() == nil {
		return nil
	}
	return runEventSink{execution: e, infrastructure: infrastructure, plan: e.dispatch()}
}

func (e *runExecution) publishPersisted(ctx context.Context, infrastructure EventSink, record session.EventRecord) {
	if e == nil {
		if infrastructure != nil {
			_ = infrastructure.Emit(ctx, runtimeEventRecord(record))
		}
		return
	}
	runEventSink{execution: e, infrastructure: infrastructure, plan: e.dispatch()}.publishPersisted(ctx, context.WithoutCancel(ctx), record)
}
