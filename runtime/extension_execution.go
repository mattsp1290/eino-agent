package runtime

import "github.com/mattsp1290/eino-agent/extension"

// runExecution owns the frozen extension plan for one fresh or resumed run.
// Request contexts carry cancellation and request values, never plan identity.
type runExecution struct {
	host *StreamingOrchestrator
	plan *RunPlan
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
	return e.plan.Dispatch
}

func (e *runExecution) release() {
	if e != nil && e.plan != nil {
		e.plan.release()
	}
}

func (e *runExecution) eventSink(infrastructure EventSink) EventSink {
	if e == nil || e.dispatch() == nil {
		return infrastructure
	}
	return compositeEventSink{infrastructure: infrastructure, plan: e.dispatch()}
}
