package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// Resume reclaims resumable durable work for runID. The current implementation
// resumes the durable tool-call boundary: pending calls are atomically claimed
// before execution, terminal calls are skipped, and active non-owned leases are
// rejected.
func (o *StreamingOrchestrator) Resume(ctx context.Context, runID session.RunID) (Handle, error) {
	if err := o.validateConfigured(); err != nil {
		return nil, err
	}
	if runID == "" {
		return nil, fmt.Errorf("%w: run id required", ErrInvalidOrchestrator)
	}
	run, err := o.store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	var plan *RunPlan
	if !run.Terminal() {
		plan, err = o.acquireResumePlan(ctx, run.ExtensionPlan)
		if err != nil {
			return nil, err
		}
		run, err = o.store.ClaimRun(ctx, session.RunClaim{
			RunID: run.ID, OwnerID: o.ownerID(), ClaimToken: string(o.ids.NewEventID()), LeaseDuration: o.lease(),
		})
		if err != nil {
			plan.release()
			if errors.Is(err, session.ErrConflict) || errors.Is(err, session.ErrSessionBusy) {
				return nil, session.ErrSessionBusy
			}
			return nil, err
		}
	}
	ownershipTransferred := false
	defer func() {
		if !ownershipTransferred && plan != nil {
			plan.release()
		}
	}()
	execution := newRunExecution(o, plan)
	execution.bindRun(run)
	runCtx, cancel := context.WithCancel(ctx)
	handle := &streamingHandle{
		runID:       runID,
		cancel:      cancel,
		done:        make(chan Result, 1),
		onInterrupt: func(reason string) { o.observeInterrupt(context.WithoutCancel(ctx), run, "", reason) },
	}
	ownershipTransferred = true
	go o.executeResume(runCtx, execution, run, handle.done)
	return handle, nil
}

func (o *StreamingOrchestrator) executeResume(ctx context.Context, execution *runExecution, run session.Run, done chan<- Result) {
	defer close(done)
	defer execution.release()
	if execution.store == nil {
		execution.bindRun(run)
	}
	if !run.Terminal() {
		ctx = execution.startLease(ctx, o.lease())
	}
	resumeStartedAt := o.now()
	settled := false
	result := o.resumeRunWithSettlement(ctx, execution, run, &settled)
	if leaseErr := execution.stopLease(); leaseErr != nil && result.Error == nil {
		result.Status = session.RunFailed
		result.Error = leaseErr
	}
	if !settled && !run.Terminal() {
		result = o.finishResume(ctx, execution, run, result, &settled)
	}
	if settled && !run.Terminal() {
		_ = extension.Notify(execution.dispatch(), context.WithoutCancel(ctx), RunSettledPoint, RunSettledNotice{SessionID: run.SessionID, Result: result, Duration: o.now().Sub(resumeStartedAt), Error: classifyExtensionError(result.Error)})
	}
	done <- result
}

func (o *StreamingOrchestrator) resumeRunWithSettlement(ctx context.Context, execution *runExecution, run session.Run, settled *bool) (result Result) {
	if execution.store == nil {
		execution.bindRun(run)
	}
	if run.Terminal() {
		return Result{RunID: run.ID, Status: run.Status, Interrupted: run.Status == session.RunInterrupted, Error: errorString(run.Error)}
	}
	o.observeResume(ctx, run, "resume")
	run.StartedAt = o.now()
	observed := o.startObservedRun(ctx, run, "", run.StartedAt)
	defer func() {
		o.finishObservedRun(observed, result, o.now())
	}()
	started, err := execution.store.StartRun(ctx, run.StartedAt)
	if err != nil {
		return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
	}
	run = started
	calls, err := o.store.ListUnfinishedToolCalls(ctx, run.ID)
	if err != nil {
		return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
	}
	if len(calls) == 0 {
		return o.finishResume(ctx, execution, run, Result{RunID: run.ID, Status: session.RunInterrupted, Interrupted: true}, settled)
	}
	snapshot := o.resumeSnapshot(run)
	tools, toolContext, err := o.resumeTools(ctx, execution, run)
	if err != nil {
		return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
	}
	for _, call := range calls {
		if session.TerminalToolCall(call.Status) {
			continue
		}
		tool, ok := tools[call.Name]
		if !ok || tool.Executor == nil {
			return Result{RunID: run.ID, Status: session.RunFailed, Error: fmt.Errorf("tool %q unavailable", call.Name)}
		}
		if call.Status == session.ToolCallRunning {
			if _, err := execution.settleInterruptedRunningTool(ctx, run, tool, call); err != nil {
				return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
			}
			continue
		}
		claimed := call
		claimed.Status = session.ToolCallRunning
		claimed.ClaimedBy = o.ownerID()
		if claimed.ClaimToken == "" || claimed.ClaimedBy != call.ClaimedBy {
			claimed.ClaimToken = string(o.ids.NewEventID())
		}
		claimed.StartedAt = o.now()
		claimed, err = execution.store.ClaimToolCall(ctx, claimed, o.lease())
		if err != nil {
			if errors.Is(err, session.ErrConflict) {
				return Result{RunID: run.ID, Status: session.RunFailed, Error: session.ErrSessionBusy}
			}
			return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
		}
		_ = extension.Notify(execution.dispatch(), ctx, ToolStartedPoint, ToolStartedNotice{SessionID: run.SessionID, RunID: run.ID, ToolCallID: claimed.ID, ToolName: claimed.Name, Time: claimed.StartedAt})
		toolCall := ToolCall{
			ID:              claimed.ID,
			SessionID:       claimed.SessionID,
			RunID:           claimed.RunID,
			MessageID:       claimed.MessageID,
			ResultMessageID: claimed.ResultMessageID,
			ResultPartID:    claimed.ResultPartID,
			Name:            claimed.Name,
			Scope:           tool.Scope,
			Pattern:         toolPattern(claimed.Input, claimed.Name),
			Input:           cloneJSON(claimed.Input),
			Context:         toolContext.Clone(),
		}
		settledTool, err := execution.executeAndSettleClaimedTool(ctx, snapshot, tool, toolCall, claimed, nil)
		if err != nil {
			return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
		}
		if errors.Is(settledTool.Outcome.RawError, errToolExecutionPanic) {
			return o.finishResume(ctx, execution, run, Result{RunID: run.ID, Status: session.RunFailed, Error: settledTool.Outcome.RawError}, settled)
		}
		if errors.Is(settledTool.Outcome.RawError, context.Canceled) {
			return o.finishResume(ctx, execution, run, Result{RunID: run.ID, Status: session.RunInterrupted, Interrupted: true, Error: settledTool.Outcome.RawError}, settled)
		}
	}
	return o.finishResume(ctx, execution, run, Result{RunID: run.ID, Status: session.RunInterrupted, Interrupted: true}, settled)
}

func (o *StreamingOrchestrator) resumeTools(ctx context.Context, execution *runExecution, run session.Run) (map[string]Tool, ToolContext, error) {
	if len(execution.plan.tools.capabilities) == 0 {
		return nil, ToolContext{}, fmt.Errorf("%w: tool registry required", ErrInvalidOrchestrator)
	}
	snapshot := o.resumeSnapshot(run)
	resolved, err := execution.plan.ResolveTools(ctx, NewToolScopeContext(snapshot))
	if err != nil {
		return nil, ToolContext{}, err
	}
	sort.Slice(resolved, func(i, j int) bool { return resolved[i].Name < resolved[j].Name })
	snapshot.Tools = cloneSlice(resolved)
	tools := make(map[string]Tool, len(resolved))
	for _, tool := range resolved {
		if _, exists := tools[tool.Name]; exists {
			return nil, ToolContext{}, fmt.Errorf("duplicate effective tool %q", tool.Name)
		}
		tools[tool.Name] = tool
	}
	return tools, toolContext(snapshot, resolved), nil
}

func (o *StreamingOrchestrator) resumeSnapshot(run session.Run) TurnSnapshot {
	return TurnSnapshot{
		RunID:     run.ID,
		SessionID: run.SessionID,
		EpochID:   run.ContextEpoch,
		Config: config.Snapshot{
			Agent: config.Agent{Name: run.Agent},
			Model: model.Selection{ProviderID: model.ProviderID(run.ProviderID), ModelID: model.ID(run.ModelID)},
			Metadata: map[string]string{
				"workspace_id":   run.Config["workspace_id"],
				"workspace_root": run.Config["workspace_root"],
			},
		},
		Model: model.Resolved{
			Provider: model.Provider{ID: model.ProviderID(run.ProviderID)},
			Model:    model.Descriptor{ID: model.ID(run.ModelID), ProviderID: model.ProviderID(run.ProviderID)},
		},
		CreatedAt: o.now(),
	}
}

func errorString(value string) error {
	if value == "" {
		return nil
	}
	return errors.New(value)
}

func (o *StreamingOrchestrator) finishResume(ctx context.Context, execution *runExecution, run session.Run, result Result, settled *bool) Result {
	if leaseErr := execution.stopLease(); leaseErr != nil && result.Error == nil {
		result.Status = session.RunFailed
		result.Error = leaseErr
	}
	run.Status = result.Status
	run.FinishedAt = o.now()
	if result.Error != nil {
		run.Error = result.Error.Error()
	}
	finalEvent := o.finalRunEvent(run, result)
	if err := execution.store.SettleRun(context.WithoutCancel(ctx), run, finalEvent); err != nil {
		if result.Error == nil {
			result.Status = session.RunFailed
			result.Error = err
		}
	} else {
		if settled != nil {
			*settled = true
		}
		o.publishRunFinished(ctx, execution, finalEvent, result)
	}
	return result
}
