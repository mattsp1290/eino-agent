package runtime

import (
	"context"
	"errors"
	"fmt"

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
	if o == nil || o.Store == nil {
		return nil, fmt.Errorf("%w: store required", ErrInvalidOrchestrator)
	}
	if runID == "" {
		return nil, fmt.Errorf("%w: run id required", ErrInvalidOrchestrator)
	}
	if o.IDs == nil {
		return nil, fmt.Errorf("%w: id generator required", ErrInvalidOrchestrator)
	}
	run, err := o.Store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	state := session.ClassifyResume(run, o.ownerID(), o.now())
	if state == session.ResumeBusy {
		return nil, session.ErrSessionBusy
	}
	var plan *RunPlan
	if !run.Terminal() {
		plan, err = o.acquireResumePlan(ctx, run.ExtensionPlan)
		if err != nil {
			return nil, err
		}
		ctx = withRunPlan(ctx, plan)
	}
	runCtx, cancel := context.WithCancel(ctx)
	handle := &streamingHandle{
		runID:       runID,
		cancel:      cancel,
		done:        make(chan Result, 1),
		onInterrupt: func(reason string) { o.observeInterrupt(context.WithoutCancel(ctx), run, "", reason) },
	}
	go o.executeResume(withRunPlan(runCtx, plan), run, handle.done)
	return handle, nil
}

func (o *StreamingOrchestrator) executeResume(ctx context.Context, run session.Run, done chan<- Result) {
	defer close(done)
	plan := runPlanFromContext(ctx)
	if plan != nil {
		defer plan.release()
	}
	result := o.resumeRun(ctx, run)
	if plan != nil && plan.Dispatch != nil && !run.Terminal() {
		_ = extension.Notify(plan.Dispatch, context.WithoutCancel(ctx), RunSettledPoint, RunSettledNotice{SessionID: run.SessionID, Result: result, Duration: o.now().Sub(run.StartedAt), Error: classifyExtensionError(result.Error)})
	}
	done <- result
}

func (o *StreamingOrchestrator) resumeRun(ctx context.Context, run session.Run) (result Result) {
	if run.Terminal() {
		return Result{RunID: run.ID, Status: run.Status, Interrupted: run.Status == session.RunInterrupted, Error: errorString(run.Error)}
	}
	o.observeResume(ctx, run, "resume")
	run.OwnerID = o.ownerID()
	run.Status = session.RunRunning
	run.LeaseUntil = o.now().Add(o.lease())
	run.StartedAt = o.now()
	observed := o.startObservedRun(ctx, run, "", run.StartedAt)
	defer func() {
		o.finishObservedRun(observed, result, o.now())
	}()
	if err := o.Store.FinishRun(ctx, run); err != nil {
		return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
	}
	var settlementStore session.ToolSettlementStore
	if plan := runPlanFromContext(ctx); plan != nil && descriptorRequiresToolSettlement(plan.Descriptor) {
		var ok bool
		settlementStore, ok = o.Store.(session.ToolSettlementStore)
		if !ok {
			return Result{RunID: run.ID, Status: session.RunFailed, Error: fmt.Errorf("%w: strict tool plan requires ToolSettlementStore", ErrInvalidOrchestrator)}
		}
		unreconciled, err := settlementStore.ListUnreconciledToolSettlements(ctx, run.ID)
		if err != nil {
			return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
		}
		for _, settlement := range unreconciled {
			if err := settlementStore.SettleToolCall(ctx, settlement); err != nil {
				return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
			}
		}
	}
	calls, err := o.Store.ListUnfinishedToolCalls(ctx, run.ID)
	if err != nil {
		return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
	}
	if len(calls) == 0 {
		return o.finishResume(ctx, run, Result{RunID: run.ID, Status: session.RunInterrupted, Interrupted: true})
	}
	snapshot := o.resumeSnapshot(run)
	tools, err := o.resumeTools(ctx, run)
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
			call.Status = session.ToolCallInterrupted
			call.CompletedAt = o.now()
			call.Error = "tool was running during resume and was not re-executed"
			if len(call.Output) == 0 {
				call.Output = mustJSON(toolOutputPayload{ToolCallID: string(call.ID), Status: "interrupted", Content: "tool execution interrupted"})
			}
			if settlementStore != nil {
				if call.ResultMessageID == "" || call.ResultPartID == "" {
					return Result{RunID: run.ID, Status: session.RunFailed, Error: session.ErrConflict}
				}
				err := settlementStore.SettleToolCall(context.WithoutCancel(ctx), session.ToolSettlement{
					ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken, Status: call.Status, Output: cloneJSON(call.Output), Error: call.Error, Metadata: cloneStringMap(call.Metadata), CompletedAt: call.CompletedAt,
					ResultMessage: session.Message{ID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, ParentID: call.MessageID, Role: session.RoleTool, ModelID: run.ModelID, CreatedAt: call.CompletedAt, UpdatedAt: call.CompletedAt},
					ResultPart:    session.Part{ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, Kind: session.PartToolResult, Payload: cloneJSON(call.Output), CreatedAt: call.CompletedAt, UpdatedAt: call.CompletedAt},
				})
				if err != nil {
					return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
				}
			} else {
				if err := o.Store.FinishToolCall(context.WithoutCancel(ctx), call); err != nil {
					return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
				}
				if err := o.appendToolResult(context.WithoutCancel(ctx), run, call, call.Output); err != nil {
					return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
				}
			}
			if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
				_ = extension.Notify(plan.Dispatch, context.WithoutCancel(ctx), ToolSettledPoint, ToolSettledNotice{SessionID: run.SessionID, RunID: run.ID, ToolCallID: call.ID, ToolName: call.Name, Status: call.Status, Error: classifyExtensionError(context.Canceled)})
			}
			continue
		}
		claimed := call
		claimed.Status = session.ToolCallRunning
		claimed.ClaimedBy = o.ownerID()
		if claimed.ClaimToken == "" || claimed.ClaimedBy != call.ClaimedBy {
			claimed.ClaimToken = string(o.IDs.NewEventID())
		}
		claimed.LeaseUntil = o.now().Add(o.lease())
		claimed.StartedAt = o.now()
		claimed, err = o.Store.ClaimToolCall(ctx, claimed)
		if err != nil {
			if errors.Is(err, session.ErrConflict) {
				return Result{RunID: run.ID, Status: session.RunFailed, Error: session.ErrSessionBusy}
			}
			return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
		}
		toolCall := ToolCall{
			ID:        claimed.ID,
			SessionID: claimed.SessionID,
			RunID:     claimed.RunID,
			MessageID: claimed.MessageID,
			Name:      claimed.Name,
			Scope:     tool.Scope,
			Pattern:   toolPattern(claimed.Input, claimed.Name),
			Input:     cloneJSON(claimed.Input),
		}
		o.observeToolMaterialized(ctx, snapshot, tool, toolCall)
		observedTool := o.startObservedToolCall(ctx, snapshot, tool, toolCall)
		outcome := o.executeToolOutcome(ctx, tool, toolCall)
		outcome = o.afterToolOutcome(ctx, tool, outcome)
		result, execErr := outcome.Result, outcome.RawError
		output, status, errText := encodeToolOutput(claimed.ID, result, tool.Retention, execErr)
		claimed.Status = status
		claimed.Output = cloneJSON(output)
		claimed.CompletedAt = o.now()
		claimed.Error = errText
		if settlementStore != nil {
			if claimed.ResultMessageID == "" || claimed.ResultPartID == "" {
				return Result{RunID: run.ID, Status: session.RunFailed, Error: session.ErrConflict}
			}
			settleTime := claimed.CompletedAt
			err = settlementStore.SettleToolCall(context.WithoutCancel(ctx), session.ToolSettlement{
				ID: claimed.ID, ClaimedBy: claimed.ClaimedBy, ClaimToken: claimed.ClaimToken, Status: status, Output: cloneJSON(output), Error: claimed.Error, Metadata: cloneStringMap(claimed.Metadata), CompletedAt: settleTime,
				ResultMessage: session.Message{ID: claimed.ResultMessageID, SessionID: claimed.SessionID, RunID: claimed.RunID, ParentID: claimed.MessageID, Role: session.RoleTool, ModelID: run.ModelID, CreatedAt: settleTime, UpdatedAt: settleTime},
				ResultPart:    session.Part{ID: claimed.ResultPartID, MessageID: claimed.ResultMessageID, SessionID: claimed.SessionID, RunID: claimed.RunID, Kind: session.PartToolResult, Payload: cloneJSON(output), CreatedAt: settleTime, UpdatedAt: settleTime},
			})
		} else {
			err = o.Store.FinishToolCall(context.WithoutCancel(ctx), claimed)
		}
		if err != nil {
			o.finishObservedToolCall(observedTool, session.ToolCallFailed, err, nil)
			o.observeToolSettled(context.WithoutCancel(ctx), snapshot, tool, toolCall, session.ToolCallFailed, o.now().Sub(claimed.StartedAt), err, nil)
			return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
		}
		if plan := runPlanFromContext(ctx); plan != nil && plan.Dispatch != nil {
			_ = extension.Notify(plan.Dispatch, context.WithoutCancel(ctx), ToolSettledPoint, ToolSettledNotice{SessionID: run.SessionID, RunID: run.ID, ToolCallID: claimed.ID, ToolName: claimed.Name, Status: status, Result: result, Error: classifyExtensionError(execErr)})
		}
		o.finishObservedToolCall(observedTool, status, execErr, result.Metadata)
		o.observeToolSettled(context.WithoutCancel(ctx), snapshot, tool, toolCall, status, claimed.CompletedAt.Sub(claimed.StartedAt), execErr, result.Metadata)
		if settlementStore == nil {
			if err := o.appendToolResult(context.WithoutCancel(ctx), run, claimed, output); err != nil {
				return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
			}
		}
		if errors.Is(execErr, context.Canceled) {
			return o.finishResume(ctx, run, Result{RunID: run.ID, Status: session.RunInterrupted, Interrupted: true, Error: execErr})
		}
	}
	return o.finishResume(ctx, run, Result{RunID: run.ID, Status: session.RunInterrupted, Interrupted: true})
}

func (o *StreamingOrchestrator) resumeTools(ctx context.Context, run session.Run) (map[string]Tool, error) {
	plan := runPlanFromContext(ctx)
	if o.Tools == nil && (plan == nil || plan.Tools == nil) {
		return nil, fmt.Errorf("%w: tool registry required", ErrInvalidOrchestrator)
	}
	snapshot := o.resumeSnapshot(run)
	var resolved []Tool
	if o.Tools != nil {
		legacy, err := o.Tools.ResolveTools(ctx, snapshot)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, legacy...)
	}
	if plan != nil && plan.Tools != nil {
		planned, err := plan.Tools.ResolveTools(ctx, snapshot)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, planned...)
	}
	tools := make(map[string]Tool, len(resolved))
	for _, tool := range resolved {
		if _, exists := tools[tool.Name]; exists {
			return nil, fmt.Errorf("duplicate effective tool %q", tool.Name)
		}
		tools[tool.Name] = tool
	}
	return tools, nil
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

func (o *StreamingOrchestrator) appendToolResult(ctx context.Context, run session.Run, call session.ToolCall, output []byte) error {
	messageID := o.IDs.NewMessageID()
	if _, err := o.Store.AppendMessage(ctx, session.Message{
		ID:        messageID,
		SessionID: run.SessionID,
		RunID:     run.ID,
		Role:      session.RoleTool,
		ModelID:   run.ModelID,
		CreatedAt: o.now(),
		UpdatedAt: o.now(),
	}); err != nil {
		return err
	}
	_, err := o.Store.AppendPart(ctx, session.Part{
		ID:        o.IDs.NewPartID(),
		MessageID: messageID,
		SessionID: run.SessionID,
		RunID:     run.ID,
		Kind:      session.PartToolResult,
		Payload:   cloneJSON(output),
		CreatedAt: o.now(),
		UpdatedAt: o.now(),
	})
	return err
}

func (o *StreamingOrchestrator) finishResume(ctx context.Context, run session.Run, result Result) Result {
	run.Status = result.Status
	run.FinishedAt = o.now()
	if result.Error != nil {
		run.Error = result.Error.Error()
	}
	if err := o.Store.FinishRun(context.WithoutCancel(ctx), run); err != nil && result.Error == nil {
		result.Status = session.RunFailed
		result.Error = err
	}
	return result
}
