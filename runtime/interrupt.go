package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/mattsp1290/eino-agent/config"
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
	runCtx, cancel := context.WithCancel(ctx)
	handle := &streamingHandle{
		runID:       runID,
		cancel:      cancel,
		done:        make(chan Result, 1),
		onInterrupt: func(reason string) { o.observeInterrupt(context.WithoutCancel(ctx), run, "", reason) },
	}
	go o.executeResume(runCtx, run, handle.done)
	return handle, nil
}

func (o *StreamingOrchestrator) executeResume(ctx context.Context, run session.Run, done chan<- Result) {
	defer close(done)
	done <- o.resumeRun(ctx, run)
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
	calls, err := o.Store.ListUnfinishedToolCalls(ctx, run.ID)
	if err != nil {
		return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
	}
	if len(calls) == 0 {
		return o.finishResume(ctx, run, Result{RunID: run.ID, Status: session.RunInterrupted, Interrupted: true})
	}
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
			if err := o.Store.FinishToolCall(context.WithoutCancel(ctx), call); err != nil {
				return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
			}
			if err := o.appendToolResult(context.WithoutCancel(ctx), run, call, call.Output); err != nil {
				return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
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
		result, execErr := o.executeTool(ctx, tool, ToolCall{
			ID:        claimed.ID,
			SessionID: claimed.SessionID,
			RunID:     claimed.RunID,
			MessageID: claimed.MessageID,
			Name:      claimed.Name,
			Scope:     tool.Scope,
			Pattern:   toolPattern(claimed.Input, claimed.Name),
			Input:     cloneJSON(claimed.Input),
		})
		output, status, errText := encodeToolOutput(claimed.ID, result, tool.Retention, execErr)
		claimed.Status = status
		claimed.Output = cloneJSON(output)
		claimed.CompletedAt = o.now()
		claimed.Error = errText
		if err := o.Store.FinishToolCall(context.WithoutCancel(ctx), claimed); err != nil {
			return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
		}
		if err := o.appendToolResult(context.WithoutCancel(ctx), run, claimed, output); err != nil {
			return Result{RunID: run.ID, Status: session.RunFailed, Error: err}
		}
		if errors.Is(execErr, context.Canceled) {
			return o.finishResume(ctx, run, Result{RunID: run.ID, Status: session.RunInterrupted, Interrupted: true, Error: execErr})
		}
	}
	return o.finishResume(ctx, run, Result{RunID: run.ID, Status: session.RunInterrupted, Interrupted: true})
}

func (o *StreamingOrchestrator) resumeTools(ctx context.Context, run session.Run) (map[string]Tool, error) {
	if o.Tools == nil {
		return nil, fmt.Errorf("%w: tool registry required", ErrInvalidOrchestrator)
	}
	snapshot := TurnSnapshot{
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
	resolved, err := o.Tools.ResolveTools(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	tools := make(map[string]Tool, len(resolved))
	for _, tool := range resolved {
		tools[tool.Name] = tool
	}
	return tools, nil
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
