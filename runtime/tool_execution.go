package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

var errToolExecutionPanic = errors.New("tool execution panicked")

type settledTool struct {
	Outcome    toolOutcome
	Settlement session.ToolSettlement
}

func (e *runExecution) settleInterruptedRunningTool(ctx context.Context, run session.Run, tool Tool, claimed session.ToolCall) (session.ToolSettlement, error) {
	return e.settleInterruptedTool(ctx, run, tool, claimed, "tool was running during resume and was not re-executed")
}

func (e *runExecution) interruptPendingTool(ctx context.Context, snapshot TurnSnapshot, pending session.ToolCall) error {
	startedAt := e.host.now()
	claimed, err := e.persistToolClaim(ctx, session.ClaimToolCallRequest{
		ID: pending.ID, ClaimedBy: e.host.ownerID(), ClaimToken: string(e.host.ids.NewEventID()), StartedAt: startedAt,
		LeaseDuration: e.host.lease(), Event: toolTransitionEnvelope(e.host, snapshot, startedAt),
	})
	if err != nil {
		return err
	}
	extension.Notify(e.dispatch(), ctx, ToolStartedPoint, ToolStartedNotice{
		SessionID: pending.SessionID, RunID: pending.RunID, ToolCallID: pending.ID, ToolName: pending.Name, Time: claimed.Call.StartedAt,
	})
	run := session.Run{ID: pending.RunID, SessionID: pending.SessionID, ModelID: string(snapshot.Model.Model.ID)}
	_, err = e.settleInterruptedTool(ctx, run, Tool{Name: pending.Name, Metadata: cloneStringMap(pending.Metadata)}, claimed.Call, "tool was skipped after an earlier fatal tool outcome")
	return err
}

func (e *runExecution) terminalizeUnfinishedTools(ctx context.Context, snapshot TurnSnapshot, calls []session.ToolCall) error {
	var errs []error
	for _, listed := range calls {
		current, err := e.host.store.GetToolCall(ctx, listed.ID)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if session.TerminalToolCall(current.Status) {
			continue
		}
		switch current.Status {
		case session.ToolCallPending:
			err = e.interruptPendingTool(ctx, snapshot, current)
		case session.ToolCallRunning:
			run := session.Run{ID: current.RunID, SessionID: current.SessionID, ModelID: string(snapshot.Model.Model.ID)}
			_, err = e.settleInterruptedTool(ctx, run, Tool{Name: current.Name, Metadata: cloneStringMap(current.Metadata)}, current, "tool was interrupted after a fatal tool lifecycle outcome")
		default:
			err = session.ErrConflict
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (e *runExecution) settleInterruptedTool(ctx context.Context, run session.Run, tool Tool, claimed session.ToolCall, errText string) (session.ToolSettlement, error) {
	completedAt := e.host.now()
	messageAt, err := e.nextDurableMessageTime(ctx, run.SessionID, completedAt)
	if err != nil {
		return session.ToolSettlement{}, err
	}
	raw := cloneJSON(claimed.Output)
	metadata := cloneStringMap(claimed.Metadata)
	result := ToolResult{}
	if len(raw) == 0 {
		var output ToolOutput
		raw, output, _, _ = encodeToolOutput(claimed.ID, ToolResult{Output: "tool execution interrupted"}, tool.Retention, ToolInterrupted, nil)
		metadata = toolSettlementMetadata(metadata, output)
		result.Output = output.Content
		result.Structured = cloneJSON(output.Structured)
	} else {
		var output ToolOutput
		_ = json.Unmarshal(raw, &output)
		result.Output = output.Content
		result.Structured = cloneJSON(output.Structured)
	}
	settlement, err := buildTerminalToolEnvelope(terminalToolEnvelopeInput{
		Claimed: claimed, Status: session.ToolCallInterrupted, Output: raw, Error: errText,
		Metadata: metadata, ModelID: run.ModelID, CompletedAt: completedAt, MessageAt: messageAt,
	})
	if err != nil {
		return session.ToolSettlement{}, err
	}
	eventEnvelope := toolTransitionEnvelope(e.host, e.host.resumeSnapshot(run), settlement.CompletedAt)
	if _, err := e.persistToolSettlement(ctx, claimed, settlement, eventEnvelope); err != nil {
		return session.ToolSettlement{}, err
	}
	extension.Notify(e.dispatch(), context.WithoutCancel(ctx), ToolSettledPoint, ToolSettledNotice{
		SessionID: run.SessionID, RunID: run.ID, ToolCallID: claimed.ID, ToolName: claimed.Name,
		Status: settlement.Status, Result: result, Error: classifyExtensionError(context.Canceled),
	})
	return settlement, nil
}

func (e *runExecution) executeAndSettleClaimedTool(ctx context.Context, snapshot TurnSnapshot, tool Tool, call ToolCall, claimed session.ToolCall, prepareErr error) (settledTool, error) {
	call.ResultMessageID = claimed.ResultMessageID
	call.ResultPartID = claimed.ResultPartID
	e.host.observeToolMaterialized(ctx, snapshot, tool, call)
	observedTool := e.host.startObservedToolCall(ctx, snapshot, tool, call)
	outcome := e.executeClaimedToolPipeline(ctx, tool, call, prepareErr)
	completedAt := e.host.now()
	messageAt, err := e.nextDurableMessageTime(ctx, snapshot.SessionID, completedAt)
	if err != nil {
		return settledTool{}, err
	}
	settlement, _, err := buildToolSettlement(ToolSettlementInput{
		Tool: tool, Call: call, Claimed: claimed, Disposition: outcome.Disposition,
		Result: outcome.Result, Err: outcome.RawError, ModelID: string(snapshot.Model.Model.ID), CompletedAt: completedAt,
	}, messageAt)
	eventEnvelope := toolTransitionEnvelope(e.host, snapshot, completedAt)
	if err == nil {
		_, err = e.persistToolSettlement(ctx, claimed, settlement, eventEnvelope)
	}
	if err != nil {
		e.host.finishObservedToolCall(observedTool, session.ToolCallFailed, err, nil)
		e.host.observeToolSettled(context.WithoutCancel(ctx), snapshot, tool, call, session.ToolCallFailed, completedAt.Sub(claimed.StartedAt), err, nil)
		return settledTool{}, err
	}
	extension.Notify(e.dispatch(), context.WithoutCancel(ctx), ToolSettledPoint, ToolSettledNotice{
		SessionID: snapshot.SessionID, RunID: snapshot.RunID, ToolCallID: claimed.ID, ToolName: claimed.Name,
		Status: settlement.Status, Result: outcome.Result, Error: classifyExtensionError(outcome.RawError),
	})
	e.host.finishObservedToolCall(observedTool, settlement.Status, outcome.RawError, outcome.Result.Metadata)
	e.host.observeToolSettled(context.WithoutCancel(ctx), snapshot, tool, call, settlement.Status, completedAt.Sub(claimed.StartedAt), outcome.RawError, outcome.Result.Metadata)
	return settledTool{Outcome: outcome, Settlement: settlement}, nil
}

func (e *runExecution) executeClaimedToolPipeline(ctx context.Context, tool Tool, call ToolCall, prepareErr error) (outcome toolOutcome) {
	defer func() {
		if recover() != nil {
			outcome = newToolOutcome(call, ToolResult{}, toolPermissionAllowed, errToolExecutionPanic)
		}
	}()
	if prepareErr != nil {
		outcome = newToolOutcome(call, ToolResult{}, toolPermissionAllowed, prepareErr)
		return e.host.transformToolOutcome(ctx, e, outcome)
	}
	outcome = e.host.executeToolOutcome(ctx, e, tool, call)
	return e.host.transformToolOutcome(ctx, e, outcome)
}

func (e *runExecution) persistToolCreation(ctx context.Context, request session.CreateToolCallRequest) (session.ToolTransitionResult, error) {
	result, err := e.store.CreateToolCall(ctx, request)
	if err == nil {
		e.publishPersisted(ctx, result.Event)
	}
	return result, err
}

func (e *runExecution) persistToolClaim(ctx context.Context, request session.ClaimToolCallRequest) (session.ToolTransitionResult, error) {
	result, err := e.store.ClaimToolCall(ctx, request)
	if err == nil {
		e.publishPersisted(ctx, result.Event)
	}
	return result, err
}

func (e *runExecution) persistToolSettlement(ctx context.Context, claimed session.ToolCall, settlement session.ToolSettlement, event session.ToolTransitionEvent) (session.ToolTransitionResult, error) {
	persistCtx := context.WithoutCancel(ctx)
	result, err := e.store.SettleToolCall(persistCtx, session.SettleToolCallRequest{Settlement: settlement, Event: event})
	if err == nil {
		e.publishPersisted(ctx, result.Event)
	}
	return result, err
}

func toolTransitionEnvelope(host *StreamingOrchestrator, snapshot TurnSnapshot, at time.Time) session.ToolTransitionEvent {
	return session.ToolTransitionEvent{
		ID: host.ids.NewEventID(), EpochID: snapshot.EpochID, ProviderID: string(snapshot.Model.Provider.ID),
		ModelID: string(snapshot.Model.Model.ID), CreatedAt: at.UTC(),
	}
}
