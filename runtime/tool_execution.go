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
	Output     ToolOutput
	Event      session.EventRecord
}

func (e *runExecution) settleInterruptedRunningTool(ctx context.Context, run session.Run, tool Tool, claimed session.ToolCall) (session.ToolSettlement, error) {
	call := ToolCall{
		ID: claimed.ID, SessionID: claimed.SessionID, RunID: claimed.RunID, MessageID: claimed.MessageID,
		ResultMessageID: claimed.ResultMessageID, ResultPartID: claimed.ResultPartID, Name: claimed.Name,
		Scope: tool.Scope, Pattern: claimed.Pattern, Input: cloneJSON(claimed.Input),
	}
	_, claimed = e.ensureToolResultIDs(call, claimed)
	completedAt := e.host.now()
	const errText = "tool was running during resume and was not re-executed"
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
		Metadata: metadata, ModelID: run.ModelID, CompletedAt: completedAt,
	})
	if err != nil {
		return session.ToolSettlement{}, err
	}
	eventEnvelope := toolTransitionEnvelope(e.host, e.host.resumeSnapshot(run), settlement.CompletedAt)
	if err := e.commitToolSettlement(context.WithoutCancel(ctx), claimed, settlement, eventEnvelope); err != nil {
		return session.ToolSettlement{}, err
	}
	extension.Notify(e.dispatch(), context.WithoutCancel(ctx), ToolSettledPoint, ToolSettledNotice{
		SessionID: run.SessionID, RunID: run.ID, ToolCallID: claimed.ID, ToolName: claimed.Name,
		Status: settlement.Status, Result: result, Error: classifyExtensionError(context.Canceled),
	})
	return settlement, nil
}

func (e *runExecution) executeAndSettleClaimedTool(ctx context.Context, snapshot TurnSnapshot, tool Tool, call ToolCall, claimed session.ToolCall, prepareErr error) (settledTool, error) {
	call, claimed = e.ensureToolResultIDs(call, claimed)
	e.host.observeToolMaterialized(ctx, snapshot, tool, call)
	observedTool := e.host.startObservedToolCall(ctx, snapshot, tool, call)
	outcome := e.executeClaimedToolPipeline(ctx, tool, call, prepareErr)
	completedAt := e.host.now()
	settlement, output, err := BuildToolSettlement(ToolSettlementInput{
		Tool: tool, Call: call, Claimed: claimed, Disposition: outcome.Disposition,
		Result: outcome.Result, Err: outcome.RawError, ModelID: string(snapshot.Model.Model.ID), CompletedAt: completedAt,
	})
	eventEnvelope := toolTransitionEnvelope(e.host, snapshot, completedAt)
	if err == nil {
		err = e.commitToolSettlement(context.WithoutCancel(ctx), claimed, settlement, eventEnvelope)
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
	eventRecord, err := session.ToolTransitionRecord(settlementCall(claimed, settlement), eventEnvelope)
	if err != nil {
		return settledTool{}, err
	}
	return settledTool{Outcome: outcome, Settlement: settlement, Output: output, Event: eventRecord}, nil
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

func (e *runExecution) ensureToolResultIDs(call ToolCall, claimed session.ToolCall) (ToolCall, session.ToolCall) {
	call.ResultMessageID = claimed.ResultMessageID
	call.ResultPartID = claimed.ResultPartID
	return call, claimed
}

func (e *runExecution) commitToolSettlement(ctx context.Context, claimed session.ToolCall, settlement session.ToolSettlement, event session.ToolTransitionEvent) error {
	if err := e.ensureStore(ctx, claimed.RunID); err != nil {
		return err
	}
	return e.store.SettleToolCall(ctx, session.SettleToolCallRequest{Settlement: settlement, Event: event})
}

func settlementCall(claimed session.ToolCall, settlement session.ToolSettlement) session.ToolCall {
	settled, _ := settlement.Apply(claimed)
	return settled
}

func toolTransitionEnvelope(host *StreamingOrchestrator, snapshot TurnSnapshot, at time.Time) session.ToolTransitionEvent {
	return session.ToolTransitionEvent{
		ID: host.ids.NewEventID(), EpochID: snapshot.EpochID, ProviderID: string(snapshot.Model.Provider.ID),
		ModelID: string(snapshot.Model.Model.ID), CreatedAt: at.UTC(),
	}
}
