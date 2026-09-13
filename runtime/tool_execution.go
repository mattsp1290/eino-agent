package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

var errToolExecutionPanic = errors.New("tool execution panicked")

// undiscoveredToolError marks a prepared call to a deferred tool the model
// has not yet discovered via tool search. Unlike an ordinary prepareErr
// (which settles as an opaque operational_failure), it settles as a
// terminal, model-visible denied call -- exactly like a guard denial -- so
// the model sees the reason and can recover by calling tool search first
// (composition-search-reviewer I3).
type undiscoveredToolError struct{ error }

type settledTool struct {
	Outcome    toolOutcome
	Settlement session.ToolSettlement
	// Output is the decoded ToolOutput record backing Settlement.ResultPart,
	// including any bounded Parts. executePreparedTools uses it (via
	// toolOutputToResultContent) to build the same-turn model-visible
	// function_tool_result message so it mirrors exactly what was persisted.
	Output ToolOutput
}

func (e *runExecution) settleInterruptedRunningTool(ctx context.Context, run session.Run, tool Tool, claimed session.ToolCall) (session.ToolSettlement, error) {
	return e.settleInterruptedTool(ctx, run, tool, claimed, "tool was running during resume and was not re-executed")
}

func (e *runExecution) interruptPendingTool(ctx context.Context, snapshot TurnSnapshot, pending session.ToolCall) (session.ToolSettlement, error) {
	startedAt := e.host.now()
	claimed, err := e.persistToolClaim(ctx, session.ClaimToolCallRequest{
		ID: pending.ID, ClaimedBy: e.host.ownerID(), ClaimToken: string(e.host.ids.NewEventID()), StartedAt: startedAt,
		LeaseDuration: e.host.lease(), Event: toolTransitionEnvelope(e.host, snapshot, startedAt),
	})
	if err != nil {
		return session.ToolSettlement{}, err
	}
	extension.Notify(e.dispatch(), ctx, ToolStartedPoint, ToolStartedNotice{
		SessionID: pending.SessionID, RunID: pending.RunID, ToolCallID: pending.ID, ToolName: pending.Name, Time: claimed.Call.StartedAt,
	})
	run := session.Run{ID: pending.RunID, SessionID: pending.SessionID, ModelID: string(snapshot.Model.Model.ID)}
	return e.settleInterruptedTool(ctx, run, Tool{Name: pending.Name, Metadata: cloneStringMap(pending.Metadata)}, claimed.Call, "tool was skipped after an earlier fatal tool outcome")
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
			_, err = e.interruptPendingTool(ctx, snapshot, current)
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
	var output ToolOutput
	// SQL stores decode an unsettled call's absent output as JSON null rather
	// than an empty payload; both mean no output was recorded.
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		raw, output, _, _ = encodeToolOutput(claimed.ID, ToolResult{Output: "tool execution interrupted"}, tool.Retention, ToolInterrupted, nil)
		metadata = toolSettlementMetadata(metadata, output)
		result.Output = output.Content
		result.Structured = cloneJSON(output.Structured)
	} else {
		if err := json.Unmarshal(raw, &output); err != nil {
			return session.ToolSettlement{}, fmt.Errorf("stored output for tool call %s is malformed: %w", claimed.ID, err)
		}
		result.Output = output.Content
		result.Structured = cloneJSON(output.Structured)
	}
	// No live TurnSnapshot exists on this crash-reconciliation path (it
	// settles a tool call that never rejoined a live turn loop), and
	// session.Store's ExecutionStore/Store interfaces expose no by-ID
	// message read to recover the calling message's already-durable
	// TurnID/AgentPath here (only ListMessages -- a concrete store's own
	// unexported GetMessage helper is not part of that interface, so it is
	// not reachable from this call site): this interrupted result message
	// is stamped with an empty turn identity rather than paying for a full
	// ListMessages page scan on a rare, already-degraded settlement path.
	// The cost of that empty identity is bounded by
	// agui.agenticIdentity's synthetic-turn-id fallback (W7 review finding
	// A1): a message with an empty TurnID still replays correctly, it just
	// gets a deterministic synthetic turn id instead of the calling
	// message's real one.
	settlement, _, err := buildTerminalToolEnvelope(terminalToolEnvelopeInput{
		Claimed: claimed, Status: session.ToolCallInterrupted, Output: raw, OutputRecord: output, Error: errText,
		Metadata: metadata, ModelID: run.ModelID, CompletedAt: completedAt, MessageAt: messageAt,
		BlockID: string(e.host.ids.NewPartID()), ContentLimits: e.host.contentLimits,
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

// toolResultWrapFunc transforms a tool's raw result before it is durably
// settled -- the seam a mounted ADK agent-handler middleware's own
// WrapInvokableToolCall/WrapEnhancedInvokableToolCall (e.g. reduction's
// MaxLengthForTrunc truncation) hooks into (see
// adkEngine.applyHandlerToolResultWrappers), so the transform happens
// before the durable settlement row and settlementSeal's baseline exist,
// not as an after-settlement rewrite. nil is a no-op: the classic,
// non-ADK resume path has no handler middleware at all.
type toolResultWrapFunc func(ctx context.Context, call ToolCall, result ToolResult) (ToolResult, error)

func (e *runExecution) executeAndSettleClaimedTool(ctx context.Context, snapshot TurnSnapshot, tool Tool, call ToolCall, claimed session.ToolCall, prepareErr error, wrap toolResultWrapFunc) (settledTool, error) {
	call.ResultMessageID = claimed.ResultMessageID
	call.ResultPartID = claimed.ResultPartID
	e.host.observeToolMaterialized(ctx, snapshot, tool, call)
	observedTool := e.host.startObservedToolCall(ctx, snapshot, tool, call)
	outcome := e.executeClaimedToolPipeline(ctx, tool, call, prepareErr, wrap)
	completedAt := e.host.now()
	failSettlement := func(err error) (settledTool, error) {
		e.host.finishObservedToolCall(observedTool, session.ToolCallFailed, err, nil)
		e.host.observeToolSettled(context.WithoutCancel(ctx), snapshot, tool, call, session.ToolCallFailed, completedAt.Sub(claimed.StartedAt), err, nil)
		return settledTool{}, err
	}
	messageAt, err := e.nextDurableMessageTime(ctx, snapshot.SessionID, completedAt)
	if err != nil {
		return failSettlement(err)
	}
	settlement, output, err := buildToolSettlement(ToolSettlementInput{
		Tool: tool, Call: call, Claimed: claimed, Disposition: outcome.Disposition,
		Result: outcome.Result, Err: outcome.RawError, ModelID: string(snapshot.Model.Model.ID), CompletedAt: completedAt,
		BlockID: string(e.host.ids.NewPartID()), ContentLimits: e.host.contentLimits,
		TurnID: snapshot.TurnID, AgentPath: snapshot.AgentPath,
	}, messageAt)
	eventEnvelope := toolTransitionEnvelope(e.host, snapshot, completedAt)
	if err == nil {
		_, err = e.persistToolSettlement(ctx, claimed, settlement, eventEnvelope)
	}
	if err != nil {
		return failSettlement(err)
	}
	extension.Notify(e.dispatch(), context.WithoutCancel(ctx), ToolSettledPoint, ToolSettledNotice{
		SessionID: snapshot.SessionID, RunID: snapshot.RunID, ToolCallID: claimed.ID, ToolName: claimed.Name,
		Status: settlement.Status, Result: outcome.Result, Error: classifyExtensionError(outcome.RawError),
	})
	e.host.finishObservedToolCall(observedTool, settlement.Status, outcome.RawError, outcome.Result.Metadata)
	e.host.observeToolSettled(context.WithoutCancel(ctx), snapshot, tool, call, settlement.Status, completedAt.Sub(claimed.StartedAt), outcome.RawError, outcome.Result.Metadata)
	return settledTool{Outcome: outcome, Settlement: settlement, Output: output}, nil
}

func (e *runExecution) executeClaimedToolPipeline(ctx context.Context, tool Tool, call ToolCall, prepareErr error, wrap toolResultWrapFunc) (outcome toolOutcome) {
	defer func() {
		if recover() != nil {
			outcome = newToolOutcome(call, ToolResult{}, toolPermissionAllowed, errToolExecutionPanic)
		}
	}()
	if undiscovered, ok := prepareErr.(undiscoveredToolError); ok {
		result := modelVisiblePermissionResult("undiscovered", undiscovered.Error())
		outcome = newToolOutcome(call, result, toolPermissionDenied, nil)
		return e.host.transformToolOutcome(ctx, e, outcome)
	}
	if prepareErr != nil {
		outcome = newToolOutcome(call, ToolResult{}, toolPermissionAllowed, prepareErr)
		return e.host.transformToolOutcome(ctx, e, outcome)
	}
	outcome = e.host.executeToolOutcome(ctx, e, tool, call)
	// A mounted handler's own tool-call wrapper (e.g. reduction's
	// MaxLengthForTrunc truncation) only ever applies to a REAL, successful
	// execution result -- never to a permission denial/prepare error above,
	// and never when the tool itself already failed (nothing meaningful to
	// transform, and a wrapper's own truncation logic is not equipped to
	// interpret an error outcome).
	if wrap != nil && outcome.RawError == nil {
		wrapped, err := wrap(ctx, call, outcome.Result)
		if err != nil {
			outcome = newToolOutcome(call, outcome.Result, outcome.Permission, err)
		} else {
			outcome = newToolOutcome(call, wrapped, outcome.Permission, nil)
		}
	}
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
		e.host.publishMessageCommitted(ctx, e, settlement.ResultMessage.SessionID, settlement.ResultMessage.RunID,
			settlement.ResultMessage.ID, event.EpochID, settlement.ResultMessage.TurnID, settlement.ResultMessage.AgentPath)
	}
	return result, err
}

func toolTransitionEnvelope(host *StreamingOrchestrator, snapshot TurnSnapshot, at time.Time) session.ToolTransitionEvent {
	return session.ToolTransitionEvent{
		ID: host.ids.NewEventID(), EpochID: snapshot.EpochID, ProviderID: string(snapshot.Model.Provider.ID),
		ModelID: string(snapshot.Model.Model.ID), CreatedAt: at.UTC(),
	}
}
