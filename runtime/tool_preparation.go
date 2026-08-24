package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

func (o *StreamingOrchestrator) persistAssistant(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, msg *einoschema.Message) error {
	calls, err := normalizeToolCalls(msg.ToolCalls)
	if err != nil {
		return err
	}
	ordinal := int64(0)
	if msg.Content != "" {
		if err := o.appendPart(ctx, session.Part{ID: o.IDs.NewPartID(), MessageID: messageID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, Kind: session.PartText, Ordinal: ordinal, Payload: mustJSON(map[string]string{"text": msg.Content}), CreatedAt: o.now(), UpdatedAt: o.now()}); err != nil {
			return err
		}
		ordinal++
	}
	if msg.ReasoningContent != "" {
		if err := o.appendPart(ctx, session.Part{ID: o.IDs.NewPartID(), MessageID: messageID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, Kind: session.PartReasoning, Ordinal: ordinal, Payload: mustJSON(map[string]string{"text": msg.ReasoningContent}), CreatedAt: o.now(), UpdatedAt: o.now()}); err != nil {
			return err
		}
		ordinal++
	}
	for _, call := range calls {
		payload := toolCallPayload{ID: call.call.ID, Name: call.call.Function.Name, Arguments: call.arguments}
		if err := o.appendPart(ctx, session.Part{ID: o.IDs.NewPartID(), MessageID: messageID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, Kind: session.PartToolCall, Ordinal: ordinal, Payload: mustJSON(payload), CreatedAt: o.now(), UpdatedAt: o.now()}); err != nil {
			return err
		}
		ordinal++
	}
	return nil
}

func (o *StreamingOrchestrator) appendPart(ctx context.Context, part session.Part) error {
	_, err := o.Store.AppendPart(ctx, part)
	return err
}

type normalizedToolCall struct {
	call      einoschema.ToolCall
	arguments json.RawMessage
}

func normalizeToolCalls(calls []einoschema.ToolCall) ([]normalizedToolCall, error) {
	normalized := make([]normalizedToolCall, 0, len(calls))
	for _, call := range calls {
		arguments, err := normalizedToolArguments(call.Function.Arguments)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, normalizedToolCall{call: call, arguments: arguments})
	}
	return normalized, nil
}

type preparedToolCall struct {
	schemaCall    einoschema.ToolCall
	tool          Tool
	call          ToolCall
	middlewareErr error
}

func (o *StreamingOrchestrator) prepareToolCalls(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, calls []einoschema.ToolCall) ([]preparedToolCall, error) {
	byName := map[string]Tool{}
	for _, tool := range snapshot.Tools {
		byName[tool.Name] = tool
	}
	prepared := make([]preparedToolCall, 0, len(calls))
	for _, schemaCall := range calls {
		callID := session.ToolCallID(schemaCall.ID)
		if callID == "" {
			callID = o.IDs.NewToolCallID()
		}
		tool, ok := byName[schemaCall.Function.Name]
		if !ok || tool.Executor == nil {
			err := fmt.Errorf("tool %q unavailable", schemaCall.Function.Name)
			o.observeToolSettled(ctx, snapshot, Tool{Name: schemaCall.Function.Name}, ToolCall{ID: callID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Name: schemaCall.Function.Name}, session.ToolCallFailed, 0, err, nil)
			return nil, err
		}
		input, err := normalizedToolArguments(schemaCall.Function.Arguments)
		if err != nil {
			return nil, err
		}
		if tool.InputDecoder != nil {
			decoded, err := tool.InputDecoder.DecodeToolInput(ctx, input)
			if err != nil {
				return nil, err
			}
			input = decoded
		}
		call := ToolCall{ID: callID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Name: schemaCall.Function.Name, Scope: tool.Scope, Input: cloneJSON(input), Context: toolContext(snapshot, snapshot.Tools)}
		input = cloneJSON(call.Input)
		var prepareErr error
		if execution.dispatch() != nil {
			prepared, err := extension.Invoke(execution.dispatch(), ctx, ToolPreparePoint, PreparedToolCall{Tool: extensionTool(tool), Call: extensionToolCall(call)}, func(_ context.Context, value PreparedToolCall) (PreparedToolCall, error) { return value, nil })
			if err != nil {
				prepareErr = err
			} else {
				call.Input = cloneJSON(prepared.Call.Input)
				input = cloneJSON(prepared.Call.Input)
			}
		}
		call.Pattern = toolPattern(input, schemaCall.Function.Name)
		schemaCall.Function.Arguments = string(input)
		_ = extension.Notify(execution.dispatch(), ctx, ToolPreparedPoint, ToolPreparedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, ToolCallID: call.ID, ToolName: call.Name, Input: call.Input, Component: cloneStringMap(tool.Metadata)})
		prepared = append(prepared, preparedToolCall{schemaCall: schemaCall, tool: tool, call: call, middlewareErr: prepareErr})
	}
	return prepared, nil
}

func (o *StreamingOrchestrator) executePreparedTools(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, calls []preparedToolCall) ([]*einoschema.Message, error) {
	messages := make([]*einoschema.Message, 0, len(calls))
	for _, prepared := range calls {
		schemaCall, tool, call := prepared.schemaCall, prepared.tool, prepared.call
		callID, input := call.ID, call.Input
		resultMessageID := o.IDs.NewMessageID()
		resultPartID := o.IDs.NewPartID()
		record, err := o.Store.CreateToolCall(ctx, session.ToolCall{ID: callID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, ResultMessageID: resultMessageID, ResultPartID: resultPartID, Name: call.Name, Input: cloneJSON(input), Status: session.ToolCallPending, RetrySafe: tool.RetrySafe, Metadata: cloneStringMap(tool.Metadata)})
		if err != nil {
			return nil, err
		}
		call.ResultMessageID, call.ResultPartID = record.ResultMessageID, record.ResultPartID
		_ = o.emitToolCall(ctx, execution, snapshot, messageID, callID, session.ToolCallPending, toolCallPayload{ID: string(callID), Name: call.Name, Arguments: cloneJSON(input)})
		record.Status, record.ClaimedBy, record.ClaimToken = session.ToolCallRunning, o.ownerID(), string(o.IDs.NewEventID())
		record.LeaseUntil, record.StartedAt = o.now().Add(o.lease()), o.now()
		record, err = o.Store.ClaimToolCall(ctx, record)
		if err != nil {
			return nil, err
		}
		call.ResultMessageID, call.ResultPartID = record.ResultMessageID, record.ResultPartID
		_ = extension.Notify(execution.dispatch(), ctx, ToolStartedPoint, ToolStartedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, ToolCallID: callID, ToolName: call.Name, Time: record.StartedAt})
		_ = o.emitToolCall(ctx, execution, snapshot, messageID, callID, session.ToolCallRunning, toolCallPayload{ID: string(callID), Name: call.Name, Arguments: cloneJSON(input)})
		settled, err := execution.executeAndSettleClaimedTool(ctx, snapshot, tool, call, record, prepared.middlewareErr)
		if err != nil {
			return nil, err
		}
		output := settled.Settlement.Output
		_ = o.emitToolCall(ctx, execution, snapshot, messageID, callID, settled.Settlement.Status, toolCallEventPayload(output, schemaCall.Function.Name, input))
		messages = append(messages, einoschema.ToolMessage(string(output), string(callID), einoschema.WithToolName(schemaCall.Function.Name)))
		if errors.Is(settled.Outcome.RawError, errToolExecutionPanic) || errors.Is(settled.Outcome.RawError, context.Canceled) {
			return messages, settled.Outcome.RawError
		}
	}
	return messages, nil
}

func (o *StreamingOrchestrator) emitToolCall(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, callID session.ToolCallID, status session.ToolCallStatus, payload any) error {
	sink := execution.eventSink(o.Events)
	if sink == nil {
		return nil
	}
	withStatus, err := withToolStatus(payload, status)
	if err != nil {
		return err
	}
	return sink.Emit(ctx, Event{Kind: EventToolCallUpdated, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, ToolCallID: callID, EpochID: snapshot.EpochID, ProviderID: string(snapshot.Model.Provider.ID), ModelID: string(snapshot.Model.Model.ID), Payload: mustJSON(withStatus), Time: o.now()})
}
