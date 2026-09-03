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

func (o *StreamingOrchestrator) persistAssistantTurn(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, msg *einoschema.Message, providerStatePayloads []json.RawMessage, calls []preparedToolCall) ([]preparedToolCall, error) {
	parts := make([]session.Part, 0, 2+len(providerStatePayloads))
	ordinal := int64(0)
	now := o.now()
	if msg.Content != "" {
		parts = append(parts, session.Part{ID: o.ids.NewPartID(), MessageID: messageID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, Kind: session.PartText, Ordinal: ordinal, Payload: mustJSON(map[string]string{"text": msg.Content}), CreatedAt: now, UpdatedAt: now})
		ordinal++
	}
	if msg.ReasoningContent != "" {
		parts = append(parts, session.Part{ID: o.ids.NewPartID(), MessageID: messageID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, Kind: session.PartReasoning, Ordinal: ordinal, Payload: mustJSON(map[string]string{"text": msg.ReasoningContent}), CreatedAt: now, UpdatedAt: now})
		ordinal++
	}
	for _, payload := range providerStatePayloads {
		parts = append(parts, session.Part{ID: o.ids.NewPartID(), MessageID: messageID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, Kind: session.PartProviderState, Ordinal: ordinal, Payload: cloneJSON(payload), CreatedAt: now, UpdatedAt: now})
		ordinal++
	}
	for index := range calls {
		prepared := &calls[index]
		requestPartID := o.ids.NewPartID()
		resultMessageID := o.ids.NewMessageID()
		resultPartID := o.ids.NewPartID()
		payload := toolCallPayload{ID: string(prepared.call.ID), Name: prepared.call.Name, Arguments: prepared.call.Input}
		prepared.request = session.CreateToolCallRequest{
			Call: session.ToolCall{
				ID: prepared.call.ID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID,
				RequestPartID: requestPartID, ResultMessageID: resultMessageID, ResultPartID: resultPartID,
				Name: prepared.call.Name, Pattern: prepared.call.Pattern, Input: cloneJSON(prepared.call.Input), Status: session.ToolCallPending,
				RetrySafe: prepared.tool.RetrySafe, Metadata: cloneStringMap(prepared.tool.Metadata),
			},
			RequestPart: session.Part{ID: requestPartID, MessageID: messageID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, Kind: session.PartToolCall, Ordinal: ordinal, Payload: mustJSON(payload), CreatedAt: now, UpdatedAt: now},
			Event:       toolTransitionEnvelope(o, snapshot, now),
		}
		ordinal++
	}
	created := make([]session.ToolTransitionResult, len(calls))
	err := execution.store.WithinTx(ctx, func(ctx context.Context, store session.ExecutionStore) error {
		for _, part := range parts {
			if _, err := store.AppendPart(ctx, part); err != nil {
				return err
			}
		}
		for index := range calls {
			result, err := store.CreateToolCall(ctx, calls[index].request)
			if err != nil {
				return err
			}
			created[index] = result
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for index := range calls {
		calls[index].record = created[index].Call
		calls[index].call.ResultMessageID = created[index].Call.ResultMessageID
		calls[index].call.ResultPartID = created[index].Call.ResultPartID
		execution.publishPersisted(ctx, created[index].Event)
	}
	return calls, nil
}

type preparedToolCall struct {
	schemaCall    einoschema.ToolCall
	tool          Tool
	call          ToolCall
	middlewareErr error
	request       session.CreateToolCallRequest
	record        session.ToolCall
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
			callID = o.ids.NewToolCallID()
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
			input, err = canonicalToolObject(decoded)
			if err != nil {
				return nil, fmt.Errorf("malformed normalized tool input: %w", err)
			}
		}
		call := ToolCall{ID: callID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Name: schemaCall.Function.Name, Scope: tool.Scope, Pattern: schemaCall.Function.Name, Input: cloneJSON(input), Context: toolContext(snapshot, snapshot.Tools)}
		input = cloneJSON(call.Input)
		var prepareErr error
		preparedCall, err := extension.ApplyTransforms(execution.dispatch(), ctx, ToolPreparePoint, PreparedToolCall{Tool: extensionTool(tool), Call: extensionToolCall(call)})
		if err != nil {
			prepareErr = err
		} else {
			input, err = canonicalToolObject(preparedCall.Call.Input)
			if err != nil {
				prepareErr = extension.ErrProtectedMutation
			} else {
				call.Input = cloneJSON(input)
			}
		}
		if prepareErr == nil && tool.Pattern != nil {
			call.Pattern, err = tool.Pattern.ResolvePermissionPattern(ctx, input)
			if err != nil {
				return nil, err
			}
		}
		if call.Pattern == "" || len(call.Pattern) > 4096 {
			return nil, fmt.Errorf("invalid permission pattern for tool %q", call.Name)
		}
		schemaCall.Function.Arguments = string(input)
		extension.Notify(execution.dispatch(), ctx, ToolPreparedPoint, ToolPreparedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, ToolCallID: call.ID, ToolName: call.Name, Input: call.Input, Component: cloneStringMap(tool.Metadata)})
		prepared = append(prepared, preparedToolCall{schemaCall: schemaCall, tool: tool, call: call, middlewareErr: prepareErr})
	}
	return prepared, nil
}

func (o *StreamingOrchestrator) executePreparedTools(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, calls []preparedToolCall) ([]*einoschema.Message, error) {
	messages := make([]*einoschema.Message, 0, len(calls))
	var fatal error
	for _, prepared := range calls {
		schemaCall, tool, call := prepared.schemaCall, prepared.tool, prepared.call
		callID := call.ID
		record := prepared.record
		call.ResultMessageID, call.ResultPartID = record.ResultMessageID, record.ResultPartID
		startedAt := o.now()
		claimEvent := toolTransitionEnvelope(o, snapshot, startedAt)
		claimed, err := execution.persistToolClaim(ctx, session.ClaimToolCallRequest{
			ID: record.ID, ClaimedBy: o.ownerID(), ClaimToken: string(o.ids.NewEventID()), StartedAt: startedAt,
			LeaseDuration: o.lease(), Event: claimEvent,
		})
		if err != nil {
			fatal = err
			break
		}
		record = claimed.Call
		call.ResultMessageID, call.ResultPartID = record.ResultMessageID, record.ResultPartID
		extension.Notify(execution.dispatch(), context.WithoutCancel(ctx), ToolStartedPoint, ToolStartedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, ToolCallID: callID, ToolName: call.Name, Time: record.StartedAt})
		settled, err := execution.executeAndSettleClaimedTool(ctx, snapshot, tool, call, record, prepared.middlewareErr)
		if err != nil {
			fatal = err
			break
		}
		output := settled.Settlement.Output
		messages = append(messages, einoschema.ToolMessage(string(output), string(callID), einoschema.WithToolName(schemaCall.Function.Name)))
		if errors.Is(settled.Outcome.RawError, errToolExecutionPanic) || errors.Is(settled.Outcome.RawError, context.Canceled) {
			fatal = settled.Outcome.RawError
			break
		}
	}
	if fatal != nil {
		records := make([]session.ToolCall, len(calls))
		for index := range calls {
			records[index] = calls[index].record
		}
		fatal = errors.Join(fatal, execution.terminalizeUnfinishedTools(context.WithoutCancel(ctx), snapshot, records))
	}
	return messages, fatal
}
