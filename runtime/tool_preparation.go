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

func (o *StreamingOrchestrator) persistAssistant(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, msg *einoschema.Message) error {
	calls, err := normalizeToolCalls(msg.ToolCalls)
	if err != nil {
		return err
	}
	ordinal := int64(0)
	if msg.Content != "" {
		if err := execution.appendPart(ctx, session.Part{ID: o.ids.NewPartID(), MessageID: messageID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, Kind: session.PartText, Ordinal: ordinal, Payload: mustJSON(map[string]string{"text": msg.Content}), CreatedAt: o.now(), UpdatedAt: o.now()}); err != nil {
			return err
		}
		ordinal++
	}
	if msg.ReasoningContent != "" {
		if err := execution.appendPart(ctx, session.Part{ID: o.ids.NewPartID(), MessageID: messageID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, Kind: session.PartReasoning, Ordinal: ordinal, Payload: mustJSON(map[string]string{"text": msg.ReasoningContent}), CreatedAt: o.now(), UpdatedAt: o.now()}); err != nil {
			return err
		}
		ordinal++
	}
	for _, call := range calls {
		payload := toolCallPayload{ID: call.call.ID, Name: call.call.Function.Name, Arguments: call.arguments}
		if err := execution.appendPart(ctx, session.Part{ID: o.ids.NewPartID(), MessageID: messageID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, Kind: session.PartToolCall, Ordinal: ordinal, Payload: mustJSON(payload), CreatedAt: o.now(), UpdatedAt: o.now()}); err != nil {
			return err
		}
		ordinal++
	}
	return nil
}

func (e *runExecution) appendPart(ctx context.Context, part session.Part) error {
	_, err := e.store.AppendPart(ctx, part)
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
	for _, prepared := range calls {
		schemaCall, tool, call := prepared.schemaCall, prepared.tool, prepared.call
		callID, input := call.ID, call.Input
		resultMessageID := o.ids.NewMessageID()
		resultPartID := o.ids.NewPartID()
		createdAt := o.now()
		createEvent := toolTransitionEnvelope(o, snapshot, createdAt)
		created, err := execution.persistToolCreation(ctx, session.CreateToolCallRequest{
			Call:  session.ToolCall{ID: callID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, ResultMessageID: resultMessageID, ResultPartID: resultPartID, Name: call.Name, Pattern: call.Pattern, Input: cloneJSON(input), Status: session.ToolCallPending, RetrySafe: tool.RetrySafe, Metadata: cloneStringMap(tool.Metadata)},
			Event: createEvent,
		})
		if err != nil {
			return nil, err
		}
		record := created.Call
		call.ResultMessageID, call.ResultPartID = record.ResultMessageID, record.ResultPartID
		startedAt := o.now()
		claimEvent := toolTransitionEnvelope(o, snapshot, startedAt)
		claimed, err := execution.persistToolClaim(ctx, session.ClaimToolCallRequest{
			ID: record.ID, ClaimedBy: o.ownerID(), ClaimToken: string(o.ids.NewEventID()), StartedAt: startedAt,
			LeaseDuration: o.lease(), Event: claimEvent,
		})
		if err != nil {
			return nil, err
		}
		record = claimed.Call
		call.ResultMessageID, call.ResultPartID = record.ResultMessageID, record.ResultPartID
		extension.Notify(execution.dispatch(), context.WithoutCancel(ctx), ToolStartedPoint, ToolStartedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, ToolCallID: callID, ToolName: call.Name, Time: record.StartedAt})
		settled, err := execution.executeAndSettleClaimedTool(ctx, snapshot, tool, call, record, prepared.middlewareErr)
		if err != nil {
			return nil, err
		}
		output := settled.Settlement.Output
		messages = append(messages, einoschema.ToolMessage(string(output), string(callID), einoschema.WithToolName(schemaCall.Function.Name)))
		if errors.Is(settled.Outcome.RawError, errToolExecutionPanic) || errors.Is(settled.Outcome.RawError, context.Canceled) {
			return messages, settled.Outcome.RawError
		}
	}
	return messages, nil
}
