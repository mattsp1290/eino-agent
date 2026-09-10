package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// persistAssistantTurn splits msg into its durable public content (via
// session.ContentFromAgenticMessage) and persists it as rich content parts,
// any captured provider-state payloads, and one tool-call record per
// prepared call, atomically. blockIDs is pre-minted, parallel to
// msg.ContentBlocks (see orchestrator.go's executeTurn and
// captureAssistantProviderState): the same durable block identity a
// block-bound provider-state item is captured against is the identity
// assigned to the corresponding durable content block here, so replay can
// re-associate the two.
func (o *StreamingOrchestrator) persistAssistantTurn(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, msg *einoschema.AgenticMessage, blockIDs []string, providerStatePayloads []json.RawMessage, calls []preparedToolCall) ([]preparedToolCall, error) {
	now := o.now()
	blockIndex := 0
	nextBlockID := func() string {
		if blockIndex >= len(blockIDs) {
			return ""
		}
		id := blockIDs[blockIndex]
		blockIndex++
		return id
	}
	content, private, err := session.ContentFromAgenticMessage(msg, nextBlockID)
	if err != nil {
		return nil, err
	}
	if len(private) != 0 {
		// The model returned provider-private material (a typed extension
		// field session.ContentFromAgenticMessage knows how to split out)
		// but no provider-state codec captured it beforehand: persisting it
		// as public content would leak it, so this fails closed instead.
		return nil, model.Error{
			Code:    "provider_state_unregistered",
			Message: "model returned provider-private content with no provider state codec registered",
			Cause:   errors.Join(model.ErrProviderState, model.ErrProviderStateMismatch),
		}
	}
	contentParts, err := session.EncodeContentParts(content, func() session.PartID { return o.ids.NewPartID() }, messageID, snapshot.SessionID, snapshot.RunID, now, o.contentLimits)
	if err != nil {
		return nil, err
	}
	// function_tool_call parts are excluded from the generic bulk-append
	// list below: they are owned by store.CreateToolCall, which persists a
	// tool call's request part atomically with its pending row (and, on the
	// fenced execution store, a generic AppendPart of a function_tool_call
	// or function_tool_result part is rejected outright, exactly like the
	// legacy PartToolCall/PartToolResult kinds it replaces).
	parts := make([]session.Part, 0, len(contentParts)+len(providerStatePayloads))
	for _, part := range contentParts {
		if part.Kind != session.PartFunctionToolCall {
			parts = append(parts, part)
		}
	}
	ordinal := int64(len(contentParts))

	callIndex := 0
	for blockPos, block := range content.Blocks {
		if block.Kind != session.BlockKindFunctionToolCall {
			continue
		}
		if callIndex >= len(calls) {
			return nil, fmt.Errorf("assistant message carries more function call blocks than prepared tool calls")
		}
		prepared := &calls[callIndex]
		requestPart := contentParts[blockPos]
		resultMessageID := o.ids.NewMessageID()
		resultPartID := o.ids.NewPartID()
		prepared.request = session.CreateToolCallRequest{
			Call: session.ToolCall{
				ID: prepared.call.ID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID,
				RequestPartID: requestPart.ID, ResultMessageID: resultMessageID, ResultPartID: resultPartID,
				Name: prepared.call.Name, Pattern: prepared.call.Pattern, Input: cloneJSON(prepared.call.Input), Status: session.ToolCallPending,
				RetrySafe: prepared.tool.RetrySafe, Metadata: cloneStringMap(prepared.tool.Metadata),
			},
			RequestPart: requestPart,
			Event:       toolTransitionEnvelope(o, snapshot, now),
		}
		callIndex++
	}
	if callIndex != len(calls) {
		return nil, fmt.Errorf("assistant message carries fewer function call blocks than prepared tool calls")
	}
	for _, payload := range providerStatePayloads {
		parts = append(parts, session.Part{ID: o.ids.NewPartID(), MessageID: messageID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, Kind: session.PartProviderState, Ordinal: ordinal, Payload: cloneJSON(payload), CreatedAt: now, UpdatedAt: now})
		ordinal++
	}

	created := make([]session.ToolTransitionResult, len(calls))
	err = execution.store.WithinTx(ctx, func(ctx context.Context, store session.ExecutionStore) error {
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
		return store.FinalizeAssistantMessage(ctx, messageID)
	})
	if err != nil {
		return nil, err
	}
	o.sessionObserver.Hint(snapshot.SessionID)
	for index := range calls {
		calls[index].record = created[index].Call
		calls[index].call.ResultMessageID = created[index].Call.ResultMessageID
		calls[index].call.ResultPartID = created[index].Call.ResultPartID
		execution.publishPersisted(ctx, created[index].Event)
	}
	return calls, nil
}

type preparedToolCall struct {
	// block points at the function_tool_call block inside the assistant
	// message being persisted; prepareToolCalls rewrites its CallID and
	// Arguments to their canonical values in place.
	block         *einoschema.FunctionToolCall
	tool          Tool
	call          ToolCall
	middlewareErr error
	request       session.CreateToolCallRequest
	record        session.ToolCall
}

// functionToolCalls extracts msg's function_tool_call blocks, in content
// block order.
func functionToolCalls(msg *einoschema.AgenticMessage) []*einoschema.FunctionToolCall {
	if msg == nil {
		return nil
	}
	var calls []*einoschema.FunctionToolCall
	for _, block := range msg.ContentBlocks {
		if block != nil && block.Type == einoschema.ContentBlockTypeFunctionToolCall && block.FunctionToolCall != nil {
			calls = append(calls, block.FunctionToolCall)
		}
	}
	return calls
}

func (o *StreamingOrchestrator) prepareToolCalls(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, calls []*einoschema.FunctionToolCall) ([]preparedToolCall, error) {
	byName := map[string]Tool{}
	for _, tool := range snapshot.Tools {
		byName[tool.Name] = tool
	}
	prepared := make([]preparedToolCall, 0, len(calls))
	for _, block := range calls {
		if block == nil {
			continue
		}
		callID := session.ToolCallID(block.CallID)
		if callID == "" {
			callID = o.ids.NewToolCallID()
			block.CallID = string(callID)
		}
		tool, ok := byName[block.Name]
		if !ok || tool.Executor == nil {
			err := fmt.Errorf("tool %q unavailable", block.Name)
			o.observeToolSettled(ctx, snapshot, Tool{Name: block.Name}, ToolCall{ID: callID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Name: block.Name}, session.ToolCallFailed, 0, err, nil)
			return nil, err
		}
		input, err := normalizedToolArguments(block.Arguments)
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
		call := ToolCall{ID: callID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Name: block.Name, Scope: tool.Scope, Pattern: block.Name, Input: cloneJSON(input), Context: toolContext(snapshot, snapshot.Tools)}
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
		block.Arguments = string(input)
		extension.Notify(execution.dispatch(), ctx, ToolPreparedPoint, ToolPreparedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, ToolCallID: call.ID, ToolName: call.Name, Input: call.Input, Component: cloneStringMap(tool.Metadata)})
		prepared = append(prepared, preparedToolCall{block: block, tool: tool, call: call, middlewareErr: prepareErr})
	}
	return prepared, nil
}

// executePreparedTools executes calls and returns one user-role
// function_tool_result agentic message per call, in order. Each message's
// single text content is exactly string(settlement.Output) (the ToolOutput
// JSON), unchanged from the classic model-visible shape.
func (o *StreamingOrchestrator) executePreparedTools(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, calls []preparedToolCall) ([]*einoschema.AgenticMessage, error) {
	messages := make([]*einoschema.AgenticMessage, 0, len(calls))
	var fatal error
	for _, prepared := range calls {
		toolName, call := prepared.call.Name, prepared.call
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
		settled, err := execution.executeAndSettleClaimedTool(ctx, snapshot, prepared.tool, call, record, prepared.middlewareErr)
		if err != nil {
			fatal = err
			break
		}
		output := settled.Settlement.Output
		messages = append(messages, &einoschema.AgenticMessage{
			Role: einoschema.AgenticRoleTypeUser,
			ContentBlocks: []*einoschema.ContentBlock{{
				Type: einoschema.ContentBlockTypeFunctionToolResult,
				FunctionToolResult: &einoschema.FunctionToolResult{
					CallID: string(callID),
					Name:   toolName,
					Content: []*einoschema.FunctionToolResultContentBlock{{
						Type: einoschema.FunctionToolResultContentBlockTypeText,
						Text: &einoschema.UserInputText{Text: string(output)},
					}},
				},
			}},
		})
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
