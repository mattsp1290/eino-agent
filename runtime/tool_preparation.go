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
				Name: prepared.call.Name, RequestedName: prepared.call.RequestedName, Pattern: prepared.call.Pattern, Input: cloneJSON(prepared.call.Input), Status: session.ToolCallPending,
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
	// isSearch marks a call to this turn's configured tool-search tool
	// (TurnSnapshot.ToolSearch). It bypasses the ordinary tool
	// normalize/InputDecoder/ToolPrepare/Pattern/guard/permission pipeline
	// entirely and is executed and settled by runtime/tool_search.go instead
	// of executeAndSettleClaimedTool.
	isSearch bool
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
	prepared := make([]preparedToolCall, 0, len(calls))
	discovered := execution.discoveredSnapshot()
	for _, block := range calls {
		if block == nil {
			continue
		}
		callID := session.ToolCallID(block.CallID)
		if callID == "" {
			callID = o.ids.NewToolCallID()
			block.CallID = string(callID)
		}
		requestedInput, err := normalizedToolArguments(block.Arguments)
		if err != nil {
			return nil, err
		}
		if snapshot.isToolSearchCall(block.Name) {
			call := ToolCall{
				ID: callID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID,
				Name: block.Name, RequestedName: block.Name, Pattern: block.Name,
				Input: cloneJSON(requestedInput), Context: toolContext(snapshot, snapshot.Tools),
			}
			block.Arguments = string(requestedInput)
			prepared = append(prepared, preparedToolCall{block: block, tool: Tool{Name: block.Name, RetrySafe: true}, call: call, isSearch: true})
			continue
		}
		// resolveToolCall resolves a model-requested tool name (which may be
		// a registered alias) to its canonical tool and remaps argument
		// aliases before normalization/InputDecoder/ToolPrepare/Pattern run,
		// per tools.Definition.Aliases/ArgumentAliases (see adk_tools.go).
		tool, canonicalName, remapped, requestedName, err := resolveToolCall(snapshot, block.Name, requestedInput)
		if err != nil {
			o.observeToolSettled(ctx, snapshot, Tool{Name: block.Name}, ToolCall{ID: callID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Name: block.Name}, session.ToolCallFailed, 0, err, nil)
			return nil, err
		}
		if tool.Deferred && !discovered[canonicalName] {
			// A deferred tool must be discovered via tool search before it
			// can be called: reject before any claim is made, exactly like
			// an unknown tool name (privilege escalation via a hallucinated
			// or premature deferred-tool call is rejected the same way).
			err := fmt.Errorf("tool %q unavailable: not yet discovered via tool search", canonicalName)
			o.observeToolSettled(ctx, snapshot, Tool{Name: canonicalName}, ToolCall{ID: callID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Name: canonicalName}, session.ToolCallFailed, 0, err, nil)
			return nil, err
		}
		input := remapped
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
		call := ToolCall{
			ID: callID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID,
			Name: canonicalName, RequestedName: requestedName, Scope: tool.Scope, Pattern: canonicalName,
			Input: cloneJSON(input), Context: toolContext(snapshot, snapshot.Tools),
		}
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
// content is built by toolOutputToResultContent from the same ToolOutput
// settlement persisted: a scalar result's single text content is exactly
// string(settlement.Output) (the ToolOutput JSON), unchanged from the
// classic model-visible shape; an enhanced result carries one content item
// per bounded part, identical to what was durably recorded.
func (o *StreamingOrchestrator) executePreparedTools(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, calls []preparedToolCall) ([]*einoschema.AgenticMessage, error) {
	messages := make([]*einoschema.AgenticMessage, 0, len(calls))
	var fatal error
	for _, prepared := range calls {
		// The function_tool_result sent back to the model must correlate on
		// the name the model actually used to call the tool (RequestedName),
		// which equals the canonical Name unless the model used an alias.
		toolName, call := prepared.call.RequestedName, prepared.call
		callID := call.ID
		record := prepared.record
		call.ResultMessageID, call.ResultPartID = record.ResultMessageID, record.ResultPartID
		if prepared.isSearch {
			matches, err := execution.executeToolSearchCall(ctx, snapshot, call, record)
			if err != nil {
				fatal = err
				break
			}
			block, err := toolSearchResultBlock("", string(callID), toolName, matches)
			if err != nil {
				fatal = err
				break
			}
			resultMessage, err := session.ContentToAgenticMessage(session.Content{Role: session.RoleUser, Blocks: []session.ContentBlock{block}})
			if err != nil {
				fatal = err
				break
			}
			messages = append(messages, resultMessage)
			continue
		}
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
		// Build the same-turn model-visible function_tool_result message from
		// exactly the content settlement persisted (toolOutputToResultContent
		// is the one function both use), so a scalar result keeps today's
		// single-text-part shape and an enhanced result mirrors its bounded
		// parts identically here and in the durable record.
		resultContent := session.Content{
			Role: session.RoleUser,
			Blocks: []session.ContentBlock{{
				Kind: session.BlockKindFunctionToolResult,
				FunctionResult: &session.FunctionResultBlock{
					CallID:  string(callID),
					Name:    toolName,
					Content: toolOutputToResultContent(settled.Settlement.Output, settled.Output),
				},
			}},
		}
		resultMessage, err := session.ContentToAgenticMessage(resultContent)
		if err != nil {
			fatal = err
			break
		}
		messages = append(messages, resultMessage)
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
