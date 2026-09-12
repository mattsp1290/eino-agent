package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// persistAssistantTurn splits msg into its durable public content (via
// session.ContentFromAgenticMessage) and persists it as rich content parts,
// any captured provider-state payloads, and one tool-call record per
// prepared call, atomically. blockIDs is pre-minted, parallel to
// msg.ContentBlocks (see adk_model.go's adkModel.commit and
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
	// or function_tool_result part is rejected outright).
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
				Name: prepared.call.Name, RequestedName: prepared.call.RequestedName, ProviderCallID: prepared.call.ProviderCallID, Pattern: prepared.call.Pattern, Input: cloneJSON(prepared.call.Input), Status: session.ToolCallPending,
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
	o.publishMessageCommitted(ctx, execution, snapshot.SessionID, snapshot.RunID, messageID, snapshot.EpochID, snapshot.TurnID, snapshot.AgentPath)
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
		// providerCallID is whatever the provider itself sent as this
		// call's CallID (possibly empty). callID is always a fresh,
		// store-wide-unique mint: some providers (llama.cpp/Ollama/vLLM
		// OpenAI-compatible endpoints, replayed fixtures) reissue the same
		// indexed id (e.g. "call_0") across unrelated responses, and
		// reusing that value verbatim as the durable tool_calls.id (which
		// is UNIQUE store-wide) fails a later turn with a clean
		// session.ErrConflict. block.CallID is overwritten unconditionally
		// (not just when empty) so the assistant message this runtime
		// persists and hands back to ADK -- and everything keyed off it
		// (ADK's own tool dispatch, GetToolCall lookups, approval/interrupt
		// reconciliation) -- uses this durable identity uniformly. The
		// provider's original id is preserved on the call record
		// (ProviderCallID) purely so the wire request rebuilt for a later
		// dispatch can show the provider its own id back (see
		// publicizeToolCallIDs).
		// A provider id that is not valid UTF-8, or that exceeds
		// session.DiscoveryMaxIdentityBytes (reused here, though this value
		// is not itself a discovery identity), is treated as if the
		// provider had left CallID empty rather than persisted as-is:
		// invalid UTF-8 would otherwise round-trip altered through the SQL
		// stores' JSON encoding (encoding/json replaces it with U+FFFD), so
		// the wire would silently stop echoing the provider's exact id, and
		// an unbounded id would sit in the free-form tool-call record with
		// no size check at all (unlike the durable content block's own
		// CallID, which session/content.go's validateVariant bounds via the
		// block's content limits). publicizeToolCallIDs falls back to the
		// minted durable id whenever ProviderCallID is empty, so an invalid
		// or oversized id simply means the provider sees its own call
		// answered under the minted id instead.
		providerCallID := block.CallID
		if !validProviderCallID(providerCallID) {
			providerCallID = ""
		}
		callID := o.ids.NewToolCallID()
		block.CallID = string(callID)
		requestedInput, err := normalizedToolArguments(block.Arguments)
		if err != nil {
			return nil, err
		}
		if snapshot.isToolSearchCall(block.Name) {
			call := ToolCall{
				ID: callID, ProviderCallID: providerCallID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID,
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
			o.observeToolSettled(ctx, snapshot, Tool{Name: block.Name}, ToolCall{ID: callID, ProviderCallID: providerCallID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Name: block.Name}, session.ToolCallFailed, 0, err, nil)
			return nil, err
		}
		if tool.Deferred && !discovered[canonicalName] {
			// A deferred tool must be discovered via tool search before it
			// can be called. Unlike an unknown/hallucinated tool name (which
			// stays fail-closed and aborts the run), this is a condition an
			// otherwise well-behaved model can walk into legitimately (it
			// may emit tool_search and a deferred call in the same
			// assistant message, since `discovered` is snapshotted once
			// before any call executes) and recover from: settle the call
			// as a terminal, model-visible denial ("call <search> first")
			// and let the turn continue, exactly like a guard denial
			// (composition-search-reviewer I3). It deliberately skips
			// normalize/InputDecoder/ToolPrepare/Pattern -- the tool has not
			// been vetted for use yet -- using the search name as the
			// permission pattern, mirroring the isSearch branch above.
			// With no tool search configured for the plan (none registered,
			// or the restriction set denies its name), a deferred tool is
			// excluded from both Controls.Tools and Controls.DeferredTools
			// in TurnSnapshot.ProviderRequest -- it is never advertised at
			// all -- so there is no search tool to name in the denial.
			var denyErr error
			if snapshot.ToolSearch != nil {
				denyErr = fmt.Errorf("tool %q unavailable: call %s first", canonicalName, snapshot.ToolSearch.Name)
			} else {
				denyErr = fmt.Errorf("tool %q is deferred and no tool search is configured", canonicalName)
			}
			call := ToolCall{
				ID: callID, ProviderCallID: providerCallID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID,
				Name: canonicalName, RequestedName: requestedName, Pattern: canonicalName,
				Input: cloneJSON(remapped), Context: toolContext(snapshot, snapshot.Tools),
			}
			block.Arguments = string(remapped)
			denyErr = undiscoveredToolError{denyErr}
			prepared = append(prepared, preparedToolCall{block: block, tool: tool, call: call, middlewareErr: denyErr})
			continue
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
			ID: callID, ProviderCallID: providerCallID, SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID,
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

// validProviderCallID reports whether id is safe to persist verbatim as
// session.ToolCall.ProviderCallID: valid UTF-8 (so it round-trips through
// the SQL stores' JSON encoding unaltered -- encoding/json otherwise
// replaces invalid UTF-8 with U+FFFD) and no longer than
// session.DiscoveryMaxIdentityBytes -- reused as a convenient existing
// bound, not because a provider call id is itself a discovery identity. An
// empty id is valid (it means the provider left CallID unset).
func validProviderCallID(id string) bool {
	return len(id) <= session.DiscoveryMaxIdentityBytes && utf8.ValidString(id)
}
