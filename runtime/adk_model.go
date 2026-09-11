package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/watch"
)

// errADKUnsupportedBlock reports a model result content block this runtime
// cannot durably record. Such a result must fail closed before ADK (or any
// downstream consumer) ever observes it: every fact ADK sees must already be
// committed.
var errADKUnsupportedBlock = errors.New("adk adapter cannot durably record content block")

// adkModel is the mandatory ADK model adapter promoted from the W1 proof's
// adkLedgerModel (runtime/adk_proof_test.go). It is the ONLY einomodel.AgenticModel
// the engine ever hands to adk.NewTypedChatModelAgent: every physical
// Generate/Stream call becomes one model-request ledger row (audited, given a
// fresh per-dispatch InvocationID) committed before dispatch, and the durable
// result -- text/reasoning parts, pending tool-call records with canonical
// identity, provider-private state, and the finalized assistant message -- is
// committed before ADK (or a retry/failover wrapper above this adapter) ever
// observes the returned message. adkModel is turn-scoped: PrepareAgent
// constructs a fresh instance per admitted turn (see runtime/turn_loop.go).
type adkModel struct {
	host      *StreamingOrchestrator
	execution *runExecution
	engine    *adkEngine
	approval  *adkApprovalBinding

	// resolvedOverride, when set, is used instead of engine.snapshot.Model
	// for this adapter instance's provider identity/streamer -- the failover
	// path's mechanism (see adk_retry.go's buildFailoverConfig) for
	// dispatching a physical attempt through an alternate resolved model
	// while sharing the same engine/execution/approval as the primary
	// adapter (same ledger, same durable projection, same turn identity).
	resolvedOverride *model.Resolved

	mu          sync.Mutex
	messageID   session.MessageID
	needMessage bool
}

var _ einomodel.AgenticModel = (*adkModel)(nil)

// activeModel returns the resolved model this adapter instance dispatches
// through: resolvedOverride when set (a failover attempt), otherwise the
// turn's primary engine.snapshot.Model.
func (m *adkModel) activeModel() model.Resolved {
	if m.resolvedOverride != nil {
		return *m.resolvedOverride
	}
	return m.engine.snapshot.Model
}

// dispatchSnapshot returns the turn's snapshot with Model swapped to
// activeModel(): every provider-facing construction (ProviderRequest,
// provider-state restoration in durableProjection) must key off the model
// this specific physical attempt actually dispatches through, which for a
// failover attempt is not engine.snapshot.Model.
func (m *adkModel) dispatchSnapshot() TurnSnapshot {
	snapshot := m.engine.snapshot
	snapshot.Model = m.activeModel()
	return snapshot
}

// adkDispatch carries the state a begin/commit/finish triple shares for one
// physical model call.
type adkDispatch struct {
	messageID session.MessageID
	record    session.ModelRequestRecord
	input     []*einoschema.AgenticMessage
}

// currentMessageID claims the turn's assistant placeholder for exactly one
// dispatch (the first that reaches here); every later dispatch within the
// same turn (tool-turn continuation, a retry/failover attempt after a
// committed step, or a child agent) mints and persists its own assistant
// message, parented to the turn's placeholder.
func (m *adkModel) currentMessageID(ctx context.Context) (session.MessageID, error) {
	if m.messageID == "" && !m.needMessage {
		if id, ok := m.engine.claimPlaceholder(); ok {
			m.messageID = id
			return id, nil
		}
	}
	if m.messageID != "" && !m.needMessage {
		return m.messageID, nil
	}
	nextID := m.host.ids.NewMessageID()
	at, err := m.execution.nextDurableMessageTime(ctx, m.engine.snapshot.SessionID, m.host.now())
	if err != nil {
		return "", err
	}
	if _, err := m.execution.store.AppendMessage(ctx, session.Message{
		ID: nextID, SessionID: m.engine.snapshot.SessionID, RunID: m.engine.snapshot.RunID, ParentID: m.engine.assistantMessageID,
		Role: session.RoleAssistant, Agent: m.engine.snapshot.Config.Agent.Name, ModelID: string(m.activeModel().Model.ID),
		CreatedAt: at, UpdatedAt: at,
	}); err != nil {
		return "", err
	}
	m.messageID = nextID
	m.needMessage = false
	return nextID, nil
}

func (m *adkModel) begin(ctx context.Context, input []*einoschema.AgenticMessage) (*adkDispatch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	step := m.engine.nextStep()
	messageID, err := m.currentMessageID(ctx)
	if err != nil {
		return nil, err
	}
	request := m.dispatchSnapshot().ProviderRequest(messageID, m.host.trace, input, m.execution.discoveredSnapshot())
	request.System, err = m.host.renderSystemPrompt(ctx, m.engine.plan, m.engine.snapshot, 1, step)
	if err != nil {
		return nil, err
	}
	request, audited, hash, err := auditModelRequest(request, m.host.modelRequestSafeOptions, m.host.modelRequestMaxBytes)
	if err != nil {
		return nil, err
	}
	record, err := m.host.prepareModelRequest(ctx, m.execution, m.engine.snapshot, request, audited, hash, messageID, modelRequestIdentity{
		InvocationID: m.host.ids.NewInvocationID(), TurnID: m.engine.turn.ID, AgentPath: m.engine.agentPath, Attempt: 1, Step: step,
	})
	if err != nil {
		return nil, err
	}
	if err := updateModelRequest(ctx, m.execution.store, &record, session.ModelRequestDispatchStarted, nil, m.host.now()); err != nil {
		// The physical call never happened (no dispatch, no
		// ModelRequestedPoint/ModelCompletedPoint notification): best-effort
		// mark the ledger row terminally failed so it does not sit in
		// "prepared" forever, but the transition-to-dispatch-started write
		// failing is itself the reported error regardless of whether this
		// second write succeeds.
		_ = updateModelRequest(ctx, m.execution.store, &record, session.ModelRequestFailed, err, m.host.now())
		return nil, err
	}
	extension.Notify(m.execution.dispatch(), ctx, ModelRequestedPoint, ModelRequestedNotice{
		SessionID: m.engine.snapshot.SessionID, RunID: m.engine.snapshot.RunID, MessageID: messageID,
		Attempt: record.Attempt, Step: record.Step, ProviderID: string(request.Identity.ProviderID), ModelID: string(request.Identity.ModelID),
		RequestRecordID: record.ID, MessageCount: len(input), ToolCount: len(request.Controls.Tools) + len(request.Controls.DeferredTools), ContentHash: hash,
	})
	if replaced, replacedErr := m.engine.takeFailedAttempt(); replaced != "" && replaced != record.InvocationID {
		event := session.EventRecord{
			ID: m.host.ids.NewEventID(), SessionID: m.engine.snapshot.SessionID, RunID: m.engine.snapshot.RunID,
			MessageID: messageID, EpochID: m.engine.snapshot.EpochID, TurnID: m.engine.turn.ID, AgentPath: m.engine.agentPath,
			ProviderID: string(request.Identity.ProviderID), ModelID: string(request.Identity.ModelID),
			Kind: session.AttemptReplacedEventKind, Correlation: replaced,
			Payload: mustJSON(map[string]string{"old_invocation_id": replaced, "new_invocation_id": record.InvocationID}), CreatedAt: m.host.now(),
		}
		committed, err := m.execution.store.AppendEvent(ctx, event)
		if err != nil {
			return nil, err
		}
		m.execution.publishPersisted(ctx, committed)
		m.host.observeRetry(ctx, m.dispatchSnapshot(), messageID, record.Step, m.host.attempts(), replacedErr)
	}
	return &adkDispatch{messageID: messageID, record: record, input: input}, nil
}

// finish updates this dispatch's ledger row and notifies ModelCompletedPoint.
// dispatchErr is the raw physical-call error (nil if the provider round trip
// itself succeeded) and controls the ledger row's terminal State: a request
// whose provider round trip completed is ledgered Completed even if a later
// step (commit's durable-persist validation, or this very ledger write)
// subsequently fails the overall Generate/Stream call -- that is a run-level
// failure, not evidence the provider request itself never completed (see
// TestUnsafeProviderOutputFailsBeforeSecondRequest). reportErr is the error
// actually surfaced to the caller/classified into the notice/ledger
// ErrorCode/recordFailedAttempt; it may be non-nil (a commit validation
// failure) even when dispatchErr is nil.
func (m *adkModel) finish(ctx context.Context, dispatch *adkDispatch, usage model.Usage, dispatchErr, reportErr error) error {
	state := session.ModelRequestCompleted
	if dispatchErr != nil {
		state = session.ModelRequestFailed
	}
	if updateErr := updateModelRequest(ctx, m.execution.store, &dispatch.record, state, reportErr, m.host.now()); updateErr != nil {
		return updateErr
	}
	extension.Notify(m.execution.dispatch(), ctx, ModelCompletedPoint, ModelCompletedNotice{
		SessionID: m.engine.snapshot.SessionID, RunID: m.engine.snapshot.RunID, MessageID: dispatch.messageID,
		Attempt: dispatch.record.Attempt, Step: dispatch.record.Step, Usage: runtimeUsage(usage), Error: classifyExtensionError(reportErr),
	})
	if reportErr != nil {
		// A retry or failover wrapper above this adapter may dispatch again;
		// begin() on that next attempt emits attempt_replaced once it knows
		// its own new InvocationID. Charged usage on this failed attempt is
		// retained regardless (see Generate/Stream: addUsage runs on the
		// error path too).
		m.engine.recordFailedAttempt(dispatch.record.InvocationID, reportErr)
	}
	return nil
}

// Generate implements einomodel.AgenticModel.
func (m *adkModel) Generate(ctx context.Context, input []*einoschema.AgenticMessage, opts ...einomodel.Option) (*einoschema.AgenticMessage, error) {
	if m.approval != nil {
		if err := m.approval.prepare(ctx, m); err != nil {
			return nil, err
		}
	}
	input, err := m.durableProjection(ctx, input)
	if err != nil {
		return nil, err
	}
	dispatch, err := m.begin(ctx, input)
	if err != nil {
		return nil, err
	}
	result, dispatchErr := m.dispatch(ctx, dispatch, input, nil)
	if dispatchErr != nil {
		m.engine.addUsage(result.usage)
		if result.receivedDelta {
			dispatchErr = fmt.Errorf("%w: %w", errPartialStreamObserved, dispatchErr)
		}
		_ = m.finish(ctx, dispatch, result.usage, dispatchErr, dispatchErr)
		return nil, dispatchErr
	}
	committed, err := m.commit(ctx, dispatch, result.message)
	// The physical provider call succeeded regardless of what happens next
	// (a commit validation failure or a subsequent ledger-write failure),
	// so this dispatch's usage is charged now -- it must survive even if
	// finish()'s own ledger update below fails and this call returns early
	// (see TestTerminalLedgerFailureOverridesProviderResultAndRetainsUsage).
	m.engine.addUsage(result.usage)
	if err != nil {
		// dispatchErr was nil (the provider call itself succeeded); this is
		// a durable-persist validation failure, so the ledger row is still
		// terminally Completed -- see finish's doc comment.
		_ = m.finish(ctx, dispatch, result.usage, nil, err)
		return nil, err
	}
	if err := m.finish(ctx, dispatch, result.usage, nil, nil); err != nil {
		return nil, fmt.Errorf("%w: %w", errCommittedDispatchFailed, err)
	}
	if m.approval != nil {
		if err := m.approval.pause(ctx, m, dispatch, committed); err != nil {
			return nil, fmt.Errorf("%w: %w", errCommittedDispatchFailed, err)
		}
	}
	return committed, nil
}

// Stream implements einomodel.AgenticModel. It fully drains the provider
// stream and commits the concatenated, durable result before ADK ever
// observes a chunk; live deltas are published separately through the session
// observer/event sink while consuming, not by forwarding the provider's own
// stream to ADK. ADK therefore only ever observes one already-committed
// chunk.
func (m *adkModel) Stream(ctx context.Context, input []*einoschema.AgenticMessage, opts ...einomodel.Option) (*einoschema.StreamReader[*einoschema.AgenticMessage], error) {
	if m.approval != nil {
		if err := m.approval.prepare(ctx, m); err != nil {
			return nil, err
		}
	}
	input, err := m.durableProjection(ctx, input)
	if err != nil {
		return nil, err
	}
	dispatch, err := m.begin(ctx, input)
	if err != nil {
		return nil, err
	}
	live := m.host.sessionObserver.BeginAttempt(watch.LiveIdentity{
		SessionID: m.engine.snapshot.SessionID, RunID: m.engine.snapshot.RunID, MessageID: dispatch.messageID,
		RequestID: dispatch.record.ID, Attempt: dispatch.record.Attempt, Step: dispatch.record.Step,
	})
	result, dispatchErr := m.dispatch(ctx, dispatch, input, func(_ int64, chunk *einoschema.AgenticMessage) {
		content, reasoning := deltaText(chunk)
		if content != "" {
			m.host.sessionObserver.AppendText(live, content)
		}
		m.execution.eventSink().Emit(ctx, session.EventRecord{
			Kind: EventMessageDelta, SessionID: m.engine.snapshot.SessionID, RunID: m.engine.snapshot.RunID,
			MessageID: dispatch.messageID, EpochID: m.engine.snapshot.EpochID, TurnID: m.engine.turn.ID, AgentPath: m.engine.agentPath,
			ProviderID: string(m.activeModel().Provider.ID), ModelID: string(m.activeModel().Model.ID),
			Payload: mustJSON(map[string]string{"content": content, "reasoning": reasoning}), LiveOnly: true, CreatedAt: m.host.now(),
		})
	})
	m.host.sessionObserver.FinishAttempt(live)
	if dispatchErr != nil {
		m.engine.addUsage(result.usage)
		if result.receivedDelta {
			dispatchErr = fmt.Errorf("%w: %w", errPartialStreamObserved, dispatchErr)
		}
		_ = m.finish(ctx, dispatch, result.usage, dispatchErr, dispatchErr)
		return nil, dispatchErr
	}
	committed, err := m.commit(ctx, dispatch, result.message)
	// The physical provider call succeeded regardless of what happens next
	// (a commit validation failure or a subsequent ledger-write failure),
	// so this dispatch's usage is charged now -- it must survive even if
	// finish()'s own ledger update below fails and this call returns early
	// (see TestTerminalLedgerFailureOverridesProviderResultAndRetainsUsage).
	m.engine.addUsage(result.usage)
	if err != nil {
		// dispatchErr was nil (the provider call itself succeeded); this is
		// a durable-persist validation failure, so the ledger row is still
		// terminally Completed -- see finish's doc comment.
		_ = m.finish(ctx, dispatch, result.usage, nil, err)
		return nil, err
	}
	if err := m.finish(ctx, dispatch, result.usage, nil, nil); err != nil {
		return nil, fmt.Errorf("%w: %w", errCommittedDispatchFailed, err)
	}
	if m.approval != nil {
		if err := m.approval.pause(ctx, m, dispatch, committed); err != nil {
			return nil, fmt.Errorf("%w: %w", errCommittedDispatchFailed, err)
		}
	}
	return einoschema.StreamReaderFromArray([]*einoschema.AgenticMessage{committed}), nil
}

// stripADKInternalExtra deep-copies messages with every message-level and
// content-block-level Extra map cleared. ADK's own ReAct loop tags messages
// it reconstructs between iterations (the assistant's own prior tool-call
// message, echoed back as part of input on the next physical dispatch) with
// framework-internal Extra bookkeeping; model.Request.Clone (used by the
// audit path -- see auditModelRequest) rejects ANY non-empty Extra found
// anywhere in a message as unauditable transient state, by design (W3: "..
// Request.Clone rejects any Extra so that transient state can never re-enter
// a request"). That guard is correct for provider-origin state but ADK's own
// internal markers are neither provider state nor something this package
// needs to persist, so they are stripped before a message ever reaches
// audit or dispatch.
func stripADKInternalExtra(messages []*einoschema.AgenticMessage) []*einoschema.AgenticMessage {
	if len(messages) == 0 {
		return messages
	}
	out := make([]*einoschema.AgenticMessage, len(messages))
	for i, msg := range messages {
		if msg == nil {
			continue
		}
		cloned := *msg
		cloned.Extra = nil
		if len(msg.ContentBlocks) != 0 {
			blocks := make([]*einoschema.ContentBlock, len(msg.ContentBlocks))
			for j, block := range msg.ContentBlocks {
				if block == nil {
					continue
				}
				clonedBlock := *block
				clonedBlock.Extra = nil
				blocks[j] = &clonedBlock
			}
			cloned.ContentBlocks = blocks
		}
		out[i] = &cloned
	}
	return out
}

// errADKProjectionDiverged reports that the set of tool-call IDs ADK's
// in-memory view presented does not match the durable projection's: ADK's
// own bookkeeping has desynced from what this run has actually committed,
// and dispatching against either view uncorroborated by the other would risk
// sending the model state that was never durably recorded (or silently
// dropping settled tool results). This must fail the dispatch, not degrade.
var errADKProjectionDiverged = errors.New("adk in-memory tool-call view diverged from the durable projection")

// errCommittedDispatchFailed wraps an error that occurred strictly after
// adkModel.commit durably persisted this dispatch's assistant message and
// tool calls (a ledger-state-update or post-commit approval-pause failure).
// A retry or failover wrapper re-dispatching past this point would issue a
// second physical model call whose result has no durable slot left to
// commit against -- the first attempt's output is already the turn's
// record. defaultShouldRetry/defaultShouldFailover both refuse to retry an
// error matching this sentinel (see their doc comments).
var errCommittedDispatchFailed = errors.New("model output already durably committed; error occurred after commit")

// errPartialStreamObserved wraps a mid-stream dispatch error that occurred
// after this dispatch's underlying provider call (dispatch always uses the
// streaming transport -- see dispatch's doc comment) had already received at
// least one delta chunk (modelStreamResult.receivedDelta), in both Generate
// and Stream. Usage has already been accumulated from those chunks (see
// addUsage on the error path) and, for Stream, at least the first chunk may
// already have been published to the session observer / live event sink.
// Retrying or failing over past this point would double-count that usage or
// show the user a second, possibly divergent response appended after a
// partial one it already started receiving.
// defaultShouldRetry/defaultShouldFailover both refuse to retry an error
// matching this sentinel.
var errPartialStreamObserved = errors.New("dispatch failed after a partial provider stream was already observed")

// durableProjection replaces ADK's in-memory transcript with the durable
// projection of this run's committed history: prior turns via
// history.LoadAgentic/ProjectAgentic (loadProviderHistory) plus this turn's
// own already-committed assistant/tool messages, which loadProviderHistory
// picks up for free since adkModel.commit and adkTool/adkToolSearch commit
// every fact to the store before ever returning to ADK -- excluding the
// current dispatch's own not-yet-finalized placeholder, which
// loadProviderHistory cannot see because FinalizeAssistantMessage has not
// run for it yet. This is what makes a tool_search call's result the actual
// persisted tool_search_result block, and an enhanced tool result the actual
// persisted multi-part function_tool_result block, exactly as replay would
// see them: ADK's own generic string-shaped tool.InvokableTool return value
// (see adkTool.InvokableRun) never reaches the model at all.
//
// Before substituting the projection, the set of tool-call IDs ADK's own
// input carries (from function_tool_call/function_tool_result blocks) is
// checked against the durable projection's set; any divergence fails closed
// (errADKProjectionDiverged) rather than silently trusting either view.
func (m *adkModel) durableProjection(ctx context.Context, adkInput []*einoschema.AgenticMessage) ([]*einoschema.AgenticMessage, error) {
	full, fullState, err := loadProviderHistory(ctx, m.host.store, session.Session{ID: m.engine.snapshot.SessionID}, m.engine.historyOptions, m.activeModel())
	if err != nil {
		return nil, err
	}
	// history.LoadAgentic/ProjectAgentic has no notion of "unfinalized": the
	// only reason the classic engine never saw its own not-yet-committed
	// assistant placeholder is that it always loaded history before minting
	// one. This engine reloads mid-turn, after AdmitTurn has already created
	// this turn's placeholder row (and, on a later physical dispatch within
	// the same turn, after currentMessageID may have minted another for a
	// still-in-flight response) -- both exist as real rows with zero parts
	// until adkModel.commit finalizes them. A committed assistant message
	// always carries at least one content block (the model always returns
	// something); an assistant message with none can therefore only be such
	// a placeholder, and is dropped. providerState entries are reindexed to
	// match (an entry addressing a dropped placeholder is dropped with it --
	// a placeholder with no committed content has no captured state either).
	full, fullState = dropUnfinalizedAssistantPlaceholders(full, fullState)
	if len(full) < m.engine.baseMessageCount {
		return nil, fmt.Errorf("%w: durable history shrank below this turn's admitted base (%d < %d)", errADKProjectionDiverged, len(full), m.engine.baseMessageCount)
	}
	// The turn's own already-transformed snapshot (prepareSnapshot's output,
	// computed once at turn-admission time) is the correct prefix: it may
	// carry ephemeral extension-injected content (contextAssemblePoint,
	// e.g. a system prompt or RAG content) that this package never persists
	// durably, by design, so a bare store reload can never see it -- and it
	// is exactly what ADK's own input was originally seeded with, so
	// splicing here (instead of re-deriving the whole array from scratch)
	// preserves the extension's chosen positions relative to that prefix
	// exactly as the classic engine did. Only the tail -- durable content
	// committed since admission (this turn's own tool-loop progress:
	// assistant responses, tool/tool-search results) -- comes from the fresh
	// reload, which is what makes a tool_search call's result the actual
	// persisted tool_search_result block and an enhanced tool result the
	// actual persisted multi-part function_tool_result block, exactly as
	// replay would see them.
	prefix := m.engine.snapshot.Messages
	tail := full[m.engine.baseMessageCount:]
	projected := make([]*einoschema.AgenticMessage, 0, len(prefix)+len(tail))
	projected = append(projected, prefix...)
	projected = append(projected, tail...)
	offset := len(prefix) - m.engine.baseMessageCount
	providerState := append([]model.ProviderMessageState(nil), m.engine.snapshot.providerState...)
	for _, state := range fullState {
		if state.MessageIndex < m.engine.baseMessageCount {
			continue
		}
		state.MessageIndex += offset
		providerState = append(providerState, state)
	}
	adkIDs := toolCallIDSet(adkInput)
	durableIDs := toolCallIDSet(projected)
	// A sibling function_tool_call on the same assistant message as an
	// MCPToolApprovalRequest is permanently orphaned once the approval pause
	// fires (adkApprovalBinding.pause): ADK's tools node never dispatches it
	// (the model node itself errors with a StatefulInterrupt before the
	// tools node ever runs), and a decided resume's continuation is a fresh
	// physical dispatch, not a resumption of that same ReAct iteration --
	// ADK's own resumed input legitimately never mentions it again, even
	// though this package's commit already durably persisted it (as a
	// still-pending call) before the pause. Exclude these IDs from the
	// durable side of the comparison rather than failing every approval
	// pause with a sibling call closed.
	for id := range approvalOrphanedToolCallIDs(projected) {
		delete(durableIDs, id)
	}
	if !sameIDSet(adkIDs, durableIDs) {
		return nil, fmt.Errorf("%w: adk=%v durable=%v", errADKProjectionDiverged, sortedKeys(adkIDs), sortedKeys(durableIDs))
	}
	m.engine.snapshot.providerState = providerState
	return stripADKInternalExtra(projected), nil
}

func dropUnfinalizedAssistantPlaceholders(messages []*einoschema.AgenticMessage, providerState []model.ProviderMessageState) ([]*einoschema.AgenticMessage, []model.ProviderMessageState) {
	remap := make(map[int]int, len(messages))
	kept := make([]*einoschema.AgenticMessage, 0, len(messages))
	for oldIndex, msg := range messages {
		if msg != nil && msg.Role == einoschema.AgenticRoleTypeAssistant && len(msg.ContentBlocks) == 0 {
			continue
		}
		remap[oldIndex] = len(kept)
		kept = append(kept, msg)
	}
	if len(kept) == len(messages) {
		return messages, providerState
	}
	keptState := make([]model.ProviderMessageState, 0, len(providerState))
	for _, state := range providerState {
		if newIndex, ok := remap[state.MessageIndex]; ok {
			state.MessageIndex = newIndex
			keptState = append(keptState, state)
		}
	}
	return kept, keptState
}

// toolCallIDSet collects every function_tool_call/function_tool_result
// CallID appearing anywhere in messages.
func toolCallIDSet(messages []*einoschema.AgenticMessage) map[string]bool {
	ids := make(map[string]bool)
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block == nil {
				continue
			}
			switch block.Type {
			case einoschema.ContentBlockTypeFunctionToolCall:
				if block.FunctionToolCall != nil && block.FunctionToolCall.CallID != "" {
					ids[block.FunctionToolCall.CallID] = true
				}
			case einoschema.ContentBlockTypeFunctionToolResult:
				if block.FunctionToolResult != nil && block.FunctionToolResult.CallID != "" {
					ids[block.FunctionToolResult.CallID] = true
				}
			}
		}
	}
	return ids
}

// approvalOrphanedToolCallIDs returns the function_tool_call CallIDs of every
// sibling call on an assistant message that also carries an
// MCPToolApprovalRequest block (see durableProjection's caller comment).
func approvalOrphanedToolCallIDs(messages []*einoschema.AgenticMessage) map[string]bool {
	orphaned := map[string]bool{}
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		hasApprovalRequest := false
		for _, block := range msg.ContentBlocks {
			if block != nil && block.Type == einoschema.ContentBlockTypeMCPToolApprovalRequest {
				hasApprovalRequest = true
				break
			}
		}
		if !hasApprovalRequest {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block != nil && block.Type == einoschema.ContentBlockTypeFunctionToolCall && block.FunctionToolCall != nil && block.FunctionToolCall.CallID != "" {
				orphaned[block.FunctionToolCall.CallID] = true
			}
		}
	}
	return orphaned
}

func sameIDSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if !b[id] {
			return false
		}
	}
	return true
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// dispatch performs the one physical provider call this adapter ever makes
// (both Generate and Stream funnel through the streaming provider
// transport), fully draining the response before returning: no live chunk
// ever reaches ADK directly (see receiveModelStream's callers here).
func (m *adkModel) dispatch(ctx context.Context, d *adkDispatch, input []*einoschema.AgenticMessage, onDelta func(int64, *einoschema.AgenticMessage)) (result modelStreamResult, err error) {
	// d.record.Step, not Attempt, is what actually varies per physical
	// dispatch under the invocation-per-physical-dispatch ledger model (see
	// adk_retry.go): Attempt is pinned to 1 for every dispatch, so using it
	// here would collide every retry/tool-loop dispatch's stream
	// observation onto the same "attempt-1" correlation ID.
	observation := m.host.startObservedStream(ctx, m.engine.snapshot, d.messageID, d.record.Step)
	request := m.dispatchSnapshot().ProviderRequest(d.messageID, m.host.trace, input, m.execution.discoveredSnapshot())
	request.System = d.record.System
	request.IdempotencyKey = string(d.record.ID)
	// A panic from the provider transport (either the initial
	// StreamProvider call or a later Recv() while receiveModelStream drains
	// it) must never propagate: its payload can carry provider-side
	// secrets (e.g. request credentials echoed back into a panic message),
	// and letting it unwind past this adapter would also skip finish()'s
	// ledger update, leaving the request row stuck at "dispatch_started"
	// forever. Recover, fold it into the fixed, secret-free
	// providerStreamPanicMessage sentinel, and return normally so
	// Generate/Stream's ordinary error path (which calls finish()) still
	// runs. Usage receiveModelStream already merged into the named `result`
	// return value before the panic survives regardless (see
	// TestLedgerRetainsPartialStateAfterReceivePanic).
	defer func() {
		if recovered := recover(); recovered != nil {
			err = newProviderStreamPanicError()
			m.host.errorObservedStream(observation, err, result.usage)
		}
	}()
	reader, streamErr := m.activeModel().Streamer.StreamProvider(ctx, request)
	if streamErr != nil {
		m.host.errorObservedStream(observation, streamErr, model.Usage{})
		return modelStreamResult{}, streamErr
	}
	receiveModelStream(ctx, reader, m.host.streamLimits, &result, func(index int64, chunk *einoschema.AgenticMessage) {
		m.host.observeStreamChunk(observation, index)
		if onDelta != nil {
			onDelta(index, chunk)
		}
	})
	if result.err != nil {
		m.host.errorObservedStream(observation, result.err, result.usage)
		return result, result.err
	}
	m.host.endObservedStream(observation, result.usage)
	return result, nil
}

// commit persists the assistant result through the existing durable seams --
// captured provider-private state, canonical pending tool-call records (with
// W4 alias resolution and RequestedName), and the finalized assistant
// message -- inside one fenced transaction, exactly mirroring the classic
// engine's per-step commit (see the deleted orchestrator.executeTurn). A
// block kind this pipeline cannot record (server/MCP-result/tool-search/
// generated-media blocks the content contract does not persist here, or an
// approval request with no bound approval adapter) fails closed before ADK
// ever sees it.
func (m *adkModel) commit(ctx context.Context, dispatch *adkDispatch, result *einoschema.AgenticMessage) (*einoschema.AgenticMessage, error) {
	if result == nil {
		return nil, errors.New("nil agentic result")
	}
	for _, block := range result.ContentBlocks {
		if block == nil {
			continue
		}
		switch block.Type {
		// Every block kind a model may legitimately emit as its own output:
		// generated content (text/reasoning/media) and calls it originates
		// (function tool calls). session.contentBlockFromEino durably
		// supports each of these as ordinary content; there is no execution
		// wiring gap for pure-content media blocks the way there would be
		// for a call kind this runtime does not yet dispatch (e.g. a
		// server-side or MCP tool call), so those remain unsupported here
		// rather than silently accepted without a dispatch path.
		case einoschema.ContentBlockTypeAssistantGenText, einoschema.ContentBlockTypeReasoning, einoschema.ContentBlockTypeFunctionToolCall,
			einoschema.ContentBlockTypeAssistantGenImage, einoschema.ContentBlockTypeAssistantGenAudio, einoschema.ContentBlockTypeAssistantGenVideo:
		case einoschema.ContentBlockTypeMCPToolApprovalRequest:
			if m.approval == nil {
				return nil, fmt.Errorf("%w: %s", errADKUnsupportedBlock, block.Type)
			}
		default:
			return nil, fmt.Errorf("%w: %s", errADKUnsupportedBlock, block.Type)
		}
	}
	normalizeToolCallIDs(result, m.host.ids)
	blockIDs := make([]string, len(result.ContentBlocks))
	for index := range blockIDs {
		blockIDs[index] = string(m.host.ids.NewPartID())
	}
	capturedState, publicMsg, err := captureAssistantProviderState(m.engine.snapshot, dispatch.messageID, result, blockIDs)
	if err != nil {
		return nil, err
	}
	result = publicMsg
	calls := functionToolCalls(result)
	if len(calls) != 0 {
		// This turn's ReAct loop will dispatch again after the tools
		// settle; that next physical dispatch's result is a new logical
		// assistant message, not a continuation of this one.
		m.needMessage = true
		callIDs := make([]session.ToolCallID, len(calls))
		for i, call := range calls {
			callIDs[i] = session.ToolCallID(call.CallID)
		}
		m.engine.registerToolBatch(dispatch.messageID, callIDs)
	}
	preparedCalls, err := m.host.prepareToolCalls(ctx, m.execution, m.engine.snapshot, dispatch.messageID, calls)
	if err != nil {
		return nil, err
	}
	for _, prepared := range preparedCalls {
		if prepared.middlewareErr != nil {
			m.engine.recordPrepareError(prepared.call.ID, prepared.middlewareErr)
		}
	}
	if _, err := m.host.persistAssistantTurn(ctx, m.execution, m.engine.snapshot, dispatch.messageID, result, blockIDs, capturedState.payloads, preparedCalls); err != nil {
		return nil, err
	}
	m.engine.recordResponseMessage(dispatch.messageID)
	// engine.snapshot.providerState is not updated incrementally here: every
	// dispatch's durableProjection recomputes it fresh from the store (the
	// PartProviderState parts persistAssistantTurn just wrote above,
	// included), so doing it here too would double-count the same captured
	// state once this message is picked up by the next dispatch's reload.
	return result, nil
}
