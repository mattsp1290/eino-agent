package runtime

import (
	"context"
	"errors"
	"fmt"
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

	// internalDispatch, when non-empty, marks this adapter instance as a
	// bounded internal-dispatch role (currently only "summarizer", handed to
	// the summarization recipe -- see adkEngine.buildAgentHandlers) instead
	// of the turn's own conversational model adapter. Every physical call
	// through an internal-dispatch instance is still durably ledgered
	// exactly like an ordinary dispatch -- its own ModelRequestRecord row
	// (AgentPath == internalDispatch), retried/failed over through the same
	// audited path, usage charged -- but currentMessageID never claims or
	// mints a real assistant message for it (it only ever reads the turn's
	// already-admitted placeholder ID for ledger correlation, never writes
	// to it), begin() strips tool controls and the rendered system prompt
	// from its request, prepareDispatchInput never resets the turn's shared
	// snapshot.providerState because of its shorter input, and commit
	// (commitInternal) never persists the result as conversational content
	// and fails closed if the result carries anything but plain text/
	// reasoning. A dedicated adkEngine.dispatches count and usage charge
	// still apply: it is a real, audited provider call, just never one that
	// becomes something the user or the next turn sees as "the assistant
	// said this".
	internalDispatch string

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
	// input is the durable-id-keyed projection begin received (this
	// package's own internal identity, ADK's own tracked conversation).
	input []*einoschema.AgenticMessage
	// wireInput is input with every function_tool_call/function_tool_result/
	// tool_search_result CallID rewritten to its provider-facing identity
	// (see publicizeToolCallIDs). It is computed exactly once, at the top
	// of begin, and is both what the request ledger audits/hashes and what
	// dispatch sends -- so the audited row and the physical wire payload
	// are always the same bytes (see begin's doc comment).
	wireInput []*einoschema.AgenticMessage
}

// currentMessageID claims the turn's assistant placeholder for exactly one
// dispatch (the first that reaches here); every later dispatch within the
// same turn (tool-turn continuation, a retry/failover attempt after a
// committed step, or a child agent) mints and persists its own assistant
// message, parented to the turn's placeholder.
func (m *adkModel) currentMessageID(ctx context.Context) (session.MessageID, error) {
	if m.internalDispatch != "" {
		// An internal-dispatch adapter never claims or mints a real
		// assistant message: it reads the turn's already-admitted
		// placeholder ID purely so its ledger row (ModelRequestRecord.
		// AssistantMessageID) correlates to a real durable message row for
		// audit purposes, without ever writing to it -- see
		// adkModel.internalDispatch's doc comment. It deliberately does not
		// call e.engine.claimPlaceholder(), which would consume the SAME
		// shared placeholderUsed flag the turn's own conversational adapter
		// still needs.
		return m.engine.assistantMessageID, nil
	}
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

// begin computes this dispatch's wire-shaped request (durable tool-call ids
// rewritten to their provider-facing identity, via publicizeToolCallIDs) and
// audits/ledgers exactly that request -- before nextStep/currentMessageID so
// a store read failure resolving a provider id neither consumes a step nor
// appends an assistant placeholder row. dispatch then sends d.wireInput
// unchanged: publicizeToolCallIDs runs exactly once per physical dispatch,
// so the ledger's Messages/ContentSHA256/ModelRequestedNotice and the bytes
// the provider actually receives can never diverge (see adk_model.go's W4
// doc references and docs/architecture/eino-feature-support.md's W4
// section).
func (m *adkModel) begin(ctx context.Context, input []*einoschema.AgenticMessage) (*adkDispatch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	wireInput, err := publicizeToolCallIDs(ctx, m.host.store, m.engine.snapshot.SessionID, &m.engine.toolCallIDCache, m.engine.authorizedRewrites, input)
	if err != nil {
		return nil, err
	}
	step := m.engine.nextStep()
	messageID, err := m.currentMessageID(ctx)
	if err != nil {
		return nil, err
	}
	request := m.dispatchSnapshot().ProviderRequest(messageID, m.host.trace, wireInput, m.execution.discoveredSnapshot())
	agentPath := m.engine.agentPath
	if m.internalDispatch != "" {
		// An internal-dispatch call (e.g. summarization's own
		// summary-generation request) carries no tool controls and no
		// rendered turn system prompt: it is not a conversational turn step,
		// so the model must never be offered this turn's tools to call, and
		// the request's AgentPath records the internal role rather than the
		// turn's own agent path -- see adkModel.internalDispatch's doc
		// comment.
		request.Controls = model.RequestControls{}
		agentPath = m.internalDispatch
	} else {
		request.System, err = m.host.renderSystemPrompt(ctx, m.engine.plan, m.engine.snapshot, 1, step)
		if err != nil {
			return nil, err
		}
	}
	request, audited, hash, err := auditModelRequest(request, m.host.modelRequestSafeOptions, m.host.modelRequestMaxBytes)
	if err != nil {
		return nil, err
	}
	record, err := m.host.prepareModelRequest(ctx, m.execution, m.engine.snapshot, request, audited, hash, messageID, modelRequestIdentity{
		InvocationID: m.host.ids.NewInvocationID(), TurnID: m.engine.turn.ID, AgentPath: agentPath, Attempt: 1, Step: step,
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
	// Durably record every authorized content-management rewrite queued
	// since the last drain (patchtoolcalls/reduction, via
	// wrapAuthorizedContentRewrites -- see authorizedRewriteRecord), each as
	// its own event correlated to this dispatch's messageID/turn/agent path.
	// A durable write failure here fails this dispatch rather than silently
	// losing the audit trail for a rewrite the seal already let through.
	for _, rewrite := range m.engine.authorizedRewrites.drainPending() {
		event := session.EventRecord{
			ID: m.host.ids.NewEventID(), SessionID: m.engine.snapshot.SessionID, RunID: m.engine.snapshot.RunID,
			MessageID: messageID, EpochID: m.engine.snapshot.EpochID, TurnID: m.engine.turn.ID, AgentPath: agentPath,
			Kind: session.AuthorizedToolResultRewriteEventKind, Correlation: rewrite.CallID,
			Payload: mustJSON(map[string]string{
				"handler_id": rewrite.HandlerID, "kind": rewrite.Kind, "call_id": rewrite.CallID,
				"before_digest": rewrite.BeforeDigest, "after_digest": rewrite.AfterDigest,
			}),
			CreatedAt: m.host.now(),
		}
		committed, err := m.execution.store.AppendEvent(ctx, event)
		if err != nil {
			return nil, err
		}
		m.execution.publishPersisted(ctx, committed)
	}
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
	// This dispatch's ledger row (record) is now durably committed
	// (ModelRequestDispatchStarted, above): count it so onAgentEvents can
	// tell a genuinely dispatched-and-ledgered turn apart from one whose
	// agent never routed a physical call through this adapter at all (see
	// adkEngine.dispatches's doc comment).
	m.engine.dispatches.Add(1)
	return &adkDispatch{messageID: messageID, record: record, input: input, wireInput: wireInput}, nil
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
	input, err := m.prepareDispatchInput(input)
	if err != nil {
		return nil, err
	}
	dispatch, err := m.begin(ctx, input)
	if err != nil {
		return nil, err
	}
	result, dispatchErr := m.dispatch(ctx, dispatch, nil)
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
	input, err := m.prepareDispatchInput(input)
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
	result, dispatchErr := m.dispatch(ctx, dispatch, func(_ int64, chunk *einoschema.AgenticMessage) {
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

// errADKProjectionDiverged reports that a fresh durable reload returned
// fewer messages than this turn was admitted with -- a genuine corruption
// signal (history should only ever grow within a turn), not a benign
// content-management restructuring by a host handler (which happens later,
// downstream of this check -- see buildDurableBaseline).
var errADKProjectionDiverged = errors.New("adk durable history diverged from this turn's admitted base")

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

// buildDurableBaseline computes this cycle's durable-projection baseline:
// prior turns' committed history plus this turn's own already-committed
// progress (tool results, prior assistant responses) via loadProviderHistory,
// spliced after the turn's ephemeral admission-time prefix. It is called by
// durableBaselineHandler.BeforeModelRewriteState (runtime/adk_middleware.go),
// the mandatory FIRST (outermost) handler in every turn's agent, once per
// logical model-call cycle -- i.e. once per ReAct iteration, not once per
// physical retry/failover attempt: ADK invokes every handler's
// BeforeModelRewriteState exactly once per handler chain, before the
// internal failover/retry wrapper (see adk/wrappers.go's
// typedChatModelAgentWrapper.Generate: handlers run, THEN
// `wrappedEndpoint(ctx, state.Messages, ...)` -- the failover/retry-wrapped
// call into this adapter -- runs once with that one fixed state.Messages
// value, however many physical attempts it takes).
//
// Host handlers (agentsmd, skill, reduction, summarization, patchtoolcalls,
// ...), registered after this one, then transform the baseline on top;
// adkModel.prepareDispatchInput dispatches exactly what they leave
// ADK's state.Messages as, without re-projecting -- see its own doc comment
// for how per-message private provider state survives that transformation,
// and settlementSeal (adk_middleware.go) for the two invariants that still
// apply regardless of what a handler does to the baseline: a settled tool
// call's result may never diverge from its durable Output unless the
// rewrite was authorized, and a fabricated function_tool_result for a call
// with no durable settlement is rejected unless authorized.
func (e *adkEngine) buildDurableBaseline(ctx context.Context) ([]*einoschema.AgenticMessage, error) {
	// e.historyOptions is resolved ONCE, at this turn's admission (see
	// resolveTurnHistoryOptions), not re-resolved per cycle: baseMessageCount
	// and every cycle's reload within one turn must agree on the SAME epoch
	// view, or the admission-time prefix/fresh-reload-tail splice below goes
	// out of sync. A summarization epoch this turn's own recipe commits mid-
	// turn therefore narrows the provider projection starting the NEXT turn
	// admitted on this session, not later cycles of this same turn -- see
	// resolveTurnHistoryOptions's doc comment.
	full, fullSourceIDs, fullState, err := loadProviderHistory(ctx, e.host.store, session.Session{ID: e.snapshot.SessionID}, e.historyOptions, e.snapshot.Model)
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
	full, fullSourceIDs, fullState = dropUnfinalizedAssistantPlaceholders(full, fullSourceIDs, fullState)
	if len(full) < e.baseMessageCount {
		return nil, fmt.Errorf("%w: durable history shrank below this turn's admitted base (%d < %d)", errADKProjectionDiverged, len(full), e.baseMessageCount)
	}
	// If THIS turn has already committed a compaction boundary mid-turn (a
	// summarization trigger that fired on an earlier cycle of this same
	// turn), reposition it in this fresh reload so it never sits between a
	// function_tool_call and its function_tool_result -- see
	// repositionMidTurnCompactionBoundary's own doc comment for why the raw
	// reload can otherwise interleave it there (round-four W6
	// correlation-and-seal review, Important #4/eino-agent-0wb's true root
	// cause). This is a no-op until a boundary has actually been committed
	// (e.summarizedBoundary() is empty for every cycle before that, and for
	// the very cycle that commits one -- its own buildDurableBaseline reload
	// already ran before Finalize existed to commit it).
	if boundaryID := e.summarizedBoundary(); boundaryID != "" {
		full, fullSourceIDs, fullState = repositionMidTurnCompactionBoundary(full, fullSourceIDs, fullState, boundaryID)
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
	prefix := e.snapshot.Messages
	tail := full[e.baseMessageCount:]
	baseline := make([]*einoschema.AgenticMessage, 0, len(prefix)+len(tail))
	baseline = append(baseline, prefix...)
	baseline = append(baseline, tail...)
	// baselineSourceIDs is baseline's own durable-message-ID parallel (round-
	// two W6 review item 8): the prefix portion's ids came from
	// e.snapshot.MessageSourceIDs (computed once at turn admission/resume,
	// remapped through prepareSnapshot's extension-transform BaseToFinal
	// mapping -- see prepareSnapshot), zero-padded defensively if shorter
	// than prefix (a call site that never populated it degrades to "no
	// durable id for any prefix message" rather than panicking); the tail
	// portion's ids come directly from this same fresh reload, in lockstep
	// with tail itself. durableBaselineHandler turns this into a
	// pointer-keyed map against the CLONED messages it actually hands ADK,
	// consumed by summarizationFinalize -- see that handler's doc comment.
	baselineSourceIDs := make([]session.MessageID, 0, len(prefix)+len(tail))
	baselineSourceIDs = append(baselineSourceIDs, paddedMessageSourceIDs(e.snapshot.MessageSourceIDs, len(prefix))...)
	baselineSourceIDs = append(baselineSourceIDs, fullSourceIDs[e.baseMessageCount:]...)
	offset := len(prefix) - e.baseMessageCount
	providerState := append([]model.ProviderMessageState(nil), e.snapshot.providerState...)
	for _, state := range fullState {
		if state.MessageIndex < e.baseMessageCount {
			continue
		}
		state.MessageIndex += offset
		providerState = append(providerState, state)
	}
	// e.snapshot.providerState keeps its original accumulate-and-carry-
	// forward role (matching the pre-W6 durableProjection exactly): each
	// cycle's freshly-reloaded tail contributions are folded on top of
	// whatever it already held. e.baselineMessages is this cycle's message
	// list this same providerState was computed against -- see
	// adkModel.prepareDispatchInput, which trusts it unchanged when a host
	// handler leaves ADK's state.Messages the same length (the common
	// case: content edited in place, or new messages only appended) and
	// drops it when the length differs (a host handler restructured
	// history -- summarization compacting is the only one of this
	// package's own recipes that does), rather than risk misapplying it to
	// the wrong message.
	e.snapshot.providerState = providerState
	e.baselineMessages = baseline
	e.baselineSourceIDs = baselineSourceIDs
	return baseline, nil
}

// summarizedBoundary reports the durable message id of the compaction
// boundary this turn's own summarization recipe has committed, if any --
// see adkEngine.summarizedBoundaryID's doc comment.
func (e *adkEngine) summarizedBoundary() session.MessageID {
	e.summarizeMu.Lock()
	defer e.summarizeMu.Unlock()
	return e.summarizedBoundaryID
}

// repositionMidTurnCompactionBoundary moves the durable message identified
// by boundaryID later within full (in memory only -- this never touches the
// store) so it never sits between a function_tool_call and its
// function_tool_result.
//
// Why this is needed: a turn's own assistant placeholder message row is
// created durably at admission (AdmitTurn), before that turn's first
// cycle's own handler chain ever runs, and adkModel.currentMessageID reuses
// that SAME row (via claimPlaceholder) for the turn's first physical
// dispatch. If summarization's trigger fires on that very first cycle --
// entirely plausible: TriggerContextMessages/TriggerContextTokens are
// evaluated against whatever history already exists BEFORE this cycle's own
// dispatch runs -- Finalize commits the compaction boundary durably before
// this cycle has generated (let alone committed) its own response. When
// that response turns out to carry a function_tool_call, the call is
// persisted into the SAME pre-existing placeholder row (an early durable
// position, from admission), while its function_tool_result is only
// minted once the tool actually settles (a fresh, later position) -- and
// the boundary committed in between durably sorts, by the store's own
// created_at/id ordering, textually between the two. A LATER cycle's own
// buildDurableBaseline reload (this turn's own admission-time epoch view
// deliberately does not narrow via applyEpoch until the NEXT turn -- see
// this function's caller) then hands a host handler chain a raw sequence
// with a system-role compaction message wedged inside a call/result group.
// Upstream patchtoolcalls' hasCorrespondingAgenticToolResult requires
// strict adjacency (the result must be the literally next message with no
// intervening non-result message) and judges the already-settled call
// dangling, appending a fabricated second result that settlementSeal
// correctly rejects as a diverged occurrence -- failing the turn closed
// (round-four W6 correlation-and-seal review, Important #4/eino-agent-0wb).
//
// This mirrors, for a mid-turn boundary's PLACEMENT, the same invariant
// moveTailStartToGroupBoundary already enforces for the tail's own CUT
// POINT, in the same direction: never move it later, only earlier, until it
// no longer separates a function_tool_call from its function_tool_result.
// It scans full[0:boundaryIndex] for every function_tool_call CallID with no
// matching result also before the boundary, then narrows that set to only
// the ones actually SPLIT by the boundary: a call whose own result lands at
// or after boundaryIndex. A call with no result anywhere in full -- a
// genuinely dangling call, exactly what patchtoolcalls exists to patch
// after external history editing -- is not split by anything and must not
// drag the boundary backwards over unrelated earlier content (round-four W6
// final-integration review followup, Important #1: an unnarrowed version of
// this check moved the boundary for a pre-existing dangling call that the
// boundary split nothing at all, and could reorder two boundaries relative
// to each other).
//
// If any split call remains, the boundary moves to sit immediately before
// the EARLIEST one -- ahead of every group it would otherwise split,
// matching where the boundary's own commit is causally accurate (Finalize
// ran, and committed it, strictly before this cycle's own dispatch produced
// that call) -- but never earlier than the session's leading contiguous
// system-role prefix: applyEpoch (session/history/projector.go) always
// places an already-narrowed EARLIER epoch's own boundary message
// immediately after that same leading run (also role=system, by
// compaction.NewBoundary's own construction), so this floor keeps both the
// genuine leading system prefix and any earlier boundary in place, matching
// what applyEpoch/summarizationFinalize already treat as untouchable. If no
// call before the boundary is left open after narrowing, the boundary is
// already safe and is left exactly where it is.
func repositionMidTurnCompactionBoundary(
	full []*einoschema.AgenticMessage,
	fullSourceIDs []session.MessageID,
	fullState []model.ProviderMessageState,
	boundaryID session.MessageID,
) ([]*einoschema.AgenticMessage, []session.MessageID, []model.ProviderMessageState) {
	boundaryIndex := -1
	for i, id := range fullSourceIDs {
		if id == boundaryID {
			boundaryIndex = i
			break
		}
	}
	if boundaryIndex < 0 {
		return full, fullSourceIDs, fullState
	}
	// open tracks, for every function_tool_call CallID seen in full[0:boundaryIndex]
	// with no result yet seen there, the index of the message that first
	// introduced it -- firstOpenIndex[id].
	open := make(map[string]bool)
	firstOpenIndex := make(map[string]int)
	for i := 0; i < boundaryIndex; i++ {
		recordToolCallAdjacency(full[i], open)
		for id := range open {
			if _, seen := firstOpenIndex[id]; !seen {
				firstOpenIndex[id] = i
			}
		}
		for id := range firstOpenIndex {
			if !open[id] {
				delete(firstOpenIndex, id)
			}
		}
	}
	if len(open) == 0 {
		return full, fullSourceIDs, fullState
	}
	// Narrow open to calls actually SPLIT by the boundary: their own result
	// must land at or after boundaryIndex. A call still "open" at
	// boundaryIndex but with no result anywhere in full is genuinely
	// dangling, not split, and must be dropped here -- see this function's
	// own doc comment.
	settledAfter := make(map[string]bool, len(open))
	for i := boundaryIndex; i < len(full); i++ {
		recordSettledCallIDs(full[i], settledAfter)
	}
	for id := range open {
		if !settledAfter[id] {
			delete(open, id)
		}
	}
	if len(open) == 0 {
		return full, fullSourceIDs, fullState
	}
	target := boundaryIndex
	for id := range open {
		if idx := firstOpenIndex[id]; idx < target {
			target = idx
		}
	}
	// Never move the boundary ahead of the session's leading contiguous
	// system-role prefix -- which also covers any earlier epoch's own
	// boundary message, itself role=system and placed immediately after
	// that prefix by applyEpoch (see this function's own doc comment).
	systemPrefix := 0
	for systemPrefix < len(full) && full[systemPrefix].Role == einoschema.AgenticRoleTypeSystem {
		systemPrefix++
	}
	if target < systemPrefix {
		target = systemPrefix
	}
	if target >= boundaryIndex {
		// Should not happen given len(open) > 0 above, but never move the
		// boundary somewhere that isn't strictly earlier.
		return full, fullSourceIDs, fullState
	}
	newOrder := make([]int, 0, len(full))
	for i := 0; i < len(full); i++ {
		if i == boundaryIndex {
			continue
		}
		if i == target {
			newOrder = append(newOrder, boundaryIndex)
		}
		newOrder = append(newOrder, i)
	}
	reorderedMessages := make([]*einoschema.AgenticMessage, len(full))
	reorderedSourceIDs := make([]session.MessageID, len(fullSourceIDs))
	oldToNew := make([]int, len(full))
	for newIndex, oldIndex := range newOrder {
		reorderedMessages[newIndex] = full[oldIndex]
		reorderedSourceIDs[newIndex] = fullSourceIDs[oldIndex]
		oldToNew[oldIndex] = newIndex
	}
	reorderedState := make([]model.ProviderMessageState, len(fullState))
	for i, state := range fullState {
		// oldToNew is indexed by this reorder's own full/fullState, which
		// buildDurableBaseline always keeps in lockstep (round-four W6
		// correlation-and-seal review followup, Suggestion S-D-1): guarded
		// rather than trusted, so a future caller passing a stale or
		// mismatched fullState degrades this one entry instead of panicking.
		idx := state.MessageIndex
		if idx < 0 || idx >= len(oldToNew) {
			reorderedState[i] = state
			continue
		}
		state.MessageIndex = oldToNew[idx]
		reorderedState[i] = state
	}
	return reorderedMessages, reorderedSourceIDs, reorderedState
}

// recordToolCallAdjacency updates open (a set of function_tool_call CallIDs
// with no result seen yet) for one message: every function_tool_call block
// it carries opens its CallID, and every block that settles a call --
// function_tool_result (an ordinary tool call) or ToolSearchFunctionToolResult
// (toolsearch's own dynamictool discovery call, a distinct content block type
// from an ordinary function_tool_result -- see moveTailStartToGroupBoundary,
// which checks the same two durable session.PartKind values,
// PartFunctionToolResult and PartToolSearchResult, for the identical reason)
// -- closes one.
func recordToolCallAdjacency(msg *einoschema.AgenticMessage, open map[string]bool) {
	if msg == nil {
		return
	}
	for _, block := range msg.ContentBlocks {
		if block == nil {
			continue
		}
		if block.FunctionToolCall != nil {
			open[block.FunctionToolCall.CallID] = true
		}
		if block.FunctionToolResult != nil {
			delete(open, block.FunctionToolResult.CallID)
		}
		if block.ToolSearchFunctionToolResult != nil {
			delete(open, block.ToolSearchFunctionToolResult.CallID)
		}
	}
}

// recordSettledCallIDs marks, in settled, every CallID one message settles
// via a function_tool_result or ToolSearchFunctionToolResult block -- the
// forward-scan counterpart to recordToolCallAdjacency, used by
// repositionMidTurnCompactionBoundary to distinguish a call actually SPLIT
// by the boundary (its result lands at or after it) from a genuinely
// dangling call (no result anywhere), which must not move the boundary.
func recordSettledCallIDs(msg *einoschema.AgenticMessage, settled map[string]bool) {
	if msg == nil {
		return
	}
	for _, block := range msg.ContentBlocks {
		if block == nil {
			continue
		}
		if block.FunctionToolResult != nil {
			settled[block.FunctionToolResult.CallID] = true
		}
		if block.ToolSearchFunctionToolResult != nil {
			settled[block.ToolSearchFunctionToolResult.CallID] = true
		}
	}
}

// paddedMessageSourceIDs returns ids truncated or zero-padded to exactly n
// entries: a defensive normalization so a TurnSnapshot whose
// MessageSourceIDs is nil, short, or (in principle) long relative to its
// own Messages never causes an index panic here -- a missing entry simply
// means "no durable id for this message" (see buildDurableBaseline).
func paddedMessageSourceIDs(ids []session.MessageID, n int) []session.MessageID {
	out := make([]session.MessageID, n)
	copy(out, ids)
	return out
}

func dropUnfinalizedAssistantPlaceholders(messages []*einoschema.AgenticMessage, sourceIDs []session.MessageID, providerState []model.ProviderMessageState) ([]*einoschema.AgenticMessage, []session.MessageID, []model.ProviderMessageState) {
	remap := make(map[int]int, len(messages))
	kept := make([]*einoschema.AgenticMessage, 0, len(messages))
	keptSourceIDs := make([]session.MessageID, 0, len(messages))
	paddedSourceIDs := paddedMessageSourceIDs(sourceIDs, len(messages))
	for oldIndex, msg := range messages {
		if msg != nil && msg.Role == einoschema.AgenticRoleTypeAssistant && len(msg.ContentBlocks) == 0 {
			continue
		}
		remap[oldIndex] = len(kept)
		kept = append(kept, msg)
		keptSourceIDs = append(keptSourceIDs, paddedSourceIDs[oldIndex])
	}
	if len(kept) == len(messages) {
		return messages, paddedSourceIDs, providerState
	}
	keptState := make([]model.ProviderMessageState, 0, len(providerState))
	for _, state := range providerState {
		if newIndex, ok := remap[state.MessageIndex]; ok {
			state.MessageIndex = newIndex
			keptState = append(keptState, state)
		}
	}
	return kept, keptSourceIDs, keptState
}

// prepareDispatchInput is this adapter's counterpart to
// adkEngine.buildDurableBaseline: it never re-projects input (the ledger
// adapter is the mandatory innermost dispatch authority -- it audits and
// dispatches exactly what the handler chain leaves ADK's state.Messages
// as), and it trusts m.engine.snapshot.providerState (already computed by
// buildDurableBaseline against m.engine.baselineMessages, this same cycle)
// exactly when input is the same length as that baseline -- the common
// case, whether input is the identical slice/pointers or a defensively
// cloned copy of it (ADK's own retry/failover wrapper may clone before a
// physical attempt) with content edited in place or new messages appended
// after the same prefix. When input is shorter (a host handler
// restructured/compacted history -- summarization is the only one of this
// package's own recipes that does), the captured provider state can no
// longer be trusted to address the right message, so it is dropped rather
// than risked being misapplied -- a bounded, documented degradation, not
// silent corruption.
//
// Steady-state note (round-four W6 correlation-and-seal review followup,
// Suggestion S-B): once a turn has summarized, reapplyDurableSummary
// re-collapses state.Messages on EVERY later cycle (not just the one that
// triggered the compaction), so this reset now fires on every remaining
// cycle of such a turn, not once. Captured per-message provider state
// (e.g. cached reasoning/provider-side blocks) is therefore discarded for
// the rest of a turn that summarized mid-way through, which is the correct
// trade against misapplying it to the wrong message -- and strictly better
// than the context regrowth reapplyDurableSummary replaced -- but it is now
// the norm for such a turn's tail, not a one-cycle exception. Finally
// strips ADK's own internal per-iteration Extra bookkeeping
// (stripADKInternalExtra).
func (m *adkModel) prepareDispatchInput(input []*einoschema.AgenticMessage) ([]*einoschema.AgenticMessage, error) {
	// An internal-dispatch call's input (e.g. summarization's own,
	// deliberately shorter, in-progress summary request) must never trigger
	// this reset: it is not the turn's own conversational dispatch, so its
	// length relative to m.engine.baselineMessages says nothing about
	// whether the TURN's history was restructured.
	if m.internalDispatch == "" && len(input) < len(m.engine.baselineMessages) {
		m.engine.snapshot.providerState = nil
	}
	if err := verifySettledToolResults(m.engine.baselineMessages, input, m.engine.authorizedRewrites); err != nil {
		return nil, err
	}
	return stripADKInternalExtra(input), nil
}

// dispatch performs the one physical provider call this adapter ever makes
// (both Generate and Stream funnel through the streaming provider
// transport), fully draining the response before returning: no live chunk
// ever reaches ADK directly (see receiveModelStream's callers here).
func (m *adkModel) dispatch(ctx context.Context, d *adkDispatch, onDelta func(int64, *einoschema.AgenticMessage)) (result modelStreamResult, err error) {
	// d.wireInput already shows the provider its own tool-call ids back
	// (begin computed it via publicizeToolCallIDs, exactly once for this
	// dispatch -- see begin's doc comment): sending it unchanged here is
	// what keeps the ledgered/audited request and the physical wire
	// payload identical. d.input (durable-id-keyed) is left untouched so
	// this package's own bookkeeping (registerToolBatch, ADK's own tracked
	// conversation via compose.GetToolCallID) stays keyed on the durable id
	// it has always used.
	// d.record.Step, not Attempt, is what actually varies per physical
	// dispatch under the invocation-per-physical-dispatch ledger model (see
	// adk_retry.go): Attempt is pinned to 1 for every dispatch, so using it
	// here would collide every retry/tool-loop dispatch's stream
	// observation onto the same "attempt-1" correlation ID.
	observation := m.host.startObservedStream(ctx, m.engine.snapshot, d.messageID, d.record.Step)
	request := m.dispatchSnapshot().ProviderRequest(d.messageID, m.host.trace, d.wireInput, m.execution.discoveredSnapshot())
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
	if m.internalDispatch != "" {
		return m.commitInternal(result)
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
	preparedCalls, err := m.host.prepareToolCalls(ctx, m.execution, m.engine.snapshot, dispatch.messageID, calls)
	if err != nil {
		return nil, err
	}
	// callIDs is read from preparedCalls (prepareToolCalls's own minted
	// call.ID) after prepareToolCalls returns, never from calls[i].CallID
	// captured beforehand: prepareToolCalls unconditionally overwrites each
	// block's CallID with a fresh durable mint (see its own doc comment),
	// so registerToolBatch below must key on that same durable id -- the
	// one ADK will actually dispatch with -- not the provider's
	// pre-mutation value.
	callIDs := make([]session.ToolCallID, len(preparedCalls))
	for i := range preparedCalls {
		callIDs[i] = preparedCalls[i].call.ID
	}
	for _, prepared := range preparedCalls {
		if prepared.middlewareErr != nil {
			m.engine.recordPrepareError(prepared.call.ID, prepared.middlewareErr)
		}
	}
	if _, err := m.host.persistAssistantTurn(ctx, m.execution, m.engine.snapshot, dispatch.messageID, result, blockIDs, capturedState.payloads, preparedCalls); err != nil {
		return nil, err
	}
	if len(callIDs) != 0 {
		// This turn's ReAct loop will dispatch again after the tools
		// settle; that next physical dispatch's result is a new logical
		// assistant message, not a continuation of this one. Set only now
		// that persistAssistantTurn has durably committed: a retried commit
		// that fails here must not have already orphaned the turn's
		// assistant placeholder as an empty durable row by flipping
		// needMessage before the write that actually finalizes it.
		m.needMessage = true
		m.engine.registerToolBatch(dispatch.messageID, callIDs)
	}
	m.engine.recordResponseMessage(dispatch.messageID)
	// engine.snapshot.providerState is not updated incrementally here: every
	// dispatch's durableProjection recomputes it fresh from the store (the
	// PartProviderState parts persistAssistantTurn just wrote above,
	// included), so doing it here too would double-count the same captured
	// state once this message is picked up by the next dispatch's reload.
	return result, nil
}

// commitInternal is the internal-dispatch counterpart to commit: it never
// persists result as conversational content (no captureAssistantProviderState,
// no prepareToolCalls, no persistAssistantTurn, no claiming/finalizing the
// turn's placeholder, no recordResponseMessage) -- the physical call is
// already durably ledgered via begin/finish exactly like an ordinary
// dispatch, but the RESULT itself exists durably only as whatever the
// caller does with it (e.g. summarization's Finalize turning it into a
// compaction boundary), never as something the turn's conversation shows as
// "the assistant said this". It fails closed if the result carries
// anything but plain generated text or reasoning: an internal-dispatch role
// must never originate a tool call, media, or an approval request.
func (m *adkModel) commitInternal(result *einoschema.AgenticMessage) (*einoschema.AgenticMessage, error) {
	for _, block := range result.ContentBlocks {
		if block == nil {
			continue
		}
		switch block.Type {
		case einoschema.ContentBlockTypeAssistantGenText, einoschema.ContentBlockTypeReasoning:
		default:
			return nil, fmt.Errorf("%w: internal dispatch %q may not emit %s", errADKUnsupportedBlock, m.internalDispatch, block.Type)
		}
	}
	return result, nil
}
