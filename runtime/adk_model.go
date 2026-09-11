package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

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

	mu          sync.Mutex
	messageID   session.MessageID
	needMessage bool
}

var _ einomodel.AgenticModel = (*adkModel)(nil)

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
		Role: session.RoleAssistant, Agent: m.engine.snapshot.Config.Agent.Name, ModelID: string(m.engine.snapshot.Model.Model.ID),
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
	request := m.engine.snapshot.ProviderRequest(messageID, m.host.trace, input, m.execution.discoveredSnapshot())
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
		return nil, err
	}
	return &adkDispatch{messageID: messageID, record: record, input: input}, nil
}

func (m *adkModel) finish(ctx context.Context, dispatch *adkDispatch, err error) error {
	state := session.ModelRequestCompleted
	if err != nil {
		state = session.ModelRequestFailed
	}
	return updateModelRequest(ctx, m.execution.store, &dispatch.record, state, err, m.host.now())
}

// Generate implements einomodel.AgenticModel.
func (m *adkModel) Generate(ctx context.Context, input []*einoschema.AgenticMessage, opts ...einomodel.Option) (*einoschema.AgenticMessage, error) {
	if m.approval != nil {
		continuation, err := m.approval.prepare(ctx, m, input)
		if err != nil {
			return nil, err
		}
		input = continuation
	}
	dispatch, err := m.begin(ctx, input)
	if err != nil {
		return nil, err
	}
	result, dispatchErr := m.dispatch(ctx, dispatch, input, nil)
	if dispatchErr != nil {
		_ = m.finish(ctx, dispatch, dispatchErr)
		return nil, dispatchErr
	}
	committed, err := m.commit(ctx, dispatch, result.message)
	if err != nil {
		_ = m.finish(ctx, dispatch, err)
		return nil, err
	}
	if err := m.finish(ctx, dispatch, nil); err != nil {
		return nil, err
	}
	m.engine.addUsage(result.usage)
	if m.approval != nil {
		if err := m.approval.pause(ctx, m, dispatch, committed); err != nil {
			return nil, err
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
		continuation, err := m.approval.prepare(ctx, m, input)
		if err != nil {
			return nil, err
		}
		input = continuation
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
			ProviderID: string(m.engine.snapshot.Model.Provider.ID), ModelID: string(m.engine.snapshot.Model.Model.ID),
			Payload: mustJSON(map[string]string{"content": content, "reasoning": reasoning}), LiveOnly: true, CreatedAt: m.host.now(),
		})
	})
	m.host.sessionObserver.FinishAttempt(live)
	if dispatchErr != nil {
		_ = m.finish(ctx, dispatch, dispatchErr)
		return nil, dispatchErr
	}
	committed, err := m.commit(ctx, dispatch, result.message)
	if err != nil {
		_ = m.finish(ctx, dispatch, err)
		return nil, err
	}
	if err := m.finish(ctx, dispatch, nil); err != nil {
		return nil, err
	}
	m.engine.addUsage(result.usage)
	if m.approval != nil {
		if err := m.approval.pause(ctx, m, dispatch, committed); err != nil {
			return nil, err
		}
	}
	return einoschema.StreamReaderFromArray([]*einoschema.AgenticMessage{committed}), nil
}

// dispatch performs the one physical provider call this adapter ever makes
// (both Generate and Stream funnel through the streaming provider
// transport), fully draining the response before returning: no live chunk
// ever reaches ADK directly (see receiveModelStream's callers here).
func (m *adkModel) dispatch(ctx context.Context, d *adkDispatch, input []*einoschema.AgenticMessage, onDelta func(int64, *einoschema.AgenticMessage)) (modelStreamResult, error) {
	request := m.engine.snapshot.ProviderRequest(d.messageID, m.host.trace, input, m.execution.discoveredSnapshot())
	reader, err := m.engine.snapshot.Model.Streamer.StreamProvider(ctx, request)
	if err != nil {
		return modelStreamResult{}, err
	}
	var result modelStreamResult
	receiveModelStream(ctx, reader, m.host.streamLimits, &result, onDelta)
	if result.err != nil {
		return modelStreamResult{}, result.err
	}
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
		case einoschema.ContentBlockTypeAssistantGenText, einoschema.ContentBlockTypeReasoning, einoschema.ContentBlockTypeFunctionToolCall:
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
	preparedCalls, err := m.host.prepareToolCalls(ctx, m.execution, m.engine.snapshot, dispatch.messageID, calls)
	if err != nil {
		return nil, err
	}
	if _, err := m.host.persistAssistantTurn(ctx, m.execution, m.engine.snapshot, dispatch.messageID, result, blockIDs, capturedState.payloads, preparedCalls); err != nil {
		return nil, err
	}
	if capturedState.state != nil {
		capturedState.state.MessageIndex = len(dispatch.input)
		m.engine.appendProviderState(*capturedState.state)
	}
	return result, nil
}
