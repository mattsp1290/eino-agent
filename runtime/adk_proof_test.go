package runtime

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	einomodel "github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
	sqlite "github.com/mattsp1290/eino-agent/store/sqlite"
)

// This file is the W1 interception proof scaffolding for Eino v0.9.19's typed
// ADK. It composes the real upstream TypedChatModelAgent, TypedRunner,
// AgenticToolsNode, checkpoint protocol and TurnLoop with the existing durable
// seams in this package (model request ledger, tool claim/settlement, run
// fencing) through mandatory adapters. It is deliberately test-only: W5
// promotes the proven adapters into the runtime and deletes this scaffolding.

func init() {
	// Both interrupt info and state reach the checkpoint: the tools node
	// persists leaf interrupt info inside its composite rerun state, so the
	// info types must be gob-registered as well.
	schema.RegisterName[*adkHostDecisionInfo]("eino_agent_proof_host_decision_info")
	schema.RegisterName[*adkHostDecisionState]("eino_agent_proof_host_decision_state")
	schema.RegisterName[*adkApprovalInfo]("eino_agent_proof_approval_info")
	schema.RegisterName[*adkApprovalState]("eino_agent_proof_approval_state")
}

// adkTrace records the actual invocation order across adapters with unique
// counters so tests compare real sequences instead of iterator output alone.
type adkTrace struct {
	mu      sync.Mutex
	entries []string
}

func (t *adkTrace) add(format string, args ...any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.entries = append(t.entries, fmt.Sprintf("%03d %s", len(t.entries)+1, fmt.Sprintf(format, args...)))
}

func (t *adkTrace) list() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.entries...)
}

func (t *adkTrace) count(substr string) int {
	n := 0
	for _, entry := range t.list() {
		if strings.Contains(entry, substr) {
			n++
		}
	}
	return n
}

// adkScriptedModel is a scripted upstream model.AgenticModel. It records every
// physical request (input + resolved common options) so tests assert exact
// dispatch contents.
type adkScriptedModel struct {
	mu        sync.Mutex
	responses []*schema.AgenticMessage
	errs      []error
	requests  [][]*schema.AgenticMessage
	options   []*einomodel.Options
	trace     *adkTrace
	calls     atomic.Int32
	onCall    func(ctx context.Context, call int) error
}

func (m *adkScriptedModel) record(ctx context.Context, input []*schema.AgenticMessage, opts []einomodel.Option) (*schema.AgenticMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	call := int(m.calls.Add(1))
	cloned := make([]*schema.AgenticMessage, len(input))
	for i, msg := range input {
		cloned[i] = cloneAgenticMessage(msg)
	}
	m.requests = append(m.requests, cloned)
	m.options = append(m.options, einomodel.GetCommonOptions(&einomodel.Options{}, opts...))
	if m.trace != nil {
		m.trace.add("provider.call %d", call)
	}
	if m.onCall != nil {
		if err := m.onCall(ctx, call); err != nil {
			return nil, err
		}
	}
	if len(m.errs) >= call && m.errs[call-1] != nil {
		return nil, m.errs[call-1]
	}
	if len(m.responses) < call {
		return nil, fmt.Errorf("scripted model has no response for call %d", call)
	}
	return cloneAgenticMessage(m.responses[call-1]), nil
}

func (m *adkScriptedModel) Generate(ctx context.Context, input []*schema.AgenticMessage, opts ...einomodel.Option) (*schema.AgenticMessage, error) {
	return m.record(ctx, input, opts)
}

func (m *adkScriptedModel) Stream(ctx context.Context, input []*schema.AgenticMessage, opts ...einomodel.Option) (*schema.StreamReader[*schema.AgenticMessage], error) {
	msg, err := m.record(ctx, input, opts)
	if err != nil {
		return nil, err
	}
	// Split every block into two chunks carrying StreamingMeta.Index so the
	// adapter must concatenate through schema.ConcatAgenticMessages.
	var chunks []*schema.AgenticMessage
	for index, block := range msg.ContentBlocks {
		if block.Type == schema.ContentBlockTypeAssistantGenText && block.AssistantGenText != nil && len(block.AssistantGenText.Text) > 1 {
			text := block.AssistantGenText.Text
			half := len(text) / 2
			chunks = append(chunks,
				&schema.AgenticMessage{Role: msg.Role, ContentBlocks: []*schema.ContentBlock{schema.NewContentBlockChunk(&schema.AssistantGenText{Text: text[:half]}, &schema.StreamingMeta{Index: index})}},
				&schema.AgenticMessage{Role: msg.Role, ContentBlocks: []*schema.ContentBlock{schema.NewContentBlockChunk(&schema.AssistantGenText{Text: text[half:]}, &schema.StreamingMeta{Index: index})}},
			)
			continue
		}
		cloned := cloneAgenticMessage(&schema.AgenticMessage{Role: msg.Role, ContentBlocks: []*schema.ContentBlock{block}})
		cloned.ContentBlocks[0].StreamingMeta = &schema.StreamingMeta{Index: index}
		chunks = append(chunks, cloned)
	}
	if msg.ResponseMeta != nil {
		chunks = append(chunks, &schema.AgenticMessage{Role: msg.Role, ResponseMeta: msg.ResponseMeta})
	}
	if len(chunks) == 0 {
		chunks = append(chunks, &schema.AgenticMessage{Role: msg.Role})
	}
	return schema.StreamReaderFromArray(chunks), nil
}

func cloneAgenticMessage(msg *schema.AgenticMessage) *schema.AgenticMessage {
	if msg == nil {
		return nil
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	var cloned schema.AgenticMessage
	if err := json.Unmarshal(raw, &cloned); err != nil {
		panic(err)
	}
	return &cloned
}

func agenticAssistant(blocks ...*schema.ContentBlock) *schema.AgenticMessage {
	return &schema.AgenticMessage{Role: schema.AgenticRoleTypeAssistant, ContentBlocks: blocks}
}

func agenticText(text string) *schema.ContentBlock {
	return schema.NewContentBlock(&schema.AssistantGenText{Text: text})
}

func agenticCall(id, name, args string) *schema.ContentBlock {
	return schema.NewContentBlock(&schema.FunctionToolCall{CallID: id, Name: name, Arguments: args})
}

// adkMemoryCheckpoints is the W1 stand-in for the W5 SQL checkpoint table. It
// records every Set/Get/Delete in the shared trace so tests can assert the
// order of checkpoint publication relative to durable commits and iterator
// draining. Bytes survive a SQLite reopen because they live in the test.
type adkMemoryCheckpoints struct {
	mu     sync.Mutex
	data   map[string][]byte
	trace  *adkTrace
	setErr error
}

func newADKMemoryCheckpoints(trace *adkTrace) *adkMemoryCheckpoints {
	return &adkMemoryCheckpoints{data: map[string][]byte{}, trace: trace}
}

func (s *adkMemoryCheckpoints) Get(_ context.Context, id string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trace != nil {
		s.trace.add("checkpoint.get %s", id)
	}
	raw, ok := s.data[id]
	return append([]byte(nil), raw...), ok, nil
}

func (s *adkMemoryCheckpoints) Set(_ context.Context, id string, raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trace != nil {
		s.trace.add("checkpoint.set %s bytes=%d", id, len(raw))
	}
	if s.setErr != nil {
		return s.setErr
	}
	s.data[id] = append([]byte(nil), raw...)
	return nil
}

func (s *adkMemoryCheckpoints) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trace != nil {
		s.trace.add("checkpoint.delete %s", id)
	}
	delete(s.data, id)
	return nil
}

func (s *adkMemoryCheckpoints) has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[id]
	return ok
}

// adkProof owns one fenced run execution over SQLite plus the adapters that
// make every ADK model call and tool execution pass the durable seams.
type adkProof struct {
	t                  testing.TB
	ctx                context.Context
	dbPath             string
	store              *sqlite.Store
	pool               *sql.DB
	host               *StreamingOrchestrator
	plan               *RunPlan
	run                session.Run
	execution          *runExecution
	snapshot           TurnSnapshot
	userMessageID      session.MessageID
	assistantMessageID session.MessageID
	trace              *adkTrace
	tools              []Tool
	executors          map[string]*atomic.Int32
	closed             bool
	stepMu             sync.Mutex
	step               int
	placeholderUsed    bool
}

// claimPlaceholder hands the admitted assistant placeholder to exactly one
// adapter; every other dispatch (child agents, later steps) owns a new
// assistant message.
func (p *adkProof) claimPlaceholder() bool {
	p.stepMu.Lock()
	defer p.stepMu.Unlock()
	if p.assistantMessageID == "" || p.placeholderUsed {
		return false
	}
	p.placeholderUsed = true
	return true
}

// nextStep allocates the next ledger step for this run. The ledger's durable
// uniqueness domain is (run, attempt, step), so every adapter sharing the run
// fence (parent, child agents, failover models) draws from one sequence. W5
// replaces this with an explicit per-dispatch invocation identity.
func (p *adkProof) nextStep() int {
	p.stepMu.Lock()
	defer p.stepMu.Unlock()
	p.step++
	return p.step
}

func (p *adkProof) seedStepsFromLedger() {
	p.stepMu.Lock()
	defer p.stepMu.Unlock()
	for _, record := range p.modelRequests() {
		if record.Step > p.step {
			p.step = record.Step
		}
	}
}

type adkProofOptions struct {
	sessionID   session.ID
	prompt      string
	tools       []Tool
	trace       *adkTrace
	clock       func() time.Time
	ids         IDGenerator
	lease       time.Duration
	permissions permissions.Policy
}

func newADKProof(t testing.TB, dbPath string, options adkProofOptions) *adkProof {
	t.Helper()
	ctx := context.Background()
	store, pool, err := openTestSQLite(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	trace := options.trace
	if trace == nil {
		trace = &adkTrace{}
	}
	proof := &adkProof{t: t, ctx: ctx, dbPath: dbPath, store: store, pool: pool, trace: trace, tools: options.tools, executors: map[string]*atomic.Int32{}}
	proof.host = proof.newHost(store, options)
	sessionID := options.sessionID
	if sessionID == "" {
		sessionID = "adk-proof-session"
	}
	prompt := options.prompt
	if prompt == "" {
		prompt = "prove the boundary"
	}
	request := Request{SessionID: sessionID, Message: TextUserMessage(prompt), Config: orchestratorConfig()}
	request.Message.Blocks = assignContentBlockIDs(request.Message.Blocks, proof.host.ids)
	plan, err := proof.host.acquireRunPlan(ctx, RunPlanRequest{SessionID: request.SessionID, Config: request.Config})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := proof.host.model.Resolve(ctx, request.Config.Model, model.Runtime{})
	if err != nil {
		t.Fatal(err)
	}
	ids := admissionIDs{
		SessionID: request.SessionID, RunID: proof.host.ids.NewRunID(), UserMessageID: proof.host.ids.NewMessageID(),
		UserPartIDs: partIDsFromBlocks(request.Message.Blocks), AssistantMessageID: proof.host.ids.NewMessageID(),
		ContextEpochID: proof.host.ids.NewEpochID(), EventID: proof.host.ids.NewEventID(), RunClaimToken: string(proof.host.ids.NewEventID()),
	}
	admitted, err := proof.host.admitter().admit(ctx, admissionRequest{
		IDs: ids, UserMessage: request.Message, History: proof.host.history, Config: request.Config, Model: resolved,
		OwnerID: proof.host.ownerID(), LeaseDuration: proof.host.lease(), ExtensionPlan: plan.Descriptor(), ContentLimits: proof.host.contentLimits,
	})
	if err != nil {
		t.Fatal(err)
	}
	proof.plan = plan
	proof.execution = newRunExecution(proof.host, plan, admitted.Run)
	proof.execution.seedDurableMessageFloor(admitted.AssistantMessage.CreatedAt)
	started, err := proof.execution.store.StartRun(ctx, proof.host.now())
	if err != nil {
		t.Fatal(err)
	}
	proof.run = started
	proof.snapshot = admitted.Snapshot
	proof.snapshot.Tools = cloneSlice(options.tools)
	proof.userMessageID = admitted.UserMessage.ID
	proof.assistantMessageID = admitted.AssistantMessage.ID
	return proof
}

// reopenADKProof simulates a process restart: the previous pool is closed, the
// file is reopened without migration and a new fence claims the run.
func reopenADKProof(t testing.TB, dbPath string, runID session.RunID, options adkProofOptions) *adkProof {
	t.Helper()
	ctx := context.Background()
	store, pool, err := reopenTestSQLite(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	trace := options.trace
	if trace == nil {
		trace = &adkTrace{}
	}
	proof := &adkProof{t: t, ctx: ctx, dbPath: dbPath, store: store, pool: pool, trace: trace, tools: options.tools, executors: map[string]*atomic.Int32{}}
	proof.host = proof.newHost(store, options)
	run, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := proof.host.acquireResumePlan(ctx, run.SessionID, run.ExtensionPlan.Clone())
	if err != nil {
		t.Fatal(err)
	}
	// A crashed process leaves its lease behind; reclaiming a running run
	// still requires expiration, so the previous proof used a short lease.
	var claimed session.Run
	deadline := time.Now().Add(10 * time.Second)
	for {
		claimed, err = store.ClaimRun(ctx, session.RunClaim{RunID: run.ID, OwnerID: proof.host.ownerID(), ClaimToken: string(proof.host.ids.NewEventID()), LeaseDuration: proof.host.lease()})
		if err == nil {
			break
		}
		if (!errors.Is(err, session.ErrSessionBusy) && !errors.Is(err, session.ErrConflict)) || time.Now().After(deadline) {
			t.Fatalf("claim run after restart: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	proof.plan = plan
	proof.execution = newRunExecution(proof.host, plan, claimed)
	started, err := proof.execution.store.StartRun(ctx, proof.host.now())
	if err != nil {
		t.Fatal(err)
	}
	proof.run = started
	proof.snapshot = proof.host.resumeSnapshot(started)
	proof.snapshot.Tools = cloneSlice(options.tools)
	proof.userMessageID = started.ParentMsgID
	proof.seedStepsFromLedger()
	return proof
}

func (p *adkProof) newHost(store *sqlite.Store, options adkProofOptions) *StreamingOrchestrator {
	clock := options.clock
	if clock == nil {
		clock = time.Now
	}
	var ids IDGenerator = &sequenceIDs{}
	if options.ids != nil {
		ids = options.ids
	}
	lease := options.lease
	if lease <= 0 {
		lease = time.Minute
	}
	hostOptions := []Option{
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*schema.Message, error) {
			return nil, errors.New("classic streamer must not be used by the ADK proof")
		})}),
		WithIDGenerator(ids),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{tools: options.tools})}),
		WithClock(clock),
		WithOwnerID("adk-proof"),
		WithLease(lease),
	}
	if options.permissions != nil {
		hostOptions = append(hostOptions, WithPermissions(options.permissions))
	}
	host, err := NewStreamingOrchestrator(hostOptions...)
	if err != nil {
		p.t.Fatal(err)
	}
	return host
}

func (p *adkProof) close() {
	if p.closed {
		return
	}
	p.closed = true
	p.execution.release()
	_ = p.pool.Close()
}

// ledgerModel returns the mandatory model adapter: every physical model call
// creates one ledger row before dispatch, and the returned message is only
// handed to ADK after its public parts and tool-call records are committed.
func (p *adkProof) ledgerModel(inner einomodel.AgenticModel) *adkLedgerModel {
	return &adkLedgerModel{proof: p, inner: inner}
}

func (p *adkProof) durableTools() []tool.BaseTool {
	tools := make([]tool.BaseTool, 0, len(p.tools))
	for _, candidate := range p.tools {
		tools = append(tools, &adkDurableTool{proof: p, tool: candidate})
	}
	return tools
}

func (p *adkProof) toolCall(id session.ToolCallID) session.ToolCall {
	record, err := p.store.GetToolCall(p.ctx, id)
	if err != nil {
		p.t.Fatalf("tool call %s: %v", id, err)
	}
	return record
}

func (p *adkProof) toolCalls() []session.ToolCall {
	batch, err := p.store.ListMessages(p.ctx, p.run.SessionID, session.ReplayCursor{Limit: 1000})
	if err != nil {
		p.t.Fatal(err)
	}
	var calls []session.ToolCall
	for _, part := range batch.Parts {
		if part.Kind != session.PartToolCall {
			continue
		}
		var payload toolCallPayload
		if err := json.Unmarshal(part.Payload, &payload); err != nil {
			p.t.Fatal(err)
		}
		calls = append(calls, p.toolCall(session.ToolCallID(payload.ID)))
	}
	return calls
}

func (p *adkProof) modelRequests() []session.ModelRequestRecord {
	batch, err := p.store.ListModelRequests(p.ctx, p.run.ID, session.ModelRequestCursor{Limit: 1000})
	if err != nil {
		p.t.Fatal(err)
	}
	return batch.Records
}

func (p *adkProof) newAgent(t testing.TB, ledger *adkLedgerModel, handlers ...adk.TypedChatModelAgentMiddleware[*schema.AgenticMessage]) *adk.TypedChatModelAgent[*schema.AgenticMessage] {
	t.Helper()
	agent, err := adk.NewTypedChatModelAgent[*schema.AgenticMessage](p.ctx, &adk.TypedChatModelAgentConfig[*schema.AgenticMessage]{
		Name:        "proof",
		Description: "W1 proof agent",
		Model:       ledger,
		ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{Tools: p.durableTools()}},
		Handlers:    handlers,
	})
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

func (p *adkProof) userInput() []*schema.AgenticMessage {
	var text string
	for _, msg := range p.snapshot.Messages {
		if msg != nil && msg.Role == schema.User {
			text = msg.Content
		}
	}
	return []*schema.AgenticMessage{schema.UserAgenticMessage(text)}
}

var errADKUnsupportedBlock = errors.New("adk proof cannot durably record content block")

// adkLedgerModel is the mandatory runtime model adapter for the proof.
type adkLedgerModel struct {
	proof       *adkProof
	inner       einomodel.AgenticModel
	mu          sync.Mutex
	step        int
	messageID   session.MessageID
	needMessage bool
	calls       atomic.Int32
	approval    *adkApprovalBinding
	commitHook  func(ctx context.Context, messageID session.MessageID, result *schema.AgenticMessage) error
}

func (m *adkLedgerModel) currentMessageID(ctx context.Context) (session.MessageID, error) {
	p := m.proof
	if m.messageID == "" && !m.needMessage && p.claimPlaceholder() {
		m.messageID = p.assistantMessageID
		return m.messageID, nil
	}
	if m.messageID != "" && !m.needMessage {
		return m.messageID, nil
	}
	nextID := p.host.ids.NewMessageID()
	at, err := p.execution.nextDurableMessageTime(ctx, p.run.SessionID, p.host.now())
	if err != nil {
		return "", err
	}
	if _, err := p.execution.store.AppendMessage(ctx, session.Message{
		ID: nextID, SessionID: p.run.SessionID, RunID: p.run.ID, ParentID: p.userMessageID, Role: session.RoleAssistant,
		Agent: p.snapshot.Config.Agent.Name, ModelID: string(p.snapshot.Model.Model.ID), CreatedAt: at, UpdatedAt: at,
	}); err != nil {
		return "", err
	}
	m.messageID = nextID
	m.needMessage = false
	return nextID, nil
}

type adkDispatch struct {
	messageID session.MessageID
	record    session.ModelRequestRecord
	step      int
	input     []*schema.AgenticMessage
}

func (m *adkLedgerModel) begin(ctx context.Context, kind string, input []*schema.AgenticMessage, opts []einomodel.Option) (*adkDispatch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.proof
	m.step = p.nextStep()
	call := int(m.calls.Add(1))
	p.trace.add("adapter.%s.begin call=%d step=%d", kind, call, m.step)
	messageID, err := m.currentMessageID(ctx)
	if err != nil {
		p.trace.add("adapter.message_allocation_failed %v", err)
		return nil, err
	}
	audited, hash, err := auditAgenticInput(input, opts)
	if err != nil {
		return nil, err
	}
	request := model.Request{Identity: model.Identity{
		SessionID: string(p.run.SessionID), RunID: string(p.run.ID), AssistantMessageID: string(messageID),
		ProviderID: p.snapshot.Model.Provider.ID, ModelID: p.snapshot.Model.Model.ID,
	}}
	record, err := p.host.prepareModelRequest(ctx, p.execution, p.snapshot, request, audited, hash, messageID, 1, m.step)
	if err != nil {
		p.trace.add("ledger.prepare_failed %v", err)
		return nil, err
	}
	if err := updateModelRequest(ctx, p.execution.store, &record, session.ModelRequestDispatchStarted, nil, p.host.now()); err != nil {
		return nil, err
	}
	p.trace.add("ledger.dispatch_started %s", record.ID)
	return &adkDispatch{messageID: messageID, record: record, step: m.step, input: input}, nil
}

func (m *adkLedgerModel) finish(ctx context.Context, dispatch *adkDispatch, err error) error {
	p := m.proof
	state := session.ModelRequestCompleted
	if err != nil {
		state = session.ModelRequestFailed
	}
	if updateErr := updateModelRequest(ctx, p.execution.store, &dispatch.record, state, err, p.host.now()); updateErr != nil {
		return updateErr
	}
	p.trace.add("ledger.%s %s", state, dispatch.record.ID)
	return nil
}

func (m *adkLedgerModel) Generate(ctx context.Context, input []*schema.AgenticMessage, opts ...einomodel.Option) (*schema.AgenticMessage, error) {
	if m.approval != nil {
		continuation, err := m.approval.prepare(ctx, m, input)
		if err != nil {
			return nil, err
		}
		input = continuation
	}
	dispatch, err := m.begin(ctx, "generate", input, opts)
	if err != nil {
		return nil, err
	}
	result, err := m.inner.Generate(ctx, input, opts...)
	if err != nil {
		_ = m.finish(ctx, dispatch, err)
		return nil, err
	}
	committed, err := m.commit(ctx, dispatch, result)
	if err != nil {
		_ = m.finish(ctx, dispatch, err)
		return nil, err
	}
	if err := m.finish(ctx, dispatch, nil); err != nil {
		return nil, err
	}
	if m.approval != nil {
		if err := m.approval.pause(ctx, m, dispatch, committed); err != nil {
			return nil, err
		}
	}
	return committed, nil
}

func (m *adkLedgerModel) Stream(ctx context.Context, input []*schema.AgenticMessage, opts ...einomodel.Option) (*schema.StreamReader[*schema.AgenticMessage], error) {
	if m.approval != nil {
		continuation, err := m.approval.prepare(ctx, m, input)
		if err != nil {
			return nil, err
		}
		input = continuation
	}
	dispatch, err := m.begin(ctx, "stream", input, opts)
	if err != nil {
		return nil, err
	}
	reader, err := m.inner.Stream(ctx, input, opts...)
	if err != nil {
		_ = m.finish(ctx, dispatch, err)
		return nil, err
	}
	var chunks []*schema.AgenticMessage
	for {
		chunk, recvErr := reader.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			reader.Close()
			_ = m.finish(ctx, dispatch, recvErr)
			return nil, recvErr
		}
		if chunk == nil {
			reader.Close()
			err := model.Error{Code: "malformed_provider_stream", Message: "provider returned nil agentic chunk"}
			_ = m.finish(ctx, dispatch, err)
			return nil, err
		}
		m.proof.trace.add("adapter.stream.chunk step=%d index=%d", dispatch.step, len(chunks))
		chunks = append(chunks, chunk)
	}
	reader.Close()
	final, err := schema.ConcatAgenticMessages(chunks)
	if err != nil {
		_ = m.finish(ctx, dispatch, err)
		return nil, err
	}
	committed, err := m.commit(ctx, dispatch, final)
	if err != nil {
		_ = m.finish(ctx, dispatch, err)
		return nil, err
	}
	if err := m.finish(ctx, dispatch, nil); err != nil {
		return nil, err
	}
	if m.approval != nil {
		if err := m.approval.pause(ctx, m, dispatch, committed); err != nil {
			return nil, err
		}
	}
	// The adapter drains the provider stream before committing, so ADK only
	// observes one committed chunk; live deltas are a W3/W7 transport concern.
	return schema.StreamReaderFromArray([]*schema.AgenticMessage{committed}), nil
}

// commit persists the assistant result through the existing seams: text and
// reasoning parts, pending tool-call records with canonical identity and the
// finalized assistant message, all inside one fenced transaction.
func (m *adkLedgerModel) commit(ctx context.Context, dispatch *adkDispatch, result *schema.AgenticMessage) (*schema.AgenticMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.proof
	if result == nil {
		return nil, errors.New("nil agentic result")
	}
	var text, reasoning []string
	var calls []schema.ToolCall
	for _, block := range result.ContentBlocks {
		if block == nil {
			continue
		}
		switch block.Type {
		case schema.ContentBlockTypeAssistantGenText:
			if block.AssistantGenText != nil {
				text = append(text, block.AssistantGenText.Text)
			}
		case schema.ContentBlockTypeReasoning:
			if block.Reasoning != nil {
				reasoning = append(reasoning, block.Reasoning.Text)
			}
		case schema.ContentBlockTypeFunctionToolCall:
			if block.FunctionToolCall != nil {
				calls = append(calls, schema.ToolCall{ID: block.FunctionToolCall.CallID, Type: "function", Function: schema.FunctionCall{Name: block.FunctionToolCall.Name, Arguments: block.FunctionToolCall.Arguments}})
			}
		case schema.ContentBlockTypeMCPToolApprovalRequest:
			// Persisted by the approval binding as a durable approval record.
			if m.approval == nil {
				return nil, fmt.Errorf("%w: %s", errADKUnsupportedBlock, block.Type)
			}
		default:
			// W1 scaffolding persists text, reasoning and function calls only.
			// Anything else must fail closed rather than reach ADK as a fact
			// without a durable row; W2 supplies the full content contract.
			return nil, fmt.Errorf("%w: %s", errADKUnsupportedBlock, block.Type)
		}
	}
	classic := &schema.Message{Role: schema.Assistant, Content: strings.Join(text, ""), ReasoningContent: strings.Join(reasoning, ""), ToolCalls: calls}
	normalizeToolCallIDs(classic, p.host.ids)
	prepared, err := p.host.prepareToolCalls(ctx, p.execution, p.snapshot, dispatch.messageID, classic.ToolCalls)
	if err != nil {
		return nil, err
	}
	if m.commitHook != nil {
		if err := m.commitHook(ctx, dispatch.messageID, result); err != nil {
			return nil, err
		}
	}
	prepared, err = p.host.persistAssistantTurn(ctx, p.execution, p.snapshot, dispatch.messageID, classic, nil, prepared)
	if err != nil {
		return nil, err
	}
	committed := cloneAgenticMessage(result)
	index := 0
	for _, block := range committed.ContentBlocks {
		if block == nil || block.Type != schema.ContentBlockTypeFunctionToolCall || block.FunctionToolCall == nil {
			continue
		}
		if index >= len(prepared) {
			return nil, errors.New("prepared tool calls do not cover every function call block")
		}
		block.FunctionToolCall.CallID = string(prepared[index].call.ID)
		block.FunctionToolCall.Arguments = string(prepared[index].call.Input)
		index++
	}
	// Every committed result finalizes its assistant message; the next
	// dispatch, whether a tool continuation or a later turn, owns a new one.
	m.needMessage = true
	p.trace.add("adapter.commit message=%s calls=%d", dispatch.messageID, len(prepared))
	return committed, nil
}

func auditAgenticInput(input []*schema.AgenticMessage, opts []einomodel.Option) (AuditedModelInput, string, error) {
	audited := AuditedModelInput{SafeCallConfig: map[string]string{}}
	for index, msg := range input {
		if msg == nil {
			return AuditedModelInput{}, "", fmt.Errorf("nil agentic message %d", index)
		}
		raw, err := json.Marshal(msg)
		if err != nil {
			return AuditedModelInput{}, "", err
		}
		audited.Messages = append(audited.Messages, AuditedMessage{Canonical: raw})
	}
	common := einomodel.GetCommonOptions(&einomodel.Options{}, opts...)
	if common.Temperature != nil {
		audited.SafeCallConfig["temperature"] = fmt.Sprint(*common.Temperature)
	}
	if common.TopP != nil {
		audited.SafeCallConfig["top_p"] = fmt.Sprint(*common.TopP)
	}
	if common.MaxTokens != nil {
		audited.SafeCallConfig["max_tokens"] = fmt.Sprint(*common.MaxTokens)
	}
	if len(common.Stop) != 0 {
		audited.SafeCallConfig["stop"] = strings.Join(common.Stop, "\x00")
	}
	if common.ToolChoice != nil {
		audited.SafeCallConfig["tool_choice"] = string(*common.ToolChoice)
	}
	for _, info := range common.Tools {
		if info == nil {
			continue
		}
		schemaRaw := json.RawMessage("null")
		if info.ParamsOneOf != nil {
			converted, err := info.ToJSONSchema()
			if err != nil {
				return AuditedModelInput{}, "", err
			}
			schemaRaw, err = json.Marshal(converted)
			if err != nil {
				return AuditedModelInput{}, "", err
			}
		}
		audited.Tools = append(audited.Tools, AuditedToolSchema{Name: info.Name, Description: info.Desc, Schema: schemaRaw})
	}
	canonical, err := json.Marshal(audited)
	if err != nil {
		return AuditedModelInput{}, "", err
	}
	digest := sha256.Sum256(canonical)
	return audited, hex.EncodeToString(digest[:]), nil
}

// adkDurableTool is the mandatory tool adapter. ADK schedules the call, but
// the leaf executor only runs after the persisted call is claimed under the
// current run fence, and the model only ever sees the settled output.
type adkDurableTool struct {
	proof *adkProof
	tool  Tool
}

type adkHostDecisionInfo struct{ ToolCallID string }
type adkHostDecisionState struct{ ToolCallID string }

func (t *adkDurableTool) Info(context.Context) (*schema.ToolInfo, error) {
	return t.tool.Info, nil
}

func (t *adkDurableTool) InvokableRun(ctx context.Context, arguments string, _ ...tool.Option) (string, error) {
	p := t.proof
	callID := session.ToolCallID(compose.GetToolCallID(ctx))
	if callID == "" {
		return "", errors.New("tool call id missing from ADK context")
	}
	record, err := p.store.GetToolCall(ctx, callID)
	if err != nil {
		return "", fmt.Errorf("persisted tool call %s: %w", callID, err)
	}
	if record.RunID != p.run.ID || record.SessionID != p.run.SessionID {
		return "", fmt.Errorf("persisted tool call %s belongs to run %s, not %s", callID, record.RunID, p.run.ID)
	}
	switch {
	case session.TerminalToolCall(record.Status):
		p.trace.add("tool.replay %s status=%s", callID, record.Status)
		return string(record.Output), nil
	case record.Status == session.ToolCallRunning:
		settled, err := p.execution.settleInterruptedRunningTool(ctx, p.run, t.tool, record)
		if err != nil {
			return "", err
		}
		p.trace.add("tool.interrupted_without_rerun %s", callID)
		return string(settled.Output), nil
	case record.Status != session.ToolCallPending:
		return "", fmt.Errorf("unexpected tool call status %q", record.Status)
	}
	decision := ""
	if t.tool.Metadata["proof_interrupt"] == "host_decision" {
		isResume, hasData, data := compose.GetResumeContext[string](ctx)
		if !isResume {
			p.trace.add("tool.interrupt %s", callID)
			return "", compose.StatefulInterrupt(ctx, &adkHostDecisionInfo{ToolCallID: string(callID)}, &adkHostDecisionState{ToolCallID: string(callID)})
		}
		// A targeted leaf with a payload of the wrong type is a host bug, not
		// a reason to pause forever without a diagnostic.
		if !hasData {
			return "", fmt.Errorf("host decision for %s must be a string", callID)
		}
		if data != "approve" && data != "deny" {
			return "", fmt.Errorf("host decision for %s must be approve or deny, got %q", callID, data)
		}
		decision = data
		p.trace.add("tool.resume %s decision=%s", callID, decision)
	}
	if canonical, err := canonicalToolObject(record.Input); err != nil || string(canonical) != string(record.Input) || string(canonical) != strings.TrimSpace(arguments) {
		return "", fmt.Errorf("ADK arguments %q diverge from persisted canonical input %q", arguments, record.Input)
	}
	startedAt := p.host.now()
	claimed, err := p.execution.persistToolClaim(ctx, session.ClaimToolCallRequest{
		ID: record.ID, ClaimedBy: p.host.ownerID(), ClaimToken: string(p.host.ids.NewEventID()), StartedAt: startedAt,
		LeaseDuration: p.host.lease(), Event: toolTransitionEnvelope(p.host, p.snapshot, startedAt),
	})
	if err != nil {
		return "", err
	}
	p.trace.add("tool.claimed %s", callID)
	call := ToolCall{
		ID: claimed.Call.ID, SessionID: claimed.Call.SessionID, RunID: claimed.Call.RunID, MessageID: claimed.Call.MessageID,
		ResultMessageID: claimed.Call.ResultMessageID, ResultPartID: claimed.Call.ResultPartID, Name: claimed.Call.Name,
		Scope: t.tool.Scope, Pattern: claimed.Call.Pattern, Input: cloneJSON(claimed.Call.Input), Context: toolContext(p.snapshot, p.snapshot.Tools),
	}
	if decision == "deny" {
		completedAt := p.host.now()
		messageAt, err := p.execution.nextDurableMessageTime(ctx, p.run.SessionID, completedAt)
		if err != nil {
			return "", err
		}
		settlement, _, err := buildToolSettlement(ToolSettlementInput{
			Tool: t.tool, Call: call, Claimed: claimed.Call, Disposition: ToolDenied,
			Result: modelVisiblePermissionResult("denied", "host denied the tool call"), ModelID: string(p.snapshot.Model.Model.ID), CompletedAt: completedAt,
		}, messageAt)
		if err != nil {
			return "", err
		}
		if _, err := p.execution.persistToolSettlement(ctx, claimed.Call, settlement, toolTransitionEnvelope(p.host, p.snapshot, completedAt)); err != nil {
			return "", err
		}
		p.trace.add("tool.settled %s status=%s", callID, settlement.Status)
		return string(settlement.Output), nil
	}
	settled, err := p.execution.executeAndSettleClaimedTool(ctx, p.snapshot, t.tool, call, claimed.Call, nil)
	if err != nil {
		return "", err
	}
	p.trace.add("tool.settled %s status=%s", callID, settled.Settlement.Status)
	return string(settled.Settlement.Output), nil
}

// proofTool builds a runtime Tool whose executor increments a per-tool counter.
func (p *adkProof) countingExecutor(name string, output func(call ToolCall) (ToolResult, error)) ToolExecutor {
	counter := &atomic.Int32{}
	p.executors[name] = counter
	return orchestratorToolExecutorFunc(func(ctx context.Context, call ToolCall) (ToolResult, error) {
		counter.Add(1)
		p.trace.add("executor.%s call=%s", name, call.ID)
		if output == nil {
			return ToolResult{Output: "ok:" + name}, nil
		}
		return output(call)
	})
}

func (p *adkProof) executions(name string) int {
	counter, ok := p.executors[name]
	if !ok {
		return 0
	}
	return int(counter.Load())
}

func proofTool(name string, executor ToolExecutor, metadata map[string]string) Tool {
	return Tool{
		Name:      name,
		Info:      &schema.ToolInfo{Name: name, Desc: "proof tool " + name, ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{"value": {Type: schema.String}})},
		Executor:  executor,
		RetrySafe: metadata["retry_safe"] == "true",
		Metadata:  metadata,
		Retention: RetentionPolicy{MaxInlineBytes: 1 << 16},
	}
}

// adkEvents drains a typed iterator, concatenating streaming outputs, and
// records observation order in the trace.
type adkEvents struct {
	outputs    []*schema.AgenticMessage
	roles      []schema.AgenticRoleType
	interrupts []*adk.InterruptCtx
	errs       []error
	actions    []*adk.AgentAction
}

func drainADKEvents(t testing.TB, trace *adkTrace, iter *adk.AsyncIterator[*adk.TypedAgentEvent[*schema.AgenticMessage]]) *adkEvents {
	t.Helper()
	drained := &adkEvents{}
	for {
		event, ok := iter.Next()
		if !ok {
			break
		}
		if event.Err != nil {
			trace.add("event.error %v", event.Err)
			drained.errs = append(drained.errs, event.Err)
			continue
		}
		if event.Output != nil && event.Output.MessageOutput != nil {
			msg, err := event.Output.MessageOutput.GetMessage()
			if err != nil {
				trace.add("event.output.error %v", err)
				drained.errs = append(drained.errs, err)
				continue
			}
			trace.add("event.output role=%s streaming=%v", event.Output.MessageOutput.AgenticRole, event.Output.MessageOutput.IsStreaming)
			drained.outputs = append(drained.outputs, msg)
			drained.roles = append(drained.roles, event.Output.MessageOutput.AgenticRole)
		}
		if event.Action != nil {
			drained.actions = append(drained.actions, event.Action)
			if event.Action.Interrupted != nil {
				trace.add("event.interrupted contexts=%d", len(event.Action.Interrupted.InterruptContexts))
				drained.interrupts = append(drained.interrupts, event.Action.Interrupted.InterruptContexts...)
			}
		}
	}
	trace.add("iterator.drained")
	return drained
}

func assistantText(msg *schema.AgenticMessage) string {
	if msg == nil {
		return ""
	}
	var parts []string
	for _, block := range msg.ContentBlocks {
		if block != nil && block.Type == schema.ContentBlockTypeAssistantGenText && block.AssistantGenText != nil {
			parts = append(parts, block.AssistantGenText.Text)
		}
	}
	return strings.Join(parts, "")
}

func functionResultText(msg *schema.AgenticMessage) (string, string) {
	if msg == nil {
		return "", ""
	}
	for _, block := range msg.ContentBlocks {
		if block != nil && block.Type == schema.ContentBlockTypeFunctionToolResult && block.FunctionToolResult != nil {
			var texts []string
			for _, content := range block.FunctionToolResult.Content {
				if content != nil && content.Text != nil {
					texts = append(texts, content.Text.Text)
				}
			}
			return block.FunctionToolResult.CallID, strings.Join(texts, "")
		}
	}
	return "", ""
}
