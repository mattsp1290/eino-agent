package runtime

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

// Shared in-memory Store and fenced ExecutionStore used by runtime tests.
// ADK's tools node dispatches every tool call declared in one assistant
// message concurrently (compose.parallelRunToolCall), and this fixture is
// reached from those goroutines directly (through fakeExecutionStore, which
// embeds *admissionStore), so every exported method must be safe for
// concurrent use -- see mu's doc comment for the locking convention used to
// avoid deadlocking on the handful of methods that call each other.
type admissionStore struct {
	// mu guards every field below. Exported methods lock it at entry; a
	// method that needs another method's behavior internally (e.g.
	// CreateToolCall persisting its RequestPart via AppendPart) calls that
	// method's unexported *Locked sibling instead of the exported,
	// self-locking one, to avoid a non-reentrant self-deadlock. clone() is
	// the one exception: it never locks itself, since its only caller
	// (WithinTx) always holds mu already.
	mu        sync.Mutex
	sessions  map[session.ID]session.Session
	runs      map[session.RunID]session.Run
	messages  map[session.MessageID]session.Message
	finalized map[session.MessageID]bool
	parts     map[session.PartID]session.Part
	events    map[session.EventID]session.EventRecord
	// eventSeq/eventOrder track each event's insertion order: events sharing
	// an identical CreatedAt (a fixed test clock is common) would otherwise
	// come back from ListEvents in Go's randomized map iteration order.
	// Every write goes through putEvent, which is the only place these are
	// mutated.
	eventSeq          int64
	eventOrder        map[session.EventID]int64
	toolCalls         map[session.ToolCallID]session.ToolCall
	epochs            map[session.EpochID]session.ContextEpoch
	modelRequests     map[session.ModelRequestID]session.ModelRequestRecord
	turns             map[session.TurnID]session.Turn
	inbox             map[session.InboxID]session.InboxItem
	checkpoints       map[fakeCheckpointKey]session.Checkpoint
	appendEventErr    error
	appendPartErrAt   int
	appendPartCalls   int
	settleToolCallErr error
	toolTransitionErr error
	createToolErrAt   int
	createToolCalls   int
	normalizeEvent    func(session.EventRecord) session.EventRecord
	listMessagesHook  func(*admissionStore, session.ID)
	listMessagesCalls atomic.Int32
	getRunCalls       atomic.Int32
}

func newAdmissionStore() *admissionStore {
	return &admissionStore{
		sessions:      map[session.ID]session.Session{},
		runs:          map[session.RunID]session.Run{},
		messages:      map[session.MessageID]session.Message{},
		finalized:     map[session.MessageID]bool{},
		parts:         map[session.PartID]session.Part{},
		events:        map[session.EventID]session.EventRecord{},
		eventOrder:    map[session.EventID]int64{},
		toolCalls:     map[session.ToolCallID]session.ToolCall{},
		epochs:        map[session.EpochID]session.ContextEpoch{},
		modelRequests: map[session.ModelRequestID]session.ModelRequestRecord{},
		turns:         map[session.TurnID]session.Turn{},
		inbox:         map[session.InboxID]session.InboxItem{},
		checkpoints:   map[fakeCheckpointKey]session.Checkpoint{},
	}
}

// fakeCheckpointKey is the in-memory identity for a staged checkpoint
// revision in admissionStore.checkpoints.
type fakeCheckpointKey struct {
	RunID    session.RunID
	Revision int64
}

func (s *admissionStore) WithinTx(ctx context.Context, fn func(context.Context, session.Store) error) error {
	s.mu.Lock()
	tx := s.clone()
	s.mu.Unlock()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions = tx.sessions
	s.runs = tx.runs
	s.messages = tx.messages
	s.finalized = tx.finalized
	s.parts = tx.parts
	s.events = tx.events
	s.eventSeq = tx.eventSeq
	s.eventOrder = tx.eventOrder
	s.toolCalls = tx.toolCalls
	s.epochs = tx.epochs
	s.modelRequests = tx.modelRequests
	s.turns = tx.turns
	s.inbox = tx.inbox
	s.checkpoints = tx.checkpoints
	return nil
}

// clone must only be called with s.mu already held (WithinTx is the only
// caller); it never locks itself.
func (s *admissionStore) clone() *admissionStore {
	return &admissionStore{
		sessions:          cloneMap(s.sessions),
		runs:              cloneMap(s.runs),
		messages:          cloneMap(s.messages),
		finalized:         cloneMap(s.finalized),
		parts:             cloneMap(s.parts),
		events:            cloneMap(s.events),
		eventSeq:          s.eventSeq,
		eventOrder:        cloneMap(s.eventOrder),
		toolCalls:         cloneMap(s.toolCalls),
		epochs:            cloneMap(s.epochs),
		modelRequests:     cloneMap(s.modelRequests),
		turns:             cloneMap(s.turns),
		inbox:             cloneMap(s.inbox),
		checkpoints:       cloneMap(s.checkpoints),
		appendEventErr:    s.appendEventErr,
		appendPartErrAt:   s.appendPartErrAt,
		appendPartCalls:   s.appendPartCalls,
		settleToolCallErr: s.settleToolCallErr,
		toolTransitionErr: s.toolTransitionErr,
		createToolErrAt:   s.createToolErrAt,
		createToolCalls:   s.createToolCalls,
		normalizeEvent:    s.normalizeEvent,
		listMessagesHook:  s.listMessagesHook,
	}
}

func (s *admissionStore) CreateSession(_ context.Context, record session.Session) (session.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.sessions[record.ID]; ok {
		if existing.Title != record.Title || existing.Directory != record.Directory {
			return session.Session{}, session.ErrConflict
		}
		return existing, nil
	}
	s.sessions[record.ID] = record
	return record, nil
}

func (s *admissionStore) GetSession(_ context.Context, id session.ID) (session.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.sessions[id]
	if !ok {
		return session.Session{}, session.ErrNotFound
	}
	return record, nil
}

func (s *admissionStore) UpdateSession(context.Context, session.Session) error { return nil }

func (s *admissionStore) SetSessionTitle(ctx context.Context, request session.SessionTitleRequest) (session.SessionTitleResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setSessionTitleLocked(ctx, request)
}

// setSessionTitleLocked is SetSessionTitle's body; callers must hold s.mu.
func (s *admissionStore) setSessionTitleLocked(ctx context.Context, request session.SessionTitleRequest) (session.SessionTitleResult, error) {
	if err := ctx.Err(); err != nil {
		return session.SessionTitleResult{}, err
	}
	if err := request.Validate(); err != nil {
		return session.SessionTitleResult{}, err
	}
	record, ok := s.sessions[request.SessionID]
	if !ok {
		return session.SessionTitleResult{}, session.ErrNotFound
	}
	if record.WorkspaceID != request.WorkspaceID {
		return session.SessionTitleResult{}, session.ErrConflict
	}
	if record.Title == request.Title {
		return session.SessionTitleResult{Title: record.Title, UpdatedAt: record.UpdatedAt}, nil
	}
	record.Title = request.Title
	record.UpdatedAt = time.Now().UTC()
	s.sessions[record.ID] = record
	return session.SessionTitleResult{Title: record.Title, UpdatedAt: record.UpdatedAt, Changed: true}, nil
}

func (s *admissionStore) AdmitRun(_ context.Context, run session.Run, leaseDuration time.Duration) (session.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.runs[run.ID]; ok {
		return session.Run{}, session.ErrConflict
	}
	for _, existing := range s.runs {
		if existing.SessionID == run.SessionID && !existing.Terminal() {
			return session.Run{}, session.ErrSessionBusy
		}
	}
	run.LeaseUntil = time.Now().UTC().Add(leaseDuration)
	s.runs[run.ID] = run
	return run, nil
}

func (s *admissionStore) ClaimRun(_ context.Context, claim session.RunClaim) (session.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[claim.RunID]
	if !ok {
		return session.Run{}, session.ErrNotFound
	}
	if run.Terminal() || run.LeaseUntil.After(time.Now().UTC()) {
		return session.Run{}, session.ErrSessionBusy
	}
	run.OwnerID = claim.OwnerID
	run.ClaimToken = claim.ClaimToken
	run.Status = session.RunRunning
	run.LeaseUntil = time.Now().UTC().Add(claim.LeaseDuration)
	s.runs[run.ID] = run
	return run, nil
}

func (s *admissionStore) Execution(fence session.RunFence) session.ExecutionStore {
	return &fakeExecutionStore{admissionStore: s, fence: fence}
}

func (s *admissionStore) GetRun(_ context.Context, id session.RunID) (session.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getRunCalls.Add(1)
	run, ok := s.runs[id]
	if !ok {
		return session.Run{}, session.ErrNotFound
	}
	return run, nil
}

func (s *admissionStore) ActiveRun(_ context.Context, sessionID session.ID) (session.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, run := range s.runs {
		if run.SessionID == sessionID && !run.Terminal() {
			return run, nil
		}
	}
	return session.Run{}, session.ErrNotFound
}

func (s *admissionStore) ListUnfinishedRuns(context.Context) ([]session.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var runs []session.Run
	for _, run := range s.runs {
		if !run.Terminal() {
			runs = append(runs, run)
		}
	}
	return runs, nil
}

func (s *admissionStore) RenewRunLease(context.Context, session.RunID, string, time.Time) error {
	return nil
}

func (s *admissionStore) FinishRun(ctx context.Context, run session.Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finishRunLocked(ctx, run)
}

// finishRunLocked is FinishRun's body; callers must hold s.mu.
func (s *admissionStore) finishRunLocked(_ context.Context, run session.Run) error {
	if _, ok := s.runs[run.ID]; !ok {
		return session.ErrNotFound
	}
	s.runs[run.ID] = run
	return nil
}

func (s *admissionStore) AppendMessage(ctx context.Context, message session.Message) (session.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendMessageLocked(ctx, message)
}

// appendMessageLocked is AppendMessage's body; callers must hold s.mu.
func (s *admissionStore) appendMessageLocked(_ context.Context, message session.Message) (session.Message, error) {
	if existing, ok := s.messages[message.ID]; ok {
		if existing.Role != message.Role {
			return session.Message{}, session.ErrConflict
		}
		return existing, nil
	}
	s.messages[message.ID] = message
	return message, nil
}

func (s *admissionStore) AppendPart(ctx context.Context, part session.Part) (session.Part, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendPartLocked(ctx, part)
}

// appendPartLocked is AppendPart's body; callers must hold s.mu.
func (s *admissionStore) appendPartLocked(_ context.Context, part session.Part) (session.Part, error) {
	s.appendPartCalls++
	if s.appendPartErrAt > 0 && s.appendPartCalls == s.appendPartErrAt {
		return session.Part{}, errors.New("injected append part failure")
	}
	if existing, ok := s.parts[part.ID]; ok {
		if existing.Kind != part.Kind {
			return session.Part{}, session.ErrConflict
		}
		return existing, nil
	}
	s.parts[part.ID] = part
	return part, nil
}

func (s *admissionStore) UpdatePart(_ context.Context, part session.Part) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.parts[part.ID]; !ok {
		return session.ErrNotFound
	}
	s.parts[part.ID] = part
	return nil
}

func (s *admissionStore) ListMessages(_ context.Context, sessionID session.ID, _ session.ReplayCursor) (session.ReplayBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listMessagesCalls.Add(1)
	if s.listMessagesHook != nil {
		s.listMessagesHook(s, sessionID)
	}
	var messages []session.Message
	for _, message := range s.messages {
		if message.SessionID == sessionID {
			messages = append(messages, message)
		}
	}
	sort.SliceStable(messages, func(i, j int) bool {
		if !messages[i].CreatedAt.Equal(messages[j].CreatedAt) {
			return messages[i].CreatedAt.Before(messages[j].CreatedAt)
		}
		return messages[i].ID < messages[j].ID
	})
	var parts []session.Part
	for _, part := range s.parts {
		if part.SessionID == sessionID {
			parts = append(parts, part)
		}
	}
	sort.SliceStable(parts, func(i, j int) bool {
		if parts[i].MessageID != parts[j].MessageID {
			return parts[i].MessageID < parts[j].MessageID
		}
		return parts[i].Ordinal < parts[j].Ordinal
	})
	return session.ReplayBatch{Messages: messages, Parts: parts}, nil
}

func (s *admissionStore) AppendEvent(ctx context.Context, event session.EventRecord) (session.EventRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendEventLocked(ctx, event)
}

// appendEventLocked is AppendEvent's body; callers must hold s.mu.
func (s *admissionStore) appendEventLocked(_ context.Context, event session.EventRecord) (session.EventRecord, error) {
	if s.appendEventErr != nil {
		return session.EventRecord{}, s.appendEventErr
	}
	if event.ToolTransition != "" || (event.Kind == session.ToolTransitionEventKind && event.ToolCallID != "") {
		return session.EventRecord{}, session.ErrConflict
	}
	if s.normalizeEvent != nil {
		event = s.normalizeEvent(event)
	}
	if existing, ok := s.events[event.ID]; ok {
		if existing.Kind != event.Kind {
			return session.EventRecord{}, session.ErrConflict
		}
		return existing, nil
	}
	s.putEvent(event)
	return event, nil
}

func cloneMap[K comparable, V any](src map[K]V) map[K]V {
	dst := make(map[K]V, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

// putEvent is the only place s.events is ever written; callers must hold
// s.mu. It records each event's insertion order in eventOrder so ListEvents
// can return a deterministic sequence even when several events share an
// identical CreatedAt (a fixed test clock is common) -- Go's map iteration
// order is randomized and cannot be relied on for that.
func (s *admissionStore) putEvent(event session.EventRecord) {
	if s.eventOrder == nil {
		s.eventOrder = map[session.EventID]int64{}
	}
	if _, exists := s.events[event.ID]; !exists {
		s.eventSeq++
		s.eventOrder[event.ID] = s.eventSeq
	}
	s.events[event.ID] = event
}

func (s *admissionStore) ListEvents(_ context.Context, sessionID session.ID, _ session.EventCursor) (session.EventBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []session.EventRecord
	for _, event := range s.events {
		if event.SessionID == sessionID {
			events = append(events, event)
		}
	}
	sort.SliceStable(events, func(i, j int) bool { return s.eventOrder[events[i].ID] < s.eventOrder[events[j].ID] })
	return session.EventBatch{Events: events}, nil
}

func (s *admissionStore) CreateToolCall(ctx context.Context, request session.CreateToolCallRequest) (session.ToolTransitionResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createToolCallLocked(ctx, request)
}

// createToolCallLocked is CreateToolCall's body; callers must hold s.mu.
func (s *admissionStore) createToolCallLocked(_ context.Context, request session.CreateToolCallRequest) (session.ToolTransitionResult, error) {
	s.createToolCalls++
	if s.createToolErrAt > 0 && s.createToolCalls == s.createToolErrAt {
		return session.ToolTransitionResult{}, errors.New("injected create tool failure")
	}
	if s.toolTransitionErr != nil {
		return session.ToolTransitionResult{}, s.toolTransitionErr
	}
	call := request.Call
	if existing, ok := s.toolCalls[call.ID]; ok {
		if existing.Name != call.Name {
			return session.ToolTransitionResult{}, session.ErrConflict
		}
		return session.ToolTransitionResult{Call: existing, Event: s.events[request.Event.ID]}, nil
	}
	event, err := session.ToolTransitionRecord(call, request.Event)
	if err != nil {
		return session.ToolTransitionResult{}, err
	}
	if request.RequestPart.ID == "" || request.RequestPart.ID != call.RequestPartID || request.RequestPart.Kind != session.PartFunctionToolCall {
		return session.ToolTransitionResult{}, session.ErrConflict
	}
	if _, err := s.appendPartLocked(context.Background(), request.RequestPart); err != nil {
		return session.ToolTransitionResult{}, err
	}
	s.toolCalls[call.ID] = call
	s.putEvent(event)
	return session.ToolTransitionResult{Call: call, Event: event}, nil
}
func (s *admissionStore) GetToolCall(_ context.Context, id session.ToolCallID) (session.ToolCall, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	call, ok := s.toolCalls[id]
	if !ok {
		return session.ToolCall{}, session.ErrNotFound
	}
	return call, nil
}
func (s *admissionStore) ListUnfinishedToolCalls(_ context.Context, runID session.RunID) ([]session.ToolCall, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var calls []session.ToolCall
	for _, call := range s.toolCalls {
		if call.RunID == runID && !session.TerminalToolCall(call.Status) {
			calls = append(calls, call)
		}
	}
	return calls, nil
}
func (s *admissionStore) ClaimToolCall(ctx context.Context, request session.ClaimToolCallRequest) (session.ToolTransitionResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimToolCallLocked(ctx, request)
}

// claimToolCallLocked is ClaimToolCall's body; callers must hold s.mu.
func (s *admissionStore) claimToolCallLocked(_ context.Context, request session.ClaimToolCallRequest) (session.ToolTransitionResult, error) {
	if s.toolTransitionErr != nil {
		return session.ToolTransitionResult{}, s.toolTransitionErr
	}
	call, ok := s.toolCalls[request.ID]
	if !ok {
		return session.ToolTransitionResult{}, session.ErrNotFound
	}
	call.Status = session.ToolCallRunning
	call.ClaimedBy = request.ClaimedBy
	call.ClaimToken = request.ClaimToken
	call.StartedAt = request.StartedAt
	event, err := session.ToolTransitionRecord(call, request.Event)
	if err != nil {
		return session.ToolTransitionResult{}, err
	}
	s.toolCalls[call.ID] = call
	s.putEvent(event)
	return session.ToolTransitionResult{Call: call, Event: event}, nil
}
func (s *admissionStore) SettleToolCall(_ context.Context, request session.SettleToolCallRequest) (session.ToolTransitionResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.toolTransitionErr != nil {
		return session.ToolTransitionResult{}, s.toolTransitionErr
	}
	settlement := request.Settlement
	if s.settleToolCallErr != nil {
		return session.ToolTransitionResult{}, s.settleToolCallErr
	}
	call, ok := s.toolCalls[settlement.ID]
	if !ok {
		return session.ToolTransitionResult{}, session.ErrNotFound
	}
	terminal, err := settlement.Apply(call)
	if err != nil {
		return session.ToolTransitionResult{}, err
	}
	event, err := session.ToolTransitionRecord(terminal, request.Event)
	if err != nil {
		return session.ToolTransitionResult{}, err
	}
	s.toolCalls[terminal.ID] = terminal
	s.messages[settlement.ResultMessage.ID] = settlement.ResultMessage
	s.parts[settlement.ResultPart.ID] = settlement.ResultPart
	s.putEvent(event)
	return session.ToolTransitionResult{Call: terminal, Event: event}, nil
}
func (s *admissionStore) StartContextEpoch(_ context.Context, epoch session.ContextEpoch) (session.ContextEpoch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.epochs[epoch.ID]; ok {
		if sameEpoch(existing, epoch) {
			return existing, nil
		}
		return session.ContextEpoch{}, session.ErrConflict
	}
	s.epochs[epoch.ID] = epoch
	return epoch, nil
}
func (s *admissionStore) FinishContextEpoch(_ context.Context, epoch session.ContextEpoch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.epochs[epoch.ID]; !ok {
		return session.ErrNotFound
	}
	s.epochs[epoch.ID] = epoch
	return nil
}
func (s *admissionStore) ListContextEpochs(_ context.Context, sessionID session.ID) ([]session.ContextEpoch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var epochs []session.ContextEpoch
	for _, epoch := range s.epochs {
		if epoch.SessionID == sessionID {
			epochs = append(epochs, epoch)
		}
	}
	sort.SliceStable(epochs, func(i, j int) bool {
		return epochs[i].ID < epochs[j].ID
	})
	return epochs, nil
}

func sameEpoch(left session.ContextEpoch, right session.ContextEpoch) bool {
	return left.ID == right.ID &&
		left.SessionID == right.SessionID &&
		left.ParentEpochID == right.ParentEpochID &&
		left.SummaryMessageID == right.SummaryMessageID &&
		left.SummarizedFromID == right.SummarizedFromID &&
		left.SummarizedToID == right.SummarizedToID &&
		left.TailStartID == right.TailStartID &&
		left.ModelID == right.ModelID &&
		left.ProviderID == right.ProviderID &&
		left.Trigger == right.Trigger &&
		left.Reason == right.Reason &&
		left.NextAction == right.NextAction
}

type fakeExecutionStore struct {
	*admissionStore
	fence session.RunFence
}

func (s *fakeExecutionStore) WithinTx(ctx context.Context, fn func(context.Context, session.ExecutionStore) error) error {
	s.mu.Lock()
	tx := s.clone()
	s.mu.Unlock()
	if err := fn(ctx, &fakeExecutionStore{admissionStore: tx, fence: s.fence}); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions = tx.sessions
	s.runs = tx.runs
	s.messages = tx.messages
	s.finalized = tx.finalized
	s.parts = tx.parts
	s.events = tx.events
	s.eventSeq = tx.eventSeq
	s.eventOrder = tx.eventOrder
	s.toolCalls = tx.toolCalls
	s.epochs = tx.epochs
	s.modelRequests = tx.modelRequests
	s.turns = tx.turns
	s.inbox = tx.inbox
	s.checkpoints = tx.checkpoints
	return nil
}

func (s *fakeExecutionStore) valid() bool {
	run, ok := s.runs[s.fence.RunID]
	return ok && !run.Terminal() && run.ClaimToken == s.fence.ClaimToken
}

func (s *fakeExecutionStore) SetSessionTitle(ctx context.Context, request session.SessionTitleRequest) (session.SessionTitleResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() || s.runs[s.fence.RunID].SessionID != request.SessionID {
		return session.SessionTitleResult{}, session.ErrConflict
	}
	return s.setSessionTitleLocked(ctx, request)
}

func (s *fakeExecutionStore) StartRun(_ context.Context, startedAt time.Time) (session.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() {
		return session.Run{}, session.ErrConflict
	}
	run := s.runs[s.fence.RunID]
	run.Status = session.RunRunning
	run.StartedAt = startedAt
	s.runs[run.ID] = run
	return run, nil
}

func (s *fakeExecutionStore) RenewRunLease(_ context.Context, duration time.Duration) (session.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() {
		return session.Run{}, session.ErrConflict
	}
	run := s.runs[s.fence.RunID]
	run.LeaseUntil = time.Now().UTC().Add(duration)
	s.runs[run.ID] = run
	return run, nil
}

func (s *fakeExecutionStore) SettleRun(ctx context.Context, request session.SettleRunRequest) (session.RunSettlementResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() {
		return session.RunSettlementResult{}, session.ErrConflict
	}
	for _, call := range s.toolCalls {
		if call.RunID == s.fence.RunID && !session.TerminalToolCall(call.Status) {
			return session.RunSettlementResult{}, session.ErrConflict
		}
	}
	current := s.runs[s.fence.RunID]
	// Mirror store/internal/sqlstore's SettleRun: a RunCompleted settlement
	// must never finalize while the session has any durably queued inbox
	// item (see runtime's Enqueue/EnqueueInboxForRun and the terminal-
	// settlement race rule).
	if request.Settlement.Status == session.RunCompleted {
		for _, item := range s.inbox {
			if item.SessionID == current.SessionID && item.State == session.InboxQueued {
				return session.RunSettlementResult{}, session.ErrRunHasQueuedInput
			}
		}
	}
	run, err := session.ApplyRunSettlement(current, request.Settlement)
	if err != nil {
		return session.RunSettlementResult{}, err
	}
	event, err := session.RunSettlementRecord(run, request.Event)
	if err != nil {
		return session.RunSettlementResult{}, err
	}
	if err := s.finishRunLocked(ctx, run); err != nil {
		return session.RunSettlementResult{}, err
	}
	record, err := s.appendEventLocked(ctx, event)
	return session.RunSettlementResult{Run: run, Event: record}, err
}

func (s *fakeExecutionStore) ClaimToolCall(ctx context.Context, request session.ClaimToolCallRequest) (session.ToolTransitionResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() {
		return session.ToolTransitionResult{}, session.ErrConflict
	}
	claimed, err := s.claimToolCallLocked(ctx, request)
	if err != nil {
		return session.ToolTransitionResult{}, err
	}
	claimed.Call.LeaseUntil = time.Now().UTC().Add(request.LeaseDuration)
	s.toolCalls[claimed.Call.ID] = claimed.Call
	return claimed, nil
}

func (s *fakeExecutionStore) CreateModelRequest(_ context.Context, record session.ModelRequestRecord) (session.ModelRequestRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() || record.ID == "" || record.RunID != s.fence.RunID || record.State != session.ModelRequestPrepared {
		return session.ModelRequestRecord{}, session.ErrConflict
	}
	run := s.runs[s.fence.RunID]
	if record.SessionID != run.SessionID {
		return session.ModelRequestRecord{}, session.ErrConflict
	}
	if existing, ok := s.modelRequests[record.ID]; ok {
		if !reflect.DeepEqual(existing, record) {
			return session.ModelRequestRecord{}, session.ErrConflict
		}
		return existing, nil
	}
	s.modelRequests[record.ID] = record
	return record, nil
}

func (s *fakeExecutionStore) UpdateModelRequest(_ context.Context, record session.ModelRequestRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() || record.RunID != s.fence.RunID {
		return session.ErrConflict
	}
	current, ok := s.modelRequests[record.ID]
	if !ok {
		return session.ErrNotFound
	}
	left, right := current, record
	left.State, right.State = "", ""
	left.ErrorCode, right.ErrorCode = "", ""
	left.UpdatedAt, right.UpdatedAt = time.Time{}, time.Time{}
	if !reflect.DeepEqual(left, right) {
		return session.ErrConflict
	}
	if current.State == record.State {
		if reflect.DeepEqual(current, record) {
			return nil
		}
		return session.ErrConflict
	}
	if !session.ValidModelRequestTransition(current.State, record.State) {
		return session.ErrConflict
	}
	s.modelRequests[record.ID] = record
	return nil
}

func (s *admissionStore) GetModelRequest(_ context.Context, id session.ModelRequestID) (session.ModelRequestRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.modelRequests[id]
	if !ok {
		return session.ModelRequestRecord{}, session.ErrNotFound
	}
	return record, nil
}

func (s *admissionStore) ListModelRequests(_ context.Context, runID session.RunID, cursor session.ModelRequestCursor) (session.ModelRequestBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	limit := cursor.Limit
	if limit <= 0 {
		limit = 100
	}
	var records []session.ModelRequestRecord
	for _, record := range s.modelRequests {
		if record.RunID == runID {
			records = append(records, record)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		if !records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].CreatedAt.Before(records[j].CreatedAt)
		}
		return records[i].ID < records[j].ID
	})
	if cursor.AfterID != "" {
		after, ok := s.modelRequests[cursor.AfterID]
		if !ok {
			return session.ModelRequestBatch{}, session.ErrNotFound
		}
		filtered := records[:0]
		for _, record := range records {
			if record.CreatedAt.After(after.CreatedAt) || record.CreatedAt.Equal(after.CreatedAt) && record.ID > after.ID {
				filtered = append(filtered, record)
			}
		}
		records = filtered
	}
	next := session.ModelRequestCursor{}
	if len(records) > limit {
		next = session.ModelRequestCursor{AfterID: records[limit-1].ID, Limit: limit}
		records = records[:limit]
	}
	return session.ModelRequestBatch{Records: records, Next: next}, nil
}

var _ session.ExecutionStore = (*fakeExecutionStore)(nil)

func (s *fakeExecutionStore) FinalizeAssistantMessage(_ context.Context, id session.MessageID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.messages[id]
	if !ok || !s.valid() || m.RunID != s.fence.RunID || m.SessionID != s.runs[s.fence.RunID].SessionID || m.Role != session.RoleAssistant {
		return session.ErrConflict
	}
	s.finalized[id] = true
	return nil
}

// --- W5 durable additions: minimal in-memory fakes ---

func (s *admissionStore) EnqueueInbox(_ context.Context, item session.InboxItem, limits session.ContentLimits) (session.InboxItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, _, err := s.enqueueInboxLocked(item, limits)
	return result, err
}

// EnqueueInboxForRun mirrors store/internal/sqlstore's EnqueueInboxForRun:
// checked (under s.mu, this fixture's single lock, the in-memory analogue of
// the real store's session-row lock) against runID's current terminal
// status before persisting.
func (s *admissionStore) EnqueueInboxForRun(_ context.Context, runID session.RunID, item session.InboxItem, limits session.ContentLimits) (session.InboxItem, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := session.ValidateEnqueueInbox(item, limits); err != nil {
		return session.InboxItem{}, false, err
	}
	run, ok := s.runs[runID]
	if !ok {
		return session.InboxItem{}, false, session.ErrRunClosed
	}
	if run.SessionID != item.SessionID {
		return session.InboxItem{}, false, session.ErrConflict
	}
	if run.Terminal() {
		return session.InboxItem{}, false, session.ErrRunClosed
	}
	return s.enqueueInboxLocked(item, limits)
}

func (s *admissionStore) enqueueInboxLocked(item session.InboxItem, limits session.ContentLimits) (session.InboxItem, bool, error) {
	if err := session.ValidateEnqueueInbox(item, limits); err != nil {
		return session.InboxItem{}, false, err
	}
	for _, existing := range s.inbox {
		if existing.SessionID == item.SessionID && existing.IdempotencyKey == item.IdempotencyKey {
			// See store/internal/sqlstore/inbox.go's matching comment:
			// ignore block ID so a genuine retry under the same
			// IdempotencyKey (whose blocks get freshly minted IDs on every
			// call) matches the original item instead of false-conflicting.
			if !session.ContentBlocksEqualIgnoringID(existing.Blocks, item.Blocks) {
				return session.InboxItem{}, false, session.ErrConflict
			}
			return existing, false, nil
		}
	}
	s.inbox[item.ID] = item
	return item, true, nil
}

func (s *admissionStore) ListInbox(_ context.Context, sessionID session.ID, states []session.InboxState) ([]session.InboxItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	allow := map[session.InboxState]bool{}
	for _, state := range states {
		allow[state] = true
	}
	var out []session.InboxItem
	for _, item := range s.inbox {
		if item.SessionID != sessionID {
			continue
		}
		if len(states) > 0 && !allow[item.State] {
			continue
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *admissionStore) GetTurn(_ context.Context, id session.TurnID) (session.Turn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	turn, ok := s.turns[id]
	if !ok {
		return session.Turn{}, session.ErrNotFound
	}
	return turn, nil
}

func (s *admissionStore) ListTurns(_ context.Context, runID session.RunID) ([]session.Turn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []session.Turn
	for _, turn := range s.turns {
		if turn.RunID == runID {
			out = append(out, turn)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ordinal < out[j].Ordinal })
	return out, nil
}

func (s *admissionStore) ReadPromotedCheckpoint(_ context.Context, runID session.RunID) (session.Checkpoint, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best session.Checkpoint
	found := false
	for key, checkpoint := range s.checkpoints {
		if key.RunID == runID && checkpoint.Promoted && (!found || checkpoint.Revision > best.Revision) {
			best, found = checkpoint, true
		}
	}
	return best, found, nil
}

func (s *admissionStore) RetireRunCheckpoints(_ context.Context, runID session.RunID, upToRevision int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok || !run.Terminal() {
		return session.ErrConflict
	}
	for key := range s.checkpoints {
		if key.RunID == runID && key.Revision <= upToRevision {
			delete(s.checkpoints, key)
		}
	}
	return nil
}

func (s *fakeExecutionStore) AdmitTurn(ctx context.Context, request session.AdmitTurnRequest) (session.AdmitTurnResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() {
		return session.AdmitTurnResult{}, session.ErrConflict
	}
	if err := session.ValidateAdmitTurn(s.runs[s.fence.RunID], request); err != nil {
		return session.AdmitTurnResult{}, err
	}
	for _, message := range request.UserMessages {
		if _, err := s.appendMessageLocked(ctx, message); err != nil {
			return session.AdmitTurnResult{}, err
		}
	}
	for _, part := range request.UserParts {
		if _, err := s.appendPartLocked(ctx, part); err != nil {
			return session.AdmitTurnResult{}, err
		}
	}
	if request.AssistantPlaceholder.ID != "" {
		if _, err := s.appendMessageLocked(ctx, request.AssistantPlaceholder); err != nil {
			return session.AdmitTurnResult{}, err
		}
	}
	if existing, ok := s.turns[request.Turn.ID]; ok && !reflect.DeepEqual(existing, request.Turn) {
		return session.AdmitTurnResult{}, session.ErrConflict
	}
	s.turns[request.Turn.ID] = request.Turn
	for _, id := range request.InboxIDs {
		item, ok := s.inbox[id]
		if !ok {
			return session.AdmitTurnResult{}, session.ErrConflict
		}
		if item.State == session.InboxConsumed && item.TurnID == request.Turn.ID {
			continue
		}
		if item.State != session.InboxQueued {
			return session.AdmitTurnResult{}, session.ErrConflict
		}
		item.State = session.InboxConsumed
		item.TurnID = request.Turn.ID
		s.inbox[id] = item
	}
	s.putEvent(request.Event)
	return session.AdmitTurnResult{Turn: request.Turn, Event: request.Event}, nil
}

func (s *fakeExecutionStore) CompleteTurn(_ context.Context, request session.CompleteTurnRequest) (session.CompleteTurnResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() {
		return session.CompleteTurnResult{}, session.ErrConflict
	}
	current, ok := s.turns[request.TurnID]
	if !ok {
		return session.CompleteTurnResult{}, session.ErrConflict
	}
	candidate, err := session.ApplyCompleteTurn(current, request)
	if err != nil {
		return session.CompleteTurnResult{}, err
	}
	s.turns[candidate.ID] = candidate
	for id, item := range s.inbox {
		if item.TurnID == candidate.ID && (item.State == session.InboxConsumed || item.State == session.InboxInterrupted) {
			item.State = session.InboxCompleted
			s.inbox[id] = item
		}
	}
	s.putEvent(request.Event)
	return session.CompleteTurnResult{Turn: candidate, Event: request.Event}, nil
}

func (s *fakeExecutionStore) InterruptTurn(_ context.Context, request session.InterruptTurnRequest) (session.InterruptTurnResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() {
		return session.InterruptTurnResult{}, session.ErrConflict
	}
	current, ok := s.turns[request.TurnID]
	if !ok {
		return session.InterruptTurnResult{}, session.ErrConflict
	}
	candidate, err := session.ApplyInterruptTurn(current, request)
	if err != nil {
		return session.InterruptTurnResult{}, err
	}
	s.turns[candidate.ID] = candidate
	for id, item := range s.inbox {
		if item.TurnID == candidate.ID && item.State == session.InboxConsumed {
			item.State = session.InboxInterrupted
			s.inbox[id] = item
		}
	}
	s.putEvent(request.Event)
	return session.InterruptTurnResult{Turn: candidate, Event: request.Event}, nil
}

func (s *fakeExecutionStore) ReconcileInterruptedTurn(_ context.Context, request session.ReconcileInterruptedTurnRequest) (session.ReconcileInterruptedTurnResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() {
		return session.ReconcileInterruptedTurnResult{}, session.ErrConflict
	}
	current, ok := s.turns[request.TurnID]
	if !ok {
		return session.ReconcileInterruptedTurnResult{}, session.ErrConflict
	}
	candidate, err := session.ApplyInterruptTurn(current, session.InterruptTurnRequest(request))
	if err != nil {
		return session.ReconcileInterruptedTurnResult{}, err
	}
	s.turns[candidate.ID] = candidate
	for id, item := range s.inbox {
		if item.TurnID == candidate.ID && item.State == session.InboxConsumed {
			item.State = session.InboxQueued
			item.TurnID = ""
			s.inbox[id] = item
		}
	}
	s.putEvent(request.Event)
	return session.ReconcileInterruptedTurnResult{Turn: candidate, Event: request.Event}, nil
}

func (s *fakeExecutionStore) RepauseRun(_ context.Context, request session.RepauseRunRequest) (session.RepauseRunResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() {
		return session.RepauseRunResult{}, session.ErrConflict
	}
	run := s.runs[s.fence.RunID]
	if err := session.ValidateRepauseRun(run, request); err != nil {
		return session.RepauseRunResult{}, err
	}
	run.Status = session.RunPaused
	run.LeaseUntil = time.Time{}
	s.runs[run.ID] = run
	s.putEvent(request.Event)
	return session.RepauseRunResult{Run: run, Event: request.Event}, nil
}

func (s *fakeExecutionStore) StageCheckpoint(_ context.Context, request session.StageCheckpointRequest) (session.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() {
		return session.Checkpoint{}, session.ErrConflict
	}
	if _, err := session.ValidateStageCheckpoint(s.runs[s.fence.RunID], request); err != nil {
		return session.Checkpoint{}, err
	}
	key := fakeCheckpointKey{RunID: request.Checkpoint.RunID, Revision: request.Checkpoint.Revision}
	if existing, ok := s.checkpoints[key]; ok {
		if !reflect.DeepEqual(existing, request.Checkpoint) {
			return session.Checkpoint{}, session.ErrConflict
		}
		return existing, nil
	}
	s.checkpoints[key] = request.Checkpoint
	return request.Checkpoint, nil
}

func (s *fakeExecutionStore) PromotePause(_ context.Context, request session.PromotePauseRequest) (session.PromotePauseResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() {
		return session.PromotePauseResult{}, session.ErrConflict
	}
	run := s.runs[s.fence.RunID]
	if err := session.ValidatePromotePause(run, request); err != nil {
		return session.PromotePauseResult{}, err
	}
	key := fakeCheckpointKey{RunID: run.ID, Revision: request.Revision}
	checkpoint, ok := s.checkpoints[key]
	if !ok {
		return session.PromotePauseResult{}, session.ErrConflict
	}
	checkpoint.Promoted = true
	s.checkpoints[key] = checkpoint
	turn, ok := s.turns[request.TurnID]
	if !ok {
		return session.PromotePauseResult{}, session.ErrConflict
	}
	interruptedTurn, err := session.ApplyInterruptTurn(turn, session.InterruptTurnRequest{TurnID: request.TurnID, Event: request.Event})
	if err != nil {
		return session.PromotePauseResult{}, err
	}
	s.turns[interruptedTurn.ID] = interruptedTurn
	for _, id := range request.InboxIDs {
		item, ok := s.inbox[id]
		if !ok {
			// Mirror store/internal/sqlstore's interruptInboxItems: an inbox
			// ID with no durable row is a protocol error, not a no-op (this
			// is what let the first-turn sentinel leak into InboxIDs ship
			// green against this fixture -- see runtime/turn_loop.go's
			// interruptedItemIDs).
			return session.PromotePauseResult{}, session.ErrConflict
		}
		if item.State == session.InboxInterrupted {
			continue
		}
		if item.State != session.InboxQueued && item.State != session.InboxConsumed {
			return session.PromotePauseResult{}, session.ErrConflict
		}
		item.State = session.InboxInterrupted
		s.inbox[id] = item
	}
	run.Status = session.RunPaused
	run.LeaseUntil = time.Time{}
	s.runs[run.ID] = run
	s.putEvent(request.Event)
	return session.PromotePauseResult{Run: run, Turn: interruptedTurn, Checkpoint: checkpoint, Event: request.Event}, nil
}

func (s *fakeExecutionStore) RetireCheckpoints(_ context.Context, upToRevision int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.valid() {
		return session.ErrConflict
	}
	for key := range s.checkpoints {
		if key.RunID == s.fence.RunID && key.Revision <= upToRevision {
			delete(s.checkpoints, key)
		}
	}
	return nil
}
