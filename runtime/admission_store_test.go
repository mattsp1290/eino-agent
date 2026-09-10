package runtime

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync/atomic"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

// Shared in-memory Store and fenced ExecutionStore used by runtime tests.
type admissionStore struct {
	sessions          map[session.ID]session.Session
	runs              map[session.RunID]session.Run
	messages          map[session.MessageID]session.Message
	finalized         map[session.MessageID]bool
	parts             map[session.PartID]session.Part
	events            map[session.EventID]session.EventRecord
	toolCalls         map[session.ToolCallID]session.ToolCall
	epochs            map[session.EpochID]session.ContextEpoch
	modelRequests     map[session.ModelRequestID]session.ModelRequestRecord
	admissions        map[string]session.AdmissionRecord
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
		toolCalls:     map[session.ToolCallID]session.ToolCall{},
		epochs:        map[session.EpochID]session.ContextEpoch{},
		modelRequests: map[session.ModelRequestID]session.ModelRequestRecord{},
		admissions:    map[string]session.AdmissionRecord{},
	}
}

func (s *admissionStore) WithinTx(ctx context.Context, fn func(context.Context, session.Store) error) error {
	tx := s.clone()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	s.sessions = tx.sessions
	s.runs = tx.runs
	s.messages = tx.messages
	s.finalized = tx.finalized
	s.parts = tx.parts
	s.events = tx.events
	s.toolCalls = tx.toolCalls
	s.epochs = tx.epochs
	s.modelRequests = tx.modelRequests
	s.admissions = tx.admissions
	return nil
}

func (s *admissionStore) clone() *admissionStore {
	return &admissionStore{
		sessions:          cloneMap(s.sessions),
		runs:              cloneMap(s.runs),
		messages:          cloneMap(s.messages),
		finalized:         cloneMap(s.finalized),
		parts:             cloneMap(s.parts),
		events:            cloneMap(s.events),
		toolCalls:         cloneMap(s.toolCalls),
		epochs:            cloneMap(s.epochs),
		modelRequests:     cloneMap(s.modelRequests),
		admissions:        cloneMap(s.admissions),
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

func admissionStoreKey(id session.ID, key string) string { return string(id) + "\x00" + key }

func (s *admissionStore) LookupAdmission(_ context.Context, id session.ID, key string) (session.AdmissionRecord, error) {
	if err := session.ValidateAdmissionKey(key); err != nil {
		return session.AdmissionRecord{}, err
	}
	record, ok := s.admissions[admissionStoreKey(id, key)]
	if !ok {
		return session.AdmissionRecord{}, session.ErrNotFound
	}
	record.RunStatus = s.runs[record.Receipt.RunID].Status
	return record, nil
}

func (s *admissionStore) GetAdmission(ctx context.Context, id session.ID, key string) (session.AdmissionRecord, error) {
	return s.LookupAdmission(ctx, id, key)
}

func (s *admissionStore) LockAdmissionSession(_ context.Context, candidate session.Session) (session.Session, error) {
	if existing, ok := s.sessions[candidate.ID]; ok {
		return existing, nil
	}
	s.sessions[candidate.ID] = candidate
	return candidate, nil
}

func (s *admissionStore) CreateSession(_ context.Context, record session.Session) (session.Session, error) {
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
	record, ok := s.sessions[id]
	if !ok {
		return session.Session{}, session.ErrNotFound
	}
	return record, nil
}

func (s *admissionStore) UpdateSession(context.Context, session.Session) error { return nil }

func (s *admissionStore) SetSessionTitle(ctx context.Context, request session.SessionTitleRequest) (session.SessionTitleResult, error) {
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
	s.getRunCalls.Add(1)
	run, ok := s.runs[id]
	if !ok {
		return session.Run{}, session.ErrNotFound
	}
	return run, nil
}

func (s *admissionStore) ActiveRun(_ context.Context, sessionID session.ID) (session.Run, error) {
	for _, run := range s.runs {
		if run.SessionID == sessionID && !run.Terminal() {
			return run, nil
		}
	}
	return session.Run{}, session.ErrNotFound
}

func (s *admissionStore) ListUnfinishedRuns(context.Context) ([]session.Run, error) {
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

func (s *admissionStore) FinishRun(_ context.Context, run session.Run) error {
	if _, ok := s.runs[run.ID]; !ok {
		return session.ErrNotFound
	}
	s.runs[run.ID] = run
	return nil
}

func (s *admissionStore) AppendMessage(_ context.Context, message session.Message) (session.Message, error) {
	if existing, ok := s.messages[message.ID]; ok {
		if existing.Role != message.Role {
			return session.Message{}, session.ErrConflict
		}
		return existing, nil
	}
	s.messages[message.ID] = message
	return message, nil
}

func (s *admissionStore) AppendPart(_ context.Context, part session.Part) (session.Part, error) {
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
	if _, ok := s.parts[part.ID]; !ok {
		return session.ErrNotFound
	}
	s.parts[part.ID] = part
	return nil
}

func (s *admissionStore) ListMessages(_ context.Context, sessionID session.ID, _ session.ReplayCursor) (session.ReplayBatch, error) {
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

func (s *admissionStore) AppendEvent(_ context.Context, event session.EventRecord) (session.EventRecord, error) {
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
	s.events[event.ID] = event
	return event, nil
}

func cloneMap[K comparable, V any](src map[K]V) map[K]V {
	dst := make(map[K]V, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func (s *admissionStore) ListEvents(_ context.Context, sessionID session.ID, _ session.EventCursor) (session.EventBatch, error) {
	var events []session.EventRecord
	for _, event := range s.events {
		if event.SessionID == sessionID {
			events = append(events, event)
		}
	}
	return session.EventBatch{Events: events}, nil
}

func (s *admissionStore) CreateToolCall(_ context.Context, request session.CreateToolCallRequest) (session.ToolTransitionResult, error) {
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
	if request.RequestPart.ID == "" || request.RequestPart.ID != call.RequestPartID || request.RequestPart.Kind != session.PartToolCall {
		return session.ToolTransitionResult{}, session.ErrConflict
	}
	if _, err := s.AppendPart(context.Background(), request.RequestPart); err != nil {
		return session.ToolTransitionResult{}, err
	}
	s.toolCalls[call.ID] = call
	s.events[event.ID] = event
	return session.ToolTransitionResult{Call: call, Event: event}, nil
}
func (s *admissionStore) GetToolCall(_ context.Context, id session.ToolCallID) (session.ToolCall, error) {
	call, ok := s.toolCalls[id]
	if !ok {
		return session.ToolCall{}, session.ErrNotFound
	}
	return call, nil
}
func (s *admissionStore) ListUnfinishedToolCalls(_ context.Context, runID session.RunID) ([]session.ToolCall, error) {
	var calls []session.ToolCall
	for _, call := range s.toolCalls {
		if call.RunID == runID && !session.TerminalToolCall(call.Status) {
			calls = append(calls, call)
		}
	}
	return calls, nil
}
func (s *admissionStore) ClaimToolCall(_ context.Context, request session.ClaimToolCallRequest) (session.ToolTransitionResult, error) {
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
	s.events[event.ID] = event
	return session.ToolTransitionResult{Call: call, Event: event}, nil
}
func (s *admissionStore) SettleToolCall(_ context.Context, request session.SettleToolCallRequest) (session.ToolTransitionResult, error) {
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
	s.events[event.ID] = event
	return session.ToolTransitionResult{Call: terminal, Event: event}, nil
}
func (s *admissionStore) StartContextEpoch(_ context.Context, epoch session.ContextEpoch) (session.ContextEpoch, error) {
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
	if _, ok := s.epochs[epoch.ID]; !ok {
		return session.ErrNotFound
	}
	s.epochs[epoch.ID] = epoch
	return nil
}
func (s *admissionStore) ListContextEpochs(_ context.Context, sessionID session.ID) ([]session.ContextEpoch, error) {
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
	tx := s.clone()
	if err := fn(ctx, &fakeExecutionStore{admissionStore: tx, fence: s.fence}); err != nil {
		return err
	}
	s.sessions = tx.sessions
	s.runs = tx.runs
	s.messages = tx.messages
	s.finalized = tx.finalized
	s.parts = tx.parts
	s.events = tx.events
	s.toolCalls = tx.toolCalls
	s.epochs = tx.epochs
	s.modelRequests = tx.modelRequests
	s.admissions = tx.admissions
	return nil
}

func (s *fakeExecutionStore) valid() bool {
	run, ok := s.runs[s.fence.RunID]
	return ok && !run.Terminal() && run.ClaimToken == s.fence.ClaimToken
}

func (s *fakeExecutionStore) SetSessionTitle(ctx context.Context, request session.SessionTitleRequest) (session.SessionTitleResult, error) {
	if !s.valid() || s.runs[s.fence.RunID].SessionID != request.SessionID {
		return session.SessionTitleResult{}, session.ErrConflict
	}
	return s.admissionStore.SetSessionTitle(ctx, request)
}

func (s *fakeExecutionStore) StartRun(_ context.Context, startedAt time.Time) (session.Run, error) {
	if !s.valid() {
		return session.Run{}, session.ErrConflict
	}
	run := s.runs[s.fence.RunID]
	run.Status = session.RunRunning
	run.StartedAt = startedAt
	s.runs[run.ID] = run
	return run, nil
}

func (s *fakeExecutionStore) RecordAdmission(_ context.Context, record session.AdmissionRecord) error {
	if !s.valid() || session.ValidateAdmissionRecord(record) != nil {
		return session.ErrAdmissionInvalid
	}
	r := record.Receipt
	run, runOK := s.runs[r.RunID]
	user, userOK := s.messages[r.UserMessageID]
	assistant, assistantOK := s.messages[r.AssistantMessageID]
	if !runOK || !userOK || !assistantOK || r.SessionID != run.SessionID || r.RunID != s.fence.RunID || user.SessionID != r.SessionID || user.RunID != r.RunID || user.Role != session.RoleUser || assistant.SessionID != r.SessionID || assistant.RunID != r.RunID || assistant.Role != session.RoleAssistant {
		return session.ErrAdmissionInvalid
	}
	key := admissionStoreKey(r.SessionID, r.Key)
	if _, exists := s.admissions[key]; exists {
		return session.AdmissionConflictError{}
	}
	s.admissions[key] = record
	return nil
}

func (s *fakeExecutionStore) RenewRunLease(_ context.Context, duration time.Duration) (session.Run, error) {
	if !s.valid() {
		return session.Run{}, session.ErrConflict
	}
	run := s.runs[s.fence.RunID]
	run.LeaseUntil = time.Now().UTC().Add(duration)
	s.runs[run.ID] = run
	return run, nil
}

func (s *fakeExecutionStore) SettleRun(ctx context.Context, request session.SettleRunRequest) (session.RunSettlementResult, error) {
	if !s.valid() {
		return session.RunSettlementResult{}, session.ErrConflict
	}
	for _, call := range s.toolCalls {
		if call.RunID == s.fence.RunID && !session.TerminalToolCall(call.Status) {
			return session.RunSettlementResult{}, session.ErrConflict
		}
	}
	run, err := session.ApplyRunSettlement(s.runs[s.fence.RunID], request.Settlement)
	if err != nil {
		return session.RunSettlementResult{}, err
	}
	event, err := session.RunSettlementRecord(run, request.Event)
	if err != nil {
		return session.RunSettlementResult{}, err
	}
	if err := s.FinishRun(ctx, run); err != nil {
		return session.RunSettlementResult{}, err
	}
	record, err := s.AppendEvent(ctx, event)
	return session.RunSettlementResult{Run: run, Event: record}, err
}

func (s *fakeExecutionStore) ClaimToolCall(ctx context.Context, request session.ClaimToolCallRequest) (session.ToolTransitionResult, error) {
	if !s.valid() {
		return session.ToolTransitionResult{}, session.ErrConflict
	}
	claimed, err := s.admissionStore.ClaimToolCall(ctx, request)
	if err != nil {
		return session.ToolTransitionResult{}, err
	}
	claimed.Call.LeaseUntil = time.Now().UTC().Add(request.LeaseDuration)
	s.toolCalls[claimed.Call.ID] = claimed.Call
	return claimed, nil
}

func (s *fakeExecutionStore) CreateModelRequest(_ context.Context, record session.ModelRequestRecord) (session.ModelRequestRecord, error) {
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
	record, ok := s.modelRequests[id]
	if !ok {
		return session.ModelRequestRecord{}, session.ErrNotFound
	}
	return record, nil
}

func (s *admissionStore) ListModelRequests(_ context.Context, runID session.RunID, cursor session.ModelRequestCursor) (session.ModelRequestBatch, error) {
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
	m, ok := s.messages[id]
	if !ok || !s.valid() || m.RunID != s.fence.RunID || m.SessionID != s.runs[s.fence.RunID].SessionID || m.Role != session.RoleAssistant {
		return session.ErrConflict
	}
	s.finalized[id] = true
	return nil
}
