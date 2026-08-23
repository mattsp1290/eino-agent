package runtime

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func TestAdmitPersistsDurableRecordsBeforeExecution(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	sink := &capturingSink{}
	now := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	admitter := Admitter{Store: store, Events: sink, Hooks: []Hook{capturingHook{}}, Clock: func() time.Time { return now }}
	request := admissionRequest()
	admitted, err := admitter.Admit(context.Background(), request)
	if err != nil {
		t.Fatalf("Admit error = %v", err)
	}
	if admitted.Session.ID != "session-1" || admitted.Run.ID != "run-1" || admitted.AssistantMessage.ID != "assistant-1" {
		t.Fatalf("admitted identity = %+v", admitted)
	}
	if _, err := store.GetSession(context.Background(), "session-1"); err != nil {
		t.Fatalf("session was not durable: %v", err)
	}
	if _, err := store.GetRun(context.Background(), "run-1"); err != nil {
		t.Fatalf("run was not durable: %v", err)
	}
	batch, err := store.ListMessages(context.Background(), "session-1", session.ReplayCursor{Limit: 10})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(batch.Messages) != 1 || batch.Messages[0].Role != session.RoleAssistant {
		t.Fatalf("messages = %#v", batch.Messages)
	}
	events, err := store.ListEvents(context.Background(), "session-1", session.EventCursor{Limit: 10})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events.Events) != 1 || events.Events[0].Kind != string(EventRunStarted) {
		t.Fatalf("events = %#v", events.Events)
	}
	epochs, err := store.ListContextEpochs(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("list context epochs: %v", err)
	}
	if len(epochs) != 1 || epochs[0].ID != "epoch-1" || epochs[0].Trigger != "turn" || epochs[0].Reason != "run_admission" {
		t.Fatalf("epochs = %#v", epochs)
	}
	if len(sink.events) != 1 || sink.events[0].Kind != EventRunStarted {
		t.Fatalf("emitted events = %#v", sink.events)
	}
	if sink.events[0].EventID != events.Events[0].ID {
		t.Fatalf("live event id = %q, want durable id %q", sink.events[0].EventID, events.Events[0].ID)
	}
	request.Config.Agent.Options["temperature"] = "changed"
	request.Input[0].Content = "changed"
	if admitted.Snapshot.Config.Agent.Options["temperature"] != "0.2" {
		t.Fatalf("snapshot config mutated: %#v", admitted.Snapshot.Config.Agent.Options)
	}
	if admitted.Snapshot.Messages[0].Content != "hello" {
		t.Fatalf("snapshot messages mutated: %#v", admitted.Snapshot.Messages[0])
	}
}

func TestAdmitRequiresContextEpochID(t *testing.T) {
	t.Parallel()

	request := admissionRequest()
	request.IDs.ContextEpochID = ""
	_, err := (Admitter{Store: newAdmissionStore()}).Admit(context.Background(), request)
	if !errors.Is(err, ErrInvalidAdmission) {
		t.Fatalf("Admit error = %v, want ErrInvalidAdmission", err)
	}
}

func TestRunAdmittedNotificationWaitsForLegacyBeforeRunSuccess(t *testing.T) {
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "admission-order", Artifact: extension.Artifact{Name: "admission-order", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	called := false
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, RunAdmittedPoint, extension.Registration{ID: "observe", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(context.Context, RunAdmittedNotice) error {
			called = true
			return nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	beforeRunErr := errors.New("before run failed")
	admitter := Admitter{Store: newAdmissionStore(), Hooks: []Hook{failingBeforeRunHook{err: beforeRunErr}}}
	admitter.Extensions = plan
	_, err = admitter.Admit(context.Background(), admissionRequest())
	if !errors.Is(err, beforeRunErr) || called {
		t.Fatalf("Admit error = %v, notification called = %t", err, called)
	}
}

func TestAdmitRejectsDuplicateActiveRun(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	sink := &capturingSink{}
	hook := &countingHook{}
	admitter := Admitter{Store: store, Events: sink, Hooks: []Hook{hook}, Clock: func() time.Time { return time.Unix(1, 0) }}
	request := admissionRequest()
	if _, err := admitter.Admit(context.Background(), request); err != nil {
		t.Fatalf("first Admit error = %v", err)
	}
	again, err := admitter.Admit(context.Background(), request)
	if err == nil {
		if again.Run.ID != "run-1" || again.AssistantMessage.ID != "assistant-1" {
			t.Fatalf("idempotent duplicate identity = %+v", again)
		}
		if !again.AlreadyAdmitted {
			t.Fatal("duplicate admission did not report AlreadyAdmitted")
		}
		if hook.calls != 1 || len(sink.events) != 1 {
			t.Fatalf("duplicate replayed side effects: hook=%d sink=%d", hook.calls, len(sink.events))
		}
		return
	}
	if !errors.Is(err, session.ErrSessionBusy) && !errors.Is(err, session.ErrConflict) {
		t.Fatalf("duplicate Admit error = %v, want idempotent or explicit busy/conflict", err)
	}
}

func TestAdmitRejectsIdempotentExtensionPlanMismatch(t *testing.T) {
	store := newAdmissionStore()
	admitter := Admitter{Store: store}
	request := admissionRequest()
	request.ExtensionPlan = session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanStrict, Entries: []session.ExtensionPlanEntry{{InstanceID: "first", Kind: session.ExtensionHandlers, Required: true}}}
	request.ExtensionPlan.Fingerprint, _ = session.FingerprintExtensionPlan(request.ExtensionPlan)
	if _, err := admitter.Admit(context.Background(), request); err != nil {
		t.Fatalf("first Admit error = %v", err)
	}

	retry := request
	retry.ExtensionPlan = session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanStrict, Entries: []session.ExtensionPlanEntry{{InstanceID: "second", Kind: session.ExtensionHandlers, Required: true}}}
	retry.ExtensionPlan.Fingerprint, _ = session.FingerprintExtensionPlan(retry.ExtensionPlan)
	if _, err := admitter.Admit(context.Background(), retry); !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("mismatched retry error = %v, want ErrExtensionPlanMismatch", err)
	}
}

func TestAdmitRejectsZeroPersistedPlanOnRetry(t *testing.T) {
	store := newAdmissionStore()
	admitter := Admitter{Store: store}
	request := admissionRequest()
	if _, err := admitter.Admit(context.Background(), request); err != nil {
		t.Fatalf("first Admit error = %v", err)
	}

	stored := store.runs[request.IDs.RunID]
	stored.ExtensionPlan = session.ExtensionPlanDescriptor{}
	store.runs[stored.ID] = stored

	if _, err := admitter.Admit(context.Background(), request); !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("zero persisted retry error = %v, want ErrExtensionPlanMismatch", err)
	}
}

func TestMatchingExtensionPlansRejectsZeroAndLegacyDescriptors(t *testing.T) {
	strict := emptyExtensionPlanDescriptor()
	for name, descriptor := range map[string]session.ExtensionPlanDescriptor{
		"zero":           {},
		"legacy":         {SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanLegacy},
		"partial legacy": {SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanPartialLegacy},
	} {
		descriptor.Fingerprint, _ = session.FingerprintExtensionPlan(descriptor)
		t.Run(name+" persisted", func(t *testing.T) {
			if err := validateMatchingExtensionPlans(descriptor, strict); !errors.Is(err, ErrExtensionPlanMismatch) {
				t.Fatalf("error = %v, want ErrExtensionPlanMismatch", err)
			}
		})
		t.Run(name+" requested", func(t *testing.T) {
			if err := validateMatchingExtensionPlans(strict, descriptor); !errors.Is(err, ErrExtensionPlanMismatch) {
				t.Fatalf("error = %v, want ErrExtensionPlanMismatch", err)
			}
		})
	}
}

func TestMatchingExtensionPlansRejectsStaleFingerprint(t *testing.T) {
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanStrict, Entries: []session.ExtensionPlanEntry{{InstanceID: "original", Kind: session.ExtensionHandlers, Required: true}}}
	descriptor.Fingerprint, _ = session.FingerprintExtensionPlan(descriptor)
	corrupt := descriptor.Clone()
	corrupt.Entries[0].InstanceID = "changed"
	if err := validateMatchingExtensionPlans(corrupt, descriptor); !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("persisted stale fingerprint error = %v", err)
	}
	if err := validateMatchingExtensionPlans(descriptor, corrupt); !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("requested stale fingerprint error = %v", err)
	}
}

func TestMatchingExtensionPlansUsesSchemaCanonicalization(t *testing.T) {
	persisted := session.ExtensionPlanDescriptor{SchemaVersion: 1, Mode: session.PlanStrict, Entries: []session.ExtensionPlanEntry{{InstanceID: "ordered", Kind: session.ExtensionPrompt, Required: true, Order: 10}}}
	requested := persisted.Clone()
	requested.Entries[0].Order = 20
	persisted.Fingerprint, _ = session.FingerprintExtensionPlan(persisted)
	requested.Fingerprint, _ = session.FingerprintExtensionPlan(requested)
	if err := validateMatchingExtensionPlans(persisted, requested); err != nil {
		t.Fatalf("schema-v1 canonical plans did not match: %v", err)
	}
}

func TestAdmitRollsBackDurableRecordsWhenTransactionalAdmissionFails(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	store.appendEventErr = errors.New("append event failed")
	admitter := Admitter{Store: store}
	_, err := admitter.Admit(context.Background(), admissionRequest())
	if !errors.Is(err, store.appendEventErr) {
		t.Fatalf("Admit error = %v, want append event failure", err)
	}
	if _, err := store.GetRun(context.Background(), "run-1"); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("run leaked after rollback err = %v, want ErrNotFound", err)
	}
	batch, err := store.ListMessages(context.Background(), "session-1", session.ReplayCursor{Limit: 10})
	if err != nil {
		t.Fatalf("ListMessages error = %v", err)
	}
	if len(batch.Messages) != 0 {
		t.Fatalf("messages leaked after rollback: %#v", batch.Messages)
	}
}

func TestFailedExecutionDoesNotEraseAdmittedHistory(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	admitter := Admitter{Store: store, Clock: func() time.Time { return time.Unix(1, 0) }}
	admitted, err := admitter.Admit(context.Background(), admissionRequest())
	if err != nil {
		t.Fatalf("Admit error = %v", err)
	}
	failed := admitted.Run
	failed.Status = session.RunFailed
	failed.Error = "provider failed"
	failed.FinishedAt = failed.CreatedAt.Add(time.Second)
	if err := store.FinishRun(context.Background(), failed); err != nil {
		t.Fatalf("FinishRun error = %v", err)
	}
	batch, err := store.ListMessages(context.Background(), admitted.Session.ID, session.ReplayCursor{Limit: 10})
	if err != nil {
		t.Fatalf("ListMessages error = %v", err)
	}
	if len(batch.Messages) != 1 || batch.Messages[0].ID != admitted.AssistantMessage.ID {
		t.Fatalf("history after failure = %#v", batch.Messages)
	}
	gotRun, err := store.GetRun(context.Background(), admitted.Run.ID)
	if err != nil {
		t.Fatalf("GetRun error = %v", err)
	}
	if gotRun.Status != session.RunFailed || gotRun.Error != "provider failed" {
		t.Fatalf("run after failure = %+v", gotRun)
	}
}

func TestAdmitFallsBackToConfigModelIdentity(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	request := admissionRequest()
	request.Model = model.Resolved{}
	admitted, err := (Admitter{Store: store}).Admit(context.Background(), request)
	if err != nil {
		t.Fatalf("Admit error = %v", err)
	}
	if admitted.Run.ProviderID != "openai" || admitted.Run.ModelID != "gpt-4.1" {
		t.Fatalf("run model identity = %q/%q", admitted.Run.ProviderID, admitted.Run.ModelID)
	}
}

func TestBeforeRunHookFailureDoesNotEraseAdmission(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	errHook := errors.New("hook failed")
	admitter := Admitter{
		Store: store,
		Hooks: []Hook{failingBeforeRunHook{err: errHook}},
	}
	_, err := admitter.Admit(context.Background(), admissionRequest())
	if !errors.Is(err, errHook) {
		t.Fatalf("Admit error = %v, want hook error", err)
	}
	if _, err := store.GetRun(context.Background(), "run-1"); err != nil {
		t.Fatalf("run missing after hook failure: %v", err)
	}
	batch, err := store.ListMessages(context.Background(), "session-1", session.ReplayCursor{Limit: 10})
	if err != nil {
		t.Fatalf("ListMessages error = %v", err)
	}
	if len(batch.Messages) != 1 {
		t.Fatalf("messages after hook failure = %#v", batch.Messages)
	}
}

func TestLiveSinkFailureDoesNotFailAdmission(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	errSink := errors.New("sink failed")
	admitter := Admitter{Store: store, Events: failingSink{err: errSink}}
	admitted, err := admitter.Admit(context.Background(), admissionRequest())
	if err != nil {
		t.Fatalf("Admit error = %v, want success despite live sink failure", err)
	}
	if admitted.Run.ID != "run-1" {
		t.Fatalf("admitted run = %+v", admitted.Run)
	}
}

func admissionRequest() AdmissionRequest {
	selection := model.Selection{ProviderID: "openai", ModelID: "gpt-4.1"}
	return AdmissionRequest{
		IDs: AdmissionIDs{
			SessionID:          "session-1",
			RunID:              "run-1",
			AssistantMessageID: "assistant-1",
			ContextEpochID:     "epoch-1",
			EventID:            "event-1",
		},
		ParentMessageID: "user-1",
		Config: config.Snapshot{
			Agent: config.Agent{
				Name:         "default",
				Model:        selection,
				SystemPrompt: "system",
				Options:      map[string]string{"temperature": "0.2"},
			},
			Model: selection,
			Metadata: map[string]string{
				"workspace_id":   "workspace-1",
				"workspace_root": "/workspace",
			},
			Plugins: []config.Plugin{{
				Name:    "plugin",
				Version: "1.0.0",
				Hash:    "abc",
				Source:  "test",
			}},
		},
		Model: model.Resolved{
			Provider: model.Provider{ID: "openai"},
			Model:    model.Descriptor{ID: "gpt-4.1", ProviderID: "openai"},
		},
		Input:         []*einoschema.Message{{Role: "user", Content: "hello"}},
		OwnerID:       "owner-1",
		Metadata:      map[string]string{"request": "admission"},
		ExtensionPlan: emptyExtensionPlanDescriptor(),
	}
}

type capturingSink struct {
	events []Event
}

func (s *capturingSink) Emit(_ context.Context, event Event) error {
	s.events = append(s.events, event)
	return nil
}

type failingSink struct {
	err error
}

func (s failingSink) Emit(context.Context, Event) error {
	return s.err
}

type capturingHook struct{}

func (capturingHook) BeforeRun(context.Context, session.Run) error { return nil }
func (capturingHook) BeforeTurn(context.Context, TurnSnapshot) (TurnSnapshot, error) {
	return TurnSnapshot{}, nil
}
func (capturingHook) AfterTurn(context.Context, TurnSnapshot, Result) error { return nil }
func (capturingHook) AfterRun(context.Context, Result) error                { return nil }

type countingHook struct {
	calls int
}

func (h *countingHook) BeforeRun(context.Context, session.Run) error {
	h.calls++
	return nil
}
func (h *countingHook) BeforeTurn(context.Context, TurnSnapshot) (TurnSnapshot, error) {
	return TurnSnapshot{}, nil
}
func (h *countingHook) AfterTurn(context.Context, TurnSnapshot, Result) error { return nil }
func (h *countingHook) AfterRun(context.Context, Result) error                { return nil }

type failingBeforeRunHook struct {
	err error
}

func (h failingBeforeRunHook) BeforeRun(context.Context, session.Run) error { return h.err }
func (h failingBeforeRunHook) BeforeTurn(context.Context, TurnSnapshot) (TurnSnapshot, error) {
	return TurnSnapshot{}, nil
}
func (h failingBeforeRunHook) AfterTurn(context.Context, TurnSnapshot, Result) error {
	return nil
}
func (h failingBeforeRunHook) AfterRun(context.Context, Result) error { return nil }

type admissionStore struct {
	sessions          map[session.ID]session.Session
	runs              map[session.RunID]session.Run
	messages          map[session.MessageID]session.Message
	parts             map[session.PartID]session.Part
	events            map[session.EventID]session.EventRecord
	toolCalls         map[session.ToolCallID]session.ToolCall
	epochs            map[session.EpochID]session.ContextEpoch
	appendEventErr    error
	finishToolCallErr error
}

func newAdmissionStore() *admissionStore {
	return &admissionStore{
		sessions:  map[session.ID]session.Session{},
		runs:      map[session.RunID]session.Run{},
		messages:  map[session.MessageID]session.Message{},
		parts:     map[session.PartID]session.Part{},
		events:    map[session.EventID]session.EventRecord{},
		toolCalls: map[session.ToolCallID]session.ToolCall{},
		epochs:    map[session.EpochID]session.ContextEpoch{},
	}
}

func (s *admissionStore) WithinTx(ctx context.Context, fn func(context.Context, session.Tx) error) error {
	tx := s.clone()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	s.sessions = tx.sessions
	s.runs = tx.runs
	s.messages = tx.messages
	s.parts = tx.parts
	s.events = tx.events
	s.toolCalls = tx.toolCalls
	s.epochs = tx.epochs
	return nil
}

func (s *admissionStore) clone() *admissionStore {
	return &admissionStore{
		sessions:          cloneMap(s.sessions),
		runs:              cloneMap(s.runs),
		messages:          cloneMap(s.messages),
		parts:             cloneMap(s.parts),
		events:            cloneMap(s.events),
		toolCalls:         cloneMap(s.toolCalls),
		epochs:            cloneMap(s.epochs),
		appendEventErr:    s.appendEventErr,
		finishToolCallErr: s.finishToolCallErr,
	}
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

func (s *admissionStore) AdmitRun(_ context.Context, run session.Run) (session.Run, error) {
	if existing, ok := s.runs[run.ID]; ok {
		if sameRun(existing, run) {
			return existing, nil
		}
		return session.Run{}, session.ErrConflict
	}
	for _, existing := range s.runs {
		if existing.SessionID == run.SessionID && !existing.Terminal() {
			return session.Run{}, session.ErrSessionBusy
		}
	}
	s.runs[run.ID] = run
	return run, nil
}

func (s *admissionStore) GetRun(_ context.Context, id session.RunID) (session.Run, error) {
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

func (s *admissionStore) CreateToolCall(_ context.Context, call session.ToolCall) (session.ToolCall, error) {
	if existing, ok := s.toolCalls[call.ID]; ok {
		if existing.Name != call.Name {
			return session.ToolCall{}, session.ErrConflict
		}
		return existing, nil
	}
	s.toolCalls[call.ID] = call
	return call, nil
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
func (s *admissionStore) ClaimToolCall(_ context.Context, call session.ToolCall) (session.ToolCall, error) {
	if _, ok := s.toolCalls[call.ID]; !ok {
		return session.ToolCall{}, session.ErrNotFound
	}
	s.toolCalls[call.ID] = call
	return call, nil
}
func (s *admissionStore) FinishToolCall(_ context.Context, call session.ToolCall) error {
	if s.finishToolCallErr != nil {
		return s.finishToolCallErr
	}
	if _, ok := s.toolCalls[call.ID]; !ok {
		return session.ErrNotFound
	}
	s.toolCalls[call.ID] = call
	return nil
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

func sameRun(left session.Run, right session.Run) bool {
	return left.ID == right.ID &&
		left.SessionID == right.SessionID &&
		left.ParentMsgID == right.ParentMsgID &&
		left.OwnerID == right.OwnerID &&
		left.Agent == right.Agent &&
		left.ProviderID == right.ProviderID &&
		left.ModelID == right.ModelID &&
		left.ContextEpoch == right.ContextEpoch &&
		left.Status == right.Status
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
