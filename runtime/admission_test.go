package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
	admitter := Admitter{Store: store, Events: sink, Clock: func() time.Time { return now }}
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

func TestAdmitPersistsResolvedWorkspaceAcrossSymlinkRetarget(t *testing.T) {
	parent := t.TempDir()
	first := filepath.Join(parent, "first")
	second := filepath.Join(parent, "second")
	if err := os.Mkdir(first, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(second, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "workspace")
	if err := os.Symlink(first, alias); err != nil {
		t.Fatal(err)
	}
	request := admissionRequest()
	request.Config.Metadata["workspace_root"] = alias
	admitted, err := (Admitter{Store: newAdmissionStore()}).Admit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(first)
	if err != nil {
		t.Fatal(err)
	}
	if admitted.Run.Config["workspace_root"] != resolved || admitted.Snapshot.Config.Metadata["workspace_root"] != resolved {
		t.Fatalf("workspace identity = run %q snapshot %q, want %q", admitted.Run.Config["workspace_root"], admitted.Snapshot.Config.Metadata["workspace_root"], resolved)
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(second, alias); err != nil {
		t.Fatal(err)
	}
	if admitted.Run.Config["workspace_root"] != resolved {
		t.Fatalf("retarget changed persisted root to %q", admitted.Run.Config["workspace_root"])
	}
}

func TestAdmitRejectsNonexistentWorkspace(t *testing.T) {
	request := admissionRequest()
	request.Config.Metadata["workspace_root"] = filepath.Join(t.TempDir(), "missing")
	if _, err := (Admitter{Store: newAdmissionStore()}).Admit(context.Background(), request); err == nil {
		t.Fatal("expected nonexistent workspace rejection")
	}
}

func TestAdmitRejectsDuplicateActiveRun(t *testing.T) {
	t.Parallel()

	store := newAdmissionStore()
	sink := &capturingSink{}
	admitter := Admitter{Store: store, Events: sink, Clock: func() time.Time { return time.Unix(1, 0) }}
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
		if len(sink.events) != 1 {
			t.Fatalf("duplicate replayed side effects: sink=%d", len(sink.events))
		}
		return
	}
	if !errors.Is(err, session.ErrSessionBusy) && !errors.Is(err, session.ErrConflict) {
		t.Fatalf("duplicate Admit error = %v, want idempotent or explicit busy/conflict", err)
	}
}

func TestAdmitCloneFailureHasNoDurableOrLiveSideEffects(t *testing.T) {
	store := newAdmissionStore()
	sink := &capturingSink{}
	request := admissionRequest()
	request.Input[0].Extra = map[string]any{"unsupported": make(chan int)}
	_, err := (Admitter{Store: store, Events: sink}).Admit(context.Background(), request)
	if !errors.Is(err, ErrInvalidAdmission) {
		t.Fatalf("Admit error = %v, want ErrInvalidAdmission", err)
	}
	if len(store.sessions) != 0 || len(store.runs) != 0 || len(store.messages) != 0 || len(store.events) != 0 || len(store.epochs) != 0 || len(sink.events) != 0 {
		t.Fatalf("clone failure mutated state: sessions=%d runs=%d messages=%d events=%d epochs=%d live=%d", len(store.sessions), len(store.runs), len(store.messages), len(store.events), len(store.epochs), len(sink.events))
	}
}

func TestAdmitRejectsMismatchedIdempotentRequestState(t *testing.T) {
	tests := map[string]func(*AdmissionRequest){
		"session":           func(r *AdmissionRequest) { r.IDs.SessionID = "other-session" },
		"epoch":             func(r *AdmissionRequest) { r.IDs.ContextEpochID = "other-epoch" },
		"assistant message": func(r *AdmissionRequest) { r.IDs.AssistantMessageID = "other-assistant" },
		"parent message":    func(r *AdmissionRequest) { r.ParentMessageID = "other-parent" },
		"config":            func(r *AdmissionRequest) { r.Config.Agent.Mode = "other-mode" },
		"model":             func(r *AdmissionRequest) { r.Model.Model.ID = "other-model" },
		"input":             func(r *AdmissionRequest) { r.Input = []*einoschema.Message{{Role: "user", Content: "other-input"}} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			store := newAdmissionStore()
			admitter := Admitter{Store: store}
			request := admissionRequest()
			if _, err := admitter.Admit(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			mutate(&request)
			if _, err := admitter.Admit(context.Background(), request); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("retry error = %v, want ErrConflict", err)
			}
		})
	}
}

func TestAdmitRejectsIdempotentExtensionPlanMismatch(t *testing.T) {
	store := newAdmissionStore()
	admitter := Admitter{Store: store}
	request := admissionRequest()
	request.ExtensionPlan = session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Entries: []session.ExtensionPlanEntry{testHandlerPlanEntry("first")}}
	request.ExtensionPlan.Fingerprint, _ = session.FingerprintExtensionPlan(request.ExtensionPlan)
	if _, err := admitter.Admit(context.Background(), request); err != nil {
		t.Fatalf("first Admit error = %v", err)
	}

	retry := request
	retry.ExtensionPlan = session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Entries: []session.ExtensionPlanEntry{testHandlerPlanEntry("second")}}
	retry.ExtensionPlan.Fingerprint, _ = session.FingerprintExtensionPlan(retry.ExtensionPlan)
	if _, err := admitter.Admit(context.Background(), retry); !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("mismatched retry error = %v, want ErrExtensionPlanMismatch", err)
	}
}

func testHandlerPlanEntry(instance string) session.ExtensionPlanEntry {
	return session.ExtensionPlanEntry{
		InstanceID: instance,
		Artifact:   session.ArtifactIdentity{Name: instance, Version: "1", Hash: instance + "-hash", ConfigHash: instance + "-config", SourceKind: string(extension.SourceNative)},
		Handlers:   &session.HandlerPlanIdentity{Registrations: []session.RegistrationIdentity{{ID: "handler", Contract: "test/handler", Version: "1", Scope: session.ExtensionScope{Kind: string(extension.ScopeGlobal)}, Kind: session.HandlerNotification}}},
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

func TestMatchingExtensionPlansRejectsZeroAndWrongSchemaDescriptors(t *testing.T) {
	current := emptyExtensionPlanDescriptor()
	for name, descriptor := range map[string]session.ExtensionPlanDescriptor{
		"zero":         {},
		"wrong schema": {SchemaVersion: session.ExtensionPlanSchemaVersion - 1},
	} {
		descriptor.Fingerprint, _ = session.FingerprintExtensionPlan(descriptor)
		t.Run(name+" persisted", func(t *testing.T) {
			if err := validateMatchingExtensionPlans(descriptor, current); !errors.Is(err, ErrExtensionPlanMismatch) {
				t.Fatalf("error = %v, want ErrExtensionPlanMismatch", err)
			}
		})
		t.Run(name+" requested", func(t *testing.T) {
			if err := validateMatchingExtensionPlans(current, descriptor); !errors.Is(err, ErrExtensionPlanMismatch) {
				t.Fatalf("error = %v, want ErrExtensionPlanMismatch", err)
			}
		})
	}
}

func TestMatchingExtensionPlansRejectsStaleFingerprint(t *testing.T) {
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Entries: []session.ExtensionPlanEntry{testHandlerPlanEntry("original")}}
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
			RunClaimToken:      "claim-run-1",
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
				"workspace_root": os.TempDir(),
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
		LeaseDuration: time.Minute,
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

type admissionStore struct {
	sessions          map[session.ID]session.Session
	runs              map[session.RunID]session.Run
	messages          map[session.MessageID]session.Message
	parts             map[session.PartID]session.Part
	events            map[session.EventID]session.EventRecord
	toolCalls         map[session.ToolCallID]session.ToolCall
	epochs            map[session.EpochID]session.ContextEpoch
	modelRequests     map[session.ModelRequestID]session.ModelRequestRecord
	appendEventErr    error
	settleToolCallErr error
}

func newAdmissionStore() *admissionStore {
	return &admissionStore{
		sessions:      map[session.ID]session.Session{},
		runs:          map[session.RunID]session.Run{},
		messages:      map[session.MessageID]session.Message{},
		parts:         map[session.PartID]session.Part{},
		events:        map[session.EventID]session.EventRecord{},
		toolCalls:     map[session.ToolCallID]session.ToolCall{},
		epochs:        map[session.EpochID]session.ContextEpoch{},
		modelRequests: map[session.ModelRequestID]session.ModelRequestRecord{},
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
	s.parts = tx.parts
	s.events = tx.events
	s.toolCalls = tx.toolCalls
	s.epochs = tx.epochs
	s.modelRequests = tx.modelRequests
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
		modelRequests:     cloneMap(s.modelRequests),
		appendEventErr:    s.appendEventErr,
		settleToolCallErr: s.settleToolCallErr,
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

func (s *admissionStore) AdmitRun(_ context.Context, run session.Run, leaseDuration time.Duration) (session.Run, error) {
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
func (s *admissionStore) SettleToolCall(_ context.Context, settlement session.ToolSettlement) error {
	if s.settleToolCallErr != nil {
		return s.settleToolCallErr
	}
	call, ok := s.toolCalls[settlement.ID]
	if !ok {
		return session.ErrNotFound
	}
	terminal, err := settlement.Apply(call)
	if err != nil {
		return err
	}
	s.toolCalls[terminal.ID] = terminal
	s.messages[settlement.ResultMessage.ID] = settlement.ResultMessage
	s.parts[settlement.ResultPart.ID] = settlement.ResultPart
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
		left.ClaimToken == right.ClaimToken &&
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
	s.parts = tx.parts
	s.events = tx.events
	s.toolCalls = tx.toolCalls
	s.epochs = tx.epochs
	s.modelRequests = tx.modelRequests
	return nil
}

func (s *fakeExecutionStore) valid() bool {
	run, ok := s.runs[s.fence.RunID]
	return ok && !run.Terminal() && run.ClaimToken == s.fence.ClaimToken
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

func (s *fakeExecutionStore) RenewRunLease(_ context.Context, duration time.Duration) (session.Run, error) {
	if !s.valid() {
		return session.Run{}, session.ErrConflict
	}
	run := s.runs[s.fence.RunID]
	run.LeaseUntil = time.Now().UTC().Add(duration)
	s.runs[run.ID] = run
	return run, nil
}

func (s *fakeExecutionStore) SettleRun(ctx context.Context, run session.Run, event *session.EventRecord) error {
	if !s.valid() || run.ID != s.fence.RunID || run.ClaimToken != s.fence.ClaimToken {
		return session.ErrConflict
	}
	if err := s.FinishRun(ctx, run); err != nil {
		return err
	}
	if event != nil {
		_, err := s.AppendEvent(ctx, *event)
		return err
	}
	return nil
}

func (s *fakeExecutionStore) ClaimToolCall(ctx context.Context, call session.ToolCall, duration time.Duration) (session.ToolCall, error) {
	if !s.valid() {
		return session.ToolCall{}, session.ErrConflict
	}
	call.LeaseUntil = time.Now().UTC().Add(duration)
	return s.admissionStore.ClaimToolCall(ctx, call)
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
