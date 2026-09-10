package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	storepkg "github.com/mattsp1290/eino-agent/store"
)

type countingResolver struct {
	calls atomic.Int32
}

type sequencedLookupStore struct {
	session.Store
	record session.AdmissionRecord
	err    error
	calls  atomic.Int32
}

func (s *sequencedLookupStore) LookupAdmission(context.Context, session.ID, string) (session.AdmissionRecord, error) {
	if s.calls.Add(1) == 1 {
		return session.AdmissionRecord{}, session.ErrNotFound
	}
	if s.err != nil {
		return session.AdmissionRecord{}, s.err
	}
	return s.record, nil
}

type failingPlanProvider struct{ err error }

func (p failingPlanProvider) AcquireRunPlan(context.Context, RunPlanRequest) (*RunPlan, error) {
	return nil, p.err
}
func (p failingPlanProvider) AcquireResumePlan(context.Context, ResumePlanRequest) (*RunPlan, error) {
	return nil, p.err
}

type invalidResolvedModel struct{}

func (invalidResolvedModel) Resolve(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
	return model.Resolved{Provider: model.Provider{ID: "wrong"}, Model: model.Descriptor{ID: "wrong", ProviderID: "wrong"}}, nil
}

func TestKeyedAdmissionRechecksAfterPlanAndResolvedValidationFailures(t *testing.T) {
	request := Request{SessionID: "recheck", AdmissionKey: "event-recheck", Message: UserMessage{Content: "hello"}, Config: keyedAdmissionConfig(t)}
	fingerprint, err := fingerprintAdmission(frozenRequest(request))
	if err != nil {
		t.Fatal(err)
	}
	record := session.AdmissionRecord{
		Receipt:            session.AdmissionReceipt{SessionID: request.SessionID, Key: request.AdmissionKey, RunID: "winner", UserMessageID: "user", AssistantMessageID: "assistant"},
		FingerprintVersion: session.AdmissionFingerprintVersion, Fingerprint: fingerprint,
	}
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{name: "plan", opts: []Option{WithRunPlanProvider(failingPlanProvider{err: errors.New("plan failed")})}},
		{name: "resolved validation", opts: []Option{WithModelResolver(invalidResolvedModel{})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := newAdmissionStore()
			store := &sequencedLookupStore{Store: base, record: record}
			opts := []Option{WithStore(store), WithModelResolver(&countingResolver{}), WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider())}
			orch := mustConfiguredOrchestrator(append(opts, tc.opts...)...)
			got, err := orch.Start(t.Context(), request)
			if err != nil || got.Disposition != AdmissionExisting || got.Receipt != record.Receipt {
				t.Fatalf("result=%+v err=%v", got, err)
			}
		})
	}
}

func TestKeyedAdmissionMasksFailedRecoveryLookup(t *testing.T) {
	request := Request{SessionID: "unknown", AdmissionKey: "event-unknown", Message: UserMessage{Content: "hello"}, Config: keyedAdmissionConfig(t)}
	store := &sequencedLookupStore{Store: newAdmissionStore(), err: errors.New("private driver details")}
	orch := mustConfiguredOrchestrator(
		WithStore(store), WithModelResolver(&countingResolver{}), WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(failingPlanProvider{err: errors.New("private plan details")}),
	)
	_, err := orch.Start(t.Context(), request)
	if !errors.Is(err, session.ErrAdmissionUnknown) || err.Error() != session.ErrAdmissionUnknown.Error() {
		t.Fatalf("error=%q", err)
	}
}

func TestKeyedAdmissionMapsAbsentAmbiguousCommitToUnknown(t *testing.T) {
	request := Request{SessionID: "ambiguous", AdmissionKey: "event-ambiguous"}
	orch := &StreamingOrchestrator{store: newAdmissionStore()}
	_, err := orch.resolveFailedAdmission(t.Context(), request, [32]byte{}, storepkg.MarkTransactionOutcomeUnknown(errors.New("commit ack lost")))
	if !errors.Is(err, session.ErrAdmissionUnknown) || err.Error() != session.ErrAdmissionUnknown.Error() {
		t.Fatalf("error=%q", err)
	}
}

func (r *countingResolver) Resolve(_ context.Context, selection model.Selection, _ model.Runtime) (model.Resolved, error) {
	r.calls.Add(1)
	return model.Resolved{
		Provider: model.Provider{ID: selection.ProviderID},
		Model:    model.Descriptor{ID: selection.ModelID, ProviderID: selection.ProviderID},
		Streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
		}),
	}, nil
}

func TestKeyedAdmissionReturnsReceiptWithoutSecondResolution(t *testing.T) {
	store := newAdmissionStore()
	resolver := &countingResolver{}
	orch := mustConfiguredOrchestrator(
		WithStore(store), WithModelResolver(resolver), WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
	)
	request := Request{SessionID: "keyed", AdmissionKey: "event-42", Message: UserMessage{Content: "hello"}, Config: keyedAdmissionConfig(t)}
	first, err := orch.Start(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Disposition != AdmissionNew || first.Handle == nil || first.Receipt.Key != request.AdmissionKey {
		t.Fatalf("first = %#v", first)
	}
	<-first.Handle.Done()

	duplicate, err := orch.Start(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Disposition != AdmissionExisting || duplicate.Handle != nil || duplicate.Receipt != first.Receipt {
		t.Fatalf("duplicate = %#v, first = %#v", duplicate, first)
	}
	if got := resolver.calls.Load(); got != 1 {
		t.Fatalf("resolver calls = %d, want 1", got)
	}
	lookup, err := orch.LookupAdmission(context.Background(), request.SessionID, request.AdmissionKey)
	if err != nil || lookup.Receipt != first.Receipt || lookup.RunStatus != session.RunCompleted {
		t.Fatalf("lookup = %#v, %v", lookup, err)
	}
}

func TestKeyedAdmissionConflictDoesNotExposePayload(t *testing.T) {
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	}))
	request := Request{SessionID: "keyed-conflict", AdmissionKey: "event-43", Message: UserMessage{Content: "first private prompt"}, Config: keyedAdmissionConfig(t)}
	first, err := orch.Start(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	<-first.Handle.Done()
	request.Message.Content = "second private prompt"
	_, err = orch.Start(context.Background(), request)
	if !errors.Is(err, session.ErrAdmissionConflict) {
		t.Fatalf("error = %v", err)
	}
	if got := err.Error(); got != "admission conflict" {
		t.Fatalf("conflict text = %q", got)
	}
}

func keyedAdmissionConfig(t *testing.T) config.Snapshot {
	t.Helper()
	result := orchestratorConfig()
	result.Metadata["workspace_root"] = t.TempDir()
	return result
}
