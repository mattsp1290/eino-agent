package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

type providerStateLoadStore struct {
	session.Store
	batch session.ReplayBatch
	runs  map[session.RunID]session.Run
}

func (s providerStateLoadStore) ListMessages(context.Context, session.ID, session.ReplayCursor) (session.ReplayBatch, error) {
	return s.batch, nil
}

func (s providerStateLoadStore) GetRun(_ context.Context, id session.RunID) (session.Run, error) {
	run, ok := s.runs[id]
	if !ok {
		return session.Run{}, session.ErrNotFound
	}
	return run, nil
}

type providerStateLoadFixture struct {
	sessionRecord session.Session
	batch         session.ReplayBatch
	runs          map[session.RunID]session.Run
}

func newProviderStateLoadFixture(t *testing.T) providerStateLoadFixture {
	t.Helper()
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	payload := providerStatePayloadForTest(t, session.ProviderStateEnvelope{
		CodecID: runtimeProviderStateContract().CodecID, Version: 1, ProviderID: "fake", SourceModelID: "test",
		CompatibilityKey: runtimeProviderStateContract().CompatibilityKey, ItemIndex: 0, Data: providerStateRawItems[0],
	})
	message := session.Message{ID: "assistant", SessionID: "session", RunID: "source-run", Role: session.RoleAssistant, ModelID: "test", CreatedAt: now, UpdatedAt: now}
	part := session.Part{ID: "state-0", MessageID: message.ID, SessionID: message.SessionID, RunID: message.RunID, Kind: session.PartProviderState, Ordinal: 0, Payload: payload, CreatedAt: now, UpdatedAt: now}
	return providerStateLoadFixture{
		sessionRecord: session.Session{ID: "session"},
		batch:         session.ReplayBatch{Messages: []session.Message{message}, Parts: []session.Part{part}, PartOwnerMessageIDs: []session.MessageID{message.ID}},
		runs:          map[session.RunID]session.Run{"source-run": {ID: "source-run", SessionID: "session", ProviderID: "fake", ModelID: "test", Status: session.RunCompleted}},
	}
}

func TestLoadProviderHistoryRejectsActiveCorruptionBeforeDispatch(t *testing.T) {
	tests := []struct {
		name string
		kind error
		edit func(*testing.T, *providerStateLoadFixture)
	}{
		{name: "malformed payload", kind: model.ErrProviderStateInvalid, edit: func(_ *testing.T, f *providerStateLoadFixture) {
			f.batch.Parts[0].Payload = json.RawMessage(`{"bad":true}`)
		}},
		{name: "unknown version", kind: model.ErrProviderStateVersion, edit: func(t *testing.T, f *providerStateLoadFixture) {
			replaceProviderStateEnvelope(t, f, func(e *session.ProviderStateEnvelope) { e.Version = 2 })
		}},
		{name: "provider mismatch", kind: model.ErrProviderStateMismatch, edit: func(t *testing.T, f *providerStateLoadFixture) {
			replaceProviderStateEnvelope(t, f, func(e *session.ProviderStateEnvelope) { e.ProviderID = "other" })
		}},
		{name: "model mismatch", kind: model.ErrProviderStateMismatch, edit: func(t *testing.T, f *providerStateLoadFixture) {
			replaceProviderStateEnvelope(t, f, func(e *session.ProviderStateEnvelope) { e.SourceModelID = "other" })
		}},
		{name: "codec mismatch", kind: model.ErrProviderStateMismatch, edit: func(t *testing.T, f *providerStateLoadFixture) {
			replaceProviderStateEnvelope(t, f, func(e *session.ProviderStateEnvelope) { e.CodecID = "other" })
		}},
		{name: "compatibility mismatch", kind: model.ErrProviderStateMismatch, edit: func(t *testing.T, f *providerStateLoadFixture) {
			replaceProviderStateEnvelope(t, f, func(e *session.ProviderStateEnvelope) { e.CompatibilityKey = "other" })
		}},
		{name: "wrong role", kind: model.ErrProviderStateMismatch, edit: func(_ *testing.T, f *providerStateLoadFixture) { f.batch.Messages[0].Role = session.RoleUser }},
		{name: "embedded part message", kind: model.ErrProviderStateMismatch, edit: func(_ *testing.T, f *providerStateLoadFixture) { f.batch.Parts[0].MessageID = "other" }},
		{name: "part session", kind: model.ErrProviderStateMismatch, edit: func(_ *testing.T, f *providerStateLoadFixture) { f.batch.Parts[0].SessionID = "other" }},
		{name: "part run", kind: model.ErrProviderStateMismatch, edit: func(_ *testing.T, f *providerStateLoadFixture) { f.batch.Parts[0].RunID = "other" }},
		{name: "message session", kind: model.ErrProviderStateMismatch, edit: func(_ *testing.T, f *providerStateLoadFixture) { f.batch.Messages[0].SessionID = "other" }},
		{name: "message model", kind: model.ErrProviderStateMismatch, edit: func(_ *testing.T, f *providerStateLoadFixture) { f.batch.Messages[0].ModelID = "other" }},
		{name: "run session", kind: model.ErrProviderStateMismatch, edit: func(_ *testing.T, f *providerStateLoadFixture) {
			run := f.runs["source-run"]
			run.SessionID = "other"
			f.runs["source-run"] = run
		}},
		{name: "missing run", kind: model.ErrProviderStateMismatch, edit: func(_ *testing.T, f *providerStateLoadFixture) { delete(f.runs, "source-run") }},
		{name: "gapped item", kind: model.ErrProviderStateInvalid, edit: func(t *testing.T, f *providerStateLoadFixture) {
			replaceProviderStateEnvelope(t, f, func(e *session.ProviderStateEnvelope) { e.ItemIndex = 1 })
		}},
		{name: "duplicate ordinal", kind: model.ErrProviderStateMismatch, edit: func(t *testing.T, f *providerStateLoadFixture) {
			second := f.batch.Parts[0]
			second.ID = "state-1"
			second.Payload = providerStatePayloadForTest(t, session.ProviderStateEnvelope{CodecID: runtimeProviderStateContract().CodecID, Version: 1, ProviderID: "fake", SourceModelID: "test", CompatibilityKey: runtimeProviderStateContract().CompatibilityKey, ItemIndex: 1, Data: providerStateRawItems[1]})
			f.batch.Parts = append(f.batch.Parts, second)
			f.batch.PartOwnerMessageIDs = append(f.batch.PartOwnerMessageIDs, "assistant")
		}},
		{name: "codec item count", kind: model.ErrProviderStateTooLarge, edit: func(t *testing.T, f *providerStateLoadFixture) {
			for index := 1; index <= runtimeProviderStateContract().Limits.MaxItems; index++ {
				part := f.batch.Parts[0]
				part.ID = session.PartID(string(rune('a' + index)))
				part.Ordinal = int64(index)
				part.Payload = providerStatePayloadForTest(t, session.ProviderStateEnvelope{CodecID: runtimeProviderStateContract().CodecID, Version: 1, ProviderID: "fake", SourceModelID: "test", CompatibilityKey: runtimeProviderStateContract().CompatibilityKey, ItemIndex: index, Data: json.RawMessage(`{"x":1}`)})
				f.batch.Parts = append(f.batch.Parts, part)
				f.batch.PartOwnerMessageIDs = append(f.batch.PartOwnerMessageIDs, "assistant")
			}
		}},
	}
	resolved := providerStateResolvedForTest(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProviderStateLoadFixture(t)
			test.edit(t, &fixture)
			store := providerStateLoadStore{batch: fixture.batch, runs: fixture.runs}
			_, _, err := loadProviderHistory(context.Background(), store, fixture.sessionRecord, history.Options{}, resolved)
			if err == nil || !errors.Is(err, test.kind) || strings.Contains(err.Error(), "STATE_SENTINEL") {
				t.Fatalf("error = %v, want %v", err, test.kind)
			}
		})
	}
}

func TestLoadProviderHistoryIgnoresMalformedInactiveCompactedState(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	batch := session.ReplayBatch{
		Messages: []session.Message{
			{ID: "old", SessionID: "session", RunID: "old-run", Role: session.RoleAssistant, ModelID: "test", CreatedAt: now},
			{ID: "tail", SessionID: "session", RunID: "tail-run", Role: session.RoleUser, CreatedAt: now.Add(time.Second)},
			{ID: "summary", SessionID: "session", RunID: "summary-run", Role: session.RoleSystem, CreatedAt: now.Add(2 * time.Second)},
		},
		Parts: []session.Part{
			{ID: "bad-state", MessageID: "old", SessionID: "session", RunID: "old-run", Kind: session.PartProviderState, Payload: json.RawMessage(`STATE_SENTINEL malformed`)},
			{ID: "tail-text", MessageID: "tail", SessionID: "session", RunID: "tail-run", Kind: session.PartText, Payload: json.RawMessage(`{"text":"tail"}`)},
			{ID: "summary-text", MessageID: "summary", SessionID: "session", RunID: "summary-run", Kind: session.PartCompaction, Payload: json.RawMessage(`{"text":"summary","epoch_id":"epoch","redacted":true}`)},
		},
		PartOwnerMessageIDs: []session.MessageID{"old", "tail", "summary"},
	}
	ordinary := model.Resolved{Provider: model.Provider{ID: "fake"}, Model: model.Descriptor{ID: "test", ProviderID: "fake"}, Streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) { return nil, nil })}
	messages, states, err := loadProviderHistory(context.Background(), providerStateLoadStore{batch: batch}, session.Session{ID: "session"}, history.Options{Epoch: &session.ContextEpoch{SummaryMessageID: "summary", TailStartID: "tail"}}, ordinary)
	if err != nil || len(states) != 0 || len(messages) != 2 || messages[0].Content != "summary" || messages[1].Content != "tail" {
		t.Fatalf("messages/states/error = %#v/%#v/%v", messages, states, err)
	}
}

func TestLoadProviderHistoryRejectsOwnerMismatchBeforeInactiveFiltering(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	payload := providerStatePayloadForTest(t, session.ProviderStateEnvelope{
		CodecID: runtimeProviderStateContract().CodecID, Version: 1, ProviderID: "fake", SourceModelID: "test",
		CompatibilityKey: runtimeProviderStateContract().CompatibilityKey, ItemIndex: 0, Data: providerStateRawItems[0],
	})
	batch := session.ReplayBatch{
		Messages: []session.Message{
			{ID: "old", SessionID: "session", RunID: "old-run", Role: session.RoleAssistant, ModelID: "test", CreatedAt: now},
			{ID: "tail", SessionID: "session", RunID: "tail-run", Role: session.RoleAssistant, ModelID: "test", CreatedAt: now.Add(time.Second)},
			{ID: "summary", SessionID: "session", RunID: "summary-run", Role: session.RoleSystem, CreatedAt: now.Add(2 * time.Second)},
		},
		Parts: []session.Part{
			{ID: "state", MessageID: "tail", SessionID: "session", RunID: "tail-run", Kind: session.PartProviderState, Payload: payload},
			{ID: "summary-text", MessageID: "summary", SessionID: "session", RunID: "summary-run", Kind: session.PartCompaction, Payload: json.RawMessage(`{"text":"summary","epoch_id":"epoch","redacted":true}`)},
		},
		PartOwnerMessageIDs: []session.MessageID{"old", "summary"},
	}
	resolved := providerStateResolvedForTest(t)
	_, _, err := loadProviderHistory(context.Background(), providerStateLoadStore{batch: batch}, session.Session{ID: "session"}, history.Options{Epoch: &session.ContextEpoch{SummaryMessageID: "summary", TailStartID: "tail"}}, resolved)
	if !errors.Is(err, model.ErrProviderStateMismatch) {
		t.Fatalf("error = %v, want provider-state mismatch", err)
	}
}

func TestProviderStateStreamerCallbackPanicsAreContentFree(t *testing.T) {
	fixture := newProviderStateLoadFixture(t)
	contractPanic := &panicRuntimeProviderStateStreamer{panicContract: true}
	resolved := model.Resolved{Provider: model.Provider{ID: "fake"}, Model: model.Descriptor{ID: "test", ProviderID: "fake"}, Streamer: contractPanic}
	_, _, err := loadProviderHistory(context.Background(), providerStateLoadStore{batch: fixture.batch, runs: fixture.runs}, fixture.sessionRecord, history.Options{}, resolved)
	if !errors.Is(err, model.ErrProviderStateInvalid) || strings.Contains(err.Error(), "STATE_SENTINEL") {
		t.Fatalf("contract panic error = %v", err)
	}

	capturePanic := &panicRuntimeProviderStateStreamer{contract: runtimeProviderStateContract(), panicCapture: true}
	snapshot := TurnSnapshot{SessionID: "session", RunID: "run", Model: model.Resolved{Provider: model.Provider{ID: "fake"}, Model: model.Descriptor{ID: "test", ProviderID: "fake"}, Streamer: capturePanic}}
	message := einoschema.AssistantMessage("answer", nil)
	message.Extra = map[string]any{providerStateExtraKey: []json.RawMessage{providerStateRawItems[0]}}
	_, err = captureAssistantProviderState(snapshot, "assistant", message)
	if !errors.Is(err, model.ErrProviderStateInvalid) || strings.Contains(err.Error(), "STATE_SENTINEL") {
		t.Fatalf("capture panic error = %v", err)
	}
}

type panicRuntimeProviderStateStreamer struct {
	contract      model.ProviderStateContract
	panicContract bool
	panicCapture  bool
}

func (s *panicRuntimeProviderStateStreamer) StreamProvider(context.Context, model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	return einoschema.StreamReaderFromArray([]model.StreamDelta{}), nil
}

func (s *panicRuntimeProviderStateStreamer) ProviderStateContract() model.ProviderStateContract {
	if s.panicContract {
		panic("STATE_SENTINEL")
	}
	return s.contract
}

func (s *panicRuntimeProviderStateStreamer) CaptureProviderState(*einoschema.Message) (model.ProviderStateCapture, error) {
	if s.panicCapture {
		panic("STATE_SENTINEL")
	}
	return model.ProviderStateCapture{}, nil
}

func providerStateResolvedForTest(t *testing.T) model.Resolved {
	t.Helper()
	codec, err := model.NewEinoJSONExtraStateCodec(model.EinoJSONExtraStateConfig{ExtraKey: providerStateExtraKey, Contract: runtimeProviderStateContract()})
	if err != nil {
		t.Fatal(err)
	}
	streamer, err := model.NewEinoStreamerWithProviderState(&runtimeProviderStateModel{}, codec)
	if err != nil {
		t.Fatal(err)
	}
	return model.Resolved{Provider: model.Provider{ID: "fake"}, Model: model.Descriptor{ID: "test", ProviderID: "fake"}, Streamer: streamer}
}

func providerStatePayloadForTest(t *testing.T, envelope session.ProviderStateEnvelope) json.RawMessage {
	t.Helper()
	payload, err := session.EncodeProviderStatePayload(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func replaceProviderStateEnvelope(t *testing.T, fixture *providerStateLoadFixture, edit func(*session.ProviderStateEnvelope)) {
	t.Helper()
	envelope, err := session.DecodeProviderStatePayload(fixture.batch.Parts[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	edit(&envelope)
	fixture.batch.Parts[0].Payload = providerStatePayloadForTest(t, envelope)
}
