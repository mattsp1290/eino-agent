package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func TestProviderStatePayloadSurvivesSQLiteCloseAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "provider-state.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: "state-session", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "state-run", SessionID: "state-session", ProviderID: "provider", ModelID: "model", OwnerID: "owner", ClaimToken: "claim", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	message := session.Message{ID: "state-message", SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, ModelID: run.ModelID, CreatedAt: now, UpdatedAt: now}
	if _, err := execution.AppendMessage(ctx, message); err != nil {
		t.Fatal(err)
	}
	rawItems := []json.RawMessage{json.RawMessage(`{"z":1, "a":"first"}`), json.RawMessage("{\n \"a\":\"second\", \"z\":2\n}")}
	for index, raw := range rawItems {
		payload, err := session.EncodeProviderStatePayload(session.ProviderStateEnvelope{CodecID: "codec", Version: 1, ProviderID: run.ProviderID, SourceModelID: run.ModelID, CompatibilityKey: "compat", ItemIndex: index, Data: raw})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := execution.AppendPart(ctx, session.Part{ID: session.PartID(fmt.Sprintf("state-part-%d", index)), MessageID: message.ID, SessionID: message.SessionID, RunID: message.RunID, Kind: session.PartProviderState, Ordinal: int64(index), Payload: payload, CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	batch, err := store.ListMessages(ctx, "state-session", session.ReplayCursor{Limit: 10})
	if err != nil || len(batch.Parts) != 2 || len(batch.PartOwnerMessageIDs) != 2 {
		t.Fatalf("batch = %#v, %v", batch, err)
	}
	for index, part := range batch.Parts {
		envelope, err := session.DecodeProviderStatePayload(part.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(envelope.Data, rawItems[index]) || batch.PartOwnerMessageIDs[index] != message.ID {
			t.Fatalf("part %d = %#v owner=%q", index, envelope, batch.PartOwnerMessageIDs[index])
		}
	}
}

func TestListMessagesRejectsProviderStateRowsAboveHardCount(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "provider-state-count.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: "state-session", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "state-run", SessionID: "state-session", ProviderID: "provider", ModelID: "model", OwnerID: "owner", ClaimToken: "claim", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	message := session.Message{ID: "state-message", SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, ModelID: run.ModelID, CreatedAt: now, UpdatedAt: now}
	if _, err := execution.AppendMessage(ctx, message); err != nil {
		t.Fatal(err)
	}
	for index := 0; index <= session.ProviderStateHardMaxItems; index++ {
		part := session.Part{
			ID: session.PartID(fmt.Sprintf("state-part-%03d", index)), MessageID: message.ID, SessionID: message.SessionID,
			RunID: message.RunID, Kind: session.PartProviderState, Ordinal: int64(index), Payload: json.RawMessage(`{}`), CreatedAt: now, UpdatedAt: now,
		}
		if _, err := execution.AppendPart(ctx, part); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.ListMessages(ctx, message.SessionID, session.ReplayCursor{Limit: 10}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("ListMessages error = %v, want ErrConflict", err)
	}
}
