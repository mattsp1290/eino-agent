package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func TestAppendMessageRetryAfterFinalizePreservesFinalization(t *testing.T) {
	for _, mode := range []string{"root", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			st, execution, _, now := setupToolTransitionTest(t)
			defer func() { _ = st.db.Close() }()
			ctx := context.Background()
			message := session.Message{ID: session.MessageID("retry-message-" + mode), SessionID: "session-tool", RunID: "run-tool", Role: session.RoleAssistant, CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second)}

			appendAndFinalize := func(store session.ExecutionStore) error {
				if _, err := store.AppendMessage(ctx, message); err != nil {
					return fmt.Errorf("append before finalize: %w", err)
				}
				if err := store.FinalizeAssistantMessage(ctx, message.ID); err != nil {
					return fmt.Errorf("finalize: %w", err)
				}
				if _, err := store.AppendMessage(ctx, message); err != nil {
					return fmt.Errorf("append after finalize: %w", err)
				}
				return nil
			}
			if mode == "root" {
				if err := appendAndFinalize(execution); err != nil {
					t.Fatal(err)
				}
			} else if err := st.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
				return appendAndFinalize(tx.Execution(session.RunFence{RunID: message.RunID, ClaimToken: "claim-tool-run"}))
			}); err != nil {
				t.Fatal(err)
			}

			snapshot, err := st.ReadObservationSnapshot(ctx, message.SessionID, observationLimits())
			if err != nil {
				t.Fatal(err)
			}
			var observed bool
			for _, observedMessage := range snapshot.Messages {
				if observedMessage.ID == message.ID {
					observed = observedMessage.Finalized
				}
			}
			if !observed {
				t.Fatalf("finalized message snapshot = %#v", snapshot.Messages)
			}
			var finalized bool
			if err := st.db.QueryRowContext(ctx, "SELECT finalized FROM messages WHERE id = ?", []byte(message.ID)).Scan(&finalized); err != nil {
				t.Fatal(err)
			}
			if !finalized {
				t.Fatal("message finalization was lost")
			}
		})
	}
}

func TestUpdatePartCreatedAtSynchronizesProjectionAndReplay(t *testing.T) {
	st, execution, _, now := setupToolTransitionTest(t)
	defer func() { _ = st.db.Close() }()
	ctx := context.Background()
	part := session.Part{
		ID: "retry-part", MessageID: "msg-tool", SessionID: "session-tool", RunID: "run-tool",
		Kind: session.PartText, Ordinal: 1, Payload: json.RawMessage(`{"text":"before"}`),
		CreatedAt: now, UpdatedAt: now,
	}
	if _, err := execution.AppendPart(ctx, part); err != nil {
		t.Fatal(err)
	}
	part.CreatedAt = now.Add(time.Second)
	part.UpdatedAt = now.Add(time.Second)
	part.Payload = json.RawMessage(`{"text":"after"}`)
	if err := execution.UpdatePart(ctx, part); err != nil {
		t.Fatal(err)
	}
	batch, err := st.ListMessages(ctx, part.SessionID, session.ReplayCursor{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Parts) != 1 || !reflect.DeepEqual(batch.Parts[0], part) {
		t.Fatalf("replayed part = %#v, want %#v", batch.Parts, part)
	}
	if _, err := execution.AppendPart(ctx, part); err != nil {
		t.Fatalf("identical append after update: %v", err)
	}
	var createdAt string
	var displayText []byte
	var raw []byte
	if err := st.db.QueryRowContext(ctx, "SELECT created_at, display_text, record FROM parts WHERE id = ?", []byte(part.ID)).Scan(&createdAt, &displayText, &raw); err != nil {
		t.Fatal(err)
	}
	wantCreatedAt := part.CreatedAt.UTC().Format("2006-01-02T15:04:05.000000000Z")
	wantText := `after`
	if createdAt != wantCreatedAt || string(displayText) != wantText {
		t.Fatalf("part projection = created_at %q display_text %q, want %q %q", createdAt, displayText, wantCreatedAt, wantText)
	}
	var durable session.Part
	if err := json.Unmarshal(raw, &durable); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(durable, part) {
		t.Fatalf("stored part = %#v, want %#v", durable, part)
	}
}

func TestDistinctModelRequestIDCollisionReturnsConflict(t *testing.T) {
	st, execution, _, now := setupToolTransitionTest(t)
	defer func() { _ = st.db.Close() }()
	ctx := context.Background()
	original := session.ModelRequestRecord{
		ID: "request-a", SessionID: "session-tool", RunID: "run-tool", AssistantMessageID: "msg-tool",
		Attempt: 1, Step: 1, State: session.ModelRequestPrepared, Messages: json.RawMessage(`[]`), Tools: json.RawMessage(`[]`), SafeCallConfig: json.RawMessage(`{}`), ContentSHA256: "hash", CreatedAt: now, UpdatedAt: now,
	}
	if _, err := execution.CreateModelRequest(ctx, original); err != nil {
		t.Fatal(err)
	}
	collision := original
	collision.ID = "request-b"
	if _, err := execution.CreateModelRequest(ctx, collision); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("distinct ID collision = %v, want ErrConflict", err)
	}
	got, err := st.GetModelRequest(ctx, original.ID)
	if err != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("original request = %#v, %v; want %#v", got, err, original)
	}
	if _, err := st.GetModelRequest(ctx, collision.ID); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("collision request = %v, want ErrNotFound", err)
	}
}

func TestCaughtModelRequestCollisionLeavesOuterTransactionUsable(t *testing.T) {
	st, execution, _, now := setupToolTransitionTest(t)
	defer func() { _ = st.db.Close() }()
	ctx := context.Background()
	original := session.ModelRequestRecord{
		ID: "request-a", SessionID: "session-tool", RunID: "run-tool", AssistantMessageID: "msg-tool",
		Attempt: 2, Step: 3, State: session.ModelRequestPrepared, Messages: json.RawMessage(`[]`), Tools: json.RawMessage(`[]`), SafeCallConfig: json.RawMessage(`{}`), ContentSHA256: "hash", CreatedAt: now, UpdatedAt: now,
	}
	if _, err := execution.CreateModelRequest(ctx, original); err != nil {
		t.Fatal(err)
	}
	collision := original
	collision.ID = "request-b"
	var collisionErr error
	err := st.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
		txExecution := tx.Execution(session.RunFence{RunID: original.RunID, ClaimToken: "claim-tool-run"})
		_, collisionErr = txExecution.CreateModelRequest(ctx, collision)
		_, err := tx.CreateSession(ctx, session.Session{ID: "unrelated-after-model-conflict"})
		return err
	})
	if !errors.Is(collisionErr, session.ErrConflict) {
		t.Fatalf("distinct ID collision = %v, want ErrConflict", collisionErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetSession(ctx, "unrelated-after-model-conflict"); err != nil {
		t.Fatalf("unrelated outer write: %v", err)
	}
	got, err := st.GetModelRequest(ctx, original.ID)
	if err != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("original request = %#v, %v; want %#v", got, err, original)
	}
	if _, err := st.GetModelRequest(ctx, collision.ID); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("collision request = %v, want ErrNotFound", err)
	}
}
