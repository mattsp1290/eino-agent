package sqlite

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-agent/session"
)

func TestLedgerReadsValidateAuthoritativeOwners(t *testing.T) {
	t.Run("model_request", func(t *testing.T) {
		st, execution, call, now := setupToolTransitionTest(t)
		defer func() { _ = st.db.Close() }()
		record := session.ModelRequestRecord{ID: "request", SessionID: call.SessionID, RunID: call.RunID, AssistantMessageID: "reserved-assistant", State: session.ModelRequestPrepared, Attempt: 1, Step: 1, CreatedAt: now}
		if _, err := execution.CreateModelRequest(t.Context(), record); err != nil {
			t.Fatal(err)
		}
		record.SessionID = "forged-owner"
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.Exec("UPDATE model_requests SET record=? WHERE id=?", raw, []byte(record.ID)); err != nil {
			t.Fatal(err)
		}
		if _, err := st.GetModelRequest(t.Context(), record.ID); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("single read trusted record owner: %v", err)
		}
		if _, err := st.ListModelRequests(t.Context(), call.RunID, session.ModelRequestCursor{Limit: 10}); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("list trusted record owner: %v", err)
		}
	})
	t.Run("epoch", func(t *testing.T) {
		st, execution, call, now := setupToolTransitionTest(t)
		defer func() { _ = st.db.Close() }()
		record := session.ContextEpoch{ID: "epoch", SessionID: call.SessionID, CreatedAt: now}
		if _, err := execution.StartContextEpoch(t.Context(), record); err != nil {
			t.Fatal(err)
		}
		record.SessionID = "forged-owner"
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.Exec("UPDATE context_epochs SET record=? WHERE id=?", raw, []byte(record.ID)); err != nil {
			t.Fatal(err)
		}
		if _, err := st.ListContextEpochs(t.Context(), call.SessionID); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("epoch list trusted record owner: %v", err)
		}
	})
	t.Run("oversized_model_request", func(t *testing.T) {
		st, execution, call, now := setupToolTransitionTest(t)
		defer func() { _ = st.db.Close() }()
		record := session.ModelRequestRecord{ID: "request", SessionID: call.SessionID, RunID: call.RunID, State: session.ModelRequestPrepared, Attempt: 1, Step: 1, CreatedAt: now}
		if _, err := execution.CreateModelRequest(t.Context(), record); err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.Exec("UPDATE model_requests SET record=zeroblob(?) WHERE id=?", (4<<20)+1, []byte(record.ID)); err != nil {
			t.Fatal(err)
		}
		if _, err := st.GetModelRequest(t.Context(), record.ID); !errors.Is(err, session.ErrModelRequestTooLarge) {
			t.Fatalf("oversized single read: %v", err)
		}
		if _, err := st.ListModelRequests(t.Context(), call.RunID, session.ModelRequestCursor{Limit: 10}); !errors.Is(err, session.ErrModelRequestTooLarge) {
			t.Fatalf("oversized list: %v", err)
		}
	})
}
