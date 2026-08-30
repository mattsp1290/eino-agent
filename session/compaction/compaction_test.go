package compaction

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

func TestBoundaryProjectsSummaryWithoutRawPromptLeak(t *testing.T) {
	t.Parallel()

	now := time.Unix(1, 0).UTC()
	epoch := session.ContextEpoch{
		ID:               "epoch-compact",
		SessionID:        "session-1",
		SummarizedFromID: "old",
		SummarizedToID:   "old",
		TailStartID:      "tail",
	}
	boundary, err := NewBoundary(epoch, BoundaryIDs{MessageID: "summary", PartID: "summary-part"}, "run-1", now, "Safe summary only.")
	if err != nil {
		t.Fatalf("NewBoundary error = %v", err)
	}
	projected, err := history.Project(session.ReplayBatch{
		Messages: []session.Message{
			message("old", session.RoleUser, now),
			boundary.Message,
			message("tail", session.RoleUser, now.Add(time.Second)),
		},
		Parts: []session.Part{
			part("old-part", "old", session.PartText, `{"text":"SECRET raw prompt"}`, now),
			boundary.Part,
			part("tail-part", "tail", session.PartText, `{"text":"Continue"}`, now.Add(time.Second)),
		},
	}, history.Options{Epoch: &session.ContextEpoch{
		SummaryMessageID: boundary.Message.ID,
		TailStartID:      "tail",
	}})
	if err != nil {
		t.Fatalf("Project error = %v", err)
	}
	if len(projected) != 2 {
		t.Fatalf("projected len = %d, want 2", len(projected))
	}
	if projected[0].Role != schema.System || projected[0].Content != "Safe summary only." {
		t.Fatalf("summary message = %#v", projected[0])
	}
	joined := projected[0].Content + projected[1].Content
	if strings.Contains(joined, "SECRET raw prompt") {
		t.Fatalf("projected compacted raw prompt: %q", joined)
	}
}

func TestBoundaryPayloadCarriesIDsOnly(t *testing.T) {
	t.Parallel()

	boundary, err := NewBoundary(session.ContextEpoch{
		ID:               "epoch-1",
		SessionID:        "session-1",
		SummarizedFromID: "old-a",
		SummarizedToID:   "old-b",
		TailStartID:      "tail",
	}, BoundaryIDs{MessageID: "summary", PartID: "part"}, "run-1", time.Unix(1, 0).UTC(), "Summary")
	if err != nil {
		t.Fatalf("NewBoundary error = %v", err)
	}
	var payload SummaryPayload
	if err := json.Unmarshal(boundary.Part.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.EpochID != "epoch-1" || payload.SummarizedFromID != "old-a" || payload.SummarizedToID != "old-b" || !payload.Redacted {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestAppendBoundaryWritesReplayableRecords(t *testing.T) {
	t.Parallel()

	store := &boundaryStore{}
	epoch := session.ContextEpoch{ID: "epoch-1", SessionID: "session-1"}
	boundary, err := AppendBoundary(context.Background(), store, epoch, BoundaryIDs{MessageID: "summary", PartID: "part"}, "run-1", time.Unix(1, 0).UTC(), "Summary")
	if err != nil {
		t.Fatalf("AppendBoundary error = %v", err)
	}
	if boundary.Message.ID != "summary" || boundary.Part.Kind != session.PartCompaction {
		t.Fatalf("boundary = %+v", boundary)
	}
	if len(store.messages) != 1 || len(store.parts) != 1 {
		t.Fatalf("store messages=%d parts=%d", len(store.messages), len(store.parts))
	}
	if store.finishedEpoch.ID != "epoch-1" || store.finishedEpoch.SummaryMessageID != "summary" || store.finishedEpoch.ClosedAt.IsZero() {
		t.Fatalf("finished epoch = %+v", store.finishedEpoch)
	}
}

func message(id session.MessageID, role session.Role, now time.Time) session.Message {
	return session.Message{ID: id, SessionID: "session-1", RunID: "run-1", Role: role, CreatedAt: now, UpdatedAt: now}
}

func part(id session.PartID, messageID session.MessageID, kind session.PartKind, payload string, now time.Time) session.Part {
	return session.Part{ID: id, MessageID: messageID, SessionID: "session-1", RunID: "run-1", Kind: kind, Payload: json.RawMessage(payload), CreatedAt: now, UpdatedAt: now}
}

type boundaryStore struct {
	session.ExecutionStore
	messages      []session.Message
	parts         []session.Part
	finishedEpoch session.ContextEpoch
}

func (s *boundaryStore) WithinTx(ctx context.Context, fn func(context.Context, session.ExecutionStore) error) error {
	return fn(ctx, s)
}

func (s *boundaryStore) AppendMessage(_ context.Context, message session.Message) (session.Message, error) {
	s.messages = append(s.messages, message)
	return message, nil
}

func (s *boundaryStore) AppendPart(_ context.Context, part session.Part) (session.Part, error) {
	s.parts = append(s.parts, part)
	return part, nil
}

func (s *boundaryStore) FinishContextEpoch(_ context.Context, epoch session.ContextEpoch) error {
	s.finishedEpoch = epoch
	return nil
}
