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
			message("old-assistant", session.RoleAssistant, now.Add(time.Nanosecond)),
			boundary.Message,
			message("tail", session.RoleUser, now.Add(time.Second)),
		},
		Parts: []session.Part{
			textContentPart(t, "old-part", "old", "SECRET raw prompt", now),
			part("old-provider-state", "old-assistant", session.PartProviderState, `{"data":"SECRET provider state"}`, now.Add(time.Nanosecond)),
			boundary.Part,
			textContentPart(t, "tail-part", "tail", "Continue", now.Add(time.Second)),
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
	if strings.Contains(joined, "SECRET raw prompt") || strings.Contains(joined, "SECRET provider state") {
		t.Fatalf("projected compacted private content: %q", joined)
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

// TestBoundaryStampsTurnIDAndAgentPath proves the W7 review's P0 finding
// (durable-identity-reviewer item 3 / A1's "also"): a compaction boundary
// message must carry the durable turn identity that triggered compaction,
// not an empty one. Before this fix, NewBoundary never set Message.TurnID
// or Message.AgentPath at all, so agui.agenticIdentity fell back to a
// synthetic turn id for EVERY compacted session, every time -- not just
// pre-fix data.
func TestBoundaryStampsTurnIDAndAgentPath(t *testing.T) {
	t.Parallel()

	boundary, err := NewBoundary(session.ContextEpoch{
		ID:               "epoch-turn",
		SessionID:        "session-1",
		SummarizedFromID: "old-a",
		SummarizedToID:   "old-b",
		TailStartID:      "tail",
	}, BoundaryIDs{MessageID: "summary", PartID: "part", TurnID: "turn-that-triggered-compaction", AgentPath: "root"}, "run-1", time.Unix(1, 0).UTC(), "Summary")
	if err != nil {
		t.Fatalf("NewBoundary error = %v", err)
	}
	if boundary.Message.TurnID != "turn-that-triggered-compaction" {
		t.Fatalf("boundary.Message.TurnID = %q, want %q", boundary.Message.TurnID, "turn-that-triggered-compaction")
	}
	if boundary.Message.AgentPath != "root" {
		t.Fatalf("boundary.Message.AgentPath = %q, want %q", boundary.Message.AgentPath, "root")
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

// textContentPart builds a single durable user_input_text part carrying
// text, using the real EncodeContentParts wire format (rather than a
// hand-written envelope) so classic history projection can decode it.
func textContentPart(t *testing.T, id session.PartID, messageID session.MessageID, text string, now time.Time) session.Part {
	t.Helper()
	content := session.Content{Role: session.RoleUser, Blocks: []session.ContentBlock{
		{ID: "b1", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: text}},
	}}
	parts, err := session.EncodeContentParts(content, func() session.PartID { return id }, messageID, "session-1", "run-1", now, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("EncodeContentParts: %v", err)
	}
	return parts[0]
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
