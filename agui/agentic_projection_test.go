package agui

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"

	"github.com/mattsp1290/eino-agent/session"
)

// TestBridgeEmitLiveMessageCommittedProjectsDurableContent is W7 item 3's
// live counterpart to the replay tests in replay_test.go: on a
// session.MessageCommittedEventKind event (see runtime/message_commit_event.go),
// Bridge.Emit reloads and reprojects the named durable message and emits it
// through the observer emitter's committed-projection path with
// DeliveryModeLiveContinuation -- custom eino.agentic.v1 supplements only,
// no duplicate native TEXT_MESSAGE_* events (those already streamed live via
// emitMessageDelta before the message committed).
func TestBridgeEmitLiveMessageCommittedProjectsDurableContent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-live-commit"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-live-commit", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-live-commit", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	const messageID session.MessageID = "assistant-live-commit"
	if _, err := execution.AppendMessage(ctx, session.Message{
		ID: messageID, SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, TurnID: "turn-live-commit", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	parts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "committed live"}}},
	}, func() session.PartID { return "part-live-commit" }, messageID, sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(ctx, parts[0]); err != nil {
		t.Fatalf("append part: %v", err)
	}

	sink := newSSESink()
	bridge := NewBridge(ctx, store, session.ContentLimits{}, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)
	bridge.Emit(ctx, session.EventRecord{
		Kind: session.MessageCommittedEventKind, SessionID: sessionID, RunID: run.ID, MessageID: messageID,
		TurnID: "turn-live-commit", Payload: []byte(`{"revision":1}`),
	})
	if err := bridge.EncErr(); err != nil {
		t.Fatalf("emitter encoding error = %v", err)
	}
	if err := bridge.Err(); err != nil {
		t.Fatalf("emitter transport error = %v", err)
	}

	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	if stringsJoined(got) != "CUSTOM" {
		t.Fatalf("event types = %#v, want a single CUSTOM content-block supplement (no duplicate native events on the live path)", got)
	}
	raw := string(sink.Bytes())
	if !strings.Contains(raw, "committed live") {
		t.Fatalf("committed text missing from stream: %s", raw)
	}
	if !strings.Contains(raw, `"turnId":"turn-live-commit"`) {
		t.Fatalf("durable turn identity missing from stream: %s", raw)
	}
}

// TestBridgeEmitLiveMessageCommittedNoopWithoutStore proves the live
// committed-projection path is a documented no-op (not a panic) when Bridge
// was constructed without a store -- existing classic-only callers/tests.
func TestBridgeEmitLiveMessageCommittedNoopWithoutStore(t *testing.T) {
	t.Parallel()

	sink := newSSESink()
	bridge := NewBridge(context.Background(), nil, session.ContentLimits{}, sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	bridge.Emit(context.Background(), session.EventRecord{
		Kind: session.MessageCommittedEventKind, SessionID: "session-1", RunID: "run-1", MessageID: "message-1",
	})
	if len(sink.Bytes()) != 0 {
		t.Fatalf("expected no output without a store, got: %s", sink.Bytes())
	}
}
