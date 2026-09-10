package agui

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func TestReplayEmitsDurableEventsAndOmitsLiveOnlyDeltas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := replayStore(t)
	sink := newSSESink()
	bridge := NewBridge(ctx, sink.Writer(), sse.NewSSEWriter(), "session-replay", "run-1", nil)

	next, err := Replay(ctx, bridge, store, "session-replay", session.EventCursor{Limit: 10}, session.ContentLimits{})
	if err != nil {
		t.Fatalf("Replay error = %v", err)
	}
	if next.AfterEventID != "evt-finished" {
		t.Fatalf("next cursor = %+v, want evt-finished", next)
	}
	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	want := []string{"MESSAGES_SNAPSHOT", "RUN_STARTED", "RUN_FINISHED"}
	if stringsJoined(got) != stringsJoined(want) {
		t.Fatalf("event types = %#v, want %#v", got, want)
	}
	if frames[0]["messages"] == nil {
		t.Fatalf("messages snapshot missing messages: %#v", frames[0])
	}
	if strings.Contains(string(sink.Bytes()), "AGUI_PROVIDER_STATE_SENTINEL") || strings.Contains(string(sink.Bytes()), "provider_state") {
		t.Fatalf("provider state leaked to replay: %s", sink.Bytes())
	}
}

func TestReconnectReplaysThenTailsLiveEventsUntilDisconnect(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := replayStore(t)
	tail := newReplayTail()
	sink := newSSESink()
	bridge := NewBridge(ctx, sink.Writer(), sse.NewSSEWriter(), "session-replay", "run-1", nil)
	done := make(chan error, 1)
	go func() {
		_, err := Reconnect(ctx, bridge, store, tail, "session-replay", session.EventCursor{Limit: 10}, session.ContentLimits{})
		done <- err
	}()
	<-tail.subscribed
	tail.events <- session.EventRecord{
		Kind:      runtime.EventRunFinished,
		ID:        "evt-finished",
		SessionID: "session-replay",
		MessageID: "assistant-1",
	}
	tail.events <- session.EventRecord{
		Kind:      runtime.EventMessageDelta,
		SessionID: "session-replay",
		MessageID: "assistant-1",
		Payload:   []byte(`{"content":"live","reasoning":""}`),
		LiveOnly:  true,
	}
	close(tail.events)
	if err := <-done; err != nil {
		t.Fatalf("Reconnect err = %v", err)
	}
	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	if stringsJoined(got) != "MESSAGES_SNAPSHOT,RUN_STARTED,RUN_FINISHED,TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT" {
		t.Fatalf("event types = %#v", got)
	}
}

func TestReconnectReportsTailOverflow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := replayStore(t)
	tail := newReplayTail()
	sink := newSSESink()
	bridge := NewBridge(ctx, sink.Writer(), sse.NewSSEWriter(), "session-replay", "run-1", nil)
	done := make(chan error, 1)
	go func() {
		_, err := Reconnect(ctx, bridge, store, tail, "session-replay", session.EventCursor{Limit: 10}, session.ContentLimits{})
		done <- err
	}()
	<-tail.subscribed
	tail.events <- session.EventRecord{Kind: runtime.EventTailOverflow, SessionID: "session-replay"}
	if err := <-done; !errors.Is(err, ErrTailOverflow) {
		t.Fatalf("Reconnect err = %v, want ErrTailOverflow", err)
	}
	<-tail.canceled
}

func TestReconnectCancelsTailOnDisconnect(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	store := replayStore(t)
	tail := newReplayTail()
	sink := newSSESink()
	bridge := NewBridge(ctx, sink.Writer(), sse.NewSSEWriter(), "session-replay", "run-1", nil)
	done := make(chan error, 1)
	go func() {
		_, err := Reconnect(ctx, bridge, store, tail, "session-replay", session.EventCursor{Limit: 10}, session.ContentLimits{})
		done <- err
	}()
	<-tail.subscribed
	cancel()
	<-tail.canceled
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Reconnect err = %v, want context canceled", err)
	}
}

func replayStore(t *testing.T) session.Store {
	t.Helper()
	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() {
		_ = storePool.Close()
	})
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: "session-replay", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-1", SessionID: "session-replay", OwnerID: "owner", ClaimToken: "claim-replay", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "assistant-1", SessionID: "session-replay", RunID: "run-1", Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	if _, err := execution.AppendPart(ctx, session.Part{ID: "part-text", MessageID: "assistant-1", SessionID: "session-replay", RunID: "run-1", Kind: session.PartText, Ordinal: 10, Payload: []byte(`{"text":"settled"}`), CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append part: %v", err)
	}
	if _, err := execution.AppendPart(ctx, session.Part{ID: "part-provider-state", MessageID: "assistant-1", SessionID: "session-replay", RunID: "run-1", Kind: session.PartProviderState, Ordinal: 11, Payload: []byte(`{"data":"AGUI_PROVIDER_STATE_SENTINEL"}`), CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append provider state: %v", err)
	}
	events := []session.EventRecord{
		{ID: "evt-started", SessionID: "session-replay", RunID: "run-1", MessageID: "assistant-1", Kind: string(runtime.EventRunStarted), CreatedAt: now},
		{ID: "evt-live", SessionID: "session-replay", RunID: "run-1", MessageID: "assistant-1", Kind: string(runtime.EventMessageDelta), Payload: []byte(`{"content":"durable?"}`), LiveOnly: true, CreatedAt: now.Add(time.Second)},
	}
	for _, event := range events {
		if _, err := execution.AppendEvent(ctx, event); err != nil {
			t.Fatalf("append event %s: %v", event.ID, err)
		}
	}
	if _, err := execution.SettleRun(ctx, session.SettleRunRequest{
		Settlement: session.RunSettlement{Status: session.RunCompleted, FinishedAt: now.Add(2 * time.Second)},
		Event:      session.RunSettlementEvent{ID: "evt-finished", MessageID: "assistant-1"},
	}); err != nil {
		t.Fatalf("settle run: %v", err)
	}
	return store
}

type replayTail struct {
	events     chan session.EventRecord
	subscribed chan struct{}
	canceled   chan struct{}
}

func newReplayTail() *replayTail {
	return &replayTail{
		events:     make(chan session.EventRecord),
		subscribed: make(chan struct{}),
		canceled:   make(chan struct{}),
	}
}

func (t *replayTail) Subscribe(ctx context.Context, _ session.ID) (<-chan session.EventRecord, error) {
	close(t.subscribed)
	go func() {
		<-ctx.Done()
		close(t.canceled)
	}()
	return t.events, nil
}

// TestReplayMessageSnapshotIncludesUserMediaBlock proves that a durable user
// message carrying a media block (persisted as a user_input_image part) does
// not break emitMessageSnapshot's history.Load call. history.Load uses the
// classic projector, which the storage-projection review flagged as
// permanently bricking replay for any session whose history contains a
// user-role media block, once that projector started rejecting
// BlockKind-backed kinds it did not explicitly support.
func TestReplayMessageSnapshotIncludesUserMediaBlock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-replay-media"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-media", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-media", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "user-media-1", SessionID: sessionID, RunID: "run-media", Role: session.RoleUser, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}

	content := session.Content{Role: session.RoleUser, Blocks: []session.ContentBlock{
		{ID: "blk-text", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "look at this"}},
		{ID: "blk-image", Kind: session.BlockKindUserInputImage, Media: &session.MediaBlock{URL: "https://example.com/pic.png", MIMEType: "image/png"}},
	}}
	n := 0
	nextPartID := func() session.PartID { n++; return session.PartID(fmt.Sprintf("part-media-%d", n)) }
	parts, err := session.EncodeContentParts(content, nextPartID, "user-media-1", sessionID, "run-media", now, session.DefaultContentLimits())
	if err != nil {
		t.Fatalf("EncodeContentParts: %v", err)
	}
	for _, part := range parts {
		if _, err := execution.AppendPart(ctx, part); err != nil {
			t.Fatalf("append part %s: %v", part.ID, err)
		}
	}

	sink := newSSESink()
	bridge := NewBridge(ctx, sink.Writer(), sse.NewSSEWriter(), string(sessionID), "run-media", nil)
	if err := emitMessageSnapshot(ctx, bridge, store, sessionID, session.ContentLimits{}); err != nil {
		t.Fatalf("emitMessageSnapshot error = %v, want nil (a user-role media block must not brick history.Load)", err)
	}
	frames := frameData(t, sink.Bytes())
	if len(frames) != 1 || frames[0]["messages"] == nil {
		t.Fatalf("messages snapshot missing or malformed: %#v", frames)
	}
}

func stringsJoined(values []string) string {
	if len(values) == 0 {
		return ""
	}
	result := values[0]
	for _, value := range values[1:] {
		result += "," + value
	}
	return result
}
