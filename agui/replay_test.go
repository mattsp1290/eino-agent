package agui

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
)

func TestReplayEmitsDurableEventsAndOmitsLiveOnlyDeltas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := replayStore(t)
	sink := newSSESink()
	bridge := NewBridge(ctx, sink.Writer(), sse.NewSSEWriter(), "session-replay", "run-1", nil)

	next, err := Replay(ctx, bridge, store, "session-replay", session.EventCursor{Limit: 10})
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
		_, err := Reconnect(ctx, bridge, store, tail, "session-replay", session.EventCursor{Limit: 10})
		done <- err
	}()
	<-tail.subscribed
	tail.events <- runtime.Event{
		Kind:      runtime.EventRunFinished,
		EventID:   "evt-finished",
		SessionID: "session-replay",
		MessageID: "assistant-1",
	}
	tail.events <- runtime.Event{
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
		_, err := Reconnect(ctx, bridge, store, tail, "session-replay", session.EventCursor{Limit: 10})
		done <- err
	}()
	<-tail.subscribed
	tail.events <- runtime.Event{Kind: runtime.EventTailOverflow, SessionID: "session-replay"}
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
		_, err := Reconnect(ctx, bridge, store, tail, "session-replay", session.EventCursor{Limit: 10})
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
	store, err := sqlitestore.Open(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: "session-replay", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.AdmitRun(ctx, session.Run{ID: "run-1", SessionID: "session-replay", OwnerID: "owner", Status: session.RunPending, CreatedAt: now}); err != nil {
		t.Fatalf("admit run: %v", err)
	}
	if _, err := store.AppendMessage(ctx, session.Message{ID: "assistant-1", SessionID: "session-replay", RunID: "run-1", Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	if _, err := store.AppendPart(ctx, session.Part{ID: "part-text", MessageID: "assistant-1", SessionID: "session-replay", RunID: "run-1", Kind: session.PartText, Ordinal: 10, Payload: []byte(`{"text":"settled"}`), CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append part: %v", err)
	}
	events := []session.EventRecord{
		{ID: "evt-started", SessionID: "session-replay", RunID: "run-1", MessageID: "assistant-1", Kind: string(runtime.EventRunStarted), CreatedAt: now},
		{ID: "evt-live", SessionID: "session-replay", RunID: "run-1", MessageID: "assistant-1", Kind: string(runtime.EventMessageDelta), Payload: []byte(`{"content":"durable?"}`), LiveOnly: true, CreatedAt: now.Add(time.Second)},
		{ID: "evt-finished", SessionID: "session-replay", RunID: "run-1", MessageID: "assistant-1", Kind: string(runtime.EventRunFinished), CreatedAt: now.Add(2 * time.Second)},
	}
	for _, event := range events {
		if _, err := store.AppendEvent(ctx, event); err != nil {
			t.Fatalf("append event %s: %v", event.ID, err)
		}
	}
	return store
}

type replayTail struct {
	events     chan runtime.Event
	subscribed chan struct{}
	canceled   chan struct{}
}

func newReplayTail() *replayTail {
	return &replayTail{
		events:     make(chan runtime.Event),
		subscribed: make(chan struct{}),
		canceled:   make(chan struct{}),
	}
}

func (t *replayTail) Subscribe(ctx context.Context, _ session.ID) (<-chan runtime.Event, error) {
	close(t.subscribed)
	go func() {
		<-ctx.Done()
		close(t.canceled)
	}()
	return t.events, nil
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
