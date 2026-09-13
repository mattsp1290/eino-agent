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
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), "session-replay", "run-1", nil)

	next, err := Replay(ctx, bridge, store, "session-replay", session.EventCursor{Limit: 10}, session.ContentLimits{}, false)
	if err != nil {
		t.Fatalf("Replay error = %v", err)
	}
	if next.AfterEventID != "evt-finished" {
		t.Fatalf("next cursor = %+v, want evt-finished", next)
	}
	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	// The single durable assistant message ("settled", one
	// assistant_gen_text block) projects through the agentic replay path
	// (see emitMessageSnapshot) as its native representable text triplet
	// plus one CUSTOM eino.agentic.v1 content-block supplement, ahead of
	// the durable run_started/run_finished lifecycle events.
	want := []string{"TEXT_MESSAGE_START", "TEXT_MESSAGE_CONTENT", "TEXT_MESSAGE_END", "CUSTOM", "RUN_STARTED", "RUN_FINISHED"}
	if stringsJoined(got) != stringsJoined(want) {
		t.Fatalf("event types = %#v, want %#v", got, want)
	}
	if !strings.Contains(string(sink.Bytes()), "settled") {
		t.Fatalf("replayed text missing from stream: %s", sink.Bytes())
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
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), "session-replay", "run-1", nil)
	done := make(chan error, 1)
	go func() {
		_, err := Reconnect(ctx, bridge, store, tail, "session-replay", session.EventCursor{Limit: 10}, session.ContentLimits{}, false)
		done <- err
	}()
	<-tail.subscribed
	tail.events <- session.EventRecord{
		Kind:      runtime.EventRunFinished,
		ID:        "evt-finished",
		SessionID: "session-replay",
		MessageID: "assistant-1",
	}
	// Deliberately a DIFFERENT messageID than the snapshot's "assistant-1":
	// Bridge.projectedMessages already marks "assistant-1" delivered (via
	// the snapshot above), and emitMessageDelta now drops any further
	// delta naming an already-projected message as stale (W7 fourth
	// fix-pass review P0-1) -- this test's purpose is to prove the live
	// loop keeps forwarding NEW events after RUN_FINISHED, not to exercise
	// that dedup, so it uses a message the snapshot never covered.
	tail.events <- session.EventRecord{
		Kind:      runtime.EventMessageDelta,
		SessionID: "session-replay",
		MessageID: "assistant-2",
		Payload:   []byte(`{"content":"live","reasoning":""}`),
		LiveOnly:  true,
	}
	close(tail.events)
	if err := <-done; err != nil {
		t.Fatalf("Reconnect err = %v", err)
	}
	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	want := "TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT,TEXT_MESSAGE_END,CUSTOM,RUN_STARTED,RUN_FINISHED,TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s", got, want)
	}
}

func TestReconnectReportsTailOverflow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := replayStore(t)
	tail := newReplayTail()
	sink := newSSESink()
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), "session-replay", "run-1", nil)
	done := make(chan error, 1)
	go func() {
		_, err := Reconnect(ctx, bridge, store, tail, "session-replay", session.EventCursor{Limit: 10}, session.ContentLimits{}, false)
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
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), "session-replay", "run-1", nil)
	done := make(chan error, 1)
	go func() {
		_, err := Reconnect(ctx, bridge, store, tail, "session-replay", session.EventCursor{Limit: 10}, session.ContentLimits{}, false)
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
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "assistant-1", SessionID: "session-replay", RunID: "run-1", Role: session.RoleAssistant, TurnID: "turn-1", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	textParts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "settled"}}},
	}, func() session.PartID { return "part-text" }, "assistant-1", "session-replay", "run-1", now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	textParts[0].Ordinal = 10
	if _, err := execution.AppendPart(ctx, textParts[0]); err != nil {
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
// not break emitMessageSnapshot: it now projects every durable message
// through the agentic pipeline (convert.ToAgenticProjection), which
// represents a user-role media block natively -- unlike the classic
// projector this replaced, which the storage-projection review flagged as
// permanently bricking replay for any session whose history contained a
// user-role media block.
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
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "user-media-1", SessionID: sessionID, RunID: "run-media", Role: session.RoleUser, TurnID: "turn-media", CreatedAt: now, UpdatedAt: now}); err != nil {
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
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), "run-media", nil)
	if err := emitMessageSnapshot(ctx, bridge, store, sessionID, session.ContentLimits{}, false); err != nil {
		t.Fatalf("emitMessageSnapshot error = %v, want nil (a user-role media block must not brick history.Load)", err)
	}
	// A user message has no native AG-UI STREAMING representation to
	// replay as (TEXT_MESSAGE_* covers assistant output, not input the
	// client already sent): CommittedNativeEvents legitimately returns
	// none for user_input_text/user_input_image, so both blocks surface as
	// their CUSTOM eino.agentic.v1 content-block supplement -- which is
	// exactly what proves the media block did not brick replay the way the
	// classic projector's ErrClassicUnsupported once did. emitMessageSnapshot
	// does NOT also emit a MESSAGES_SNAPSHOT here (W7 fix-pass review
	// finding P0-C reverted that: NativeMessage is user-role-only upstream,
	// so a snapshot built from it would reorder any transcript with
	// assistant messages in it -- see emitMessageSnapshot's doc comment) --
	// a native-only AG-UI client that never parses eino.agentic.v1 has no
	// representation of this user turn at all on this path.
	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	if stringsJoined(got) != "CUSTOM,CUSTOM" {
		t.Fatalf("event types = %#v, want two CUSTOM content-block supplements (no MESSAGES_SNAPSHOT)", got)
	}
	raw := string(sink.Bytes())
	if !strings.Contains(raw, "look at this") || !strings.Contains(raw, "https://example.com/pic.png") {
		t.Fatalf("replayed content missing text or media: %s", raw)
	}
}

// TestReplayToleratesMessageWithEmptyTurnID proves the W7 review's top P0
// finding (both reviewers independently reproduced it, C1/A1): one durable
// message with TurnID == "" must never brick replay for the whole session.
// Before this fix, agenticIdentity passed message.TurnID straight through,
// convert.validateIdentity rejected the empty string outright, and
// loadCommittedProjections failed its WHOLE BATCH on that one message --
// aborting emitMessageSnapshot before a single byte reached the client, for
// every message in the session, healthy or not. This affects all pre-W7
// durable data, every compaction boundary message (session/compaction did
// not stamp a TurnID until this same review pass), and every
// crash-interrupted tool result (runtime.settleInterruptedTool deliberately
// leaves TurnID empty).
func TestReplayToleratesMessageWithEmptyTurnID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-empty-turn"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-empty-turn", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-empty-turn", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})

	// The bad message: no TurnID at all, exactly like pre-W7 data, an
	// unstamped compaction boundary, or a crash-interrupted tool result.
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "assistant-no-turn", SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	noTurnParts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "no turn id here"}}},
	}, func() session.PartID { return "part-no-turn" }, "assistant-no-turn", sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(ctx, noTurnParts[0]); err != nil {
		t.Fatalf("append part: %v", err)
	}

	// A second, healthy message with a real TurnID: this is what must
	// still reach the client even though the first message is broken.
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "assistant-healthy", SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, TurnID: "turn-healthy", CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second)}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	healthyParts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "healthy content"}}},
	}, func() session.PartID { return "part-healthy" }, "assistant-healthy", sessionID, run.ID, now.Add(time.Second), session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(ctx, healthyParts[0]); err != nil {
		t.Fatalf("append part: %v", err)
	}

	sink := newSSESink()
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)
	if _, err := Replay(ctx, bridge, store, sessionID, session.EventCursor{Limit: 10}, session.ContentLimits{}, false); err != nil {
		t.Fatalf("Replay err = %v, want nil (a message with an empty TurnID must not brick the whole session's replay)", err)
	}
	raw := string(sink.Bytes())
	if !strings.Contains(raw, "no turn id here") {
		t.Fatalf("replayed content missing the empty-TurnID message's own text (it should still get a synthetic turn id, not be dropped): %s", raw)
	}
	if !strings.Contains(raw, "healthy content") {
		t.Fatalf("replayed content missing the healthy message's text: %s", raw)
	}
}

// TestReplayDoesNotReEmitAlreadySnapshottedMessage proves the W7 review's
// A3/finding I2: a durable session.MessageCommittedEventKind event naming a
// message emitMessageSnapshot already delivered must not be re-projected
// and re-emitted a second time. replay() itself forwards this event to
// bridge.Emit like any other non-LiveOnly durable event (see replay()'s doc
// comment); it is Bridge.projectedMessages/emitLiveMessageCommitted
// (agui/bridge.go) that recognizes the message ID is already recorded and
// skips it outright, with no reload and no re-projection attempt. Before
// the fix this test pins, there was no such dedup at all: a redundant
// message_committed reprojected and re-emitted the SAME message a second
// time -- on an idle session this collided on the receipt key and latched
// an EncErr the replay loop never checked, silently masking any later
// genuine encoding error.
func TestReplayDoesNotReEmitAlreadySnapshottedMessage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-committed-skip"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-committed-skip", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-committed-skip", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "assistant-committed-skip", SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, TurnID: "turn-committed-skip", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	parts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "settled once"}}},
	}, func() session.PartID { return "part-committed-skip" }, "assistant-committed-skip", sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(ctx, parts[0]); err != nil {
		t.Fatalf("append part: %v", err)
	}
	// The durable notification a real turn would have published via
	// runtime.publishMessageCommitted right after this same content
	// committed.
	if _, err := execution.AppendEvent(ctx, session.EventRecord{
		ID: "evt-committed", SessionID: sessionID, RunID: run.ID, MessageID: "assistant-committed-skip",
		Kind: session.MessageCommittedEventKind, TurnID: "turn-committed-skip",
		Payload: []byte(`{"revision":1}`), CreatedAt: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("append message_committed event: %v", err)
	}

	sink := newSSESink()
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)
	if _, err := Replay(ctx, bridge, store, sessionID, session.EventCursor{Limit: 10}, session.ContentLimits{}, false); err != nil {
		t.Fatalf("Replay err = %v", err)
	}
	if err := bridge.EncErr(); err != nil {
		t.Fatalf("EncErr = %v, want nil (message_committed must not be re-projected and collide with the snapshot's own receipt)", err)
	}
	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	want := "TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT,TEXT_MESSAGE_END,CUSTOM"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s (Bridge must dedupe the redundant message_committed, not re-emit the already-snapshotted message as a second CUSTOM)", got, want)
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
