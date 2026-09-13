package agui

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	sqlite "github.com/mattsp1290/eino-agent/store/sqlite"
)

// lateCommitStore wraps a real *sqlite.Store and, on the first ListEvents
// call naming sessionID, durably appends one more message plus its
// message_committed event BEFORE delegating to the real ListEvents. This
// simulates a message that commits strictly between emitMessageSnapshot's
// batch load (which already ran, by construction, before replay()'s own
// ListEvents loop starts) and this ListEvents sweep observing it -- the
// exact window the W7 fix-pass review's P0-B finding is about. Embedding
// the concrete *sqlite.Store (not the session.Store interface) means every
// other Store method, including session.ObservationReader, is promoted
// unchanged, so currentRevision's type assertion still finds it.
type lateCommitStore struct {
	*sqlite.Store
	fence     session.RunFence
	sessionID session.ID
	fired     bool
}

func (s *lateCommitStore) ListEvents(ctx context.Context, sessionID session.ID, cursor session.EventCursor) (session.EventBatch, error) {
	if !s.fired && sessionID == s.sessionID {
		s.fired = true
		if err := s.injectLateMessage(ctx); err != nil {
			return session.EventBatch{}, err
		}
	}
	return s.Store.ListEvents(ctx, sessionID, cursor)
}

func (s *lateCommitStore) injectLateMessage(ctx context.Context) error {
	execution := s.Execution(s.fence)
	now := time.Date(2026, 6, 28, 12, 0, 5, 0, time.UTC)
	if _, err := execution.AppendMessage(ctx, session.Message{
		ID: "assistant-late", SessionID: s.sessionID, RunID: s.fence.RunID,
		Role: session.RoleAssistant, TurnID: "turn-late", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return err
	}
	parts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b-late", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "LATE-TEXT"}}},
	}, func() session.PartID { return "part-late" }, "assistant-late", s.sessionID, s.fence.RunID, now, session.DefaultContentLimits())
	if err != nil {
		return err
	}
	if _, err := execution.AppendPart(ctx, parts[0]); err != nil {
		return err
	}
	_, err = execution.AppendEvent(ctx, session.EventRecord{
		ID: "evt-late-commit", SessionID: s.sessionID, RunID: s.fence.RunID, MessageID: "assistant-late",
		Kind: session.MessageCommittedEventKind, TurnID: "turn-late", Payload: []byte(`{"revision":2}`), CreatedAt: now,
	})
	return err
}

// TestReplayForwardsMessageCommittedDuringReplayWindow proves the W7
// fix-pass review's P0-B finding (fix-verification-reviewer C2,
// regression-surface-reviewer item 1/2): a message that commits strictly
// during replay()'s own ListEvents sweep -- after emitMessageSnapshot's
// batch load, so never covered by the snapshot -- must still reach the
// client on this same connection, not be silently dropped.
//
// Before this fix, replay() unconditionally skipped every
// session.MessageCommittedEventKind record regardless of whether the
// snapshot above it had actually covered that message (the skip was keyed
// on record.Kind, not on whether the message was actually already
// delivered), so this exact late-committing message's content was lost for
// the life of the connection: both by the skip itself, and because
// seen[record.ID] was still set for the skipped record, which would also
// have suppressed the same event's live-tail copy on a Reconnect.
// TestReplaySkipsMessageCommittedEvents cannot catch this: its
// message_committed event is appended before Replay is ever called, so the
// message it names is always already in the snapshot.
func TestReplayForwardsMessageCommittedDuringReplayWindow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-late-commit"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-late-commit", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-late-commit", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	fence := session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken}
	execution := store.Execution(fence)
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "assistant-early", SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, TurnID: "turn-early", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	earlyParts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b-early", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "EARLY-TEXT"}}},
	}, func() session.PartID { return "part-early" }, "assistant-early", sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(ctx, earlyParts[0]); err != nil {
		t.Fatalf("append part: %v", err)
	}

	lateStore := &lateCommitStore{Store: store, fence: fence, sessionID: sessionID}
	sink := newSSESink()
	bridge := NewBridge(ctx, lateStore, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)
	next, err := Replay(ctx, bridge, lateStore, sessionID, session.EventCursor{Limit: 10}, session.ContentLimits{}, false)
	if err != nil {
		t.Fatalf("Replay err = %v", err)
	}
	if next.AfterEventID != "evt-late-commit" {
		t.Fatalf("next cursor = %+v, want AfterEventID evt-late-commit (the late commit must be consumed, not skipped without delivery)", next)
	}
	if err := bridge.EncErr(); err != nil {
		t.Fatalf("EncErr = %v, want nil", err)
	}
	if err := bridge.LiveErr(); err != nil {
		t.Fatalf("LiveErr = %v, want nil", err)
	}
	raw := string(sink.Bytes())
	if !strings.Contains(raw, "EARLY-TEXT") {
		t.Fatalf("EARLY-TEXT (delivered by the snapshot) missing from replay: %s", raw)
	}
	if !strings.Contains(raw, "LATE-TEXT") {
		t.Fatalf("LATE-TEXT (committed during the replay window) missing from replay -- it was dropped: %s", raw)
	}
}

// TestReconnectToleratesDuplicateLiveMessageCommittedReceipt proves the W7
// fix-pass review's P0-A finding (fix-verification-reviewer C1): a live
// message_committed re-notifying a message the snapshot already delivered
// must not kill the stream.
//
// publishMessageCommitted (runtime/message_commit_event.go) appends and
// publishes its notification in a separate, best-effort call strictly
// AFTER the message's content already committed. On an active session that
// notification can therefore reach a reconnecting client purely through the
// live tail -- never through replay()'s own ListEvents sweep at all, if the
// notification is durably appended only after that sweep's last page
// already returned. Before this fix, the live loop's EncErr() check turned
// the resulting duplicate-receipt collision (eino-agui's
// allowAgenticReceipt latches "commit receipt has already been emitted"
// when the same identity/revision is re-projected) into a fatal error that
// aborted Reconnect before its follow-on RUN_FINISHED ever reached the
// client. Before the review's first fix pass, the same duplicate merely
// latched an EncErr nothing checked; this test's job is to prove the
// duplicate no longer happens at all, so the (still present, and now
// meaningful) EncErr check never trips on it.
func TestReconnectToleratesDuplicateLiveMessageCommittedReceipt(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storeCtx := context.Background()
	store, storePool, err := openTestSQLite(storeCtx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-dup-receipt"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(storeCtx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(storeCtx, session.Run{ID: "run-dup-receipt", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-dup-receipt", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	if _, err := execution.AppendMessage(storeCtx, session.Message{ID: "assistant-dup", SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, TurnID: "turn-dup", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	parts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b-dup", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "settled before reconnect"}}},
	}, func() session.PartID { return "part-dup" }, "assistant-dup", sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(storeCtx, parts[0]); err != nil {
		t.Fatalf("append part: %v", err)
	}
	// Deliberately no durable message_committed EventRecord for
	// "assistant-dup": in the race this reproduces, the notification has
	// not been durably appended by the time replay()'s ListEvents sweep
	// completes, so it reaches this connection ONLY via the live tail
	// below -- exactly like a message whose content settled moments
	// before Reconnect was called, with its commit notification still in
	// flight.

	tail := newReplayTail()
	sink := newSSESink()
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)
	done := make(chan error, 1)
	go func() {
		_, err := Reconnect(ctx, bridge, store, tail, sessionID, session.EventCursor{Limit: 10}, session.ContentLimits{}, false)
		done <- err
	}()
	<-tail.subscribed
	// Sends and the final receive are all guarded by a deadline (rather
	// than blocking sends/receives) so that a regression -- Reconnect
	// exiting early after the FIRST event, as it did before this fix --
	// fails this test outright instead of hanging it: with nothing left
	// draining tail.events, an unconditional second send would block
	// forever.
	const timeout = 5 * time.Second
	sendOrTimeout := func(event session.EventRecord) {
		t.Helper()
		select {
		case tail.events <- event:
		case err := <-done:
			t.Fatalf("Reconnect returned (err = %v) before the test finished delivering live events -- it exited early on the first live message_committed instead of tolerating the duplicate", err)
		case <-time.After(timeout):
			t.Fatalf("timed out sending event %s to the live tail", event.ID)
		}
	}
	sendOrTimeout(session.EventRecord{
		ID: "evt-dup-commit", SessionID: sessionID, RunID: run.ID, MessageID: "assistant-dup",
		Kind: session.MessageCommittedEventKind, TurnID: "turn-dup", Payload: []byte(`{"revision":1}`),
	})
	sendOrTimeout(session.EventRecord{Kind: runtime.EventRunFinished, ID: "evt-dup-finished", SessionID: sessionID, MessageID: "assistant-dup"})
	close(tail.events)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Reconnect err = %v, want nil (a duplicate live message_committed for an already-snapshotted message must not kill the stream)", err)
		}
	case <-time.After(timeout):
		t.Fatalf("Reconnect did not return after the live tail closed")
	}
	if err := bridge.EncErr(); err != nil {
		t.Fatalf("EncErr() = %v, want nil", err)
	}
	if err := bridge.LiveErr(); err != nil {
		t.Fatalf("LiveErr() = %v, want nil", err)
	}
	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	want := "TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT,TEXT_MESSAGE_END,CUSTOM,RUN_FINISHED"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s (the duplicate commit must be skipped silently, and RUN_FINISHED must still be delivered)", got, want)
	}
}
