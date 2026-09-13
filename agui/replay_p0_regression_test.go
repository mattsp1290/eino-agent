package agui

import (
	"context"
	"path/filepath"
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
// TestReplayDoesNotReEmitAlreadySnapshottedMessage cannot catch this: its
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
	// Assert the exact frame-type sequence, like the sibling tests in this
	// package (TestReconnectToleratesDuplicateLiveMessageCommittedReceipt,
	// TestReplayDoesNotReEmitAlreadySnapshottedMessage) already do. A substring check
	// against the raw SSE bytes (the previous version of this test) passes
	// even when LATE-TEXT is trapped inside the CUSTOM envelope's own JSON
	// payload with no native TEXT_MESSAGE_* events -- exactly the shape a
	// native-only AG-UI client renders as nothing (W7 fix-pass review
	// findings P0-1/C1/I2). Both assistant-early (delivered by the
	// snapshot, DeliveryModeReplay) and assistant-late (delivered by
	// emitLiveMessageCommitted with DeliveryModeCommittedOnly, because this
	// connection had not yet natively streamed any content for it --
	// bridge.nativeStreamed was empty at commit time) must each carry a
	// full native text message plus its custom supplement.
	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	want := "TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT,TEXT_MESSAGE_END,CUSTOM,TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT,TEXT_MESSAGE_END,CUSTOM"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s (assistant-late must get native TEXT_MESSAGE_* events too, not just a CUSTOM supplement)", got, want)
	}
	if messageID, _ := frames[0]["messageId"].(string); messageID != "assistant-early" {
		t.Fatalf("frame[0] messageId = %q, want assistant-early", messageID)
	}
	if delta, _ := frames[1]["delta"].(string); delta != "EARLY-TEXT" {
		t.Fatalf("frame[1] delta = %q, want EARLY-TEXT", delta)
	}
	if messageID, _ := frames[4]["messageId"].(string); messageID != "assistant-late" {
		t.Fatalf("frame[4] messageId = %q, want assistant-late (the late-committing message's own native TEXT_MESSAGE_START)", messageID)
	}
	if delta, _ := frames[5]["delta"].(string); delta != "LATE-TEXT" {
		t.Fatalf("frame[5] delta = %q, want LATE-TEXT (committed during the replay window, must still reach the client natively)", delta)
	}
}

// TestReplaySurfacesLiveCommittedProjectionFailureDuringDurableSweep proves
// the W7 fix-pass review's C7/I4 finding: agui/replay.go:79-81's
// bridge.LiveErr() check, inside replay()'s own durable ListEvents sweep,
// is load-bearing on its own -- deleting it left the whole suite green
// (mutation M4), because the only existing coverage
// (transport/http_liveerr_test.go) exercises the sibling check in
// Reconnect's LIVE tail loop (agui/replay.go:137-139), not this one.
//
// Reuses lateCommitStore's exact scenario (a message that commits strictly
// during replay()'s own ListEvents sweep, after emitMessageSnapshot's batch
// load already ran) but with a caller ContentLimits too small to decode the
// late message's content -- the exact SSEConfig.ContentLimits/ContentLimits
// mismatch transport/http.go's doc comment warns about, here landing on the
// DURABLE sweep instead of the live-tail path
// TestSSEHandlerSurfacesLiveCommittedProjectionFailureThroughOnComplete
// covers. Before the C7 fix, replay() silently swallowed this failure and
// returned a nil error, exactly like P1-E on the durable path instead of
// the live one.
func TestReplaySurfacesLiveCommittedProjectionFailureDuringDurableSweep(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-late-commit-fatal"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-late-commit-fatal", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-late-commit-fatal", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	fence := session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken}
	// Deliberately no pre-existing message: emitMessageSnapshot's own
	// initial load sees zero messages and succeeds trivially, isolating
	// the failure to the durable ListEvents sweep's own live-continuation
	// dispatch (emitLiveMessageCommitted), not the initial snapshot.
	lateStore := &lateCommitStore{Store: store, fence: fence, sessionID: sessionID}
	sink := newSSESink()
	// Mismatched against lateCommitStore's own (large, default) encoding
	// limits for "LATE-TEXT" -- the same mismatch lateAppendStore uses in
	// transport/http_liveerr_test.go, here reached through replay()'s
	// durable sweep instead of Reconnect's live tail.
	tinyLimits := session.ContentLimits{MaxMessageBytes: 1024, MaxBlocks: 4, MaxBlockBytes: 1}
	bridge := NewBridge(ctx, lateStore, tinyLimits, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)
	_, err = Replay(ctx, bridge, lateStore, sessionID, session.EventCursor{Limit: 10}, tinyLimits, false)
	if err == nil {
		t.Fatalf("Replay err = nil, want a surfaced failure (a message that commits during replay()'s own durable sweep failed to decode under the caller's ContentLimits, and replay() must not silently swallow that)")
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

// TestReconnectToleratesLiveMessageCommittedNamingUnknownMessage proves the
// W7 fix-pass review's P0-2 finding (dedup-correctness-reviewer I1,
// wire-contract-reviewer C7's sibling): a live message_committed naming a
// message not (yet) present in this reload's committed projections is a
// BENIGN miss -- runtime.publishMessageCommitted documents itself as
// best-effort and appends/publishes its notification in a separate call
// strictly AFTER the message's content transaction commits, so a reload
// through a different session.Store handle (a read replica, a long-held
// repeatable-read snapshot, ...) can observe the notification before the
// content it names. Before this fix, emitLiveMessageCommitted latched this
// exact case into Bridge.liveErr indistinguishably from a genuine hard
// reload failure, and cadfc7a made Reconnect's live loop return on any
// non-nil LiveErr() -- so this benign race silently killed the stream: no
// RUN_ERROR, no RUN_FINISHED, nothing wrong reported anywhere.
func TestReconnectToleratesLiveMessageCommittedNamingUnknownMessage(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storeCtx := context.Background()
	store, storePool, err := openTestSQLite(storeCtx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-unknown-commit"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(storeCtx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(storeCtx, session.Run{ID: "run-unknown-commit", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-unknown-commit", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	// Deliberately no durable message for "assistant-never-written": the
	// session has zero messages, so loadCommittedProjections' reload
	// succeeds with an empty projection set, and the message named below is
	// never found in it -- the benign-miss branch, not the hard-failure one.

	tail := newReplayTail()
	sink := newSSESink()
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)
	done := make(chan error, 1)
	go func() {
		_, err := Reconnect(ctx, bridge, store, tail, sessionID, session.EventCursor{Limit: 10}, session.ContentLimits{}, false)
		done <- err
	}()
	<-tail.subscribed
	const timeout = 5 * time.Second
	sendOrTimeout := func(event session.EventRecord) {
		t.Helper()
		select {
		case tail.events <- event:
		case err := <-done:
			t.Fatalf("Reconnect returned (err = %v) before the test finished delivering live events -- it exited early on the benign unknown-message commit instead of tolerating it", err)
		case <-time.After(timeout):
			t.Fatalf("timed out sending event %s to the live tail", event.ID)
		}
	}
	sendOrTimeout(session.EventRecord{
		ID: "evt-unknown-commit", SessionID: sessionID, RunID: run.ID, MessageID: "assistant-never-written",
		Kind: session.MessageCommittedEventKind, TurnID: "turn-unknown-commit", Payload: []byte(`{"revision":1}`),
	})
	sendOrTimeout(session.EventRecord{Kind: runtime.EventRunFinished, ID: "evt-unknown-finished", SessionID: sessionID, MessageID: "assistant-never-written"})
	close(tail.events)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Reconnect err = %v, want nil (a message_committed naming a message not yet visible to this reload must not kill the stream)", err)
		}
	case <-time.After(timeout):
		t.Fatalf("Reconnect did not return after the live tail closed")
	}
	if err := bridge.LiveErr(); err != nil {
		t.Fatalf("LiveErr() = %v, want nil (a benign not-found miss must not be latched as a live error)", err)
	}
	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	want := "RUN_FINISHED"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s (the unknown-message commit must be skipped silently -- no CUSTOM, no natives -- and RUN_FINISHED must still be delivered)", got, want)
	}
}

// TestReconnectDoesNotDuplicateNativeContentForMessageCommittedDuringReplayWindow
// proves the fourth W7 fix-pass review's P0-1 finding
// (phase-state-reviewer C1 / final-coherence-reviewer C1,
// reviews/w7-fixes3-2026-09-12/): a message that commits strictly during
// replay()'s own ListEvents sweep -- reusing lateCommitStore's exact
// scenario from TestReplayForwardsMessageCommittedDuringReplayWindow -- is
// delivered with DeliveryModeCommittedOnly (native TEXT_MESSAGE_* plus
// CUSTOM), because this connection has not yet natively streamed any
// content for it. Reconnect subscribes to the live tail BEFORE calling
// replay() (see Reconnect's doc comment), so a stale runtime.EventMessageDelta
// for that SAME message -- one that was, in production, published to the
// live tail before its content ever committed, sat buffered in the tail's
// channel through the whole replay window, and is only now drained by the
// live loop -- can still arrive after the sweep already delivered that
// message in full. Before this fix, emitMessageDelta had no way to know the
// message it names was already fully, natively delivered by the sweep
// (Bridge.projectedMessages), and unconditionally re-emitted native
// TEXT_MESSAGE_* frames for it: a native-only AG-UI client would render
// "LATE-TEXT" twice and receive a second TEXT_MESSAGE_START for a message
// it was already told had ended.
//
// This MUST drive Reconnect, not Replay: Replay never subscribes to a live
// tail at all, so it cannot reproduce the buffered-delta interaction, and
// per the review, transport.SSEHandler only ever calls Reconnect.
func TestReconnectDoesNotDuplicateNativeContentForMessageCommittedDuringReplayWindow(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storeCtx := context.Background()
	store, storePool, err := openTestSQLite(storeCtx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-late-commit-reconnect"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(storeCtx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(storeCtx, session.Run{ID: "run-late-commit-reconnect", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-late-commit-reconnect", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	fence := session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken}
	// Deliberately no pre-existing messages: emitMessageSnapshot's own
	// initial load sees zero messages, isolating "assistant-late" entirely
	// to lateCommitStore's ListEvents-time injection below (the exact
	// scenario TestReplayForwardsMessageCommittedDuringReplayWindow uses).
	lateStore := &lateCommitStore{Store: store, fence: fence, sessionID: sessionID}

	tail := newReplayTail()
	sink := newSSESink()
	bridge := NewBridge(ctx, lateStore, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)
	done := make(chan error, 1)
	go func() {
		_, err := Reconnect(ctx, bridge, lateStore, tail, sessionID, session.EventCursor{Limit: 10}, session.ContentLimits{}, false)
		done <- err
	}()
	<-tail.subscribed

	const timeout = 5 * time.Second
	sendOrTimeout := func(event session.EventRecord) {
		t.Helper()
		select {
		case tail.events <- event:
		case err := <-done:
			t.Fatalf("Reconnect returned (err = %v) before the test finished delivering live events -- it exited early instead of tolerating the stale buffered delta", err)
		case <-time.After(timeout):
			t.Fatalf("timed out sending event %s to the live tail", event.ID)
		}
	}
	// A stale buffered delta for "assistant-late" -- the exact message
	// lateCommitStore.injectLateMessage durably commits during replay()'s
	// own ListEvents sweep, which the sweep therefore already delivered
	// natively (DeliveryModeCommittedOnly, since Bridge.nativeStreamed is
	// empty at that point). This delta carries no ID, matching every real
	// runtime.EventMessageDelta record (runtime/adk_model.go never sets
	// one), so agui/replay.go's seen[event.ID] guard cannot suppress it --
	// only Bridge.projectedMessages (emitMessageDelta's new guard) can.
	sendOrTimeout(session.EventRecord{
		Kind: runtime.EventMessageDelta, SessionID: sessionID, RunID: run.ID, MessageID: "assistant-late",
		Payload: []byte(`{"content":"LATE-TEXT","reasoning":""}`),
	})
	sendOrTimeout(session.EventRecord{Kind: runtime.EventRunFinished, ID: "evt-late-finished", SessionID: sessionID, MessageID: "assistant-late"})
	close(tail.events)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Reconnect err = %v, want nil", err)
		}
	case <-time.After(timeout):
		t.Fatalf("Reconnect did not return after the live tail closed")
	}
	if err := bridge.EncErr(); err != nil {
		t.Fatalf("EncErr = %v, want nil", err)
	}
	if err := bridge.LiveErr(); err != nil {
		t.Fatalf("LiveErr = %v, want nil", err)
	}
	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	// Exactly one native TEXT_MESSAGE_START/_CONTENT/_END triplet (from the
	// sweep's DeliveryModeCommittedOnly commit projection) plus its CUSTOM
	// supplement, then RUN_FINISHED. Before this fix, the stale buffered
	// delta processed by the live loop after the sweep added a SECOND
	// native TEXT_MESSAGE_START/_CONTENT pair (delta events never close
	// their own text span) between CUSTOM and RUN_FINISHED.
	want := "TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT,TEXT_MESSAGE_END,CUSTOM,RUN_FINISHED"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s (assistant-late's native content must reach the client exactly once -- from the sweep's commit projection -- not a second time from the stale buffered delta)", got, want)
	}
	if messageID, _ := frames[0]["messageId"].(string); messageID != "assistant-late" {
		t.Fatalf("frame[0] messageId = %q, want assistant-late", messageID)
	}
	if delta, _ := frames[1]["delta"].(string); delta != "LATE-TEXT" {
		t.Fatalf("frame[1] delta = %q, want LATE-TEXT (the sweep's own native projection of the durably committed content)", delta)
	}
}

// TestReconnectDeliversLiveDeltaForUnfinalizedAssistantPlaceholder proves
// the W7 fifth fix-pass review's P0-2 finding: a mid-stream reconnect must
// not drop the entire in-flight assistant turn it reconnected to watch.
//
// runtime/adk_model.go appends the assistant message ROW before the model
// streams a single token (currentMessageID is called from begin(), and
// content parts are only written later, at persistAssistantTurn) --
// store/internal/sqlstore/messages.go's ListMessages/loadReplayMessages has
// no "finalized" predicate, so this in-flight, zero-part row is exactly
// what a reconnecting client's own emitMessageSnapshot reload sees for the
// message it is reconnecting to watch. Before this fix,
// loadCommittedProjections projected that placeholder anyway (producing a
// zero-block AgenticMessage that emitted no frames) and the caller
// (emitMessageSnapshot) still called markMessageProjected on it -- so every
// live EventMessageDelta this SAME connection then received for that exact
// message id was immediately dropped by emitMessageDelta's messageProjected
// guard as "necessarily stale", even though nothing had actually reached
// the client yet. The fix applies the same lesson
// runtime.dropUnfinalizedAssistantPlaceholders already encodes on the
// model-input side (runtime/adk_model.go): loadCommittedProjections now
// excludes a zero-content-block assistant row from what it returns, so
// emitMessageSnapshot never marks it projected, and the live delta below
// reaches the wire normally.
func TestReconnectDeliversLiveDeltaForUnfinalizedAssistantPlaceholder(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storeCtx := context.Background()
	store, storePool, err := openTestSQLite(storeCtx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-unfinalized-placeholder"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(storeCtx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(storeCtx, session.Run{ID: "run-unfinalized-placeholder", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-unfinalized-placeholder", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	// The unfinalized placeholder: a row exists (visible to ListMessages),
	// but it carries zero content parts and store/internal/sqlstore never
	// received a FinalizeAssistantMessage call for it -- exactly the shape
	// the real runtime produces between begin() and persistAssistantTurn.
	const messageID session.MessageID = "assistant-inflight"
	if _, err := execution.AppendMessage(ctx, session.Message{
		ID: messageID, SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, TurnID: "turn-inflight", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("append message: %v", err)
	}

	tail := newReplayTail()
	sink := newSSESink()
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)
	done := make(chan error, 1)
	go func() {
		_, err := Reconnect(ctx, bridge, store, tail, sessionID, session.EventCursor{Limit: 10}, session.ContentLimits{}, false)
		done <- err
	}()
	<-tail.subscribed

	const timeout = 5 * time.Second
	sendOrTimeout := func(event session.EventRecord) {
		t.Helper()
		select {
		case tail.events <- event:
		case err := <-done:
			t.Fatalf("Reconnect returned (err = %v) before the test finished delivering live events", err)
		case <-time.After(timeout):
			t.Fatalf("timed out sending event %s to the live tail", event.ID)
		}
	}
	// The turn's own streaming text, arriving live -- exactly what a real
	// reconnecting client is waiting to see for the message it reconnected
	// mid-stream to watch.
	sendOrTimeout(session.EventRecord{
		Kind: runtime.EventMessageDelta, SessionID: sessionID, RunID: run.ID, MessageID: messageID,
		Payload: []byte(`{"content":"HELLO-LIVE","reasoning":""}`),
	})
	sendOrTimeout(session.EventRecord{Kind: runtime.EventRunFinished, ID: "evt-inflight-finished", SessionID: sessionID, MessageID: messageID})
	close(tail.events)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Reconnect err = %v, want nil", err)
		}
	case <-time.After(timeout):
		t.Fatalf("Reconnect did not return after the live tail closed")
	}
	if err := bridge.EncErr(); err != nil {
		t.Fatalf("EncErr = %v, want nil", err)
	}
	if err := bridge.LiveErr(); err != nil {
		t.Fatalf("LiveErr = %v, want nil", err)
	}
	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	// The snapshot emits NOTHING for the unfinalized placeholder (zero
	// content blocks to project), so the very first frames on the wire are
	// the live delta's own native text (closed by Emit's own closeOpen
	// ahead of the terminal frame), followed by RUN_FINISHED. Before this
	// fix, "FINAL types: []" -- not one frame reached the wire for the
	// whole turn.
	want := "TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT,TEXT_MESSAGE_END,RUN_FINISHED"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s (a live delta for an in-flight, unfinalized assistant message must reach the client, not be dropped as stale)", got, want)
	}
	if messageID, _ := frames[0]["messageId"].(string); messageID != "assistant-inflight" {
		t.Fatalf("frame[0] messageId = %q, want assistant-inflight", messageID)
	}
	if delta, _ := frames[1]["delta"].(string); delta != "HELLO-LIVE" {
		t.Fatalf("frame[1] delta = %q, want HELLO-LIVE", delta)
	}
}
