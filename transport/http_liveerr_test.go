package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
	sqlite "github.com/mattsp1290/eino-agent/store/sqlite"
)

// lateAppendStore wraps a real *sqlite.Store and, on the first ListMessages
// call naming sessionID (emitMessageSnapshot's own batch load, which
// therefore sees an empty session and is a no-op), durably appends one
// message whose content will fail to decode under a caller-supplied tiny
// session.ContentLimits. This isolates a committed-projection failure to
// the LIVE path (emitLiveMessageCommitted) specifically: the message does
// not exist yet when the initial snapshot runs, so only a later live
// message_committed notification for it can trigger the failure.
type lateAppendStore struct {
	*sqlite.Store
	sessionID session.ID
	fence     session.RunFence
	messageID session.MessageID
	text      string
	fired     bool
}

func (s *lateAppendStore) ListMessages(ctx context.Context, sessionID session.ID, cursor session.ReplayCursor) (session.ReplayBatch, error) {
	batch, err := s.Store.ListMessages(ctx, sessionID, cursor)
	if err == nil && !s.fired && sessionID == s.sessionID {
		s.fired = true
		if injectErr := s.injectLateMessage(ctx); injectErr != nil {
			return batch, injectErr
		}
	}
	return batch, err
}

func (s *lateAppendStore) injectLateMessage(ctx context.Context) error {
	execution := s.Execution(s.fence)
	now := time.Now().UTC()
	if _, err := execution.AppendMessage(ctx, session.Message{
		ID: s.messageID, SessionID: s.sessionID, RunID: s.fence.RunID,
		Role: session.RoleAssistant, TurnID: "turn-liveerr-http", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return err
	}
	parts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: s.text}}},
	}, func() session.PartID { return "part-liveerr-http" }, s.messageID, s.sessionID, s.fence.RunID, now, session.DefaultContentLimits())
	if err != nil {
		return err
	}
	_, err = execution.AppendPart(ctx, parts[0])
	return err
}

// pushableTail is an EventTail double that lets the test push live events
// on demand, unlike blockingTail (which never delivers any).
type pushableTail struct {
	events     chan session.EventRecord
	subscribed chan struct{}
}

func newPushableTail() *pushableTail {
	return &pushableTail{events: make(chan session.EventRecord), subscribed: make(chan struct{})}
}

func (t *pushableTail) Subscribe(ctx context.Context, _ session.ID) (<-chan session.EventRecord, error) {
	close(t.subscribed)
	return t.events, nil
}

// TestSSEHandlerSurfacesLiveCommittedProjectionFailureThroughOnComplete
// proves the W7 fix-pass review's P1-E finding (fix-verification-reviewer
// I2): Bridge.LiveErr() must be reachable by a real transport.SSEHandler
// caller, not just agui's own package tests. Before this fix, a
// ContentLimits mismatch between transport.SSEConfig and the orchestrator
// that produced the durable content made every live message_committed
// reprojection fail inside loadCommittedProjections; emitLiveMessageCommitted
// recorded that failure only on Bridge.liveErr, which neither Reconnect nor
// SSEHandler consulted, so the stream went quiet for the rest of the
// connection and OnComplete fired with err == nil -- a silently truncated
// stream a host had no way to detect.
func TestSSEHandlerSurfacesLiveCommittedProjectionFailureThroughOnComplete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, t.TempDir()+"/store.db")
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-liveerr-http"
	const messageID session.MessageID = "assistant-liveerr-http"
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-liveerr-http", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-liveerr-http", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	fence := session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken}
	lateStore := &lateAppendStore{
		Store: store, sessionID: sessionID, fence: fence, messageID: messageID,
		text: "this text is longer than one byte, and will fail to decode under a MaxBlockBytes: 1 limit",
	}

	tail := newPushableTail()
	completed := make(chan struct{})
	var gotErr error
	handler := SSEHandler(SSEConfig{
		Store:   lateStore,
		Tail:    tail,
		Session: func(*http.Request) (session.ID, error) { return sessionID, nil },
		RunID:   func(*http.Request) string { return string(run.ID) },
		// Mismatched against the store's own (large, default) encoding
		// limits -- the exact scenario SSEConfig.ContentLimits's doc
		// comment (transport/http.go) warns about.
		ContentLimits: session.ContentLimits{MaxMessageBytes: 1024, MaxBlocks: 4, MaxBlockBytes: 1},
		OnComplete: func(_ session.EventCursor, err error) {
			gotErr = err
			close(completed)
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	rec := httptest.NewRecorder()
	served := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(served)
	}()

	select {
	case <-tail.subscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never subscribed to the tail")
	}
	select {
	case tail.events <- session.EventRecord{
		ID: "evt-liveerr-commit", SessionID: sessionID, RunID: run.ID, MessageID: messageID,
		Kind: session.MessageCommittedEventKind, TurnID: "turn-liveerr-http", Payload: []byte(`{"revision":1}`),
	}:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out delivering the live message_committed event")
	}
	close(tail.events)

	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnComplete was not called")
	}
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("handler.ServeHTTP did not return")
	}

	if gotErr == nil {
		t.Fatalf("OnComplete err = nil, want a surfaced live committed-projection failure instead of a silently truncated stream")
	}
	// This session's snapshot has zero durable messages at the time
	// Reconnect starts (the late message is only injected as a side effect
	// of the snapshot's own load, so it does not exist for that load), so
	// nothing is written to the wire before the live failure happens
	// below: tracked.wrote is still false, and SSEHandler still has a
	// non-200 status available to signal it. Assert the wire body directly
	// (not just the host-side OnComplete callback) so this test also
	// proves the client sees a real signal, not merely that the host did.
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d (no bytes were written before the failure, so a non-200 status is still available)", rec.Code, http.StatusBadGateway)
	}
	if body := rec.Body.String(); !strings.Contains(body, gotErr.Error()) {
		t.Fatalf("body = %q, want it to include the surfaced error message %q", body, gotErr.Error())
	}
}

// failOnNthListMessagesStore wraps a real *sqlite.Store and fails the Nth
// ListMessages call naming sessionID (1-indexed) with a synthetic store
// error, succeeding normally on every other call. Unlike lateAppendStore,
// this fails the RELOAD itself rather than a ContentLimits mismatch, so it
// can fail the live path's second loadCommittedProjections call without
// also being unable to decode the snapshot's first, healthy message.
type failOnNthListMessagesStore struct {
	*sqlite.Store
	sessionID session.ID
	failOn    int
	calls     int
}

var errSimulatedListMessagesFailure = errors.New("simulated store failure")

func (s *failOnNthListMessagesStore) ListMessages(ctx context.Context, sessionID session.ID, cursor session.ReplayCursor) (session.ReplayBatch, error) {
	if sessionID == s.sessionID {
		s.calls++
		if s.calls == s.failOn {
			return session.ReplayBatch{}, errSimulatedListMessagesFailure
		}
	}
	return s.Store.ListMessages(ctx, sessionID, cursor)
}

// TestSSEHandlerEmitsRunErrorFrameForStreamTruncatedAfterWriting proves the
// W7 fix-pass review's P0-3/C10 finding: once transport.SSEHandler has
// already written at least one byte of the SSE response, a fatal
// agui.Reconnect error (here: a live committed-projection reload failure,
// via LiveErr()) can no longer be signaled with a non-200 status --
// tracked.wrote is true, so the http.Error branch is skipped and the
// handler falls through to flushing whatever was already written. Before
// this fix, nothing further happened: OnComplete told only the SERVER the
// stream failed, and the CLIENT's response was a 200 SSE stream that just
// stopped, indistinguishable from a normal completed turn. This reproduces
// that shape with one healthy durable message the snapshot delivers
// successfully (so the response body is non-empty and tracked.wrote flips
// true) before failOnNthListMessagesStore fails the SECOND ListMessages
// call -- the one loadCommittedProjections issues for the live
// message_committed notification below.
func TestSSEHandlerEmitsRunErrorFrameForStreamTruncatedAfterWriting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, t.TempDir()+"/store.db")
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-liveerr-truncated"
	const healthyMessageID session.MessageID = "assistant-liveerr-healthy"
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-liveerr-truncated", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-liveerr-truncated", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	fence := session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken}
	execution := store.Execution(fence)
	// A healthy message under normal (default) content limits: the
	// snapshot's own ListMessages call (call #1) decodes and emits it
	// successfully, so the response has already written real bytes by the
	// time the live path's ListMessages call (#2) fails below.
	if _, err := execution.AppendMessage(ctx, session.Message{
		ID: healthyMessageID, SessionID: sessionID, RunID: run.ID,
		Role: session.RoleAssistant, TurnID: "turn-liveerr-truncated-healthy", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("append healthy message: %v", err)
	}
	healthyParts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b-healthy", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "healthy content"}}},
	}, func() session.PartID { return "part-liveerr-healthy" }, healthyMessageID, sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(ctx, healthyParts[0]); err != nil {
		t.Fatalf("append healthy part: %v", err)
	}

	failingStore := &failOnNthListMessagesStore{Store: store, sessionID: sessionID, failOn: 2}

	tail := newPushableTail()
	completed := make(chan struct{})
	var gotErr error
	handler := SSEHandler(SSEConfig{
		Store:   failingStore,
		Tail:    tail,
		Session: func(*http.Request) (session.ID, error) { return sessionID, nil },
		RunID:   func(*http.Request) string { return string(run.ID) },
		OnComplete: func(_ session.EventCursor, err error) {
			gotErr = err
			close(completed)
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	rec := httptest.NewRecorder()
	served := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(served)
	}()

	select {
	case <-tail.subscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never subscribed to the tail")
	}
	select {
	case tail.events <- session.EventRecord{
		ID: "evt-liveerr-truncated-commit", SessionID: sessionID, RunID: run.ID, MessageID: "assistant-liveerr-late",
		Kind: session.MessageCommittedEventKind, TurnID: "turn-liveerr-truncated-late", Payload: []byte(`{"revision":2}`),
	}:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out delivering the live message_committed event")
	}
	close(tail.events)

	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnComplete was not called")
	}
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("handler.ServeHTTP did not return")
	}

	if gotErr == nil {
		t.Fatalf("OnComplete err = nil, want a surfaced live committed-projection failure")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (bytes were already written, so the status line cannot change)", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "healthy content") || !strings.Contains(body, "TEXT_MESSAGE_START") {
		t.Fatalf("body = %s, want the healthy message's native frames to have already been written", body)
	}
	if !strings.Contains(body, "RUN_ERROR") {
		t.Fatalf("body = %s, want a terminal RUN_ERROR frame so the client can tell the stream was truncated instead of completed", body)
	}
	if !strings.Contains(body, gotErr.Error()) {
		t.Fatalf("body = %s, want the RUN_ERROR frame to include the surfaced error message %q", body, gotErr.Error())
	}
}
