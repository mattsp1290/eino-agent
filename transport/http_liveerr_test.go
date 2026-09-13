package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
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
}
