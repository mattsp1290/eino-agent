package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	agentagui "github.com/mattsp1290/eino-agent/agui"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

// TestSSEHandlerEmitsRunErrorFrameOnHostDeadlineAfterWriting proves the
// fourth W7 fix-pass review's P0-2 finding
// (reviews/w7-fixes3-2026-09-12/phase-state-reviewer/01-critical-and-important.md
// C2): a host-imposed request deadline (context.DeadlineExceeded, e.g. from
// an AuthFunc-installed deadline or an upstream http.TimeoutHandler) leaves
// the CLIENT still connected, unlike a client disconnect. Before this fix,
// Reconnect returning ctx.Err() bound the SAME (already-Done) context to
// transport/http.go's bridge.Error call, and the AG-UI SDK's JSON encoder
// refuses to encode anything once ctx.Err() != nil (checked before
// encoding) -- so bridge.Error silently failed to write anything: no
// transport error either, since the failure classifies as an encoding
// error. The client got a 200 response that just stopped, indistinguishable
// from a completed turn. Bridge.Terminate fixes this by deriving an
// uncancelable context (context.WithoutCancel) for the terminal frame.
func TestSSEHandlerEmitsRunErrorFrameOnHostDeadlineAfterWriting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, t.TempDir()+"/store.db")
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-deadline-truncated"
	const healthyMessageID session.MessageID = "assistant-deadline-healthy"
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-deadline-truncated", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-deadline-truncated", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	if _, err := execution.AppendMessage(ctx, session.Message{
		ID: healthyMessageID, SessionID: sessionID, RunID: run.ID,
		Role: session.RoleAssistant, TurnID: "turn-deadline-healthy", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("append healthy message: %v", err)
	}
	healthyParts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b-healthy", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "healthy content"}}},
	}, func() session.PartID { return "part-deadline-healthy" }, healthyMessageID, sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(ctx, healthyParts[0]); err != nil {
		t.Fatalf("append healthy part: %v", err)
	}

	// Never sends anything: the live loop blocks in its select until the
	// request's own deadline fires.
	tail := newPushableTail()
	completed := make(chan struct{})
	var gotErr error
	handler := SSEHandler(SSEConfig{
		Store:   store,
		Tail:    tail,
		Session: func(*http.Request) (session.ID, error) { return sessionID, nil },
		RunID:   func(*http.Request) string { return string(run.ID) },
		OnComplete: func(_ session.EventCursor, err error) {
			gotErr = err
			close(completed)
		},
	})

	deadlineCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/events", nil).WithContext(deadlineCtx)
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
		t.Fatalf("OnComplete err = nil, want context.DeadlineExceeded")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (the healthy message was already written, so the status line cannot change)", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "healthy content") || !strings.Contains(body, "TEXT_MESSAGE_START") {
		t.Fatalf("body = %s, want the healthy message's native frames to have already been written", body)
	}
	if !strings.Contains(body, "RUN_ERROR") {
		t.Fatalf("body = %s, want a terminal RUN_ERROR frame: the client is still connected (a host deadline, not a client disconnect) and needs the truncation signal (P0-2)", body)
	}
	if strings.Contains(body, gotErr.Error()) {
		t.Fatalf("body = %s, want the RUN_ERROR frame to NOT include the raw context error text %q", body, gotErr.Error())
	}
	if !strings.Contains(body, agentagui.TerminalErrorMessage) {
		t.Fatalf("body = %s, want the RUN_ERROR frame to carry the fixed terminal error message %q", body, agentagui.TerminalErrorMessage)
	}
}

// TestSSEHandlerDoesNotEmitRunErrorFrameOnClientDisconnect proves the other
// half of P0-2/C5: a client disconnect (or graceful server shutdown, which
// looks identical from Reconnect's side -- both cancel the request context
// with context.Canceled rather than exceeding a deadline) must NOT produce
// a terminal RUN_ERROR frame. Nothing is listening to receive it, and (for
// a graceful shutdown) the earlier REJECTED claim in this pass's brief
// established that a client that IS still attached during a graceful
// shutdown was never able to receive one anyway (the encode fails before
// anything is written) -- so treating this case identically to a genuine
// failure would be new, incorrect behavior, not a fix.
func TestSSEHandlerDoesNotEmitRunErrorFrameOnClientDisconnect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, t.TempDir()+"/store.db")
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-disconnect"
	const healthyMessageID session.MessageID = "assistant-disconnect-healthy"
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-disconnect", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-disconnect", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	if _, err := execution.AppendMessage(ctx, session.Message{
		ID: healthyMessageID, SessionID: sessionID, RunID: run.ID,
		Role: session.RoleAssistant, TurnID: "turn-disconnect-healthy", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("append healthy message: %v", err)
	}
	healthyParts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b-healthy", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "healthy content"}}},
	}, func() session.PartID { return "part-disconnect-healthy" }, healthyMessageID, sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(ctx, healthyParts[0]); err != nil {
		t.Fatalf("append healthy part: %v", err)
	}

	tail := newPushableTail()
	completed := make(chan struct{})
	var gotErr error
	handler := SSEHandler(SSEConfig{
		Store:   store,
		Tail:    tail,
		Session: func(*http.Request) (session.ID, error) { return sessionID, nil },
		RunID:   func(*http.Request) string { return string(run.ID) },
		OnComplete: func(_ session.EventCursor, err error) {
			gotErr = err
			close(completed)
		},
	})

	cancelCtx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/events", nil).WithContext(cancelCtx)
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
	// Simulate the client disconnecting (or a graceful server shutdown
	// cancelling every in-flight request context) once the healthy content
	// has had a chance to be written.
	time.Sleep(20 * time.Millisecond)
	cancel()

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
		t.Fatalf("OnComplete err = nil, want context.Canceled")
	}
	body := rec.Body.String()
	if strings.Contains(body, "RUN_ERROR") {
		t.Fatalf("body = %s, want NO terminal RUN_ERROR frame for a benign client disconnect/graceful shutdown (nothing is listening; this is not a truncation failure)", body)
	}
}

// TestSSEHandlerDoesNotDuplicateTerminalFrameAfterSuccessfulRunFinished
// proves the fourth W7 fix-pass review's P0-3 finding
// (final-coherence-reviewer C5 point 2 / phase-state-reviewer C3): once a
// durable RUN_FINISHED has already reached the wire, a subsequent
// Reconnect-ending error (here: runtime.EventTailOverflow, delivered right
// after the live RUN_FINISHED) must NOT also produce a terminal RUN_ERROR
// frame. AG-UI treats RUN_FINISHED and RUN_ERROR as equally terminal for a
// run; a client that already closed out this run as successful must not
// then see a contradicting RUN_ERROR for the same run.
func TestSSEHandlerDoesNotDuplicateTerminalFrameAfterSuccessfulRunFinished(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, t.TempDir()+"/store.db")
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-dedup-terminal"
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-dedup-terminal", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-dedup-terminal", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	// Deliberately no durable messages/events: RUN_FINISHED below arrives
	// only via the live tail, isolating this test to the terminal-frame
	// dedup itself rather than replay's own durable lifecycle forwarding.

	tail := newPushableTail()
	completed := make(chan struct{})
	var gotErr error
	handler := SSEHandler(SSEConfig{
		Store:   store,
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
	case tail.events <- session.EventRecord{Kind: runtime.EventRunFinished, ID: "evt-dedup-finished", SessionID: sessionID, RunID: run.ID}:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out delivering the live RUN_FINISHED event")
	}
	select {
	case tail.events <- session.EventRecord{Kind: runtime.EventTailOverflow, SessionID: sessionID, LiveOnly: true}:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out delivering the live tail-overflow event")
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
		t.Fatalf("OnComplete err = nil, want agui.ErrTailOverflow (the host must still learn the tail overflowed)")
	}
	body := rec.Body.String()
	if !strings.Contains(body, "RUN_FINISHED") {
		t.Fatalf("body = %s, want the successful RUN_FINISHED frame", body)
	}
	if strings.Contains(body, "RUN_ERROR") {
		t.Fatalf("body = %s, want NO RUN_ERROR frame after a successful RUN_FINISHED already reached the wire (P0-3: at most one terminal frame per run)", body)
	}
}
