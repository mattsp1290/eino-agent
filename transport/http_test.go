package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
)

func TestSSEHandlerAppliesAuthAndReplayCursor(t *testing.T) {
	t.Parallel()

	store := replayStore(t)
	tail := &closingTail{done: make(chan struct{})}
	var gotCursor session.EventCursor
	handler := SSEHandler(SSEConfig{
		Store: store,
		Tail:  tail,
		Auth: func(ctx context.Context, r *http.Request) (context.Context, error) {
			if r.Header.Get("Authorization") != "Bearer ok" {
				return ctx, ErrUnauthorized
			}
			return context.WithValue(ctx, authKey{}, "tenant-1"), nil
		},
		Session: func(r *http.Request) (session.ID, error) {
			if r.Context().Value(authKey{}) != "tenant-1" {
				return "", errors.New("missing tenant")
			}
			return "session-http", nil
		},
		Cursor: func(r *http.Request) session.EventCursor {
			return session.EventCursor{AfterEventID: session.EventID(r.URL.Query().Get("after")), Limit: 10}
		},
		RunID: func(*http.Request) string {
			return "run-http"
		},
		OnComplete: func(cursor session.EventCursor, _ error) {
			gotCursor = cursor
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/events?after=evt-started", nil)
	req.Header.Set("Authorization", "Bearer ok")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "RUN_FINISHED") {
		t.Fatalf("SSE body = %s, want RUN_FINISHED", rec.Body.String())
	}
	if gotCursor.AfterEventID != "evt-finished" {
		t.Fatalf("cursor = %+v, want evt-finished", gotCursor)
	}
}

func TestSSEHandlerRejectsUnauthorizedBeforeSessionExtraction(t *testing.T) {
	t.Parallel()

	calledSession := false
	handler := SSEHandler(SSEConfig{
		Auth: func(ctx context.Context, _ *http.Request) (context.Context, error) {
			return ctx, ErrUnauthorized
		},
		Session: func(*http.Request) (session.ID, error) {
			calledSession = true
			return "session", nil
		},
	})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if strings.Contains(rec.Body.String(), ErrUnauthorized.Error()) {
		t.Fatalf("auth body leaked internal error: %q", rec.Body.String())
	}
	if calledSession {
		t.Fatal("session extraction ran after auth failure")
	}
}

func TestSSEHandlerRequiresSessionExtractor(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	SSEHandler(SSEConfig{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestSSEHandlerCancelsTailOnDisconnect(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/events", nil).WithContext(ctx)
	tail := &blockingTail{subscribed: make(chan struct{}), canceled: make(chan struct{})}
	handler := SSEHandler(SSEConfig{
		Store:   replayStore(t),
		Tail:    tail,
		Session: func(*http.Request) (session.ID, error) { return "session-http", nil },
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(httptest.NewRecorder(), req)
	}()
	<-tail.subscribed
	cancel()
	select {
	case <-tail.canceled:
	case <-time.After(time.Second):
		t.Fatal("tail subscription was not canceled")
	}
	<-done
}

func TestSSEHandlerSurfacesReplayErrorsBeforeWriting(t *testing.T) {
	t.Parallel()

	handler := SSEHandler(SSEConfig{
		Store:   nil,
		Session: func(*http.Request) (session.ID, error) { return "session-http", nil },
	})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestInterruptHandlerCallsRuntimeHandle(t *testing.T) {
	t.Parallel()

	handle := &interruptHandle{}
	handler := InterruptHandler(func(ctx context.Context, _ *http.Request) (context.Context, error) {
		return context.WithValue(ctx, authKey{}, "tenant-1"), nil
	}, func(_ context.Context, r *http.Request) (Interruptor, error) {
		if r.Context().Value(authKey{}) != "tenant-1" {
			return nil, errors.New("missing tenant")
		}
		return handle, nil
	})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/runs/run-1/interrupt?reason=user", nil))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d", rec.Code)
	}
	if handle.reason != "user" {
		t.Fatalf("reason = %q, want user", handle.reason)
	}
}

func TestInterruptHandlerRejectsNonPost(t *testing.T) {
	t.Parallel()

	called := false
	handler := InterruptHandler(nil, func(context.Context, *http.Request) (Interruptor, error) {
		called = true
		return &interruptHandle{}, nil
	})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/runs/run-1/interrupt", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
	if called {
		t.Fatal("lookup called for non-POST interrupt")
	}
}

func TestResumeHandlerCallsRuntimeResumeSurface(t *testing.T) {
	t.Parallel()

	handle := resumeHandle{runID: "run-resume"}
	handler := ResumeHandler(nil, func(context.Context, *http.Request) (runtime.Handle, error) {
		return handle, nil
	})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/runs/run-resume/resume", nil))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d", rec.Code)
	}
	if rec.Header().Get("Eino-Agent-Run-ID") != "run-resume" {
		t.Fatalf("run header = %q", rec.Header().Get("Eino-Agent-Run-ID"))
	}
}

func TestResumeHandlerRejectsNonPost(t *testing.T) {
	t.Parallel()

	called := false
	handler := ResumeHandler(nil, func(context.Context, *http.Request) (runtime.Handle, error) {
		called = true
		return resumeHandle{runID: "run-resume"}, nil
	})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/runs/run-resume/resume", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
	if called {
		t.Fatal("resume called for non-POST")
	}
}

func replayStore(t *testing.T) session.Store {
	t.Helper()
	ctx := context.Background()
	store, err := sqlitestore.Open(ctx, t.TempDir()+"/store.db")
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: "session-http", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-http", SessionID: "session-http", OwnerID: "owner", ClaimToken: "claim-http", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	if _, err := execution.AppendMessage(ctx, session.Message{ID: "msg-http", SessionID: "session-http", RunID: "run-http", Role: session.RoleAssistant, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	if _, err := execution.AppendPart(ctx, session.Part{ID: "part-http", MessageID: "msg-http", SessionID: "session-http", RunID: "run-http", Kind: session.PartText, Payload: []byte(`{"text":"hello"}`), CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append part: %v", err)
	}
	events := []session.EventRecord{
		{ID: "evt-started", SessionID: "session-http", RunID: "run-http", MessageID: "msg-http", Kind: string(runtime.EventRunStarted), CreatedAt: now},
		{ID: "evt-finished", SessionID: "session-http", RunID: "run-http", MessageID: "msg-http", Kind: string(runtime.EventRunFinished), CreatedAt: now.Add(time.Second)},
	}
	for _, event := range events {
		if _, err := execution.AppendEvent(ctx, event); err != nil {
			t.Fatalf("append event: %v", err)
		}
	}
	return store
}

type authKey struct{}

type closingTail struct {
	done chan struct{}
}

func (t *closingTail) Subscribe(context.Context, session.ID) (<-chan runtime.Event, error) {
	ch := make(chan runtime.Event)
	close(ch)
	return ch, nil
}

type blockingTail struct {
	subscribed chan struct{}
	canceled   chan struct{}
}

func (t *blockingTail) Subscribe(ctx context.Context, _ session.ID) (<-chan runtime.Event, error) {
	close(t.subscribed)
	go func() {
		<-ctx.Done()
		close(t.canceled)
	}()
	return make(chan runtime.Event), nil
}

type interruptHandle struct {
	reason string
}

func (h *interruptHandle) Interrupt(_ context.Context, reason string) error {
	h.reason = reason
	return nil
}

type resumeHandle struct {
	runID session.RunID
}

func (h resumeHandle) RunID() session.RunID { return h.runID }
func (h resumeHandle) Done() <-chan runtime.Result {
	ch := make(chan runtime.Result)
	close(ch)
	return ch
}
func (h resumeHandle) Interrupt(context.Context, string) error { return nil }
