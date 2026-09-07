package transport

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/sqlite"
	"github.com/mattsp1290/eino-agent/watch"
)

func watchFixture(t *testing.T) (*watch.Service, *sqlite.Store, SessionWatchConfig) {
	t.Helper()
	st, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "watch.db"))
	if err != nil {
		t.Fatal(err)
	}
	s, err := watch.NewService(st, watch.Options{Snapshot: session.ObservationLimits{MaxMessages: 10, MaxTools: 10, MaxParts: 20, MaxSnapshotBytes: 32 << 20, MaxTextBytes: 2 << 20}, PollInterval: 5 * time.Millisecond, ReadTimeout: time.Second, MaxSubscriptions: 10, MaxWatchedSessions: 10, MaxLiveRuns: 10, MaxLiveTextBytes: 2 << 20, PendingUpdates: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()); _ = st.Close() })
	return s, st, SessionWatchConfig{Service: s, Auth: func(ctx context.Context, _ *http.Request) (context.Context, error) { return ctx, nil }, Session: func(*http.Request) (session.ID, error) { return "s", nil }, WriteTimeout: 100 * time.Millisecond}
}
func TestSessionWatchHTTPFreshReconnectAndAuth(t *testing.T) {
	_, _, config := watchFixture(t)
	server := httptest.NewServer(SessionWatchHandler(config))
	defer server.Close()
	for range 2 {
		req, _ := http.NewRequestWithContext(t.Context(), "GET", server.URL, nil)
		req.Header.Set("Last-Event-ID", "invalid-history-cursor")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(resp.Body)
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(line, "MESSAGES_SNAPSHOT") {
			t.Fatal(line)
		}
		_ = resp.Body.Close()
	}
	config.Auth = func(context.Context, *http.Request) (context.Context, error) {
		return nil, fmt.Errorf("%w: SECRET", ErrUnauthorized)
	}
	rec := httptest.NewRecorder()
	SessionWatchHandler(config).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != http.StatusUnauthorized || strings.Contains(rec.Body.String(), "SECRET") {
		t.Fatal(rec)
	}
}
func TestSessionWatchRejectsUnsupportedWriterBeforeFrame(t *testing.T) {
	_, _, config := watchFixture(t)
	rec := httptest.NewRecorder()
	SessionWatchHandler(config).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "MESSAGES_SNAPSHOT") {
		t.Fatal(rec)
	}
}

type blockedWriteProbe struct {
	http.ResponseWriter
	started chan struct{}
	once    sync.Once
}

func (w *blockedWriteProbe) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *blockedWriteProbe) Write(p []byte) (int, error) {
	if len(p) > 1<<20 {
		w.once.Do(func() { close(w.started) })
	}
	return w.ResponseWriter.Write(p)
}
func TestSessionWatchConnectedNonReadingClientHasBoundedExit(t *testing.T) {
	service, st, config := watchFixture(t)
	ctx := t.Context()
	if _, err := st.CreateSession(ctx, session.Session{ID: "s"}); err != nil {
		t.Fatal(err)
	}
	run, err := st.AdmitRun(ctx, session.Run{ID: "r", SessionID: "s", Status: session.RunRunning, ClaimToken: "f"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ex := st.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	if _, err = ex.AppendMessage(ctx, session.Message{ID: "m", SessionID: "s", RunID: "r", Role: session.RoleAssistant}); err != nil {
		t.Fatal(err)
	}
	id := service.BeginAttempt(watch.LiveIdentity{SessionID: "s", RunID: "r", MessageID: "m", RequestID: "req"})
	service.AppendText(id, strings.Repeat("x", 2<<20))
	started, done := make(chan struct{}), make(chan struct{})
	handler := SessionWatchHandler(config)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		handler.ServeHTTP(&blockedWriteProbe{ResponseWriter: w, started: started}, r)
	}))
	server.Config.ConnState = func(conn net.Conn, state http.ConnState) {
		if state == http.StateNew {
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.SetWriteBuffer(1024)
			}
		}
	}
	server.Start()
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetReadBuffer(1024)
	}
	if _, err = fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: fixture\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("large live write never started")
	}
	select {
	case <-done:
		t.Fatal("write did not block")
	default:
	}
	startedAt := time.Now()
	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err = service.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(config.WriteTimeout + time.Second):
		t.Fatal("nonreading connected client pinned handler")
	}
	if elapsed := time.Since(startedAt); elapsed > config.WriteTimeout+time.Second {
		t.Fatal(elapsed)
	}
	// Closing observation leaves independent execution ownership intact.
	if err = ex.FinalizeAssistantMessage(ctx, "m"); err != nil {
		t.Fatal(err)
	}
}
