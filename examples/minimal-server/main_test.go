package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func TestMinimalServerStreamsAndReplaysAGUIEvents(t *testing.T) {
	t.Parallel()

	server, err := NewServer(context.Background(), filepath.Join(t.TempDir(), "minimal.db"))
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	runID := startRun(t, httpServer.URL, defaultSessionID, "hello")
	eventsURL := httpServer.URL + "/sessions/" + string(defaultSessionID) + "/events?run_id=" + string(runID)
	live := readSSEUntil(t, eventsURL, "RUN_FINISHED", 2*time.Second)
	for _, want := range []string{"RUN_STARTED", "TEXT_MESSAGE_CONTENT", "RUN_FINISHED"} {
		if !strings.Contains(live, want) {
			t.Fatalf("live SSE missing %s:\n%s", want, live)
		}
	}
	if !strings.Contains(live, string(runID)) {
		t.Fatalf("live SSE missing run id %s:\n%s", runID, live)
	}

	replay := readSSEUntil(t, eventsURL, "RUN_FINISHED", 2*time.Second)
	for _, want := range []string{"MESSAGES_SNAPSHOT", "RUN_STARTED", "RUN_FINISHED", "Minimal server received"} {
		if !strings.Contains(replay, want) {
			t.Fatalf("replay SSE missing %s:\n%s", want, replay)
		}
	}
}

func TestMinimalServerInterruptsActiveRun(t *testing.T) {
	t.Parallel()

	server, err := NewServer(context.Background(), filepath.Join(t.TempDir(), "minimal.db"))
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.config.Agent.Options["stream_delay_ms"] = "500"
	t.Cleanup(func() { _ = server.Close() })
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	runID := startRun(t, httpServer.URL, defaultSessionID, "interrupt me")
	req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/runs/"+string(runID)+"/interrupt?reason=test", nil)
	if err != nil {
		t.Fatalf("new interrupt request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post interrupt: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close interrupt body: %v", err)
		}
	}()
	if resp.StatusCode != http.StatusAccepted {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("interrupt status = %d body=%s", resp.StatusCode, payload)
	}
}

func TestMinimalServerRejectsControlRouteMethods(t *testing.T) {
	t.Parallel()

	server, err := NewServer(context.Background(), filepath.Join(t.TempDir(), "minimal.db"))
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	for _, target := range []string{
		"/sessions/" + string(defaultSessionID) + "/runs",
		"/runs/run-1/interrupt",
	} {
		resp, err := http.Get(httpServer.URL + target)
		if err != nil {
			t.Fatalf("get %s: %v", target, err)
		}
		payload, _ := io.ReadAll(resp.Body)
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close %s body: %v", target, err)
		}
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("GET %s status = %d body=%s", target, resp.StatusCode, payload)
		}
	}
}

func TestMinimalServerRejectsNullTerminalMessage(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(
		http.MethodPost,
		"/sessions/"+string(defaultSessionID)+"/runs",
		strings.NewReader(`{"messages":[null]}`),
	)
	response := httptest.NewRecorder()

	(&Server{}).ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "terminal message required") {
		t.Fatalf("body = %q, want terminal message error", response.Body.String())
	}
}

func TestMinimalServerCloseInterruptsActiveRunsBeforeClosingStore(t *testing.T) {
	t.Parallel()

	server, err := NewServer(context.Background(), filepath.Join(t.TempDir(), "minimal.db"))
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.config.Agent.Options["stream_delay_ms"] = "500"
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()

	_ = startRun(t, httpServer.URL, defaultSessionID, "close while running")
	if err := server.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
}

func startRun(t *testing.T, baseURL string, sessionID session.ID, message string) session.RunID {
	t.Helper()
	body, err := json.Marshal(map[string]string{"message": message})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	resp, err := http.Post(baseURL+"/sessions/"+string(sessionID)+"/runs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post run: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close response body: %v", err)
		}
	}()
	payload, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", resp.StatusCode, payload)
	}
	var decoded struct {
		RunID session.RunID `json:"run_id"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if decoded.RunID == "" {
		t.Fatalf("missing run id in response: %s", payload)
	}
	return decoded.RunID
}

func readSSEUntil(t *testing.T, url string, marker string, timeout time.Duration) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get SSE: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close SSE body: %v", err)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("SSE status = %d body=%s", resp.StatusCode, payload)
	}

	var buf bytes.Buffer
	tmp := make([]byte, 256)
	for !strings.Contains(buf.String(), marker) {
		n, err := resp.Body.Read(tmp)
		if n > 0 {
			buf.Write(tmp[:n])
		}
		if err != nil {
			if ctx.Err() != nil {
				t.Fatalf("timed out waiting for %s in SSE:\n%s", marker, buf.String())
			}
			t.Fatalf("read SSE: %v\n%s", err, buf.String())
		}
	}
	cancel()
	return buf.String()
}
