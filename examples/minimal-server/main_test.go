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

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
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

func TestMinimalServerRejectsInvalidRunMessage(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"blank":             `{"message":"  \n"}`,
		"missing":           `{}`,
		"caller transcript": `{"messages":[{"role":"user","content":"old"}]}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(
				http.MethodPost,
				"/sessions/"+string(defaultSessionID)+"/runs",
				strings.NewReader(body),
			)
			response := httptest.NewRecorder()

			(&Server{}).ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestMinimalServerSequentialRunsUseDurableHistory(t *testing.T) {
	t.Parallel()

	server, err := NewServer(context.Background(), filepath.Join(t.TempDir(), "minimal.db"))
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	const sessionID session.ID = "sequential"
	var secondRunID session.RunID
	for _, prompt := range []string{"first-user", "second-user"} {
		runID := startRun(t, httpServer.URL, sessionID, prompt)
		waitForTerminalRun(t, server.store, runID, 2*time.Second)
		secondRunID = runID
	}

	requests, err := server.store.ListModelRequests(context.Background(), secondRunID, session.ModelRequestCursor{Limit: 10})
	if err != nil {
		t.Fatalf("ListModelRequests error = %v", err)
	}
	if len(requests.Records) != 1 {
		t.Fatalf("second-run model requests = %d, want 1", len(requests.Records))
	}
	var audited []runtime.AuditedMessage
	if err := json.Unmarshal(requests.Records[0].Messages, &audited); err != nil {
		t.Fatalf("decode audited messages: %v", err)
	}
	wantProvider := []struct {
		role    einoschema.RoleType
		content string
	}{
		{einoschema.User, "first-user"},
		{einoschema.Assistant, `Minimal server received "first-user"`},
		{einoschema.User, "second-user"},
	}
	if len(audited) != len(wantProvider) {
		t.Fatalf("second-run provider history length = %d, want %d", len(audited), len(wantProvider))
	}
	for index := range wantProvider {
		var message einoschema.Message
		if err := json.Unmarshal(audited[index].Canonical, &message); err != nil {
			t.Fatalf("decode provider message %d: %v", index, err)
		}
		if message.Role != wantProvider[index].role || message.Content != wantProvider[index].content {
			t.Fatalf("provider history[%d] = (%s, %q), want (%s, %q)", index, message.Role, message.Content, wantProvider[index].role, wantProvider[index].content)
		}
	}

	messages, err := runtime.LoadHistory(context.Background(), server.store, sessionID, history.Options{})
	if err != nil {
		t.Fatalf("LoadHistory error = %v", err)
	}
	want := []struct {
		role    einoschema.RoleType
		content string
	}{
		{einoschema.User, "first-user"},
		{einoschema.Assistant, `Minimal server received "first-user"`},
		{einoschema.User, "second-user"},
		{einoschema.Assistant, `Minimal server received "second-user"`},
	}
	if len(messages) != len(want) {
		t.Fatalf("history length = %d, want %d: %#v", len(messages), len(want), messages)
	}
	for index := range want {
		if messages[index].Role != want[index].role || messages[index].Content != want[index].content {
			t.Fatalf("history[%d] = (%s, %q), want (%s, %q)", index, messages[index].Role, messages[index].Content, want[index].role, want[index].content)
		}
	}
}

func waitForTerminalRun(t *testing.T, store session.Store, runID session.RunID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := store.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatalf("GetRun(%s) error = %v", runID, err)
		}
		switch run.Status {
		case session.RunCompleted:
			return
		case session.RunFailed, session.RunInterrupted:
			t.Fatalf("run %s settled as %s", runID, run.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("run %s did not settle within %s", runID, timeout)
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
