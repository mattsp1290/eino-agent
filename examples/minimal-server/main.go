package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/sqlite"
	"github.com/mattsp1290/eino-agent/transport"
	"github.com/mattsp1290/eino-agent/watch"
)

const (
	defaultSessionID = session.ID("minimal")
	workspaceID      = "minimal-server"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dbPath := flag.String("db", "minimal-server.db", "SQLite store path")
	flag.Parse()

	server, err := NewServer(context.Background(), *dbPath)
	if err != nil {
		log.Fatalf("open minimal server: %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			log.Printf("close store: %v", err)
		}
	}()

	log.Printf("minimal AG-UI server listening on %s", *addr)
	log.Printf("SSE:  GET  /sessions/%s/events", defaultSessionID)
	log.Printf("Run:  POST /sessions/%s/runs", defaultSessionID)
	if err := http.ListenAndServe(*addr, server); err != nil {
		log.Fatal(err)
	}
}

// Server is a small embeddable HTTP surface around the runtime and AG-UI
// transport adapters.
type Server struct {
	store    *sqlite.Store
	observer *watch.Service
	mount    *composition.Mount
	runtime  *runtime.StreamingOrchestrator
	config   config.Snapshot

	mu      sync.Mutex
	handles map[session.RunID]activeHandle
}

// NewServer opens local durable storage and wires the runtime to AG-UI SSE
// session state-watch handlers.
func NewServer(ctx context.Context, dbPath string) (*Server, error) {
	if dbPath == "" {
		dbPath = "minimal-server.db"
	}
	store, err := sqlite.Open(ctx, dbPath)
	if err != nil {
		return nil, err
	}
	ids := &sequenceIDs{prefix: rand.Text()}
	observer, err := watch.NewService(store, watch.Options{
		Snapshot:     session.ObservationLimits{MaxMessages: 50, MaxTools: 100, MaxParts: 200, MaxSnapshotBytes: 1 << 20, MaxTextBytes: 128 << 10},
		PollInterval: 50 * time.Millisecond, ReadTimeout: time.Second, MaxSubscriptions: 64, MaxWatchedSessions: 32, MaxLiveRuns: 32, MaxLiveTextBytes: 1 << 20, PendingUpdates: 64,
	})
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	snapshot := minimalConfig()
	plans, err := composition.NewRegistry(nil)
	if err != nil {
		_ = observer.Close(ctx)
		_ = store.Close()
		return nil, err
	}
	mount, err := mountScriptedTool(ctx, plans)
	if err != nil {
		_ = observer.Close(ctx)
		_ = store.Close()
		return nil, err
	}
	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(store),
		runtime.WithModelResolver(scriptedResolver{}),
		runtime.WithSessionObserver(observer),
		runtime.WithIDGenerator(ids),
		runtime.WithRunPlanProvider(plans),
		runtime.WithOwnerID("minimal-server"),
		runtime.WithQueueSize(16),
	)
	if err != nil {
		mount.Deactivate()
		_ = mount.Close(ctx)
		_ = observer.Close(ctx)
		_ = store.Close()
		return nil, err
	}
	return &Server{
		store:    store,
		observer: observer,
		mount:    mount,
		config:   snapshot,
		handles:  map[session.RunID]activeHandle{},
		runtime:  orchestrator,
	}, nil
}

func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	handles := make([]activeHandle, 0, len(s.handles))
	for _, handle := range s.handles {
		handles = append(handles, handle)
	}
	s.mu.Unlock()
	for _, handle := range handles {
		_ = handle.handle.Interrupt(context.Background(), "server closing")
	}
	deadline := time.After(2 * time.Second)
	for _, handle := range handles {
		select {
		case <-handle.done:
		case <-deadline:
			return fmt.Errorf("timed out waiting for run %s to stop during close", handle.handle.RunID())
		}
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if s.mount != nil {
		s.mount.Deactivate()
		if err := s.mount.Close(cleanupCtx); err != nil {
			return err
		}
	}
	if s.observer != nil {
		if err := s.observer.Close(cleanupCtx); err != nil {
			return err
		}
	}
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/healthz":
		w.WriteHeader(http.StatusNoContent)
	case strings.HasPrefix(r.URL.Path, "/sessions/"):
		s.serveSession(w, r)
	case strings.HasPrefix(r.URL.Path, "/runs/"):
		s.serveRunControl(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) serveSession(w http.ResponseWriter, r *http.Request) {
	sessionID, action, ok := parseSessionRoute(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	switch action {
	case "events":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		transport.SessionWatchHandler(transport.SessionWatchConfig{
			Service: s.observer, WriteTimeout: time.Second,
			Auth:    func(ctx context.Context, _ *http.Request) (context.Context, error) { return ctx, nil },
			Session: func(*http.Request) (session.ID, error) { return sessionID, nil },
		}).ServeHTTP(w, r)
	case "runs":
		s.startRun(w, r, sessionID)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) startRun(w http.ResponseWriter, r *http.Request, sessionID session.ID) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	message, err := decodeRunMessage(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	handle, err := s.runtime.Start(context.WithoutCancel(r.Context()), runtime.Request{
		SessionID: sessionID,
		Message:   message,
		Config:    s.config,
		Metadata:  map[string]string{"example": "minimal-server"},
	})
	if err != nil {
		status := http.StatusConflict
		if !errors.Is(err, session.ErrSessionBusy) {
			status = http.StatusBadGateway
		}
		http.Error(w, err.Error(), status)
		return
	}
	s.remember(handle)

	writeJSON(w, http.StatusAccepted, map[string]string{
		"session_id": string(sessionID),
		"run_id":     string(handle.RunID()),
		"events":     fmt.Sprintf("/sessions/%s/events", sessionID),
	})
}

func (s *Server) serveRunControl(w http.ResponseWriter, r *http.Request) {
	runID, action, ok := parseRunRoute(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	switch action {
	case "interrupt":
		transport.InterruptHandler(nil, func(context.Context, *http.Request) (transport.Interruptor, error) {
			return s.lookup(runID)
		}).ServeHTTP(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) remember(handle runtime.Handle) {
	active := activeHandle{handle: handle, done: make(chan struct{})}
	s.mu.Lock()
	s.handles[handle.RunID()] = active
	s.mu.Unlock()
	go s.releaseWhenDone(active)
}

func (s *Server) lookup(runID session.RunID) (runtime.Handle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	active, ok := s.handles[runID]
	if !ok {
		return nil, fmt.Errorf("run %s not active", runID)
	}
	return active.handle, nil
}

func (s *Server) releaseWhenDone(active activeHandle) {
	<-active.handle.Done()
	s.mu.Lock()
	delete(s.handles, active.handle.RunID())
	s.mu.Unlock()
	close(active.done)
}

type activeHandle struct {
	handle runtime.Handle
	done   chan struct{}
}

func minimalConfig() config.Snapshot {
	selection := model.Selection{ProviderID: "minimal", ModelID: "scripted"}
	cwd, _ := os.Getwd()
	return config.Snapshot{
		Agent: config.Agent{
			Name:         "minimal",
			SystemPrompt: "You are the minimal embedded AG-UI server example.",
			Model:        selection,
			Options:      map[string]string{"stream_delay_ms": "50"},
		},
		Model: selection,
		Metadata: map[string]string{
			"workspace_id":   workspaceID,
			"workspace_root": cwd,
		},
	}
}

func decodeRunMessage(r *http.Request) (runtime.UserMessage, error) {
	var payload struct {
		Message string `json:"message"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return runtime.UserMessage{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("exactly one JSON object required")
		}
		return runtime.UserMessage{}, err
	}
	if strings.TrimSpace(payload.Message) == "" {
		return runtime.UserMessage{}, fmt.Errorf("message required")
	}
	return runtime.UserMessage{Content: payload.Message}, nil
}

func parseSessionRoute(path string) (session.ID, string, bool) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(path, "/sessions/"), "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return session.ID(parts[0]), parts[1], true
}

func parseRunRoute(path string) (session.RunID, string, bool) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(path, "/runs/"), "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return session.RunID(parts[0]), parts[1], true
}

func methodNotAllowed(w http.ResponseWriter, method string) {
	w.Header().Set("Allow", method)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

type scriptedResolver struct{}

func (scriptedResolver) Resolve(_ context.Context, selection model.Selection, _ model.Runtime) (model.Resolved, error) {
	return model.Resolved{
		Provider: model.Provider{ID: selection.ProviderID, Name: "Minimal scripted provider", Source: "examples/minimal-server"},
		Model: model.Descriptor{
			ID:           selection.ModelID,
			ProviderID:   selection.ProviderID,
			Name:         "Scripted response",
			ContextLimit: 8192,
			OutputLimit:  512,
			Capabilities: map[string]bool{"streaming": true},
		},
		Streamer: scriptedStreamer{},
	}, nil
}

type scriptedStreamer struct{}

func (scriptedStreamer) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	delay := streamDelay(request.Options)
	reader, writer := einoschema.Pipe[model.StreamDelta](2)
	go func() {
		defer writer.Close()
		chunks := []*einoschema.Message{
			einoschema.AssistantMessage("Minimal server received ", nil),
			einoschema.AssistantMessage(lastUserText(request.Messages), nil),
		}
		if !currentTurnHasTool(request.Messages) {
			chunks = []*einoschema.Message{einoschema.AssistantMessage("Checking input. ", nil), einoschema.AssistantMessage("", []einoschema.ToolCall{{
				ID: rand.Text(), Type: "function", Function: einoschema.FunctionCall{Name: "echo", Arguments: `{"text":"safe scripted input"}`},
			}})}
		}
		for _, chunk := range chunks {
			select {
			case <-ctx.Done():
				writer.Send(model.StreamDelta{}, ctx.Err())
				return
			case <-time.After(delay):
			}
			if writer.Send(model.StreamDelta{Message: chunk}, nil) {
				return
			}
		}
	}()
	return reader, nil
}

func streamDelay(options map[string]string) time.Duration {
	if raw := options["stream_delay_ms"]; raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms >= 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return 50 * time.Millisecond
}

func lastUserText(messages []*einoschema.Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i] != nil && messages[i].Role == einoschema.User {
			return strconv.Quote(messages[i].Content)
		}
	}
	return "the request"
}

type sequenceIDs struct {
	prefix string
	mu     sync.Mutex
	n      int
}

func (s *sequenceIDs) next(prefix string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return s.prefix + "-" + prefix + "-" + strconv.Itoa(s.n)
}

func (s *sequenceIDs) NewRunID() session.RunID         { return session.RunID(s.next("run")) }
func (s *sequenceIDs) NewMessageID() session.MessageID { return session.MessageID(s.next("message")) }
func (s *sequenceIDs) NewPartID() session.PartID       { return session.PartID(s.next("part")) }
func (s *sequenceIDs) NewToolCallID() session.ToolCallID {
	return session.ToolCallID(s.next("tool-call"))
}
func (s *sequenceIDs) NewEventID() session.EventID { return session.EventID(s.next("event")) }
func (s *sequenceIDs) NewEpochID() session.EpochID { return session.EpochID(s.next("epoch")) }
