package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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
	"github.com/mattsp1290/eino-agent/stream"
	"github.com/mattsp1290/eino-agent/transport"
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
	store   *sqlite.Store
	tail    *stream.Tail
	runtime *runtime.StreamingOrchestrator
	config  config.Snapshot

	mu      sync.Mutex
	handles map[session.RunID]activeHandle
}

// NewServer opens local durable storage and wires the runtime to AG-UI SSE
// replay/live-tail handlers.
func NewServer(ctx context.Context, dbPath string) (*Server, error) {
	if dbPath == "" {
		dbPath = "minimal-server.db"
	}
	store, err := sqlite.Open(ctx, dbPath)
	if err != nil {
		return nil, err
	}
	ids := &sequenceIDs{}
	tail := stream.NewTail(64)
	sink := eventSink{tail: tail}
	snapshot := minimalConfig()
	plans, err := composition.NewRegistry(nil)
	if err != nil {
		tail.Close()
		_ = store.Close()
		return nil, err
	}
	orchestrator, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(store),
		runtime.WithModelResolver(scriptedResolver{}),
		runtime.WithEventSink(sink),
		runtime.WithIDGenerator(ids),
		runtime.WithRunPlanProvider(plans),
		runtime.WithOwnerID("minimal-server"),
		runtime.WithQueueSize(16),
	)
	if err != nil {
		tail.Close()
		_ = store.Close()
		return nil, err
	}
	return &Server{
		store:   store,
		tail:    tail,
		config:  snapshot,
		handles: map[session.RunID]activeHandle{},
		runtime: orchestrator,
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
			log.Printf("timed out waiting for run %s to stop during close", handle.handle.RunID())
		}
	}
	if s.tail != nil {
		s.tail.Close()
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
		transport.SSEHandler(transport.SSEConfig{
			Store:   s.store,
			Tail:    s.tail,
			Session: func(*http.Request) (session.ID, error) { return sessionID, nil },
			Cursor:  eventCursorFromRequest,
			ThreadID: func(_ *http.Request, id session.ID) string {
				return string(id)
			},
			RunID: func(r *http.Request) string {
				return r.URL.Query().Get("run_id")
			},
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
	messages, err := decodeRunMessages(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	handle, err := s.runtime.Start(context.WithoutCancel(r.Context()), runtime.Request{
		SessionID: sessionID,
		Input:     messages,
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

func decodeRunMessages(r *http.Request) ([]*einoschema.Message, error) {
	var payload struct {
		Message  string                `json:"message"`
		Messages []*einoschema.Message `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if len(payload.Messages) > 0 {
		return payload.Messages, nil
	}
	if strings.TrimSpace(payload.Message) == "" {
		return nil, fmt.Errorf("message or messages required")
	}
	return []*einoschema.Message{einoschema.UserMessage(payload.Message)}, nil
}

func eventCursorFromRequest(r *http.Request) session.EventCursor {
	cursor := session.EventCursor{Limit: 100}
	query := r.URL.Query()
	if after := query.Get("after"); after != "" {
		cursor.AfterEventID = session.EventID(after)
	}
	if limit := query.Get("limit"); limit != "" {
		if parsed, err := strconv.Atoi(limit); err == nil && parsed > 0 {
			cursor.Limit = parsed
		}
	}
	return cursor
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

type eventSink struct {
	tail *stream.Tail
}

func (s eventSink) Emit(ctx context.Context, event session.EventRecord) {
	s.tail.Emit(ctx, event)
}

type sequenceIDs struct {
	mu sync.Mutex
	n  int
}

func (s *sequenceIDs) next(prefix string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return prefix + "-" + strconv.Itoa(s.n)
}

func (s *sequenceIDs) NewRunID() session.RunID         { return session.RunID(s.next("run")) }
func (s *sequenceIDs) NewMessageID() session.MessageID { return session.MessageID(s.next("message")) }
func (s *sequenceIDs) NewPartID() session.PartID       { return session.PartID(s.next("part")) }
func (s *sequenceIDs) NewToolCallID() session.ToolCallID {
	return session.ToolCallID(s.next("tool-call"))
}
func (s *sequenceIDs) NewEventID() session.EventID { return session.EventID(s.next("event")) }
func (s *sequenceIDs) NewEpochID() session.EpochID { return session.EpochID(s.next("epoch")) }
