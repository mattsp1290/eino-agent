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
	handles map[session.RunID]runtime.Handle
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
	sink := eventSink{store: store, tail: tail, ids: ids, now: time.Now}
	snapshot := minimalConfig()
	return &Server{
		store:   store,
		tail:    tail,
		config:  snapshot,
		handles: map[session.RunID]runtime.Handle{},
		runtime: &runtime.StreamingOrchestrator{
			Store:      store,
			Transactor: store,
			Model:      scriptedResolver{},
			Events:     sink,
			IDs:        ids,
			OwnerID:    "minimal-server",
			QueueSize:  16,
		},
	}, nil
}

func (s *Server) Close() error {
	if s == nil {
		return nil
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
	go s.releaseWhenDone(handle)

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
	case "resume":
		transport.ResumeHandler(nil, func(ctx context.Context, _ *http.Request) (runtime.Handle, error) {
			handle, err := s.runtime.Resume(ctx, runID)
			if err != nil {
				return nil, err
			}
			s.remember(handle)
			go s.releaseWhenDone(handle)
			return handle, nil
		}).ServeHTTP(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) remember(handle runtime.Handle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handles[handle.RunID()] = handle
}

func (s *Server) lookup(runID session.RunID) (runtime.Handle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	handle, ok := s.handles[runID]
	if !ok {
		return nil, fmt.Errorf("run %s not active", runID)
	}
	return handle, nil
}

func (s *Server) releaseWhenDone(handle runtime.Handle) {
	<-handle.Done()
	s.mu.Lock()
	delete(s.handles, handle.RunID())
	s.mu.Unlock()
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

func (scriptedStreamer) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	delay := streamDelay(request.Options)
	reader, writer := einoschema.Pipe[*einoschema.Message](2)
	go func() {
		defer writer.Close()
		chunks := []*einoschema.Message{
			einoschema.AssistantMessage("Minimal server received ", nil),
			einoschema.AssistantMessage(lastUserText(request.Messages), nil),
		}
		for _, chunk := range chunks {
			select {
			case <-ctx.Done():
				writer.Send(nil, ctx.Err())
				return
			case <-time.After(delay):
			}
			if writer.Send(chunk, nil) {
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
	store session.Store
	tail  *stream.Tail
	ids   runtime.IDGenerator
	now   func() time.Time
}

func (s eventSink) Emit(ctx context.Context, event runtime.Event) error {
	if !event.LiveOnly && event.Kind != runtime.EventRunStarted {
		if event.EventID == "" && s.ids != nil {
			event.EventID = s.ids.NewEventID()
		}
		if _, err := s.store.AppendEvent(ctx, eventRecord(event, s.now)); err != nil {
			return err
		}
	}
	return s.tail.Emit(ctx, event)
}

func eventRecord(event runtime.Event, now func() time.Time) session.EventRecord {
	createdAt := event.Time
	if createdAt.IsZero() {
		createdAt = now().UTC()
	}
	return session.EventRecord{
		ID:         event.EventID,
		SessionID:  event.SessionID,
		RunID:      event.RunID,
		MessageID:  event.MessageID,
		PartID:     event.PartID,
		ToolCallID: event.ToolCallID,
		EpochID:    event.EpochID,
		ProviderID: event.ProviderID,
		ModelID:    event.ModelID,
		ParentID:   event.ParentID,
		Kind:       string(event.Kind),
		Usage: session.Usage{
			InputTokens:      event.Usage.InputTokens,
			OutputTokens:     event.Usage.OutputTokens,
			ReasoningTokens:  event.Usage.ReasoningTokens,
			CacheReadTokens:  event.Usage.CacheReadTokens,
			CacheWriteTokens: event.Usage.CacheWriteTokens,
			Cost:             event.Usage.Cost,
		},
		Error: session.EventError{
			Code:      event.Error.Code,
			Message:   event.Error.Message,
			Retryable: event.Error.Retryable,
		},
		Redaction: session.RedactionClass(event.Redaction),
		Payload:   event.Payload,
		LiveOnly:  event.LiveOnly,
		CreatedAt: createdAt,
	}
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
