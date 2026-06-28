package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"
	einoschema "github.com/cloudwego/eino/schema"

	agentagui "github.com/mattsp1290/eino-agent/agui"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

var ErrUnauthorized = errors.New("transport unauthorized")
var ErrInvalidConfig = errors.New("invalid transport config")

// AuthFunc injects application auth and request-scoped values before handlers
// call runtime APIs. Applications own route layout and identity policy.
type AuthFunc func(context.Context, *http.Request) (context.Context, error)

// SessionFunc extracts the durable session ID from an application-owned route.
type SessionFunc func(*http.Request) (session.ID, error)

// Runtime is the runtime surface used by embeddable HTTP handlers.
type Runtime interface {
	Start(context.Context, runtime.Request) (runtime.Handle, error)
	Resume(context.Context, session.RunID) (runtime.Handle, error)
}

// Interruptor is the narrow surface for application interrupt endpoints.
type Interruptor interface {
	Interrupt(context.Context, string) error
}

// Tail is the live AG-UI event tail used by SSE reconnect handlers.
type Tail interface {
	Subscribe(context.Context, session.ID) (<-chan runtime.Event, error)
}

// SSEConfig wires a consuming server's route to the AG-UI replay/live-tail
// primitives without owning product-specific paths or auth policy.
type SSEConfig struct {
	Store      session.Store
	Tail       Tail
	Session    SessionFunc
	Auth       AuthFunc
	Cursor     func(*http.Request) session.EventCursor
	ThreadID   func(*http.Request, session.ID) string
	RunID      func(*http.Request) string
	OnComplete func(session.EventCursor, error)
}

// SSEHandler returns an http.Handler for AG-UI SSE reconnect streams.
func SSEHandler(config SSEConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if config.Session == nil {
			http.Error(w, "session extractor required", http.StatusInternalServerError)
			return
		}
		ctx := r.Context()
		if config.Auth != nil {
			next, err := config.Auth(ctx, r)
			if err != nil {
				writeAuthError(w, err)
				return
			}
			ctx = next
			r = r.WithContext(ctx)
		}
		if config.Store == nil {
			http.Error(w, "store required", http.StatusInternalServerError)
			return
		}
		sessionID, err := config.Session(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cursor := session.EventCursor{Limit: 100}
		if config.Cursor != nil {
			cursor = config.Cursor(r)
		}
		threadID := string(sessionID)
		if config.ThreadID != nil {
			threadID = config.ThreadID(r, sessionID)
		}
		runID := ""
		if config.RunID != nil {
			runID = config.RunID(r)
		}
		header := w.Header()
		header.Set("Content-Type", "text/event-stream")
		header.Set("Cache-Control", "no-cache")
		header.Set("Connection", "keep-alive")
		flusher, _ := w.(http.Flusher)
		tracked := &trackingWriter{ResponseWriter: w}
		writer := bufio.NewWriter(flushWriter{writer: tracked, flusher: flusher})
		bridge := agentagui.NewBridge(ctx, writer, sse.NewSSEWriter(), threadID, runID, nil)
		next, err := agentagui.Reconnect(ctx, bridge, config.Store, config.Tail, sessionID, cursor)
		if err != nil && !tracked.wrote {
			http.Error(w, err.Error(), http.StatusBadGateway)
			if config.OnComplete != nil {
				config.OnComplete(next, err)
			}
			return
		}
		_ = writer.Flush()
		if flusher != nil {
			flusher.Flush()
		}
		if config.OnComplete != nil {
			config.OnComplete(next, err)
		}
	})
}

// InterruptHandler adapts application-owned interrupt routes to runtime handles.
func InterruptHandler(auth AuthFunc, lookup func(context.Context, *http.Request) (Interruptor, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx := r.Context()
		if auth != nil {
			next, err := auth(ctx, r)
			if err != nil {
				writeAuthError(w, err)
				return
			}
			ctx = next
			r = r.WithContext(ctx)
		}
		handle, err := lookup(ctx, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		reason := r.URL.Query().Get("reason")
		if reason == "" {
			reason = "interrupt"
		}
		if err := handle.Interrupt(ctx, reason); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
}

// ResumeHandler adapts application-owned resume routes to runtime.Resume.
func ResumeHandler(auth AuthFunc, resume func(context.Context, *http.Request) (runtime.Handle, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx := r.Context()
		if auth != nil {
			next, err := auth(ctx, r)
			if err != nil {
				writeAuthError(w, err)
				return
			}
			ctx = next
			r = r.WithContext(ctx)
		}
		handle, err := resume(ctx, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Eino-Agent-Run-ID", string(handle.RunID()))
		w.WriteHeader(http.StatusAccepted)
	})
}

// DecodeMessages decodes an application request body into Eino messages for
// callers that want a small default JSON contract.
func DecodeMessages(r *http.Request) ([]*einoschema.Message, error) {
	var payload struct {
		Messages []*einoschema.Message `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if len(payload.Messages) == 0 {
		return nil, fmt.Errorf("messages required")
	}
	return payload.Messages, nil
}

func writeAuthError(w http.ResponseWriter, err error) {
	status := http.StatusForbidden
	message := "forbidden"
	if errors.Is(err, ErrUnauthorized) {
		status = http.StatusUnauthorized
		message = "unauthorized"
	}
	http.Error(w, message, status)
}

type trackingWriter struct {
	http.ResponseWriter
	wrote bool
}

func (w *trackingWriter) Write(data []byte) (int, error) {
	if len(bytes.TrimSpace(data)) > 0 {
		w.wrote = true
	}
	return w.ResponseWriter.Write(data)
}

type flushWriter struct {
	writer  http.ResponseWriter
	flusher http.Flusher
}

func (w flushWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	if w.flusher != nil {
		w.flusher.Flush()
	}
	return n, err
}
