package transport

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"
	"github.com/mattsp1290/eino-agui/emitter"

	agentagui "github.com/mattsp1290/eino-agent/agui"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/watch"
)

// SessionWatchConfig requires explicit host authorization and exact session
// resolution. Auth may explicitly permit public access. Writers must honor
// ResponseController deadlines; wrappers should expose Unwrap.
type SessionWatchConfig struct {
	Service      *watch.Service
	Auth         AuthFunc
	Session      SessionFunc
	WriteTimeout time.Duration
}

// SessionWatchHandler streams current AG-UI message/state snapshots. Every
// reconnect starts fresh; EventCursor and Last-Event-ID are not resume tokens.
func SessionWatchHandler(c SessionWatchConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c.Service == nil || c.Auth == nil || c.Session == nil || c.WriteTimeout <= 0 {
			http.Error(w, "invalid watch configuration", http.StatusInternalServerError)
			return
		}
		authCtx, err := c.Auth(r.Context(), r)
		if err != nil {
			writeAuthError(w, err)
			return
		}
		if authCtx == nil {
			http.Error(w, "invalid watch authorization", http.StatusInternalServerError)
			return
		}
		ctx, cancel := context.WithCancel(authCtx)
		defer cancel()
		stop := context.AfterFunc(r.Context(), cancel)
		defer stop()
		id, err := c.Session(r.WithContext(ctx))
		if err != nil || id == "" {
			http.Error(w, "invalid session", http.StatusBadRequest)
			return
		}
		controller := http.NewResponseController(w)
		if err = controller.SetWriteDeadline(time.Now().Add(c.WriteTimeout)); err != nil {
			http.Error(w, "watch writer requires deadlines", http.StatusInternalServerError)
			return
		}
		sub, err := c.Service.Watch(ctx, id)
		if err != nil {
			writeWatchError(w, err)
			return
		}
		defer sub.Close()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		output := &deadlineWriter{writer: w, controller: controller, timeout: c.WriteTimeout}
		writer := bufio.NewWriter(output)
		emit := emitter.NewEmitter(ctx, writer, sse.NewSSEWriter(), string(id), "", cancel)
		bridge := agentagui.NewWatchBridge(emit, sub.Initial())
		if err = bridge.Initial(); err != nil {
			return
		}
		for {
			update, nextErr := sub.Next(ctx)
			if nextErr != nil {
				if ctx.Err() == nil && emit.Err() == nil {
					// A safe bounded marker is best effort after streaming has started.
					emit.StateSnapshot(struct{ ResyncRequired bool }{true})
				}
				return
			}
			if err = bridge.Apply(update); err != nil {
				return
			}
		}
	})
}
func writeWatchError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	switch {
	case errors.Is(err, watch.ErrCapacity), errors.Is(err, watch.ErrClosed):
		status = http.StatusServiceUnavailable
	case errors.Is(err, session.ErrObservationTooLarge):
		status = http.StatusRequestEntityTooLarge
	case errors.Is(err, watch.ErrOptions):
		status = http.StatusBadRequest
	}
	http.Error(w, "session observation unavailable", status)
}

// Each synchronous network operation sets its own deadline. No other goroutine
// can extend it while that operation is blocked.
type deadlineWriter struct {
	writer     http.ResponseWriter
	controller *http.ResponseController
	timeout    time.Duration
}

func (w *deadlineWriter) Write(p []byte) (int, error) {
	if err := w.controller.SetWriteDeadline(time.Now().Add(w.timeout)); err != nil {
		return 0, err
	}
	n, err := w.writer.Write(p)
	if err != nil {
		return n, err
	}
	if err = w.controller.SetWriteDeadline(time.Now().Add(w.timeout)); err != nil {
		return n, err
	}
	return n, w.controller.Flush()
}
