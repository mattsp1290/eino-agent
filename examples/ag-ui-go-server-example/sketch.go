// Package agui_go_server_example sketches how the Fiber-based
// ag-ui-go-server-example can embed eino-agent without copying runtime
// internals.
//
// The package intentionally does not import the consumer application's internal
// packages. A real integration should adapt these functions inside that app's
// existing Fiber route handlers and dependency container.
package agui_go_server_example

import (
	"context"
	"net/http"

	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"
	einoschema "github.com/cloudwego/eino/schema"
	"github.com/mattsp1290/eino-agui/convert"

	agentagui "github.com/mattsp1290/eino-agent/agui"
	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
	toolagui "github.com/mattsp1290/eino-agent/tools/agui"
	"github.com/mattsp1290/eino-agent/transport"
)

// RunInput is the subset of AG-UI RunAgentInput data that eino-agent needs at
// admission time. The consuming server still owns its exact request body.
type RunInput struct {
	ThreadID   string
	RunID      string
	Messages   []aguitypes.Message
	Tools      []aguitypes.Tool
	Generation uint64
}

// StartRequest converts an AG-UI request body into an eino-agent runtime
// admission request. The application supplies the session ID and immutable
// runtime config snapshot.
func StartRequest(sessionID session.ID, input RunInput, snapshot config.Snapshot) runtime.Request {
	return runtime.Request{
		SessionID: sessionID,
		Input:     convert.ToEinoMessages(input.Messages),
		Config:    snapshot,
		Metadata: map[string]string{
			"agui_thread_id": input.ThreadID,
			"agui_run_id":    input.RunID,
		},
	}
}

// SetClientTools installs per-session AG-UI client tool definitions into the
// eino-agent tool registry. The registry combines these with any server-side
// tools when runtime prepares a turn.
func SetClientTools(registry *toolagui.Registry, sessionID session.ID, input RunInput) error {
	if registry == nil || len(input.Tools) == 0 {
		return nil
	}
	return registry.SetClientTools(agentagui.ClientToolSnapshot{
		SessionID:  sessionID,
		Generation: input.Generation,
		Tools:      input.Tools,
	})
}

// ReplayHandler builds the AG-UI SSE endpoint for durable replay plus live
// tailing. Fiber handlers can delegate to this through an adapter, or mirror
// these same transport.SSEConfig fields in Fiber's streaming writer.
func ReplayHandler(store session.Store, tail transport.Tail, sessionFromRequest transport.SessionFunc) http.Handler {
	return transport.SSEHandler(transport.SSEConfig{
		Store:   store,
		Tail:    tail,
		Session: sessionFromRequest,
		Cursor: func(r *http.Request) session.EventCursor {
			return session.EventCursor{AfterEventID: session.EventID(r.URL.Query().Get("after")), Limit: 100}
		},
		ThreadID: func(_ *http.Request, id session.ID) string {
			return string(id)
		},
		RunID: func(r *http.Request) string {
			return r.URL.Query().Get("run_id")
		},
	})
}

// InterruptHandler adapts an application-owned run lookup to the stable
// eino-agent interrupt transport contract.
func InterruptHandler(lookup func(context.Context, *http.Request) (transport.Interruptor, error)) http.Handler {
	return transport.InterruptHandler(nil, lookup)
}

// OpenLocalStore uses eino-agent's SQLite store for local durable sessions,
// messages, parts, runs, tool calls, context epochs, and replayable events.
func OpenLocalStore(ctx context.Context, path string) (*sqlitestore.Store, error) {
	return sqlitestore.Open(ctx, path)
}

// ClientToolInfos exposes the lower-level conversion when the consuming server
// needs to bind AG-UI client tools directly to an Eino model outside the full
// runtime orchestrator.
func ClientToolInfos(tools []aguitypes.Tool) ([]*einoschema.ToolInfo, error) {
	return agentagui.ClientToolInfos(tools)
}
