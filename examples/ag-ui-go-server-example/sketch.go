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
	"errors"
	"net/http"
	"sync"

	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"
	einoschema "github.com/cloudwego/eino/schema"
	"github.com/mattsp1290/eino-agui/convert"

	agentagui "github.com/mattsp1290/eino-agent/agui"
	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	sqlitestore "github.com/mattsp1290/eino-agent/store/sqlite"
	toolagui "github.com/mattsp1290/eino-agent/tools/agui"
	"github.com/mattsp1290/eino-agent/transport"
)

// ErrAGUIResumeRequiresAppAdapter reports AG-UI resume payloads that must stay
// on the consuming app's existing streamed resume path until it has a runtime
// tool-call settlement adapter.
var ErrAGUIResumeRequiresAppAdapter = errors.New("ag-ui resume requires app-owned streamed resume adapter")

// ErrClientToolCompositionRequired reports a client-tool request without the
// composition registry needed to seal those tools into run plans.
var ErrClientToolCompositionRequired = errors.New("ag-ui client tool composition registry required")

// RunInput is the subset of AG-UI RunAgentInput data that eino-agent needs at
// admission time. The consuming server still owns its exact request body.
type RunInput struct {
	ThreadID string
	RunID    string
	Messages []aguitypes.Message
	Tools    []aguitypes.Tool
	Resume   []aguitypes.ResumeEntry

	// ClientToolGeneration is server-owned monotonic state. AG-UI RunAgentInput
	// does not provide a generation; consumers should assign this with a
	// per-session counter before calling MountClientTools.
	ClientToolGeneration uint64
	// ClientToolDispatcherArtifactID identifies the executable client dispatch
	// behavior and must change whenever that behavior changes.
	ClientToolDispatcherArtifactID string
}

// StartRequest converts an AG-UI request body into an eino-agent runtime
// admission request. The application supplies the session ID and immutable
// runtime config snapshot.
func StartRequest(sessionID session.ID, input RunInput, snapshot config.Snapshot) (runtime.Request, error) {
	if len(input.Resume) > 0 {
		return runtime.Request{}, ErrAGUIResumeRequiresAppAdapter
	}
	return runtime.Request{
		SessionID: sessionID,
		Input:     convert.ToEinoMessages(input.Messages),
		Config:    snapshot,
		Metadata: map[string]string{
			"agui_thread_id": input.ThreadID,
			"agui_run_id":    input.RunID,
		},
	}, nil
}

// SessionIDFromThreadID preserves ag-ui-go-server-example's thread identity as
// the durable conversation key used for replay and reconnect.
func SessionIDFromThreadID(threadID string) session.ID {
	return session.ID(threadID)
}

// ToolGenerations owns per-session client-tool definition revisions for the
// consuming server. The first generated value is 1.
type ToolGenerations struct {
	mu     sync.Mutex
	values map[session.ID]uint64
}

func (g *ToolGenerations) Next(sessionID session.ID) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.values == nil {
		g.values = map[session.ID]uint64{}
	}
	g.values[sessionID]++
	return g.values[sessionID]
}

// MountClientTools seals per-session AG-UI client definitions into the
// canonical composition registry. The host owns the returned mount and closes
// the prior generation before publishing a replacement.
func MountClientTools(ctx context.Context, registry *composition.Registry, dispatcher agentagui.ClientToolDispatcher, sessionID session.ID, input RunInput) (*composition.Mount, error) {
	if registry == nil {
		if len(input.Tools) == 0 {
			return nil, nil
		}
		return nil, ErrClientToolCompositionRequired
	}
	if len(input.Tools) == 0 {
		return nil, nil
	}
	return toolagui.MountClientTools(ctx, registry, agentagui.ClientToolSnapshot{
		SessionID: sessionID, Generation: input.ClientToolGeneration,
		DispatcherArtifactID: input.ClientToolDispatcherArtifactID, Tools: input.Tools,
	}, dispatcher)
}

// ReplayHandler builds the AG-UI SSE endpoint for durable replay plus live
// tailing. Fiber handlers can delegate to this through an adapter, or mirror
// these same transport.SSEConfig fields in Fiber's streaming writer.
func ReplayHandler(store session.Store, tail transport.Tail, sessionFromRequest transport.SessionFunc, runIDFromRequest func(*http.Request) string) http.Handler {
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
			if runIDFromRequest != nil {
				return runIDFromRequest(r)
			}
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
