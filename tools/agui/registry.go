package agui

import (
	"context"
	"sync"

	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"

	agentagui "github.com/mattsp1290/eino-agent/agui"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

// Registry composes server-side tools with per-session AG-UI client tools.
type Registry struct {
	Server     runtime.ToolRegistry
	Dispatcher agentagui.ClientToolDispatcher

	mu          sync.RWMutex
	clients     map[session.ID]agentagui.ClientToolSnapshot
	generations map[session.ID]uint64
}

// NewRegistry returns a registry that resolves server tools plus client tools.
func NewRegistry(server runtime.ToolRegistry, dispatcher agentagui.ClientToolDispatcher) *Registry {
	return &Registry{
		Server:      server,
		Dispatcher:  dispatcher,
		clients:     map[session.ID]agentagui.ClientToolSnapshot{},
		generations: map[session.ID]uint64{},
	}
}

// SetClientTools replaces the client tool definitions for one session. Older
// generations are rejected so delayed client updates cannot overwrite newer
// definitions.
func (r *Registry) SetClientTools(snapshot agentagui.ClientToolSnapshot) error {
	if snapshot.SessionID == "" {
		return agenttools.ErrInvalidDefinition
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.clients == nil {
		r.clients = map[session.ID]agentagui.ClientToolSnapshot{}
	}
	if r.generations == nil {
		r.generations = map[session.ID]uint64{}
	}
	current := r.generations[snapshot.SessionID]
	if snapshot.Generation <= current {
		return agenttools.ErrStaleRegistration
	}
	r.generations[snapshot.SessionID] = snapshot.Generation
	r.clients[snapshot.SessionID] = snapshot.Clone()
	return nil
}

// ClearClientTools removes all client tool definitions for a session.
func (r *Registry) ClearClientTools(sessionID session.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, sessionID)
}

// ResolveTools materializes server and session-scoped client tools.
func (r *Registry) ResolveTools(ctx context.Context, snapshot runtime.TurnSnapshot) ([]runtime.Tool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := []runtime.Tool{}
	if r.Server != nil {
		server, err := r.Server.ResolveTools(ctx, snapshot.Clone())
		if err != nil {
			return nil, err
		}
		result = append(result, cloneRuntimeTools(server)...)
	}
	clientSnapshot, ok := r.clientSnapshot(snapshot.SessionID)
	if !ok {
		return result, nil
	}
	clientTools, err := clientSnapshot.RuntimeTools(r.Dispatcher)
	if err != nil {
		return nil, err
	}
	serverNames := map[string]bool{}
	for _, tool := range result {
		serverNames[tool.Name] = true
	}
	enabled := enabledSet(snapshot)
	disabled := disabledSet(snapshot)
	for _, tool := range clientTools {
		if serverNames[tool.Name] || !includeTool(tool.Name, enabled, disabled) {
			continue
		}
		result = append(result, tool)
	}
	return result, nil
}

func (r *Registry) clientSnapshot(sessionID session.ID) (agentagui.ClientToolSnapshot, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot, ok := r.clients[sessionID]
	return snapshot.Clone(), ok
}

func cloneRuntimeTools(src []runtime.Tool) []runtime.Tool {
	if src == nil {
		return nil
	}
	dst := make([]runtime.Tool, len(src))
	copy(dst, src)
	return dst
}

// ClientNames returns the current client tool name set for call
// classification.
func ClientNames(tools []aguitypes.Tool, serverNames ...map[string]bool) map[string]bool {
	result := make(map[string]bool, len(tools))
	blocked := map[string]bool{}
	if len(serverNames) > 0 {
		blocked = serverNames[0]
	}
	for _, tool := range tools {
		if tool.Name != "" && !blocked[tool.Name] {
			result[tool.Name] = true
		}
	}
	return result
}

func enabledSet(snapshot runtime.TurnSnapshot) map[string]bool {
	if snapshot.Config.Tools.Enabled == nil {
		return nil
	}
	result := make(map[string]bool, len(snapshot.Config.Tools.Enabled))
	for _, name := range snapshot.Config.Tools.Enabled {
		result[name] = true
	}
	return result
}

func disabledSet(snapshot runtime.TurnSnapshot) map[string]bool {
	if len(snapshot.Config.Tools.Disabled) == 0 {
		return nil
	}
	result := make(map[string]bool, len(snapshot.Config.Tools.Disabled))
	for _, name := range snapshot.Config.Tools.Disabled {
		result[name] = true
	}
	return result
}

func includeTool(name string, enabled map[string]bool, disabled map[string]bool) bool {
	if disabled[name] {
		return false
	}
	if enabled == nil {
		return true
	}
	return enabled[name]
}
