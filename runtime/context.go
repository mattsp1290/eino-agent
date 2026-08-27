package runtime

import (
	"context"

	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/session"
)

type runtimeContextKey struct{}

// WithContextIdentity attaches runtime identity metadata to ctx while
// preserving the caller's cancellation, deadline, and values.
func WithContextIdentity(ctx context.Context, identity agentcontext.Identity) context.Context {
	return context.WithValue(ctx, runtimeContextKey{}, identity.Clone())
}

// ContextIdentityFrom returns runtime identity metadata carried by ctx.
func ContextIdentityFrom(ctx context.Context) (agentcontext.Identity, bool) {
	identity, ok := ctx.Value(runtimeContextKey{}).(agentcontext.Identity)
	if !ok {
		return agentcontext.Identity{}, false
	}
	return identity.Clone(), true
}

// ContextIdentity builds the identity visible to tools, model adapters, hooks,
// observability, and context sources for this turn snapshot.
func (s TurnSnapshot) ContextIdentity(messageID session.MessageID, toolCallID session.ToolCallID, trace agentcontext.TraceContext) agentcontext.Identity {
	return agentcontext.Identity{
		SessionID:          s.SessionID,
		RunID:              s.RunID,
		AgentID:            s.Config.Agent.Name,
		AssistantMessageID: messageID,
		ToolCallID:         toolCallID,
		ProviderID:         s.Model.Provider.ID,
		ModelID:            s.Model.Model.ID,
		Trace:              trace,
	}
}

// ContextRequest returns a host-neutral request for loading one configured
// context source under this turn snapshot.
func (s TurnSnapshot) ContextRequest(sourceName string, kind agentcontext.Kind, location string, options map[string]string, bounds agentcontext.Bounds, metadata map[string]string) agentcontext.Request {
	return agentcontext.Request{
		Identity: s.ContextIdentity("", "", agentcontext.TraceContext{}),
		Source: agentcontext.Source{
			Name:     sourceName,
			Kind:     kind,
			Location: location,
			Options:  cloneStringMap(options),
		},
		Bounds:   bounds,
		Metadata: cloneStringMap(metadata),
	}
}

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
