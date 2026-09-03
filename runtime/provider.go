package runtime

import (
	einoschema "github.com/cloudwego/eino/schema"

	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// ProviderRequest assembles the transport-neutral request for one turn. The
// caller validates and takes canonical ownership of the complete graph before
// dispatch.
func (s TurnSnapshot) ProviderRequest(messageID session.MessageID, trace agentcontext.TraceContext, messages []*einoschema.Message) model.Request {
	tools := make([]*einoschema.ToolInfo, 0, len(s.Tools))
	for _, tool := range s.Tools {
		if tool.Info != nil {
			tools = append(tools, tool.Info)
		}
	}
	return model.Request{
		Identity:      modelIdentity(s.ContextIdentity(messageID, "", trace)),
		Messages:      messages,
		ProviderState: s.providerState,
		Tools:         tools,
		Options:       s.Config.Agent.Options,
	}
}

func modelIdentity(identity agentcontext.Identity) model.Identity {
	return model.Identity{
		SessionID:          string(identity.SessionID),
		RunID:              string(identity.RunID),
		AgentID:            identity.AgentID,
		AssistantMessageID: string(identity.AssistantMessageID),
		ToolCallID:         string(identity.ToolCallID),
		ProviderID:         identity.ProviderID,
		ModelID:            identity.ModelID,
		TraceID:            identity.Trace.TraceID,
		SpanID:             identity.Trace.SpanID,
		ParentSpanID:       identity.Trace.ParentSpanID,
		TraceAttributes:    identity.Trace.Attributes,
	}
}
