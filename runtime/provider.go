package runtime

import (
	einoschema "github.com/cloudwego/eino/schema"

	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// ProviderRequest builds the transport-neutral request passed to model
// adapters for one turn. The caller validates and defensively clones the
// complete request before dispatch.
func (s TurnSnapshot) ProviderRequest(messageID session.MessageID, trace agentcontext.TraceContext) model.Request {
	tools := make([]*einoschema.ToolInfo, 0, len(s.Tools))
	for _, tool := range s.Tools {
		if tool.Info != nil {
			tools = append(tools, tool.Info)
		}
	}
	return model.Request{
		Identity: modelIdentity(s.ContextIdentity(messageID, "", trace)),
		Messages: cloneSlice(s.Messages),
		Tools:    tools,
		Options:  cloneStringMap(s.Config.Agent.Options),
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
		TraceAttributes:    cloneStringMap(identity.Trace.Attributes),
	}
}
