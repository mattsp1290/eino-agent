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
//
// discovered is the per-execution advertised set of deferred tool names the
// model has already discovered via the tool-search tool (see
// runtime/tool_search.go); it may be nil. A tool is eager (listed in
// Controls.Tools) when it is not Deferred, or once its name appears in
// discovered; otherwise it is deferred (listed in Controls.DeferredTools).
// The tool-search tool itself, if configured (s.ToolSearch), is never listed
// in either list and instead becomes Controls.ToolSearchTool.
//
// When s.ToolSearch is nil (no tool-search tool registered for the plan, or
// a restriction denies its configured name), a Deferred tool has no
// mechanism by which it could ever be discovered: it is excluded from both
// Controls.Tools and Controls.DeferredTools rather than advertised with no
// way to call it. A model call to such a tool settles as the model-visible
// denial "tool %q is deferred and no tool search is configured" (see
// runtime/tool_preparation.go).
func (s TurnSnapshot) ProviderRequest(messageID session.MessageID, trace agentcontext.TraceContext, messages []*einoschema.AgenticMessage, discovered map[string]bool) model.Request {
	var eager, deferred []*einoschema.ToolInfo
	for _, t := range s.Tools {
		if t.Info == nil {
			continue
		}
		if s.ToolSearch != nil && t.Name == s.ToolSearch.Name {
			continue
		}
		if t.Deferred {
			if s.ToolSearch == nil {
				// No tool search is configured for this plan (none
				// registered, or the restriction set denies its name): a
				// deferred tool has no mechanism by which it could ever be
				// discovered, so it is excluded from both Controls.Tools
				// and Controls.DeferredTools rather than advertised with no
				// way to call it.
				continue
			}
			if !discovered[t.Name] {
				deferred = append(deferred, t.Info)
				continue
			}
		}
		eager = append(eager, t.Info)
	}
	controls := model.RequestControls{Tools: eager}
	if len(deferred) != 0 {
		controls.DeferredTools = deferred
	}
	if s.ToolSearch != nil {
		controls.ToolSearchTool = toolSearchToolInfo(s.ToolSearch)
	}
	return model.Request{
		Identity:      modelIdentity(s.ContextIdentity(messageID, "", trace)),
		Messages:      messages,
		ProviderState: s.providerState,
		Controls:      controls,
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
