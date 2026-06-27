package runtime

import (
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/model"
)

func TestProviderRequestCarriesRuntimeIdentityAndTools(t *testing.T) {
	t.Parallel()

	snapshot := TurnSnapshot{
		SessionID: "session-1",
		RunID:     "run-1",
		Config: config.Snapshot{
			Agent: config.Agent{
				Name:    "agent",
				Options: map[string]string{"temperature": "0"},
			},
			Model: model.Selection{ProviderID: "configured-provider", ModelID: "configured-model"},
		},
		Model: model.Resolved{
			Provider: model.Provider{ID: "resolved-provider"},
			Model:    model.Descriptor{ID: "resolved-model"},
		},
		Messages: []*einoschema.Message{einoschema.UserMessage("hello")},
		Tools: []Tool{{
			Info: &einoschema.ToolInfo{Name: "search"},
		}},
	}

	request := snapshot.ProviderRequest("assistant-1", agentcontext.TraceContext{TraceID: "trace"}, nil)
	if request.Identity.ProviderID != "resolved-provider" || request.Identity.ModelID != "resolved-model" {
		t.Fatalf("identity provider/model = %q/%q", request.Identity.ProviderID, request.Identity.ModelID)
	}
	if request.Identity.AssistantMessageID != "assistant-1" || request.Identity.TraceID != "trace" {
		t.Fatalf("identity = %#v", request.Identity)
	}
	if len(request.Messages) != 1 || len(request.Tools) != 1 {
		t.Fatalf("messages/tools = %d/%d", len(request.Messages), len(request.Tools))
	}
	snapshot.Messages = nil
	snapshot.Tools = nil
	snapshot.Config.Agent.Options["temperature"] = "changed"
	if len(request.Messages) != 1 || len(request.Tools) != 1 || request.Options["temperature"] != "0" {
		t.Fatalf("request was not cloned: %#v", request)
	}
}
