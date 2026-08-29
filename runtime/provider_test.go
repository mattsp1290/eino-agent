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

	originalMessage := einoschema.UserMessage("hello")
	originalTool := &einoschema.ToolInfo{Name: "search"}
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
		Messages: []*einoschema.Message{originalMessage},
		Tools: []Tool{{
			Info: originalTool,
		}},
	}

	request, err := snapshot.ProviderRequest("assistant-1", agentcontext.TraceContext{TraceID: "trace"}).Clone()
	if err != nil {
		t.Fatal(err)
	}
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
	request.Messages[0].Content = "changed"
	request.Tools[0].Name = "changed"
	if len(request.Messages) != 1 || len(request.Tools) != 1 || request.Options["temperature"] != "0" {
		t.Fatalf("request was not cloned: %#v", request)
	}
	if originalMessage.Content != "hello" || originalTool.Name != "search" {
		t.Fatalf("original message/tool mutated: %q/%q", originalMessage.Content, originalTool.Name)
	}
}
