package runtime

import (
	"strings"
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

	traceAttributes := map[string]string{"request": "original"}
	request, audited, _, err := auditModelRequest(
		snapshot.ProviderRequest("assistant-1", agentcontext.TraceContext{TraceID: "trace", Attributes: traceAttributes}, []*einoschema.Message{originalMessage}),
		[]string{"temperature"}, 0,
	)
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
	originalMessage.Content = "changed"
	originalTool.Name = "changed"
	traceAttributes["request"] = "changed"
	if len(request.Messages) != 1 || len(request.Tools) != 1 || request.Messages[0].Content != "hello" || request.Tools[0].Name != "search" || request.Options["temperature"] != "0" || request.Identity.TraceAttributes["request"] != "original" {
		t.Fatalf("request was not cloned: %#v", request)
	}
	request.Messages[0].Content = "canonical changed"
	request.Tools[0].Name = "canonical changed"
	if !strings.Contains(string(audited.Messages[0].Canonical), `"hello"`) || strings.Contains(string(audited.Messages[0].Canonical), "canonical changed") || audited.Tools[0].Name != "search" || audited.SafeCallConfig["temperature"] != "0" {
		t.Fatalf("audit projection changed with canonical request: %#v", audited)
	}
}
