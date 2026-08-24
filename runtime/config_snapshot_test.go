package runtime

import (
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func TestFreezeTurnSnapshotClonesConfigAndMessages(t *testing.T) {
	t.Parallel()

	cfg := config.Snapshot{
		Agent: config.Agent{
			Name:    "default",
			Options: map[string]string{"temperature": "0.2"},
		},
		Tools: config.ToolConfig{Enabled: []string{"file_read"}},
	}
	index := 3
	imageURL := "https://example.test/image.png"
	messages := []*einoschema.Message{{Role: "user", Content: "hello", ToolCalls: []einoschema.ToolCall{{Index: &index}}, UserInputMultiContent: []einoschema.MessageInputPart{{
		Type: einoschema.ChatMessagePartTypeImageURL, Image: &einoschema.MessageInputImage{MessagePartCommon: einoschema.MessagePartCommon{URL: &imageURL}},
	}}}}
	resolved := model.Resolved{
		Provider: model.Provider{
			ID:          "openai",
			Environment: []string{"OPENAI_API_KEY"},
			Options:     map[string]string{"region": "us"},
		},
		Model: model.Descriptor{
			ID:           "gpt-4.1",
			ProviderID:   "openai",
			Capabilities: map[string]bool{"tools": true},
			Options:      map[string]string{"tier": "standard"},
		},
	}
	frozen, err := FreezeTurnSnapshot(
		"run-1",
		"session-1",
		"epoch-1",
		cfg,
		resolved,
		messages,
		"system",
		time.Unix(1, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Agent.Options["temperature"] = "changed"
	cfg.Tools.Enabled[0] = "changed"
	resolved.Provider.Environment[0] = "CHANGED"
	resolved.Provider.Options["region"] = "changed"
	resolved.Model.Capabilities["tools"] = false
	resolved.Model.Options["tier"] = "changed"
	messages[0].Content = "mutated-before-replace"
	*messages[0].ToolCalls[0].Index = 9
	*messages[0].UserInputMultiContent[0].Image.URL = "https://mutated.test/image.png"
	messages[0] = &einoschema.Message{Role: "user", Content: "changed"}
	if frozen.Config.Agent.Options["temperature"] != "0.2" {
		t.Fatalf("frozen config mutated: %#v", frozen.Config.Agent.Options)
	}
	if frozen.Config.Tools.Enabled[0] != "file_read" {
		t.Fatalf("frozen tools mutated: %#v", frozen.Config.Tools.Enabled)
	}
	if frozen.Messages[0].Content != "hello" {
		t.Fatalf("frozen messages mutated: %#v", frozen.Messages[0])
	}
	if *frozen.Messages[0].ToolCalls[0].Index != 3 || *frozen.Messages[0].UserInputMultiContent[0].Image.URL != "https://example.test/image.png" {
		t.Fatalf("nested message graph mutated: %#v", frozen.Messages[0])
	}
	if frozen.Model.Provider.Environment[0] != "OPENAI_API_KEY" ||
		frozen.Model.Provider.Options["region"] != "us" ||
		!frozen.Model.Model.Capabilities["tools"] ||
		frozen.Model.Model.Options["tier"] != "standard" {
		t.Fatalf("frozen model mutated: %+v", frozen.Model)
	}
	if frozen.RunID != session.RunID("run-1") || frozen.SessionID != session.ID("session-1") {
		t.Fatalf("frozen identity = %q/%q", frozen.RunID, frozen.SessionID)
	}
}
