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
	messages := []*einoschema.Message{{Role: "user", Content: "hello"}}
	frozen := FreezeTurnSnapshot(
		"run-1",
		"session-1",
		"epoch-1",
		cfg,
		model.Resolved{
			Provider: model.Provider{ID: "openai"},
			Model:    model.Descriptor{ID: "gpt-4.1", ProviderID: "openai"},
		},
		messages,
		"system",
		time.Unix(1, 0),
	)
	cfg.Agent.Options["temperature"] = "changed"
	cfg.Tools.Enabled[0] = "changed"
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
	if frozen.RunID != session.RunID("run-1") || frozen.SessionID != session.ID("session-1") {
		t.Fatalf("frozen identity = %q/%q", frozen.RunID, frozen.SessionID)
	}
}
