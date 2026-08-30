package runtime

import (
	"context"
	"testing"

	"github.com/mattsp1290/eino-agent/config"
	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func TestWithContextIdentityPreservesCancellationAndCopiesIdentity(t *testing.T) {
	t.Parallel()

	base, cancel := context.WithCancel(context.Background())
	identity := agentcontext.Identity{
		SessionID: "session-1",
		RunID:     "run-1",
		Trace: agentcontext.TraceContext{
			Attributes: map[string]string{"key": "value"},
		},
	}
	ctx := WithContextIdentity(base, identity)
	identity.Trace.Attributes["key"] = "changed"

	got, ok := ContextIdentityFrom(ctx)
	if !ok {
		t.Fatal("identity missing from context")
	}
	if got.Trace.Attributes["key"] != "value" {
		t.Fatalf("stored identity attribute = %q, want value", got.Trace.Attributes["key"])
	}
	got.Trace.Attributes["key"] = "mutated"
	again, _ := ContextIdentityFrom(ctx)
	if again.Trace.Attributes["key"] != "value" {
		t.Fatalf("returned identity was not cloned: %q", again.Trace.Attributes["key"])
	}
	cancel()
	if err := ctx.Err(); err != context.Canceled {
		t.Fatalf("context err = %v, want context.Canceled", err)
	}
}

func TestTurnSnapshotContextIdentityUsesResolvedModel(t *testing.T) {
	t.Parallel()

	snapshot := TurnSnapshot{
		SessionID: "session-1",
		RunID:     "run-1",
		Config: config.Snapshot{
			Agent: config.Agent{Name: "agent"},
			Model: model.Selection{ProviderID: "configured-provider", ModelID: "configured-model"},
		},
		Model: model.Resolved{
			Provider: model.Provider{ID: "resolved-provider"},
			Model:    model.Descriptor{ID: "resolved-model"},
		},
	}

	identity := snapshot.ContextIdentity("assistant-1", "tool-1", agentcontext.TraceContext{TraceID: "trace"})
	if identity.SessionID != snapshot.SessionID || identity.RunID != snapshot.RunID {
		t.Fatalf("identity ids = %q/%q, want %q/%q", identity.SessionID, identity.RunID, snapshot.SessionID, snapshot.RunID)
	}
	if identity.AgentID != "agent" {
		t.Fatalf("agent id = %q, want agent", identity.AgentID)
	}
	if identity.AssistantMessageID != session.MessageID("assistant-1") || identity.ToolCallID != session.ToolCallID("tool-1") {
		t.Fatalf("message/tool ids = %q/%q", identity.AssistantMessageID, identity.ToolCallID)
	}
	if identity.ProviderID != "resolved-provider" || identity.ModelID != "resolved-model" {
		t.Fatalf("provider/model = %q/%q, want resolved-provider/resolved-model", identity.ProviderID, identity.ModelID)
	}
	if identity.Trace.TraceID != "trace" {
		t.Fatalf("trace id = %q, want trace", identity.Trace.TraceID)
	}
}

func TestTurnSnapshotContextIdentityDoesNotReconstructModelFromConfig(t *testing.T) {
	t.Parallel()

	snapshot := TurnSnapshot{
		SessionID: "session-1",
		RunID:     "run-1",
		Config: config.Snapshot{
			Agent: config.Agent{Name: "agent"},
			Model: model.Selection{ProviderID: "configured-provider", ModelID: "configured-model"},
		},
	}

	identity := snapshot.ContextIdentity("", "", agentcontext.TraceContext{})
	if identity.ProviderID != "" || identity.ModelID != "" {
		t.Fatalf("provider/model = %q/%q, want empty resolved identity", identity.ProviderID, identity.ModelID)
	}
}

func TestContextRequestCopiesSourceAndMetadata(t *testing.T) {
	t.Parallel()

	snapshot := TurnSnapshot{
		SessionID: "session-1",
		RunID:     "run-1",
		Config: config.Snapshot{
			Agent: config.Agent{Name: "agent"},
			Model: model.Selection{ProviderID: "provider", ModelID: "model"},
		},
	}
	options := map[string]string{"path": "project://instructions"}
	metadata := map[string]string{"tenant": "test"}

	request := snapshot.ContextRequest("project", agentcontext.KindProjectInstructions, "project://instructions", options, agentcontext.Bounds{MaxItems: 1}, metadata)
	options["path"] = "changed"
	metadata["tenant"] = "changed"

	if request.Source.Options["path"] != "project://instructions" {
		t.Fatalf("source option = %q, want project://instructions", request.Source.Options["path"])
	}
	if request.Metadata["tenant"] != "test" {
		t.Fatalf("metadata tenant = %q, want test", request.Metadata["tenant"])
	}
}
