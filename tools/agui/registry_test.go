package agui

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"
	einoschema "github.com/cloudwego/eino/schema"

	agentagui "github.com/mattsp1290/eino-agent/agui"
	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	agenttools "github.com/mattsp1290/eino-agent/tools"
)

func TestMountClientToolsPublishesSessionScopedPlanTool(t *testing.T) {
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := MountClientTools(context.Background(), registry, clientSnapshot("session-a", "dispatcher-v1"), dispatcher())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()

	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	resolved, err := plan.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: "session-a"})
	if err != nil || len(resolved) != 1 || resolved[0].Name != "client_lookup" {
		t.Fatalf("resolved = %#v, %v", resolved, err)
	}
	normalized, err := resolved[0].InputDecoder.DecodeToolInput(context.Background(), json.RawMessage(`{"query":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	result, err := resolved[0].Executor.Execute(context.Background(), runtime.ToolCall{ID: "call", Input: normalized})
	if err != nil || string(result.Structured) != `{"client":true}` {
		t.Fatalf("result = %#v, %v", result, err)
	}

	other, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-b"})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Release()
	otherTools, err := other.ResolveTools(context.Background(), runtime.ToolScopeContext{SessionID: "session-b"})
	if err != nil || len(otherTools) != 0 {
		t.Fatalf("other session tools = %#v, %v", otherTools, err)
	}
}

func TestDispatcherArtifactIdentityParticipatesInResumeFingerprint(t *testing.T) {
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := MountClientTools(context.Background(), registry, clientSnapshot("session-a", "dispatcher-v1"), dispatcher())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := plan.Descriptor()
	plan.Release()
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := MountClientTools(context.Background(), registry, clientSnapshot("session-a", "dispatcher-v2"), dispatcher())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close(context.Background()) }()
	assertAGUIResumePlanDrift(t, registry, descriptor)
}

func TestClientGenerationParticipatesInResumeFingerprint(t *testing.T) {
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	firstSnapshot := clientSnapshot("session-a", "dispatcher-v1")
	first, err := MountClientTools(context.Background(), registry, firstSnapshot, dispatcher())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.AcquireRunPlan(context.Background(), runtime.RunPlanRequest{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := plan.Descriptor()
	plan.Release()
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	secondSnapshot := clientSnapshot("session-a", "dispatcher-v1")
	secondSnapshot.Generation = 2
	second, err := MountClientTools(context.Background(), registry, secondSnapshot, dispatcher())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close(context.Background()) }()
	assertAGUIResumePlanDrift(t, registry, descriptor)
}

func aguiResumePlanRequest(sessionID session.ID, descriptor session.ExtensionPlanDescriptor) runtime.ResumePlanRequest {
	plan, _ := session.VerifyExtensionPlanForSession(sessionID, descriptor)
	return runtime.ResumePlanRequest{SessionID: sessionID, Plan: plan}
}

func assertAGUIResumePlanDrift(t *testing.T, registry *composition.Registry, persisted session.ExtensionPlanDescriptor) {
	t.Helper()
	plan, err := registry.AcquireResumePlan(context.Background(), aguiResumePlanRequest("session-a", persisted))
	if err != nil {
		t.Fatalf("AcquireResumePlan error = %v", err)
	}
	defer plan.Release()
	if plan.Descriptor().Fingerprint == persisted.Fingerprint {
		t.Fatal("drifted AG-UI composition retained persisted fingerprint")
	}
}

func TestGlobalAndClientToolNameCollisionsAreRejectedInEitherMountOrder(t *testing.T) {
	for _, clientFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "global-first", true: "client-first"}[clientFirst], func(t *testing.T) {
			registry, err := composition.NewRegistry(nil)
			if err != nil {
				t.Fatal(err)
			}
			var first *composition.Mount
			if clientFirst {
				first, err = MountClientTools(context.Background(), registry, clientSnapshot("session-a", "dispatcher-v1"), dispatcher())
			} else {
				first, err = mountGlobalTool(registry, "client_lookup")
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = first.Close(context.Background()) }()
			if clientFirst {
				_, err = mountGlobalTool(registry, "client_lookup")
			} else {
				_, err = MountClientTools(context.Background(), registry, clientSnapshot("session-a", "dispatcher-v1"), dispatcher())
			}
			if !errors.Is(err, agenttools.ErrDuplicateRegistration) {
				t.Fatalf("collision error = %v", err)
			}
		})
	}
}

func TestMountClientToolsValidatesIdentityAndDispatcher(t *testing.T) {
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, snapshot := range map[string]agentagui.ClientToolSnapshot{
		"session":             {Generation: 1, DispatcherArtifactID: "dispatcher", Tools: []aguitypes.Tool{clientTool("client")}},
		"generation":          {SessionID: "session", DispatcherArtifactID: "dispatcher", Tools: []aguitypes.Tool{clientTool("client")}},
		"dispatcher identity": {SessionID: "session", Generation: 1, Tools: []aguitypes.Tool{clientTool("client")}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := MountClientTools(context.Background(), registry, snapshot, dispatcher()); !errors.Is(err, agenttools.ErrInvalidDefinition) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	if _, err := MountClientTools(context.Background(), registry, clientSnapshot("session", "dispatcher"), nil); !errors.Is(err, agentagui.ErrClientToolDispatchRequired) {
		t.Fatalf("missing dispatcher error = %v", err)
	}
}

func clientSnapshot(sessionID session.ID, dispatcherID string) agentagui.ClientToolSnapshot {
	return agentagui.ClientToolSnapshot{SessionID: sessionID, Generation: 1, DispatcherArtifactID: dispatcherID, Tools: []aguitypes.Tool{clientTool("client_lookup")}}
}

func clientTool(name string) aguitypes.Tool {
	return aguitypes.Tool{Name: name, Description: "client tool", Parameters: map[string]any{"type": "object"}}
}

func dispatcher() agentagui.ClientToolDispatcher {
	return agentagui.ClientToolDispatcherFunc(func(context.Context, runtime.ToolCall) (json.RawMessage, error) {
		return json.RawMessage(`{"client":true}`), nil
	})
}

func mountGlobalTool(registry *composition.Registry, name string) (*composition.Mount, error) {
	component := extension.Component{InstanceID: "server-" + name, Artifact: extension.Artifact{Name: "server-tools", Version: "1", Hash: "server-hash", ConfigHash: "server-config", SourceKind: extension.SourceNative}}
	return registry.Mount(context.Background(), component, composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
		definition := agenttools.Definition{
			Name: name, Description: "server", Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{}),
			Execute: func(context.Context, agenttools.Execution) (json.RawMessage, error) {
				return json.RawMessage(`{}`), nil
			},
		}
		return registrar.Tool(composition.ToolRegistration{ID: name, Scope: extension.GlobalScope(), Definition: definition})
	}))
}
