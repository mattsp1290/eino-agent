package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
)

func TestSystemPromptMaterializationIsExplicitAndOrdered(t *testing.T) {
	snapshot := TurnSnapshot{RunID: "run", SessionID: "session", EpochID: "epoch", SystemPrompt: "configured"}
	plan := &RunPlan{Prompts: []MountedPrompt{
		{Name: "z", Order: 10, InstanceID: "b", Provider: PromptProviderFunc(func(context.Context, PromptContext) (string, error) { return "last", nil })},
		{Name: "a", Order: -10, InstanceID: "a", Provider: PromptProviderFunc(func(context.Context, PromptContext) (string, error) { return "first", nil })},
	}}
	ctx := withRunPlan(context.Background(), plan)
	orchestrator := &StreamingOrchestrator{}
	text, err := orchestrator.renderSystemPrompt(ctx, snapshot, 1, 2)
	if err != nil || text != "first\n\nlast" {
		t.Fatalf("default-off prompt = %q, %v", text, err)
	}
	orchestrator.SystemPromptMaterialization = true
	text, err = orchestrator.renderSystemPrompt(ctx, snapshot, 1, 2)
	if err != nil || text != "first\n\nconfigured\n\nlast" {
		t.Fatalf("enabled prompt = %q, %v", text, err)
	}
}

func TestFallbackModelPrependsSystemWithoutReorderingDurableMessages(t *testing.T) {
	client := &capturingChatModel{}
	reader, err := openStream(context.Background(), model.Resolved{Client: client}, model.Request{System: "generated", Messages: []*einoschema.Message{einoschema.SystemMessage("durable"), einoschema.UserMessage("hello")}})
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
	roles := []einoschema.RoleType{client.messages[0].Role, client.messages[1].Role, client.messages[2].Role}
	contents := []string{client.messages[0].Content, client.messages[1].Content, client.messages[2].Content}
	if !reflect.DeepEqual(roles, []einoschema.RoleType{einoschema.System, einoschema.System, einoschema.User}) || !reflect.DeepEqual(contents, []string{"generated", "durable", "hello"}) {
		t.Fatalf("messages = %#v", client.messages)
	}
}

func TestMountedGuardsAllRunAndDenyBeforePermissions(t *testing.T) {
	var sequence []string
	plan := &RunPlan{Guards: []MountedToolGuard{
		{ID: "deny", Guard: ToolGuardFunc(func(context.Context, ToolGuardRequest) (ToolGuardResult, error) {
			sequence = append(sequence, "deny")
			return ToolGuardResult{Decision: ToolGuardDeny, Message: "blocked"}, nil
		})},
		{ID: "audit", Guard: ToolGuardFunc(func(context.Context, ToolGuardRequest) (ToolGuardResult, error) {
			sequence = append(sequence, "audit")
			return ToolGuardResult{Decision: ToolGuardAbstain}, nil
		})},
	}}
	permissionsCalled := false
	executed := false
	orchestrator := &StreamingOrchestrator{Permissions: permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
		permissionsCalled = true
		return permissions.Decision{Action: permissions.ActionAllow}, nil
	})}
	tool := Tool{Name: "danger", Executor: runtimeToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		executed = true
		return ToolResult{Output: "bad"}, nil
	})}
	outcome := orchestrator.executeToolOutcome(withRunPlan(context.Background(), plan), tool, ToolCall{ID: "call", Name: "danger", Input: []byte(`{}`)})
	if outcome.Disposition != ToolDenied || outcome.Result.Metadata["permission_status"] != "denied" || permissionsCalled || executed || !reflect.DeepEqual(sequence, []string{"deny", "audit"}) {
		t.Fatalf("outcome=%#v permissions=%t executed=%t sequence=%v", outcome, permissionsCalled, executed, sequence)
	}
}

func TestToolOutcomeSealRejectsDispositionMutation(t *testing.T) {
	outcome := sealToolOutcome(ToolOutcome{Call: ToolCall{ID: "call"}, Disposition: ToolExecuted})
	outcome.Disposition = ToolDenied
	if err := validateToolOutcome(outcome); !errors.Is(err, extension.ErrProtectedMutation) {
		t.Fatalf("validation = %v", err)
	}
}

func TestModelStreamPointRejectsFabricatedSuccessfulReader(t *testing.T) {
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "stream-test", Artifact: extension.Artifact{Name: "stream-test", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.Use(registrar, ModelStreamPoint, extension.Registration{ID: "replace", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(ctx context.Context, input ModelStreamInput, next extension.Next[ModelStreamInput, *einoschema.StreamReader[*einoschema.Message]]) (*einoschema.StreamReader[*einoschema.Message], error) {
			delegated, err := next(ctx, input)
			if delegated != nil {
				delegated.Close()
			}
			if err != nil {
				return nil, err
			}
			reader, writer := einoschema.Pipe[*einoschema.Message](1)
			writer.Close()
			return reader, nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	_, err = extension.Invoke(plan, context.Background(), ModelStreamPoint, ModelStreamInput{}, func(context.Context, ModelStreamInput) (*einoschema.StreamReader[*einoschema.Message], error) {
		reader, writer := einoschema.Pipe[*einoschema.Message](1)
		writer.Close()
		return reader, nil
	})
	if !errors.Is(err, extension.ErrProtectedMutation) {
		t.Fatalf("fabricated stream error = %v", err)
	}
}

func TestPublishedExtensionPointsAppearInCatalog(t *testing.T) {
	catalog, err := os.ReadFile(filepath.Join("..", "docs", "architecture", "extension-points.md"))
	if err != nil {
		t.Fatal(err)
	}
	contracts := []extension.Contract{
		RunAdmittedPoint.Contract(), RunStartedPoint.Contract(), RunSettledPoint.Contract(),
		ModelRequestedPoint.Contract(), ModelCompletedPoint.Contract(), ToolPreparedPoint.Contract(),
		ToolStartedPoint.Contract(), ToolSettledPoint.Contract(), EventPublishedPoint.Contract(),
		RunBeforeExecutePoint.Contract(), ContextAssemblePoint.Contract(), ModelStreamPoint.Contract(),
		ToolPreparePoint.Contract(), ToolExecutePoint.Contract(), ToolResultTransformPoint.Contract(),
	}
	for _, contract := range contracts {
		if !strings.Contains(string(catalog), "`"+contract.ID+"`") {
			t.Errorf("catalog missing %s", contract.ID)
		}
	}
}

func TestStrictToolPlanRejectsUnsupportedStoreBeforeAdmission(t *testing.T) {
	released := false
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: 1, Mode: session.PlanStrict, Entries: []session.ExtensionPlanEntry{{InstanceID: "tool-plugin", Kind: session.ExtensionTool, Required: true}}}
	descriptor.Fingerprint, _ = session.FingerprintExtensionPlan(descriptor)
	orchestrator := &StreamingOrchestrator{Store: newAdmissionStore(), Plans: staticRunPlanProvider{plan: &RunPlan{Descriptor: descriptor, RequiresToolSettlement: true, Release: func() { released = true }}}}
	_, err := orchestrator.acquireRunPlan(context.Background(), RunPlanRequest{SessionID: "session"})
	if !errors.Is(err, ErrInvalidOrchestrator) || !released {
		t.Fatalf("acquire strict tool plan = %v released=%t", err, released)
	}
}

type staticRunPlanProvider struct{ plan *RunPlan }

func (p staticRunPlanProvider) AcquireRunPlan(context.Context, RunPlanRequest) (*RunPlan, error) {
	return p.plan, nil
}

func (p staticRunPlanProvider) AcquireResumePlan(context.Context, session.ExtensionPlanDescriptor) (*RunPlan, error) {
	return p.plan, nil
}

type capturingChatModel struct{ messages []*einoschema.Message }

func (m *capturingChatModel) Generate(context.Context, []*einoschema.Message, ...einomodel.Option) (*einoschema.Message, error) {
	return nil, errors.New("unused")
}

func (m *capturingChatModel) Stream(_ context.Context, messages []*einoschema.Message, _ ...einomodel.Option) (*einoschema.StreamReader[*einoschema.Message], error) {
	m.messages = cloneMessages(messages)
	reader, writer := einoschema.Pipe[*einoschema.Message](1)
	writer.Close()
	return reader, nil
}

func (m *capturingChatModel) WithTools([]*einoschema.ToolInfo) (einomodel.ToolCallingChatModel, error) {
	return m, nil
}
