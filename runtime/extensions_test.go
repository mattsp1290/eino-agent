package runtime

import (
	"context"
	"encoding/json"
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
		RunBeforeExecutePoint.Contract(), ContextAssemblePoint.Contract(), TurnPreparePoint.Contract(), ModelStreamPoint.Contract(),
		ToolPreparePoint.Contract(), ToolExecutePoint.Contract(), ToolResultTransformPoint.Contract(),
	}
	for _, contract := range contracts {
		if !strings.Contains(string(catalog), "`"+contract.ID+"`") {
			t.Errorf("catalog missing %s", contract.ID)
		}
	}
}

func TestToolSettledObserversReceiveDeepClonedAttachmentMetadata(t *testing.T) {
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "settled-copy", Artifact: extension.Artifact{Name: "settled-copy", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var secondValue string
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.On(registrar, ToolSettledPoint, extension.Registration{ID: "first", InstanceID: component.InstanceID, Order: 0, Scope: extension.GlobalScope()}, func(_ context.Context, notice ToolSettledNotice) error {
			notice.Result.Attachments[0].Metadata["owner"] = "mutated"
			return nil
		}); err != nil {
			return err
		}
		return extension.On(registrar, ToolSettledPoint, extension.Registration{ID: "second", InstanceID: component.InstanceID, Order: 1, Scope: extension.GlobalScope()}, func(_ context.Context, notice ToolSettledNotice) error {
			secondValue = notice.Result.Attachments[0].Metadata["owner"]
			return nil
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
	source := ToolSettledNotice{Result: ToolResult{Attachments: []Attachment{{Metadata: map[string]string{"owner": "original"}}}}}
	_ = extension.Notify(plan, context.Background(), ToolSettledPoint, source)
	if secondValue != "original" || source.Result.Attachments[0].Metadata["owner"] != "original" {
		t.Fatalf("attachment metadata leaked: second=%q source=%q", secondValue, source.Result.Attachments[0].Metadata["owner"])
	}
}

func TestTurnPreparePointRunsAfterPlannedToolsResolve(t *testing.T) {
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "turn-order", Artifact: extension.Artifact{Name: "turn-order", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var seen BoundedTurnMetadata
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.Use(registrar, TurnPreparePoint, extension.Registration{ID: "observe", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(ctx context.Context, metadata BoundedTurnMetadata, next extension.Next[BoundedTurnMetadata, BoundedTurnMetadata]) (BoundedTurnMetadata, error) {
			seen = cloneBoundedTurnMetadata(metadata)
			return next(ctx, metadata)
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer dispatch.Release()
	plan := &RunPlan{Dispatch: dispatch, Tools: ToolRegistryFunc(func(context.Context, TurnSnapshot) ([]Tool, error) {
		return []Tool{{Name: "echo"}}, nil
	})}
	snapshot := TurnSnapshot{RunID: "run", SessionID: "session", Messages: []*einoschema.Message{einoschema.UserMessage("hidden")}}
	prepared, err := (&StreamingOrchestrator{}).prepareSnapshot(withRunPlan(context.Background(), plan), snapshot, "message")
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.Tools) != 1 || !reflect.DeepEqual(seen.ToolNames, []string{"echo"}) || seen.MessageCount != 1 || seen.RoleCounts.User != 1 {
		t.Fatalf("prepared=%#v metadata=%#v", prepared.Tools, seen)
	}
}

func TestInterfaceIdentityRejectsNonComparableCallableValuesWithoutPanic(t *testing.T) {
	callable := runtimeToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{}, nil })
	if sameInterfaceIdentity(callable, callable) {
		t.Fatal("non-comparable callable value unexpectedly acquired stable identity")
	}
	first := &callable
	other := callable
	second := &other
	if !sameInterfaceIdentity(first, first) || sameInterfaceIdentity(first, second) {
		t.Fatal("pointer-backed callable identity comparison is not exact")
	}
}

func TestToolValidationRejectsSameTypeCallableReplacement(t *testing.T) {
	firstExecutor, secondExecutor := testExecutor("first"), testExecutor("second")
	firstDecoder, secondDecoder := &testInputDecoder{}, &testInputDecoder{}
	firstApproval, secondApproval := &testApprovalRequester{}, &testApprovalRequester{}
	original := PreparedToolCall{
		Tool: Tool{Name: "echo", Executor: firstExecutor, InputDecoder: firstDecoder},
		Call: ToolCall{ID: "call", Name: "echo", Input: json.RawMessage(`{}`), Approval: firstApproval},
	}
	for _, test := range []struct {
		name   string
		mutate func(*PreparedToolCall)
	}{
		{name: "executor", mutate: func(value *PreparedToolCall) { value.Tool.Executor = secondExecutor }},
		{name: "decoder", mutate: func(value *PreparedToolCall) { value.Tool.InputDecoder = secondDecoder }},
		{name: "approval", mutate: func(value *PreparedToolCall) { value.Call.Approval = secondApproval }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := clonePreparedToolCall(original)
			test.mutate(&candidate)
			if err := validatePreparedToolCallInput(original, candidate); !errors.Is(err, extension.ErrProtectedMutation) {
				t.Fatalf("validation = %v", err)
			}
		})
	}
}

type testInputDecoder [1]byte

func (*testInputDecoder) DecodeToolInput(_ context.Context, raw json.RawMessage) (json.RawMessage, error) {
	return raw, nil
}

type testApprovalRequester [1]byte

func (*testApprovalRequester) Ask(context.Context, ApprovalRequest) error { return nil }

func testExecutor(marker string) ToolExecutor {
	executor := runtimeToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		return ToolResult{Output: marker}, nil
	})
	return &executor
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
