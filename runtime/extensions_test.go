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
	"time"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
)

func mustClonePreparedToolCall(t *testing.T, value PreparedToolCall) PreparedToolCall {
	t.Helper()
	cloned, err := clonePreparedToolCallChecked(value)
	if err != nil {
		t.Fatal(err)
	}
	return cloned
}

func TestSystemPromptMaterializationIsUnconditionalAndOrdered(t *testing.T) {
	snapshot := TurnSnapshot{RunID: "run", SessionID: "session", EpochID: "epoch", SystemPrompt: "configured"}
	plan := mustTestRunPlan(RunPlanSpec{Prompts: []PlanPrompt{
		{Identity: testPromptIdentity("z", "b", 10), Prompt: MountedPrompt{Name: "z", Order: 10, InstanceID: "b", Provider: PromptProviderFunc(func(context.Context, PromptContext) (string, error) { return "last", nil })}},
		{Identity: testPromptIdentity("a", "a", -10), Prompt: MountedPrompt{Name: "a", Order: -10, InstanceID: "a", Provider: PromptProviderFunc(func(context.Context, PromptContext) (string, error) { return "first", nil })}},
	}})
	orchestrator := mustConfiguredOrchestrator()
	text, err := orchestrator.renderSystemPrompt(context.Background(), plan, snapshot, 1, 2)
	if err != nil || text != "first\n\nconfigured\n\nlast" {
		t.Fatalf("prompt = %q, %v", text, err)
	}
}

func TestFallbackModelPrependsSystemWithoutReorderingDurableMessages(t *testing.T) {
	client := &capturingChatModel{}
	reader, err := openStream(context.Background(), model.Resolved{Streamer: model.NewEinoStreamer(client)}, model.Request{System: "generated", Messages: []*einoschema.Message{einoschema.SystemMessage("durable"), einoschema.UserMessage("hello")}, Tools: []*einoschema.ToolInfo{{Name: "echo"}}})
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
	roles := []einoschema.RoleType{client.messages[0].Role, client.messages[1].Role, client.messages[2].Role}
	contents := []string{client.messages[0].Content, client.messages[1].Content, client.messages[2].Content}
	if !reflect.DeepEqual(roles, []einoschema.RoleType{einoschema.System, einoschema.System, einoschema.User}) || !reflect.DeepEqual(contents, []string{"generated", "durable", "hello"}) {
		t.Fatalf("messages = %#v", client.messages)
	}
	if client.toolBindings != 1 || client.streamOptions != 0 {
		t.Fatalf("tool bindings=%d stream options=%d, want 1/0", client.toolBindings, client.streamOptions)
	}
}

func TestMountedGuardsAllRunAndDenyBeforePermissions(t *testing.T) {
	var sequence []string
	plan := mustTestRunPlan(RunPlanSpec{Guards: []PlanGuard{
		{Identity: testGuardIdentity("deny"), Guard: MountedToolGuard{ID: "deny", InstanceID: "test-guards", Guard: ToolGuardFunc(func(context.Context, ToolGuardRequest) (ToolGuardResult, error) {
			sequence = append(sequence, "deny")
			return ToolGuardResult{Decision: ToolGuardDeny, Message: "blocked"}, nil
		})}},
		{Identity: testGuardIdentity("audit"), Guard: MountedToolGuard{ID: "audit", InstanceID: "test-guards", Guard: ToolGuardFunc(func(context.Context, ToolGuardRequest) (ToolGuardResult, error) {
			sequence = append(sequence, "audit")
			return ToolGuardResult{Decision: ToolGuardAbstain}, nil
		})}},
	}})
	permissionsCalled := false
	executed := false
	orchestrator := mustConfiguredOrchestrator(WithPermissions(permissions.PolicyFunc(func(context.Context, permissions.Request) (permissions.Decision, error) {
		permissionsCalled = true
		return permissions.Decision{Action: permissions.ActionAllow}, nil
	})))
	tool := Tool{Name: "danger", Executor: runtimeToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		executed = true
		return ToolResult{Output: "bad"}, nil
	})}
	outcome := orchestrator.executeToolOutcome(context.Background(), newRunExecution(orchestrator, plan), tool, ToolCall{ID: "call", Name: "danger", Input: []byte(`{}`)})
	if outcome.Disposition != ToolDenied || outcome.Result.Metadata["permission_status"] != "denied" || permissionsCalled || executed || !reflect.DeepEqual(sequence, []string{"deny", "audit"}) {
		t.Fatalf("outcome=%#v permissions=%t executed=%t sequence=%v", outcome, permissionsCalled, executed, sequence)
	}
}

func TestMountedGuardsReceiveIsolatedRequests(t *testing.T) {
	original := ToolCall{
		SessionID: "session",
		RunID:     "run",
		Input:     json.RawMessage(`{"op":"delete"}`),
		Scope:     ToolScope{Permissions: []string{"write"}},
	}
	plan := mustTestRunPlan(RunPlanSpec{Guards: []PlanGuard{
		{Identity: testGuardIdentity("mutate"), Guard: MountedToolGuard{ID: "mutate", InstanceID: "test-guards", Guard: ToolGuardFunc(func(_ context.Context, request ToolGuardRequest) (ToolGuardResult, error) {
			copy(request.Call.Input, json.RawMessage(`{"op":"hidden"}`))
			copy(request.Call.Scope.Permissions, []string{"audit"})
			return ToolGuardResult{Decision: ToolGuardAbstain}, nil
		})}},
		{Identity: testGuardIdentity("deny-original"), Guard: MountedToolGuard{ID: "deny-original", InstanceID: "test-guards", Guard: ToolGuardFunc(func(_ context.Context, request ToolGuardRequest) (ToolGuardResult, error) {
			if string(request.Call.Input) != `{"op":"delete"}` || !reflect.DeepEqual(request.Call.Scope.Permissions, []string{"write"}) {
				t.Fatalf("second guard request = %#v", request.Call)
			}
			return ToolGuardResult{Decision: ToolGuardDeny}, nil
		})}},
	}})

	decision, err := evaluateToolGuards(context.Background(), plan, Tool{Name: "danger"}, original)
	if err != nil || decision.Decision != ToolGuardDeny {
		t.Fatalf("evaluateToolGuards = %#v, %v", decision, err)
	}
	if string(original.Input) != `{"op":"delete"}` || !reflect.DeepEqual(original.Scope.Permissions, []string{"write"}) {
		t.Fatalf("original call mutated = %#v", original)
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
		return extension.Use(registrar, ModelStreamPoint, extension.Registration{ID: "replace", Scope: extension.GlobalScope()}, func(ctx context.Context, input ModelStreamInput, next extension.Next[ModelStreamInput, *einoschema.StreamReader[*einoschema.Message]]) (*einoschema.StreamReader[*einoschema.Message], error) {
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

func TestModelStreamPointRejectsSwallowedProviderFailure(t *testing.T) {
	providerErr := errors.New("provider failure")
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "stream-swallow", Artifact: extension.Artifact{Name: "stream-swallow", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.Use(registrar, ModelStreamPoint, extension.Registration{ID: "swallow", Scope: extension.GlobalScope()}, func(ctx context.Context, input ModelStreamInput, next extension.Next[ModelStreamInput, *einoschema.StreamReader[*einoschema.Message]]) (*einoschema.StreamReader[*einoschema.Message], error) {
			_, _ = next(ctx, input)
			return nil, nil
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
	reader, err := extension.Invoke(plan, context.Background(), ModelStreamPoint, ModelStreamInput{}, func(context.Context, ModelStreamInput) (*einoschema.StreamReader[*einoschema.Message], error) {
		return nil, providerErr
	})
	if reader != nil || !errors.Is(err, providerErr) {
		t.Fatalf("model stream = %#v, %v; want provider failure", reader, err)
	}
}

func TestModelStreamValidationUsesDataOnlyView(t *testing.T) {
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "unrelated", Artifact: extension.Artifact{Name: "unrelated", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, ModelRequestedPoint, extension.Registration{ID: "notice", Scope: extension.GlobalScope()}, func(context.Context, ModelRequestedNotice) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	terminalCalled := false
	input := ModelStreamInput{ProviderID: "provider", ModelID: "model", ContentHash: "hash", Audited: AuditedModelInput{System: "system"}}
	reader, err := extension.Invoke(plan, context.Background(), ModelStreamPoint, input, func(_ context.Context, value ModelStreamInput) (*einoschema.StreamReader[*einoschema.Message], error) {
		terminalCalled = true
		if value.ProviderID != "provider" || value.ModelID != "model" || value.ContentHash != "hash" {
			t.Fatalf("model stream terminal received wrong data: %#v", value)
		}
		reader, writer := einoschema.Pipe[*einoschema.Message](1)
		writer.Close()
		return reader, nil
	})
	if err != nil || !terminalCalled || reader == nil {
		t.Fatalf("model stream = %#v, %v terminal=%t", reader, err, terminalCalled)
	}
	reader.Close()
}

func TestModelStreamValidationRejectsCanonicalDataReplacement(t *testing.T) {
	original := ModelStreamInput{ProviderID: "provider", ModelID: "model", ContentHash: "hash"}
	candidate, err := cloneModelStreamInput(original)
	if err != nil {
		t.Fatal(err)
	}
	candidate.ContentHash = "changed"
	if err := validateModelStreamInput(original, candidate); !errors.Is(err, extension.ErrProtectedMutation) {
		t.Fatalf("content hash replacement validation = %v", err)
	}
}

func TestModelStreamPointRejectsNestedRequestMutationWithoutAliasingOriginal(t *testing.T) {
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "stream-nested-mutation", Artifact: extension.Artifact{Name: "stream-nested-mutation", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.Use(registrar, ModelStreamPoint, extension.Registration{ID: "mutate", Scope: extension.GlobalScope()}, func(ctx context.Context, input ModelStreamInput, next extension.Next[ModelStreamInput, *einoschema.StreamReader[*einoschema.Message]]) (*einoschema.StreamReader[*einoschema.Message], error) {
			input.Audited.Messages[0].Canonical[0] = '['
			return next(ctx, input)
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

	original := ModelStreamInput{ContentHash: "hash", Audited: AuditedModelInput{Messages: []AuditedMessage{{Canonical: json.RawMessage(`{"role":"user","content":"original"}`)}}}}
	providerCalled := false
	reader, err := extension.Invoke(plan, context.Background(), ModelStreamPoint, original, func(context.Context, ModelStreamInput) (*einoschema.StreamReader[*einoschema.Message], error) {
		providerCalled = true
		return nil, nil
	})
	if reader != nil || !errors.Is(err, extension.ErrProtectedMutation) || providerCalled {
		t.Fatalf("reader=%v error=%v provider_called=%t", reader, err, providerCalled)
	}
	if got := string(original.Audited.Messages[0].Canonical); got != `{"role":"user","content":"original"}` {
		t.Fatalf("original request mutated through interceptor alias: %v", got)
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
		if err := extension.On(registrar, ToolSettledPoint, extension.Registration{ID: "first", Order: 0, Scope: extension.GlobalScope()}, func(_ context.Context, notice ToolSettledNotice) error {
			notice.Result.Attachments[0].Metadata["owner"] = "mutated"
			return nil
		}); err != nil {
			return err
		}
		return extension.On(registrar, ToolSettledPoint, extension.Registration{ID: "second", Order: 1, Scope: extension.GlobalScope()}, func(_ context.Context, notice ToolSettledNotice) error {
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
		return extension.Use(registrar, TurnPreparePoint, extension.Registration{ID: "observe", Scope: extension.GlobalScope()}, func(ctx context.Context, metadata BoundedTurnMetadata, next extension.Next[BoundedTurnMetadata, BoundedTurnMetadata]) (BoundedTurnMetadata, error) {
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
	plan := mustTestRunPlan(RunPlanSpec{Dispatch: dispatch, Tools: []PlanTool{{
		Identity: testToolIdentity("echo"),
		Resolve:  func(context.Context, ToolScopeContext) (Tool, error) { return Tool{Name: "echo"}, nil },
	}}})
	snapshot := TurnSnapshot{RunID: "run", SessionID: "session", Messages: []*einoschema.Message{einoschema.UserMessage("hidden")}}
	host := mustConfiguredOrchestrator()
	prepared, err := host.prepareSnapshot(context.Background(), newRunExecution(host, plan), snapshot, "message")
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.Tools) != 1 || !reflect.DeepEqual(seen.ToolNames, []string{"echo"}) || seen.MessageCount != 1 || seen.RoleCounts.User != 1 {
		t.Fatalf("prepared=%#v metadata=%#v", prepared.Tools, seen)
	}
}

func TestProtectedViewsRejectCallableInjection(t *testing.T) {
	modelInput := ModelStreamInput{ContentHash: "original"}
	modelCandidate, err := cloneModelStreamInput(modelInput)
	if err != nil {
		t.Fatal(err)
	}
	modelCandidate.ContentHash = "changed"
	if err := validateModelStreamInput(modelInput, modelCandidate); !errors.Is(err, extension.ErrProtectedMutation) {
		t.Fatalf("streamer injection validation = %v", err)
	}

	tool := extensionTool(Tool{Name: "tool"})
	toolCandidate, err := cloneToolChecked(tool)
	if err != nil {
		t.Fatal(err)
	}
	toolCandidate.Executor = runtimeToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{}, nil })
	if sameProtectedTool(tool, toolCandidate) {
		t.Fatal("executor injection passed protected tool validation")
	}

	call := extensionToolCall(ToolCall{ID: "call"})
	callCandidate := cloneToolCall(call)
	callCandidate.Approval = approvalFunc(func(context.Context, ApprovalRequest) error { return nil })
	if sameProtectedToolCall(call, callCandidate) {
		t.Fatal("approval injection passed protected call validation")
	}
}

func TestProtectedCloneFailureStopsContextAndToolInterceptors(t *testing.T) {
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "clone-failure", Artifact: extension.Artifact{Name: "clone-failure", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var contextEntered, toolEntered bool
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.Use(registrar, ContextAssemblePoint, extension.Registration{ID: "context", Scope: extension.GlobalScope()}, func(ctx context.Context, input ContextAssembly, next extension.Next[ContextAssembly, ContextAssembly]) (ContextAssembly, error) {
			contextEntered = true
			input.Base[0].Extra["nested"].(map[string]any)["value"] = "mutated"
			return next(ctx, input)
		}); err != nil {
			return err
		}
		return extension.Use(registrar, ToolPreparePoint, extension.Registration{ID: "tool", Scope: extension.GlobalScope()}, func(ctx context.Context, input PreparedToolCall, next extension.Next[PreparedToolCall, PreparedToolCall]) (PreparedToolCall, error) {
			toolEntered = true
			input.Tool.Info.Extra["nested"].(map[string]any)["value"] = "mutated"
			return next(ctx, input)
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
	messageNested := map[string]any{"value": "original"}
	message := einoschema.UserMessage("protected")
	message.Extra = map[string]any{"nested": messageNested, "unsupported": make(chan struct{})}
	_, err = extension.Invoke(plan, context.Background(), ContextAssemblePoint, ContextAssembly{Base: []*einoschema.Message{message}}, func(_ context.Context, input ContextAssembly) (ContextAssembly, error) { return input, nil })
	if err == nil || contextEntered || messageNested["value"] != "original" {
		t.Fatalf("context clone failure = %v entered=%t nested=%v", err, contextEntered, messageNested)
	}
	toolNested := map[string]any{"value": "original"}
	tool := Tool{Name: "tool", Info: &einoschema.ToolInfo{Name: "tool", Extra: map[string]any{"nested": toolNested, "unsupported": make(chan struct{})}}}
	prepared := PreparedToolCall{Tool: tool, Call: ToolCall{ID: "call", Name: "tool", Input: json.RawMessage(`{}`)}}
	_, err = extension.Invoke(plan, context.Background(), ToolPreparePoint, prepared, func(_ context.Context, input PreparedToolCall) (PreparedToolCall, error) { return input, nil })
	if err == nil || toolEntered || toolNested["value"] != "original" {
		t.Fatalf("tool clone failure = %v entered=%t nested=%v", err, toolEntered, toolNested)
	}
	clonedExecution, cloneErr := cloneToolExecutionChecked(ToolExecution(prepared))
	if cloneErr == nil || clonedExecution.Tool.Name != "" || clonedExecution.Call.ID != "" {
		t.Fatalf("tool execution clone failure = %v, clone = %#v", cloneErr, clonedExecution)
	}
	for _, test := range []struct {
		name   string
		params *einoschema.ParamsOneOf
	}{
		{name: "panicking parameter entry", params: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{"broken": nil})},
		{name: "empty schema wrapper", params: &einoschema.ParamsOneOf{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			toolEntered = false
			malformed := PreparedToolCall{
				Tool: Tool{Name: "tool", Info: &einoschema.ToolInfo{Name: "tool", ParamsOneOf: test.params}},
				Call: ToolCall{ID: "call", Name: "tool", Input: json.RawMessage(`{}`)},
			}
			_, invokeErr := extension.Invoke(plan, context.Background(), ToolPreparePoint, malformed, func(_ context.Context, input PreparedToolCall) (PreparedToolCall, error) { return input, nil })
			if invokeErr == nil || toolEntered {
				t.Fatalf("malformed schema clone failure = %v entered=%t", invokeErr, toolEntered)
			}
		})
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
			candidate := mustClonePreparedToolCall(t, original)
			test.mutate(&candidate)
			if err := validatePreparedToolCallInput(original, candidate); !errors.Is(err, extension.ErrProtectedMutation) {
				t.Fatalf("validation = %v", err)
			}
		})
	}
}

func TestToolValidationAcceptsJSONCloneTypeNormalization(t *testing.T) {
	original := PreparedToolCall{
		Tool: Tool{Name: "echo", Info: &einoschema.ToolInfo{Name: "echo", Extra: map[string]any{"count": 1}}},
		Call: ToolCall{ID: "call", Name: "echo", Input: json.RawMessage(`{}`)},
	}
	if err := validatePreparedToolCallInput(original, mustClonePreparedToolCall(t, original)); err != nil {
		t.Fatalf("unchanged tool validation = %v", err)
	}
}

func TestProtectedToolInfoClonePreservesAndValidatesParameterSchema(t *testing.T) {
	params := einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{
		"text": {Type: einoschema.String, Required: true},
	})
	original := PreparedToolCall{
		Tool: Tool{Name: "echo", Info: &einoschema.ToolInfo{Name: "echo", ParamsOneOf: params}},
		Call: ToolCall{ID: "call", Name: "echo", Input: json.RawMessage(`{}`)},
	}
	cloned := mustClonePreparedToolCall(t, original)
	if cloned.Tool.Info == nil || cloned.Tool.Info.ParamsOneOf == nil || cloned.Tool.Info.ParamsOneOf == params {
		t.Fatalf("cloned tool info = %#v", cloned.Tool.Info)
	}
	wantSchema, err := params.ToJSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	gotSchema, err := cloned.Tool.Info.ToJSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	wantRaw, _ := json.Marshal(wantSchema)
	gotRaw, _ := json.Marshal(gotSchema)
	if !reflect.DeepEqual(wantRaw, gotRaw) {
		t.Fatalf("cloned schema = %s, want %s", gotRaw, wantRaw)
	}
	if err := validatePreparedToolCallInput(original, cloned); err != nil {
		t.Fatalf("unchanged schema validation = %v", err)
	}

	replaced := mustClonePreparedToolCall(t, original)
	replaced.Tool.Info.ParamsOneOf = einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{
		"count": {Type: einoschema.Integer, Required: true},
	})
	if err := validatePreparedToolCallInput(original, replaced); !errors.Is(err, extension.ErrProtectedMutation) {
		t.Fatalf("schema replacement validation = %v", err)
	}
	removed := mustClonePreparedToolCall(t, original)
	removed.Tool.Info.ParamsOneOf = nil
	if err := validatePreparedToolCallInput(original, removed); !errors.Is(err, extension.ErrProtectedMutation) {
		t.Fatalf("schema removal validation = %v", err)
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

func TestEmptyPlanAcquiresAndResumesWithoutProvider(t *testing.T) {
	orchestrator := mustConfiguredOrchestrator()
	plan, err := orchestrator.acquireRunPlan(context.Background(), RunPlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := plan.Descriptor()
	if descriptor.SchemaVersion != session.ExtensionPlanSchemaVersion || descriptor.Fingerprint == "" {
		t.Fatalf("empty descriptor = %#v", descriptor)
	}
	resumed, err := orchestrator.acquireResumePlan(context.Background(), descriptor)
	if err != nil {
		t.Fatalf("resume empty strict plan = %v", err)
	}
	resumedDescriptor := resumed.Descriptor()
	if resumedDescriptor.Fingerprint != descriptor.Fingerprint || resumedDescriptor.SchemaVersion != descriptor.SchemaVersion || len(resumedDescriptor.Entries) != 0 {
		t.Fatalf("resumed descriptor = %#v, want %#v", resumedDescriptor, descriptor)
	}
}

func TestAcquireResumePlanRejectsInvalidPersistedFingerprintBeforeProvider(t *testing.T) {
	valid := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Entries: []session.ExtensionPlanEntry{testHandlerPlanEntry("callbacks")}}
	valid.Fingerprint, _ = session.FingerprintExtensionPlan(valid)
	for name, descriptor := range map[string]session.ExtensionPlanDescriptor{
		"missing": func() session.ExtensionPlanDescriptor { next := valid.Clone(); next.Fingerprint = ""; return next }(),
		"stale": func() session.ExtensionPlanDescriptor {
			next := valid.Clone()
			next.Entries[0].InstanceID = "corrupt"
			return next
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			resumeCalls := 0
			orchestrator := mustConfiguredOrchestrator(WithRunPlanProvider(staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{}), resumeCalls: &resumeCalls}))
			if _, err := orchestrator.acquireResumePlan(context.Background(), descriptor); !errors.Is(err, ErrExtensionPlanMismatch) {
				t.Fatalf("acquireResumePlan = %v, want ErrExtensionPlanMismatch", err)
			}
			if resumeCalls != 0 {
				t.Fatalf("AcquireResumePlan calls = %d, want 0", resumeCalls)
			}
		})
	}
}

func TestStartReleasesAcquiredPlanWhenResolverPanics(t *testing.T) {
	releases := 0
	plan, err := NewRunPlan(RunPlanSpec{Release: func() { releases++ }})
	if err != nil {
		t.Fatal(err)
	}
	orchestrator := newTestOrchestrator(newAdmissionStore(), scriptedStreamer(nil),
		WithRunPlanProvider(staticRunPlanProvider{plan: plan}),
		WithModelResolver(model.ResolverFunc(func(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
			panic("resolver failed")
		})),
	)
	defer func() {
		if recovered := recover(); recovered != "resolver failed" {
			t.Fatalf("recovered = %#v", recovered)
		}
		if releases != 1 {
			t.Fatalf("plan releases = %d, want 1", releases)
		}
	}()
	_, _ = orchestrator.Start(context.Background(), Request{SessionID: "session", Config: orchestratorConfig()})
}

func TestRunPlanDescriptorReturnsDefensiveClone(t *testing.T) {
	plan, err := NewRunPlan(RunPlanSpec{})
	if err != nil {
		t.Fatal(err)
	}
	first := plan.Descriptor()
	first.SchemaVersion = 0
	if plan.Descriptor().SchemaVersion != session.ExtensionPlanSchemaVersion {
		t.Fatal("descriptor mutation changed sealed plan")
	}
}

func TestResumeRunCallbacksOnlyDoesNotRequireTools(t *testing.T) {
	store := newAdmissionStore()
	now := time.Now().UTC()
	if _, err := store.CreateSession(context.Background(), session.Session{ID: "session", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	run, err := store.AdmitRun(context.Background(), session.Run{ID: "run", SessionID: "session", OwnerID: "old-owner", ClaimToken: "old-claim", Status: session.RunPending, CreatedAt: now}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	orchestrator := mustConfiguredOrchestrator(WithStore(store), WithOwnerID("new-owner"), WithClock(func() time.Time { return now }))
	execution := newRunExecution(orchestrator, mustTestRunPlan(RunPlanSpec{}))
	execution.bindRun(run)
	result := orchestrator.resumeRunWithSettlement(context.Background(), execution, run, nil)
	if errors.Is(result.Error, ErrInvalidOrchestrator) {
		t.Fatalf("resumeRun required settlement store for callback-only plan: %v", result.Error)
	}
}

func TestExecuteResumeSettledDurationStartsAtResumeExecution(t *testing.T) {
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "resume-duration", Artifact: extension.Artifact{Name: "resume-duration", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var duration time.Duration
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, RunSettledPoint, extension.Registration{ID: "settled", Scope: extension.GlobalScope()}, func(_ context.Context, notice RunSettledNotice) error {
			duration = notice.Duration
			return nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	store := newAdmissionStore()
	run := session.Run{ID: "run", SessionID: "session", Status: session.RunPending, CreatedAt: time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)}
	store.runs[run.ID] = run
	now := time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC)
	orchestrator := mustConfiguredOrchestrator(WithStore(store), WithClock(func() time.Time { return now }))
	done := make(chan Result, 1)
	orchestrator.executeResume(context.Background(), newRunExecution(orchestrator, newTestDispatchPlan(dispatch)), run, done)
	result := <-done
	if result.Error != nil || duration != 0 {
		t.Fatalf("resume result=%+v duration=%s", result, duration)
	}
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRunSettledNoticeRequiresDurableFreshTerminalState(t *testing.T) {
	finishErr := errors.New("terminal finish failed")
	for _, test := range []struct {
		name       string
		streamer   scriptedStreamer
		finishErr  error
		wantStatus session.RunStatus
		wantNotice int
		wantError  bool
	}{
		{name: "completed", streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
		}), wantStatus: session.RunCompleted, wantNotice: 1},
		{name: "failed and persisted", streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			return nil, errors.New("provider failed")
		}), wantStatus: session.RunFailed, wantNotice: 1, wantError: true},
		{name: "terminal persistence failed", streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
			return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
		}), finishErr: finishErr, wantStatus: session.RunFailed, wantNotice: 0, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := newAdmissionStore()
			store := &runLifecycleStore{admissionStore: base, terminalFinishErr: test.finishErr}
			events := &capturingSink{}
			var notices []RunSettledNotice
			plan, closePlan := settledNoticePlan(t, &notices)
			defer closePlan()
			orchestrator := mustConfiguredOrchestrator(
				WithStore(store), WithModelResolver(resolvedModel{streamer: test.streamer}),
				WithClock(func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }),
				WithOwnerID("owner-1"), WithQueueSize(2), WithRunPlanProvider(staticRunPlanProvider{plan: plan}), WithEventSink(events),
			)

			result := startAndWait(t, orchestrator)
			if result.Status != test.wantStatus || (result.Error != nil) != test.wantError {
				t.Fatalf("result = %+v", result)
			}
			if len(notices) != test.wantNotice {
				t.Fatalf("settled notices = %#v, want %d", notices, test.wantNotice)
			}
			if len(notices) == 1 && notices[0].Result.Status != test.wantStatus {
				t.Fatalf("settled notice result = %+v", notices[0].Result)
			}
			if got := countEvents(events.events, EventRunFinished); got != test.wantNotice {
				t.Fatalf("run_finished events = %d, want %d", got, test.wantNotice)
			}
		})
	}
}

func TestRunSettledNoticeRequiresDurableResumeTerminalState(t *testing.T) {
	finishErr := errors.New("terminal finish failed")
	listErr := errors.New("unfinished calls unavailable")
	for _, test := range []struct {
		name       string
		finishErr  error
		listErr    error
		wantNotice int
		wantError  error
	}{
		{name: "interrupted and persisted", wantNotice: 1},
		{name: "terminal persistence failed", finishErr: finishErr, wantError: finishErr},
		{name: "pre-finalization failure", listErr: listErr, wantNotice: 1, wantError: listErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := newAdmissionStore()
			run := session.Run{ID: "resume-run", SessionID: "resume-session", OwnerID: "old-owner", ClaimToken: "resume-claim", Status: session.RunPending, CreatedAt: time.Now().UTC()}
			base.runs[run.ID] = run
			store := &runLifecycleStore{admissionStore: base, terminalFinishErr: test.finishErr, listErr: test.listErr}
			events := &capturingSink{}
			var notices []RunSettledNotice
			plan, closePlan := settledNoticePlan(t, &notices)
			defer closePlan()
			orchestrator := mustConfiguredOrchestrator(WithStore(store), WithOwnerID("new-owner"), WithClock(time.Now), WithEventSink(events))
			done := make(chan Result, 1)
			orchestrator.executeResume(context.Background(), newRunExecution(orchestrator, plan), run, done)
			result := <-done

			if !errors.Is(result.Error, test.wantError) {
				t.Fatalf("resume result = %+v, want error %v", result, test.wantError)
			}
			if len(notices) != test.wantNotice {
				t.Fatalf("settled notices = %#v, want %d", notices, test.wantNotice)
			}
			if got := countEvents(events.events, EventRunFinished); got != test.wantNotice {
				t.Fatalf("run_finished events = %d, want %d", got, test.wantNotice)
			}
			wantStatus := session.RunInterrupted
			if test.listErr != nil {
				wantStatus = session.RunFailed
			}
			if len(notices) == 1 && notices[0].Result.Status != wantStatus {
				t.Fatalf("settled notice result = %+v, want %s", notices[0].Result, wantStatus)
			}
		})
	}
}

func countEvents(events []Event, kind EventKind) int {
	var count int
	for _, event := range events {
		if event.Kind == kind {
			count++
		}
	}
	return count
}

type runLifecycleStore struct {
	*admissionStore
	terminalFinishErr error
	listErr           error
}

func (s *runLifecycleStore) Execution(fence session.RunFence) session.ExecutionStore {
	return &runLifecycleExecution{ExecutionStore: s.admissionStore.Execution(fence), terminalFinishErr: s.terminalFinishErr}
}

type runLifecycleExecution struct {
	session.ExecutionStore
	terminalFinishErr error
}

func (s *runLifecycleExecution) SettleRun(ctx context.Context, run session.Run, event *session.EventRecord) error {
	if run.Terminal() && s.terminalFinishErr != nil {
		return s.terminalFinishErr
	}
	return s.ExecutionStore.SettleRun(ctx, run, event)
}

func (s *runLifecycleStore) FinishRun(ctx context.Context, run session.Run) error {
	if run.Terminal() && s.terminalFinishErr != nil {
		return s.terminalFinishErr
	}
	return s.admissionStore.FinishRun(ctx, run)
}

func (s *runLifecycleStore) ListUnfinishedToolCalls(ctx context.Context, runID session.RunID) ([]session.ToolCall, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.admissionStore.ListUnfinishedToolCalls(ctx, runID)
}

func settledNoticePlan(t *testing.T, notices *[]RunSettledNotice) (*RunPlan, func()) {
	t.Helper()
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "settlement-gate", Artifact: extension.Artifact{Name: "settlement-gate", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, RunSettledPoint, extension.Registration{ID: "settled", Scope: extension.GlobalScope()}, func(_ context.Context, notice RunSettledNotice) error {
			*notices = append(*notices, notice)
			return nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	return newTestDispatchPlan(dispatch), func() {
		if err := mount.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

type staticRunPlanProvider struct {
	plan        *RunPlan
	resumeCalls *int
}

func (p staticRunPlanProvider) AcquireRunPlan(context.Context, RunPlanRequest) (*RunPlan, error) {
	return p.plan, nil
}

func (p staticRunPlanProvider) AcquireResumePlan(context.Context, session.ExtensionPlanDescriptor) (*RunPlan, error) {
	if p.resumeCalls != nil {
		(*p.resumeCalls)++
	}
	return p.plan, nil
}

type capturingChatModel struct {
	messages      []*einoschema.Message
	toolBindings  int
	streamOptions int
}

func (m *capturingChatModel) Generate(context.Context, []*einoschema.Message, ...einomodel.Option) (*einoschema.Message, error) {
	return nil, errors.New("unused")
}

func (m *capturingChatModel) Stream(_ context.Context, messages []*einoschema.Message, options ...einomodel.Option) (*einoschema.StreamReader[*einoschema.Message], error) {
	m.messages = cloneMessages(messages)
	m.streamOptions = len(options)
	reader, writer := einoschema.Pipe[*einoschema.Message](1)
	writer.Close()
	return reader, nil
}

func (m *capturingChatModel) WithTools([]*einoschema.ToolInfo) (einomodel.ToolCallingChatModel, error) {
	m.toolBindings++
	return m, nil
}
