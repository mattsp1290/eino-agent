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

func TestMountedGuardsReceiveIsolatedRequests(t *testing.T) {
	original := ToolCall{
		SessionID: "session",
		RunID:     "run",
		Input:     json.RawMessage(`{"op":"delete"}`),
		Scope:     ToolScope{Permissions: []string{"write"}},
	}
	plan := &RunPlan{Guards: []MountedToolGuard{
		{ID: "mutate", Guard: ToolGuardFunc(func(_ context.Context, request ToolGuardRequest) (ToolGuardResult, error) {
			copy(request.Call.Input, json.RawMessage(`{"op":"hidden"}`))
			copy(request.Call.Scope.Permissions, []string{"audit"})
			return ToolGuardResult{Decision: ToolGuardAbstain}, nil
		})},
		{ID: "deny-original", Guard: ToolGuardFunc(func(_ context.Context, request ToolGuardRequest) (ToolGuardResult, error) {
			if string(request.Call.Input) != `{"op":"delete"}` || !reflect.DeepEqual(request.Call.Scope.Permissions, []string{"write"}) {
				t.Fatalf("second guard request = %#v", request.Call)
			}
			return ToolGuardResult{Decision: ToolGuardDeny}, nil
		})},
	}}

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

func TestModelStreamPointRejectsSwallowedProviderFailure(t *testing.T) {
	providerErr := errors.New("provider failure")
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "stream-swallow", Artifact: extension.Artifact{Name: "stream-swallow", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.Use(registrar, ModelStreamPoint, extension.Registration{ID: "swallow", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(ctx context.Context, input ModelStreamInput, next extension.Next[ModelStreamInput, *einoschema.StreamReader[*einoschema.Message]]) (*einoschema.StreamReader[*einoschema.Message], error) {
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

func TestModelStreamValidationAcceptsUnchangedFunctionBackedModelWithoutHandlers(t *testing.T) {
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "unrelated", Artifact: extension.Artifact{Name: "unrelated", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, ModelRequestedPoint, extension.Registration{ID: "notice", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(context.Context, ModelRequestedNotice) error { return nil })
	}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	streamer := functionStreamer(streamFromFunction)
	client := functionClient(clientFunctionOne)
	terminalCalled := false
	observer := functionObserver(func() {})
	reader, err := extension.Invoke(plan, context.Background(), ModelStreamPoint, ModelStreamInput{Resolved: model.Resolved{Client: client, Streamer: streamer}, Request: model.Request{Observer: observer}}, func(context.Context, ModelStreamInput) (*einoschema.StreamReader[*einoschema.Message], error) {
		terminalCalled = true
		reader, writer := einoschema.Pipe[*einoschema.Message](1)
		writer.Close()
		return reader, nil
	})
	if err != nil || !terminalCalled || reader == nil {
		t.Fatalf("model stream = %#v, %v terminal=%t", reader, err, terminalCalled)
	}
	reader.Close()
}

func TestModelStreamValidationRejectsCallableReplacement(t *testing.T) {
	original := ModelStreamInput{Resolved: model.Resolved{Client: functionClient(clientFunctionOne), Streamer: functionStreamer(streamFromFunction)}}
	candidate := cloneModelStreamInput(original)
	candidate.Resolved.Client = functionClient(clientFunctionTwo)
	if err := validateModelStreamInput(original, candidate); !errors.Is(err, extension.ErrProtectedMutation) {
		t.Fatalf("client replacement validation = %v", err)
	}
}

func TestModelStreamPointRejectsNestedRequestMutationWithoutAliasingOriginal(t *testing.T) {
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "stream-nested-mutation", Artifact: extension.Artifact{Name: "stream-nested-mutation", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.Use(registrar, ModelStreamPoint, extension.Registration{ID: "mutate", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(ctx context.Context, input ModelStreamInput, next extension.Next[ModelStreamInput, *einoschema.StreamReader[*einoschema.Message]]) (*einoschema.StreamReader[*einoschema.Message], error) {
			input.Request.Messages[0].ToolCalls[0].Extra["nested"].(map[string]any)["secret"] = "mutated"
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

	original := ModelStreamInput{Request: model.Request{Messages: []*einoschema.Message{{
		ToolCalls: []einoschema.ToolCall{{Extra: map[string]any{"nested": map[string]any{"secret": "original"}}}},
	}}}}
	providerCalled := false
	reader, err := extension.Invoke(plan, context.Background(), ModelStreamPoint, original, func(context.Context, ModelStreamInput) (*einoschema.StreamReader[*einoschema.Message], error) {
		providerCalled = true
		return nil, nil
	})
	if reader != nil || !errors.Is(err, extension.ErrProtectedMutation) || providerCalled {
		t.Fatalf("reader=%v error=%v provider_called=%t", reader, err, providerCalled)
	}
	if got := original.Request.Messages[0].ToolCalls[0].Extra["nested"].(map[string]any)["secret"]; got != "original" {
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

func TestInterfaceIdentitySupportsFunctionValuesAndPointerIdentity(t *testing.T) {
	callable := runtimeToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{}, nil })
	if !sameInterfaceIdentity(callable, callable) {
		t.Fatal("unchanged function-backed callable lost identity")
	}
	functionFactory := func(marker string) runtimeToolExecutorFunc {
		return func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: marker}, nil }
	}
	if sameInterfaceIdentity(functionFactory("first"), functionFactory("second")) {
		t.Fatal("distinct closures from the same factory shared identity")
	}
	first := &callable
	other := callable
	second := &other
	if !sameInterfaceIdentity(first, first) || sameInterfaceIdentity(first, second) {
		t.Fatal("pointer-backed callable identity comparison is not exact")
	}
}

func TestProtectedCloneFailureStopsContextAndToolInterceptors(t *testing.T) {
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "clone-failure", Artifact: extension.Artifact{Name: "clone-failure", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var contextEntered, toolEntered bool
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.Use(registrar, ContextAssemblePoint, extension.Registration{ID: "context", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(ctx context.Context, input ContextAssembly, next extension.Next[ContextAssembly, ContextAssembly]) (ContextAssembly, error) {
			contextEntered = true
			input.Base[0].Extra["nested"].(map[string]any)["value"] = "mutated"
			return next(ctx, input)
		}); err != nil {
			return err
		}
		return extension.Use(registrar, ToolPreparePoint, extension.Registration{ID: "tool", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(ctx context.Context, input PreparedToolCall, next extension.Next[PreparedToolCall, PreparedToolCall]) (PreparedToolCall, error) {
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
	if !errors.Is(err, extension.ErrProtectedMutation) || contextEntered || messageNested["value"] != "original" {
		t.Fatalf("context clone failure = %v entered=%t nested=%v", err, contextEntered, messageNested)
	}
	toolNested := map[string]any{"value": "original"}
	tool := Tool{Name: "tool", Info: &einoschema.ToolInfo{Name: "tool", Extra: map[string]any{"nested": toolNested, "unsupported": make(chan struct{})}}}
	prepared := PreparedToolCall{Tool: tool, Call: ToolCall{ID: "call", Name: "tool", Input: json.RawMessage(`{}`)}}
	_, err = extension.Invoke(plan, context.Background(), ToolPreparePoint, prepared, func(_ context.Context, input PreparedToolCall) (PreparedToolCall, error) { return input, nil })
	if !errors.Is(err, extension.ErrProtectedMutation) || toolEntered || toolNested["value"] != "original" {
		t.Fatalf("tool clone failure = %v entered=%t nested=%v", err, toolEntered, toolNested)
	}
	if err := validateToolExecutionInput(ToolExecution(prepared), cloneToolExecution(ToolExecution(prepared))); !errors.Is(err, extension.ErrProtectedMutation) {
		t.Fatalf("tool execution clone failure validation = %v", err)
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
			if !errors.Is(invokeErr, extension.ErrProtectedMutation) || toolEntered {
				t.Fatalf("malformed schema clone failure = %v entered=%t", invokeErr, toolEntered)
			}
		})
	}
}

type functionStreamer func(context.Context, model.Request) (*einoschema.StreamReader[*einoschema.Message], error)

func (f functionStreamer) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	return f(ctx, request)
}

func streamFromFunction(context.Context, model.Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	reader, writer := einoschema.Pipe[*einoschema.Message](1)
	writer.Close()
	return reader, nil
}

type functionClient func()

func (f functionClient) Generate(context.Context, []*einoschema.Message, ...einomodel.Option) (*einoschema.Message, error) {
	f()
	return nil, nil
}

func (f functionClient) Stream(context.Context, []*einoschema.Message, ...einomodel.Option) (*einoschema.StreamReader[*einoschema.Message], error) {
	f()
	return streamFromFunction(context.Background(), model.Request{})
}

func (f functionClient) WithTools([]*einoschema.ToolInfo) (einomodel.ToolCallingChatModel, error) {
	return f, nil
}

func clientFunctionOne() {}
func clientFunctionTwo() {}

type functionObserver func()

func (f functionObserver) OnProviderStart(context.Context, model.Request)     { f() }
func (f functionObserver) OnProviderDelta(context.Context, model.StreamDelta) { f() }
func (f functionObserver) OnProviderError(context.Context, model.Error)       { f() }
func (f functionObserver) OnProviderEnd(context.Context, model.Response)      { f() }

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

func TestToolValidationAcceptsJSONCloneTypeNormalization(t *testing.T) {
	original := PreparedToolCall{
		Tool: Tool{Name: "echo", Info: &einoschema.ToolInfo{Name: "echo", Extra: map[string]any{"count": 1}}},
		Call: ToolCall{ID: "call", Name: "echo", Input: json.RawMessage(`{}`)},
	}
	if err := validatePreparedToolCallInput(original, clonePreparedToolCall(original)); err != nil {
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
	cloned := clonePreparedToolCall(original)
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

	replaced := clonePreparedToolCall(original)
	replaced.Tool.Info.ParamsOneOf = einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{
		"count": {Type: einoschema.Integer, Required: true},
	})
	if err := validatePreparedToolCallInput(original, replaced); !errors.Is(err, extension.ErrProtectedMutation) {
		t.Fatalf("schema replacement validation = %v", err)
	}
	removed := clonePreparedToolCall(original)
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

func TestPartialLegacyToolPlanDoesNotRequireSettlementStore(t *testing.T) {
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanStrict, Entries: []session.ExtensionPlanEntry{{InstanceID: "tool-plugin", Kind: session.ExtensionTool, Required: true}}}
	descriptor.Fingerprint, _ = session.FingerprintExtensionPlan(descriptor)
	orchestrator := &StreamingOrchestrator{Store: newAdmissionStore(), Tools: ToolRegistryFunc(func(context.Context, TurnSnapshot) ([]Tool, error) { return nil, nil }), Plans: staticRunPlanProvider{plan: &RunPlan{Descriptor: descriptor}}}
	plan, err := orchestrator.acquireRunPlan(context.Background(), RunPlanRequest{SessionID: "session"})
	if err != nil {
		t.Fatalf("acquire partial-legacy tool plan = %v", err)
	}
	if plan.Descriptor.Mode != session.PlanPartialLegacy {
		t.Fatalf("mode = %s, want partial-legacy", plan.Descriptor.Mode)
	}
}

func TestAcquireResumePlanUsesStrictToolSettlementPredicate(t *testing.T) {
	for _, test := range []struct {
		name       string
		descriptor session.ExtensionPlanDescriptor
	}{
		{name: "strict callbacks only", descriptor: session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanStrict, Entries: []session.ExtensionPlanEntry{{InstanceID: "callbacks", Kind: session.ExtensionHandlers, Required: true}}}},
		{name: "partial legacy tool", descriptor: session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanPartialLegacy, Entries: []session.ExtensionPlanEntry{{InstanceID: "tool", Kind: session.ExtensionTool, Required: true}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.descriptor.Fingerprint, _ = session.FingerprintExtensionPlan(test.descriptor)
			orchestrator := &StreamingOrchestrator{Store: newAdmissionStore(), Plans: staticRunPlanProvider{plan: &RunPlan{Descriptor: test.descriptor}}}
			if _, err := orchestrator.acquireResumePlan(context.Background(), test.descriptor); err != nil {
				t.Fatalf("acquireResumePlan = %v", err)
			}
		})
	}
}

func TestAcquireResumePlanRejectsLiveLegacyExtensionsForStrictDescriptor(t *testing.T) {
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanStrict}
	descriptor.Fingerprint, _ = session.FingerprintExtensionPlan(descriptor)

	tests := []struct {
		name      string
		configure func(*StreamingOrchestrator)
	}{
		{name: "tools", configure: func(orchestrator *StreamingOrchestrator) {
			orchestrator.Tools = ToolRegistryFunc(func(context.Context, TurnSnapshot) ([]Tool, error) { return nil, nil })
		}},
		{name: "context", configure: func(orchestrator *StreamingOrchestrator) {
			orchestrator.Context = []ContextSource{ContextSourceFunc(func(context.Context, TurnSnapshot) ([]*einoschema.Message, error) { return nil, nil })}
		}},
		{name: "hooks", configure: func(orchestrator *StreamingOrchestrator) {
			orchestrator.Hooks = []Hook{HookFuncs{}}
		}},
		{name: "middleware", configure: func(orchestrator *StreamingOrchestrator) {
			orchestrator.Middleware = []ToolMiddleware{ToolMiddlewareFuncs{}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resumeCalls := 0
			orchestrator := &StreamingOrchestrator{Store: newAdmissionStore(), Plans: staticRunPlanProvider{plan: &RunPlan{Descriptor: descriptor}, resumeCalls: &resumeCalls}}
			test.configure(orchestrator)
			if _, err := orchestrator.acquireResumePlan(context.Background(), descriptor); !errors.Is(err, ErrExtensionPlanMismatch) {
				t.Fatalf("acquireResumePlan error = %v, want ErrExtensionPlanMismatch", err)
			}
			if resumeCalls != 0 {
				t.Fatalf("AcquireResumePlan calls = %d, want 0", resumeCalls)
			}
		})
	}

	t.Run("strict without legacy extensions", func(t *testing.T) {
		orchestrator := &StreamingOrchestrator{Store: newAdmissionStore(), Plans: staticRunPlanProvider{plan: &RunPlan{Descriptor: descriptor}}}
		if _, err := orchestrator.acquireResumePlan(context.Background(), descriptor); err != nil {
			t.Fatalf("acquireResumePlan = %v", err)
		}
	})

	t.Run("partial legacy permits live extensions", func(t *testing.T) {
		partial := descriptor.Clone()
		partial.Mode = session.PlanPartialLegacy
		partial.Fingerprint, _ = session.FingerprintExtensionPlan(partial)
		orchestrator := &StreamingOrchestrator{
			Store:      newAdmissionStore(),
			Plans:      staticRunPlanProvider{plan: &RunPlan{Descriptor: partial}},
			Middleware: []ToolMiddleware{ToolMiddlewareFuncs{}},
		}
		if _, err := orchestrator.acquireResumePlan(context.Background(), partial); err != nil {
			t.Fatalf("acquireResumePlan = %v", err)
		}
	})
}

func TestAcquireResumePlanRecomputesProviderDescriptorFingerprint(t *testing.T) {
	descriptor := session.ExtensionPlanDescriptor{
		SchemaVersion: session.ExtensionPlanSchemaVersion,
		Mode:          session.PlanStrict,
		Entries: []session.ExtensionPlanEntry{{
			InstanceID: "callbacks",
			Kind:       session.ExtensionHandlers,
			Required:   true,
			Artifact:   session.ArtifactIdentity{Name: "callbacks", Hash: "persisted"},
		}},
	}
	descriptor.Fingerprint, _ = session.FingerprintExtensionPlan(descriptor)

	t.Run("rejects copied stale fingerprint", func(t *testing.T) {
		changed := descriptor.Clone()
		changed.Entries[0].Artifact.Hash = "changed"
		changed.Fingerprint = descriptor.Fingerprint
		released := false
		orchestrator := &StreamingOrchestrator{
			Store: newAdmissionStore(),
			Plans: staticRunPlanProvider{plan: &RunPlan{Descriptor: changed, Release: func() {
				released = true
			}}},
		}
		if _, err := orchestrator.acquireResumePlan(context.Background(), descriptor); !errors.Is(err, ErrExtensionPlanMismatch) || !released {
			t.Fatalf("acquireResumePlan = %v released=%t", err, released)
		}
	})

	t.Run("canonicalizes omitted fingerprint", func(t *testing.T) {
		matching := descriptor.Clone()
		matching.Fingerprint = ""
		orchestrator := &StreamingOrchestrator{Store: newAdmissionStore(), Plans: staticRunPlanProvider{plan: &RunPlan{Descriptor: matching}}}
		plan, err := orchestrator.acquireResumePlan(context.Background(), descriptor)
		if err != nil {
			t.Fatalf("acquireResumePlan = %v", err)
		}
		if plan.Descriptor.Fingerprint != descriptor.Fingerprint {
			t.Fatalf("fingerprint = %q, want %q", plan.Descriptor.Fingerprint, descriptor.Fingerprint)
		}
	})
}

func TestResumeRunStrictCallbacksOnlyDoesNotRequireSettlementStore(t *testing.T) {
	store := newAdmissionStore()
	now := time.Now().UTC()
	if _, err := store.CreateSession(context.Background(), session.Session{ID: "session", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	run, err := store.AdmitRun(context.Background(), session.Run{ID: "run", SessionID: "session", OwnerID: "old-owner", Status: session.RunPending, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := session.ExtensionPlanDescriptor{SchemaVersion: session.ExtensionPlanSchemaVersion, Mode: session.PlanStrict, Entries: []session.ExtensionPlanEntry{{InstanceID: "callbacks", Kind: session.ExtensionHandlers, Required: true}}}
	orchestrator := &StreamingOrchestrator{Store: store, OwnerID: "new-owner", Clock: func() time.Time { return now }}
	result := orchestrator.resumeRun(withRunPlan(context.Background(), &RunPlan{Descriptor: descriptor}), run)
	if errors.Is(result.Error, ErrInvalidOrchestrator) {
		t.Fatalf("resumeRun required settlement store for callback-only plan: %v", result.Error)
	}
}

func TestExecuteResumeSettledDurationStartsAtResumeExecution(t *testing.T) {
	registry := extension.NewRegistry(nil)
	component := extension.Component{InstanceID: "resume-duration", Artifact: extension.Artifact{Name: "resume-duration", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var duration time.Duration
	mount, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.On(registrar, RunSettledPoint, extension.Registration{ID: "settled", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(_ context.Context, notice RunSettledNotice) error {
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
	orchestrator := &StreamingOrchestrator{Store: store, Clock: func() time.Time { return now }}
	done := make(chan Result, 1)
	orchestrator.executeResume(withRunPlan(context.Background(), &RunPlan{Dispatch: dispatch}), run, done)
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
			var notices []RunSettledNotice
			plan, closePlan := settledNoticePlan(t, &notices)
			defer closePlan()
			orchestrator := &StreamingOrchestrator{
				Store: store, Model: resolvedModel{streamer: test.streamer}, IDs: &sequenceIDs{},
				Clock: func() time.Time { return time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC) }, OwnerID: "owner-1", QueueSize: 2,
			}
			orchestrator.Plans = staticRunPlanProvider{plan: plan}

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
		{name: "pre-finalization failure", listErr: listErr, wantError: listErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := newAdmissionStore()
			run := session.Run{ID: "resume-run", SessionID: "resume-session", OwnerID: "old-owner", Status: session.RunPending, CreatedAt: time.Now().UTC()}
			base.runs[run.ID] = run
			store := &runLifecycleStore{admissionStore: base, terminalFinishErr: test.finishErr, listErr: test.listErr}
			var notices []RunSettledNotice
			plan, closePlan := settledNoticePlan(t, &notices)
			defer closePlan()
			orchestrator := &StreamingOrchestrator{Store: store, IDs: &sequenceIDs{}, OwnerID: "new-owner", Clock: time.Now}
			done := make(chan Result, 1)
			orchestrator.executeResume(withRunPlan(context.Background(), plan), run, done)
			result := <-done

			if !errors.Is(result.Error, test.wantError) {
				t.Fatalf("resume result = %+v, want error %v", result, test.wantError)
			}
			if len(notices) != test.wantNotice {
				t.Fatalf("settled notices = %#v, want %d", notices, test.wantNotice)
			}
			if len(notices) == 1 && notices[0].Result.Status != session.RunInterrupted {
				t.Fatalf("settled notice result = %+v", notices[0].Result)
			}
		})
	}
}

type runLifecycleStore struct {
	*admissionStore
	terminalFinishErr error
	listErr           error
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
		return extension.On(registrar, RunSettledPoint, extension.Registration{ID: "settled", InstanceID: component.InstanceID, Scope: extension.GlobalScope()}, func(_ context.Context, notice RunSettledNotice) error {
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
	return &RunPlan{Dispatch: dispatch}, func() {
		if err := mount.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestVersionOnePromptAndGuardDescriptorsAreUnverifiable(t *testing.T) {
	for _, kind := range []session.ExtensionKind{session.ExtensionPrompt, session.ExtensionGuard} {
		descriptor := session.ExtensionPlanDescriptor{SchemaVersion: 1, Mode: session.PlanStrict, Entries: []session.ExtensionPlanEntry{{InstanceID: "ordered", Kind: kind, Required: true}}}
		descriptor.Fingerprint, _ = session.FingerprintExtensionPlan(descriptor)
		provider := staticRunPlanProvider{plan: &RunPlan{Descriptor: descriptor}}
		orchestrator := &StreamingOrchestrator{Store: newAdmissionStore(), Plans: provider}
		if _, err := orchestrator.acquireRunPlan(context.Background(), RunPlanRequest{}); !errors.Is(err, ErrExtensionPlanMismatch) {
			t.Fatalf("kind %s fresh error = %v, want mismatch", kind, err)
		}
		provider.plan = &RunPlan{Descriptor: descriptor}
		orchestrator.Plans = provider
		if _, err := orchestrator.acquireResumePlan(context.Background(), descriptor); !errors.Is(err, ErrExtensionPlanMismatch) {
			t.Fatalf("kind %s resume error = %v, want mismatch", kind, err)
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
