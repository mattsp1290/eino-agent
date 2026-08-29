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

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
)

func mustClonePreparedToolCall(t *testing.T, value PreparedToolCall) PreparedToolCall {
	t.Helper()
	cloned, err := clonePreparedToolCallChecked(value)
	if err != nil {
		t.Fatal(err)
	}
	return cloned
}

func TestToolResultTransformRejectsCallMutation(t *testing.T) {
	original := ToolResultTransform{ToolName: "echo", Call: ToolCall{ID: "call", Name: "echo"}}
	candidate := original
	candidate.Call.ID = "other"
	if err := validateToolResultTransformInput(original, candidate); !errors.Is(err, extension.ErrProtectedMutation) {
		t.Fatalf("validation = %v", err)
	}
}

func TestToolExecutionPreservesExecutorAndCallbackErrors(t *testing.T) {
	executorErr := errors.New("executor failed")
	callbackErr := errors.New("middleware failed")
	registry := newTestExtensionRegistry(nil)
	mount, err := registry.Mount(context.Background(), testExtensionComponent("tool-errors"), extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.OnAround(registrar, ToolExecutePoint, extension.Registration{ID: "execute", Scope: extension.GlobalScope()}, func(ctx context.Context, input ToolExecution, next extension.Next[ToolExecution, ToolResult]) (ToolResult, error) {
			result, err := next(ctx, input)
			if err != nil {
				return ToolResult{}, err
			}
			return result, callbackErr
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	plan := mustTestRunPlan(testDispatchPlanSpec(dispatch))
	defer func() {
		plan.Release()
		_ = mount.Close(context.Background())
	}()
	host := mustConfiguredOrchestrator()
	tool := Tool{Name: "echo", Executor: runtimeToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		return ToolResult{Output: "partial"}, executorErr
	})}
	outcome := host.executeToolOutcome(context.Background(), newTestRunExecution(host, plan), tool, ToolCall{ID: "call", Name: "echo", Input: json.RawMessage(`{}`)})
	if !errors.Is(outcome.RawError, executorErr) || !errors.Is(outcome.RawError, callbackErr) || outcome.Disposition != ToolFailed {
		t.Fatalf("outcome = %#v", outcome)
	}
}

func TestToolMiddlewareCannotForgePermissionStateOrReceiveApproval(t *testing.T) {
	registry := newTestExtensionRegistry(nil)
	mount, err := registry.Mount(context.Background(), testExtensionComponent("tool-permission-state"), extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.OnAround(registrar, ToolExecutePoint, extension.Registration{ID: "execute", Scope: extension.GlobalScope()}, func(ctx context.Context, input ToolExecution, next extension.Next[ToolExecution, ToolResult]) (ToolResult, error) {
			result, err := next(ctx, input)
			result.Metadata = map[string]string{"permission_status": "denied", "permission_forged": "true"}
			return result, err
		}); err != nil {
			return err
		}
		return extension.OnTransform(registrar, ToolResultTransformPoint, extension.Registration{ID: "result", Scope: extension.GlobalScope()}, func(_ context.Context, input ToolResultTransform) (ToolResultTransform, error) {
			if input.Call.Approval != nil {
				return ToolResultTransform{}, errors.New("result middleware received approval capability")
			}
			input.Result.Metadata = map[string]string{"permission_status": "approval_required", "permission_forged": "true"}
			return input, nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	plan := mustTestRunPlan(testDispatchPlanSpec(dispatch))
	defer func() {
		plan.Release()
		_ = mount.Close(context.Background())
	}()
	host := mustConfiguredOrchestrator()
	tool := Tool{Name: "echo", Executor: runtimeToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		return ToolResult{Output: "ok", Metadata: map[string]string{"permission_status": "interrupted"}}, nil
	})}
	call := ToolCall{ID: "call", Name: "echo", Input: json.RawMessage(`{}`), Approval: approvalFunc(func(context.Context, ApprovalRequest) error { return nil })}
	outcome := host.executeToolOutcome(context.Background(), newTestRunExecution(host, plan), tool, call)
	outcome = host.transformToolOutcome(context.Background(), newTestRunExecution(host, plan), outcome)
	if outcome.Disposition != ToolExecuted || outcome.Result.Metadata["permission_status"] != "" || outcome.Result.Metadata["permission_forged"] != "" {
		t.Fatalf("outcome = %#v", outcome)
	}
}

func testExtensionComponent(id string) extension.Component {
	return extension.Component{InstanceID: id, Artifact: extension.Artifact{Name: id, Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
}

func TestModelStreamPointRejectsFabricatedSuccessfulReader(t *testing.T) {
	registry := newTestExtensionRegistry(nil)
	component := extension.Component{InstanceID: "stream-test", Artifact: extension.Artifact{Name: "stream-test", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.OnAround(registrar, ModelStreamPoint, extension.Registration{ID: "replace", Scope: extension.GlobalScope()}, func(ctx context.Context, input ModelStreamInput, next extension.Next[ModelStreamInput, *einoschema.StreamReader[model.StreamDelta]]) (*einoschema.StreamReader[model.StreamDelta], error) {
			delegated, err := next(ctx, input)
			if delegated != nil {
				delegated.Close()
			}
			if err != nil {
				return nil, err
			}
			reader, writer := einoschema.Pipe[model.StreamDelta](1)
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
	_, err = extension.InvokeAround(plan, context.Background(), ModelStreamPoint, ModelStreamInput{}, func(context.Context, ModelStreamInput) (*einoschema.StreamReader[model.StreamDelta], error) {
		reader, writer := einoschema.Pipe[model.StreamDelta](1)
		writer.Close()
		return reader, nil
	})
	if !errors.Is(err, extension.ErrProtectedMutation) {
		t.Fatalf("fabricated stream error = %v", err)
	}
}

func TestModelStreamPointRejectsSwallowedProviderFailure(t *testing.T) {
	providerErr := errors.New("provider failure")
	registry := newTestExtensionRegistry(nil)
	component := extension.Component{InstanceID: "stream-swallow", Artifact: extension.Artifact{Name: "stream-swallow", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.OnAround(registrar, ModelStreamPoint, extension.Registration{ID: "swallow", Scope: extension.GlobalScope()}, func(ctx context.Context, input ModelStreamInput, next extension.Next[ModelStreamInput, *einoschema.StreamReader[model.StreamDelta]]) (*einoschema.StreamReader[model.StreamDelta], error) {
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
	reader, err := extension.InvokeAround(plan, context.Background(), ModelStreamPoint, ModelStreamInput{}, func(context.Context, ModelStreamInput) (*einoschema.StreamReader[model.StreamDelta], error) {
		return nil, providerErr
	})
	if reader != nil || !errors.Is(err, providerErr) {
		t.Fatalf("model stream = %#v, %v; want provider failure", reader, err)
	}
}

func TestModelStreamValidationUsesDataOnlyView(t *testing.T) {
	registry := newTestExtensionRegistry(nil)
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
	reader, err := extension.InvokeAround(plan, context.Background(), ModelStreamPoint, input, func(_ context.Context, value ModelStreamInput) (*einoschema.StreamReader[model.StreamDelta], error) {
		terminalCalled = true
		if value.ProviderID != "provider" || value.ModelID != "model" || value.ContentHash != "hash" {
			t.Fatalf("model stream terminal received wrong data: %#v", value)
		}
		reader, writer := einoschema.Pipe[model.StreamDelta](1)
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
	registry := newTestExtensionRegistry(nil)
	component := extension.Component{InstanceID: "stream-nested-mutation", Artifact: extension.Artifact{Name: "stream-nested-mutation", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.OnAround(registrar, ModelStreamPoint, extension.Registration{ID: "mutate", Scope: extension.GlobalScope()}, func(ctx context.Context, input ModelStreamInput, next extension.Next[ModelStreamInput, *einoschema.StreamReader[model.StreamDelta]]) (*einoschema.StreamReader[model.StreamDelta], error) {
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
	reader, err := extension.InvokeAround(plan, context.Background(), ModelStreamPoint, original, func(context.Context, ModelStreamInput) (*einoschema.StreamReader[model.StreamDelta], error) {
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
	registry := newTestExtensionRegistry(nil)
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
	extension.Notify(plan, context.Background(), ToolSettledPoint, source)
	if secondValue != "original" || source.Result.Attachments[0].Metadata["owner"] != "original" {
		t.Fatalf("attachment metadata leaked: second=%q source=%q", secondValue, source.Result.Attachments[0].Metadata["owner"])
	}
}

func TestTurnPreparePointRunsAfterPlannedToolsResolve(t *testing.T) {
	registry := newTestExtensionRegistry(nil)
	component := extension.Component{InstanceID: "turn-order", Artifact: extension.Artifact{Name: "turn-order", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var seen BoundedTurnMetadata
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return extension.OnHook(registrar, TurnPreparePoint, extension.Registration{ID: "observe", Scope: extension.GlobalScope()}, func(_ context.Context, metadata BoundedTurnMetadata) error {
			seen = cloneBoundedTurnMetadata(metadata)
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
	defer dispatch.Release()
	spec := testDispatchPlanSpec(dispatch)
	spec.Components = append(spec.Components, PlanComponent{Component: testPlanComponent("test-tools"), Tools: []PlanTool{{Identity: testToolIdentity("echo"), Resolve: func(context.Context, ToolScopeContext) (Tool, error) { return Tool{Name: "echo"}, nil }}}})
	plan := mustTestRunPlan(spec)
	snapshot := TurnSnapshot{RunID: "run", SessionID: "session", Messages: []*einoschema.Message{einoschema.UserMessage("hidden")}}
	host := mustConfiguredOrchestrator()
	prepared, err := host.prepareSnapshot(context.Background(), newTestRunExecution(host, plan), snapshot)
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
	registry := newTestExtensionRegistry(nil)
	component := extension.Component{InstanceID: "clone-failure", Artifact: extension.Artifact{Name: "clone-failure", Version: "1", Hash: "artifact", ConfigHash: "config", SourceKind: extension.SourceNative}}
	var contextEntered, toolEntered bool
	_, err := registry.Mount(context.Background(), component, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		if err := extension.OnTransform(registrar, ContextAssemblePoint, extension.Registration{ID: "context", Scope: extension.GlobalScope()}, func(_ context.Context, input ContextAssembly) (ContextAssembly, error) {
			contextEntered = true
			input.Base[0].Extra["nested"].(map[string]any)["value"] = "mutated"
			return input, nil
		}); err != nil {
			return err
		}
		return extension.OnTransform(registrar, ToolPreparePoint, extension.Registration{ID: "tool", Scope: extension.GlobalScope()}, func(_ context.Context, input PreparedToolCall) (PreparedToolCall, error) {
			toolEntered = true
			input.Tool.Info.Extra["nested"].(map[string]any)["value"] = "mutated"
			return input, nil
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
	_, err = extension.ApplyTransforms(plan, context.Background(), ContextAssemblePoint, ContextAssembly{Base: []*einoschema.Message{message}})
	if err == nil || contextEntered || messageNested["value"] != "original" {
		t.Fatalf("context clone failure = %v entered=%t nested=%v", err, contextEntered, messageNested)
	}
	toolNested := map[string]any{"value": "original"}
	tool := Tool{Name: "tool", Info: &einoschema.ToolInfo{Name: "tool", Extra: map[string]any{"nested": toolNested, "unsupported": make(chan struct{})}}}
	prepared := PreparedToolCall{Tool: tool, Call: ToolCall{ID: "call", Name: "tool", Input: json.RawMessage(`{}`)}}
	_, err = extension.ApplyTransforms(plan, context.Background(), ToolPreparePoint, prepared)
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
			_, invokeErr := extension.ApplyTransforms(plan, context.Background(), ToolPreparePoint, malformed)
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
