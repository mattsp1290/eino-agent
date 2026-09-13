package runtime

import (
	"context"
	"reflect"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
)

func TestMaterializeContextAssemblyUsesSystemPreludeAndUserSuffix(t *testing.T) {
	t.Parallel()
	base := []*einoschema.AgenticMessage{
		agenticUserText("base-user"),
		agenticAssistantText("base-assistant"),
		{Role: einoschema.AgenticRoleTypeUser, ContentBlocks: []*einoschema.ContentBlock{{
			Type: einoschema.ContentBlockTypeFunctionToolResult,
			FunctionToolResult: &einoschema.FunctionToolResult{
				CallID: "call-1",
				Content: []*einoschema.FunctionToolResultContentBlock{{
					Type: einoschema.FunctionToolResultContentBlockTypeText, Text: &einoschema.UserInputText{Text: "base-tool"},
				}},
			},
		}}},
	}
	contributions := []contextContribution{
		{Source: "user-b", Order: 20, Message: agenticUserText("user-b")},
		{Source: "system-b", Order: 30, Message: agenticSystemText("system-b")},
		{Source: "system-a", Order: 10, Message: agenticSystemText("system-a")},
		{Source: "user-a", Order: 20, Message: agenticUserText("user-a")},
	}
	messages, err := materializeContextAssembly(contextAssembly{Base: base, Contributions: contributions})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"system-a", "system-b", "base-user", "base-assistant", "base-tool", "user-a", "user-b"}
	got := make([]string, len(messages))
	for index, message := range messages {
		if isFunctionToolResultMessage(message) {
			got[index] = agenticFunctionResultText(message)
		} else {
			got[index] = agenticMessageText(message)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("messages = %v, want %v", got, want)
	}
	if contributions[0].Source != "user-b" || agenticMessageText(base[0]) != "base-user" {
		t.Fatal("materialization mutated input ordering or base history")
	}
	mapped, err := materializeContextAssemblyWithMapping(contextAssembly{Base: base, Contributions: contributions})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mapped.BaseToFinal, []int{2, 3, 4}) || mapped.Messages[mapped.BaseToFinal[1]].Role != einoschema.AgenticRoleTypeAssistant {
		t.Fatalf("base-to-final mapping = %#v", mapped)
	}
}

func TestContextContributionRejectsNonTextSystemOrUserMessages(t *testing.T) {
	t.Parallel()
	toolResultMessage := &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeUser, ContentBlocks: []*einoschema.ContentBlock{{
		Type: einoschema.ContentBlockTypeFunctionToolResult,
		FunctionToolResult: &einoschema.FunctionToolResult{CallID: "call", Content: []*einoschema.FunctionToolResultContentBlock{{
			Type: einoschema.FunctionToolResultContentBlockTypeText, Text: &einoschema.UserInputText{Text: "tool"},
		}}},
	}}}
	tests := map[string]*einoschema.AgenticMessage{
		"assistant":     agenticAssistantText("assistant"),
		"tool result":   toolResultMessage,
		"unknown role":  {Role: einoschema.AgenticRoleType("future"), ContentBlocks: []*einoschema.ContentBlock{{Type: einoschema.ContentBlockTypeUserInputText, UserInputText: &einoschema.UserInputText{Text: "future"}}}},
		"response meta": {Role: einoschema.AgenticRoleTypeUser, ContentBlocks: []*einoschema.ContentBlock{{Type: einoschema.ContentBlockTypeUserInputText, UserInputText: &einoschema.UserInputText{Text: "user"}}}, ResponseMeta: &einoschema.AgenticResponseMeta{}},
		"extra":         {Role: einoschema.AgenticRoleTypeUser, ContentBlocks: []*einoschema.ContentBlock{{Type: einoschema.ContentBlockTypeUserInputText, UserInputText: &einoschema.UserInputText{Text: "user"}}}, Extra: map[string]any{"key": "value"}},
		"streaming meta": {Role: einoschema.AgenticRoleTypeUser, ContentBlocks: []*einoschema.ContentBlock{{
			Type: einoschema.ContentBlockTypeUserInputText, UserInputText: &einoschema.UserInputText{Text: "user"}, StreamingMeta: &einoschema.StreamingMeta{Index: 0},
		}}},
		"block extra": {Role: einoschema.AgenticRoleTypeUser, ContentBlocks: []*einoschema.ContentBlock{{
			Type: einoschema.ContentBlockTypeUserInputText, UserInputText: &einoschema.UserInputText{Text: "user"}, Extra: map[string]any{"key": "value"},
		}}},
		"input media": {Role: einoschema.AgenticRoleTypeUser, ContentBlocks: []*einoschema.ContentBlock{{
			Type: einoschema.ContentBlockTypeUserInputImage, UserInputImage: &einoschema.UserInputImage{URL: "https://example.test/image.png"},
		}}},
		"output media": {Role: einoschema.AgenticRoleTypeSystem, ContentBlocks: []*einoschema.ContentBlock{{
			Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: "system"},
		}}},
	}
	for name, message := range tests {
		message := message
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := materializeContextAssembly(contextAssembly{Contributions: []contextContribution{{Source: name, Message: message}}})
			if err == nil {
				t.Fatal("unsupported contribution was accepted")
			}
		})
	}
}

func TestContextContributionReachesProviderInCanonicalOrder(t *testing.T) {
	registry := newTestExtensionRegistry(nil)
	mount, err := registry.Mount(context.Background(), extension.Component{InstanceID: "context-order", Artifact: extension.Artifact{Name: "context-order", Version: "1", Hash: "hash", ConfigHash: "config", SourceKind: extension.SourceNative}}, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
		return OnContextSource(registrar, extension.Registration{ID: "context", Order: 10, Scope: extension.GlobalScope()}, func(_ context.Context, _ ContextSourceInput) ([]*einoschema.AgenticMessage, error) {
			return []*einoschema.AgenticMessage{
				agenticSystemText("extension-system"),
				agenticUserText("extension-user"),
			}, nil
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
	var captured []string
	orchestrator := newTestOrchestrator(newAdmissionStore(), scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		for _, message := range request.Messages {
			captured = append(captured, agenticMessageText(message))
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}), WithRunPlanProvider(staticRunPlanProvider{plan: plan}))
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "session", Message: TextUserMessage("base-user"), Config: orchestratorConfig()})
	if result.Error != nil {
		t.Fatal(result.Error)
	}
	want := []string{"extension-system", "base-user", "extension-user"}
	if !reflect.DeepEqual(captured, want) {
		t.Fatalf("provider messages = %v, want %v", captured, want)
	}
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestContextSourcesAreIsolatedAndHostOwned(t *testing.T) {
	registry := newTestExtensionRegistry(nil)
	for index, instanceID := range []string{"a/b", "a"} {
		index, instanceID := index, instanceID
		_, err := registry.Mount(context.Background(), extension.Component{InstanceID: instanceID, Artifact: extension.Artifact{Name: instanceID, Version: "1", Hash: "hash", ConfigHash: "config", SourceKind: extension.SourceNative}}, extension.InstallerFunc(func(_ context.Context, registrar extension.Registrar) error {
			return OnContextSource(registrar, extension.Registration{ID: []string{"c", "b/c"}[index], Order: 20 - index, Scope: extension.GlobalScope()}, func(_ context.Context, input ContextSourceInput) ([]*einoschema.AgenticMessage, error) {
				if !reflect.DeepEqual(input.Metadata.ToolNames, []string{"original"}) {
					t.Fatalf("source %d metadata = %#v", index, input.Metadata)
				}
				input.Metadata.ToolNames[0] = "mutated"
				return []*einoschema.AgenticMessage{agenticUserText(instanceID)}, nil
			})
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	plan, err := registry.Snapshot(extension.GlobalScope())
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Release()
	assembled, err := extension.ApplyTransforms(plan, context.Background(), contextAssemblePoint, contextAssembly{Metadata: BoundedTurnMetadata{ToolNames: []string{"original"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(assembled.Contributions) != 2 || assembled.Contributions[0].Source == assembled.Contributions[1].Source {
		t.Fatalf("contributions = %#v", assembled.Contributions)
	}
	if assembled.Contributions[0].Order != 19 || assembled.Contributions[1].Order != 20 {
		t.Fatalf("host orders = %d, %d", assembled.Contributions[0].Order, assembled.Contributions[1].Order)
	}
}
