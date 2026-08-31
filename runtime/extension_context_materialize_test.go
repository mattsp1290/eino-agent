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
	base := []*einoschema.Message{
		einoschema.UserMessage("base-user"),
		einoschema.AssistantMessage("base-assistant", nil),
		einoschema.ToolMessage("base-tool", "call-1"),
	}
	contributions := []contextContribution{
		{Source: "user-b", Order: 20, Message: einoschema.UserMessage("user-b")},
		{Source: "system-b", Order: 30, Message: einoschema.SystemMessage("system-b")},
		{Source: "system-a", Order: 10, Message: einoschema.SystemMessage("system-a")},
		{Source: "user-a", Order: 20, Message: einoschema.UserMessage("user-a")},
	}
	messages, err := materializeContextAssembly(contextAssembly{Base: base, Contributions: contributions})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"system-a", "system-b", "base-user", "base-assistant", "base-tool", "user-a", "user-b"}
	got := make([]string, len(messages))
	for index, message := range messages {
		got[index] = message.Content
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("messages = %v, want %v", got, want)
	}
	if contributions[0].Source != "user-b" || base[0].Content != "base-user" {
		t.Fatal("materialization mutated input ordering or base history")
	}
}

func TestContextContributionRejectsNonTextSystemOrUserMessages(t *testing.T) {
	t.Parallel()
	index := 1
	tests := map[string]*einoschema.Message{
		"assistant":  einoschema.AssistantMessage("assistant", nil),
		"tool":       einoschema.ToolMessage("tool", "call"),
		"unknown":    {Role: einoschema.RoleType("future"), Content: "future"},
		"name":       {Role: einoschema.User, Content: "user", Name: "name"},
		"tool calls": {Role: einoschema.User, Content: "user", ToolCalls: []einoschema.ToolCall{{Index: &index}}},
		"reasoning":  {Role: einoschema.System, Content: "system", ReasoningContent: "hidden"},
		"metadata":   {Role: einoschema.User, Content: "user", Extra: map[string]any{"key": "value"}},
		"input media": {Role: einoschema.User, UserInputMultiContent: []einoschema.MessageInputPart{{
			Type: einoschema.ChatMessagePartTypeText, Text: "user",
		}}},
		"output media": {Role: einoschema.System, AssistantGenMultiContent: []einoschema.MessageOutputPart{{
			Type: einoschema.ChatMessagePartTypeText, Text: "system",
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
		return OnContextSource(registrar, extension.Registration{ID: "context", Order: 10, Scope: extension.GlobalScope()}, func(_ context.Context, _ ContextSourceInput) ([]*einoschema.Message, error) {
			return []*einoschema.Message{
				einoschema.SystemMessage("extension-system"),
				einoschema.UserMessage("extension-user"),
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
	orchestrator := newTestOrchestrator(newAdmissionStore(), scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, message := range request.Messages {
			captured = append(captured, message.Content)
		}
		return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
	}), WithRunPlanProvider(staticRunPlanProvider{plan: plan}))
	result := startAndWaitRequest(t, orchestrator, Request{SessionID: "session", Message: UserMessage{Content: "base-user"}, Config: orchestratorConfig()})
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
			return OnContextSource(registrar, extension.Registration{ID: []string{"c", "b/c"}[index], Order: 20 - index, Scope: extension.GlobalScope()}, func(_ context.Context, input ContextSourceInput) ([]*einoschema.Message, error) {
				if !reflect.DeepEqual(input.Metadata.ToolNames, []string{"original"}) {
					t.Fatalf("source %d metadata = %#v", index, input.Metadata)
				}
				input.Metadata.ToolNames[0] = "mutated"
				return []*einoschema.Message{einoschema.UserMessage(instanceID)}, nil
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
