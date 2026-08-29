package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
)

func TestSystemPromptMaterializationIsUnconditionalAndOrdered(t *testing.T) {
	snapshot := TurnSnapshot{RunID: "run", SessionID: "session", EpochID: "epoch", SystemPrompt: "configured"}
	plan := mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{
		{Component: testPlanComponent("b"), Prompts: []PlanPrompt{{Identity: testPromptIdentity("z", "b", 10), Provider: PromptProviderFunc(func(context.Context, PromptContext) (string, error) { return "last", nil })}}},
		{Component: testPlanComponent("a"), Prompts: []PlanPrompt{{Identity: testPromptIdentity("a", "a", -10), Provider: PromptProviderFunc(func(context.Context, PromptContext) (string, error) { return "first", nil })}}},
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
	plan := mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{{Component: testPlanComponent("test-guards"), Guards: []PlanGuard{
		{Identity: testGuardIdentityAt("deny", 0), Guard: ToolGuardFunc(func(context.Context, ToolGuardRequest) (ToolGuardResult, error) {
			sequence = append(sequence, "deny")
			return ToolGuardResult{Decision: ToolGuardDeny, Message: "blocked"}, nil
		})},
		{Identity: testGuardIdentityAt("audit", 1), Guard: ToolGuardFunc(func(context.Context, ToolGuardRequest) (ToolGuardResult, error) {
			sequence = append(sequence, "audit")
			return ToolGuardResult{Decision: ToolGuardAbstain}, nil
		})},
	}}}})
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
	original := ToolCall{SessionID: "session", RunID: "run", Input: json.RawMessage(`{"op":"delete"}`), Scope: ToolScope{Permissions: []string{"write"}}}
	plan := mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{{Component: testPlanComponent("test-guards"), Guards: []PlanGuard{
		{Identity: testGuardIdentityAt("mutate", 0), Guard: ToolGuardFunc(func(_ context.Context, request ToolGuardRequest) (ToolGuardResult, error) {
			copy(request.Call.Input, json.RawMessage(`{"op":"hidden"}`))
			copy(request.Call.Scope.Permissions, []string{"audit"})
			return ToolGuardResult{Decision: ToolGuardAbstain}, nil
		})},
		{Identity: testGuardIdentityAt("deny-original", 1), Guard: ToolGuardFunc(func(_ context.Context, request ToolGuardRequest) (ToolGuardResult, error) {
			if string(request.Call.Input) != `{"op":"delete"}` || !reflect.DeepEqual(request.Call.Scope.Permissions, []string{"write"}) {
				return ToolGuardResult{}, errors.New("guard received mutated input")
			}
			return ToolGuardResult{Decision: ToolGuardDeny}, nil
		})},
	}}}})
	decision, err := evaluateToolGuards(context.Background(), plan, Tool{Name: "danger"}, original)
	if err != nil || decision.Decision != ToolGuardDeny {
		t.Fatalf("evaluateToolGuards = %#v, %v", decision, err)
	}
	if string(original.Input) != `{"op":"delete"}` || !reflect.DeepEqual(original.Scope.Permissions, []string{"write"}) {
		t.Fatalf("original call mutated = %#v", original)
	}
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
