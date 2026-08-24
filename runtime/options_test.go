package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func TestNewStreamingOrchestratorMinimalAndOrderedOptions(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	ids := &sequenceIDs{}
	orch, err := NewStreamingOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{}),
		WithIDGenerator(ids),
		WithAttempts(2),
		WithAttempts(4),
		WithOwnerID("first"),
		WithOwnerID("last"),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator error = %v", err)
	}
	if orch.Store != store || orch.IDs != ids || orch.Attempts != 4 || orch.OwnerID != "last" {
		t.Fatalf("orchestrator options not applied: %+v", orch)
	}
	if orch.toolTurns() != 8 || orch.lease() != time.Minute || orch.ownerID() != "last" {
		t.Fatalf("zero-value fallbacks changed")
	}
}

func TestNewStreamingOrchestratorReportsEveryMissingDependency(t *testing.T) {
	t.Parallel()
	_, err := NewStreamingOrchestrator()
	if !errors.Is(err, ErrInvalidOrchestrator) {
		t.Fatalf("error = %v", err)
	}
	for _, name := range []string{"Store", "Model", "IDs"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %s", err, name)
		}
	}
}

func TestNewStreamingOrchestratorRejectsNilInterfaceOptions(t *testing.T) {
	t.Parallel()
	var typedNilStore *admissionStore
	tests := map[string]Option{
		"Store":           WithStore(nil),
		"TypedNilStore":   WithStore(typedNilStore),
		"Transactor":      WithTransactor(nil),
		"ModelResolver":   WithModelResolver(nil),
		"RunPlanProvider": WithRunPlanProvider(nil),
		"EventSink":       WithEventSink(nil),
		"Permissions":     WithPermissions(nil),
		"IDGenerator":     WithIDGenerator(nil),
	}
	for name, option := range tests {
		name, option := name, option
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := NewStreamingOrchestrator(option)
			if !errors.Is(err, ErrInvalidOrchestrator) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestFunctionAdaptersParticipateInOrchestratorOptions(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	var toolsResolved, eventEmitted bool
	toolPlan := mustTestRunPlan(RunPlanSpec{Tools: []PlanTool{{
		Identity: testToolIdentity("probe"),
		Resolve: func(context.Context, ToolScopeContext) (Tool, error) {
			toolsResolved = true
			return Tool{Name: "probe", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{}, nil })}, nil
		},
	}}})
	orch, err := NewStreamingOrchestrator(
		WithStore(store),
		WithModelResolver(model.ResolverFunc(func(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
			return resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
				return []*einoschema.Message{einoschema.AssistantMessage("ok", nil)}, nil
			})}.Resolve(context.Background(), model.Selection{}, model.Runtime{})
		})),
		WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(staticRunPlanProvider{plan: toolPlan}),
		WithEventSink(EventSinkFunc(func(context.Context, Event) error {
			eventEmitted = true
			return nil
		})),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator error = %v", err)
	}
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted || !toolsResolved || !eventEmitted {
		t.Fatalf("result = %+v; tools=%v event=%v", result, toolsResolved, eventEmitted)
	}
}
