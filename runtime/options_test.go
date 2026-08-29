package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

func TestNewStreamingOrchestratorMinimalAndOrderedOptions(t *testing.T) {
	t.Parallel()
	store := newAdmissionStore()
	ids := &sequenceIDs{}
	orch, err := NewStreamingOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{}),
		WithIDGenerator(ids),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
		WithAttempts(2),
		WithAttempts(4),
		WithOwnerID("first"),
		WithOwnerID("last"),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator error = %v", err)
	}
	if orch.store != store || orch.ids != ids || orch.attempts() != 4 || orch.ownerID() != "last" {
		t.Fatalf("orchestrator options not applied: %+v", orch)
	}
	if orch.toolTurns() != 8 || orch.lease() != time.Minute || orch.queueSize != 64 || orch.modelRequestMaxBytes != defaultModelRequestMaxBytes {
		t.Fatalf("constructor defaults changed")
	}
}

func TestNewStreamingOrchestratorRejectsInvalidExplicitBounds(t *testing.T) {
	t.Parallel()
	tests := map[string]Option{
		"Clock":                WithClock(nil),
		"OwnerID":              WithOwnerID(""),
		"Attempts":             WithAttempts(0),
		"ToolTurns":            WithToolTurns(0),
		"QueueSize":            WithQueueSize(0),
		"Lease":                WithLease(0),
		"ModelRequestMaxBytes": WithModelRequestMaxBytes(0),
	}
	for name, option := range tests {
		name, option := name, option
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := NewStreamingOrchestrator(
				WithStore(newAdmissionStore()),
				WithModelResolver(resolvedModel{}),
				WithIDGenerator(&sequenceIDs{}),
				WithRunPlanProvider(emptyTestRunPlanProvider()),
				option,
			)
			if !errors.Is(err, ErrInvalidOrchestrator) {
				t.Fatalf("error = %v, want ErrInvalidOrchestrator", err)
			}
		})
	}
}

func TestNewStreamingOrchestratorCopiesMutableOptionsAndExportsNoConfigurationFields(t *testing.T) {
	t.Parallel()
	attributes := map[string]string{"source": "original"}
	epoch := session.ContextEpoch{ID: "epoch-original"}
	safeOptions := []string{"temperature"}
	orch, err := NewStreamingOrchestrator(
		WithStore(newAdmissionStore()),
		WithModelResolver(resolvedModel{}),
		WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(emptyTestRunPlanProvider()),
		WithTrace(agentcontext.TraceContext{Attributes: attributes}),
		WithHistory(history.Options{Epoch: &epoch}),
		WithModelRequestSafeOptions(safeOptions...),
	)
	if err != nil {
		t.Fatal(err)
	}
	attributes["source"] = "mutated"
	epoch.ID = "epoch-mutated"
	safeOptions[0] = "secret"
	if orch.trace.Attributes["source"] != "original" || orch.history.Epoch.ID != "epoch-original" || orch.modelRequestSafeOptions[0] != "temperature" {
		t.Fatalf("mutable option alias retained: trace=%v history=%v safe=%v", orch.trace, orch.history, orch.modelRequestSafeOptions)
	}
	typeOf := reflect.TypeOf(StreamingOrchestrator{})
	for index := 0; index < typeOf.NumField(); index++ {
		if field := typeOf.Field(index); field.IsExported() {
			t.Errorf("StreamingOrchestrator exports configuration field %s", field.Name)
		}
	}
}

func TestNewStreamingOrchestratorReportsEveryMissingDependency(t *testing.T) {
	t.Parallel()
	_, err := NewStreamingOrchestrator()
	if !errors.Is(err, ErrInvalidOrchestrator) {
		t.Fatalf("error = %v", err)
	}
	for _, name := range []string{"Store", "Model", "IDs", "RunPlanProvider"} {
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
	var toolsResolved bool
	var eventEmitted atomic.Bool
	probe := testPlanTool("probe")
	probe.Resolve = func(context.Context, ToolScopeContext) (Tool, error) {
		toolsResolved = true
		return Tool{Name: "probe", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{}, nil })}, nil
	}
	toolPlan := mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{{Component: testPlanComponent("test-tools"), Tools: []PlanTool{probe}}}})
	orch, err := NewStreamingOrchestrator(
		WithStore(store),
		WithModelResolver(model.ResolverFunc(func(context.Context, model.Selection, model.Runtime) (model.Resolved, error) {
			return resolvedModel{streamer: scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
				return []*einoschema.Message{einoschema.AssistantMessage("ok", nil)}, nil
			})}.Resolve(context.Background(), model.Selection{}, model.Runtime{})
		})),
		WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(staticRunPlanProvider{plan: toolPlan}),
		WithEventSink(EventSinkFunc(func(context.Context, session.EventRecord) {
			eventEmitted.Store(true)
		})),
	)
	if err != nil {
		t.Fatalf("NewStreamingOrchestrator error = %v", err)
	}
	result := startAndWait(t, orch)
	deadline := time.Now().Add(time.Second)
	for !eventEmitted.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if result.Status != session.RunCompleted || !toolsResolved || !eventEmitted.Load() {
		t.Fatalf("result = %+v; tools=%v event=%v", result, toolsResolved, eventEmitted.Load())
	}
}
