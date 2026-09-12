package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/compose"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// noBaselineNoSealAgentFactory builds a real adk.TypedChatModelAgent from
// build.Model/build.Tools and DOES install build.Guard (so guardRan
// passes) -- but deliberately never installs build.DurableBaseline or
// build.SettlementSeal into its handler chain, the way DefaultChatModelAgentFactory
// does. It proves HA-S6 in the round-two W6 review: guardRan and
// dispatchCount>0 alone do not prove a factory wired the OTHER two
// mandatory AgentBuildContext handlers, since a factory can satisfy both
// of those while silently dropping DurableBaseline/SettlementSeal.
type noBaselineNoSealAgentFactory struct{}

func (noBaselineNoSealAgentFactory) BuildAgent(ctx context.Context, build AgentBuildContext) (adk.TypedAgent[*einoschema.AgenticMessage], error) {
	cfg := &adk.TypedChatModelAgentConfig[*einoschema.AgenticMessage]{
		Name: "agent", Model: build.Model, MaxIterations: build.MaxIterations,
		ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{Tools: build.Tools, ToolAliases: build.ToolAliases}},
	}
	if build.Guard != nil {
		cfg.Handlers = append(cfg.Handlers, build.Guard)
	}
	// Deliberately never appends build.DurableBaseline or build.SettlementSeal.
	return adk.NewTypedChatModelAgent[*einoschema.AgenticMessage](ctx, cfg)
}

// TestNonCompliantFactoryWithoutBaselineOrSealFailsAsConstructionError
// proves the engine-side enforcement added for HA-S6: a factory that
// installs Guard and dispatches through build.Model (passing guardRan and
// dispatchCount>0) but never installs DurableBaseline/SettlementSeal fails
// the turn as a construction error instead of silently completing a turn
// whose state was never seeded from durable history and never settlement-
// verified.
func TestNonCompliantFactoryWithoutBaselineOrSealFailsAsConstructionError(t *testing.T) {
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("should never settle durably")}, nil
	}))
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Agent: noBaselineNoSealAgentFactory{}})}

	handle, err := orch.Start(context.Background(), Request{SessionID: "no-baseline-no-seal-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunFailed {
		t.Fatalf("result = %+v, want failed (construction error)", result)
	}
	if !errors.Is(result.Error, ErrInvalidOrchestrator) {
		t.Fatalf("result.Error = %v, want ErrInvalidOrchestrator", result.Error)
	}
}
