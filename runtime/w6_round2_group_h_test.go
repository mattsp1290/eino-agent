package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-agent/extension"
)

// This file proves the W6 round-two reconciliation's group H fix (item 18,
// HA-S2 in the round-two W6 review): plan compilation rejects a toolsearch
// handler tool whose name collides with the native tool search name (or any
// plan tool or alias).

// TestToolSearchHandlerToolCollidesWithNativeToolSearchNameIsRejected is the
// literal named scenario: upstream's dynamictool/toolsearch middleware
// hardcodes its own contributed tool's name to "tool_search"
// (toolSearchToolName in eino's own package) -- the exact same default name
// normalizeToolSearchConfig gives the runtime's OWN native tool-search
// feature when Name is left unset. Mounting both in the same plan must be
// rejected at compile time, not silently let one shadow the other with no
// error anywhere either is actually dispatched.
func TestToolSearchHandlerToolCollidesWithNativeToolSearchNameIsRejected(t *testing.T) {
	handlerComponent := PlanComponent{
		Component: testPlanComponent("toolsearch-collision-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "toolsearch", Order: 0, Scope: extension.GlobalScope(),
			Kind: HandlerKindToolSearch, Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: NewToolSearchHandlerFactory(ToolSearchHandlerConfig{}),
		}},
	}
	_, err := NewRunPlan(RunPlanSpec{
		Components: []PlanComponent{handlerComponent},
		ToolSearch: &ToolSearchConfig{Name: "tool_search"},
	})
	if err == nil {
		t.Fatal("a toolsearch handler tool colliding with the native tool search name was accepted, want it rejected")
	}
	if !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("err = %v, want errors.Is(err, ErrExtensionPlanMismatch)", err)
	}
}

// TestToolSearchHandlerToolCollidesWithAPlanToolIsRejected proves the
// "any plan tool" half: a toolsearch handler tool colliding with an
// ordinary composition-registered tool's own canonical name must also be
// rejected, even with no native tool search configured at all.
func TestToolSearchHandlerToolCollidesWithAPlanToolIsRejected(t *testing.T) {
	collidingTool := Tool{Name: "tool_search", Info: nil, Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
		return ToolResult{Output: "n/a"}, nil
	})}
	handlerComponent := PlanComponent{
		Component: testPlanComponent("toolsearch-plan-tool-collision-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "toolsearch", Order: 0, Scope: extension.GlobalScope(),
			Kind: HandlerKindToolSearch, Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: NewToolSearchHandlerFactory(ToolSearchHandlerConfig{}),
		}},
		Tools: testPlanTools(staticToolRegistry{tools: []Tool{collidingTool}}),
	}
	_, err := NewRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})
	if err == nil {
		t.Fatal("a toolsearch handler tool colliding with a plan tool name was accepted, want it rejected")
	}
	if !errors.Is(err, ErrExtensionPlanMismatch) {
		t.Fatalf("err = %v, want errors.Is(err, ErrExtensionPlanMismatch)", err)
	}
}

// TestToolSearchHandlerToolNoCollisionSucceeds is the positive control: a
// toolsearch handler tool under a NON-colliding native tool-search name
// (or none at all) constructs the plan normally.
func TestToolSearchHandlerToolNoCollisionSucceeds(t *testing.T) {
	handlerComponent := PlanComponent{
		Component: testPlanComponent("toolsearch-no-collision-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "toolsearch", Order: 0, Scope: extension.GlobalScope(),
			Kind: HandlerKindToolSearch, Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: NewToolSearchHandlerFactory(ToolSearchHandlerConfig{}),
		}},
	}
	if _, err := NewRunPlan(RunPlanSpec{
		Components: []PlanComponent{handlerComponent},
		ToolSearch: &ToolSearchConfig{Name: "native_search_distinct_name"},
	}); err != nil {
		t.Fatalf("NewRunPlan with a non-colliding native tool search name = %v, want nil", err)
	}
}
