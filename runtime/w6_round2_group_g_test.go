package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// TestTwoHandlersRewriteTwoDifferentResultsInOneTurnBothAuthorized is
// round-two W6 review item 15 ("ordering with two rewrites"): reduction and
// patchtoolcalls -- two DIFFERENT handlers -- each rewrite a DIFFERENT
// call's result in the SAME turn: reduction clears an older, real settled
// round (bigecho's call-1, once a second, more recent round exists);
// patchtoolcalls fills in a genuinely dangling call this session's history
// never settled at all (seeded directly, before the turn starts). Both
// rewrites must be authorized (the turn completes, neither is rejected by
// settlementSeal) and both must be durably audited as
// AuthorizedToolResultRewriteEventKind events, correctly attributed to
// their own handler ID -- in the SAME deterministic plan order every run
// (reduction registered at Order 0, patchtoolcalls at Order 1).
func TestTwoHandlersRewriteTwoDifferentResultsInOneTurnBothAuthorized(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("two-rewrites-session")
	seedAt := time.Date(2026, 6, 27, 11, 0, 0, 0, time.UTC)
	seedOrphanedFunctionToolCall(t, store, sessionID, "prior-assistant-message", "dangling-call", "orphan_tool", seedAt)

	const original = "this output is intentionally much longer than the configured truncation threshold so reduction clears it once a newer round exists"
	const patchedText = "PATCHED-DANGLING-CALL-PLACEHOLDER"
	echo := Tool{
		Name: "bigecho", Info: &einoschema.ToolInfo{Name: "bigecho", Desc: "returns a large payload"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: original}, nil
		}),
		Retention: RetentionPolicy{MaxInlineBytes: -1},
	}

	var calls int
	var thirdDispatchByCallID map[string]string
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		switch calls {
		case 1:
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-1", "bigecho", `{}`)}, nil
		case 2:
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-2", "bigecho", `{}`)}, nil
		default:
			thirdDispatchByCallID = functionToolResultTextByCallID(request.Messages)
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	}))
	handlerComponent := PlanComponent{
		Component: testPlanComponent("two-rewrites-component"),
		AgentHandlers: []PlanAgentHandler{
			{
				ID: "reduction", Order: 0, Scope: extension.GlobalScope(),
				Kind: HandlerKindReduction, Version: HandlerVersion1, ConfigHash: "test-hash-reduction",
				Factory: NewReductionHandlerFactory(ReductionConfig{MaxTokensForClear: 1}),
			},
			{
				ID: "patchtoolcalls", Order: 1, Scope: extension.GlobalScope(),
				Kind: HandlerKindPatchToolCalls, Version: HandlerVersion1, ConfigHash: "test-hash-patch",
				Factory: NewPatchToolCallsHandlerFactory(PatchToolCallsConfig{PatchedText: patchedText}),
			},
		},
		Tools: testPlanTools(staticToolRegistry{tools: []Tool{echo}}),
	}
	root := t.TempDir()
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}
	cfg := orchestratorConfig()
	cfg.Metadata = map[string]string{"workspace_id": "w", "workspace_root": root}
	handle, err := orch.Start(context.Background(), Request{SessionID: sessionID, Message: TextUserMessage("hi"), Config: cfg})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed (both rewrites should be authorized, not rejected)", result)
	}

	if len(thirdDispatchByCallID) != 3 {
		t.Fatalf("third dispatch tool results by call ID = %#v, want call-1, call-2, and dangling-call", thirdDispatchByCallID)
	}
	if strings.Contains(thirdDispatchByCallID["call-1"], original) {
		t.Fatalf("call-1 content = %q, want it cleared by reduction once call-2 (a more recent round) exists", thirdDispatchByCallID["call-1"])
	}
	if !strings.Contains(thirdDispatchByCallID["call-2"], original) {
		t.Fatalf("call-2 content = %q, want the most recent round retained in full (ClearRetentionSuffixLimit protects it)", thirdDispatchByCallID["call-2"])
	}
	if !strings.Contains(thirdDispatchByCallID["dangling-call"], patchedText) {
		t.Fatalf("dangling-call content = %q, want patchtoolcalls' own placeholder", thirdDispatchByCallID["dangling-call"])
	}

	events, err := store.ListEvents(context.Background(), sessionID, session.EventCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var sawReductionRewrite, sawPatchRewrite bool
	for _, event := range events.Events {
		if event.Kind != session.AuthorizedToolResultRewriteEventKind {
			continue
		}
		// Correlation is the DURABLE call id (reduction's own rewrite
		// correlates to whichever durable id call-1 was minted as, not the
		// provider-facing "call-1" string itself); handler_id in the
		// payload is the stable, origin-identifying field instead.
		var payload struct {
			HandlerID string `json:"handler_id"`
			CallID    string `json:"call_id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatalf("decode AuthorizedToolResultRewriteEventKind payload: %v", err)
		}
		switch payload.HandlerID {
		case "reduction":
			sawReductionRewrite = true
		case "patchtoolcalls":
			if payload.CallID != "dangling-call" {
				t.Fatalf("patchtoolcalls rewrite call_id = %q, want dangling-call", payload.CallID)
			}
			sawPatchRewrite = true
		}
	}
	if !sawReductionRewrite {
		t.Fatalf("no AuthorizedToolResultRewriteEventKind event with handler_id \"reduction\" among %+v", events.Events)
	}
	if !sawPatchRewrite {
		t.Fatalf("no AuthorizedToolResultRewriteEventKind event with handler_id \"patchtoolcalls\" among %+v", events.Events)
	}

	if _, err := store.GetToolCall(context.Background(), session.ToolCallID("dangling-call")); err == nil {
		t.Fatal("patchtoolcalls fabricated a durable settlement row for the dangling call -- it must only patch model input, never durable state")
	}
}
