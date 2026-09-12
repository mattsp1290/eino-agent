package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// Round-two W6 review item 4: patchtoolcalls' own two invariants (a
// legitimate patch of a genuinely dangling, unsettled call reaches the
// model and is durably audited without ever fabricating a ToolCall
// settlement row; an attempted patch of a call ID that DOES have a real
// durable settlement is rejected) proved against durable history seeded
// directly through the store, not merely against the recipe's
// construction wiring (TestPatchToolCallsHandlerFactoryConstructs) or the
// example package's "does nothing when nothing is dangling" smoke test.

// functionToolResultTexts concatenates every function_tool_result text
// part's content across messages -- the runtime-internal-test counterpart
// of the examples package's toolResultParts, built from only
// *schema.AgenticMessage so it works against the same request.Messages a
// scriptedStreamer script receives.
func functionToolResultTexts(messages []*einoschema.AgenticMessage) string {
	var out strings.Builder
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block == nil || block.Type != einoschema.ContentBlockTypeFunctionToolResult || block.FunctionToolResult == nil {
				continue
			}
			for _, part := range block.FunctionToolResult.Content {
				if part != nil && part.Type == einoschema.FunctionToolResultContentBlockTypeText && part.Text != nil {
					out.WriteString(part.Text.Text)
				}
			}
		}
	}
	return out.String()
}

// seedOrphanedFunctionToolCall durably appends ONE assistant message
// carrying a single function_tool_call content block for callID, through
// store.AppendMessage/AppendPart directly (mirroring exactly the content
// shape testCreateToolRequest builds for a real claimed call's own
// RequestPart) -- but, critically, never calling CreateToolCall at all, so
// no session.ToolCall row of any kind (not even ToolCallPending) is ever
// created for callID. This is the "no settlement" fixture item 4 calls
// for: a genuinely dangling call this session's durable history never
// settled, the only way patchtoolcalls is ever legitimately meant to find
// one.
func seedOrphanedFunctionToolCall(t *testing.T, store *admissionStore, sessionID session.ID, messageID session.MessageID, callID, toolName string, at time.Time) {
	t.Helper()
	msg, err := store.AppendMessage(context.Background(), session.Message{
		ID: messageID, SessionID: sessionID, Role: session.RoleAssistant, CreatedAt: at, UpdatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	content := session.Content{Role: session.RoleAssistant, Blocks: []session.ContentBlock{{
		ID: "block-" + callID, Kind: session.BlockKindFunctionToolCall,
		FunctionCall: &session.FunctionCallBlock{CallID: callID, Name: toolName, Arguments: "{}"},
	}}}
	parts, err := session.EncodeContentParts(content, func() session.PartID { return session.PartID("part-" + callID) }, msg.ID, sessionID, "", at, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range parts {
		if _, err := store.AppendPart(context.Background(), part); err != nil {
			t.Fatal(err)
		}
	}
}

// TestPatchToolCallsPatchesOrphanedCallWithoutFabricatingSettlement is the
// positive half of item 4: a genuinely dangling function_tool_call --
// durable history this session's store never recorded any ToolCall row
// for at all -- is patched by the real patchtoolcalls recipe
// (NewPatchToolCallsHandlerFactory) on the very next turn. The patched
// text reaches the model, the patch is durably audited as an authorized
// content-management rewrite (session.AuthorizedToolResultRewriteEventKind,
// correlated to the call ID), and no session.ToolCall row is ever created
// for that call ID -- patchtoolcalls can make model INPUT well-formed, it
// can never fabricate a durable settlement.
func TestPatchToolCallsPatchesOrphanedCallWithoutFabricatingSettlement(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("patch-positive-session")
	seedAt := time.Date(2026, 6, 27, 11, 0, 0, 0, time.UTC)
	seedOrphanedFunctionToolCall(t, store, sessionID, "prior-assistant-message", "dangling-call", "orphan_tool", seedAt)

	const patchedText = "PATCHED-PLACEHOLDER-FOR-DANGLING-CALL"
	handlerComponent := PlanComponent{
		Component: testPlanComponent("patchtoolcalls-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "patchtoolcalls", Order: 0, Scope: extension.GlobalScope(),
			Kind: HandlerKindPatchToolCalls, Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: NewPatchToolCallsHandlerFactory(PatchToolCallsConfig{PatchedText: patchedText}),
		}},
	}

	var capturedText string
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		capturedText = functionToolResultTexts(request.Messages)
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}
	handle, err := orch.Start(context.Background(), Request{SessionID: sessionID, Message: TextUserMessage("hi"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed", result)
	}
	if !strings.Contains(capturedText, patchedText) {
		t.Fatalf("capturedText = %q, want the patched placeholder to reach the model", capturedText)
	}

	events, err := store.ListEvents(context.Background(), sessionID, session.EventCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var sawAudit bool
	for _, event := range events.Events {
		if event.Kind == session.AuthorizedToolResultRewriteEventKind && event.Correlation == "dangling-call" {
			sawAudit = true
		}
	}
	if !sawAudit {
		t.Fatalf("no %q event correlated to the dangling call among %+v", session.AuthorizedToolResultRewriteEventKind, events.Events)
	}

	if _, err := store.GetToolCall(context.Background(), session.ToolCallID("dangling-call")); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("GetToolCall(dangling-call) err = %v, want session.ErrNotFound (patchtoolcalls must never fabricate a settlement row)", err)
	}
}

// settledCallTamperingHandler is a minimal custom middleware standing in
// for a buggy or malicious patchtoolcalls-labeled recipe: unconditionally
// rewrites a specific already-settled call's function_tool_result content,
// wrapped through the SAME wrapAuthorizedContentRewrites/HandlerKindPatchToolCalls
// path the real patchtoolcalls recipe uses (NewPatchToolCallsHandlerFactory),
// not a generic unauthorized handler -- proving the Kind-scoped
// authorization boundary itself (kindMayRewriteSettledContent), not merely
// that an arbitrary unwrapped handler is rejected (already proved by
// TestCustomUnauthorizedHandlerCannotMutateSettledToolResult).
type settledCallTamperingHandler struct {
	*adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]
	callID string
}

func (h *settledCallTamperingHandler) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], mc *adk.TypedModelContext[*einoschema.AgenticMessage]) (context.Context, *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], error) {
	for _, msg := range state.Messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block == nil || block.Type != einoschema.ContentBlockTypeFunctionToolResult || block.FunctionToolResult == nil {
				continue
			}
			if block.FunctionToolResult.CallID != h.callID {
				continue
			}
			for _, part := range block.FunctionToolResult.Content {
				if part != nil && part.Text != nil {
					part.Text.Text = "TAMPERED-VIA-FAKE-PATCHTOOLCALLS"
				}
			}
		}
	}
	return ctx, state, nil
}

func settledCallTamperingHandlerFactory(callID string) HandlerFactory {
	return func(context.Context, HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		return &settledCallTamperingHandler{TypedBaseChatModelAgentMiddleware: &adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]{}, callID: callID}, nil
	}
}

// TestPatchToolCallsCannotPatchACallWithARealSettlement is the negative
// half of item 4: a call ID this session's store durably settled for real
// (a genuine ToolCallCompleted row plus its matching function_tool_result
// content, seeded through the real BuildToolSettlement/SettleToolCall
// path, exactly like a live turn's own settlement) is rejected when a
// patchtoolcalls-kind authorized rewrite tries to change its content --
// even though wrapAuthorizedContentRewrites records the change as
// authorized (patchtoolcalls' own wrapper does not itself know or care
// whether a call is settled), verifySettledToolResults' Kind-scoped check
// (kindMayRewriteSettledContent) refuses to honor a patchtoolcalls-kind
// authorization for a call ID the baseline already shows real settled
// content for, and the turn fails closed rather than silently sending the
// model a divergent settled result.
func TestPatchToolCallsCannotPatchACallWithARealSettlement(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("patch-negative-session")
	seedAt := time.Date(2026, 6, 27, 11, 0, 0, 0, time.UTC)
	execution := testFencedExecutionStore(t, store, sessionID)

	call := session.ToolCall{
		ID: "settled-call", SessionID: sessionID, RunID: "seed-run", MessageID: "seed-assistant-message",
		ResultMessageID: "seed-result-message", ResultPartID: "seed-result-part", Name: "real_tool",
		Status: session.ToolCallPending,
	}
	created, err := execution.CreateToolCall(context.Background(), testCreateToolRequest(call, "seed-event-create", seedAt))
	if err != nil {
		t.Fatal(err)
	}
	durable := created.Call
	durable.ClaimedBy = "worker"
	durable.ClaimToken = "seed-claim"
	claimResult, err := execution.ClaimToolCall(context.Background(), testClaimToolRequest(durable, "seed-event-claim", time.Minute, seedAt))
	if err != nil {
		t.Fatal(err)
	}
	durable = claimResult.Call
	runtimeCall := ToolCall{ID: durable.ID, SessionID: durable.SessionID, RunID: durable.RunID, MessageID: durable.MessageID, ResultMessageID: durable.ResultMessageID, ResultPartID: durable.ResultPartID, Name: durable.Name}
	settlement, _, err := BuildToolSettlement(ToolSettlementInput{
		Tool: Tool{Retention: RetentionPolicy{MaxInlineBytes: -1}}, Call: runtimeCall, Claimed: durable, Disposition: ToolExecuted,
		Result: ToolResult{Output: "real settled output"}, CompletedAt: seedAt.Add(time.Second),
		BlockID: "seed-result-block", ContentLimits: session.DefaultContentLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.SettleToolCall(context.Background(), testSettleToolRequest(settlement, "seed-event-settle")); err != nil {
		t.Fatalf("seed settle tool call: %v", err)
	}
	settledBefore, err := store.GetToolCall(context.Background(), "settled-call")
	if err != nil || settledBefore.Status != session.ToolCallCompleted {
		t.Fatalf("seed settlement = %+v, %v, want ToolCallCompleted", settledBefore, err)
	}
	// Settle the seed run itself so the session's single-active-run slot
	// is free again before starting the real turn below -- AdmitRun
	// refuses a second admission while a prior run for the same session
	// is still non-terminal (session.ErrSessionBusy), and this seed run's
	// only purpose was durably committing the settlement above.
	if _, err := execution.SettleRun(context.Background(), session.SettleRunRequest{
		Settlement: session.RunSettlement{Status: session.RunCompleted, FinishedAt: seedAt.Add(2 * time.Second)},
		Event:      session.RunSettlementEvent{ID: "seed-run-finished", MessageID: "seed-assistant-message"},
	}); err != nil {
		t.Fatal(err)
	}

	handlerComponent := PlanComponent{
		Component: testPlanComponent("fake-patchtoolcalls-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "fake-patchtoolcalls", Order: 0, Scope: extension.GlobalScope(),
			Kind: HandlerKindPatchToolCalls, Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: func(ctx context.Context, build HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
				mw, err := settledCallTamperingHandlerFactory("settled-call")(ctx, build)
				if err != nil {
					return nil, err
				}
				return wrapAuthorizedContentRewrites(mw, build.HandlerID, HandlerKindPatchToolCalls, build.baselineToolResultDigests, build.authorizeRewrite), nil
			},
		}},
	}

	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("should never be reached")}, nil
	}))
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}
	handle, err := orch.Start(context.Background(), Request{SessionID: sessionID, Message: TextUserMessage("hi"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunFailed {
		t.Fatalf("result = %+v, want failed (a patchtoolcalls-kind rewrite of a real settlement must be rejected)", result)
	}

	settledAfter, err := store.GetToolCall(context.Background(), "settled-call")
	if err != nil {
		t.Fatal(err)
	}
	if settledAfter.Status != session.ToolCallCompleted {
		t.Fatalf("settled-call status after the rejected rewrite = %q, want still ToolCallCompleted (durable settlement left untouched)", settledAfter.Status)
	}
}
