package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// This file proves the W6 round-two reconciliation's group A (Authority)
// fixes: DA I1 (rewrite authority bound to durable facts), I2 (tool_search_result
// sealing), I3 (duplicate occurrence rejection), and I7 (capabilities never
// ride on the shared HandlerBuildContext for an arbitrary/foreign-Kind
// factory).

// TestVerifySettledToolResultsRejectsReductionAuthorizedFabrication is I1's
// core bug, proved directly against verifySettledToolResults (the real
// dispatch-time authority): a reduction-kind authorization for a call ID
// with NO durable baseline occurrence at all (a fabricated call, never
// settled) must never let that call's content reach the wire, even though
// the authorized digest matches exactly what was recorded -- reduction may
// only ever rewrite a call the baseline already shows settled
// (authorityBindsToBaseline).
func TestVerifySettledToolResultsRejectsReductionAuthorizedFabrication(t *testing.T) {
	msg := toolResultBlockMessage("fabricated-call", "tool")
	digest := canonicalFunctionToolResultContent(msg.ContentBlocks[0].FunctionToolResult.Content)

	authorized := newAuthorizedRewriteSet()
	authorized.record("some-handler", HandlerKindReduction, "fabricated-call", "", digest)

	var baseline []*einoschema.AgenticMessage // no durable settlement at all
	input := []*einoschema.AgenticMessage{msg}

	err := verifySettledToolResults(baseline, input, authorized)
	if !errors.Is(err, errUnauthorizedFabricatedToolResult) {
		t.Fatalf("verifySettledToolResults err = %v, want errUnauthorizedFabricatedToolResult (a reduction-kind authorization must never cover a call with no baseline settlement)", err)
	}
}

// TestVerifySettledToolResultsRejectsPatchToolCallsAuthorizedSettledRewrite
// is I1's mirror case, proved directly: a patchtoolcalls-kind authorization
// for a call ID the baseline ALREADY shows settled must never be honored --
// patchtoolcalls may only ever fill a genuinely dangling call.
func TestVerifySettledToolResultsRejectsPatchToolCallsAuthorizedSettledRewrite(t *testing.T) {
	settled := toolResultBlockMessage("settled-call", "tool")
	tampered := toolResultBlockMessage("settled-call", "tool")
	tampered.ContentBlocks[0].FunctionToolResult.Content[0].Text.Text = "TAMPERED"
	digest := canonicalFunctionToolResultContent(tampered.ContentBlocks[0].FunctionToolResult.Content)

	authorized := newAuthorizedRewriteSet()
	authorized.record("some-handler", HandlerKindPatchToolCalls, "settled-call", "", digest)

	baseline := []*einoschema.AgenticMessage{settled}
	input := []*einoschema.AgenticMessage{tampered}

	err := verifySettledToolResults(baseline, input, authorized)
	if !errors.Is(err, errSettledToolResultDiverged) {
		t.Fatalf("verifySettledToolResults err = %v, want errSettledToolResultDiverged (a patchtoolcalls-kind authorization must never cover an already-settled call)", err)
	}
}

// TestVerifySettledToolResultsSealsToolSearchResultBlocks is I2: a
// tool_search_result block was previously invisible to verifySettledToolResults
// entirely. A fabricated one (no baseline occurrence) must fail closed, and
// no recipe is ever authorized to rewrite one (the authorized set is
// consulted but can never cover this block kind).
func TestVerifySettledToolResultsSealsToolSearchResultBlocks(t *testing.T) {
	authorized := newAuthorizedRewriteSet()
	// Even a (nonsensical, but hypothetically attempted) authorization for
	// this call ID must not help: no kind ever authorizes a
	// tool_search_result rewrite.
	authorized.record("some-handler", HandlerKindReduction, "search-call", "", "whatever")

	var baseline []*einoschema.AgenticMessage
	input := []*einoschema.AgenticMessage{toolSearchResultBlockMessage("search-call")}

	err := verifySettledToolResults(baseline, input, authorized)
	if !errors.Is(err, errUnauthorizedFabricatedToolResult) {
		t.Fatalf("verifySettledToolResults err = %v, want errUnauthorizedFabricatedToolResult (a fabricated tool_search_result must fail closed)", err)
	}

	// A genuine, settled tool_search_result (present identically in
	// baseline) must still pass.
	settled := toolSearchResultBlockMessage("settled-search-call")
	if err := verifySettledToolResults([]*einoschema.AgenticMessage{settled}, []*einoschema.AgenticMessage{settled}, nil); err != nil {
		t.Fatalf("verifySettledToolResults on a genuinely settled tool_search_result = %v, want nil", err)
	}
}

// TestVerifySettledToolResultsRejectsDuplicateSettledOccurrence is I3: the
// baseline has exactly ONE settled occurrence for a call ID; two IDENTICAL
// copies of that occurrence in input must not both pass merely because each
// individually matches baseline -- the occurrence count itself must never
// exceed the baseline's own count.
func TestVerifySettledToolResultsRejectsDuplicateSettledOccurrence(t *testing.T) {
	settled := toolResultBlockMessage("dup-call", "tool")
	baseline := []*einoschema.AgenticMessage{settled}
	// Two identical occurrences reaching the model in one request.
	input := []*einoschema.AgenticMessage{settled, settled}

	err := verifySettledToolResults(baseline, input, nil)
	if !errors.Is(err, errSettledToolResultDiverged) {
		t.Fatalf("verifySettledToolResults err = %v, want errSettledToolResultDiverged (a duplicated settled occurrence must fail closed)", err)
	}

	// The same message ONCE must still pass (sanity: the fix must not
	// reject the ordinary case).
	if err := verifySettledToolResults(baseline, []*einoschema.AgenticMessage{settled}, nil); err != nil {
		t.Fatalf("verifySettledToolResults on a single genuine occurrence = %v, want nil", err)
	}
}

// TestPublicizeToolCallIDsFallbackRejectsNonPatchToolCallsAuthorization is
// I1's third clause: publicizeToolCallIDs' "authorized dangling call"
// fallback (used when store.GetToolCall returns session.ErrNotFound) must
// accept only a patchtoolcalls-kind authorization -- a reduction-kind
// authorization for a call ID with no durable row at all must still fail
// closed, never reach the wire under its durable id.
func TestPublicizeToolCallIDsFallbackRejectsNonPatchToolCallsAuthorization(t *testing.T) {
	store := newAdmissionStore()
	// No ToolCall row for "ghost-call" at all.
	authorized := newAuthorizedRewriteSet()
	authorized.record("some-handler", HandlerKindReduction, "ghost-call", "", "digest")

	messages := []*einoschema.AgenticMessage{toolResultBlockMessage("ghost-call", "tool")}
	_, err := publicizeToolCallIDs(context.Background(), store, "session-1", nil, authorized, messages)
	if !errors.Is(err, errToolCallIDUnresolved) {
		t.Fatalf("publicizeToolCallIDs err = %v, want errToolCallIDUnresolved (a reduction-kind authorization must not satisfy the dangling-call fallback)", err)
	}

	// The mirror positive case: a patchtoolcalls-kind authorization for the
	// same shape of call DOES satisfy the fallback (the durable id goes on
	// the wire, matching an empty ProviderCallID).
	authorizedPatch := newAuthorizedRewriteSet()
	authorizedPatch.record("some-handler", HandlerKindPatchToolCalls, "ghost-call", "", "digest")
	out, err := publicizeToolCallIDs(context.Background(), store, "session-1", nil, authorizedPatch, messages)
	if err != nil {
		t.Fatalf("publicizeToolCallIDs (patchtoolcalls-authorized) err = %v, want nil", err)
	}
	if out[0].ContentBlocks[0].FunctionToolResult.CallID != "ghost-call" {
		t.Fatalf("resolved call id = %q, want ghost-call (durable id fallback)", out[0].ContentBlocks[0].FunctionToolResult.CallID)
	}
}

// TestPatchToolCallsWrappedUnderForeignKindGetsNoRewriteAuthorization is I7:
// the REAL runtime.NewPatchToolCallsHandlerFactory constructor, registered
// under a Kind OTHER than HandlerKindPatchToolCalls (simulating a host that
// mislabels -- deliberately or not -- the plan entry), must receive no
// rewrite-authorization capability at all: adkEngine.buildAgentHandlers
// gates authorizeRewrite/baselineToolResultDigests on the entry's OWN
// declared Kind, not on which constructor produced the factory. Patching a
// genuinely dangling call under the wrong Kind must therefore fail the turn
// closed instead of silently succeeding the way
// TestPatchToolCallsPatchesOrphanedCallWithoutFabricatingSettlement's
// correctly-Kinded registration does.
func TestPatchToolCallsWrappedUnderForeignKindGetsNoRewriteAuthorization(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("patch-foreign-kind-session")
	seedAt := time.Date(2026, 6, 27, 11, 0, 0, 0, time.UTC)
	seedOrphanedFunctionToolCall(t, store, sessionID, "prior-assistant-message", "dangling-call", "orphan_tool", seedAt)

	handlerComponent := PlanComponent{
		Component: testPlanComponent("patchtoolcalls-foreign-kind-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "patchtoolcalls", Order: 0, Scope: extension.GlobalScope(),
			// Deliberately NOT HandlerKindPatchToolCalls, even though the
			// factory itself is the real constructor.
			Kind: "example.not-patchtoolcalls", Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: NewPatchToolCallsHandlerFactory(PatchToolCallsConfig{PatchedText: "PATCHED"}),
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
		t.Fatalf("result = %+v, want failed (patchtoolcalls registered under a foreign Kind must get no rewrite authorization)", result)
	}
}

// TestSummarizationWrappedUnderForeignKindFailsConstructionClosed is I7's
// summarization case: the REAL runtime.NewSummarizationHandlerFactory
// constructor, registered under a Kind other than HandlerKindSummarization,
// must receive no epoch capability -- its own precondition check
// (build.epochs.ready()) then fails construction closed, so it can never
// reach a point where it might call the model at all under the wrong
// authority.
func TestSummarizationWrappedUnderForeignKindFailsConstructionClosed(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("summarization-foreign-kind-session")

	handlerComponent := PlanComponent{
		Component: testPlanComponent("summarization-foreign-kind-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "summarization", Order: 0, Scope: extension.GlobalScope(),
			Kind: "example.not-summarization", Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: NewSummarizationHandlerFactory(SummarizationConfig{TriggerContextMessages: 1}),
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
		t.Fatalf("result = %+v, want failed (summarization registered under a foreign Kind must fail construction closed, never dispatch)", result)
	}
	epochs, err := store.ListContextEpochs(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	for _, epoch := range epochs {
		if epoch.Trigger == "summarization" {
			t.Fatalf("found a summarization ContextEpoch %+v, want none (construction must have failed before ever generating a summary)", epoch)
		}
	}
}

// arbitraryModelProbeHandler is a fully custom HandlerFactory (not one of
// this package's own eight recipes) standing in for a third-party host's
// own middleware. It records the exact *adkModel value it was handed via
// HandlerBuildContext.Model so the test can assert it is a bounded
// internal-dispatch adapter tagged with this handler's own HandlerID --
// never the turn's real, placeholder-claiming conversational adapter.
type arbitraryModelProbeHandler struct {
	*adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]
}

func arbitraryModelProbeFactory(capturedInternalDispatch *string, capturedHasRewrite, capturedHasEpochs *bool) HandlerFactory {
	return func(_ context.Context, build HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		if m, ok := build.Model.(*adkModel); ok {
			*capturedInternalDispatch = m.internalDispatch
		}
		*capturedHasRewrite = build.authorizeRewrite != nil
		*capturedHasEpochs = build.epochs.ready()
		return &arbitraryModelProbeHandler{TypedBaseChatModelAgentMiddleware: &adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]{}}, nil
	}
}

// TestArbitraryHandlerFactoryNeverReceivesTurnsRealModelAdapter is I7's
// general case, for a factory that is not any of this package's own
// recipes at all: HandlerBuildContext.Model must always be a bounded
// internal-dispatch adapter tagged with the entry's own HandlerID (agent
// path), and authorizeRewrite/epochs must be absent, regardless of what
// Kind the plan declares for it.
func TestArbitraryHandlerFactoryNeverReceivesTurnsRealModelAdapter(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("arbitrary-handler-session")

	var capturedInternalDispatch string
	var capturedHasRewrite, capturedHasEpochs bool
	handlerComponent := PlanComponent{
		Component: testPlanComponent("arbitrary-handler-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "my-custom-handler", Order: 0, Scope: extension.GlobalScope(),
			Kind: "example.custom", Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: arbitraryModelProbeFactory(&capturedInternalDispatch, &capturedHasRewrite, &capturedHasEpochs),
		}},
	}

	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("ok")}, nil
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
	if capturedInternalDispatch != "my-custom-handler" {
		t.Fatalf("HandlerBuildContext.Model.internalDispatch = %q, want %q (the entry's own HandlerID, never the turn's real adapter)", capturedInternalDispatch, "my-custom-handler")
	}
	if capturedHasRewrite {
		t.Fatalf("HandlerBuildContext.authorizeRewrite was populated for an arbitrary Kind, want nil")
	}
	if capturedHasEpochs {
		t.Fatalf("HandlerBuildContext.epochs was ready() for an arbitrary Kind, want not-ready")
	}
}
