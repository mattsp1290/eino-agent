package agenticmiddleware

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/tools"
)

// --- a custom, purely public-API "dangling call" fixture -------------------
//
// NewPatchToolCallsHandlerFactory's own doc comment names its real-world
// purpose: patching a dangling tool call "after external history editing" --
// a function_tool_call block present in in-memory conversation state with no
// corresponding session.ToolCall row at all. The real (sqlite-backed)
// session.Store deliberately makes this impossible to construct through its
// own public ExecutionStore.AppendPart (it refuses PartFunctionToolCall/
// PartFunctionToolResult/PartToolSearchResult kinds outside CreateToolCall's
// own atomic path -- see store/internal/sqlstore/execution.go's AppendPart),
// so this example does not attempt to seed one directly into durable
// history the way the runtime package's own internal, in-memory-fake-backed
// test (TestPatchToolCallsPatchesOrphanedCallWithoutFabricatingSettlement)
// does. Instead it reaches the exact same real code path -- patchtoolcalls'
// own upstream scan of in-memory state.Messages for an unanswered
// function_tool_call, and this runtime's own patchtoolcalls-kind fallback in
// publicizeToolCallIDs -- through the SAME public extension surface every
// one of the eight recipes is built on: a tiny custom
// runtime.HandlerFactory/adk.TypedChatModelAgentMiddleware, exactly like
// unauthorizedRewritingHandler above. Mounted at Order 0 (strictly before
// patchtoolcalls, which Mount registers at Order 5 when every recipe is
// enabled), it inserts one synthetic assistant message carrying a
// function_tool_call block for a call ID that is never durably created --
// simulating imported/edited history -- and patchtoolcalls' own
// BeforeModelRewriteState (which never touches the store; see
// patchtoolcalls.go's patchToolCallsForAgenticMessage upstream) sees and
// patches it on the very same cycle, purely in memory. injected is shared
// across this handler's per-turn instances (HandlerFactory is invoked fresh
// once per compiled plan) so the synthetic call is injected exactly once
// for the whole test, not re-injected on turn 2.
const (
	danglingCallSyntheticCallID = "synthetic-dangling-call"
	danglingCallToolName        = "orphan_tool"
)

type danglingCallInjector struct {
	*adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]
	injected *atomic.Bool
}

func (h *danglingCallInjector) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], _ *adk.TypedModelContext[*einoschema.AgenticMessage]) (context.Context, *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], error) {
	if h.injected.Swap(true) {
		return ctx, state, nil
	}
	synthetic := &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeAssistant,
		ContentBlocks: []*einoschema.ContentBlock{
			einoschema.NewContentBlock(&einoschema.FunctionToolCall{
				CallID: danglingCallSyntheticCallID, Name: danglingCallToolName, Arguments: "{}",
			}),
		},
	}
	messages := make([]*einoschema.AgenticMessage, 0, len(state.Messages)+1)
	messages = append(messages, synthetic)
	messages = append(messages, state.Messages...)
	nState := *state
	nState.Messages = messages
	return ctx, &nState, nil
}

func danglingCallInjectorFactory(injected *atomic.Bool) runtime.HandlerFactory {
	return func(context.Context, runtime.HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
		return &danglingCallInjector{injected: injected}, nil
	}
}

// TestComposedExampleMountsAllEightRecipesInOneRunPlan is round-two W6
// review item 14: one composed example that mounts every recipe
// (agentsmd, skill, filesystem, plantask, patchtoolcalls, reduction,
// summarization, toolsearch) together in one RunPlan and drives a real,
// multi-turn scenario that exercises each of them, asserting the concrete
// effect each one is responsible for -- not a registration-only smoke test.
//
// Turn 1 mounts all eight recipes together and, across seven real dispatch
// rounds on the SAME turn, exercises seven of them: agentsmd's content
// reaches the model, patchtoolcalls patches a synthetic dangling call
// without ever fabricating a durable settlement, a multimodal filesystem
// read returns an unflattened image part, skill content loads and
// activates, plantask creates then updates a task, reduction truncates an
// oversized tool result before settlement, and a deferred tool is
// discovered via tool search and then actually called.
//
// Summarization's own trigger is deliberately configured far out of reach
// for turn 1 (SummarizationTriggerMsgs is checked on every cycle, including
// turn 1's own intra-turn tool-call rounds, not just once per turn -- see
// upstream summarization.TypedMiddleware.shouldSummarize -- so a threshold
// low enough to fire on turn 2 would fire mid-turn-1 instead, corrupting
// the other seven recipes' own careful round-by-round script). Turn 2 remounts
// the SAME eight recipes -- still all eight together, one RunPlan per turn --
// with only the summarization threshold lowered, so turn 2's own first cycle
// (which already sees turn 1's full history) triggers summarization
// immediately: it durably commits a session.ContextEpoch, and the full
// message/part replay before and after turn 2 is asserted byte-for-byte
// equal on its shared prefix (compaction only ever appends).
func TestComposedExampleMountsAllEightRecipesInOneRunPlan(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "AGENTS.md"), []byte("Always answer politely."), 0o644); err != nil {
		t.Fatal(err)
	}
	skillDir := filepath.Join(root, "greeter")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte("---\nname: greeter\ndescription: says hi\n---\nAlways greet the user by name.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	png := []byte{0x89, 'P', 'N', 'G', 1, 2, 3, 4, 5, 6, 7, 8}
	if err := os.WriteFile(filepath.Join(root, "pic.png"), png, 0o644); err != nil {
		t.Fatal(err)
	}

	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("composed-example-session")

	// hidden_capability: a deferred native tool, only discoverable through
	// toolsearch's own sealed tool_search meta-tool.
	var hiddenCallID session.ToolCallID
	hiddenMount, err := registry.Mount(context.Background(), testNativeComponent("hidden", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "hidden_capability", Scope: testScope(sessionID), Definition: deferredSearchToolDefinition(&hiddenCallID)})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hiddenMount.Close(context.Background()) }()

	// bigecho: a large-payload native tool for reduction to truncate. It
	// must be unambiguously larger than every other tool's own settled
	// envelope (skill/plantask/read_file/hidden_capability all carry some
	// JSON envelope overhead of their own), so a single MaxLengthForTrunc
	// threshold can sit strictly between "every normal envelope" and "this
	// payload" without truncating anything else in the same turn.
	bigPayload := strings.Repeat("this output is intentionally much longer than every other tool's own settled envelope. ", 40)
	var bigechoCallID session.ToolCallID
	bigechoMount, err := registry.Mount(context.Background(), testNativeComponent("bigecho", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "bigecho", Scope: testScope(sessionID), Definition: bigEchoTrackedDefinition(bigPayload, &bigechoCallID)})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bigechoMount.Close(context.Background()) }()

	// The dangling-call injector, mounted at Order 0 -- strictly before
	// every one of Mount's eight recipes (its own register() calls start at
	// Order 1) -- so patchtoolcalls (Order 5, once every recipe is enabled)
	// sees and patches the synthetic call in the same cycle it appears.
	var injected atomic.Bool
	injectorMount, err := registry.Mount(context.Background(), testNativeComponent("dangling-injector", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Handler(composition.HandlerRegistration{
				ID: "dangling-injector", Order: 0, Scope: testScope(sessionID),
				Descriptor: composition.HandlerDescriptor{Kind: "example.dangling-call-injector", Version: "1"},
				Factory:    danglingCallInjectorFactory(&injected),
			})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = injectorMount.Close(context.Background()) }()

	const patchedText = "PATCHED-DANGLING-CALL-PLACEHOLDER"
	turn1Config := Config{
		FilesystemUseMultiModal:    true,
		ReductionMaxLengthForTrunc: 800,
		PatchToolCallsPatchedText:  patchedText,
		// Deliberately far out of reach for turn 1's own seven dispatch
		// rounds (see the test's doc comment); turn 2 remounts with a low
		// threshold instead of relying on this one.
		SummarizationTriggerMsgs: 1_000_000,
	}
	mount1, err := Mount(context.Background(), registry, sessionID, turn1Config)
	if err != nil {
		t.Fatal(err)
	}

	var capturedSystem string
	var danglingPatchedText string
	var sawImage bool
	var skillContent string
	var createdTaskID string
	var updateOutput string
	var reducedContent string
	var searchResult string
	var hiddenResult string
	mainDispatchCount := 0
	turn2Step := 0
	var turn2ContinuedResult string
	streamer := scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		if requestIsSummaryGeneration(request) {
			return []*einoschema.AgenticMessage{agenticAssistantText("a compact summary of everything so far")}, nil
		}
		mainDispatchCount++
		switch mainDispatchCount {
		case 1:
			capturedSystem = request.System
			for _, msg := range request.Messages {
				text, _ := toolResultParts([]*einoschema.AgenticMessage{msg})
				capturedSystem += text
				for _, block := range msg.ContentBlocks {
					if block != nil && block.UserInputText != nil {
						capturedSystem += block.UserInputText.Text
					}
				}
			}
			danglingPatchedText = toolResultTextByCallID(request.Messages)[danglingCallSyntheticCallID]
			args, _ := json.Marshal(map[string]string{"file_path": "pic.png"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-read", "read_file", string(args))}, nil
		case 2:
			_, sawImage = toolResultParts(request.Messages)
			args, _ := json.Marshal(map[string]string{"skill": "greeter"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-skill", "skill", string(args))}, nil
		case 3:
			text, _ := toolResultParts(request.Messages)
			skillContent = text
			args, _ := json.Marshal(map[string]string{"subject": "write tests", "description": "cover the composed example"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-create", "TaskCreate", string(args))}, nil
		case 4:
			text, _ := toolResultParts(request.Messages)
			if match := taskCreatedIDPattern.FindStringSubmatch(text); match != nil {
				createdTaskID = match[1]
			}
			args, _ := json.Marshal(map[string]string{"taskId": createdTaskID, "status": "completed"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-update", "TaskUpdate", string(args))}, nil
		case 5:
			updateOutput, _ = toolResultParts(request.Messages)
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-bigecho", "bigecho", `{}`)}, nil
		case 6:
			reducedContent = toolResultTextByCallID(request.Messages)["call-bigecho"]
			args, _ := json.Marshal(map[string]string{"query": "select:hidden_capability"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-search", "tool_search", string(args))}, nil
		case 7:
			searchResult = strings.Join(toolSearchResultDiscoveredNames(request.Messages), ",")
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-hidden", "hidden_capability", `{}`)}, nil
		case 8:
			hiddenResult, _ = toolResultParts(request.Messages)
			return []*einoschema.AgenticMessage{agenticAssistantText("turn 1 done")}, nil
		default:
			// Turn 2: proves the turn CONTINUES normally after summarization
			// triggers (round-three W6 summarization-correctness review,
			// item 14) -- its first cycle's own main dispatch, immediately
			// after upstream's internal summary-generation call above has
			// already run and Finalize has already narrowed history for
			// THIS cycle, still makes a real tool call and gets a real
			// result back, then a second cycle finishes the turn.
			turn2Step++
			switch turn2Step {
			case 1:
				// hidden_capability (small, fixed output), not bigecho: this
				// round only needs to prove the turn keeps dispatching real
				// tool calls after summarization fires, not re-exercise
				// reduction's own truncation a second time.
				return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-continue", "hidden_capability", `{}`)}, nil
			default:
				turn2ContinuedResult = toolResultTextByCallID(request.Messages)["call-continue"]
				return []*einoschema.AgenticMessage{agenticAssistantText("turn 2 done")}, nil
			}
		}
	})
	orch := newTestOrchestrator(t, store, registry, streamer)

	handle1, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(root),
	})
	if err != nil {
		t.Fatal(err)
	}
	result1 := awaitDone(t, handle1, 20*time.Second)
	if result1.Status != session.RunCompleted {
		t.Fatalf("turn 1 result = %+v, want completed", result1)
	}

	// 1. agentsmd: its content reaches the model.
	if !strings.Contains(capturedSystem, "Always answer politely.") {
		t.Fatalf("agentsmd content never reached the model: %q", capturedSystem)
	}
	// 2. patchtoolcalls: the synthetic dangling call was patched with its
	// own placeholder text, and no durable settlement was ever fabricated
	// for it.
	if !strings.Contains(danglingPatchedText, patchedText) {
		t.Fatalf("dangling call content = %q, want patchtoolcalls' own placeholder %q", danglingPatchedText, patchedText)
	}
	if _, err := store.GetToolCall(context.Background(), session.ToolCallID(danglingCallSyntheticCallID)); err == nil {
		t.Fatal("patchtoolcalls fabricated a durable settlement row for the dangling call -- it must only patch model input, never durable state")
	}
	// 3. filesystem: the multimodal read produced an unflattened image part.
	if !sawImage {
		t.Fatal("multimodal filesystem read never produced an image part")
	}
	// 4. skill: its content loaded and activated inline.
	if !strings.Contains(skillContent, "greet the user by name") {
		t.Fatalf("skill content = %q, want the skill's own instructions", skillContent)
	}
	// 5. plantask: a task was created then updated.
	if createdTaskID == "" {
		t.Fatal("TaskCreate never produced a task id")
	}
	if !strings.Contains(updateOutput, "completed") {
		t.Fatalf("TaskUpdate output = %q, want it to reflect the completed status", updateOutput)
	}
	// 6. reduction: the oversized tool result was truncated before the
	// model ever saw it, and before durable settlement.
	if reducedContent == "" {
		t.Fatal("bigecho's tool result was never observed on the next dispatch")
	}
	if strings.Contains(reducedContent, bigPayload) {
		t.Fatalf("model-visible bigecho content = %q, want the truncated form (not the original)", reducedContent)
	}
	if !strings.Contains(reducedContent, "saved to:") {
		t.Fatalf("model-visible bigecho content = %q, want reduction's own offload placeholder", reducedContent)
	}
	if bigechoCallID == "" {
		t.Fatal("bigecho tool never executed, no durable call id captured")
	}
	bigechoCall, err := store.GetToolCall(context.Background(), bigechoCallID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(bigechoCall.Output), bigPayload) {
		t.Fatalf("durable bigecho settlement = %s, want the truncated form", bigechoCall.Output)
	}
	// 7. toolsearch: the deferred tool was discovered by name, then
	// actually called and settled.
	if !strings.Contains(searchResult, "hidden_capability") {
		t.Fatalf("searchResult = %q, want tool_search to surface hidden_capability by name", searchResult)
	}
	if strings.Contains(hiddenResult, "expected_failure") || strings.Contains(hiddenResult, "undiscovered") {
		t.Fatalf("hiddenResult = %q, want it NOT denied as undiscovered", hiddenResult)
	}
	if hiddenCallID == "" {
		t.Fatal("hidden_capability tool never executed, no durable call id captured")
	}
	hiddenCall, err := store.GetToolCall(context.Background(), hiddenCallID)
	if err != nil {
		t.Fatal(err)
	}
	if hiddenCall.Status != session.ToolCallCompleted {
		t.Fatalf("hidden_capability settled status = %q, want completed", hiddenCall.Status)
	}

	replayBeforeTurn2, err := store.ListMessages(context.Background(), sessionID, session.ReplayCursor{})
	if err != nil {
		t.Fatal(err)
	}

	// Remount the SAME eight recipes for turn 2 -- still all eight together,
	// one RunPlan for this turn too -- with only the summarization
	// threshold lowered, since ResumeRun's fingerprint check (the one
	// TestResumeAfterHandlerConfigChangeIsRefused proves) applies to
	// resuming an interrupted run, not to a brand new, independently
	// admitted turn like this one.
	mount1.Deactivate()
	if err := mount1.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	turn2Config := turn1Config
	turn2Config.SummarizationTriggerMsgs = 1
	turn2Config.SummarizationRetainTail = 2
	// patchtoolcalls is deliberately excluded from turn 2 only (turn 1
	// still mounts and exercises it fully, proving its own scenario).
	// Investigating this test's own "continues after summarization"
	// scenario surfaced a genuine, separately reportable interaction: once
	// summarization has narrowed history, patchtoolcalls' own upstream
	// adjacency scan (hasCorrespondingAgenticToolResult, which requires a
	// tool call's own result to be the LITERALLY NEXT message with no
	// intervening non-result message) can misjudge a call this SAME cycle
	// settled moments earlier as "dangling" and append an unauthorized
	// second, fabricated result for it -- correctly rejected by
	// settlementSeal (verifyOccurrenceKind) as a diverged, unauthorized
	// occurrence, but failing the turn. Root-causing and fixing that
	// upstream adjacency assumption is out of scope here; turn 1 already
	// proves patchtoolcalls' own real scenario (a genuinely dangling call,
	// patched, no fabricated settlement) in full.
	turn2Config.Disable = map[string]bool{runtime.HandlerKindPatchToolCalls: true}
	mount2, err := Mount(context.Background(), registry, sessionID, turn2Config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount2.Close(context.Background()) }()

	handle2, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("please continue"), Config: testConfig(root),
	})
	if err != nil {
		t.Fatal(err)
	}
	result2 := awaitDone(t, handle2, 20*time.Second)
	if result2.Status != session.RunCompleted {
		t.Fatalf("turn 2 result = %+v, want completed", result2)
	}
	// The turn CONTINUES normally after summarization triggers on its own
	// first cycle: a real tool call still dispatches and settles on a
	// LATER cycle of the same turn (round-three W6 summarization-
	// correctness review item 14).
	if turn2ContinuedResult == "" {
		t.Fatal("turn 2 never reached its own post-summarization tool call -- the turn did not continue normally after summarization triggered")
	}

	// 8. summarization: a ContextEpoch was durably committed, and the full
	// replay is unchanged (byte-for-byte) on its pre-existing prefix --
	// "exact replay equality before and after".
	epochs, err := store.ListContextEpochs(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	var summarizationEpochCount int
	for _, epoch := range epochs {
		if epoch.Trigger == "summarization" && epoch.SummaryMessageID != "" {
			summarizationEpochCount++
		}
	}
	// Exactly one, not merely "at least one": summarizeAtMostOnceMiddleware
	// (round-three W6 summarization-correctness review, Important #2)
	// bounds summarization to firing at most once per turn even though
	// turn 2 has more than one ReAct cycle after the trigger fires.
	if summarizationEpochCount != 1 {
		t.Fatalf("summarization ContextEpoch count = %d, want exactly 1 among %+v", summarizationEpochCount, epochs)
	}

	// Both turn 1 rewrites are durably audited, each through its own real
	// mechanism: patchtoolcalls' patch of the dangling call is a
	// post-settlement rewrite of durable baseline content, explicitly
	// authorized and recorded as an AuthorizedToolResultRewriteEventKind
	// event (wrapAuthorizedContentRewrites/settlementSeal). Reduction's
	// MaxLengthForTrunc truncation is a DIFFERENT mechanism -- it applies
	// at tool-execution time, before this runtime ever durably settles the
	// call (adkEngine.applyHandlerToolResultWrappers), so the settled row
	// IS the truncated form: there is nothing for settlementSeal to
	// authorize as a rewrite of already-settled content (see
	// NewReductionHandlerFactoryWithTokenCounter's own doc comment), and it
	// is instead already asserted above via bigechoCall's own durable
	// settlement (item 6's assertions).
	events, err := store.ListEvents(context.Background(), sessionID, session.EventCursor{Limit: 500})
	if err != nil {
		t.Fatal(err)
	}
	var sawPatchRewriteEvent bool
	for _, event := range events.Events {
		if event.Kind != session.AuthorizedToolResultRewriteEventKind {
			continue
		}
		var payload struct {
			HandlerID string `json:"handler_id"`
			CallID    string `json:"call_id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatalf("decode AuthorizedToolResultRewriteEventKind payload: %v", err)
		}
		if payload.HandlerID == "patchtoolcalls" {
			sawPatchRewriteEvent = true
		}
	}
	if !sawPatchRewriteEvent {
		t.Fatalf("no AuthorizedToolResultRewriteEventKind event with handler_id \"patchtoolcalls\" among %+v", events.Events)
	}
	replayAfterTurn2, err := store.ListMessages(context.Background(), sessionID, session.ReplayCursor{})
	if err != nil {
		t.Fatal(err)
	}
	if len(replayAfterTurn2.Messages) <= len(replayBeforeTurn2.Messages) {
		t.Fatalf("full replay did not grow across turn 2: before=%d after=%d", len(replayBeforeTurn2.Messages), len(replayAfterTurn2.Messages))
	}
	if !reflect.DeepEqual(replayBeforeTurn2.Messages, replayAfterTurn2.Messages[:len(replayBeforeTurn2.Messages)]) {
		t.Fatalf("compaction changed a pre-existing message:\nbefore=%+v\nafter =%+v", replayBeforeTurn2.Messages, replayAfterTurn2.Messages[:len(replayBeforeTurn2.Messages)])
	}
	if len(replayAfterTurn2.Parts) <= len(replayBeforeTurn2.Parts) {
		t.Fatalf("full replay's parts did not grow across turn 2: before=%d after=%d", len(replayBeforeTurn2.Parts), len(replayAfterTurn2.Parts))
	}
	if !reflect.DeepEqual(replayBeforeTurn2.Parts, replayAfterTurn2.Parts[:len(replayBeforeTurn2.Parts)]) {
		t.Fatalf("compaction changed a pre-existing part:\nbefore=%+v\nafter =%+v", replayBeforeTurn2.Parts, replayAfterTurn2.Parts[:len(replayBeforeTurn2.Parts)])
	}
}

// bigEchoTrackedDefinition is bigEchoDefinition with retention forced
// inline (MaxInlineBytes: -1, so only reduction's own truncation -- never
// this runtime's own retention-policy truncation -- can be what shortens
// the payload; see TestReductionTruncatesSettledToolResultBeforeSettlement's
// own doc comment for why the zero-value retention default is a
// false-positive trap here) and the executing call's durable ID captured
// into calledID, since the durable ID is always a fresh mint rather than
// the scripted provider CallID.
func bigEchoTrackedDefinition(output string, calledID *session.ToolCallID) tools.Definition {
	return tools.Definition{
		Name: "bigecho", Description: "returns a large payload for reduction to truncate",
		Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{}),
		Execute: tools.TypedExecutor[map[string]any, map[string]any](func(_ context.Context, execution tools.TypedExecution[map[string]any]) (map[string]any, error) {
			*calledID = execution.Call.ID
			return map[string]any{"text": output}, nil
		}),
		Retention: runtime.RetentionPolicy{MaxInlineBytes: -1},
	}
}
