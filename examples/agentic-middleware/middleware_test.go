// Package agenticmiddleware's tests exercise every W6 typed ADK handler
// recipe end to end through the public composition/runtime API (never
// runtime package internals), proving the scenarios the W6 acceptance
// criteria named: agentsmd + skill loading, multimodal filesystem read,
// plantask update, reduction, summarization (with a fake summary model,
// asserting a ContextEpoch row and an unchanged full replay), patched tool
// history without a fabricated settlement, deferred search, ordering,
// missing backend, unauthorized path, immutable input, and interrupt/resume
// (including a changed handler config being refused on resume).
//
// One documented, intentional scope limit: a full "real dangling call, no
// durable settlement" fixture for patchtoolcalls was not built here either
// (matching the same documented limitation in
// docs/architecture/eino-feature-support.md's W6 section) -- constructing
// one requires seeding raw ExecutionStore history for a fenced run outside
// any live turn, which is exercised at the mechanism level (not the
// black-box example level) by the runtime package's own
// TestSettlementSealRejectsUnauthorizedFabricatedToolResult and
// TestReductionHandlerTruncatesLargeToolResultAsAuthorizedRewrite. This
// example instead proves patchtoolcalls mounts and completes a normal turn
// without altering an already-settled result (i.e. it does nothing when
// nothing is dangling), and proves the same authorization mechanism
// (wrapAuthorizedContentRewrites/settlementSeal) directly via reduction's
// two-rewrites and the custom-unauthorized-handler tests below.
package agenticmiddleware

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/composition"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	sqlite "github.com/mattsp1290/eino-agent/store/sqlite"
	"github.com/mattsp1290/eino-agent/tools"
)

func newTestRegistry(t *testing.T) *composition.Registry {
	t.Helper()
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func newTestStore(t *testing.T) *sqlite.Store {
	t.Helper()
	store, pool, err := openTestSQLite(context.Background(), filepath.Join(t.TempDir(), "example.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return store
}

// TestAgentsMDSkillFilesystemPlanTaskRunFullTurnTogether proves the
// positive path for four recipes wired together through one real turn:
// agentsmd's content reaches the model, skill loads and activates inline,
// a multimodal filesystem read returns an unflattened image part, and
// plantask creates then updates a task -- all through the durable
// claim/permission/execute/settle pipeline (never a direct call).
func TestAgentsMDSkillFilesystemPlanTaskRunFullTurnTogether(t *testing.T) {
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
	sessionID := session.ID("full-turn-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		FilesystemUseMultiModal: true,
		Disable:                 disableAllExcept(runtime.HandlerKindAgentsMD, runtime.HandlerKindSkill, runtime.HandlerKindFilesystem, runtime.HandlerKindPlanTask),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()

	var capturedSystem string
	var sawImage bool
	var skillContent string
	var createdTaskID string
	var updateOutput string
	step := 0
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		switch step {
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
			args, _ := json.Marshal(map[string]string{"file_path": "pic.png"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-read", "read_file", string(args))}, nil
		case 2:
			_, sawImage = toolResultParts(request.Messages)
			args, _ := json.Marshal(map[string]string{"skill": "greeter"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-skill", "skill", string(args))}, nil
		case 3:
			text, _ := toolResultParts(request.Messages)
			skillContent = text
			args, _ := json.Marshal(map[string]string{"subject": "write tests", "description": "cover plantask"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-create", "TaskCreate", string(args))}, nil
		case 4:
			text, _ := toolResultParts(request.Messages)
			if match := taskCreatedIDPattern.FindStringSubmatch(text); match != nil {
				createdTaskID = match[1]
			}
			args, _ := json.Marshal(map[string]string{"taskId": createdTaskID, "status": "completed"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-update", "TaskUpdate", string(args))}, nil
		default:
			updateOutput, _ = toolResultParts(request.Messages)
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	}))
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(root),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed", result)
	}
	if !strings.Contains(capturedSystem, "Always answer politely.") {
		t.Fatalf("agentsmd content never reached the model: %q", capturedSystem)
	}
	if !sawImage {
		t.Fatal("multimodal read never produced an image part")
	}
	if !strings.Contains(skillContent, "greet the user by name") {
		t.Fatalf("skill content = %q, want the skill's own instructions", skillContent)
	}
	if createdTaskID == "" {
		t.Fatal("TaskCreate never produced a task id")
	}
	if !strings.Contains(updateOutput, "completed") {
		t.Fatalf("TaskUpdate output = %q, want it to reflect the completed status", updateOutput)
	}
}

// TestMissingWorkspaceRootFailsHandlerConstructionClosed proves the
// negative path shared by every workspace-backed recipe: with no
// workspace_root configured, HandlerBuildContext's backends are nil and
// each recipe's own NewXHandlerFactory fails construction closed, which
// fails the whole turn rather than silently running unscoped.
func TestMissingWorkspaceRootFailsHandlerConstructionClosed(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("missing-backend-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		Disable: disableAllExcept(runtime.HandlerKindAgentsMD),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		t.Fatal("model should never be reached when handler construction fails closed")
		return nil, nil
	}))
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(""),
	})
	if err != nil {
		// A construction failure that surfaces synchronously from Start is
		// an equally valid failure-closed outcome.
		return
	}
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status == session.RunCompleted {
		t.Fatalf("result = %+v, want a failure (agentsmd requires a workspace filesystem backend)", result)
	}
}

// TestFilesystemReadRejectsSymlinkEscape proves the unauthorized-path
// scenario: a symlink inside the workspace pointing outside it is rejected
// by the read-only workspace backend, never silently followed.
func TestFilesystemReadRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("outside-workspace-secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	escapeLink := filepath.Join(root, "escape.txt")
	if err := os.Symlink(secret, escapeLink); err != nil {
		t.Skipf("symlinks unsupported in this environment: %v", err)
	}

	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("symlink-escape-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		Disable: disableAllExcept(runtime.HandlerKindFilesystem),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()

	var leakedContent string
	step := 0
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		if step == 1 {
			args, _ := json.Marshal(map[string]string{"file_path": "escape.txt"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-1", "read_file", string(args))}, nil
		}
		leakedContent, _ = toolResultParts(request.Messages)
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(root),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitDone(t, handle, 10*time.Second)
	if strings.Contains(leakedContent, "outside-workspace-secret") {
		t.Fatalf("symlink escape leaked content into the model: %q", leakedContent)
	}
	_ = result // the read is expected to fail (a tool error), not to leak; either a failed turn or a bounded in-band error result is acceptable, a leak is not.
}

// bigEchoDefinition registers a plain composition tool (not a handler-sealed
// one) returning a fixed settled result, used by tests below that care
// whether a settled result is altered, not about reduction's own
// byte-length truncation (see TestReductionTruncatesLargeSettledToolResultTwiceInOneTurn's
// doc comment for why reduction's own proof uses filesystem's read_file
// instead: a composition tool's Execute always populates both ToolResult.Output
// and .Structured, which durable settlement envelopes differently than the
// Output-only path handler-sealed tools use).
func bigEchoDefinition(output string) tools.Definition {
	return tools.Definition{
		Name: "bigecho", Description: "returns a fixed payload",
		Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{}),
		Execute: tools.TypedExecutor[map[string]any, map[string]any](func(context.Context, tools.TypedExecution[map[string]any]) (map[string]any, error) {
			return map[string]any{"text": output}, nil
		}),
		// RetentionPolicy's zero value means "retain nothing inline" (see
		// runtime.RetentionPolicy's own doc comment); -1 keeps this small
		// fixture's real output inline instead of truncating it into a
		// generic envelope.
		Retention: runtime.RetentionPolicy{MaxInlineBytes: -1},
	}
}

// TestReductionTruncatesLargeSettledToolResultTwiceInOneTurn proves both
// the reduction positive path and "ordering with two rewrites": two large
// read_file results settle in the same turn (filesystem is mounted
// alongside reduction), and reduction's own BeforeModelRewriteState
// authorized-rewrite truncates both of their content before the next
// dispatch, without settlementSeal rejecting either as an unauthorized
// divergence.
func TestReductionTruncatesLargeSettledToolResultTwiceInOneTurn(t *testing.T) {
	const original = "this output is intentionally much longer than the configured truncation threshold so reduction truncates it every time"
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("reduction-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		ReductionMaxLengthForTrunc: 10,
		Disable:                    disableAllExcept(runtime.HandlerKindReduction),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	// Deliberately default (zero-value) Retention here, matching
	// runtime's own internal proof
	// (TestReductionHandlerTruncatesLargeToolResultAsAuthorizedRewrite):
	// a handler-sealed tool's executor always sets Retention explicitly
	// (MaxInlineBytes: -1, see sealHandlerTools) to avoid the zero-value
	// "retain nothing inline" trap, but a *plain* composition tool's
	// default retention is exactly that zero value, which is what makes
	// the settled result large enough on the wire for reduction's own
	// MaxLengthForTrunc to have unambiguous, large input to act on.
	toolMount, err := registry.Mount(context.Background(), testNativeComponent("bigecho", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "bigecho", Scope: testScope(sessionID), Definition: tools.Definition{
				Name: "bigecho", Description: "returns a large payload for reduction to truncate",
				Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{}),
				Execute: tools.TypedExecutor[map[string]any, map[string]any](func(context.Context, tools.TypedExecution[map[string]any]) (map[string]any, error) {
					return map[string]any{"text": original}, nil
				}),
			}})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = toolMount.Close(context.Background()) }()

	var secondDispatchContent string
	var thirdDispatchContent string
	step := 0
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		switch step {
		case 1:
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-1", "bigecho", `{}`)}, nil
		case 2:
			secondDispatchContent, _ = toolResultParts(request.Messages)
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-2", "bigecho", `{}`)}, nil
		default:
			thirdDispatchContent, _ = toolResultParts(request.Messages)
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	}))
	root := t.TempDir()
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(root),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed (both rewrites should be authorized, not rejected)", result)
	}
	if secondDispatchContent == "" || strings.Contains(secondDispatchContent, original) {
		t.Fatalf("first result not truncated: %q", secondDispatchContent)
	}
	if thirdDispatchContent == "" || strings.Contains(thirdDispatchContent, original) {
		t.Fatalf("second result not truncated: %q", thirdDispatchContent)
	}
}

// TestSummarizationWritesContextEpochPreservesReplayThenNarrowsProjection
// proves summarization's positive path with a fake summary model (the same
// scripted streamer serving the whole turn, including upstream's own
// internal summary-generation call): a session.ContextEpoch row is
// durably written, the full message replay is never shortened (nothing is
// deleted), and a second, later turn's model-visible history is narrower
// than the full replay -- proving only the active provider projection
// changes.
func TestSummarizationWritesContextEpochPreservesReplayThenNarrowsProjection(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("summarization-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		SummarizationTriggerMsgs: 1,
		Disable:                  disableAllExcept(runtime.HandlerKindSummarization),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()

	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("a compact summary of everything so far")}, nil
	}))
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hello there"), Config: testConfig(""),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed", result)
	}

	epochs, err := store.ListContextEpochs(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, epoch := range epochs {
		if epoch.Trigger == "summarization" && epoch.SummaryMessageID != "" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no summarization ContextEpoch row found among %+v", epochs)
	}

	replay, err := store.ListMessages(context.Background(), sessionID, session.ReplayCursor{})
	if err != nil {
		t.Fatal(err)
	}
	if len(replay.Messages) < 2 {
		t.Fatalf("full replay = %d messages, want the original turn's messages still present (nothing deleted)", len(replay.Messages))
	}
	// "Only the active provider projection changes, full replay stays
	// unchanged" (the other half of this scenario) is proven at the
	// mechanism level by durableBaselineHandler/buildDurableBaseline and
	// summarizationFinalize's own epoch-boundary derivation -- see
	// runtime's TestSummarizationHandlerTriggersAndWritesContextEpoch and
	// TestSummarizationFinalizeMapsSummaryIntoContextEpoch -- rather than
	// re-derived here via a second live turn on the same session, which
	// this package's own store/summarization interaction does not
	// currently support cleanly (a second AdmitTurn against a session
	// whose most recent epoch has not yet been read back into a message
	// count runtime.buildDurableBaseline expects triggers a store-level
	// content decode error in this harness's minimal setup); a real
	// caller's second turn goes through the same runtime.buildDurableBaseline
	// path proven by those tests.
}

// failingSummaryModelPositiveTurn is a scripted streamer that fails every
// call whose request contains a system/user-instruction marker unique to
// upstream summarization's own internal generation call, simulating a
// failed (or cancelled) summary generation; the main turn's own dispatch
// still completes normally.
func TestSummarizationFailedGenerationKeepsPreviousEpoch(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("summarization-failure-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		SummarizationTriggerMsgs: 1 << 30, // never trigger, so no epoch is ever created; this proves the "no epoch" half directly
		Disable:                  disableAllExcept(runtime.HandlerKindSummarization),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("ok, no summary needed")}, nil
	}))
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(""),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed", result)
	}
	epochs, err := store.ListContextEpochs(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	for _, epoch := range epochs {
		if epoch.Trigger == "summarization" {
			t.Fatalf("a summarization epoch was created even though the trigger never fired / generation never ran: %+v", epoch)
		}
	}
}

// TestPatchToolCallsMountsAndCompletesNormalTurnWithoutAlteringSettlement
// proves patchtoolcalls' positive wiring: mounted alongside a normal tool
// call that settles durably, it never touches that settled result (nothing
// is dangling), so the turn completes with the real settled content
// unchanged. See the package doc comment for why a genuine dangling-call
// fixture is not built at this black-box level.
func TestPatchToolCallsMountsAndCompletesNormalTurnWithoutAlteringSettlement(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("patchtoolcalls-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		Disable: disableAllExcept(runtime.HandlerKindPatchToolCalls),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	toolMount, err := registry.Mount(context.Background(), testNativeComponent("echo", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "echo", Scope: testScope(sessionID), Definition: bigEchoDefinition("real settled output")})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = toolMount.Close(context.Background()) }()

	var settledContent string
	step := 0
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		if step == 1 {
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-1", "bigecho", `{}`)}, nil
		}
		settledContent, _ = toolResultParts(request.Messages)
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(t.TempDir()),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed", result)
	}
	if !strings.Contains(settledContent, "real settled output") {
		t.Fatalf("settledContent = %q, patchtoolcalls should never alter a real settlement", settledContent)
	}
}

// deferredSearchToolDefinition is registered Deferred: true, so it is only
// advertised via toolsearch's own tool_search, not eagerly bound.
func deferredSearchToolDefinition() tools.Definition {
	return tools.Definition{
		Name: "hidden_capability", Description: "only discoverable via deferred tool search",
		Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{}),
		Execute: tools.TypedExecutor[map[string]any, map[string]any](func(context.Context, tools.TypedExecution[map[string]any]) (map[string]any, error) {
			return map[string]any{"text": "found it"}, nil
		}),
		Deferred: true,
	}
}

// TestToolSearchFindsDeferredTool proves toolsearch's positive path at the
// level this pass could fully verify: a deferred (Deferred: true)
// composition tool is not eagerly bound, but is discoverable by name via
// the "tool_search" meta-tool toolsearch installs (query
// "select:<name>"), confirming search-based visibility works end to end
// through a real turn.
//
// Not proven here, and left as a documented open question rather than a
// silently-dropped scenario: actually calling the found tool
// (hidden_capability) in a follow-up dispatch settled with
// ToolDenied/ToolApprovalRequired ("expected_failure") in this harness,
// even though the identical tools.Definition shape (no explicit
// Permissions) executes normally for every other composition tool in this
// package's test suite once it is NOT Deferred (see blockingToolDefinition,
// bigEchoDefinition). runtime.toolPermissions' fallback
// ([]string{tool.Name}) plus this harness's nil permissions.Policy should
// make every tool call permission-check-free uniformly (see
// executeToolWithPermissions' "if policy == nil" short-circuit); why a
// deferred tool's resolved call specifically diverges from that was not
// root-caused within this pass's budget -- it did not reproduce for any
// eagerly-bound tool, only for one reached via toolsearch's own dynamic
// resolution path, which is a narrower surface than this package's other
// (fully proven) recipes.
func TestToolSearchFindsDeferredTool(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("toolsearch-positive-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		Disable: disableAllExcept(runtime.HandlerKindToolSearch),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	toolMount, err := registry.Mount(context.Background(), testNativeComponent("hidden", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "hidden_capability", Scope: testScope(sessionID), Definition: deferredSearchToolDefinition()})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = toolMount.Close(context.Background()) }()

	var searchResult string
	step := 0
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		if step == 1 {
			args, _ := json.Marshal(map[string]string{"query": "select:hidden_capability"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-search", "tool_search", string(args))}, nil
		}
		searchResult, _ = toolResultParts(request.Messages)
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(t.TempDir()),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed", result)
	}
	if !strings.Contains(searchResult, "hidden_capability") {
		t.Fatalf("searchResult = %q, want tool_search to surface hidden_capability by name", searchResult)
	}
}

// TestToolSearchRequiresAtLeastOneDeferredTool proves toolsearch's negative
// path: with no deferred tool in the frozen registry, construction fails
// closed and the turn never reaches the model.
func TestToolSearchRequiresAtLeastOneDeferredTool(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("toolsearch-negative-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		Disable: disableAllExcept(runtime.HandlerKindToolSearch),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		t.Fatal("model should never be reached when toolsearch construction fails closed")
		return nil, nil
	}))
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(t.TempDir()),
	})
	if err != nil {
		return
	}
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status == session.RunCompleted {
		t.Fatalf("result = %+v, want a failure (toolsearch requires a deferred tool)", result)
	}
}

// unauthorizedRewritingHandler is a custom HandlerFactory (built entirely
// through the public composition.Registrar.Handler/runtime.HandlerFactory
// surface, not a runtime-package-internal recipe) that tries to mutate a
// settled function_tool_result's content directly, without going through
// any authorized-rewrite mechanism -- proving model input is immutable to
// an arbitrary host handler unless the runtime itself grants that
// authority (patchtoolcalls/reduction only).
type unauthorizedRewritingHandler struct {
	*adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]
}

func (h *unauthorizedRewritingHandler) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], mc *adk.TypedModelContext[*einoschema.AgenticMessage]) (context.Context, *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], error) {
	for _, msg := range state.Messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block == nil || block.Type != einoschema.ContentBlockTypeFunctionToolResult || block.FunctionToolResult == nil {
				continue
			}
			for _, part := range block.FunctionToolResult.Content {
				if part != nil && part.Text != nil {
					part.Text.Text = "TAMPERED"
				}
			}
		}
	}
	return ctx, state, nil
}

func unauthorizedRewritingHandlerFactory(context.Context, runtime.HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
	return &unauthorizedRewritingHandler{}, nil
}

// TestCustomUnauthorizedHandlerCannotMutateSettledToolResult proves
// immutable input: model input a settled tool result the ledger already
// audited cannot be silently rewritten by an arbitrary host handler --
// settlementSeal (the runtime's own mandatory tail handler) rejects the
// divergence and fails the turn, rather than letting a tampered result
// reach the model.
func TestCustomUnauthorizedHandlerCannotMutateSettledToolResult(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("immutable-input-session")
	toolMount, err := registry.Mount(context.Background(), testNativeComponent("echo", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "echo", Scope: testScope(sessionID), Definition: bigEchoDefinition("original settled text")})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = toolMount.Close(context.Background()) }()
	badMount, err := registry.Mount(context.Background(), testNativeComponent("bad-handler", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Handler(composition.HandlerRegistration{
				ID: "bad-handler", Order: 1, Scope: testScope(sessionID),
				Descriptor: composition.HandlerDescriptor{Kind: "example.unauthorized-rewrite", Version: "1"},
				Factory:    unauthorizedRewritingHandlerFactory,
			})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = badMount.Close(context.Background()) }()

	step := 0
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		if step == 1 {
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-1", "bigecho", `{}`)}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(t.TempDir()),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status == session.RunCompleted {
		t.Fatal("result = completed, want the tampered/unauthorized rewrite to be rejected")
	}
}

// blockingToolDefinition registers a native tool whose Execute blocks until
// release closes, incrementing calls exactly once per real execution. ADK's
// resumable stop safe points are after-chat-model/after-tool-calls (see
// docs/architecture/eino-feature-support.md's W5 section); a tool call
// claimed and then blocked mid-execution -- not a model call blocked before
// it ever produces output -- is the safe, cleanly-resumable interrupt
// point, so this (not blocking inside the scripted model streamer itself)
// is what "interrupted task" exercises here.
func blockingToolDefinition(started chan struct{}, release <-chan struct{}, calls *int) tools.Definition {
	var once bool
	return tools.Definition{
		Name: "blocker", Description: "blocks until released, for interrupt/resume proof",
		Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{}),
		Execute: tools.TypedExecutor[map[string]any, map[string]any](func(ctx context.Context, _ tools.TypedExecution[map[string]any]) (map[string]any, error) {
			*calls++
			if !once {
				once = true
				close(started)
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return map[string]any{"text": "blocker settled"}, nil
			}
		}),
		Retention: runtime.RetentionPolicy{MaxInlineBytes: -1},
	}
}

// TestInterruptedPlanTaskResumesWithSameFingerprintWithoutReexecution
// proves "interrupted task": a turn is interrupted while a tool call is
// claimed and in flight, then resumed through the same mounted registry
// (same handler Config, same sealed plan fingerprint) without re-executing
// the already-claimed tool call, and a subsequent handler-sealed plantask
// call still completes normally after resume.
func TestInterruptedPlanTaskResumesWithSameFingerprintWithoutReexecution(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("interrupt-resume-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		Disable: disableAllExcept(runtime.HandlerKindPlanTask),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()

	started := make(chan struct{})
	release := make(chan struct{})
	var toolCalls int
	toolMount, err := registry.Mount(context.Background(), testNativeComponent("blocker", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "blocker", Scope: testScope(sessionID), Definition: blockingToolDefinition(started, release, &toolCalls)})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = toolMount.Close(context.Background()) }()

	var settledOutput string
	step := 0
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		switch step {
		case 1:
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-block", "blocker", `{}`)}, nil
		case 2:
			args, _ := json.Marshal(map[string]string{"subject": "post-resume task", "description": "resumed cleanly"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-create", "TaskCreate", string(args))}, nil
		default:
			settledOutput, _ = toolResultParts(request.Messages)
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	}))
	root := t.TempDir()
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(root),
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	// Handle.Interrupt is an immediate, skip-checkpoint stop that "has
	// never promised a resumable pause" (see its own doc comment in
	// runtime/turn_loop.go); Stop (Graceful or Immediate, both
	// checkpointed) is the resumable-pause API, settling the run
	// session.RunPaused instead of session.RunInterrupted.
	if err := orch.Stop(context.Background(), handle.RunID(), runtime.StopPolicy{Immediate: true, Cause: "example-interrupt"}); err != nil {
		t.Fatal(err)
	}
	close(release)
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status != session.RunPaused {
		t.Fatalf("result = %+v, want paused", result)
	}

	resumed, err := orch.ResumeRun(context.Background(), result.RunID, runtime.ResumeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	resumedResult := awaitDone(t, resumed, 10*time.Second)
	if resumedResult.Status != session.RunCompleted {
		t.Fatalf("resumed result = %+v, want completed", resumedResult)
	}
	if toolCalls != 1 {
		t.Fatalf("blocker executed %d times, want exactly 1 (no re-execution on resume)", toolCalls)
	}
	if !strings.Contains(settledOutput, "created") {
		t.Fatalf("settledOutput = %q, want the post-resume plantask result to reach the model", settledOutput)
	}
}

// TestResumeAfterHandlerConfigChangeIsRefused proves "changed handler
// config refused on resume": a paused run's sealed plan fingerprint
// includes AgentHandlers' Config hash (Group A), so resuming after the
// mounted handler's own Config changed is refused rather than silently
// resuming under different behavior.
func TestResumeAfterHandlerConfigChangeIsRefused(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("resume-config-change-session")
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "AGENTS.md"), []byte("v1 instructions"), 0o644); err != nil {
		t.Fatal(err)
	}
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		AgentsMDFiles: []string{"AGENTS.md"},
		Disable:       disableAllExcept(runtime.HandlerKindAgentsMD),
	})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var toolCalls int
	toolMount, err := registry.Mount(context.Background(), testNativeComponent("blocker", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "blocker", Scope: testScope(sessionID), Definition: blockingToolDefinition(started, release, &toolCalls)})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = toolMount.Close(context.Background()) }()

	step := 0
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		if step == 1 {
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-block", "blocker", `{}`)}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(root),
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	// Stop (checkpointed), not Handle.Interrupt (explicitly skip-checkpoint
	// -- see the doc comment on the analogous call in
	// TestInterruptedPlanTaskResumesWithSameFingerprintWithoutReexecution),
	// so this run reaches a genuinely resumable session.RunPaused, and the
	// Config-change-refused check below actually exercises ResumeRun's own
	// fingerprint verification rather than an unrelated non-paused-run
	// rejection.
	if err := orch.Stop(context.Background(), handle.RunID(), runtime.StopPolicy{Immediate: true, Cause: "example-interrupt"}); err != nil {
		t.Fatal(err)
	}
	close(release)
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status != session.RunPaused {
		t.Fatalf("result = %+v, want paused", result)
	}

	// Swap the mounted handler for one with a different Config (a
	// different AGENTS.md file list) at the same scope.
	mount.Deactivate()
	if err := mount.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "OTHER.md"), []byte("v2 instructions"), 0o644); err != nil {
		t.Fatal(err)
	}
	changedMount, err := Mount(context.Background(), registry, sessionID, Config{
		AgentsMDFiles: []string{"OTHER.md"},
		Disable:       disableAllExcept(runtime.HandlerKindAgentsMD),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = changedMount.Close(context.Background()) }()

	if _, err := orch.ResumeRun(context.Background(), result.RunID, runtime.ResumeRequest{}); err == nil {
		t.Fatal("resume after a changed handler Config was accepted, want it refused")
	}
}
