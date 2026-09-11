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
	"errors"
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

// TestReductionTruncatesSettledToolResultBeforeSettlement proves item 2 of
// the round-two W6 review through the public API: reduction's
// MaxLengthForTrunc truncation now runs BEFORE this runtime durably settles
// the tool call (runtime.adkEngine.applyHandlerToolResultWrappers), so a
// single round is enough -- unlike clearing (see
// TestReductionClearsOlderRoundAsAuthorizedRewrite below), truncation is not
// gated by ClearRetentionSuffixLimit's recency protection.
func TestReductionTruncatesSettledToolResultBeforeSettlement(t *testing.T) {
	const original = "this output is intentionally much longer than the configured truncation threshold so reduction truncates it every time"
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("reduction-trunc-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		ReductionMaxLengthForTrunc: 10,
		Disable:                    disableAllExcept(runtime.HandlerKindReduction),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	// The scripted provider CallID ("call-1") is preserved separately as
	// ProviderCallID; the durable ID is always a fresh mint now, so capture
	// it from the tool's own execution context instead of assuming the
	// literal survives to the durable store.
	var bigechoCallID session.ToolCallID
	// Retention is explicit (MaxInlineBytes:-1), not the zero-value
	// default: the payload must be genuinely inline so only reduction's own
	// truncation -- not this runtime's own retention-policy truncation --
	// can be what shortens it (see the runtime-internal proof's control
	// test, TestReductionWithoutMountLeavesOriginalPayloadIntact, for why
	// the zero-value default is a false-positive trap here).
	toolMount, err := registry.Mount(context.Background(), testNativeComponent("bigecho", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "bigecho", Scope: testScope(sessionID), Definition: tools.Definition{
				Name: "bigecho", Description: "returns a large payload for reduction to truncate",
				Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{}),
				Execute: tools.TypedExecutor[map[string]any, map[string]any](func(_ context.Context, execution tools.TypedExecution[map[string]any]) (map[string]any, error) {
					bigechoCallID = execution.Call.ID
					return map[string]any{"text": original}, nil
				}),
				Retention: runtime.RetentionPolicy{MaxInlineBytes: -1},
			}})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = toolMount.Close(context.Background()) }()

	var secondDispatchContent string
	step := 0
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		if step == 1 {
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-1", "bigecho", `{}`)}, nil
		}
		secondDispatchContent, _ = toolResultParts(request.Messages)
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
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
		t.Fatalf("result = %+v, want completed", result)
	}
	if secondDispatchContent == "" {
		t.Fatal("second dispatch never saw the tool result")
	}
	if strings.Contains(secondDispatchContent, original) {
		t.Fatalf("model-visible content = %q, want the truncated form (not the original)", secondDispatchContent)
	}
	if !strings.Contains(secondDispatchContent, "saved to:") {
		t.Fatalf("model-visible content = %q, want reduction's own offload placeholder", secondDispatchContent)
	}
	// The durable settlement itself -- not just the model-visible copy --
	// and replay must both already be the truncated form.
	if bigechoCallID == "" {
		t.Fatal("bigecho tool never executed, no durable call id captured")
	}
	toolCall, err := store.GetToolCall(context.Background(), bigechoCallID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(toolCall.Output), original) {
		t.Fatalf("durable settlement = %s, want the truncated form", toolCall.Output)
	}
	replay, err := store.ListMessages(context.Background(), sessionID, session.ReplayCursor{})
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range replay.Parts {
		if part.Kind == session.PartFunctionToolResult && strings.Contains(string(part.Payload), original) {
			t.Fatalf("replay contains the original, untruncated payload: %s", part.Payload)
		}
	}
}

// TestReductionClearsOlderRoundAsAuthorizedRewrite proves reduction's
// legitimate settled-result CLEARING is still recognized by settlementSeal
// as an authorized content-management rewrite (via
// wrapAuthorizedContentRewrites), not an unauthorized divergence, and that
// it actually changes what the model dispatched against sees -- "ordering
// with two rewrites in one turn". MaxLengthForTrunc is left at upstream's
// default (a no-op for this fixture's short payload) so only clearing is
// exercised; see TestReductionTruncatesSettledToolResultBeforeSettlement
// for truncation's own, now pre-settlement, proof.
func TestReductionClearsOlderRoundAsAuthorizedRewrite(t *testing.T) {
	const original = "this output is intentionally much longer than the configured truncation threshold so reduction truncates it every time"
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("reduction-clear-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		// Larger than this fixture's payload, so truncation (Mount's own
		// default MaxLengthForTrunc is 64, smaller than the payload below)
		// never fires here -- this test exercises clearing only.
		ReductionMaxLengthForTrunc: 4096,
		// Low enough that clearing fires for the older round once a
		// second, more recent round exists (upstream's default
		// ClearRetentionSuffixLimit, 1, always protects the single
		// most-recent tool-call round).
		ReductionMaxTokensForClear: 1,
		Disable:                    disableAllExcept(runtime.HandlerKindReduction),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	// Retention is explicit (MaxInlineBytes:-1), not the zero-value
	// default: the payload must be genuinely inline so only reduction's own
	// clearing -- not this runtime's own retention-policy truncation --
	// can be what shortens it (see the runtime-internal proof's control
	// test, TestReductionWithoutMountLeavesOriginalPayloadIntact, for why
	// the zero-value default is a false-positive trap here).
	toolMount, err := registry.Mount(context.Background(), testNativeComponent("bigecho", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "bigecho", Scope: testScope(sessionID), Definition: tools.Definition{
				Name: "bigecho", Description: "returns a large payload for reduction to truncate",
				Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{}),
				Execute: tools.TypedExecutor[map[string]any, map[string]any](func(context.Context, tools.TypedExecution[map[string]any]) (map[string]any, error) {
					return map[string]any{"text": original}, nil
				}),
				Retention: runtime.RetentionPolicy{MaxInlineBytes: -1},
			}})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = toolMount.Close(context.Background()) }()

	var secondDispatchContent string
	var thirdDispatchByCallID map[string]string
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
			thirdDispatchByCallID = toolResultTextByCallID(request.Messages)
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
	// Round 2 sees only call-1's result so far, the single most-recent
	// round: upstream's default ClearRetentionSuffixLimit (1) protects it,
	// so it must still be the full original text here.
	if secondDispatchContent == "" || !strings.Contains(secondDispatchContent, original) {
		t.Fatalf("second dispatch content = %q, want the only-so-far round still retained in full", secondDispatchContent)
	}
	if len(thirdDispatchByCallID) != 2 {
		t.Fatalf("third dispatch tool results by call ID = %#v, want call-1 and call-2", thirdDispatchByCallID)
	}
	if strings.Contains(thirdDispatchByCallID["call-1"], original) {
		t.Fatalf("call-1 content = %q, want it cleared once a second, more recent round exists", thirdDispatchByCallID["call-1"])
	}
	if !strings.Contains(thirdDispatchByCallID["call-2"], original) {
		t.Fatalf("call-2 content = %q, want the most recent round's result still retained in full", thirdDispatchByCallID["call-2"])
	}
}

// toolResultTextByCallID is toolResultParts keyed by call ID, so a test can
// assert on one specific round's content independently of any other
// round's.
func toolResultTextByCallID(messages []*einoschema.AgenticMessage) map[string]string {
	result := make(map[string]string)
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block == nil || block.Type != einoschema.ContentBlockTypeFunctionToolResult || block.FunctionToolResult == nil {
				continue
			}
			var b strings.Builder
			for _, part := range block.FunctionToolResult.Content {
				if part != nil && part.Type == einoschema.FunctionToolResultContentBlockTypeText && part.Text != nil {
					b.WriteString(part.Text.Text)
				}
			}
			result[block.FunctionToolResult.CallID] = b.String()
		}
	}
	return result
}

// summaryGenerationMarker is upstream summarization's own fixed
// user-instruction text (adk/middlewares/summarization/prompt.go's
// userSummaryInstruction), used here to tell its internal summary-
// generation call apart from this turn's own main dispatch in a scripted
// streamer, without depending on any runtime-internal plumbing.
const summaryGenerationMarker = "CRITICAL: Respond with TEXT ONLY"

// requestIsSummaryGeneration reports whether request carries upstream
// summarization's own fixed user-instruction marker text anywhere in its
// messages.
func requestIsSummaryGeneration(request model.Request) bool {
	for _, msg := range request.Messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block == nil || block.UserInputText == nil {
				continue
			}
			if strings.Contains(block.UserInputText.Text, summaryGenerationMarker) {
				return true
			}
		}
	}
	return false
}

// TestSummarizationWritesContextEpochPreservesReplayThenNarrowsProjection
// proves summarization's full positive path with a fake summary model (the
// same scripted streamer serving the whole turn, including upstream's own
// internal summary-generation call, distinguished via
// requestIsSummaryGeneration): a session.ContextEpoch row is durably
// written with agent path "summarization" (this handler's own registered
// ID -- see adkEngine.buildAgentHandlers) on its own ledger row, the summary
// is never persisted as the turn's own assistant answer (exactly one
// assistant_gen_text part on the turn's message), the full message replay
// is never shortened (nothing is deleted), and a SECOND, later turn on the
// same session -- a real second orch.Start call, not a re-derivation --
// both completes (proving the prior "session content is invalid" admission
// bug is fixed) and sees a narrower model-visible history than the full
// durable replay, proving only the active provider projection changes.
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

	var secondDispatchMessageCount int
	var sawSecondDispatch bool
	streamer := scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		if requestIsSummaryGeneration(request) {
			return []*einoschema.AgenticMessage{agenticAssistantText("a compact summary of everything so far")}, nil
		}
		if sawSecondDispatch {
			// The turn-2 main dispatch: record how many messages the
			// provider actually saw, to compare against the full durable
			// replay below.
			secondDispatchMessageCount = len(request.Messages)
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("ok, understood")}, nil
	})
	orch := newTestOrchestrator(t, store, registry, streamer)

	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hello there"), Config: testConfig(""),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status != session.RunCompleted {
		t.Fatalf("turn 1 result = %+v, want completed", result)
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

	// The summarization recipe's own internal call is durably ledgered with
	// agent path "summarization" (its own registered HandlerID, tagging
	// every entry's bounded internal-dispatch adapter -- round-two W6
	// review I7) -- never the turn's own (empty) agent path -- and never
	// claims/persists onto the turn's own assistant message.
	requests, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var sawSummarizerAgentPath bool
	for _, record := range requests.Records {
		if record.AgentPath == "summarization" {
			sawSummarizerAgentPath = true
		}
	}
	if !sawSummarizerAgentPath {
		t.Fatalf("no model request ledger row has AgentPath == \"summarization\": %+v", requests.Records)
	}

	replay, err := store.ListMessages(context.Background(), sessionID, session.ReplayCursor{})
	if err != nil {
		t.Fatal(err)
	}
	if len(replay.Messages) < 2 {
		t.Fatalf("full replay = %d messages, want the original turn's messages still present (nothing deleted)", len(replay.Messages))
	}
	owners, err := session.ResolveReplayPartOwners(replay.Parts, replay.PartOwnerMessageIDs)
	if err != nil {
		t.Fatal(err)
	}
	var turnAssistantGenTextParts int
	for i, part := range replay.Parts {
		if owners[i] == result.MessageID && part.Kind == session.PartAssistantGenText {
			turnAssistantGenTextParts++
		}
	}
	if turnAssistantGenTextParts != 1 {
		t.Fatalf("turn's own assistant message has %d assistant_gen_text parts, want exactly 1 (the summary must never be persisted onto it)", turnAssistantGenTextParts)
	}

	// Second turn, same session: a real second orch.Start call. This must
	// complete -- the summarization recipe's internal-dispatch adapter no
	// longer corrupts the turn's assistant message, so admission's durable
	// history reload succeeds instead of failing with a content decode
	// error.
	sawSecondDispatch = true
	handle2, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("please continue"), Config: testConfig(""),
	})
	if err != nil {
		t.Fatal(err)
	}
	result2 := awaitDone(t, handle2, 10*time.Second)
	if result2.Status != session.RunCompleted {
		t.Fatalf("turn 2 result = %+v, want completed", result2)
	}
	if secondDispatchMessageCount == 0 {
		t.Fatal("turn 2's main dispatch was never observed")
	}
	fullReplay, err := store.ListMessages(context.Background(), sessionID, session.ReplayCursor{})
	if err != nil {
		t.Fatal(err)
	}
	if len(fullReplay.Messages) <= len(replay.Messages) {
		t.Fatalf("full replay did not grow across turn 2: before=%d after=%d", len(replay.Messages), len(fullReplay.Messages))
	}
	if secondDispatchMessageCount >= len(fullReplay.Messages) {
		t.Fatalf("turn 2's provider-visible message count (%d) did not narrow below the full durable replay (%d)", secondDispatchMessageCount, len(fullReplay.Messages))
	}
}

// TestSummarizationFailedGenerationKeepsPreviousEpoch proves the "failed
// generation" half of the epoch-preservation contract with a streamer that
// actually triggers summarization (SummarizationTriggerMsgs: 1) and then
// fails specifically the internal summary-generation call (identified via
// requestIsSummaryGeneration, upstream's own fixed marker text) while the
// turn's own main dispatch still succeeds: upstream never calls Finalize
// when its own model call fails, so no session.ContextEpoch row is ever
// created for this run, and the turn still completes.
func TestSummarizationFailedGenerationKeepsPreviousEpoch(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("summarization-failure-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		SummarizationTriggerMsgs: 1,
		Disable:                  disableAllExcept(runtime.HandlerKindSummarization),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		if requestIsSummaryGeneration(request) {
			return nil, errors.New("simulated summary generation failure")
		}
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
		t.Fatalf("result = %+v, want completed even though the summary generation call failed", result)
	}
	epochs, err := store.ListContextEpochs(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	for _, epoch := range epochs {
		if epoch.Trigger == "summarization" {
			t.Fatalf("a summarization epoch was created even though generation failed: %+v", epoch)
		}
	}
}

// TestSummarizationCancelledGenerationKeepsPreviousEpoch is the cancelled-
// context counterpart to TestSummarizationFailedGenerationKeepsPreviousEpoch:
// the summary generation call's own context is cancelled instead of
// returning an ordinary error, and the same invariant holds -- no
// session.ContextEpoch row is created, and the turn's own main dispatch
// still completes.
func TestSummarizationCancelledGenerationKeepsPreviousEpoch(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("summarization-cancel-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		SummarizationTriggerMsgs: 1,
		Disable:                  disableAllExcept(runtime.HandlerKindSummarization),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		if requestIsSummaryGeneration(request) {
			return nil, context.Canceled
		}
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
		t.Fatalf("result = %+v, want completed even though the summary generation call was cancelled", result)
	}
	epochs, err := store.ListContextEpochs(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	for _, epoch := range epochs {
		if epoch.Trigger == "summarization" {
			t.Fatalf("a summarization epoch was created even though generation was cancelled: %+v", epoch)
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
// deferredSearchToolDefinition's calledID, when non-nil, is set to the
// durable tool-call id of the most recent execution: the scripted provider
// CallID a test script uses (e.g. "call-hidden") is preserved separately as
// ProviderCallID now -- the durable ID is always a fresh mint -- so a
// caller that needs to look the settled call back up in the store must
// capture it from here rather than assume the scripted literal survives.
func deferredSearchToolDefinition(calledID *session.ToolCallID) tools.Definition {
	return tools.Definition{
		Name: "hidden_capability", Description: "only discoverable via deferred tool search",
		Parameters: einoschema.NewParamsOneOfByParams(map[string]*einoschema.ParameterInfo{}),
		Execute: tools.TypedExecutor[map[string]any, map[string]any](func(_ context.Context, execution tools.TypedExecution[map[string]any]) (map[string]any, error) {
			if calledID != nil {
				*calledID = execution.Call.ID
			}
			return map[string]any{"text": "found it"}, nil
		}),
		Deferred: true,
	}
}

// TestToolSearchFindsDeferredTool proves toolsearch's positive path all the
// way through: a deferred (Deferred: true) composition tool is not eagerly
// bound, is discoverable by name via the "tool_search" meta-tool toolsearch
// installs (query "select:<name>"), and -- item 6's fix -- the discovered
// tool is then actually CALLABLE in a follow-up dispatch, settling
// ToolCallCompleted with its real output, not denied by the runtime's
// undiscovered-deferred-tool gate.
//
// Root cause (see the W6 round-1 review's I4 finding, confirmed against
// source): upstream's toolsearch middleware settles its own "tool_search"
// call as an ordinary function result and never calls this runtime's
// markDiscovered the way the native tool-search path
// (adkToolSearch/executeToolSearchCall) does, so tool_preparation.go's
// undiscovered-deferred-tool gate (`tool.Deferred && !discovered[name]`)
// still fires on the very next call to the tool the model just found --
// this was never a permissions issue (this harness's nil permissions.Policy
// gates nothing). This test's live-run half of the fix is now superseded
// by a durable one (round-two W6 review item 1): the recipe's own sealed
// search tool no longer settles as an ordinary function_tool_result with
// only an in-memory markDiscovered side effect -- adkTool.InvokableRun
// type-asserts it as a toolSearchHandlerToolExecutor and routes it through
// adkEngine.executeAndSettleHandlerToolSearch (runtime/tool_search.go),
// which settles the call as a durable tool_search_result block via
// buildTerminalToolSearchEnvelope, the SAME content-block shape the native
// tool-search path produces. Because discoveredToolsFromMessages/
// discoveredToolsFromHistoryPaged read that shape (not the live run's
// in-memory set) to rebuild the advertised deferred-tool set, discovery
// now replays across a fresh turn on the same session, across ResumeRun,
// and across a brand-new orchestrator instance against the same store --
// proved respectively by TestToolSearchDiscoveryReplaysOnNextTurn,
// TestToolSearchDiscoveryReplaysAfterResumeRun, and
// TestToolSearchDiscoveryReplaysAfterProcessRestart below.
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
	var hiddenCallID session.ToolCallID
	toolMount, err := registry.Mount(context.Background(), testNativeComponent("hidden", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "hidden_capability", Scope: testScope(sessionID), Definition: deferredSearchToolDefinition(&hiddenCallID)})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = toolMount.Close(context.Background()) }()

	var searchResult string
	var calledResult string
	step := 0
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		switch step {
		case 1:
			args, _ := json.Marshal(map[string]string{"query": "select:hidden_capability"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-search", "tool_search", string(args))}, nil
		case 2:
			// The recipe's own sealed search tool now settles durably as a
			// tool_search_result block (item 1 of the round-two W6 review),
			// not an ordinary function_tool_result -- see
			// toolSearchResultDiscoveredNames.
			searchResult = strings.Join(toolSearchResultDiscoveredNames(request.Messages), ",")
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-hidden", "hidden_capability", `{}`)}, nil
		default:
			calledResult, _ = toolResultParts(request.Messages)
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
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
	if strings.Contains(calledResult, "expected_failure") || strings.Contains(calledResult, "undiscovered") {
		t.Fatalf("calledResult = %q, want it NOT denied as undiscovered", calledResult)
	}
	// The model-visible/durable envelope is truncated under this harness's
	// zero-inline-retention config (orthogonal to this test), so ToolCallCompleted
	// -- not the specific bytes -- is the proof that discovery actually
	// unblocked the call: an undiscovered-deferred-tool denial settles
	// ToolCallFailed with an "expected_failure" envelope instead.
	if hiddenCallID == "" {
		t.Fatal("hidden_capability tool never executed, no durable call id captured")
	}
	toolCall, err := store.GetToolCall(context.Background(), hiddenCallID)
	if err != nil {
		t.Fatal(err)
	}
	if toolCall.Status != session.ToolCallCompleted {
		t.Fatalf("hidden_capability settled status = %q, want completed", toolCall.Status)
	}
}

// TestToolSearchDiscoveryReplaysOnNextTurn proves discovery survives across
// a real turn boundary: turn 1 searches and discovers hidden_capability
// (settling durably as a tool_search_result block, per item 1 of the
// round-two W6 review); turn 2 is a genuinely separate orch.Start call on
// the SAME session that calls hidden_capability directly, with no search
// anywhere in turn 2's own script. This only succeeds if turn 2's snapshot
// admission (discoveredToolsFromMessages, seeded from the durably replayed
// history FreezeTurnSnapshot/prepareSnapshot load, not any live-run
// in-memory carryover from turn 1) already sees the tool as discovered.
func TestToolSearchDiscoveryReplaysOnNextTurn(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("toolsearch-replay-next-turn-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		Disable: disableAllExcept(runtime.HandlerKindToolSearch),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	var hiddenCallID session.ToolCallID
	toolMount, err := registry.Mount(context.Background(), testNativeComponent("hidden", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "hidden_capability", Scope: testScope(sessionID), Definition: deferredSearchToolDefinition(&hiddenCallID)})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = toolMount.Close(context.Background()) }()

	var turn2Result string
	step := 0
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		switch step {
		case 1:
			args, _ := json.Marshal(map[string]string{"query": "select:hidden_capability"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-search-t1", "tool_search", string(args))}, nil
		case 2:
			return []*einoschema.AgenticMessage{agenticAssistantText("turn 1 done")}, nil
		case 3:
			// Turn 2's very first dispatch: call the deferred tool
			// directly, with no search anywhere in this turn's own
			// script.
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-hidden-t2", "hidden_capability", `{}`)}, nil
		default:
			turn2Result, _ = toolResultParts(request.Messages)
			return []*einoschema.AgenticMessage{agenticAssistantText("turn 2 done")}, nil
		}
	}))
	// A single shared workspace root across both turns: admission's own
	// session-identity check (sameAdmissionSessionIdentity, comparing
	// Session.Directory) rejects a second Start on the same session whose
	// Config names a different workspace_root as session.ErrConflict, so
	// two independent t.TempDir() calls here would fail turn 2 for a
	// reason unrelated to what this test proves.
	root := t.TempDir()
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(root),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status != session.RunCompleted {
		t.Fatalf("turn 1 result = %+v, want completed", result)
	}

	handle2, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("call it directly this time"), Config: testConfig(root),
	})
	if err != nil {
		t.Fatal(err)
	}
	result2 := awaitDone(t, handle2, 10*time.Second)
	if result2.Status != session.RunCompleted {
		t.Fatalf("turn 2 result = %+v, want completed", result2)
	}
	if strings.Contains(turn2Result, "expected_failure") || strings.Contains(turn2Result, "undiscovered") {
		t.Fatalf("turn2Result = %q, want hidden_capability NOT denied as undiscovered in a fresh turn", turn2Result)
	}
	if hiddenCallID == "" {
		t.Fatal("hidden_capability tool never executed, no durable call id captured")
	}
	toolCall, err := store.GetToolCall(context.Background(), hiddenCallID)
	if err != nil {
		t.Fatal(err)
	}
	if toolCall.Status != session.ToolCallCompleted {
		t.Fatalf("hidden_capability settled status (turn 2, no re-search) = %q, want completed", toolCall.Status)
	}
}

// TestToolSearchDiscoveryReplaysAfterResumeRun proves discovery survives a
// checkpointed pause/resume: the run searches and discovers
// hidden_capability, then a blocking tool call pauses the run
// (orch.Stop(Immediate)) before the discovered tool is ever called; after
// orch.ResumeRun, the SAME paused run calls hidden_capability and it must
// not be denied as undiscovered. This exercises turn_loop.go's
// resumeInterruptedTurn path, which reseeds the advertised deferred-tool
// set via discoveredToolsFromHistoryPaged (the durable, paged-history
// counterpart of discoveredToolsFromMessages) rather than trusting any
// live in-memory state the paused run held before the pause.
func TestToolSearchDiscoveryReplaysAfterResumeRun(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("toolsearch-replay-resume-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		Disable: disableAllExcept(runtime.HandlerKindToolSearch),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	var hiddenCallID session.ToolCallID
	toolMount, err := registry.Mount(context.Background(), testNativeComponent("hidden", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "hidden_capability", Scope: testScope(sessionID), Definition: deferredSearchToolDefinition(&hiddenCallID)})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = toolMount.Close(context.Background()) }()

	started := make(chan struct{})
	release := make(chan struct{})
	var toolCalls int
	blockerMount, err := registry.Mount(context.Background(), testNativeComponent("blocker", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "blocker", Scope: testScope(sessionID), Definition: blockingToolDefinition(started, release, &toolCalls)})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blockerMount.Close(context.Background()) }()

	var calledResult string
	step := 0
	orch := newTestOrchestrator(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		switch step {
		case 1:
			args, _ := json.Marshal(map[string]string{"query": "select:hidden_capability"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-search-resume", "tool_search", string(args))}, nil
		case 2:
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-block-resume", "blocker", `{}`)}, nil
		case 3:
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-hidden-resume", "hidden_capability", `{}`)}, nil
		default:
			calledResult, _ = toolResultParts(request.Messages)
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	}))
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(t.TempDir()),
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := orch.Stop(context.Background(), handle.RunID(), runtime.StopPolicy{Immediate: true, Cause: "toolsearch-resume-example"}); err != nil {
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
	if strings.Contains(calledResult, "expected_failure") || strings.Contains(calledResult, "undiscovered") {
		t.Fatalf("calledResult = %q, want hidden_capability NOT denied as undiscovered after resume", calledResult)
	}
	if hiddenCallID == "" {
		t.Fatal("hidden_capability tool never executed, no durable call id captured")
	}
	toolCall, err := store.GetToolCall(context.Background(), hiddenCallID)
	if err != nil {
		t.Fatal(err)
	}
	if toolCall.Status != session.ToolCallCompleted {
		t.Fatalf("hidden_capability settled status (post-resume) = %q, want completed", toolCall.Status)
	}
}

// TestToolSearchDiscoveryReplaysAfterProcessRestart proves discovery
// survives a genuine process restart, not merely a live run's in-memory
// markDiscovered set: turn 1 searches and discovers hidden_capability
// under one *runtime.StreamingOrchestrator instance and completes; a
// SECOND, brand-new orchestrator instance (fresh IDGenerator, fresh
// in-process state, zero carryover -- newTestOrchestrator constructs a
// wholly separate runtime.NewStreamingOrchestrator) is then pointed at the
// same durable store/registry/session and calls hidden_capability
// directly in its very first dispatch.
func TestToolSearchDiscoveryReplaysAfterProcessRestart(t *testing.T) {
	store := newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("toolsearch-replay-restart-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		Disable: disableAllExcept(runtime.HandlerKindToolSearch),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mount.Close(context.Background()) }()
	var hiddenCallID session.ToolCallID
	toolMount, err := registry.Mount(context.Background(), testNativeComponent("hidden", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "hidden_capability", Scope: testScope(sessionID), Definition: deferredSearchToolDefinition(&hiddenCallID)})
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = toolMount.Close(context.Background()) }()

	// A shared IDGenerator across both orchestrator instances, so the
	// second instance's counter doesn't restart from 1 and collide with
	// IDs the first already durably wrote -- see
	// newTestOrchestratorWithIDs's doc comment.
	ids := &testIDs{}
	turn1Step := 0
	orch1 := newTestOrchestratorWithIDs(t, store, registry, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		turn1Step++
		if turn1Step == 1 {
			args, _ := json.Marshal(map[string]string{"query": "select:hidden_capability"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-search-restart", "tool_search", string(args))}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("turn 1 done")}, nil
	}), ids)
	// A single shared workspace root across both orchestrator instances --
	// see the matching comment in TestToolSearchDiscoveryReplaysOnNextTurn
	// on why a mismatched Config.Metadata["workspace_root"] across two
	// Start calls on the same session fails admission with
	// session.ErrConflict for a reason unrelated to what this test proves.
	root := t.TempDir()
	handle, err := orch1.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(root),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status != session.RunCompleted {
		t.Fatalf("turn 1 result = %+v, want completed", result)
	}

	var restartResult string
	step := 0
	orch2 := newTestOrchestratorWithIDs(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		if step == 1 {
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-hidden-restart", "hidden_capability", `{}`)}, nil
		}
		restartResult, _ = toolResultParts(request.Messages)
		return []*einoschema.AgenticMessage{agenticAssistantText("done after restart")}, nil
	}), ids)
	handle2, err := orch2.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("new orchestrator instance, call it directly"), Config: testConfig(root),
	})
	if err != nil {
		t.Fatal(err)
	}
	result2 := awaitDone(t, handle2, 10*time.Second)
	if result2.Status != session.RunCompleted {
		t.Fatalf("post-restart result = %+v, want completed", result2)
	}
	if strings.Contains(restartResult, "expected_failure") || strings.Contains(restartResult, "undiscovered") {
		t.Fatalf("restartResult = %q, want hidden_capability NOT denied as undiscovered after a process restart", restartResult)
	}
	if hiddenCallID == "" {
		t.Fatal("hidden_capability tool never executed, no durable call id captured")
	}
	toolCall, err := store.GetToolCall(context.Background(), hiddenCallID)
	if err != nil {
		t.Fatal(err)
	}
	if toolCall.Status != session.ToolCallCompleted {
		t.Fatalf("hidden_capability settled status (post-restart) = %q, want completed", toolCall.Status)
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

// resumeSkillFixture sets up a paused run whose turn already durably
// activated a skill (SkillActivatedEventKind, via NewSkillHandlerFactory's
// activationRecordingSkillBackend) before it paused mid-turn on a blocking
// tool call -- the shared setup for the two ResumeRun-skill-verification
// tests below (item 3 of the round-two W6 review). skillDir is the
// on-disk directory the caller may mutate between pause and resume to
// exercise the negative case.
func resumeSkillFixture(t *testing.T) (orch *runtime.StreamingOrchestrator, store *sqlite.Store, runID session.RunID, skillDir string) {
	t.Helper()
	root := t.TempDir()
	skillDir = filepath.Join(root, "greeter")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte("---\nname: greeter\ndescription: says hi\n---\nAlways greet the user by name.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store = newTestStore(t)
	registry := newTestRegistry(t)
	sessionID := session.ID("resume-skill-session")
	mount, err := Mount(context.Background(), registry, sessionID, Config{
		Disable: disableAllExcept(runtime.HandlerKindSkill),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mount.Close(context.Background()) })

	started := make(chan struct{})
	release := make(chan struct{})
	var toolCalls int
	blockerMount, err := registry.Mount(context.Background(), testNativeComponent("blocker", sessionID),
		composition.InstallerFunc(func(_ context.Context, registrar *composition.Registrar) error {
			return registrar.Tool(composition.ToolRegistration{ID: "blocker", Scope: testScope(sessionID), Definition: blockingToolDefinition(started, release, &toolCalls)})
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blockerMount.Close(context.Background()) })

	step := 0
	orch = newTestOrchestrator(t, store, registry, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		step++
		switch step {
		case 1:
			args, _ := json.Marshal(map[string]string{"skill": "greeter"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-skill-resume", "skill", string(args))}, nil
		case 2:
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-block-resume", "blocker", `{}`)}, nil
		default:
			return []*einoschema.AgenticMessage{agenticAssistantText("resumed and done")}, nil
		}
	}))
	handle, err := orch.Start(context.Background(), runtime.Request{
		SessionID: sessionID, Message: runtime.TextUserMessage("hi"), Config: testConfig(root),
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := orch.Stop(context.Background(), handle.RunID(), runtime.StopPolicy{Immediate: true, Cause: "resume-skill-example"}); err != nil {
		t.Fatal(err)
	}
	close(release)
	result := awaitDone(t, handle, 10*time.Second)
	if result.Status != session.RunPaused {
		t.Fatalf("result = %+v, want paused", result)
	}
	return orch, store, result.RunID, skillDir
}

// TestResumeAfterUnchangedSkillContentSucceeds proves the positive half of
// item 3's skill resume verification: a paused run whose turn already
// activated a skill (durably recorded via SkillActivatedEventKind) resumes
// normally when that skill's on-disk content is untouched between pause
// and resume -- verifySkillActivationsUnchanged's fresh digest re-read
// matches the recorded one, so ResumeRun's new pre-claim check never
// rejects an ordinary resume.
func TestResumeAfterUnchangedSkillContentSucceeds(t *testing.T) {
	orch, _, runID, _ := resumeSkillFixture(t)
	resumed, err := orch.ResumeRun(context.Background(), runID, runtime.ResumeRequest{})
	if err != nil {
		t.Fatalf("resume with unchanged skill content was refused: %v", err)
	}
	result := awaitDone(t, resumed, 10*time.Second)
	if result.Status != session.RunCompleted {
		t.Fatalf("resumed result = %+v, want completed", result)
	}
}

// TestResumeAfterChangedSkillContentIsRefused proves the negative half of
// item 3: editing the activated skill's SKILL.md between pause and resume
// is detected and rejected predictably, with a clear
// runtime.ErrSkillChangedSinceActivation error, and the run is left
// exactly as paused as it was before the rejected ResumeRun call -- never
// silently resumed under skill content the model was never actually shown
// this way.
func TestResumeAfterChangedSkillContentIsRefused(t *testing.T) {
	orch, store, runID, skillDir := resumeSkillFixture(t)
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte("---\nname: greeter\ndescription: says hi differently now\n---\nGreet the user in French instead.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := orch.ResumeRun(context.Background(), runID, runtime.ResumeRequest{}); err == nil {
		t.Fatal("resume after changed skill content was accepted, want it refused")
	} else if !errors.Is(err, runtime.ErrSkillChangedSinceActivation) {
		t.Fatalf("resume error = %v, want errors.Is(err, runtime.ErrSkillChangedSinceActivation)", err)
	}
	run, err := store.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != session.RunPaused {
		t.Fatalf("run status after a refused resume = %q, want still paused (run left untouched)", run.Status)
	}
}
