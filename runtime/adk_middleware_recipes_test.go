package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/compaction"
)

// TestHandlerBuildContextExposesNoDurableStoreAuthority is the compile-time-
// backed regression trip-wire for C1: HandlerBuildContext is handed
// identically to every registered HandlerFactory, host-provided ones
// included, so no EXPORTED field on it may carry session.Store or
// session.ExecutionStore -- either would let an arbitrary host handler
// fabricate a durable ToolCall settlement (ClaimToolCall/SettleToolCall) or
// read checkpoint/provider-private state (ReadPromotedCheckpoint,
// ListMessages' provider_state parts). This is primarily enforced by the Go
// compiler already (the struct literally has no such field, so
// `build.Store`/`build.Execution` do not compile -- see git history for the
// pre-fix version, which did), but this test also checks by FIELD TYPE, not
// name, so it still catches a differently-named field re-introducing the
// same capability.
func TestHandlerBuildContextExposesNoDurableStoreAuthority(t *testing.T) {
	storeType := reflect.TypeOf((*session.Store)(nil)).Elem()
	executionType := reflect.TypeOf((*session.ExecutionStore)(nil)).Elem()
	typ := reflect.TypeOf(HandlerBuildContext{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		if field.Type == storeType || field.Type == executionType {
			t.Fatalf("HandlerBuildContext exports field %q of type %s: a host-registered HandlerFactory could reach durable claim/settle authority or checkpoint bytes through it", field.Name, field.Type)
		}
	}
}

func newTestWorkspaceHandlerBuildContext(t *testing.T) HandlerBuildContext {
	t.Helper()
	root := t.TempDir()
	fsBackend, skillBackend, err := newWorkspaceBackends(root)
	if err != nil {
		t.Fatal(err)
	}
	planBackend, err := newWritableWorkspaceBackend(root, filepath.Join(".eino-agent", "plantask"))
	if err != nil {
		t.Fatal(err)
	}
	reductionBackend, err := newWritableWorkspaceBackend(root, filepath.Join(".eino-agent", "reduction"))
	if err != nil {
		t.Fatal(err)
	}
	return HandlerBuildContext{
		SessionID: "session-1", WorkspaceRoot: root,
		FilesystemBackend: fsBackend, SkillBackend: skillBackend,
		PlanTaskBackend: planBackend, ReductionBackend: reductionBackend,
	}
}

func TestAgentsMDHandlerFactoryPositiveAndMissingBackend(t *testing.T) {
	build := newTestWorkspaceHandlerBuildContext(t)
	if err := os.WriteFile(filepath.Join(build.WorkspaceRoot, "AGENTS.md"), []byte("be nice"), 0o644); err != nil {
		t.Fatal(err)
	}
	mw, err := NewAgentsMDHandlerFactory(AgentsMDConfig{AgentsMDFiles: []string{"AGENTS.md"}})(context.Background(), build)
	if err != nil || mw == nil {
		t.Fatalf("positive case: mw=%v err=%v", mw, err)
	}

	noBackend := build
	noBackend.FilesystemBackend = nil
	if _, err := NewAgentsMDHandlerFactory(AgentsMDConfig{AgentsMDFiles: []string{"AGENTS.md"}})(context.Background(), noBackend); err == nil {
		t.Fatal("missing filesystem backend was accepted")
	}
	if _, err := NewAgentsMDHandlerFactory(AgentsMDConfig{})(context.Background(), build); err == nil {
		t.Fatal("empty AgentsMDFiles was accepted")
	}
}

func TestSkillHandlerFactoryPositiveAndMissingBackend(t *testing.T) {
	build := newTestWorkspaceHandlerBuildContext(t)
	skillDir := filepath.Join(build.WorkspaceRoot, "greeter")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte("---\nname: greeter\ndescription: says hi\n---\nSay hi.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mw, err := NewSkillHandlerFactory(SkillConfig{})(context.Background(), build)
	if err != nil || mw == nil {
		t.Fatalf("positive case: mw=%v err=%v", mw, err)
	}

	noBackend := build
	noBackend.SkillBackend = nil
	if _, err := NewSkillHandlerFactory(SkillConfig{})(context.Background(), noBackend); err == nil {
		t.Fatal("missing skill backend was accepted")
	}
}

func TestFilesystemHandlerFactoryPositiveAndMissingBackend(t *testing.T) {
	build := newTestWorkspaceHandlerBuildContext(t)
	mw, err := NewFilesystemHandlerFactory(FilesystemConfig{UseMultiModalRead: true})(context.Background(), build)
	if err != nil || mw == nil {
		t.Fatalf("positive case: mw=%v err=%v", mw, err)
	}
	noBackend := build
	noBackend.FilesystemBackend = nil
	if _, err := NewFilesystemHandlerFactory(FilesystemConfig{})(context.Background(), noBackend); err == nil {
		t.Fatal("missing filesystem backend was accepted")
	}
}

func TestPlanTaskHandlerFactoryPositiveAndMissingBackend(t *testing.T) {
	build := newTestWorkspaceHandlerBuildContext(t)
	mw, err := NewPlanTaskHandlerFactory(PlanTaskConfig{})(context.Background(), build)
	if err != nil || mw == nil {
		t.Fatalf("positive case: mw=%v err=%v", mw, err)
	}
	noBackend := build
	noBackend.PlanTaskBackend = nil
	if _, err := NewPlanTaskHandlerFactory(PlanTaskConfig{})(context.Background(), noBackend); err == nil {
		t.Fatal("missing plantask backend was accepted")
	}
}

func TestReductionHandlerFactoryPositiveAndMissingBackend(t *testing.T) {
	build := newTestWorkspaceHandlerBuildContext(t)
	mw, err := NewReductionHandlerFactory(ReductionConfig{MaxLengthForTrunc: 100, MaxTokensForClear: 1000})(context.Background(), build)
	if err != nil || mw == nil {
		t.Fatalf("positive case: mw=%v err=%v", mw, err)
	}
	noBackend := build
	noBackend.ReductionBackend = nil
	if _, err := NewReductionHandlerFactory(ReductionConfig{})(context.Background(), noBackend); err == nil {
		t.Fatal("missing reduction backend was accepted")
	}
}

func TestPatchToolCallsHandlerFactoryConstructs(t *testing.T) {
	build := newTestWorkspaceHandlerBuildContext(t)
	mw, err := NewPatchToolCallsHandlerFactory(PatchToolCallsConfig{PatchedText: "patched"})(context.Background(), build)
	if err != nil || mw == nil {
		t.Fatalf("mw=%v err=%v", mw, err)
	}
}

type fakeDeferredTool struct{ name string }

func (t fakeDeferredTool) Info(context.Context) (*einoschema.ToolInfo, error) {
	return &einoschema.ToolInfo{Name: t.name, Desc: "a deferred fake tool"}, nil
}
func (t fakeDeferredTool) InvokableRun(context.Context, string, ...tool.Option) (string, error) {
	return "ok", nil
}

func TestToolSearchHandlerFactoryPositiveAndNoDeferredTools(t *testing.T) {
	build := newTestWorkspaceHandlerBuildContext(t)
	build.DeferredTools = []tool.BaseTool{fakeDeferredTool{name: "search_web"}}
	mw, err := NewToolSearchHandlerFactory(ToolSearchHandlerConfig{})(context.Background(), build)
	if err != nil || mw == nil {
		t.Fatalf("positive case: mw=%v err=%v", mw, err)
	}
	build.DeferredTools = nil
	if _, err := NewToolSearchHandlerFactory(ToolSearchHandlerConfig{})(context.Background(), build); err == nil {
		t.Fatal("empty DeferredTools was accepted")
	}
}

func TestSummarizationHandlerFactoryRequiresStoreAndModel(t *testing.T) {
	build := newTestWorkspaceHandlerBuildContext(t)
	if _, err := NewSummarizationHandlerFactory(SummarizationConfig{})(context.Background(), build); err == nil {
		t.Fatal("summarization without a durable store/model was accepted")
	}
}

// TestSummarizationHandlerFactoryRejectsNeitherTriggerConfigured is
// round-two W6 review item 9's second half: a SummarizationConfig with
// neither TriggerContextTokens nor TriggerContextMessages set must fail
// construction closed, rather than silently falling through to upstream's
// own hidden default trigger (a fixed 160000-token threshold the host never
// asked for and cannot see reflected in its own sealed Config).
func TestSummarizationHandlerFactoryRejectsNeitherTriggerConfigured(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("no-trigger-session")
	execution := testFencedExecutionStore(t, store, sessionID)
	build := HandlerBuildContext{
		SessionID: sessionID, Model: &adkModel{},
		epochs: contextEpochCapability{sessionID: sessionID, store: store, execution: execution, ids: &sequenceIDs{}, now: func() time.Time { return time.Unix(0, 0) }},
	}
	_, err := NewSummarizationHandlerFactory(SummarizationConfig{})(context.Background(), build)
	if !errors.Is(err, ErrHandlerConfiguration) {
		t.Fatalf("err = %v, want errors.Is(err, ErrHandlerConfiguration) for a config with neither trigger threshold set", err)
	}
}

func testFencedExecutionStore(t *testing.T, store *admissionStore, sessionID session.ID) session.ExecutionStore {
	t.Helper()
	run := session.Run{ID: session.RunID("run-" + string(sessionID)), SessionID: sessionID, ClaimToken: "claim-" + string(sessionID), Status: session.RunRunning}
	admitted, err := store.AdmitRun(context.Background(), run, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return store.Execution(session.RunFence{RunID: admitted.ID, ClaimToken: admitted.ClaimToken})
}

func TestSummarizationFinalizeMapsSummaryIntoContextEpoch(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("summary-session")
	now := func() time.Time { return time.Unix(1000, 0).UTC() }
	ids := &sequenceIDs{}
	var durable []session.Message
	for i, role := range []session.Role{session.RoleSystem, session.RoleUser, session.RoleAssistant, session.RoleUser, session.RoleAssistant} {
		msg, err := store.AppendMessage(context.Background(), session.Message{ID: session.MessageID("m" + string(rune('0'+i))), SessionID: sessionID, Role: role, CreatedAt: now(), UpdatedAt: now()})
		if err != nil {
			t.Fatal(err)
		}
		// A real durably-committed message always owns at least one
		// DECODABLE content part: summarizationFinalize now correlates via
		// history.LoadAgentic's own projection (matching what ADK's real
		// in-memory input is built from), which -- unlike the prior
		// PartApprovalDecision-only fixture -- only counts a message as
		// "real" (non-placeholder) content if it actually decodes to a
		// non-empty AgenticMessage.
		blockKind := session.BlockKindUserInputText
		if role == session.RoleAssistant {
			blockKind = session.BlockKindAssistantGenText
		}
		content := session.Content{Role: role, Blocks: []session.ContentBlock{{
			ID: "b" + string(rune('0'+i)), Kind: blockKind, Text: &session.TextBlock{Text: "content " + string(rune('0'+i))},
		}}}
		parts, err := session.EncodeContentParts(content, func() session.PartID { return session.PartID("p" + string(rune('0'+i))) }, msg.ID, sessionID, "", now(), session.DefaultContentLimits())
		if err != nil {
			t.Fatal(err)
		}
		for _, part := range parts {
			if _, err := store.AppendPart(context.Background(), part); err != nil {
				t.Fatal(err)
			}
		}
		durable = append(durable, msg)
	}
	execution := testFencedExecutionStore(t, store, sessionID)

	original := make([]*einoschema.AgenticMessage, len(durable))
	for i := range durable {
		original[i] = einoschema.UserAgenticMessage("msg")
	}
	// sourceByPointer simulates what durableBaselineHandler records each
	// cycle: originalMessages[i] correlates to durable[i].ID by pointer,
	// exactly what a real cycle with no injected content produces.
	sourceByPointer := make(map[*einoschema.AgenticMessage]session.MessageID, len(original))
	for i, msg := range original {
		sourceByPointer[msg] = durable[i].ID
	}
	build := HandlerBuildContext{
		SessionID: sessionID, epochs: contextEpochCapability{sessionID: sessionID, store: store, execution: execution, ids: ids, now: now},
		sourceMessageID: func(msg *einoschema.AgenticMessage) (session.MessageID, bool) {
			id, ok := sourceByPointer[msg]
			return id, ok
		},
	}
	finalize := summarizationFinalize(build, 1)

	summary := agenticAssistantText("compact summary of the conversation")

	result, err := finalize(context.Background(), original, summary)
	if err != nil {
		t.Fatalf("finalize error = %v", err)
	}
	// The leading system message is always preserved ahead of the summary
	// (see moveTailStartToGroupBoundary/summarizationFinalize's system
	// prefix handling): system prefix (1) + summary (1) + 1 retained tail
	// message.
	if len(result) != 3 {
		t.Fatalf("finalize result length = %d, want 3", len(result))
	}

	epochs, err := store.ListContextEpochs(context.Background(), sessionID)
	if err != nil || len(epochs) != 1 {
		t.Fatalf("epochs = %+v, err = %v", epochs, err)
	}
	epoch := epochs[0]
	// durable[0] is the leading system message, excluded from the
	// summarized range; the summarized range starts at durable[1].
	if epoch.SummarizedFromID != durable[1].ID || epoch.TailStartID != durable[len(durable)-1].ID {
		t.Fatalf("epoch boundaries = %+v", epoch)
	}
	if epoch.SummaryMessageID == "" {
		t.Fatal("epoch has no SummaryMessageID")
	}
}

// TestSummarizationFinalizeNeverSplitsAFunctionCallFromItsResult is
// round-two W6 review item 11's call/result group-boundary protection proof
// (moveTailStartToGroupBoundary): [system, user, assistant(tool_call),
// user(tool_result), assistant(final)] with retainTail=2 -- a NAIVE cut
// (len(durable)-retainTail=3) would land exactly between the tool_call and
// its own tool_result, splitting them across the summarized/tail boundary.
// The real boundary must instead retreat to keep the whole call/result
// group in the tail.
func TestSummarizationFinalizeNeverSplitsAFunctionCallFromItsResult(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("group-boundary-session")
	now := func() time.Time { return time.Unix(2000, 0).UTC() }
	ids := &sequenceIDs{}

	type fixture struct {
		role  session.Role
		block session.ContentBlock
	}
	fixtures := []fixture{
		{session.RoleSystem, session.ContentBlock{ID: "b0", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "system prompt"}}},
		{session.RoleUser, session.ContentBlock{ID: "b1", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "please do the thing"}}},
		{session.RoleAssistant, session.ContentBlock{ID: "b2", Kind: session.BlockKindFunctionToolCall, FunctionCall: &session.FunctionCallBlock{CallID: "call-1", Name: "thing", Arguments: "{}"}}},
		{session.RoleUser, session.ContentBlock{ID: "b3", Kind: session.BlockKindFunctionToolResult, FunctionResult: &session.FunctionResultBlock{CallID: "call-1", Name: "thing", Content: []session.ResultContent{{Type: session.ResultContentText, Text: "done"}}}}},
		{session.RoleAssistant, session.ContentBlock{ID: "b4", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "all set"}}},
	}
	var durable []session.Message
	for i, fx := range fixtures {
		msg, err := store.AppendMessage(context.Background(), session.Message{ID: session.MessageID("gm" + string(rune('0'+i))), SessionID: sessionID, Role: fx.role, CreatedAt: now(), UpdatedAt: now()})
		if err != nil {
			t.Fatal(err)
		}
		content := session.Content{Role: fx.role, Blocks: []session.ContentBlock{fx.block}}
		parts, err := session.EncodeContentParts(content, func() session.PartID { return session.PartID("gp" + string(rune('0'+i))) }, msg.ID, sessionID, "", now(), session.DefaultContentLimits())
		if err != nil {
			t.Fatal(err)
		}
		for _, part := range parts {
			if _, err := store.AppendPart(context.Background(), part); err != nil {
				t.Fatal(err)
			}
		}
		durable = append(durable, msg)
	}
	execution := testFencedExecutionStore(t, store, sessionID)
	original := make([]*einoschema.AgenticMessage, len(durable))
	for i := range durable {
		original[i] = einoschema.UserAgenticMessage("msg")
	}
	sourceByPointer := make(map[*einoschema.AgenticMessage]session.MessageID, len(original))
	for i, msg := range original {
		sourceByPointer[msg] = durable[i].ID
	}
	build := HandlerBuildContext{
		SessionID: sessionID, epochs: contextEpochCapability{sessionID: sessionID, store: store, execution: execution, ids: ids, now: now},
		sourceMessageID: func(msg *einoschema.AgenticMessage) (session.MessageID, bool) {
			id, ok := sourceByPointer[msg]
			return id, ok
		},
	}
	finalize := summarizationFinalize(build, 2)
	summary := agenticAssistantText("compact summary")
	if _, err := finalize(context.Background(), original, summary); err != nil {
		t.Fatalf("finalize error = %v", err)
	}
	epochs, err := store.ListContextEpochs(context.Background(), sessionID)
	if err != nil || len(epochs) != 1 {
		t.Fatalf("epochs = %+v, err = %v", epochs, err)
	}
	epoch := epochs[0]
	// durable[2] is the tool_call; durable[3] is its own tool_result. The
	// group-boundary protection must keep both in the tail, so TailStartID
	// must be durable[2] (the call), never durable[3] (the result, which
	// would mean the call itself got summarized away while its own result
	// survived in the tail -- a broken transcript no provider can replay).
	if epoch.TailStartID != durable[2].ID {
		t.Fatalf("epoch.TailStartID = %v, want durable[2] (%v) -- the tool call must never be separated from its own result", epoch.TailStartID, durable[2].ID)
	}
}

// TestCommitSummaryEpochIsAtomicAcrossFailure is round-two W6 review item
// 11's single-transaction-commit protection proof: contextEpochCapability.commitSummaryEpoch
// wraps StartContextEpoch and the boundary append (AppendMessage/AppendPart/
// FinishContextEpoch, via compaction.AppendBoundaryTx) in ONE
// execution.WithinTx transaction. A forced failure in the boundary append
// step (a deliberately conflicting pre-seeded message at the exact id the
// boundary would use) must leave NO trace of the epoch at all -- not even
// the row StartContextEpoch itself would otherwise have committed.
func TestCommitSummaryEpochIsAtomicAcrossFailure(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("atomic-commit-session")
	execution := testFencedExecutionStore(t, store, sessionID)
	ids := &sequenceIDs{}
	now := func() time.Time { return time.Unix(3000, 0).UTC() }
	epochCap := contextEpochCapability{sessionID: sessionID, runID: "run-1", store: store, execution: execution, ids: ids, now: now}

	epochID := ids.NewEpochID()
	boundaryIDs := compaction.BoundaryIDs{MessageID: ids.NewMessageID(), PartID: ids.NewPartID()}

	// Pre-seed a message at the boundary's own predicted id with a
	// MISMATCHED role: appendMessageLocked returns session.ErrConflict for
	// an id collision with a different role, deterministically forcing
	// AppendBoundaryTx's own internal AppendMessage call (which always uses
	// session.RoleSystem for the boundary message) to fail.
	if _, err := store.AppendMessage(context.Background(), session.Message{
		ID: boundaryIDs.MessageID, SessionID: sessionID, Role: session.RoleUser, CreatedAt: now(), UpdatedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}

	epoch := session.ContextEpoch{
		ID: epochID, SessionID: sessionID, SummarizedFromID: "seed-from", SummarizedToID: "seed-to",
		Trigger: "summarization", Reason: "context_budget", NextAction: session.EpochNextAutoContinue, CreatedAt: now(),
	}
	if _, _, err := epochCap.commitSummaryEpoch(context.Background(), epoch, boundaryIDs, "a summary"); err == nil {
		t.Fatal("commitSummaryEpoch succeeded despite a forced boundary-append conflict")
	}

	epochs, err := store.ListContextEpochs(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range epochs {
		if e.ID == epochID {
			t.Fatalf("StartContextEpoch's row survived a failed boundary append -- commitSummaryEpoch is not atomic: %+v", e)
		}
	}
}

// TestSummarizationFinalizeFailsClosedOnLengthMismatch is now (round-two W6
// review item 8) actually proving the no-source-correlation-capability
// case: a HandlerBuildContext with no sourceMessageID at all (the shape a
// non-summarization-Kind entry, or a bare unit test, would see) must fail
// Finalize closed rather than guess a correlation -- the positional
// "length mismatch" this test originally proved no longer exists as a
// failure mode, since correlation is per-message now, not positional.
func TestSummarizationFinalizeFailsClosedOnLengthMismatch(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("mismatch-session")
	execution := testFencedExecutionStore(t, store, sessionID)
	now := func() time.Time { return time.Unix(0, 0) }
	build := HandlerBuildContext{SessionID: sessionID, epochs: contextEpochCapability{sessionID: sessionID, store: store, execution: execution, ids: &sequenceIDs{}, now: now}}
	finalize := summarizationFinalize(build, 0)
	_, err := finalize(context.Background(), []*einoschema.AgenticMessage{einoschema.UserAgenticMessage("x")}, agenticAssistantText("summary"))
	if err == nil {
		t.Fatal("missing source correlation was accepted instead of failing closed")
	}
	// RW-S7 in the round-two W6 review: a correlation failure is a
	// configuration-shaped problem, not merely "adk adapter cannot
	// durably record this content block" -- both sentinels must classify
	// it, so neither an existing nor a new errors.Is check breaks.
	if !errors.Is(err, ErrHandlerConfiguration) {
		t.Fatalf("err = %v, want errors.Is(err, ErrHandlerConfiguration)", err)
	}
	if !errors.Is(err, errADKUnsupportedBlock) {
		t.Fatalf("err = %v, want errors.Is(err, errADKUnsupportedBlock) preserved", err)
	}
}

// TestHandlerConfigurationErrorClassifiesMissingBackendAndNoDeferredTools
// proves RW-S7's fix for the other two named sites: a missing required
// backend (errHandlerMissingBackend) and toolsearch's empty deferred-tool
// registry both classify as ErrHandlerConfiguration, alongside the
// preexisting errADKUnsupportedBlock classification.
func TestHandlerConfigurationErrorClassifiesMissingBackendAndNoDeferredTools(t *testing.T) {
	_, missingBackendErr := NewReductionHandlerFactory(ReductionConfig{})(context.Background(), HandlerBuildContext{})
	if !errors.Is(missingBackendErr, ErrHandlerConfiguration) || !errors.Is(missingBackendErr, errADKUnsupportedBlock) {
		t.Fatalf("missing-backend err = %v, want both ErrHandlerConfiguration and errADKUnsupportedBlock", missingBackendErr)
	}

	build := newTestWorkspaceHandlerBuildContext(t)
	build.DeferredTools = nil
	_, noDeferredErr := NewToolSearchHandlerFactory(ToolSearchHandlerConfig{})(context.Background(), build)
	if !errors.Is(noDeferredErr, ErrHandlerConfiguration) || !errors.Is(noDeferredErr, errADKUnsupportedBlock) {
		t.Fatalf("no-deferred-tools err = %v, want both ErrHandlerConfiguration and errADKUnsupportedBlock", noDeferredErr)
	}
}
