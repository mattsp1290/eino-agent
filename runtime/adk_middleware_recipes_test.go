package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

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
		durable = append(durable, msg)
	}
	execution := testFencedExecutionStore(t, store, sessionID)
	build := HandlerBuildContext{SessionID: sessionID, Store: store, Execution: execution, IDs: ids, Now: now}
	finalize := summarizationFinalize(build, 1)

	original := make([]*einoschema.AgenticMessage, len(durable))
	for i := range durable {
		original[i] = einoschema.UserAgenticMessage("msg")
	}
	summary := agenticAssistantText("compact summary of the conversation")

	result, err := finalize(context.Background(), original, summary)
	if err != nil {
		t.Fatalf("finalize error = %v", err)
	}
	if len(result) != 2 { // summary + 1 retained tail message
		t.Fatalf("finalize result length = %d, want 2", len(result))
	}

	epochs, err := store.ListContextEpochs(context.Background(), sessionID)
	if err != nil || len(epochs) != 1 {
		t.Fatalf("epochs = %+v, err = %v", epochs, err)
	}
	epoch := epochs[0]
	if epoch.SummarizedFromID != durable[0].ID || epoch.TailStartID != durable[len(durable)-1].ID {
		t.Fatalf("epoch boundaries = %+v", epoch)
	}
	if epoch.SummaryMessageID == "" {
		t.Fatal("epoch has no SummaryMessageID")
	}
}

func TestSummarizationFinalizeFailsClosedOnLengthMismatch(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("mismatch-session")
	execution := testFencedExecutionStore(t, store, sessionID)
	build := HandlerBuildContext{SessionID: sessionID, Store: store, Execution: execution, IDs: &sequenceIDs{}, Now: func() time.Time { return time.Unix(0, 0) }}
	finalize := summarizationFinalize(build, 0)
	if _, err := finalize(context.Background(), []*einoschema.AgenticMessage{einoschema.UserAgenticMessage("x")}, agenticAssistantText("summary")); err == nil {
		t.Fatal("length mismatch was accepted instead of failing closed")
	}
}
