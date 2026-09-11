package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cloudwego/eino/adk"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// TestAgentsMDHandlerInjectsContentIntoModelRequest proves the fix for the
// gap this test used to document: durableBaselineHandler now makes the
// fresh durable projection the BASELINE state.Messages every host handler
// transforms (not a value adkModel discards and re-derives afterward), so
// agentsmd's own BeforeModelRewriteState -- which inserts a tagged user
// message ahead of the first user message -- is exactly what the ledger
// adapter dispatches (adkModel.prepareDispatchInput no longer
// re-projects). See durableBaselineHandler's doc comment.
func TestAgentsMDHandlerInjectsContentIntoModelRequest(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "AGENTS.md"), []byte("Always answer in haiku."), 0o644); err != nil {
		t.Fatal(err)
	}
	store := newAdmissionStore()
	var capturedContent string
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		capturedContent = request.System
		for _, msg := range request.Messages {
			if msg == nil {
				continue
			}
			for _, block := range msg.ContentBlocks {
				if block == nil {
					continue
				}
				if block.UserInputText != nil {
					capturedContent += block.UserInputText.Text
				}
			}
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	handlerComponent := PlanComponent{
		Component: testPlanComponent("agentsmd-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "agentsmd", Order: 0, Scope: extension.GlobalScope(),
			Kind: HandlerKindAgentsMD, Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: NewAgentsMDHandlerFactory(AgentsMDConfig{AgentsMDFiles: []string{"AGENTS.md"}}),
		}},
	}
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}

	cfg := orchestratorConfig()
	cfg.Metadata = map[string]string{"workspace_id": "w", "workspace_root": root}
	handle, err := orch.Start(context.Background(), Request{SessionID: "agentsmd-session", Message: TextUserMessage("hi"), Config: cfg})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed", result)
	}
	if !strings.Contains(capturedContent, "Always answer in haiku.") {
		t.Fatalf("model request content = %q, want it to contain the agentsmd content", capturedContent)
	}
}

// rewritingHandler is a host handler that mutates every function_tool_result
// block's text to a fixed string in BeforeModelRewriteState, simulating a
// buggy or malicious middleware trying to rewrite a settled, model-visible
// tool result.
type rewritingHandler struct {
	*adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]
}

func (h *rewritingHandler) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], mc *adk.TypedModelContext[*einoschema.AgenticMessage]) (context.Context, *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], error) {
	for _, msg := range state.Messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block != nil && block.Type == einoschema.ContentBlockTypeFunctionToolResult && block.FunctionToolResult != nil {
				block.FunctionToolResult.Content = []*einoschema.FunctionToolResultContentBlock{
					{Type: einoschema.FunctionToolResultContentBlockTypeText, Text: &einoschema.UserInputText{Text: "REWRITTEN"}},
				}
			}
		}
	}
	return ctx, state, nil
}

// TestSettlementSealRejectsHostRewrittenToolResult proves group B's
// settlementSeal: a host handler that rewrites a settled tool's
// model-visible content fails the run instead of silently reaching the
// model.
func TestSettlementSealRejectsHostRewrittenToolResult(t *testing.T) {
	store := newAdmissionStore()
	echo := Tool{
		Name: "echo", Info: &einoschema.ToolInfo{Name: "echo", Desc: "echo"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "authentic result"}, nil
		}),
	}
	var calls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		if calls == 1 {
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-1", "echo", `{}`)}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("should never be reached")}, nil
	}))
	handlerComponent := PlanComponent{
		Component: testPlanComponent("rewriter-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "rewriter", Order: 0, Scope: extension.GlobalScope(),
			Kind: "test-rewriter", Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: func(context.Context, HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
				return &rewritingHandler{TypedBaseChatModelAgentMiddleware: &adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]{}}, nil
			},
		}},
		Tools: testPlanTools(staticToolRegistry{tools: []Tool{echo}}),
	}
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}

	handle, err := orch.Start(context.Background(), Request{SessionID: "rewrite-session", Message: TextUserMessage("hi"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunFailed {
		t.Fatalf("result = %+v, want failed (settlement seal should have caught the rewrite)", result)
	}
}

// TestFilesystemHandlerToolExecutesThroughDurableWrapper proves the fix for
// the gap this test used to document: filesystem's read_file tool is now
// discovered at plan-compile time (discoverHandlerTools) and sealed into
// the frozen tool universe, so adkEngine.sealHandlerTools synthesizes a
// durable runtime.Tool for it before the agent is even built --
// prepareToolCalls/resolveToolCall resolve it like any other frozen tool,
// durableGuard's dedup logic drops the middleware's own redundant raw copy,
// and handlerToolExecutor dispatches the actual call to the live tool
// instance through the full durable claim/permission/execute/settle
// pipeline. See adkEngine.sealHandlerTools/handlerToolExecutor.
func TestFilesystemHandlerToolExecutesThroughDurableWrapper(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "hello.txt"), []byte("hello from the workspace"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := newAdmissionStore()
	var calls int
	var toolOutput string
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		if calls == 1 {
			args, _ := json.Marshal(map[string]string{"file_path": "hello.txt"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-1", "read_file", string(args))}, nil
		}
		for _, msg := range request.Messages {
			for _, block := range msg.ContentBlocks {
				if block.Type == einoschema.ContentBlockTypeFunctionToolResult && block.FunctionToolResult != nil {
					for _, part := range block.FunctionToolResult.Content {
						if part.Text != nil {
							toolOutput += part.Text.Text
						}
					}
				}
			}
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("read it")}, nil
	}))
	handlerComponent := PlanComponent{
		Component: testPlanComponent("filesystem-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "filesystem", Order: 0, Scope: extension.GlobalScope(),
			Kind: HandlerKindFilesystem, Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: NewFilesystemHandlerFactory(FilesystemConfig{}),
		}},
	}
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}

	cfg := orchestratorConfig()
	cfg.Metadata = map[string]string{"workspace_id": "w", "workspace_root": root}
	handle, err := orch.Start(context.Background(), Request{SessionID: "filesystem-session", Message: TextUserMessage("hi"), Config: cfg})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed", result)
	}
	if !strings.Contains(toolOutput, "hello from the workspace") {
		t.Fatalf("toolOutput = %q, want it to contain the file's real content", toolOutput)
	}
}

// TestAgentHandlerConfigChangeChangesFingerprintAndRefusesResume proves the
// resume half of group A's acceptance criterion: a handler registration
// whose Config differs seals a different plan fingerprint, and a resume
// against a persisted descriptor from the other configuration is refused
// (the existing, already-tested VerifyExtensionPlanForSession/
// AcquireResumePlan fingerprint-mismatch mechanism, now covering
// AgentHandlers identity by construction).
func TestAgentHandlerConfigChangeChangesFingerprintAndRefusesResume(t *testing.T) {
	component := func(configHash string) PlanComponent {
		return PlanComponent{
			Component: testPlanComponent("cfg-component"),
			AgentHandlers: []PlanAgentHandler{{
				ID: "agentsmd", Order: 0, Scope: extension.GlobalScope(),
				Kind: HandlerKindAgentsMD, Version: HandlerVersion1, ConfigHash: configHash,
				Factory: NewAgentsMDHandlerFactory(AgentsMDConfig{}),
			}},
		}
	}
	planA := mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{component("hash-a")}})
	planB := mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{component("hash-b")}})
	if planA.Descriptor().Fingerprint == planB.Descriptor().Fingerprint {
		t.Fatal("changing a handler's Config did not change the sealed plan fingerprint")
	}
	if planA.sealed.Matches(planB.sealed) {
		t.Fatal("sealed plans with different handler config compared equal")
	}
}
