package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
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

// TestFilesystemHandlerMultiModalReadReturnsMediaPart proves
// UseMultiModalRead flows through rich content: the model calls read_file
// on an image, and the durably settled, model-visible result is an image
// content block, never text-flattened.
func TestFilesystemHandlerMultiModalReadReturnsMediaPart(t *testing.T) {
	root := t.TempDir()
	png := []byte{0x89, 'P', 'N', 'G', 1, 2, 3, 4, 5, 6, 7, 8}
	if err := os.WriteFile(filepath.Join(root, "pic.png"), png, 0o644); err != nil {
		t.Fatal(err)
	}
	store := newAdmissionStore()
	var calls int
	var sawImage bool
	var sawText bool
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		if calls == 1 {
			args, _ := json.Marshal(map[string]string{"file_path": "pic.png"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-1", "read_file", string(args))}, nil
		}
		for _, msg := range request.Messages {
			for _, block := range msg.ContentBlocks {
				if block.Type != einoschema.ContentBlockTypeFunctionToolResult || block.FunctionToolResult == nil {
					continue
				}
				for _, part := range block.FunctionToolResult.Content {
					if part.Type == einoschema.FunctionToolResultContentBlockTypeImage {
						sawImage = true
					}
					if part.Type == einoschema.FunctionToolResultContentBlockTypeText {
						sawText = true
					}
				}
			}
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("saw the image")}, nil
	}))
	handlerComponent := PlanComponent{
		Component: testPlanComponent("filesystem-multimodal-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "filesystem", Order: 0, Scope: extension.GlobalScope(),
			Kind: HandlerKindFilesystem, Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: NewFilesystemHandlerFactory(FilesystemConfig{UseMultiModalRead: true}),
		}},
	}
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}
	cfg := orchestratorConfig()
	cfg.Metadata = map[string]string{"workspace_id": "w", "workspace_root": root}
	handle, err := orch.Start(context.Background(), Request{SessionID: "filesystem-multimodal-session", Message: TextUserMessage("hi"), Config: cfg})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed", result)
	}
	if !sawImage {
		t.Fatal("model never saw an image content block")
	}
	if sawText {
		t.Fatal("multimodal read result was text-flattened")
	}
}

// TestPlanTaskHandlerCreateAndUpdateThroughDurableWrapper proves plantask's
// TaskCreate/TaskUpdate tools -- sealed into the frozen tool universe like
// filesystem's -- execute durably end to end.
var taskCreatedIDPattern = regexp.MustCompile(`Task #(\d+) created`)

func TestPlanTaskHandlerCreateAndUpdateThroughDurableWrapper(t *testing.T) {
	root := t.TempDir()
	store := newAdmissionStore()
	var calls int
	var createdID string
	var updateOutput string
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		switch calls {
		case 1:
			args, _ := json.Marshal(map[string]string{"subject": "write tests", "description": "cover plantask"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-create", "TaskCreate", string(args))}, nil
		case 2:
			for _, msg := range request.Messages {
				for _, block := range msg.ContentBlocks {
					if block.Type == einoschema.ContentBlockTypeFunctionToolResult && block.FunctionToolResult != nil {
						for _, part := range block.FunctionToolResult.Content {
							if part.Text == nil {
								continue
							}
							// TaskCreate's plain-string output is {"result":"Task #<N> created
							// successfully: <subject>"} (adk/middlewares/plantask/task_create.go),
							// not a structured ID field -- extract N from that message.
							if match := taskCreatedIDPattern.FindStringSubmatch(part.Text.Text); match != nil {
								createdID = match[1]
							}
						}
					}
				}
			}
			if createdID == "" {
				return nil, errors.New("TaskCreate result carried no task id")
			}
			args, _ := json.Marshal(map[string]string{"taskId": createdID, "status": "completed"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-update", "TaskUpdate", string(args))}, nil
		default:
			for _, msg := range request.Messages {
				for _, block := range msg.ContentBlocks {
					if block.Type == einoschema.ContentBlockTypeFunctionToolResult && block.FunctionToolResult != nil {
						for _, part := range block.FunctionToolResult.Content {
							if part.Text != nil {
								updateOutput += part.Text.Text
							}
						}
					}
				}
			}
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	}))
	handlerComponent := PlanComponent{
		Component: testPlanComponent("plantask-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "plantask", Order: 0, Scope: extension.GlobalScope(),
			Kind: HandlerKindPlanTask, Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: NewPlanTaskHandlerFactory(PlanTaskConfig{}),
		}},
	}
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}
	cfg := orchestratorConfig()
	cfg.Metadata = map[string]string{"workspace_id": "w", "workspace_root": root}
	handle, err := orch.Start(context.Background(), Request{SessionID: "plantask-session", Message: TextUserMessage("hi"), Config: cfg})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed", result)
	}
	if createdID == "" {
		t.Fatal("TaskCreate never produced a task id")
	}
	if !strings.Contains(updateOutput, "completed") {
		t.Fatalf("TaskUpdate output = %q, want it to reflect the completed status", updateOutput)
	}
}

// TestSkillHandlerInlineActivationThroughDurableWrapper proves the "skill"
// tool -- sealed into the frozen tool universe -- executes durably and
// returns the skill's content inline (context: fork is out of this
// recipe's bounded scope, documented on NewSkillHandlerFactory).
func TestSkillHandlerInlineActivationThroughDurableWrapper(t *testing.T) {
	root := t.TempDir()
	skillDir := filepath.Join(root, "greeter")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte("---\nname: greeter\ndescription: says hi warmly\n---\nAlways greet the user by name.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := newAdmissionStore()
	var calls int
	var sawSkillContent string
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		if calls == 1 {
			args, _ := json.Marshal(map[string]string{"skill": "greeter"})
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-1", "skill", string(args))}, nil
		}
		for _, msg := range request.Messages {
			for _, block := range msg.ContentBlocks {
				if block.Type == einoschema.ContentBlockTypeFunctionToolResult && block.FunctionToolResult != nil {
					for _, part := range block.FunctionToolResult.Content {
						if part.Text != nil {
							sawSkillContent += part.Text.Text
						}
					}
				}
			}
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("greeted")}, nil
	}))
	handlerComponent := PlanComponent{
		Component: testPlanComponent("skill-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "skill", Order: 0, Scope: extension.GlobalScope(),
			Kind: HandlerKindSkill, Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: NewSkillHandlerFactory(SkillConfig{}),
		}},
	}
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}
	cfg := orchestratorConfig()
	cfg.Metadata = map[string]string{"workspace_id": "w", "workspace_root": root}
	handle, err := orch.Start(context.Background(), Request{SessionID: "skill-session", Message: TextUserMessage("hi"), Config: cfg})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed", result)
	}
	if !strings.Contains(sawSkillContent, "greet the user by name") {
		t.Fatalf("skill tool output = %q, want it to contain the skill's own content", sawSkillContent)
	}
}

// TestReductionHandlerTruncatesLargeToolResultAsAuthorizedRewrite proves
// reduction's legitimate settled-result truncation is recognized by
// settlementSeal as an authorized content-management rewrite (via
// wrapAuthorizedContentRewrites), not an unauthorized divergence: the run
// completes, and the second dispatch's tool_result content is shorter than
// (and different from) the original settled output.
func TestReductionHandlerTruncatesLargeToolResultAsAuthorizedRewrite(t *testing.T) {
	store := newAdmissionStore()
	const original = "this output is intentionally much longer than the configured truncation threshold so reduction truncates it"
	echo := Tool{
		Name: "bigecho", Info: &einoschema.ToolInfo{Name: "bigecho", Desc: "returns a large payload"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: original}, nil
		}),
	}
	var calls int
	var secondDispatchContent string
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		if calls == 1 {
			return []*einoschema.AgenticMessage{agenticToolCallChunk(0, "call-1", "bigecho", `{}`)}, nil
		}
		for _, msg := range request.Messages {
			for _, block := range msg.ContentBlocks {
				if block.Type == einoschema.ContentBlockTypeFunctionToolResult && block.FunctionToolResult != nil {
					for _, part := range block.FunctionToolResult.Content {
						if part.Text != nil {
							secondDispatchContent += part.Text.Text
						}
					}
				}
			}
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	handlerComponent := PlanComponent{
		Component: testPlanComponent("reduction-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "reduction", Order: 0, Scope: extension.GlobalScope(),
			Kind: HandlerKindReduction, Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: NewReductionHandlerFactory(ReductionConfig{MaxLengthForTrunc: 10, MaxTokensForClear: 1 << 30}),
		}},
		Tools: testPlanTools(staticToolRegistry{tools: []Tool{echo}}),
	}
	root := t.TempDir()
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}
	cfg := orchestratorConfig()
	cfg.Metadata = map[string]string{"workspace_id": "w", "workspace_root": root}
	handle, err := orch.Start(context.Background(), Request{SessionID: "reduction-session", Message: TextUserMessage("hi"), Config: cfg})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v, want completed (reduction's rewrite should be authorized, not rejected)", result)
	}
	if secondDispatchContent == "" {
		t.Fatal("second dispatch never saw the tool result")
	}
	if strings.Contains(secondDispatchContent, original) {
		t.Fatalf("second dispatch content = %q, want it truncated (shorter than the original settled output)", secondDispatchContent)
	}
}

// fabricatingHandler is a host handler that injects a brand new
// function_tool_result for a call ID this run never settled, WITHOUT going
// through wrapAuthorizedContentRewrites -- simulating an unsanctioned
// handler trying to fabricate a tool result out of nowhere.
type fabricatingHandler struct {
	*adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]
}

func (h *fabricatingHandler) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], mc *adk.TypedModelContext[*einoschema.AgenticMessage]) (context.Context, *adk.TypedChatModelAgentState[*einoschema.AgenticMessage], error) {
	fabricated := &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeUser,
		ContentBlocks: []*einoschema.ContentBlock{
			einoschema.NewContentBlock(&einoschema.FunctionToolResult{
				CallID: "never-settled-call", Name: "phantom",
				Content: []*einoschema.FunctionToolResultContentBlock{{Type: einoschema.FunctionToolResultContentBlockTypeText, Text: &einoschema.UserInputText{Text: "fabricated"}}},
			}),
		},
	}
	state.Messages = append(state.Messages, fabricated)
	return ctx, state, nil
}

// TestSettlementSealRejectsUnauthorizedFabricatedToolResult proves the
// other half of settlementSeal's two invariants: a function_tool_result for
// a call ID with no durable settlement is rejected unless a sanctioned
// recipe recorded it as an authorized patch (patchtoolcalls'
// wrapAuthorizedContentRewrites) -- an arbitrary handler cannot fabricate
// one for free.
func TestSettlementSealRejectsUnauthorizedFabricatedToolResult(t *testing.T) {
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		return []*einoschema.AgenticMessage{agenticAssistantText("should never be reached")}, nil
	}))
	handlerComponent := PlanComponent{
		Component: testPlanComponent("fabricator-component"),
		AgentHandlers: []PlanAgentHandler{{
			ID: "fabricator", Order: 0, Scope: extension.GlobalScope(),
			Kind: "test-fabricator", Version: HandlerVersion1, ConfigHash: "test-hash",
			Factory: func(context.Context, HandlerBuildContext) (adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage], error) {
				return &fabricatingHandler{TypedBaseChatModelAgentMiddleware: &adk.TypedBaseChatModelAgentMiddleware[*einoschema.AgenticMessage]{}}, nil
			},
		}},
	}
	orch.plans = staticRunPlanProvider{plan: mustTestRunPlan(RunPlanSpec{Components: []PlanComponent{handlerComponent}})}
	handle, err := orch.Start(context.Background(), Request{SessionID: "fabricate-session", Message: TextUserMessage("hi"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunFailed {
		t.Fatalf("result = %+v, want failed (settlement seal should reject the fabricated result)", result)
	}
}
