package runtime

import (
	"context"
	"path/filepath"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// TestApprovalPausesBeforeSiblingToolExecutionAndResumesViaProductionAgent
// proves the native MCP approval pause (runtime/adk_approval.go) end to end
// against the production stack: a real adk.NewTypedChatModelAgent
// (DefaultChatModelAgentFactory), the production TurnLoop/adkCheckpointStore,
// and StreamingOrchestrator.Start/ResumeRun -- not the deleted W1 proof
// scaffolding (adk_approval_proof_test.go/adk_approval_test.go), which this
// replaces.
//
// Every physical dispatch in production now requests streaming
// (turnLoopCoordinator.genInput/genResume set TypedAgentInput.EnableStreaming:
// true -- see runtime/turn_loop.go and
// TestPublicSessionWatchConstructionExecutionAndReopen, which needs live
// per-chunk delta publishing that only adkModel.Stream wires up), and ADK
// selects Stream vs. Generate once per run from that flag for every node in
// the composed graph, including tool-loop continuations -- so
// adkModel.Generate is not reachable through this entry point in production
// at all. adkModel.Generate and adkModel.Stream share the same
// approval.prepare/pause, durableProjection, begin and commit call sequence
// (see adk_model.go); this test's coverage of that shared sequence through
// Stream is what Generate would also run, and Generate has no additional or
// divergent approval-specific logic of its own to prove independently.
func TestApprovalPausesBeforeSiblingToolExecutionAndResumesViaProductionAgent(t *testing.T) {
	for _, decision := range []string{"approve", "deny"} {
		t.Run(decision, func(t *testing.T) {
			store := newAdmissionStore()
			var executions int
			echo := Tool{
				Name: "echo", Info: &einoschema.ToolInfo{Name: "echo", Desc: "echo"},
				Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
					executions++
					return ToolResult{Output: "executed"}, nil
				}),
			}
			var calls int
			orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
				calls++
				if calls == 1 {
					// A mixed result: an approval request alongside a sibling
					// function call, exactly the case the pause must catch
					// before ADK's tools node ever dispatches the sibling.
					return []*einoschema.AgenticMessage{{
						Role: einoschema.AgenticRoleTypeAssistant,
						ContentBlocks: []*einoschema.ContentBlock{
							{Type: einoschema.ContentBlockTypeAssistantGenText, AssistantGenText: &einoschema.AssistantGenText{Text: "need approval"}},
							{Type: einoschema.ContentBlockTypeMCPToolApprovalRequest, MCPToolApprovalRequest: &einoschema.MCPToolApprovalRequest{ID: "apr-1", Name: "remote_write", ServerLabel: "srv", Arguments: `{"path":"x"}`}},
							{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &einoschema.FunctionToolCall{CallID: "call-sibling", Name: "echo", Arguments: `{}`}},
						},
					}}, nil
				}
				return []*einoschema.AgenticMessage{agenticAssistantText("continued after " + decision)}, nil
			}))
			configureTestTools(orch, staticToolRegistry{tools: []Tool{echo}})

			handle, err := orch.Start(context.Background(), Request{
				SessionID: session.ID("approval-session-" + decision), Message: TextUserMessage("hello"), Config: orchestratorConfig(),
			})
			if err != nil {
				t.Fatalf("Start error = %v", err)
			}
			result := <-handle.Done()
			if result.Status != session.RunPaused || !result.Interrupted {
				t.Fatalf("result = %+v", result)
			}
			pause, ok := <-handle.AwaitPause()
			if !ok || len(pause.InterruptContexts) != 1 {
				t.Fatalf("pause = %+v, ok=%v", pause, ok)
			}
			if executions != 0 {
				t.Fatalf("sibling tool executed before approval decision: executions=%d", executions)
			}
			// The sibling call is permanently orphaned by the approval
			// pause (ADK's tools node never dispatches it -- see
			// adkApprovalBinding.pause's doc comment) and is terminalized
			// interrupted immediately, not left pending forever (which
			// would also block this run from ever settling, since SettleRun
			// refuses any non-terminal tool call).
			// The scripted provider CallID ("call-sibling") is preserved
			// separately as ProviderCallID; the durable ID is always a fresh
			// mint now, so discover it from the store instead.
			siblingCallID := onlyToolCallID(t, store)
			siblingCall, err := store.GetToolCall(context.Background(), siblingCallID)
			if err != nil || siblingCall.Name != "echo" || siblingCall.Status != session.ToolCallInterrupted {
				t.Fatalf("sibling tool call = %+v, err=%v", siblingCall, err)
			}
			run, err := store.GetRun(context.Background(), result.RunID)
			if err != nil || run.Status != session.RunPaused {
				t.Fatalf("run = %+v, err=%v", run, err)
			}

			// An untargeted resume keeps the leaf paused without dispatching
			// a new physical call, but it re-pauses at a new checkpoint
			// generation with its own fresh InterruptCtx.ID (the original
			// pause.InterruptContexts[0].ID is now stale -- a stale target on
			// the next resume would be a silent no-op re-pause, not a real
			// error, per the W5 doc's resume-address-portability finding).
			untargetedHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{})
			if err != nil {
				t.Fatalf("untargeted ResumeRun error = %v", err)
			}
			untargeted := <-untargetedHandle.Done()
			if untargeted.Status != session.RunPaused || !untargeted.Interrupted || calls != 1 {
				t.Fatalf("untargeted resume result = %+v calls=%d", untargeted, calls)
			}
			repause, ok := <-untargetedHandle.AwaitPause()
			if !ok || len(repause.InterruptContexts) != 1 {
				t.Fatalf("repause = %+v, ok=%v", repause, ok)
			}

			resumeHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{
				Targets: map[string]any{repause.InterruptContexts[0].ID: decision},
			})
			if err != nil {
				t.Fatalf("ResumeRun error = %v", err)
			}
			resumed := <-resumeHandle.Done()
			if resumed.Status != session.RunCompleted || resumed.Error != nil {
				t.Fatalf("resumed result = %+v", resumed)
			}
			if calls != 2 {
				t.Fatalf("continuation dispatches = %d, want 2", calls)
			}
			finalRun, err := store.GetRun(context.Background(), result.RunID)
			if err != nil || finalRun.Status != session.RunCompleted {
				t.Fatalf("final run = %+v, err=%v", finalRun, err)
			}
			batch, err := store.ListMessages(context.Background(), session.ID("approval-session-"+decision), session.ReplayCursor{Limit: 100})
			if err != nil {
				t.Fatalf("ListMessages error = %v", err)
			}
			roleByMessage := make(map[session.MessageID]session.Role, len(batch.Messages))
			for _, message := range batch.Messages {
				roleByMessage[message.ID] = message.Role
			}
			var sawRequest, sawResponse bool
			for _, part := range batch.Parts {
				content, err := session.DecodeContentParts(roleByMessage[part.MessageID], []session.Part{part}, session.DefaultContentLimits())
				if err != nil {
					continue
				}
				for _, block := range content.Blocks {
					if block.Kind == session.BlockKindMCPToolApprovalRequest && block.MCPApprovalRequest != nil && block.MCPApprovalRequest.ID == "apr-1" {
						sawRequest = true
					}
					if block.Kind == session.BlockKindMCPToolApprovalResponse && block.MCPApprovalResponse != nil {
						if block.MCPApprovalResponse.ApprovalRequestID != "apr-1" || block.MCPApprovalResponse.Approve != (decision == "approve") {
							t.Fatalf("committed approval response = %+v", block.MCPApprovalResponse)
						}
						sawResponse = true
					}
				}
			}
			if !sawRequest || !sawResponse {
				t.Fatalf("durable approval request/response not both found: request=%v response=%v", sawRequest, sawResponse)
			}

			// A duplicate decision for the same already-decided request must
			// not dispatch again.
			duplicateHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{
				Targets: map[string]any{pause.InterruptContexts[0].ID: decision},
			})
			if err == nil {
				duplicate := <-duplicateHandle.Done()
				if duplicate.Error == nil {
					t.Fatalf("duplicate decision unexpectedly succeeded: %+v", duplicate)
				}
			}
			if calls != 2 {
				t.Fatalf("duplicate decision dispatched again: calls=%d", calls)
			}
		})
	}
}

// TestApprovalOnlyResponseStillPausesViaProductionAgent proves an
// approval-only result (no sibling function calls at all) also pauses and
// resumes correctly through the production stack.
func TestApprovalOnlyResponseStillPausesViaProductionAgent(t *testing.T) {
	store := newAdmissionStore()
	var calls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		if calls == 1 {
			return []*einoschema.AgenticMessage{{
				Role:          einoschema.AgenticRoleTypeAssistant,
				ContentBlocks: []*einoschema.ContentBlock{{Type: einoschema.ContentBlockTypeMCPToolApprovalRequest, MCPToolApprovalRequest: &einoschema.MCPToolApprovalRequest{ID: "apr-only", Name: "remote_read", ServerLabel: "srv"}}},
			}}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("finished")}, nil
	}))

	handle, err := orch.Start(context.Background(), Request{SessionID: "approval-only-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunPaused || !result.Interrupted {
		t.Fatalf("result = %+v", result)
	}
	pause, ok := <-handle.AwaitPause()
	if !ok || len(pause.InterruptContexts) != 1 {
		t.Fatalf("pause = %+v, ok=%v", pause, ok)
	}
	resumeHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil || calls != 2 {
		t.Fatalf("resumed result = %+v calls=%d", resumed, calls)
	}
}

// TestApprovalDecisionPartRoundTripsThroughRealSQLiteStore proves that
// adkApprovalBinding's runtime-private session.PartApprovalDecision
// decision-CAS record (adk_approval.go's pause/commitResponse) round-trips
// through a real SQLite store, not just the in-memory admissionStore fake
// every other approval production test uses. This is the store-level
// complement to storetest's
// "compaction and approval_decision parts are accepted by the store" case:
// that case pins the Go PartKind constant against the sqlite/postgres
// `parts.kind` CHECK constraint with a hand-built part; this test pins that
// the runtime's actual approval write path produces a part the same CHECK
// constraint (and the store's insert/replay path generally) accepts.
func TestApprovalDecisionPartRoundTripsThroughRealSQLiteStore(t *testing.T) {
	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "approval-decision.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer func() { _ = storePool.Close() }()

	var calls int
	orch := newTestOrchestrator(newAdmissionStore(), scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		if calls == 1 {
			return []*einoschema.AgenticMessage{{
				Role:          einoschema.AgenticRoleTypeAssistant,
				ContentBlocks: []*einoschema.ContentBlock{{Type: einoschema.ContentBlockTypeMCPToolApprovalRequest, MCPToolApprovalRequest: &einoschema.MCPToolApprovalRequest{ID: "apr-sqlite", Name: "remote_read", ServerLabel: "srv"}}},
			}}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("finished")}, nil
	}), WithStore(store))

	handle, err := orch.Start(ctx, Request{SessionID: "approval-sqlite-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunPaused || !result.Interrupted {
		t.Fatalf("result = %+v", result)
	}
	pause, ok := <-handle.AwaitPause()
	if !ok || len(pause.InterruptContexts) != 1 {
		t.Fatalf("pause = %+v, ok=%v", pause, ok)
	}
	resumeHandle, err := orch.ResumeRun(ctx, result.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil || calls != 2 {
		t.Fatalf("resumed result = %+v calls=%d", resumed, calls)
	}

	batch, err := store.ListMessages(ctx, "approval-sqlite-session", session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatalf("ListMessages error = %v", err)
	}
	var approvalDecisionParts int
	for _, part := range batch.Parts {
		if part.Kind == session.PartApprovalDecision {
			approvalDecisionParts++
			if len(part.Payload) == 0 {
				t.Fatalf("approval_decision part %s has empty payload", part.ID)
			}
		}
	}
	if approvalDecisionParts == 0 {
		t.Fatal("no approval_decision part was durably written to the real SQLite store")
	}
}
