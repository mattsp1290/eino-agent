package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	einomodel "github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

// W1 proof: a completed native MCP approval request pauses the typed agent at
// the model boundary before sibling local function calls execute. Resume
// dispatches exactly one continuation with the matching approval response;
// duplicate decisions cannot dispatch again.

func TestADKApprovalPausesBeforeSiblingFunctionCallsAndResumesOnce(t *testing.T) {
	t.Parallel()
	for _, variant := range []struct {
		decision  string
		streaming bool
	}{{"approve", false}, {"deny", false}, {"approve", true}} {
		decision, streaming := variant.decision, variant.streaming
		t.Run(decision+map[bool]string{false: "-generate", true: "-stream"}[streaming], func(t *testing.T) {
			t.Parallel()
			trace := &adkTrace{}
			dbPath := filepath.Join(t.TempDir(), "proof.db")
			checkpoints := newADKMemoryCheckpoints(trace)
			first := newADKProof(t, dbPath, adkProofOptions{trace: trace, lease: 50 * time.Millisecond})
			echo := proofTool("echo", first.countingExecutor("echo", nil), nil)
			first.tools, first.snapshot.Tools = []Tool{echo}, []Tool{echo}
			request := &schema.MCPToolApprovalRequest{ID: "apr-1", Name: "remote_write", Arguments: `{"path":"x"}`, ServerLabel: "srv"}
			scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
				agenticAssistant(agenticText("need approval"), schema.NewContentBlock(request), agenticCall("", "echo", `{"value":"sibling"}`)),
			}}
			ledger := first.ledgerModel(scripted)
			ledger.approval = &adkApprovalBinding{}
			runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: first.newAgent(t, ledger), CheckPointStore: checkpoints, EnableStreaming: streaming})
			drained := drainADKEvents(t, trace, runner.Run(first.ctx, first.userInput(), adk.WithCheckPointID("approval")))
			if len(drained.errs) != 0 || len(drained.interrupts) != 1 {
				t.Fatalf("errors=%v interrupts=%d\n%s", drained.errs, len(drained.interrupts), strings.Join(trace.list(), "\n"))
			}
			info, ok := drained.interrupts[0].Info.(*adkApprovalInfo)
			if !ok || info.ApprovalRequestID != "apr-1" {
				t.Fatalf("interrupt info = %#v", drained.interrupts[0].Info)
			}
			calls := first.toolCalls()
			if len(calls) != 1 || calls[0].Status != session.ToolCallPending || first.executions("echo") != 0 {
				t.Fatalf("sibling function call executed before approval: %+v executions=%d", calls, first.executions("echo"))
			}
			records := first.approvalRecords()
			if len(records) != 1 || records[0].Status != "pending" {
				t.Fatalf("approval records = %+v", records)
			}
			if len(first.modelRequests()) != 1 {
				t.Fatalf("ledger rows = %d", len(first.modelRequests()))
			}
			assertTraceOrder(t, trace, "adapter.commit", "ledger.completed", "approval.pause", "checkpoint.set approval", "event.interrupted", "iterator.drained")

			// An untargeted resume keeps the leaf paused without dispatching.
			iter, err := runner.Resume(first.ctx, "approval")
			if err != nil {
				t.Fatal(err)
			}
			untargeted := drainADKEvents(t, trace, iter)
			if len(untargeted.errs) != 0 || len(untargeted.interrupts) != 1 || scripted.calls.Load() != 1 || len(first.modelRequests()) != 1 {
				t.Fatalf("untargeted resume: errs=%v interrupts=%d calls=%d\n%s", untargeted.errs, len(untargeted.interrupts), scripted.calls.Load(), strings.Join(trace.list(), "\n"))
			}
			interruptID := untargeted.interrupts[0].ID
			runID := first.run.ID
			first.close()
			trace.add("process.restart")

			second := reopenADKProof(t, dbPath, runID, adkProofOptions{trace: trace, ids: &sequenceIDs{n: 1000}})
			defer second.close()
			echo2 := proofTool("echo", second.countingExecutor("echo", nil), nil)
			second.tools, second.snapshot.Tools = []Tool{echo2}, []Tool{echo2}
			continued := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{agenticAssistant(agenticText("continued after " + decision))}}
			ledger2 := second.ledgerModel(continued)
			ledger2.approval = &adkApprovalBinding{}
			runner2 := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: second.newAgent(t, ledger2), CheckPointStore: checkpoints, EnableStreaming: streaming})
			targets := &adk.ResumeParams{Targets: map[string]any{interruptID: decision}}
			iter, err = runner2.ResumeWithParams(second.ctx, "approval", targets)
			if err != nil {
				t.Fatal(err)
			}
			resumed := drainADKEvents(t, trace, iter)
			if len(resumed.errs) != 0 || len(resumed.interrupts) != 0 {
				t.Fatalf("resume errors=%v interrupts=%d\n%s", resumed.errs, len(resumed.interrupts), strings.Join(trace.list(), "\n"))
			}
			if text := assistantText(resumed.outputs[len(resumed.outputs)-1]); text != "continued after "+decision {
				t.Fatalf("final text = %q", text)
			}
			// Exactly one continuation dispatch with the exact native shape:
			// original input, committed approval-bearing transcript, then the
			// user-role approval response bound to the request ID.
			if continued.calls.Load() != 1 || len(second.modelRequests()) != 2 {
				t.Fatalf("continuation dispatches = %d ledger rows = %d", continued.calls.Load(), len(second.modelRequests()))
			}
			continuation := continued.requests[0]
			if len(continuation) != 3 {
				t.Fatalf("continuation messages = %d", len(continuation))
			}
			if continuation[0].Role != schema.AgenticRoleTypeUser || continuation[0].ContentBlocks[0].UserInputText.Text != "prove the boundary" {
				t.Fatalf("continuation[0] = %v", continuation[0])
			}
			transcript := continuation[1]
			if transcript.Role != schema.AgenticRoleTypeAssistant || approvalRequestBlock(transcript) == nil || approvalRequestBlock(transcript).ID != "apr-1" {
				t.Fatalf("continuation transcript = %v", transcript)
			}
			for _, block := range transcript.ContentBlocks {
				if block.Type == schema.ContentBlockTypeFunctionToolCall && block.FunctionToolCall.CallID != string(calls[0].ID) {
					t.Fatalf("transcript lost canonical call id: %q != %q", block.FunctionToolCall.CallID, calls[0].ID)
				}
			}
			response := continuation[2]
			if response.Role != schema.AgenticRoleTypeUser || len(response.ContentBlocks) != 1 || response.ContentBlocks[0].MCPToolApprovalResponse == nil {
				t.Fatalf("continuation response = %v", response)
			}
			if got := response.ContentBlocks[0].MCPToolApprovalResponse; got.ApprovalRequestID != "apr-1" || got.Approve != (decision == "approve") {
				t.Fatalf("approval response = %+v", got)
			}
			// The sibling function call was never executed or manufactured.
			after := second.toolCalls()
			if len(after) != 1 || after[0].Status != session.ToolCallPending || second.executions("echo") != 0 {
				t.Fatalf("sibling call after continuation = %+v executions=%d", after, second.executions("echo"))
			}
			records = second.approvalRecords()
			if len(records) != 1 || records[0].Status != "decided" || records[0].Decision != decision {
				t.Fatalf("approval records after decision = %+v", records)
			}
			kind := map[bool]string{false: "generate", true: "stream"}[streaming]
			assertTraceOrder(t, trace, "process.restart", "checkpoint.get approval", "approval.decided", "adapter."+kind+".begin", "provider.call 1", "adapter.commit")

			// A duplicate decision for the same paused request cannot dispatch.
			iter, err = runner2.ResumeWithParams(second.ctx, "approval", targets)
			if err != nil {
				t.Fatal(err)
			}
			duplicate := drainADKEvents(t, trace, iter)
			if len(duplicate.errs) != 1 || !strings.Contains(duplicate.errs[0].Error(), "already decided") {
				t.Fatalf("duplicate decision errors = %v", duplicate.errs)
			}
			if continued.calls.Load() != 1 || len(second.modelRequests()) != 2 {
				t.Fatalf("duplicate decision dispatched: calls=%d ledger=%d", continued.calls.Load(), len(second.modelRequests()))
			}
		})
	}
}

// interruptGuard refuses to retry or fail over a model-boundary interrupt.
// Upstream retry and failover only recognize graph-level interrupt errors
// (compose.ExtractInterruptInfo), not a StatefulInterrupt signal returned by
// the model itself, so the host-provided decision must guard it.
func interruptGuard(err error) bool {
	_, ok := compose.IsInterruptRerunError(err)
	return ok
}

func TestADKApprovalOnlyResponseStillPauses(t *testing.T) {
	t.Parallel()
	for _, variant := range []string{"plain", "retry", "failover", "unguarded-retry"} {
		t.Run(variant, func(t *testing.T) {
			t.Parallel()
			trace := &adkTrace{}
			checkpoints := newADKMemoryCheckpoints(trace)
			proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
			defer proof.close()
			scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
				agenticAssistant(schema.NewContentBlock(&schema.MCPToolApprovalRequest{ID: "apr-only", Name: "remote_read", ServerLabel: "srv"})),
				agenticAssistant(agenticText("finished")),
			}}
			ledger := proof.ledgerModel(scripted)
			ledger.approval = &adkApprovalBinding{}
			config := &adk.TypedChatModelAgentConfig[*schema.AgenticMessage]{Name: "proof", Model: ledger}
			switch variant {
			case "retry":
				config.ModelRetryConfig = &adk.TypedModelRetryConfig[*schema.AgenticMessage]{MaxRetries: 2, ShouldRetry: func(_ context.Context, retryCtx *adk.TypedRetryContext[*schema.AgenticMessage]) *adk.TypedRetryDecision[*schema.AgenticMessage] {
					return &adk.TypedRetryDecision[*schema.AgenticMessage]{Retry: retryCtx.Err != nil && !interruptGuard(retryCtx.Err)}
				}}
			case "failover":
				config.ModelFailoverConfig = &adk.ModelFailoverConfig[*schema.AgenticMessage]{
					MaxRetries: 1,
					ShouldFailover: func(_ context.Context, _ *schema.AgenticMessage, err error) bool {
						return err != nil && !interruptGuard(err)
					},
					GetFailoverModel: func(context.Context, *adk.FailoverContext[*schema.AgenticMessage]) (einomodel.BaseModel[*schema.AgenticMessage], []*schema.AgenticMessage, error) {
						return nil, nil, errors.New("failover must not be consulted for an interrupt")
					},
				}
			case "unguarded-retry":
				// Documented hazard: the deprecated IsRetryAble path retries the
				// pause as if it were a failure, re-dispatching the request.
				config.ModelRetryConfig = &adk.TypedModelRetryConfig[*schema.AgenticMessage]{MaxRetries: 1, IsRetryAble: func(context.Context, error) bool { return true }, BackoffFunc: func(context.Context, int) time.Duration { return 0 }}
			}
			agent, err := adk.NewTypedChatModelAgent[*schema.AgenticMessage](proof.ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: agent, CheckPointStore: checkpoints})
			drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput(), adk.WithCheckPointID("approval-only")))
			if variant == "unguarded-retry" {
				if len(drained.interrupts) != 0 || len(proof.modelRequests()) != 2 || scripted.calls.Load() != 2 {
					t.Fatalf("unguarded retry hazard changed: interrupts=%d ledger=%d calls=%d", len(drained.interrupts), len(proof.modelRequests()), scripted.calls.Load())
				}
				return
			}
			if len(drained.errs) != 0 || len(drained.interrupts) != 1 {
				t.Fatalf("approval-only response did not pause: errs=%v interrupts=%d\n%s", drained.errs, len(drained.interrupts), strings.Join(trace.list(), "\n"))
			}
			if len(drained.outputs) != 0 {
				t.Fatalf("approval-only response must not complete the agent normally: %d outputs", len(drained.outputs))
			}
			if len(proof.modelRequests()) != 1 || len(proof.approvalRecords()) != 1 || scripted.calls.Load() != 1 {
				t.Fatalf("wrapper duplicated the pause: ledger=%d approvals=%d calls=%d", len(proof.modelRequests()), len(proof.approvalRecords()), scripted.calls.Load())
			}
			iter, err := runner.ResumeWithParams(proof.ctx, "approval-only", &adk.ResumeParams{Targets: map[string]any{drained.interrupts[0].ID: "approve"}})
			if err != nil {
				t.Fatal(err)
			}
			resumed := drainADKEvents(t, trace, iter)
			if len(resumed.errs) != 0 || assistantText(resumed.outputs[len(resumed.outputs)-1]) != "finished" || len(proof.modelRequests()) != 2 {
				t.Fatalf("resume = errs %v outputs %d ledger %d", resumed.errs, len(resumed.outputs), len(proof.modelRequests()))
			}
		})
	}
}
