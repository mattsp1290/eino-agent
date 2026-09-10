package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
)

// W1 proof: a real typed ADK runner drives a real TypedChatModelAgent whose
// model and tools are the mandatory runtime adapters. Every physical model
// call is a ledger row and every function execution passes claim/settlement
// under the run fence; ADK observes only committed facts.

func TestADKBoundaryFreshRunInterceptsEveryModelAndToolCall(t *testing.T) {
	t.Parallel()
	for _, streaming := range []bool{false, true} {
		t.Run(map[bool]string{false: "generate", true: "stream"}[streaming], func(t *testing.T) {
			t.Parallel()
			trace := &adkTrace{}
			proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
			defer proof.close()
			echo := proofTool("echo", proof.countingExecutor("echo", func(call ToolCall) (ToolResult, error) {
				var input struct{ Value string }
				_ = json.Unmarshal(call.Input, &input)
				return ToolResult{Output: "echo:" + input.Value}, nil
			}), map[string]string{"retry_safe": "true"})
			proof.tools = []Tool{echo}
			proof.snapshot.Tools = []Tool{echo}

			scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
				agenticAssistant(agenticText("calling the tool"), agenticCall("", "echo", `{"value": "a"}`)),
				agenticAssistant(agenticText("done")),
			}}
			ledger := proof.ledgerModel(scripted)
			runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: proof.newAgent(t, ledger), EnableStreaming: streaming})
			drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput()))
			if len(drained.errs) != 0 {
				t.Fatalf("events returned errors: %v\n%s", drained.errs, strings.Join(trace.list(), "\n"))
			}

			// Exactly one ledger row per physical dispatch, each completed.
			records := proof.modelRequests()
			if len(records) != 2 || scripted.calls.Load() != 2 {
				t.Fatalf("ledger rows = %d, provider calls = %d", len(records), scripted.calls.Load())
			}
			for _, record := range records {
				if record.State != session.ModelRequestCompleted {
					t.Fatalf("ledger row %s state = %s", record.ID, record.State)
				}
			}
			if records[0].ID == records[1].ID || records[0].AssistantMessageID == records[1].AssistantMessageID {
				t.Fatalf("ledger identities collide: %+v", records)
			}
			// The second request carries the first committed assistant message
			// with the canonical call identity plus the settled tool output.
			second := scripted.requests[1]
			if len(second) != 3 {
				t.Fatalf("second request messages = %d", len(second))
			}
			calls := proof.toolCalls()
			if len(calls) != 1 || calls[0].Status != session.ToolCallCompleted || proof.executions("echo") != 1 {
				t.Fatalf("tool calls = %+v executions = %d", calls, proof.executions("echo"))
			}
			var callID string
			for _, block := range second[1].ContentBlocks {
				if block.Type == schema.ContentBlockTypeFunctionToolCall {
					callID = block.FunctionToolCall.CallID
					if block.FunctionToolCall.Arguments != `{"value":"a"}` {
						t.Fatalf("model-visible arguments were not canonicalized: %q", block.FunctionToolCall.Arguments)
					}
				}
			}
			if callID != string(calls[0].ID) {
				t.Fatalf("model-visible call id %q != durable %q", callID, calls[0].ID)
			}
			resultID, resultText := functionResultText(second[2])
			if resultID != callID || resultText != string(calls[0].Output) {
				t.Fatalf("model-visible result (%q, %q) != settled output %q", resultID, resultText, calls[0].Output)
			}
			var output ToolOutput
			if err := json.Unmarshal(calls[0].Output, &output); err != nil || output.Content != "echo:a" || output.Status != "completed" {
				t.Fatalf("settled output = %s (%v)", calls[0].Output, err)
			}
			// ADK's tool result event observes the settled output too.
			var eventResult string
			for _, out := range drained.outputs {
				if id, text := functionResultText(out); id != "" {
					eventResult = text
				}
			}
			if eventResult != string(calls[0].Output) {
				t.Fatalf("event result %q != settled %q", eventResult, calls[0].Output)
			}
			// Invocation order: ledger row before provider dispatch, commit before
			// the model event, claim before executor, settlement before the tool
			// event and before the next dispatch.
			kind := map[bool]string{false: "generate", true: "stream"}[streaming]
			assertTraceOrder(t, trace,
				"adapter."+kind+".begin call=1", "ledger.dispatch_started", "provider.call 1", "adapter.commit", "ledger.completed",
				"event.output role=assistant", "tool.claimed", "executor.echo", "tool.settled",
				"adapter."+kind+".begin call=2", "provider.call 2", "iterator.drained")
			// The tool result event is delivered asynchronously, but always after
			// settlement.
			assertTraceOrder(t, trace, "tool.settled", "event.output role=user")
			if streaming {
				if trace.count("adapter.stream.chunk") < 2 {
					t.Fatalf("expected concatenated stream chunks:\n%s", strings.Join(trace.list(), "\n"))
				}
			}
			if text := assistantText(drained.outputs[len(drained.outputs)-1]); text != "done" {
				t.Fatalf("final text = %q", text)
			}
			// Durable parts: text part for each assistant message, one tool call
			// part, one tool result part.
			batch, err := proof.store.ListMessages(proof.ctx, proof.run.SessionID, session.ReplayCursor{Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			kinds := map[session.PartKind]int{}
			for _, part := range batch.Parts {
				kinds[part.Kind]++
			}
			if kinds[session.PartText] != 2 || kinds[session.PartUserInputText] != 1 || kinds[session.PartToolCall] != 1 || kinds[session.PartToolResult] != 1 {
				t.Fatalf("durable parts = %v", kinds)
			}
		})
	}
}

func assertTraceOrder(t testing.TB, trace *adkTrace, needles ...string) {
	t.Helper()
	entries := trace.list()
	position := 0
	for _, needle := range needles {
		found := -1
		for index := position; index < len(entries); index++ {
			if strings.Contains(entries[index], needle) {
				found = index
				break
			}
		}
		if found < 0 {
			t.Fatalf("trace entry %q not found after position %d:\n%s", needle, position, strings.Join(entries, "\n"))
		}
		position = found + 1
	}
}

func TestADKBoundaryModelFailureIsLedgeredWithoutToolRows(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
	defer proof.close()
	failure := model.Error{Code: "provider_rejected", Message: "scripted failure", Cause: model.ErrProviderRejected}
	scripted := &adkScriptedModel{trace: trace, errs: []error{failure}}
	runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: proof.newAgent(t, proof.ledgerModel(scripted))})
	drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput()))
	if len(drained.errs) != 1 || !errors.Is(drained.errs[0], model.ErrProviderRejected) {
		t.Fatalf("errors = %v", drained.errs)
	}
	records := proof.modelRequests()
	if len(records) != 1 || records[0].State != session.ModelRequestFailed || records[0].ErrorCode == "" {
		t.Fatalf("ledger = %+v", records)
	}
	if calls := proof.toolCalls(); len(calls) != 0 {
		t.Fatalf("unexpected tool rows %+v", calls)
	}
	if len(drained.outputs) != 0 {
		t.Fatalf("failed dispatch must not publish outputs: %d", len(drained.outputs))
	}
}

func TestADKBoundaryUnknownToolNeverExecutesAndFailsBeforeClaim(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
	defer proof.close()
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
		agenticAssistant(agenticCall("call-x", "missing", `{}`)),
	}}
	runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: proof.newAgent(t, proof.ledgerModel(scripted))})
	drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput()))
	if len(drained.errs) != 1 || !strings.Contains(drained.errs[0].Error(), `tool "missing" unavailable`) {
		t.Fatalf("errors = %v", drained.errs)
	}
	if calls := proof.toolCalls(); len(calls) != 0 {
		t.Fatalf("unexpected tool rows %+v", calls)
	}
	records := proof.modelRequests()
	if len(records) != 1 || records[0].State != session.ModelRequestFailed {
		t.Fatalf("ledger = %+v", records)
	}
}

func TestADKBoundaryCancellationModes(t *testing.T) {
	t.Parallel()
	t.Run("before dispatch", func(t *testing.T) {
		t.Parallel()
		trace := &adkTrace{}
		proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
		defer proof.close()
		scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{agenticAssistant(agenticText("never"))}}
		cancelOpt, cancel := adk.WithCancel()
		runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: proof.newAgent(t, proof.ledgerModel(scripted))})
		ctx, stop := context.WithCancel(proof.ctx)
		stop()
		_, _ = cancel(adk.WithAgentCancelMode(adk.CancelImmediate))
		drained := drainADKEvents(t, trace, runner.Run(ctx, proof.userInput(), cancelOpt))
		if scripted.calls.Load() != 0 || len(proof.modelRequests()) != 0 {
			t.Fatalf("dispatch happened after cancellation: calls=%d ledger=%d", scripted.calls.Load(), len(proof.modelRequests()))
		}
		if len(drained.errs) == 0 {
			t.Fatalf("expected a cancellation error:\n%s", strings.Join(trace.list(), "\n"))
		}
	})
	t.Run("after chat model", func(t *testing.T) {
		t.Parallel()
		trace := &adkTrace{}
		proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
		defer proof.close()
		echo := proofTool("echo", proof.countingExecutor("echo", nil), nil)
		proof.tools, proof.snapshot.Tools = []Tool{echo}, []Tool{echo}
		cancelOpt, cancel := adk.WithCancel()
		scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
			agenticAssistant(agenticCall("", "echo", `{"value":"a"}`)),
			agenticAssistant(agenticText("unreachable")),
		}}
		scripted.onCall = func(context.Context, int) error {
			_, _ = cancel(adk.WithAgentCancelMode(adk.CancelAfterChatModel))
			return nil
		}
		checkpoints := newADKMemoryCheckpoints(trace)
		runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: proof.newAgent(t, proof.ledgerModel(scripted)), CheckPointStore: checkpoints})
		drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput(), cancelOpt, adk.WithCheckPointID("cancel-after-model")))
		var cancelErr *adk.CancelError
		if len(drained.errs) != 1 || !errors.As(drained.errs[0], &cancelErr) {
			t.Fatalf("errors = %v\n%s", drained.errs, strings.Join(trace.list(), "\n"))
		}
		// The model result was committed, the pending tool call exists, but the
		// executor never ran and no second dispatch happened.
		calls := proof.toolCalls()
		if len(calls) != 1 || calls[0].Status != session.ToolCallPending || proof.executions("echo") != 0 || scripted.calls.Load() != 1 {
			t.Fatalf("calls=%+v executions=%d provider=%d", calls, proof.executions("echo"), scripted.calls.Load())
		}
		if !checkpoints.has("cancel-after-model") {
			t.Fatalf("safe-point cancellation must checkpoint:\n%s", strings.Join(trace.list(), "\n"))
		}
		assertTraceOrder(t, trace, "adapter.commit", "checkpoint.set", "event.error")
	})
	t.Run("after tool calls", func(t *testing.T) {
		t.Parallel()
		trace := &adkTrace{}
		proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
		defer proof.close()
		cancelOpt, cancel := adk.WithCancel()
		echo := proofTool("echo", proof.countingExecutor("echo", func(ToolCall) (ToolResult, error) {
			_, _ = cancel(adk.WithAgentCancelMode(adk.CancelAfterToolCalls))
			return ToolResult{Output: "settled"}, nil
		}), nil)
		proof.tools, proof.snapshot.Tools = []Tool{echo}, []Tool{echo}
		scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
			agenticAssistant(agenticCall("", "echo", `{"value":"a"}`)),
			agenticAssistant(agenticText("unreachable")),
		}}
		checkpoints := newADKMemoryCheckpoints(trace)
		runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: proof.newAgent(t, proof.ledgerModel(scripted)), CheckPointStore: checkpoints})
		drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput(), cancelOpt, adk.WithCheckPointID("cancel-after-tools")))
		var cancelErr *adk.CancelError
		if len(drained.errs) != 1 || !errors.As(drained.errs[0], &cancelErr) {
			t.Fatalf("errors = %v\n%s", drained.errs, strings.Join(trace.list(), "\n"))
		}
		calls := proof.toolCalls()
		if len(calls) != 1 || calls[0].Status != session.ToolCallCompleted || proof.executions("echo") != 1 || scripted.calls.Load() != 1 {
			t.Fatalf("calls=%+v executions=%d provider=%d", calls, proof.executions("echo"), scripted.calls.Load())
		}
		assertTraceOrder(t, trace, "tool.settled", "checkpoint.set", "event.error")
	})
	t.Run("immediate during tool", func(t *testing.T) {
		t.Parallel()
		trace := &adkTrace{}
		proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
		defer proof.close()
		cancelOpt, cancel := adk.WithCancel()
		release := make(chan struct{})
		finished := make(chan struct{})
		echo := proofTool("echo", proof.countingExecutor("echo", func(call ToolCall) (ToolResult, error) {
			defer close(finished)
			_, _ = cancel(adk.WithAgentCancelMode(adk.CancelImmediate))
			<-release
			return ToolResult{Output: "late"}, nil
		}), nil)
		proof.tools, proof.snapshot.Tools = []Tool{echo}, []Tool{echo}
		scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
			agenticAssistant(agenticCall("", "echo", `{"value":"a"}`)),
			agenticAssistant(agenticText("unreachable")),
		}}
		runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: proof.newAgent(t, proof.ledgerModel(scripted))})
		iter := runner.Run(proof.ctx, proof.userInput(), cancelOpt)
		drained := drainADKEvents(t, trace, iter)
		var cancelErr *adk.CancelError
		if len(drained.errs) != 1 || !errors.As(drained.errs[0], &cancelErr) {
			close(release)
			t.Fatalf("errors = %v\n%s", drained.errs, strings.Join(trace.list(), "\n"))
		}
		if scripted.calls.Load() != 1 {
			close(release)
			t.Fatalf("provider calls = %d", scripted.calls.Load())
		}
		// The immediate cancel ended the run while the leaf was still running.
		// Release it, wait for the durable settlement to land, and check the
		// row: the runtime settles the late result under its own claim.
		calls := proof.toolCalls()
		if len(calls) != 1 || calls[0].Status != session.ToolCallRunning {
			close(release)
			t.Fatalf("tool call during immediate cancel = %+v", calls)
		}
		close(release)
		<-finished
		deadline := time.Now().Add(10 * time.Second)
		for {
			calls = proof.toolCalls()
			if session.TerminalToolCall(calls[0].Status) || time.Now().After(deadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if proof.executions("echo") != 1 || calls[0].Status != session.ToolCallCompleted {
			t.Fatalf("late settlement: executions=%d row=%+v", proof.executions("echo"), calls[0])
		}
	})
}

func TestADKBoundaryUnsupportedBlocksFailClosed(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
	defer proof.close()
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
		agenticAssistant(agenticText("searching"), schema.NewContentBlock(&schema.ServerToolCall{Name: "web_search", CallID: "srv-1"})),
	}}
	runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: proof.newAgent(t, proof.ledgerModel(scripted))})
	drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput()))
	if len(drained.errs) != 1 || !errors.Is(drained.errs[0], errADKUnsupportedBlock) || len(drained.outputs) != 0 {
		t.Fatalf("errs=%v outputs=%d", drained.errs, len(drained.outputs))
	}
	records := proof.modelRequests()
	if len(records) != 1 || records[0].State != session.ModelRequestFailed {
		t.Fatalf("ledger = %+v", records)
	}
}

// askPolicy is a real permissions.Policy that asks for one tool and denies
// another; without an approval requester an ask settles as expected_failure.
type askPolicy struct{}

func (askPolicy) Decide(_ context.Context, request permissions.Request) (permissions.Decision, error) {
	switch request.ToolName {
	case "guarded":
		return permissions.Decision{Action: permissions.ActionAsk, Message: "needs a human"}, nil
	case "forbidden":
		return permissions.Decision{Action: permissions.ActionDeny, Message: "never"}, nil
	}
	return permissions.Decision{Action: permissions.ActionAllow}, nil
}

func TestADKBoundaryPermissionPolicyDecidesBeforeExecution(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace, permissions: askPolicy{}})
	defer proof.close()
	guarded := proofTool("guarded", proof.countingExecutor("guarded", nil), nil)
	forbidden := proofTool("forbidden", proof.countingExecutor("forbidden", nil), nil)
	open := proofTool("open", proof.countingExecutor("open", nil), nil)
	proof.tools, proof.snapshot.Tools = []Tool{guarded, forbidden, open}, []Tool{guarded, forbidden, open}
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
		agenticAssistant(agenticCall("", "guarded", `{"value":"g"}`), agenticCall("", "forbidden", `{"value":"f"}`), agenticCall("", "open", `{"value":"o"}`)),
		agenticAssistant(agenticText("done")),
	}}
	runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: proof.newAgent(t, proof.ledgerModel(scripted))})
	drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput()))
	if len(drained.errs) != 0 {
		t.Fatalf("errors = %v\n%s", drained.errs, strings.Join(trace.list(), "\n"))
	}
	calls := statusByName(proof.toolCalls())
	if proof.executions("guarded") != 0 || proof.executions("forbidden") != 0 || proof.executions("open") != 1 {
		t.Fatalf("executions guarded=%d forbidden=%d open=%d", proof.executions("guarded"), proof.executions("forbidden"), proof.executions("open"))
	}
	for name, want := range map[string]string{"guarded": "approval_required", "forbidden": "denied"} {
		call := calls[name]
		if call.Status != session.ToolCallFailed {
			t.Fatalf("%s status = %s", name, call.Status)
		}
		var output ToolOutput
		if err := json.Unmarshal(call.Output, &output); err != nil || output.Status != "expected_failure" {
			t.Fatalf("%s output = %s (%v)", name, call.Output, err)
		}
		var structured struct{ Status string }
		if err := json.Unmarshal(output.Structured, &structured); err != nil || structured.Status != want {
			t.Fatalf("%s structured = %s (%v)", name, output.Structured, err)
		}
		if call.Metadata["permission_status"] != "" {
			t.Fatalf("%s leaked permission metadata into the durable row: %v", name, call.Metadata)
		}
	}
	// The model sees the settled permission outcomes, not the executor.
	seen := map[string]string{}
	for _, msg := range scripted.requests[1] {
		if id, text := functionResultText(msg); id != "" {
			seen[id] = text
		}
	}
	if seen[string(calls["guarded"].ID)] != string(calls["guarded"].Output) || seen[string(calls["forbidden"].ID)] != string(calls["forbidden"].Output) {
		t.Fatalf("model-visible results %v differ from durable rows", seen)
	}
}
