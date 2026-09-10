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

	"github.com/mattsp1290/eino-agent/session"
)

// W1 proof: real ADK checkpoints round-trip through a SQLite reopen. Settled
// tools are never re-executed, unresolved unsafe tools are interrupted without
// rerun, and targeted resume only touches the addressed leaf.

func recoveryTools(proof *adkProof) (Tool, Tool) {
	safe := proofTool("safe", proof.countingExecutor("safe", func(call ToolCall) (ToolResult, error) {
		return ToolResult{Output: "safe:" + string(call.ID)}, nil
	}), map[string]string{"retry_safe": "true"})
	gate := proofTool("gate", proof.countingExecutor("gate", func(call ToolCall) (ToolResult, error) {
		return ToolResult{Output: "gate:" + string(call.ID)}, nil
	}), map[string]string{"proof_interrupt": "host_decision"})
	proof.tools, proof.snapshot.Tools = []Tool{safe, gate}, []Tool{safe, gate}
	return safe, gate
}

func interruptedFirstTurn(t *testing.T, proof *adkProof, trace *adkTrace, checkpoints *adkMemoryCheckpoints, checkpointID string) (*adkEvents, *adkScriptedModel) {
	t.Helper()
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
		agenticAssistant(agenticText("running two tools"), agenticCall("", "safe", `{"value":"1"}`), agenticCall("", "gate", `{"value":"2"}`)),
		agenticAssistant(agenticText("unreachable in first process")),
	}}
	runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: proof.newAgent(t, proof.ledgerModel(scripted)), CheckPointStore: checkpoints})
	drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput(), adk.WithCheckPointID(checkpointID)))
	if len(drained.errs) != 0 {
		t.Fatalf("errors = %v\n%s", drained.errs, strings.Join(trace.list(), "\n"))
	}
	if len(drained.interrupts) != 1 || drained.interrupts[0].Info == nil {
		t.Fatalf("interrupt contexts = %+v\n%s", drained.interrupts, strings.Join(trace.list(), "\n"))
	}
	if !checkpoints.has(checkpointID) {
		t.Fatalf("checkpoint %s missing", checkpointID)
	}
	// The checkpoint is written before the interrupt event reaches the
	// consumer, and only after the safe tool settled.
	// Tools run in parallel, so the settle/interrupt order between them is
	// free; both precede the checkpoint write, which precedes the event.
	assertTraceOrder(t, trace, "tool.settled", "checkpoint.set "+checkpointID, "event.interrupted", "iterator.drained")
	assertTraceOrder(t, trace, "tool.interrupt", "checkpoint.set "+checkpointID)
	if scripted.calls.Load() != 1 || len(proof.modelRequests()) != 1 {
		t.Fatalf("first process dispatched %d provider calls, %d ledger rows", scripted.calls.Load(), len(proof.modelRequests()))
	}
	return drained, scripted
}

func statusByName(calls []session.ToolCall) map[string]session.ToolCall {
	result := make(map[string]session.ToolCall, len(calls))
	for _, call := range calls {
		result[call.Name] = call
	}
	return result
}

func TestADKRecoveryCheckpointSurvivesReopenAndResumesTargetedLeaf(t *testing.T) {
	t.Parallel()
	for _, decision := range []string{"approve", "deny"} {
		t.Run(decision, func(t *testing.T) {
			t.Parallel()
			trace := &adkTrace{}
			dbPath := filepath.Join(t.TempDir(), "proof.db")
			checkpoints := newADKMemoryCheckpoints(trace)
			first := newADKProof(t, dbPath, adkProofOptions{trace: trace, lease: 50 * time.Millisecond})
			recoveryTools(first)
			drained, _ := interruptedFirstTurn(t, first, trace, checkpoints, "run-checkpoint")
			before := statusByName(first.toolCalls())
			if before["safe"].Status != session.ToolCallCompleted || before["gate"].Status != session.ToolCallPending || first.executions("safe") != 1 || first.executions("gate") != 0 {
				t.Fatalf("first process tool state = %+v safe=%d gate=%d", before, first.executions("safe"), first.executions("gate"))
			}
			runID := first.run.ID
			first.close()
			trace.add("process.restart")

			second := reopenADKProof(t, dbPath, runID, adkProofOptions{trace: trace, ids: &sequenceIDs{n: 1000}})
			defer second.close()
			recoveryTools(second)
			scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{agenticAssistant(agenticText("done after " + decision))}}
			runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: second.newAgent(t, second.ledgerModel(scripted)), CheckPointStore: checkpoints})
			iter, err := runner.ResumeWithParams(second.ctx, "run-checkpoint", &adk.ResumeParams{Targets: map[string]any{drained.interrupts[0].ID: decision}})
			if err != nil {
				t.Fatal(err)
			}
			resumed := drainADKEvents(t, trace, iter)
			if len(resumed.errs) != 0 || len(resumed.interrupts) != 0 {
				t.Fatalf("resume errors=%v interrupts=%d\n%s", resumed.errs, len(resumed.interrupts), strings.Join(trace.list(), "\n"))
			}
			after := statusByName(second.toolCalls())
			// Safe tool: never re-executed after the restart; its settled row is unchanged.
			if second.executions("safe") != 0 || after["safe"].Status != session.ToolCallCompleted || string(after["safe"].Output) != string(before["safe"].Output) {
				t.Fatalf("safe tool changed across restart: executions=%d before=%s after=%s", second.executions("safe"), before["safe"].Output, after["safe"].Output)
			}
			switch decision {
			case "approve":
				if second.executions("gate") != 1 || after["gate"].Status != session.ToolCallCompleted {
					t.Fatalf("gate approve: executions=%d status=%s", second.executions("gate"), after["gate"].Status)
				}
			case "deny":
				if second.executions("gate") != 0 || after["gate"].Status != session.ToolCallFailed {
					t.Fatalf("gate deny: executions=%d status=%s", second.executions("gate"), after["gate"].Status)
				}
				var output ToolOutput
				if err := json.Unmarshal(after["gate"].Output, &output); err != nil || output.Status != "expected_failure" {
					t.Fatalf("denied output = %s (%v)", after["gate"].Output, err)
				}
			}
			// Exactly one new ledger row under the new fence, and the continuation
			// request carries both settled results with canonical identity.
			records := second.modelRequests()
			if len(records) != 2 || scripted.calls.Load() != 1 {
				t.Fatalf("ledger rows = %d provider calls = %d", len(records), scripted.calls.Load())
			}
			request := scripted.requests[0]
			seen := map[string]string{}
			for _, msg := range request {
				if id, text := functionResultText(msg); id != "" {
					seen[id] = text
				}
			}
			if len(seen) != 2 || seen[string(after["safe"].ID)] != string(after["safe"].Output) || seen[string(after["gate"].ID)] != string(after["gate"].Output) {
				t.Fatalf("continuation results %v do not match durable rows safe=%s gate=%s", seen, after["safe"].Output, after["gate"].Output)
			}
			if text := assistantText(resumed.outputs[len(resumed.outputs)-1]); text != "done after "+decision {
				t.Fatalf("final text = %q", text)
			}
			assertTraceOrder(t, trace, "process.restart", "checkpoint.get run-checkpoint", "tool.resume", "adapter.generate.begin", "provider.call 1", "adapter.commit", "iterator.drained")
			if decision == "approve" {
				assertTraceOrder(t, trace, "process.restart", "tool.claimed", "executor.gate", "tool.settled")
			}
		})
	}
}

func TestADKRecoveryRunningUnsafeToolIsInterruptedWithoutRerun(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	dbPath := filepath.Join(t.TempDir(), "proof.db")
	checkpoints := newADKMemoryCheckpoints(trace)
	first := newADKProof(t, dbPath, adkProofOptions{trace: trace, lease: 50 * time.Millisecond})
	recoveryTools(first)
	drained, _ := interruptedFirstTurn(t, first, trace, checkpoints, "run-checkpoint")
	before := statusByName(first.toolCalls())
	// Simulate a process that claimed the gated call, started executing, and
	// crashed before settlement: the durable record is running with a lease.
	startedAt := first.host.now()
	if _, err := first.execution.persistToolClaim(first.ctx, session.ClaimToolCallRequest{
		ID: before["gate"].ID, ClaimedBy: first.host.ownerID(), ClaimToken: "crashed-claim", StartedAt: startedAt,
		LeaseDuration: first.host.lease(), Event: toolTransitionEnvelope(first.host, first.snapshot, startedAt),
	}); err != nil {
		t.Fatal(err)
	}
	runID := first.run.ID
	first.close()
	trace.add("process.restart")

	second := reopenADKProof(t, dbPath, runID, adkProofOptions{trace: trace, ids: &sequenceIDs{n: 1000}})
	defer second.close()
	recoveryTools(second)
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{agenticAssistant(agenticText("done"))}}
	runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: second.newAgent(t, second.ledgerModel(scripted)), CheckPointStore: checkpoints})
	iter, err := runner.ResumeWithParams(second.ctx, "run-checkpoint", &adk.ResumeParams{Targets: map[string]any{drained.interrupts[0].ID: "approve"}})
	if err != nil {
		t.Fatal(err)
	}
	resumed := drainADKEvents(t, trace, iter)
	if len(resumed.errs) != 0 {
		t.Fatalf("resume errors = %v\n%s", resumed.errs, strings.Join(trace.list(), "\n"))
	}
	after := statusByName(second.toolCalls())
	if second.executions("gate") != 0 || second.executions("safe") != 0 || after["gate"].Status != session.ToolCallInterrupted {
		t.Fatalf("unsafe running tool was rerun: gate=%d safe=%d status=%s", second.executions("gate"), second.executions("safe"), after["gate"].Status)
	}
	var output ToolOutput
	if err := json.Unmarshal(after["gate"].Output, &output); err != nil || output.Status != "interrupted" {
		t.Fatalf("interrupted output = %s (%v)", after["gate"].Output, err)
	}
	request := scripted.requests[0]
	seen := map[string]string{}
	for _, msg := range request {
		if id, text := functionResultText(msg); id != "" {
			seen[id] = text
		}
	}
	if seen[string(after["gate"].ID)] != string(after["gate"].Output) {
		t.Fatalf("model did not observe the interrupted settlement: %v", seen)
	}
	assertTraceOrder(t, trace, "process.restart", "tool.interrupted_without_rerun", "adapter.generate.begin")
}

func TestADKRecoverySettledToolReplaysWithoutExecutorWhenCheckpointIsStale(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	dbPath := filepath.Join(t.TempDir(), "proof.db")
	checkpoints := newADKMemoryCheckpoints(trace)
	first := newADKProof(t, dbPath, adkProofOptions{trace: trace, lease: 50 * time.Millisecond})
	_, gate := recoveryTools(first)
	drained, _ := interruptedFirstTurn(t, first, trace, checkpoints, "run-checkpoint")
	before := statusByName(first.toolCalls())
	// The checkpoint says the gated call still needs a rerun, but a process
	// settled it after the checkpoint was written and crashed before any
	// checkpoint promotion. SQL is authoritative: the executor must not rerun.
	startedAt := first.host.now()
	claimed, err := first.execution.persistToolClaim(first.ctx, session.ClaimToolCallRequest{
		ID: before["gate"].ID, ClaimedBy: first.host.ownerID(), ClaimToken: "late-claim", StartedAt: startedAt,
		LeaseDuration: first.host.lease(), Event: toolTransitionEnvelope(first.host, first.snapshot, startedAt),
	})
	if err != nil {
		t.Fatal(err)
	}
	call := ToolCall{ID: claimed.Call.ID, SessionID: claimed.Call.SessionID, RunID: claimed.Call.RunID, MessageID: claimed.Call.MessageID, ResultMessageID: claimed.Call.ResultMessageID, ResultPartID: claimed.Call.ResultPartID, Name: "gate", Pattern: claimed.Call.Pattern, Input: cloneJSON(claimed.Call.Input)}
	settled, err := first.execution.executeAndSettleClaimedTool(first.ctx, first.snapshot, gate, call, claimed.Call, nil)
	if err != nil || settled.Settlement.Status != session.ToolCallCompleted || first.executions("gate") != 1 {
		t.Fatalf("late settlement = %+v err=%v executions=%d", settled.Settlement, err, first.executions("gate"))
	}
	runID := first.run.ID
	first.close()
	trace.add("process.restart")

	second := reopenADKProof(t, dbPath, runID, adkProofOptions{trace: trace, ids: &sequenceIDs{n: 1000}})
	defer second.close()
	recoveryTools(second)
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{agenticAssistant(agenticText("done"))}}
	runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: second.newAgent(t, second.ledgerModel(scripted)), CheckPointStore: checkpoints})
	iter, err := runner.ResumeWithParams(second.ctx, "run-checkpoint", &adk.ResumeParams{Targets: map[string]any{drained.interrupts[0].ID: "approve"}})
	if err != nil {
		t.Fatal(err)
	}
	resumed := drainADKEvents(t, trace, iter)
	if len(resumed.errs) != 0 {
		t.Fatalf("resume errors = %v\n%s", resumed.errs, strings.Join(trace.list(), "\n"))
	}
	after := statusByName(second.toolCalls())
	if second.executions("gate") != 0 || after["gate"].Status != session.ToolCallCompleted || string(after["gate"].Output) != string(settled.Settlement.Output) {
		t.Fatalf("settled tool was re-executed or rewritten: executions=%d status=%s", second.executions("gate"), after["gate"].Status)
	}
	seen := map[string]string{}
	for _, msg := range scripted.requests[0] {
		if id, text := functionResultText(msg); id != "" {
			seen[id] = text
		}
	}
	if seen[string(after["gate"].ID)] != string(settled.Settlement.Output) {
		t.Fatalf("model observed %q instead of the recorded settlement", seen[string(after["gate"].ID)])
	}
	assertTraceOrder(t, trace, "process.restart", "tool.replay", "adapter.generate.begin")
}

func TestADKRecoveryUntargetedResumeKeepsLeafPaused(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	checkpoints := newADKMemoryCheckpoints(trace)
	proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
	defer proof.close()
	recoveryTools(proof)
	first, _ := interruptedFirstTurn(t, proof, trace, checkpoints, "run-checkpoint")
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{agenticAssistant(agenticText("resumed"))}}
	runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: proof.newAgent(t, proof.ledgerModel(scripted)), CheckPointStore: checkpoints})
	iter, err := runner.Resume(proof.ctx, "run-checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	resumed := drainADKEvents(t, trace, iter)
	if len(resumed.errs) != 0 || len(resumed.interrupts) != 1 {
		t.Fatalf("untargeted resume errors=%v interrupts=%d\n%s", resumed.errs, len(resumed.interrupts), strings.Join(trace.list(), "\n"))
	}
	if scripted.calls.Load() != 0 || len(proof.modelRequests()) != 1 || proof.executions("gate") != 0 {
		t.Fatalf("untargeted resume dispatched work: provider=%d ledger=%d gate=%d", scripted.calls.Load(), len(proof.modelRequests()), proof.executions("gate"))
	}
	if trace.count("checkpoint.set run-checkpoint") != 2 {
		t.Fatalf("expected the paused leaf to checkpoint again:\n%s", strings.Join(trace.list(), "\n"))
	}
	// Eino mints a fresh interrupt ID on every pause; the address is the
	// stable identity. A stale ID target resolves to nothing and re-pauses
	// silently, so public pause identity must be the address (or a durable
	// runtime record resolved to the current ID at resume time).
	if resumed.interrupts[0].ID == first.interrupts[0].ID {
		t.Fatalf("interrupt id unexpectedly stable across re-pause: %s", resumed.interrupts[0].ID)
	}
	if !resumed.interrupts[0].Address.Equals(first.interrupts[0].Address) {
		t.Fatalf("interrupt address changed across re-pause: %v != %v", resumed.interrupts[0].Address, first.interrupts[0].Address)
	}
	iter, err = runner.ResumeWithParams(proof.ctx, "run-checkpoint", &adk.ResumeParams{Targets: map[string]any{first.interrupts[0].ID: "approve"}})
	if err != nil {
		t.Fatal(err)
	}
	stale := drainADKEvents(t, trace, iter)
	if len(stale.errs) != 0 || len(stale.interrupts) != 1 || proof.executions("gate") != 0 || scripted.calls.Load() != 0 {
		t.Fatalf("stale interrupt id must be a silent no-op re-pause: errs=%v interrupts=%d gate=%d calls=%d", stale.errs, len(stale.interrupts), proof.executions("gate"), scripted.calls.Load())
	}
	iter, err = runner.ResumeWithParams(proof.ctx, "run-checkpoint", &adk.ResumeParams{Targets: map[string]any{stale.interrupts[0].ID: "approve"}})
	if err != nil {
		t.Fatal(err)
	}
	current := drainADKEvents(t, trace, iter)
	if len(current.errs) != 0 || len(current.interrupts) != 0 || proof.executions("gate") != 1 {
		t.Fatalf("current interrupt id must resume: errs=%v interrupts=%d gate=%d", current.errs, len(current.interrupts), proof.executions("gate"))
	}
}

func TestADKRecoveryCheckpointSetFailureStillEmitsInterrupt(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	checkpoints := newADKMemoryCheckpoints(trace)
	checkpoints.setErr = errors.New("checkpoint store unavailable")
	proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
	defer proof.close()
	recoveryTools(proof)
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
		agenticAssistant(agenticCall("", "safe", `{"value":"1"}`), agenticCall("", "gate", `{"value":"2"}`)),
	}}
	runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: proof.newAgent(t, proof.ledgerModel(scripted)), CheckPointStore: checkpoints})
	drained := drainADKEvents(t, trace, runner.Run(proof.ctx, proof.userInput(), adk.WithCheckPointID("run-checkpoint")))
	// The runner reports the failed Set as an error event and still sends the
	// interrupt event, so an interrupt action alone never proves durability:
	// public pause promotion requires the interrupt and an error-free drain.
	if len(drained.errs) != 1 || !strings.Contains(drained.errs[0].Error(), "checkpoint store unavailable") || len(drained.interrupts) != 1 {
		t.Fatalf("errs=%v interrupts=%d\n%s", drained.errs, len(drained.interrupts), strings.Join(trace.list(), "\n"))
	}
	if checkpoints.has("run-checkpoint") {
		t.Fatal("failed Set left a checkpoint behind")
	}
	after := statusByName(proof.toolCalls())
	if after["safe"].Status != session.ToolCallCompleted || after["gate"].Status != session.ToolCallPending {
		t.Fatalf("durable rows after failed checkpoint = %+v", after)
	}
}

func TestADKRecoveryConcurrentResumeYieldsOneFence(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	dbPath := filepath.Join(t.TempDir(), "proof.db")
	checkpoints := newADKMemoryCheckpoints(trace)
	first := newADKProof(t, dbPath, adkProofOptions{trace: trace, lease: 50 * time.Millisecond})
	recoveryTools(first)
	_, _ = interruptedFirstTurn(t, first, trace, checkpoints, "run-checkpoint")
	runID := first.run.ID
	first.close()
	second := reopenADKProof(t, dbPath, runID, adkProofOptions{trace: trace, ids: &sequenceIDs{n: 1000}})
	defer second.close()
	// A competing claim while the reopened fence is live must be rejected.
	if _, err := second.store.ClaimRun(second.ctx, session.RunClaim{RunID: runID, OwnerID: "intruder", ClaimToken: "intruder-token", LeaseDuration: second.host.lease()}); !errors.Is(err, session.ErrConflict) && !errors.Is(err, session.ErrSessionBusy) {
		t.Fatalf("second live claim = %v", err)
	}
	// Stale fence writes are rejected after the new claim.
	stale := second.store.Execution(session.RunFence{RunID: runID, ClaimToken: "stale"})
	if _, err := stale.AppendMessage(second.ctx, session.Message{ID: "stale-message", SessionID: second.run.SessionID, RunID: runID, Role: session.RoleAssistant, CreatedAt: second.host.now(), UpdatedAt: second.host.now()}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("stale fence write = %v", err)
	}
}

func TestADKRecoveryCancellationAfterCheckpointDoesNotDuplicateWork(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	checkpoints := newADKMemoryCheckpoints(trace)
	proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
	defer proof.close()
	recoveryTools(proof)
	drained, _ := interruptedFirstTurn(t, proof, trace, checkpoints, "run-checkpoint")
	// Cancel the resumed run before it dispatches: the resume context is
	// cancelled, so no ledger row or executor call may happen.
	ctx, cancel := context.WithCancel(proof.ctx)
	cancel()
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{agenticAssistant(agenticText("unreachable"))}}
	runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: proof.newAgent(t, proof.ledgerModel(scripted)), CheckPointStore: checkpoints})
	iter, err := runner.ResumeWithParams(ctx, "run-checkpoint", &adk.ResumeParams{Targets: map[string]any{drained.interrupts[0].ID: "approve"}})
	if err != nil {
		t.Fatal(err)
	}
	resumed := drainADKEvents(t, trace, iter)
	if scripted.calls.Load() != 0 {
		t.Fatalf("cancelled resume dispatched the model: %v", resumed.errs)
	}
	after := statusByName(proof.toolCalls())
	if proof.executions("gate") != 0 {
		t.Fatalf("cancelled resume ran the gated executor %d times", proof.executions("gate"))
	}
	if after["gate"].Status != session.ToolCallPending {
		t.Fatalf("cancelled resume changed the gated call to %s", after["gate"].Status)
	}
	if len(resumed.errs) == 0 {
		t.Fatalf("cancelled resume must surface an error:\n%s", strings.Join(trace.list(), "\n"))
	}
}
