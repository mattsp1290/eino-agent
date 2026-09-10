package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

// W1 proof: adk.TurnLoop drives successive turns under one live run fence.
// The outer checkpoint store is written only at loop exit, after
// OnAgentEvents returned, and before Wait returns the exit state.

type turnItem struct {
	ID   string
	Text string
}

type turnLoopProof struct {
	proof       *adkProof
	trace       *adkTrace
	ledger      *adkLedgerModel
	checkpoints *adkMemoryCheckpoints
	mu          sync.Mutex
	turns       int
	turnDone    chan int
	events      []*adkEvents
	onEvents    func(tc *adk.TurnContext[turnItem, *schema.AgenticMessage])
	resume      *adk.ResumeParams
}

func (l *turnLoopProof) config(t *testing.T, checkpointID string) adk.TurnLoopConfig[turnItem, *schema.AgenticMessage] {
	return adk.TurnLoopConfig[turnItem, *schema.AgenticMessage]{
		GenInput: func(ctx context.Context, _ *adk.TurnLoop[turnItem, *schema.AgenticMessage], items []turnItem) (*adk.GenInputResult[turnItem, *schema.AgenticMessage], error) {
			// Consume one durable inbox item per turn; the rest stay queued.
			l.trace.add("loop.gen_input items=%d first=%s", len(items), items[0].ID)
			return &adk.GenInputResult[turnItem, *schema.AgenticMessage]{
				Input:     &adk.TypedAgentInput[*schema.AgenticMessage]{Messages: []*schema.AgenticMessage{schema.UserAgenticMessage(items[0].Text)}},
				Consumed:  items[:1],
				Remaining: items[1:],
			}, nil
		},
		GenResume: func(ctx context.Context, _ *adk.TurnLoop[turnItem, *schema.AgenticMessage], interrupted, unhandled, newItems []turnItem) (*adk.GenResumeResult[turnItem, *schema.AgenticMessage], error) {
			l.trace.add("loop.gen_resume interrupted=%d unhandled=%d new=%d", len(interrupted), len(unhandled), len(newItems))
			return &adk.GenResumeResult[turnItem, *schema.AgenticMessage]{ResumeParams: l.resume, Consumed: interrupted, Remaining: append(append([]turnItem{}, unhandled...), newItems...)}, nil
		},
		PrepareAgent: func(ctx context.Context, _ *adk.TurnLoop[turnItem, *schema.AgenticMessage], consumed []turnItem) (adk.TypedAgent[*schema.AgenticMessage], error) {
			l.trace.add("loop.prepare_agent consumed=%d", len(consumed))
			return l.proof.newAgent(t, l.ledger), nil
		},
		OnAgentEvents: func(ctx context.Context, tc *adk.TurnContext[turnItem, *schema.AgenticMessage], events *adk.AsyncIterator[*adk.TypedAgentEvent[*schema.AgenticMessage]]) error {
			l.mu.Lock()
			l.turns++
			turn := l.turns
			l.mu.Unlock()
			l.trace.add("loop.events_begin turn=%d", turn)
			drained := drainADKEvents(t, l.trace, events)
			l.mu.Lock()
			l.events = append(l.events, drained)
			l.mu.Unlock()
			if l.onEvents != nil {
				l.onEvents(tc)
			}
			l.trace.add("loop.events_returned turn=%d", turn)
			select {
			case l.turnDone <- turn:
			default:
			}
			for _, err := range drained.errs {
				var cancelErr *adk.CancelError
				if !errors.As(err, &cancelErr) {
					return err
				}
			}
			return nil
		},
		Store:        l.checkpoints,
		CheckpointID: checkpointID,
	}
}

func mustPush(t *testing.T, loop *adk.TurnLoop[turnItem, *schema.AgenticMessage], item turnItem) {
	t.Helper()
	if ok, _ := loop.Push(item); !ok {
		t.Fatalf("push %s rejected", item.ID)
	}
}

func newTurnLoopProof(t *testing.T, proof *adkProof, trace *adkTrace, scripted *adkScriptedModel, checkpoints *adkMemoryCheckpoints) *turnLoopProof {
	return &turnLoopProof{proof: proof, trace: trace, ledger: proof.ledgerModel(scripted), checkpoints: checkpoints, turnDone: make(chan int, 16)}
}

func (l *turnLoopProof) waitTurn(t *testing.T, want int) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case turn := <-l.turnDone:
			if turn == want {
				return
			}
			if turn > want {
				t.Fatalf("turn %d completed, want %d", turn, want)
			}
		case <-deadline:
			t.Fatalf("turn %d did not complete:\n%s", want, strings.Join(l.trace.list(), "\n"))
		}
	}
}

func TestADKTurnLoopTwoTurnsUnderOneRunFence(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
	defer proof.close()
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{agenticAssistant(agenticText("one")), agenticAssistant(agenticText("two"))}}
	checkpoints := newADKMemoryCheckpoints(trace)
	loopProof := newTurnLoopProof(t, proof, trace, scripted, checkpoints)
	loop := adk.NewTurnLoop(loopProof.config(t, "loop-1"))
	loop.Run(proof.ctx)
	if ok, _ := loop.Push(turnItem{ID: "inbox-1", Text: "first"}); !ok {
		t.Fatal("push 1 rejected")
	}
	loopProof.waitTurn(t, 1)
	if ok, _ := loop.Push(turnItem{ID: "inbox-2", Text: "second"}); !ok {
		t.Fatal("push 2 rejected")
	}
	loopProof.waitTurn(t, 2)
	loop.Stop()
	state := loop.Wait()
	trace.add("test.wait_returned")
	if state.ExitReason != nil || state.CheckpointAttempted || state.CheckpointErr != nil || len(state.UnhandledItems) != 0 || len(state.InterruptedItems) != 0 {
		t.Fatalf("exit state = %+v", state)
	}
	// Two physical dispatches, two ledger rows under one run, distinct
	// assistant messages, and the run is still live (no terminal settlement).
	records := proof.modelRequests()
	if len(records) != 2 || records[0].AssistantMessageID == records[1].AssistantMessageID || records[0].RunID != records[1].RunID {
		t.Fatalf("ledger = %+v", records)
	}
	run, err := proof.store.GetRun(proof.ctx, proof.run.ID)
	if err != nil || run.Status != session.RunRunning {
		t.Fatalf("run = %+v (%v)", run, err)
	}
	if scripted.requests[1][0] == nil || scripted.requests[1][0].ContentBlocks[0].UserInputText.Text != "second" {
		t.Fatalf("second turn input = %v", scripted.requests[1])
	}
	if len(scripted.requests[1]) != 1 {
		t.Fatalf("second turn reused the first turn's transcript: %d messages", len(scripted.requests[1]))
	}
	// OnAgentEvents starts concurrently with the agent, so only the durable
	// sequence is asserted: input planning precedes dispatch, dispatch
	// precedes the callback return, and the second turn starts only after.
	assertTraceOrder(t, trace, "loop.gen_input items=1 first=inbox-1", "loop.prepare_agent", "adapter.generate.begin call=1", "loop.events_returned turn=1",
		"loop.gen_input items=1 first=inbox-2", "adapter.generate.begin call=2", "loop.events_returned turn=2", "test.wait_returned")
	if trace.count("checkpoint.set") != 0 {
		t.Fatalf("clean idle exit must not checkpoint:\n%s", strings.Join(trace.list(), "\n"))
	}
	batch, err := proof.store.ListMessages(proof.ctx, proof.run.SessionID, session.ReplayCursor{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	assistants := 0
	for _, message := range batch.Messages {
		if message.Role == session.RoleAssistant && message.RunID == proof.run.ID {
			assistants++
		}
	}
	if assistants != 2 {
		t.Fatalf("assistant messages = %d", assistants)
	}
}

func TestADKTurnLoopInterruptedTurnCheckpointsAfterEventsReturn(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
	defer proof.close()
	recoveryTools(proof)
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
		agenticAssistant(agenticText("two tools"), agenticCall("", "safe", `{"value":"1"}`), agenticCall("", "gate", `{"value":"2"}`)),
		agenticAssistant(agenticText("resumed")),
	}}
	checkpoints := newADKMemoryCheckpoints(trace)
	loopProof := newTurnLoopProof(t, proof, trace, scripted, checkpoints)
	loop := adk.NewTurnLoop(loopProof.config(t, "loop-1"))
	loop.Run(proof.ctx)
	mustPush(t, loop, turnItem{ID: "inbox-1", Text: "first"})
	mustPush(t, loop, turnItem{ID: "inbox-2", Text: "queued"})
	state := loop.Wait()
	trace.add("test.wait_returned")
	var interruptErr *adk.InterruptError
	if !errors.As(state.ExitReason, &interruptErr) || !state.CheckpointAttempted || state.CheckpointErr != nil {
		t.Fatalf("exit state = %+v\n%s", state, strings.Join(trace.list(), "\n"))
	}
	if len(state.InterruptedItems) != 1 || state.InterruptedItems[0].ID != "inbox-1" || len(state.UnhandledItems) != 1 || state.UnhandledItems[0].ID != "inbox-2" {
		t.Fatalf("items: interrupted=%+v unhandled=%+v", state.InterruptedItems, state.UnhandledItems)
	}
	// Protocol: OnAgentEvents returns -> outer Set -> Wait returns.
	assertTraceOrder(t, trace, "loop.events_returned turn=1", "checkpoint.set loop-1", "test.wait_returned")
	if trace.count("checkpoint.set loop-1") != 1 {
		t.Fatalf("outer store written %d times", trace.count("checkpoint.set loop-1"))
	}
	before := statusByName(proof.toolCalls())
	if before["safe"].Status != session.ToolCallCompleted || before["gate"].Status != session.ToolCallPending {
		t.Fatalf("tool state = %+v", before)
	}
	// Resume the same loop identity: GenResume targets the paused leaf, the
	// queued item runs as the next turn, then the loop idles out.
	loopProof.resume = &adk.ResumeParams{Targets: map[string]any{interruptErr.InterruptContexts[0].ID: "approve"}}
	scripted.responses = append(scripted.responses, agenticAssistant(agenticText("queued done")))
	resumed := adk.NewTurnLoop(loopProof.config(t, "loop-1"))
	resumed.Run(proof.ctx)
	loopProof.waitTurn(t, 2)
	loopProof.waitTurn(t, 3)
	resumed.Stop()
	state = resumed.Wait()
	if state.ExitReason != nil || state.CheckpointAttempted {
		t.Fatalf("resumed exit state = %+v\n%s", state, strings.Join(trace.list(), "\n"))
	}
	after := statusByName(proof.toolCalls())
	if after["gate"].Status != session.ToolCallCompleted || proof.executions("gate") != 1 || proof.executions("safe") != 1 {
		t.Fatalf("after resume: %+v gate=%d safe=%d", after, proof.executions("gate"), proof.executions("safe"))
	}
	if len(proof.modelRequests()) != 3 {
		t.Fatalf("ledger rows = %d", len(proof.modelRequests()))
	}
	assertTraceOrder(t, trace, "checkpoint.get loop-1", "loop.gen_resume interrupted=1 unhandled=1 new=0", "tool.resume", "adapter.generate.begin call=2", "loop.events_returned turn=2",
		"loop.gen_input items=1 first=inbox-2", "adapter.generate.begin call=3", "loop.events_returned turn=3")
	// A clean exit retires the loaded checkpoint through CheckPointDeleter.
	if checkpoints.has("loop-1") {
		t.Fatalf("loaded checkpoint was not retired after clean exit:\n%s", strings.Join(trace.list(), "\n"))
	}
}

func TestADKTurnLoopCheckpointSetFailureIsReportedNotSilent(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
	defer proof.close()
	recoveryTools(proof)
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
		agenticAssistant(agenticCall("", "safe", `{"value":"1"}`), agenticCall("", "gate", `{"value":"2"}`)),
	}}
	checkpoints := newADKMemoryCheckpoints(trace)
	checkpoints.setErr = errors.New("disk full")
	loopProof := newTurnLoopProof(t, proof, trace, scripted, checkpoints)
	loop := adk.NewTurnLoop(loopProof.config(t, "loop-1"))
	loop.Run(proof.ctx)
	mustPush(t, loop, turnItem{ID: "inbox-1", Text: "first"})
	state := loop.Wait()
	if !state.CheckpointAttempted || state.CheckpointErr == nil || !strings.Contains(state.CheckpointErr.Error(), "disk full") {
		t.Fatalf("exit state = %+v", state)
	}
	if checkpoints.has("loop-1") {
		t.Fatal("failed Set must not leave a promoted checkpoint")
	}
	// Durable effects committed earlier remain authoritative regardless.
	after := statusByName(proof.toolCalls())
	if after["safe"].Status != session.ToolCallCompleted || after["gate"].Status != session.ToolCallPending || len(proof.modelRequests()) != 1 {
		t.Fatalf("durable state after failed checkpoint = %+v", after)
	}
}

func TestADKTurnLoopBetweenTurnStopKeepsQueuedInput(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
	defer proof.close()
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{agenticAssistant(agenticText("one")), agenticAssistant(agenticText("never"))}}
	checkpoints := newADKMemoryCheckpoints(trace)
	loopProof := newTurnLoopProof(t, proof, trace, scripted, checkpoints)
	loopProof.onEvents = func(tc *adk.TurnContext[turnItem, *schema.AgenticMessage]) {
		// Stop after the first turn's events are handled but before the
		// queued item starts its turn.
		tc.Loop.Stop()
	}
	loop := adk.NewTurnLoop(loopProof.config(t, "loop-1"))
	loop.Run(proof.ctx)
	mustPush(t, loop, turnItem{ID: "inbox-1", Text: "first"})
	mustPush(t, loop, turnItem{ID: "inbox-2", Text: "queued"})
	state := loop.Wait()
	if state.ExitReason != nil || len(state.InterruptedItems) != 0 || len(state.UnhandledItems) != 1 || state.UnhandledItems[0].ID != "inbox-2" {
		t.Fatalf("exit state = %+v\n%s", state, strings.Join(trace.list(), "\n"))
	}
	if scripted.calls.Load() != 1 || len(proof.modelRequests()) != 1 {
		t.Fatalf("queued item was dispatched: calls=%d ledger=%d", scripted.calls.Load(), len(proof.modelRequests()))
	}
	// A between-turn stop with queued input is a queued continuation, not a
	// resumable interrupted agent: the outer checkpoint holds only the queue.
	if !state.CheckpointAttempted || state.CheckpointErr != nil {
		t.Fatalf("queued continuation checkpoint = %+v", state)
	}
	loopProof.resume = nil
	resumed := adk.NewTurnLoop(loopProof.config(t, "loop-1"))
	resumed.Run(proof.ctx)
	loopProof.waitTurn(t, 2)
	resumed.Stop()
	state = resumed.Wait()
	if state.ExitReason != nil || scripted.calls.Load() != 2 {
		t.Fatalf("continuation exit = %+v calls=%d", state, scripted.calls.Load())
	}
	// Upstream semantics: a between-turn checkpoint carries only the queue, so
	// the loop plans the queued item through GenInput, never GenResume, and
	// retires the loaded checkpoint on clean exit.
	assertTraceOrder(t, trace, "checkpoint.get loop-1", "loop.gen_input items=1 first=inbox-2", "adapter.generate.begin call=2", "checkpoint.delete loop-1")
	if trace.count("loop.gen_resume") != 0 {
		t.Fatalf("queued continuation must not be planned as a resume:\n%s", strings.Join(trace.list(), "\n"))
	}
}

func TestADKTurnLoopPreemptTargetsOnlyCapturedTurn(t *testing.T) {
	t.Parallel()
	trace := &adkTrace{}
	proof := newADKProof(t, filepath.Join(t.TempDir(), "proof.db"), adkProofOptions{trace: trace})
	defer proof.close()
	echo := proofTool("echo", proof.countingExecutor("echo", nil), nil)
	proof.tools, proof.snapshot.Tools = []Tool{echo}, []Tool{echo}
	scripted := &adkScriptedModel{trace: trace, responses: []*schema.AgenticMessage{
		agenticAssistant(agenticCall("", "echo", `{"value":"1"}`)),
		agenticAssistant(agenticText("after preempt")),
		agenticAssistant(agenticText("second turn")),
	}}
	checkpoints := newADKMemoryCheckpoints(trace)
	loopProof := newTurnLoopProof(t, proof, trace, scripted, checkpoints)
	loop := adk.NewTurnLoop(loopProof.config(t, "loop-1"))
	var preempted <-chan struct{}
	scripted.onCall = func(ctx context.Context, call int) error {
		if call == 1 {
			_, preempted = loop.Push(turnItem{ID: "inbox-2", Text: "urgent"}, adk.WithPreempt[turnItem, *schema.AgenticMessage](adk.AfterChatModel))
		}
		return nil
	}
	loop.Run(proof.ctx)
	mustPush(t, loop, turnItem{ID: "inbox-1", Text: "first"})
	loopProof.waitTurn(t, 1)
	if preempted != nil {
		<-preempted
	}
	loopProof.waitTurn(t, 2)
	loop.Stop()
	state := loop.Wait()
	if state.ExitReason != nil {
		t.Fatalf("exit = %+v\n%s", state, strings.Join(trace.list(), "\n"))
	}
	// The preempted first turn committed its model output but never executed
	// the tool; the urgent item ran as its own turn under the same fence.
	if proof.executions("echo") != 0 || scripted.calls.Load() != 2 {
		t.Fatalf("echo=%d calls=%d\n%s", proof.executions("echo"), scripted.calls.Load(), strings.Join(trace.list(), "\n"))
	}
	calls := proof.toolCalls()
	if len(calls) != 1 || calls[0].Status != session.ToolCallPending {
		t.Fatalf("preempted tool call = %+v", calls)
	}
	if got := scripted.requests[1][0].ContentBlocks[0].UserInputText.Text; got != "urgent" {
		t.Fatalf("second turn input = %q", got)
	}
}
