package runtime

import (
	"bytes"
	"context"
	"encoding/gob"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// upstreamTurnLoopCheckpointShape is a field-identical mirror of eino's
// private adk.turnLoopCheckpoint[session.InboxID] (adk/turn_loop.go): gob
// matches by field name and type, not by concrete struct identity, so
// decoding marshalEmptyLoopCheckpoint's bytes into this type proves what
// eino's own unmarshalTurnLoopCheckpoint would see, without importing an
// unexported upstream type. Round-three reconciliation item 5 (SR-5): the
// turnLoopCheckpointShape doc comment in adk_checkpoint.go claims this
// round trip is verified by a test; this is that test.
type upstreamTurnLoopCheckpointShape struct {
	RunnerCheckpoint []byte
	HasRunnerState   bool
	UnhandledItems   []session.InboxID
	CanceledItems    []session.InboxID
}

func TestEmptyLoopCheckpointMatchesUpstreamGobShape(t *testing.T) {
	raw, err := marshalEmptyLoopCheckpoint()
	if err != nil {
		t.Fatalf("marshalEmptyLoopCheckpoint: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("empty payload would fail decodeCheckpointEnvelope's len(Payload) != 0 guard")
	}
	var out upstreamTurnLoopCheckpointShape
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&out); err != nil {
		t.Fatalf("eino's unmarshalTurnLoopCheckpoint would fail decoding this payload: %v", err)
	}
	if out.HasRunnerState || len(out.RunnerCheckpoint) != 0 || len(out.UnhandledItems) != 0 || len(out.CanceledItems) != 0 {
		t.Fatalf("decoded = %+v, want the zero between-turns shape", out)
	}
}

// TestDuplicateDeliveryOfAlreadyAdmittedItemDoesNotDispatch proves the
// round-three reconciliation item 2 "belt and braces" guard in genInput
// (runtime/turn_loop.go): a batch that claimItems empties entirely because
// every non-sentinel id in it was already admitted earlier in this
// coordinator's lifetime must NOT mint a fresh, content-free turn that
// dispatches the model again -- it must divert to a between-turn
// queued-continuation pause instead.
//
// The natural race this guards against (a duplicate delivery splitting
// across two GenInput calls) is closed for the reachable, in-process case
// by item 2's primary fix (pushing pushIDs before loop.Run in runTurnLoop),
// so this test forces the residual condition deterministically instead of
// racing for it: it re-pushes an inbox id into the still-live loop from
// INSIDE the very model dispatch that admits it, at which point claimItems
// has already (synchronously, before PrepareAgent/dispatch) recorded that
// id in admittedItems -- so the re-push is guaranteed to be treated as a
// genuine duplicate on the loop's next cycle, not a fresh item.
func TestDuplicateDeliveryOfAlreadyAdmittedItemDoesNotDispatch(t *testing.T) {
	toolCalled := make(chan struct{})
	release := make(chan struct{})
	gate := Tool{
		Name: "gate", Info: &einoschema.ToolInfo{Name: "gate", Desc: "needs approval"},
		InterruptPolicy: pausingInterruptPolicy{},
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			return ToolResult{Output: "decision:" + call.ResumeDecision}, nil
		}),
	}

	var calls int
	var orch *StreamingOrchestrator
	var dupRunID session.RunID
	var dupItemID session.InboxID

	streamer := scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.AgenticMessage, error) {
		calls++
		switch calls {
		case 1:
			// Signal the test goroutine once the model has dispatched the
			// tool call that will pause -- Enqueue must land while the
			// loop is still live, never after.
			close(toolCalled)
			<-release
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-1", "gate", `{}`))}, nil
		case 2:
			return []*einoschema.AgenticMessage{agenticAssistantText("first turn done")}, nil
		case 3:
			// This dispatch is admitting the enqueued item as the run's
			// second turn: genInput's claimItems has already recorded
			// dupItemID in admittedItems by now (it runs synchronously
			// inside genInput, strictly before PrepareAgent/dispatch).
			// Re-pushing it here, mid-dispatch, simulates a genuine
			// duplicate re-delivery landing while this coordinator's loop
			// is still live -- the loop cannot have gone idle yet, so this
			// is deterministic, not a race.
			if entry := orch.liveLoopFor(dupRunID); entry != nil {
				entry.loop.Push(dupItemID)
			}
			return []*einoschema.AgenticMessage{agenticAssistantText("second turn done")}, nil
		default:
			t.Errorf("unexpected dispatch #%d: genInput's duplicate-delivery guard should have prevented a third turn from ever dispatching", calls)
			return []*einoschema.AgenticMessage{agenticAssistantText("should not happen")}, nil
		}
	})
	var cleanup func()
	orch, cleanup = newSQLiteTestOrchestrator(t, streamer)
	defer cleanup()
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	handle, err := orch.Start(context.Background(), Request{SessionID: "sqlite-dup-delivery-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	select {
	case <-toolCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("model never dispatched the first call")
	}
	item, err := orch.Enqueue(context.Background(), "sqlite-dup-delivery-session", EnqueueRequest{
		RunID: handle.RunID(), IdempotencyKey: "dup-delivery-key", Message: TextUserMessage("second message"),
	})
	if err != nil {
		t.Fatalf("Enqueue error = %v", err)
	}
	dupItemID = item.ID
	close(release)

	result := <-handle.Done()
	if result.Status != session.RunPaused || !result.Interrupted {
		t.Fatalf("result = %+v", result)
	}
	pause, ok := <-handle.AwaitPause()
	if !ok || len(pause.InterruptContexts) != 1 {
		t.Fatalf("pause = %+v, ok=%v", pause, ok)
	}
	dupRunID = result.RunID

	resumeHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	// The re-pushed duplicate must divert the loop to a between-turn
	// queued-continuation pause, NOT a third dispatch.
	if resumed.Status != session.RunPaused {
		t.Fatalf("resumed result = %+v, want paused (the duplicate-delivery guard must divert to a queued-continuation pause, not dispatch again)", resumed)
	}
	if calls != 3 {
		t.Fatalf("model dispatch count = %d, want exactly 3 (no spurious third-turn dispatch)", calls)
	}

	turns, err := orch.store.ListTurns(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("ListTurns error = %v", err)
	}
	// Three turns: turn 1 (tool call, paused/resumed), turn 2 (the
	// enqueued item, dispatched), and turn 3 -- promoteQueuedContinuation's
	// own degenerate, NEVER-dispatched turn minted purely to carry the
	// between-turn pause's identity (see its doc comment). Turn 3 existing
	// is expected and is NOT a spurious dispatch: calls stayed at 3 above,
	// so no model call was ever made for it.
	if len(turns) != 3 {
		t.Fatalf("turns = %d, want 3 (turn 1, turn 2 with the enqueued item, and the degenerate pause-carrier turn)", len(turns))
	}
	for _, turn := range turns {
		switch turn.Ordinal {
		case 2:
			if len(turn.UserMessageIDs) != 1 {
				t.Fatalf("second turn UserMessageIDs = %d, want exactly 1", len(turn.UserMessageIDs))
			}
		case 3:
			if len(turn.UserMessageIDs) != 0 {
				t.Fatalf("degenerate pause-carrier turn UserMessageIDs = %d, want 0 (never dispatched)", len(turn.UserMessageIDs))
			}
			if turn.State != session.TurnInterrupted {
				t.Fatalf("degenerate pause-carrier turn state = %q, want interrupted", turn.State)
			}
		}
	}

	// The now-paused run must still be resumable: a further ResumeRun picks
	// the (already-consumed, unaffected) state back up and settles cleanly.
	finalRun, err := orch.store.GetRun(context.Background(), result.RunID)
	if err != nil || finalRun.Status != session.RunPaused {
		t.Fatalf("final run = %+v, err=%v, want status=paused", finalRun, err)
	}
}
