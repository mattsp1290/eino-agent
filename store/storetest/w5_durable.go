package storetest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func inboxBlocks(text string) []session.ContentBlock {
	return []session.ContentBlock{{ID: "block-1", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: text}}}
}

func inboxItem(id session.InboxID, sessionID session.ID, key, text string, at time.Time) session.InboxItem {
	return session.InboxItem{
		ID: id, SessionID: sessionID, IdempotencyKey: key, Blocks: inboxBlocks(text),
		State: session.InboxQueued, CreatedAt: at, UpdatedAt: at,
	}
}

func buildTurn(id session.TurnID, r session.Run, ordinal int64, at time.Time) session.Turn {
	return session.Turn{ID: id, RunID: r.ID, SessionID: r.SessionID, Ordinal: ordinal, State: session.TurnAdmitted, CreatedAt: at}
}

func turnStartedEvent(id session.EventID, r session.Run, turnID session.TurnID, at time.Time) session.EventRecord {
	return session.EventRecord{ID: id, SessionID: r.SessionID, RunID: r.ID, TurnID: turnID, Kind: session.TurnStartedEventKind, Payload: []byte(`{}`), CreatedAt: at}
}

func turnCompletedEvent(id session.EventID, r session.Run, turnID session.TurnID, at time.Time) session.EventRecord {
	return session.EventRecord{ID: id, SessionID: r.SessionID, RunID: r.ID, TurnID: turnID, Kind: session.TurnCompletedEventKind, Payload: []byte(`{}`), CreatedAt: at}
}

func runPausedEvent(id session.EventID, r session.Run, at time.Time) session.EventRecord {
	return session.EventRecord{ID: id, SessionID: r.SessionID, RunID: r.ID, Kind: session.RunPausedEventKind, Payload: []byte(`{}`), CreatedAt: at}
}

func stagedCheckpoint(r session.Run, revision int64, id string, kind session.CheckpointKind, bytes []byte, at time.Time) session.Checkpoint {
	return session.Checkpoint{
		RunID: r.ID, Revision: revision, Kind: kind, AgentFingerprint: "fingerprint", EinoVersion: "v-test",
		CodecVersion: 1, CheckpointID: id, Bytes: bytes, CreatedAt: at,
	}
}

// turnsContract exercises session.Turn admission, completion, and
// interruption: atomic ordinal allocation, inbox claiming, idempotent
// replay, and contradictory-replay rejection.
func turnsContract(t *testing.T, factory Factory) {
	t.Run("turns", func(t *testing.T) {
		t.Run("admit turn enforces ordinal and is atomic", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-turns-atomic")
			r := admitRun(t, ctx, subject.Store, run("run-turns-atomic", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			request := session.AdmitTurnRequest{
				Turn:                 buildTurn("turn-1", r, 1, at),
				UserMessages:         []session.Message{message("turn1-user", s.ID, r.ID, session.RoleUser)},
				AssistantPlaceholder: message("turn1-assistant", s.ID, r.ID, session.RoleAssistant),
				Event:                turnStartedEvent("turn-1-started", r, "turn-1", at),
			}
			result, err := execution.AdmitTurn(ctx, request)
			if err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			if result.Turn.Ordinal != 1 || result.Turn.State != session.TurnAdmitted || result.Event.Kind != session.TurnStartedEventKind {
				t.Fatalf("admit result = %#v", result)
			}
			replay, err := execution.AdmitTurn(ctx, request)
			if err != nil || !reflect.DeepEqual(replay, result) {
				t.Fatalf("admit turn replay = %#v, %v; want %#v", replay, err, result)
			}
			skip := session.AdmitTurnRequest{
				Turn:         buildTurn("turn-3", r, 3, at),
				UserMessages: []session.Message{message("turn3-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-3-started", r, "turn-3", at),
			}
			if _, err := execution.AdmitTurn(ctx, skip); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("out-of-order ordinal = %v, want ErrConflict", err)
			}
			if _, err := subject.Store.GetTurn(ctx, "turn-3"); !errors.Is(err, session.ErrNotFound) {
				t.Fatalf("rejected turn persisted: %v", err)
			}
			batch, err := subject.Store.ListMessages(ctx, s.ID, session.ReplayCursor{Limit: 10})
			if err != nil {
				t.Fatalf("list messages: %v", err)
			}
			for _, msg := range batch.Messages {
				if msg.ID == "turn3-user" {
					t.Fatalf("rejected turn's user message persisted: %#v", msg)
				}
			}
			second := session.AdmitTurnRequest{
				Turn:         buildTurn("turn-2", r, 2, at),
				UserMessages: []session.Message{message("turn2-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-2-started", r, "turn-2", at),
			}
			if _, err := execution.AdmitTurn(ctx, second); err != nil {
				t.Fatalf("admit second turn: %v", err)
			}
			turns, err := subject.Store.ListTurns(ctx, r.ID)
			if err != nil || len(turns) != 2 || turns[0].Ordinal != 1 || turns[1].Ordinal != 2 {
				t.Fatalf("list turns = %#v, %v", turns, err)
			}
		})

		t.Run("admit turn claims queued inbox items exactly once", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-turns-inbox")
			r := admitRun(t, ctx, subject.Store, run("run-turns-inbox", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			enqueued, err := subject.Store.EnqueueInbox(ctx, inboxItem("inbox-1", s.ID, "key-1", "hello", at), session.DefaultContentLimits())
			if err != nil {
				t.Fatalf("enqueue inbox: %v", err)
			}
			request := session.AdmitTurnRequest{
				Turn:         buildTurn("turn-inbox-1", r, 1, at),
				UserMessages: []session.Message{message("turn-inbox-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-inbox-started", r, "turn-inbox-1", at),
				InboxIDs:     []session.InboxID{enqueued.ID},
			}
			if _, err := execution.AdmitTurn(ctx, request); err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			items, err := subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxConsumed})
			if err != nil || len(items) != 1 || items[0].TurnID != "turn-inbox-1" {
				t.Fatalf("consumed inbox = %#v, %v", items, err)
			}
			other := session.AdmitTurnRequest{
				Turn:         buildTurn("turn-inbox-2", r, 2, at),
				UserMessages: []session.Message{message("turn-inbox-user-2", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-inbox-started-2", r, "turn-inbox-2", at),
				InboxIDs:     []session.InboxID{enqueued.ID},
			}
			if _, err := execution.AdmitTurn(ctx, other); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("reclaiming consumed inbox item = %v, want ErrConflict", err)
			}
		})

		t.Run("complete turn settles state and inbox without touching run status, and is idempotent", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-turns-complete")
			r := admitRun(t, ctx, subject.Store, run("run-turns-complete", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			enqueued, err := subject.Store.EnqueueInbox(ctx, inboxItem("inbox-complete", s.ID, "key-complete", "hi", at), session.DefaultContentLimits())
			if err != nil {
				t.Fatalf("enqueue inbox: %v", err)
			}
			admitted, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:         buildTurn("turn-complete", r, 1, at),
				UserMessages: []session.Message{message("turn-complete-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-complete-started", r, "turn-complete", at),
				InboxIDs:     []session.InboxID{enqueued.ID},
			})
			if err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			completedAt := at.Add(time.Second)
			completeRequest := session.CompleteTurnRequest{
				TurnID:             admitted.Turn.ID,
				ResponseMessageIDs: []session.MessageID{"turn-complete-assistant"},
				Event:              turnCompletedEvent("turn-complete-finished", r, admitted.Turn.ID, completedAt),
			}
			result, err := execution.CompleteTurn(ctx, completeRequest)
			if err != nil {
				t.Fatalf("complete turn: %v", err)
			}
			if result.Turn.State != session.TurnCompleted || !result.Turn.FinishedAt.Equal(completedAt.UTC()) {
				t.Fatalf("completed turn = %#v", result.Turn)
			}
			unchanged, err := subject.Store.GetRun(ctx, r.ID)
			if err != nil || unchanged.Status != r.Status {
				t.Fatalf("run status after complete turn = %#v, %v; want unchanged %q", unchanged, err, r.Status)
			}
			items, err := subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxCompleted})
			if err != nil || len(items) != 1 || items[0].ID != enqueued.ID {
				t.Fatalf("completed inbox = %#v, %v", items, err)
			}
			replay, err := execution.CompleteTurn(ctx, completeRequest)
			if err != nil || !reflect.DeepEqual(replay, result) {
				t.Fatalf("complete turn replay = %#v, %v; want %#v", replay, err, result)
			}
			contradiction := completeRequest
			contradiction.ResponseMessageIDs = []session.MessageID{"different-assistant"}
			if _, err := execution.CompleteTurn(ctx, contradiction); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("contradictory complete turn = %v, want ErrConflict", err)
			}
		})

		t.Run("interrupt turn settles state and inbox, and is idempotent", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-turns-interrupt")
			r := admitRun(t, ctx, subject.Store, run("run-turns-interrupt", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			enqueued, err := subject.Store.EnqueueInbox(ctx, inboxItem("inbox-interrupt", s.ID, "key-interrupt", "hi", at), session.DefaultContentLimits())
			if err != nil {
				t.Fatalf("enqueue inbox: %v", err)
			}
			admitted, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:         buildTurn("turn-interrupt", r, 1, at),
				UserMessages: []session.Message{message("turn-interrupt-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-interrupt-started", r, "turn-interrupt", at),
				InboxIDs:     []session.InboxID{enqueued.ID},
			})
			if err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			interruptedAt := at.Add(time.Second)
			event := session.EventRecord{ID: "turn-interrupt-event", SessionID: s.ID, RunID: r.ID, TurnID: admitted.Turn.ID, Kind: "turn_interrupt_test", Payload: []byte(`{}`), CreatedAt: interruptedAt}
			result, err := execution.InterruptTurn(ctx, session.InterruptTurnRequest{TurnID: admitted.Turn.ID, Event: event})
			if err != nil {
				t.Fatalf("interrupt turn: %v", err)
			}
			if result.Turn.State != session.TurnInterrupted {
				t.Fatalf("interrupted turn = %#v", result.Turn)
			}
			items, err := subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxInterrupted})
			if err != nil || len(items) != 1 || items[0].ID != enqueued.ID {
				t.Fatalf("interrupted inbox = %#v, %v", items, err)
			}
			replay, err := execution.InterruptTurn(ctx, session.InterruptTurnRequest{TurnID: admitted.Turn.ID, Event: event})
			if err != nil || !reflect.DeepEqual(replay, result) {
				t.Fatalf("interrupt turn replay = %#v, %v; want %#v", replay, err, result)
			}
		})

		t.Run("reconcile interrupted turn carries consumed inbox forward as interrupted and is idempotent", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-turns-reconcile")
			r := admitRun(t, ctx, subject.Store, run("run-turns-reconcile", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			enqueued, err := subject.Store.EnqueueInbox(ctx, inboxItem("inbox-reconcile", s.ID, "key-reconcile", "hi", at), session.DefaultContentLimits())
			if err != nil {
				t.Fatalf("enqueue inbox: %v", err)
			}
			admitted, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:         buildTurn("turn-reconcile", r, 1, at),
				UserMessages: []session.Message{message("turn-reconcile-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-reconcile-started", r, "turn-reconcile", at),
				InboxIDs:     []session.InboxID{enqueued.ID},
			})
			if err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			reconciledAt := at.Add(time.Second)
			event := session.EventRecord{ID: "turn-reconcile-event", SessionID: s.ID, RunID: r.ID, TurnID: admitted.Turn.ID, Kind: "turn_reconcile_test", Payload: []byte(`{}`), CreatedAt: reconciledAt}
			result, err := execution.ReconcileInterruptedTurn(ctx, session.ReconcileInterruptedTurnRequest{TurnID: admitted.Turn.ID, Event: event})
			if err != nil {
				t.Fatalf("reconcile interrupted turn: %v", err)
			}
			if result.Turn.State != session.TurnInterrupted {
				t.Fatalf("reconciled turn = %#v", result.Turn)
			}
			// Round-four reconciliation item 1 (CR-C1): unlike the shipped
			// round-three shape, the consumed item must NOT be requeued --
			// it settles InboxInterrupted, exactly like InterruptTurn,
			// still linked to the turn that already durably committed its
			// content. Requeuing it would let a fresh AdmitTurn re-consume
			// the same content into a second turn and duplicate it in
			// provider history.
			items, err := subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxInterrupted})
			if err != nil || len(items) != 1 || items[0].ID != enqueued.ID || items[0].TurnID != admitted.Turn.ID {
				t.Fatalf("interrupted inbox = %#v, %v", items, err)
			}
			if queued, err := subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxQueued}); err != nil || len(queued) != 0 {
				t.Fatalf("queued inbox = %#v, %v, want none (never requeued)", queued, err)
			}
			replay, err := execution.ReconcileInterruptedTurn(ctx, session.ReconcileInterruptedTurnRequest{TurnID: admitted.Turn.ID, Event: event})
			if err != nil || !reflect.DeepEqual(replay, result) {
				t.Fatalf("reconcile interrupted turn replay = %#v, %v; want %#v", replay, err, result)
			}
			items, err = subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxInterrupted})
			if err != nil || len(items) != 1 || items[0].ID != enqueued.ID {
				t.Fatalf("interrupted inbox after replay = %#v, %v", items, err)
			}
		})

		t.Run("resume interrupted turn resumes the same turn and inbox item, and is idempotent", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-turns-resume")
			r := admitRun(t, ctx, subject.Store, run("run-turns-resume", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			enqueued, err := subject.Store.EnqueueInbox(ctx, inboxItem("inbox-resume", s.ID, "key-resume", "hi", at), session.DefaultContentLimits())
			if err != nil {
				t.Fatalf("enqueue inbox: %v", err)
			}
			admitted, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:         buildTurn("turn-resume", r, 1, at),
				UserMessages: []session.Message{message("turn-resume-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-resume-started", r, "turn-resume", at),
				InboxIDs:     []session.InboxID{enqueued.ID},
			})
			if err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			reconciledAt := at.Add(time.Second)
			reconcileEvent := session.EventRecord{ID: "turn-resume-reconcile-event", SessionID: s.ID, RunID: r.ID, TurnID: admitted.Turn.ID, Kind: "turn_reconcile_test", Payload: []byte(`{}`), CreatedAt: reconciledAt}
			if _, err := execution.ReconcileInterruptedTurn(ctx, session.ReconcileInterruptedTurnRequest{TurnID: admitted.Turn.ID, Event: reconcileEvent}); err != nil {
				t.Fatalf("reconcile interrupted turn: %v", err)
			}
			resumedAt := reconciledAt.Add(time.Second)
			result, err := execution.ResumeInterruptedTurn(ctx, session.ResumeInterruptedTurnRequest{TurnID: admitted.Turn.ID, ResumedAt: resumedAt})
			if err != nil {
				t.Fatalf("resume interrupted turn: %v", err)
			}
			if result.Turn.ID != admitted.Turn.ID || result.Turn.State != session.TurnRunning {
				t.Fatalf("resumed turn = %#v, want state=running, same TurnID", result.Turn)
			}
			// The inbox item is back to InboxConsumed, still linked to the
			// SAME turn -- never re-admitted as a fresh AdmitTurn.
			items, err := subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxConsumed})
			if err != nil || len(items) != 1 || items[0].ID != enqueued.ID || items[0].TurnID != admitted.Turn.ID {
				t.Fatalf("resumed inbox = %#v, %v", items, err)
			}
			replay, err := execution.ResumeInterruptedTurn(ctx, session.ResumeInterruptedTurnRequest{TurnID: admitted.Turn.ID, ResumedAt: resumedAt})
			if err != nil || !reflect.DeepEqual(replay, result) {
				t.Fatalf("resume interrupted turn replay = %#v, %v; want %#v", replay, err, result)
			}
			// The resumed (TurnRunning) turn now completes through the
			// ordinary CompleteTurn path, under the SAME TurnID, and its
			// inbox item reaches InboxCompleted exactly once.
			completedAt := resumedAt.Add(time.Second)
			completeResult, err := execution.CompleteTurn(ctx, session.CompleteTurnRequest{
				TurnID: admitted.Turn.ID, ResponseMessageIDs: []session.MessageID{"turn-resume-assistant"},
				Event: turnCompletedEvent("turn-resume-finished", r, admitted.Turn.ID, completedAt),
			})
			if err != nil {
				t.Fatalf("complete resumed turn: %v", err)
			}
			if completeResult.Turn.State != session.TurnCompleted {
				t.Fatalf("completed resumed turn = %#v", completeResult.Turn)
			}
			completedItems, err := subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxCompleted})
			if err != nil || len(completedItems) != 1 || completedItems[0].ID != enqueued.ID {
				t.Fatalf("completed inbox = %#v, %v, want exactly the one item, completed once", completedItems, err)
			}
			// ResumeInterruptedTurn refuses a turn that is not TurnInterrupted
			// (e.g. already TurnCompleted here).
			if _, err := execution.ResumeInterruptedTurn(ctx, session.ResumeInterruptedTurnRequest{TurnID: admitted.Turn.ID, ResumedAt: completedAt.Add(time.Second)}); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("resume of a completed turn = %v, want ErrConflict", err)
			}
		})

		// Round-six reconciliation item 2 (round-seven fix-pass-6 item 2,
		// MG-I4/RT): a run's own terminal SettleRun must terminalize every
		// turn it leaves behind still TurnAdmitted, TurnRunning, or
		// TurnInterrupted -- including a content-free carrier -- so no turn
		// is ever left implying it might still run again once its run
		// cannot be resumed. Exercised directly against the shared
		// sqlstore (both dialects, via this contract).
		t.Run("settle run terminalizes every turn left admitted, running, or interrupted", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-turns-settle-terminalize")
			r := admitRun(t, ctx, subject.Store, run("run-turns-settle-terminalize", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)

			// Turn 1: admitted, completed for real -- must be left
			// completely untouched by the settlement below.
			completeInbox, err := subject.Store.EnqueueInbox(ctx, inboxItem("inbox-settle-complete", s.ID, "key-settle-complete", "hi", at), session.DefaultContentLimits())
			if err != nil {
				t.Fatalf("enqueue inbox (complete): %v", err)
			}
			completedTurn, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:         buildTurn("turn-settle-complete", r, 1, at),
				UserMessages: []session.Message{message("turn-settle-complete-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-settle-complete-started", r, "turn-settle-complete", at),
				InboxIDs:     []session.InboxID{completeInbox.ID},
			})
			if err != nil {
				t.Fatalf("admit turn (complete): %v", err)
			}
			if _, err := execution.CompleteTurn(ctx, session.CompleteTurnRequest{
				TurnID: completedTurn.Turn.ID, ResponseMessageIDs: []session.MessageID{"turn-settle-complete-assistant"},
				Event: turnCompletedEvent("turn-settle-complete-finished", r, completedTurn.Turn.ID, at.Add(time.Second)),
			}); err != nil {
				t.Fatalf("complete turn: %v", err)
			}

			// Turn 2: admitted with real content, never redriven, never
			// settled -- exactly what a graceful stop landing before its
			// first dispatch leaves behind (MG-I4's reproduction).
			danglingInbox, err := subject.Store.EnqueueInbox(ctx, inboxItem("inbox-settle-dangling", s.ID, "key-settle-dangling", "hi", at), session.DefaultContentLimits())
			if err != nil {
				t.Fatalf("enqueue inbox (dangling): %v", err)
			}
			danglingTurn, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:         buildTurn("turn-settle-dangling", r, 2, at.Add(2*time.Second)),
				UserMessages: []session.Message{message("turn-settle-dangling-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-settle-dangling-started", r, "turn-settle-dangling", at.Add(2*time.Second)),
				InboxIDs:     []session.InboxID{danglingInbox.ID},
			})
			if err != nil {
				t.Fatalf("admit turn (dangling): %v", err)
			}

			// Turn 3: a content-free carrier, already durably interrupted
			// (promoteQueuedContinuation's own shape) -- must ALSO be
			// terminalized, not merely left interrupted.
			carrierTurn, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:  buildTurn("turn-settle-carrier", r, 3, at.Add(3*time.Second)),
				Event: turnStartedEvent("turn-settle-carrier-started", r, "turn-settle-carrier", at.Add(3*time.Second)),
			})
			if err != nil {
				t.Fatalf("admit turn (carrier): %v", err)
			}
			carrierEvent := session.EventRecord{ID: "turn-settle-carrier-interrupt", SessionID: s.ID, RunID: r.ID, TurnID: carrierTurn.Turn.ID, Kind: session.RunPausedEventKind, CreatedAt: at.Add(4 * time.Second)}
			if _, err := execution.InterruptTurn(ctx, session.InterruptTurnRequest{TurnID: carrierTurn.Turn.ID, Event: carrierEvent}); err != nil {
				t.Fatalf("interrupt turn (carrier): %v", err)
			}

			finishedAt := at.Add(5 * time.Second)
			settleRequest := session.SettleRunRequest{
				Settlement: session.RunSettlement{Status: session.RunCompleted, FinishedAt: finishedAt},
				Event:      session.RunSettlementEvent{ID: "run-settle-terminalize-finished"},
			}
			if _, err := execution.SettleRun(ctx, settleRequest); err != nil {
				t.Fatalf("settle run: %v", err)
			}

			gotCompleted, err := subject.Store.GetTurn(ctx, completedTurn.Turn.ID)
			if err != nil || gotCompleted.State != session.TurnCompleted {
				t.Fatalf("completed turn after settlement = %#v, err=%v, want untouched completed", gotCompleted, err)
			}
			gotDangling, err := subject.Store.GetTurn(ctx, danglingTurn.Turn.ID)
			if err != nil || gotDangling.State != session.TurnFailed || !gotDangling.FinishedAt.Equal(finishedAt.UTC()) {
				t.Fatalf("dangling turn after settlement = %#v, err=%v, want failed at %s", gotDangling, err, finishedAt)
			}
			gotCarrier, err := subject.Store.GetTurn(ctx, carrierTurn.Turn.ID)
			if err != nil || gotCarrier.State != session.TurnFailed || !gotCarrier.FinishedAt.Equal(finishedAt.UTC()) {
				t.Fatalf("carrier turn after settlement = %#v, err=%v, want failed at %s", gotCarrier, err, finishedAt)
			}

			// No turn under this now-terminal run may remain admitted,
			// running, or interrupted.
			turns, err := subject.Store.ListTurns(ctx, r.ID)
			if err != nil {
				t.Fatalf("list turns: %v", err)
			}
			for _, turn := range turns {
				if turn.State == session.TurnAdmitted || turn.State == session.TurnRunning || turn.State == session.TurnInterrupted {
					t.Fatalf("turn %+v left non-terminal under a terminal run", turn)
				}
			}

			// The dangling turn's own inbox item -- still InboxConsumed --
			// is carried forward as InboxInterrupted, exactly like
			// ReconcileInterruptedTurn's own inbox step, never requeued.
			interrupted, err := subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxInterrupted})
			if err != nil || len(interrupted) != 1 || interrupted[0].ID != danglingInbox.ID {
				t.Fatalf("interrupted inbox after settlement = %#v, %v, want exactly the dangling item", interrupted, err)
			}
			if queued, err := subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxQueued}); err != nil || len(queued) != 0 {
				t.Fatalf("queued inbox after settlement = %#v, %v, want none (never requeued)", queued, err)
			}

			// Idempotent replay of the identical settlement must not error
			// and must not disturb the now-terminalized turns.
			if _, err := execution.SettleRun(ctx, settleRequest); err != nil {
				t.Fatalf("settle run replay: %v", err)
			}
			replayDangling, err := subject.Store.GetTurn(ctx, danglingTurn.Turn.ID)
			if err != nil || replayDangling.State != session.TurnFailed {
				t.Fatalf("dangling turn after replayed settlement = %#v, err=%v, want still failed", replayDangling, err)
			}
		})
	})
}

// inboxContract exercises session.InboxItem admission: idempotent replay by
// (session, IdempotencyKey), contradictory-payload rejection, state
// filtering, and content bounds.
func inboxContract(t *testing.T, factory Factory) {
	t.Run("inbox", func(t *testing.T) {
		t.Run("enqueue is idempotent by key and rejects contradictory payloads", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-inbox-idempotent")
			at := time.Now().UTC()
			item := inboxItem("inbox-idem-1", s.ID, "idem-key", "hello", at)
			first, err := subject.Store.EnqueueInbox(ctx, item, session.DefaultContentLimits())
			if err != nil {
				t.Fatalf("enqueue inbox: %v", err)
			}
			retry := item
			retry.ID = "inbox-idem-2"
			second, err := subject.Store.EnqueueInbox(ctx, retry, session.DefaultContentLimits())
			if err != nil || second.ID != first.ID {
				t.Fatalf("idempotent enqueue = %#v, %v; want id %s", second, err, first.ID)
			}
			contradiction := item
			contradiction.ID = "inbox-idem-3"
			contradiction.Blocks = inboxBlocks("different")
			if _, err := subject.Store.EnqueueInbox(ctx, contradiction, session.DefaultContentLimits()); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("contradictory enqueue = %v, want ErrConflict", err)
			}
		})

		t.Run("list inbox filters by state", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-inbox-list")
			at := time.Now().UTC()
			if _, err := subject.Store.EnqueueInbox(ctx, inboxItem("inbox-list-1", s.ID, "list-key-1", "a", at), session.DefaultContentLimits()); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			if _, err := subject.Store.EnqueueInbox(ctx, inboxItem("inbox-list-2", s.ID, "list-key-2", "b", at.Add(time.Second)), session.DefaultContentLimits()); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			all, err := subject.Store.ListInbox(ctx, s.ID, nil)
			if err != nil || len(all) != 2 {
				t.Fatalf("list all inbox = %#v, %v", all, err)
			}
			queued, err := subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxQueued})
			if err != nil || len(queued) != 2 {
				t.Fatalf("list queued inbox = %#v, %v", queued, err)
			}
			consumed, err := subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxConsumed})
			if err != nil || len(consumed) != 0 {
				t.Fatalf("list consumed inbox = %#v, %v", consumed, err)
			}
		})

		t.Run("bounds reject content over the configured limit", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-inbox-bounds")
			at := time.Now().UTC()
			limits := session.ContentLimits{MaxMessageBytes: 8, MaxBlocks: 8, MaxBlockBytes: 8}
			oversize := inboxItem("inbox-bounds-1", s.ID, "bounds-key", strings.Repeat("x", 4096), at)
			if _, err := subject.Store.EnqueueInbox(ctx, oversize, limits); !errors.Is(err, session.ErrContentTooLarge) {
				t.Fatalf("oversize enqueue = %v, want ErrContentTooLarge", err)
			}
		})

		// Round-three reconciliation item 7 (RD-I3): EnqueueInboxForRun is a
		// new session.Store method with four distinct outcomes, previously
		// pinned by nothing but the in-memory runtime fixture the same
		// commit wrote -- exactly the divergence class item 7 was raised
		// for, re-created while an earlier instance of it was being fixed.
		t.Run("enqueue for run reports created exactly once and enforces run identity", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-inbox-for-run")
			other := createSession(t, ctx, subject.Store, "session-inbox-for-run-other")
			r := admitRun(t, ctx, subject.Store, run("run-inbox-for-run", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			item := inboxItem("inbox-for-run-1", s.ID, "for-run-key-1", "hello", at)
			first, created, err := subject.Store.EnqueueInboxForRun(ctx, r.ID, item, session.DefaultContentLimits())
			if err != nil || !created {
				t.Fatalf("enqueue for run = %#v, created=%v, %v; want created=true", first, created, err)
			}
			replay := item
			replay.ID = "inbox-for-run-1-replay"
			replayed, replayCreated, err := subject.Store.EnqueueInboxForRun(ctx, r.ID, replay, session.DefaultContentLimits())
			if err != nil || replayCreated || replayed.ID != first.ID {
				t.Fatalf("enqueue for run replay = %#v, created=%v, %v; want created=false, id=%s", replayed, replayCreated, err, first.ID)
			}
			contradiction := item
			contradiction.ID = "inbox-for-run-1-conflict"
			contradiction.Blocks = inboxBlocks("different")
			if _, _, err := subject.Store.EnqueueInboxForRun(ctx, r.ID, contradiction, session.DefaultContentLimits()); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("contradictory enqueue for run = %v, want ErrConflict", err)
			}
			wrongSession := inboxItem("inbox-for-run-wrong-session", other.ID, "for-run-key-wrong-session", "hello", at)
			if _, _, err := subject.Store.EnqueueInboxForRun(ctx, r.ID, wrongSession, session.DefaultContentLimits()); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("enqueue for run with mismatched session = %v, want ErrConflict", err)
			}
			if _, _, err := subject.Store.EnqueueInboxForRun(ctx, "no-such-run", inboxItem("inbox-for-run-missing", s.ID, "for-run-key-missing", "hello", at), session.DefaultContentLimits()); !errors.Is(err, session.ErrRunClosed) {
				t.Fatalf("enqueue for missing run = %v, want ErrRunClosed", err)
			}
			// RunInterrupted, not RunCompleted: the still-queued `first`
			// item above (never consumed by any turn) would otherwise trip
			// SettleRun's own RunCompleted-scoped queued-inbox guard (see
			// the "settle run completed refuses..." case below) -- this
			// case is only about EnqueueInboxForRun's terminal-run check.
			if _, err := execution.SettleRun(ctx, session.SettleRunRequest{
				Settlement: session.RunSettlement{Status: session.RunInterrupted, FinishedAt: at.Add(time.Second), Error: "test terminal"},
				Event:      session.RunSettlementEvent{ID: "inbox-for-run-settle-event"},
			}); err != nil {
				t.Fatalf("settle run interrupted: %v", err)
			}
			terminal := inboxItem("inbox-for-run-terminal", s.ID, "for-run-key-terminal", "hello", at)
			if _, _, err := subject.Store.EnqueueInboxForRun(ctx, r.ID, terminal, session.DefaultContentLimits()); !errors.Is(err, session.ErrRunClosed) {
				t.Fatalf("enqueue for terminal run = %v, want ErrRunClosed", err)
			}
		})

		// Round-three reconciliation item 7 (RD-I3): the residual
		// terminal-settlement-race closer (SettleRun's queued-inbox guard)
		// was mirrored by hand in the runtime fixture but pinned by no
		// store contract, so it ran against PostgreSQL never.
		t.Run("settle run completed refuses while the session has a queued inbox item", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-settle-queued")
			r := admitRun(t, ctx, subject.Store, run("run-settle-queued", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			if _, err := subject.Store.EnqueueInbox(ctx, inboxItem("inbox-settle-queued", s.ID, "settle-queued-key", "hi", at), session.DefaultContentLimits()); err != nil {
				t.Fatalf("enqueue inbox: %v", err)
			}
			completedSettlement := session.SettleRunRequest{
				Settlement: session.RunSettlement{Status: session.RunCompleted, FinishedAt: at.Add(time.Second)},
				Event:      session.RunSettlementEvent{ID: "settle-queued-completed-event"},
			}
			if _, err := execution.SettleRun(ctx, completedSettlement); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("settle completed with queued inbox = %v, want ErrConflict", err)
			}
			// The guard is deliberately scoped to RunCompleted: the same
			// run can still settle failed/interrupted, leaving the item
			// simply queued for the session's next Start.
			failedSettlement := session.SettleRunRequest{
				Settlement: session.RunSettlement{Status: session.RunFailed, FinishedAt: at.Add(time.Second), Error: "injected"},
				Event:      session.RunSettlementEvent{ID: "settle-queued-failed-event"},
			}
			result, err := execution.SettleRun(ctx, failedSettlement)
			if err != nil || result.Run.Status != session.RunFailed {
				t.Fatalf("settle failed with queued inbox = %#v, %v, want success", result, err)
			}
			items, err := subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxQueued})
			if err != nil || len(items) != 1 {
				t.Fatalf("queued inbox after failed settlement = %#v, %v, want still queued", items, err)
			}
		})
	})
}

// checkpointContract exercises session.Checkpoint staging: atomic insert,
// idempotent replay, contradictory-replay rejection, byte bounds, and
// retire-never-deletes-newer-revision for both the fenced (running) and
// unfenced (terminal) retire paths.
func checkpointContract(t *testing.T, factory Factory) {
	t.Run("checkpoints", func(t *testing.T) {
		t.Run("stage checkpoint is idempotent and unpromoted revisions are invisible to reads", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-checkpoint-stage")
			r := admitRun(t, ctx, subject.Store, run("run-checkpoint-stage", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			cp := stagedCheckpoint(r, 1, "cp-1", session.CheckpointKindRunner, []byte("state-bytes"), at)
			staged, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp})
			if err != nil {
				t.Fatalf("stage checkpoint: %v", err)
			}
			if staged.Promoted {
				t.Fatalf("staged checkpoint promoted = %v, want false", staged.Promoted)
			}
			replay, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp})
			if err != nil || !reflect.DeepEqual(replay, staged) {
				t.Fatalf("stage replay = %#v, %v; want %#v", replay, err, staged)
			}
			contradiction := cp
			contradiction.Bytes = []byte("different")
			if _, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: contradiction}); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("contradictory stage = %v, want ErrConflict", err)
			}
			if _, found, err := subject.Store.ReadPromotedCheckpoint(ctx, r.ID); err != nil || found {
				t.Fatalf("unpromoted checkpoint visible: found=%v err=%v", found, err)
			}
		})

		t.Run("bounds reject bytes over MaxCheckpointBytes", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-checkpoint-bounds")
			r := admitRun(t, ctx, subject.Store, run("run-checkpoint-bounds", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			cp := stagedCheckpoint(r, 1, "cp-bounds", session.CheckpointKindRunner, make([]byte, 32), at)
			if _, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp, MaxBytes: 16}); !errors.Is(err, session.ErrCheckpointTooLarge) {
				t.Fatalf("oversize checkpoint = %v, want ErrCheckpointTooLarge", err)
			}
			if _, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp, MaxBytes: -1}); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("negative MaxBytes = %v, want ErrConflict", err)
			}
		})

		t.Run("retire never deletes a newer revision and is idempotent", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-checkpoint-retire")
			r := admitRun(t, ctx, subject.Store, run("run-checkpoint-retire", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			for revision := int64(1); revision <= 3; revision++ {
				cp := stagedCheckpoint(r, revision, fmt.Sprintf("cp-%d", revision), session.CheckpointKindRunner, []byte("bytes"), at)
				if _, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp}); err != nil {
					t.Fatalf("stage revision %d: %v", revision, err)
				}
			}
			if err := execution.RetireCheckpoints(ctx, 2); err != nil {
				t.Fatalf("retire checkpoints: %v", err)
			}
			if err := execution.RetireCheckpoints(ctx, 2); err != nil {
				t.Fatalf("retire checkpoints again: %v", err)
			}
			// Revision 3 (newer than the retire bound) must still be
			// stageable identically to its original value, proving it was
			// never deleted; revisions 1-2 must be gone, so re-staging them
			// now succeeds as a fresh insert rather than an idempotent replay.
			cp3 := stagedCheckpoint(r, 3, "cp-3", session.CheckpointKindRunner, []byte("bytes"), at)
			if _, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp3}); err != nil {
				t.Fatalf("revision 3 was altered or deleted by retire: %v", err)
			}
		})

		t.Run("retire run checkpoints requires a terminal run and is idempotent", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-checkpoint-retire-terminal")
			r := admitRun(t, ctx, subject.Store, run("run-checkpoint-retire-terminal", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			cp := stagedCheckpoint(r, 1, "cp-terminal-1", session.CheckpointKindRunner, []byte("bytes"), at)
			if _, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp}); err != nil {
				t.Fatalf("stage checkpoint: %v", err)
			}
			if err := subject.Store.RetireRunCheckpoints(ctx, r.ID, 1); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("retire on running run = %v, want ErrConflict", err)
			}
			terminal := r
			terminal.Status, terminal.FinishedAt = session.RunCompleted, at.Add(time.Second)
			if _, err := execution.SettleRun(ctx, settleRunRequest(terminal)); err != nil {
				t.Fatalf("settle run: %v", err)
			}
			if err := subject.Store.RetireRunCheckpoints(ctx, r.ID, 1); err != nil {
				t.Fatalf("retire terminal run checkpoints: %v", err)
			}
			if err := subject.Store.RetireRunCheckpoints(ctx, r.ID, 1); err != nil {
				t.Fatalf("retire terminal run checkpoints again: %v", err)
			}
		})
	})
}

// pausedRunContract exercises the durable pause boundary: PromotePause's
// atomicity across checkpoint/turn/inbox/run, the dead fence after
// promotion, immediate ClaimRun on a paused run, continued session
// reservation while paused, and paused-as-nonterminal observation.
func pausedRunContract(t *testing.T, factory Factory) {
	t.Run("paused runs", func(t *testing.T) {
		t.Run("promote pause is atomic and the old fence can no longer write", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-pause-atomic")
			r := admitRun(t, ctx, subject.Store, run("run-pause-atomic", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			admitted, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:         buildTurn("turn-pause", r, 1, at),
				UserMessages: []session.Message{message("turn-pause-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-pause-started", r, "turn-pause", at),
			})
			if err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			enqueued, err := subject.Store.EnqueueInbox(ctx, inboxItem("inbox-pause", s.ID, "pause-key", "queued next", at), session.DefaultContentLimits())
			if err != nil {
				t.Fatalf("enqueue inbox: %v", err)
			}
			cp := stagedCheckpoint(r, 1, "cp-pause", session.CheckpointKindLoop, []byte("pause-bytes"), at)
			if _, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp}); err != nil {
				t.Fatalf("stage checkpoint: %v", err)
			}
			pausedAt := at.Add(time.Second)
			pauseEvent := runPausedEvent("run-pause-event", r, pausedAt)
			promoteRequest := session.PromotePauseRequest{Revision: 1, TurnID: admitted.Turn.ID, InboxIDs: []session.InboxID{enqueued.ID}, Event: pauseEvent}
			result, err := execution.PromotePause(ctx, promoteRequest)
			if err != nil {
				t.Fatalf("promote pause: %v", err)
			}
			if result.Run.Status != session.RunPaused {
				t.Fatalf("paused run status = %q", result.Run.Status)
			}
			if result.Run.LeaseUntil.After(time.Now().UTC()) {
				t.Fatalf("paused run retains a live lease: %v", result.Run.LeaseUntil)
			}
			if !result.Checkpoint.Promoted {
				t.Fatalf("checkpoint not promoted: %#v", result.Checkpoint)
			}
			if result.Turn.State != session.TurnInterrupted {
				t.Fatalf("turn not interrupted: %#v", result.Turn)
			}
			items, err := subject.Store.ListInbox(ctx, s.ID, []session.InboxState{session.InboxInterrupted})
			if err != nil || len(items) != 1 || items[0].ID != enqueued.ID {
				t.Fatalf("interrupted inbox = %#v, %v", items, err)
			}
			promoted, found, err := subject.Store.ReadPromotedCheckpoint(ctx, r.ID)
			if err != nil || !found || promoted.Revision != 1 {
				t.Fatalf("read promoted checkpoint = %#v, %v, %v", promoted, found, err)
			}
			if _, err := execution.AppendEvent(ctx, eventRecord("post-pause-event", s.ID, r.ID, 1)); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("post-pause AppendEvent = %v, want ErrConflict", err)
			}
			if _, err := execution.RenewRunLease(ctx, time.Minute); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("post-pause RenewRunLease = %v, want ErrConflict", err)
			}
			if _, err := execution.PromotePause(ctx, promoteRequest); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("promote pause replay = %v, want ErrConflict", err)
			}
			if _, err := subject.Store.AdmitRun(ctx, run("run-pause-atomic-second", s.ID, "owner-2"), time.Minute); !errors.Is(err, session.ErrSessionBusy) {
				t.Fatalf("admit while paused = %v, want ErrSessionBusy", err)
			}
		})

		// Item 7 (TL-6)'s unmet second half: the fixture in
		// runtime/admission_store_test.go was aligned to
		// store/internal/sqlstore's interruptInboxItems (an unknown inbox ID,
		// or one in a state that is neither queued nor consumed, is a
		// protocol error), but nothing pinned that behaviour against the
		// real stores -- so the fixture could silently drift from them again
		// with no test catching it (exactly the divergence class that let
		// round-one Critical 1 ship green).
		t.Run("promote pause rejects an inbox id with no durable row", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-pause-unknown-inbox")
			r := admitRun(t, ctx, subject.Store, run("run-pause-unknown-inbox", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			admitted, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:         buildTurn("turn-pause-unknown-inbox", r, 1, at),
				UserMessages: []session.Message{message("turn-pause-unknown-inbox-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-pause-unknown-inbox-started", r, "turn-pause-unknown-inbox", at),
			})
			if err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			cp := stagedCheckpoint(r, 1, "cp-pause-unknown-inbox", session.CheckpointKindLoop, []byte("pause-bytes"), at)
			if _, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp}); err != nil {
				t.Fatalf("stage checkpoint: %v", err)
			}
			pausedAt := at.Add(time.Second)
			_, err = execution.PromotePause(ctx, session.PromotePauseRequest{
				Revision: 1, TurnID: admitted.Turn.ID,
				InboxIDs: []session.InboxID{"no-such-inbox-row"},
				Event:    runPausedEvent("run-pause-unknown-inbox-event", r, pausedAt),
			})
			if !errors.Is(err, session.ErrConflict) {
				t.Fatalf("promote pause with unknown inbox id = %v, want ErrConflict", err)
			}
			// The whole transaction must have rolled back: the run must not
			// have been left paused (or the checkpoint promoted, or the turn
			// interrupted) by a promotion that failed on its inbox step.
			got, err := subject.Store.GetRun(ctx, r.ID)
			if err != nil {
				t.Fatalf("get run after rolled-back promote: %v", err)
			}
			if got.Status == session.RunPaused {
				t.Fatalf("run after rolled-back promote = %#v, want not paused", got)
			}
			if _, found, err := subject.Store.ReadPromotedCheckpoint(ctx, r.ID); err != nil || found {
				t.Fatalf("promoted checkpoint after rolled-back promote: found=%v err=%v, want not found", found, err)
			}
		})

		t.Run("claim run on a paused run succeeds immediately without waiting for lease expiry", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-pause-claim")
			r := admitRun(t, ctx, subject.Store, run("run-pause-claim", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			admitted, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:         buildTurn("turn-pause-claim", r, 1, at),
				UserMessages: []session.Message{message("turn-pause-claim-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-pause-claim-started", r, "turn-pause-claim", at),
			})
			if err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			cp := stagedCheckpoint(r, 1, "cp-pause-claim", session.CheckpointKindLoop, []byte("bytes"), at)
			if _, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp}); err != nil {
				t.Fatalf("stage checkpoint: %v", err)
			}
			// Renew the lease far into the future first: the paused claim
			// must not depend on that lease ever expiring.
			if _, err := execution.RenewRunLease(ctx, time.Hour); err != nil {
				t.Fatalf("renew lease: %v", err)
			}
			pauseEvent := runPausedEvent("run-pause-claim-event", r, at.Add(time.Second))
			if _, err := execution.PromotePause(ctx, session.PromotePauseRequest{Revision: 1, TurnID: admitted.Turn.ID, Event: pauseEvent}); err != nil {
				t.Fatalf("promote pause: %v", err)
			}
			claimed, err := subject.Store.ClaimRun(ctx, session.RunClaim{RunID: r.ID, OwnerID: "resumer", ClaimToken: "resume-token", LeaseDuration: time.Minute})
			if err != nil {
				t.Fatalf("claim paused run: %v", err)
			}
			if claimed.Status != session.RunRunning || claimed.OwnerID != "resumer" || claimed.ClaimToken != "resume-token" {
				t.Fatalf("claimed run = %#v", claimed)
			}
			if !claimed.LeaseUntil.After(time.Now().UTC()) {
				t.Fatalf("claimed run lease not renewed: %v", claimed.LeaseUntil)
			}
		})

		// Round-three reconciliation item 4 (SR-4a,c): RepauseRun is the
		// compensating write a post-claim failure (ResumeRun's StartRun, or
		// a crash-recovery reclaim with nothing further to drive
		// automatically) uses to put a claimed run back into a resumable
		// paused state without touching its already-promoted checkpoint.
		t.Run("repause run reverts a claim to paused without touching the promoted checkpoint", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-repause")
			r := admitRun(t, ctx, subject.Store, run("run-repause", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			admitted, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:         buildTurn("turn-repause", r, 1, at),
				UserMessages: []session.Message{message("turn-repause-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-repause-started", r, "turn-repause", at),
			})
			if err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			cp := stagedCheckpoint(r, 1, "cp-repause", session.CheckpointKindLoop, []byte("bytes"), at)
			if _, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp}); err != nil {
				t.Fatalf("stage checkpoint: %v", err)
			}
			pauseEvent := runPausedEvent("run-repause-event", r, at.Add(time.Second))
			if _, err := execution.PromotePause(ctx, session.PromotePauseRequest{Revision: 1, TurnID: admitted.Turn.ID, Event: pauseEvent}); err != nil {
				t.Fatalf("promote pause: %v", err)
			}
			// Reclaim, as ResumeRun's ClaimRun would, then simulate a
			// post-claim failure that must not strand the run: revert via
			// RepauseRun instead of leaving it running with no driver.
			claimed, err := subject.Store.ClaimRun(ctx, session.RunClaim{RunID: r.ID, OwnerID: "resumer", ClaimToken: "resume-token", LeaseDuration: time.Minute})
			if err != nil {
				t.Fatalf("claim paused run: %v", err)
			}
			resumedExecution := executionFor(subject.Store, claimed)
			repauseEvent := runPausedEvent("run-repause-compensate-event", claimed, at.Add(2*time.Second))
			result, err := resumedExecution.RepauseRun(ctx, session.RepauseRunRequest{Event: repauseEvent})
			if err != nil {
				t.Fatalf("repause run: %v", err)
			}
			if result.Run.Status != session.RunPaused {
				t.Fatalf("repaused run status = %q", result.Run.Status)
			}
			if result.Run.LeaseUntil.After(time.Now().UTC()) {
				t.Fatalf("repaused run retains a live lease: %v", result.Run.LeaseUntil)
			}
			// The already-promoted checkpoint (revision 1) must be
			// completely untouched: RepauseRun neither promotes nor retires
			// anything.
			promoted, found, err := subject.Store.ReadPromotedCheckpoint(ctx, r.ID)
			if err != nil || !found || promoted.Revision != 1 {
				t.Fatalf("read promoted checkpoint after repause = %#v, %v, %v", promoted, found, err)
			}
			// The claim that was just reverted can no longer write.
			if _, err := resumedExecution.RenewRunLease(ctx, time.Minute); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("post-repause RenewRunLease = %v, want ErrConflict", err)
			}
			// A fresh claim succeeds immediately (paused, no lease wait),
			// exactly as after any other durable pause.
			reclaimed, err := subject.Store.ClaimRun(ctx, session.RunClaim{RunID: r.ID, OwnerID: "resumer-2", ClaimToken: "resume-token-2", LeaseDuration: time.Minute})
			if err != nil || reclaimed.Status != session.RunRunning {
				t.Fatalf("reclaim after repause = %#v, %v", reclaimed, err)
			}
		})

		// Round-five reconciliation item 2 (TR-I1): crash reconciliation of
		// a dangling turn stages a fresh, correctly turn-identified
		// Kind=loop checkpoint for it, then promotes that revision as part
		// of the SAME repause that reverts the reclaimed run back to
		// paused -- since the reconciled turn is already durably
		// TurnInterrupted by that point (ReconcileInterruptedTurn already
		// ran), PromotePause's own turn-interrupt precondition (admitted/
		// running) no longer holds, so RepauseRun's PromoteRevision is what
		// promotes it instead.
		t.Run("repause run also promotes a requested staged revision, superseding the previously promoted one", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-repause-promote")
			r := admitRun(t, ctx, subject.Store, run("run-repause-promote", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			admitted, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:         buildTurn("turn-repause-promote", r, 1, at),
				UserMessages: []session.Message{message("turn-repause-promote-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-repause-promote-started", r, "turn-repause-promote", at),
			})
			if err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			cp1 := stagedCheckpoint(r, 1, "cp-repause-promote-1", session.CheckpointKindLoop, []byte("bytes-1"), at)
			if _, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp1}); err != nil {
				t.Fatalf("stage checkpoint 1: %v", err)
			}
			pauseEvent := runPausedEvent("run-repause-promote-pause-event", r, at.Add(time.Second))
			if _, err := execution.PromotePause(ctx, session.PromotePauseRequest{Revision: 1, TurnID: admitted.Turn.ID, Event: pauseEvent}); err != nil {
				t.Fatalf("promote pause: %v", err)
			}
			// A second process reclaims (as reconcileCrashedRun's own
			// ClaimRun does), reconciles the dangling turn interrupted --
			// already durably TurnInterrupted from the promote above, so
			// this is the ReconcileInterruptedTurn idempotent-replay branch
			// here, standing in for a genuinely dangling turn from a later
			// crash -- and stages a fresh checkpoint revision for it.
			claimed, err := subject.Store.ClaimRun(ctx, session.RunClaim{RunID: r.ID, OwnerID: "reconciler", ClaimToken: "reconcile-token", LeaseDuration: time.Minute})
			if err != nil {
				t.Fatalf("claim paused run: %v", err)
			}
			reconcileExecution := executionFor(subject.Store, claimed)
			cp2 := stagedCheckpoint(claimed, 2, "cp-repause-promote-2", session.CheckpointKindLoop, []byte("bytes-2"), at.Add(2*time.Second))
			if _, err := reconcileExecution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp2}); err != nil {
				t.Fatalf("stage checkpoint 2: %v", err)
			}
			repauseEvent := runPausedEvent("run-repause-promote-repause-event", claimed, at.Add(3*time.Second))
			result, err := reconcileExecution.RepauseRun(ctx, session.RepauseRunRequest{Event: repauseEvent, PromoteRevision: 2})
			if err != nil {
				t.Fatalf("repause run with promote revision: %v", err)
			}
			if result.Run.Status != session.RunPaused {
				t.Fatalf("repaused run status = %q", result.Run.Status)
			}
			promoted, found, err := subject.Store.ReadPromotedCheckpoint(ctx, r.ID)
			if err != nil || !found || promoted.Revision != 2 || string(promoted.Bytes) != "bytes-2" || !promoted.Promoted {
				t.Fatalf("read promoted checkpoint after repause-promote = %#v, %v, %v, want revision 2 promoted", promoted, found, err)
			}
			// A fresh claim succeeds immediately, exactly as after any
			// other durable pause.
			reclaimed, err := subject.Store.ClaimRun(ctx, session.RunClaim{RunID: r.ID, OwnerID: "resumer-3", ClaimToken: "resume-token-3", LeaseDuration: time.Minute})
			if err != nil || reclaimed.Status != session.RunRunning {
				t.Fatalf("reclaim after repause-promote = %#v, %v", reclaimed, err)
			}
		})

		t.Run("repause run rejects a promote revision that was never staged", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-repause-promote-missing")
			r := admitRun(t, ctx, subject.Store, run("run-repause-promote-missing", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			if _, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:         buildTurn("turn-repause-promote-missing", r, 1, at),
				UserMessages: []session.Message{message("turn-repause-promote-missing-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-repause-promote-missing-started", r, "turn-repause-promote-missing", at),
			}); err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			event := runPausedEvent("run-repause-promote-missing-event", r, at.Add(time.Second))
			if _, err := execution.RepauseRun(ctx, session.RepauseRunRequest{Event: event, PromoteRevision: 7}); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("repause run with an unstaged promote revision = %v, want ErrConflict", err)
			}
		})

		t.Run("observation shows paused runs as nonterminal", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-pause-observation")
			r := admitRun(t, ctx, subject.Store, run("run-pause-observation", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			admitted, err := execution.AdmitTurn(ctx, session.AdmitTurnRequest{
				Turn:         buildTurn("turn-pause-observation", r, 1, at),
				UserMessages: []session.Message{message("turn-pause-observation-user", s.ID, r.ID, session.RoleUser)},
				Event:        turnStartedEvent("turn-pause-observation-started", r, "turn-pause-observation", at),
			})
			if err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			cp := stagedCheckpoint(r, 1, "cp-pause-observation", session.CheckpointKindLoop, []byte("bytes"), at)
			if _, err := execution.StageCheckpoint(ctx, session.StageCheckpointRequest{Checkpoint: cp}); err != nil {
				t.Fatalf("stage checkpoint: %v", err)
			}
			pauseEvent := runPausedEvent("run-pause-observation-event", r, at.Add(time.Second))
			if _, err := execution.PromotePause(ctx, session.PromotePauseRequest{Revision: 1, TurnID: admitted.Turn.ID, Event: pauseEvent}); err != nil {
				t.Fatalf("promote pause: %v", err)
			}
			reader, ok := subject.Store.(session.ObservationReader)
			if !ok {
				t.Fatal("store does not expose session.ObservationReader")
			}
			snapshot, err := reader.ReadObservationSnapshot(ctx, s.ID, session.ObservationLimits{MaxMessages: 10, MaxTools: 10, MaxParts: 10, MaxSnapshotBytes: 1 << 20, MaxTextBytes: 1 << 20})
			if err != nil {
				t.Fatalf("read observation snapshot: %v", err)
			}
			found := false
			for _, observed := range snapshot.Runs {
				if observed.ID == r.ID {
					found = true
					if observed.Status != session.RunPaused || observed.Terminal() {
						t.Fatalf("observed paused run = %#v", observed)
					}
				}
			}
			if !found {
				t.Fatal("paused run missing from observation snapshot")
			}
		})
	})
}

// durableIdentityContract exercises session.Message.TurnID/AgentPath (W7
// durable identity): both fields round-trip through AdmitTurn and a
// follow-up AppendMessage exactly as stamped, a message that leaves TurnID
// unset is tolerated (it is correlation metadata, like
// session.EventRecord.TurnID -- see ValidateAdmitTurn), and a message that
// stamps a turn other than the one being admitted is rejected.
func durableIdentityContract(t *testing.T, factory Factory) {
	t.Run("durable_identity", func(t *testing.T) {
		t.Run("turn id and agent path round trip through admit turn and append message", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-durable-identity")
			r := admitRun(t, ctx, subject.Store, run("run-durable-identity", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			userMessage := message("durable-identity-user", s.ID, r.ID, session.RoleUser)
			userMessage.TurnID = "turn-durable-identity"
			userMessage.AgentPath = "root"
			assistant := message("durable-identity-assistant", s.ID, r.ID, session.RoleAssistant)
			assistant.TurnID = "turn-durable-identity"
			assistant.AgentPath = "root"
			request := session.AdmitTurnRequest{
				Turn:                 buildTurn("turn-durable-identity", r, 1, at),
				UserMessages:         []session.Message{userMessage},
				AssistantPlaceholder: assistant,
				Event:                turnStartedEvent("turn-durable-identity-started", r, "turn-durable-identity", at),
			}
			if _, err := execution.AdmitTurn(ctx, request); err != nil {
				t.Fatalf("admit turn: %v", err)
			}
			// A follow-up message this turn appends outside admission (e.g.
			// a tool result or approval response) stamps the same identity.
			followUp := message("durable-identity-followup", s.ID, r.ID, session.RoleUser)
			followUp.TurnID = "turn-durable-identity"
			followUp.AgentPath = "root"
			followUp.CreatedAt, followUp.UpdatedAt = at.Add(time.Second), at.Add(time.Second)
			if _, err := execution.AppendMessage(ctx, followUp); err != nil {
				t.Fatalf("append message: %v", err)
			}
			batch, err := subject.Store.ListMessages(ctx, s.ID, session.ReplayCursor{Limit: 10})
			if err != nil {
				t.Fatalf("list messages: %v", err)
			}
			seen := map[session.MessageID]session.Message{}
			for _, msg := range batch.Messages {
				seen[msg.ID] = msg
			}
			for _, id := range []session.MessageID{"durable-identity-user", "durable-identity-assistant", "durable-identity-followup"} {
				got, ok := seen[id]
				if !ok {
					t.Fatalf("message %s missing from replay", id)
				}
				if got.TurnID != "turn-durable-identity" || got.AgentPath != "root" {
					t.Fatalf("message %s identity = %#v, want turn-durable-identity/root", id, got)
				}
			}
		})

		t.Run("turn id left unset on a message is tolerated", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-durable-identity-unset")
			r := admitRun(t, ctx, subject.Store, run("run-durable-identity-unset", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			request := session.AdmitTurnRequest{
				Turn:                 buildTurn("turn-durable-identity-unset", r, 1, at),
				UserMessages:         []session.Message{message("durable-identity-unset-user", s.ID, r.ID, session.RoleUser)},
				AssistantPlaceholder: message("durable-identity-unset-assistant", s.ID, r.ID, session.RoleAssistant),
				Event:                turnStartedEvent("turn-durable-identity-unset-started", r, "turn-durable-identity-unset", at),
			}
			if _, err := execution.AdmitTurn(ctx, request); err != nil {
				t.Fatalf("admit turn with unset TurnID on messages: %v", err)
			}
		})

		t.Run("a message stamped with a different turn is rejected", func(t *testing.T) {
			subject := setup(t, factory)
			ctx := context.Background()
			s := createSession(t, ctx, subject.Store, "session-durable-identity-mismatch")
			r := admitRun(t, ctx, subject.Store, run("run-durable-identity-mismatch", s.ID, "owner"))
			execution := executionFor(subject.Store, r)
			at := r.CreatedAt.Add(time.Second)
			userMessage := message("durable-identity-mismatch-user", s.ID, r.ID, session.RoleUser)
			userMessage.TurnID = "some-other-turn"
			request := session.AdmitTurnRequest{
				Turn:                 buildTurn("turn-durable-identity-mismatch", r, 1, at),
				UserMessages:         []session.Message{userMessage},
				AssistantPlaceholder: message("durable-identity-mismatch-assistant", s.ID, r.ID, session.RoleAssistant),
				Event:                turnStartedEvent("turn-durable-identity-mismatch-started", r, "turn-durable-identity-mismatch", at),
			}
			if _, err := execution.AdmitTurn(ctx, request); !errors.Is(err, session.ErrConflict) {
				t.Fatalf("admit turn with mismatched user message TurnID = %v, want ErrConflict", err)
			}
		})
	})
}
