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
