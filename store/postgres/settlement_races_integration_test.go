//go:build postgres_integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
)

type raceOutcome[T any] struct {
	index int
	value T
	err   error
}

func raceStores[T any](ctx context.Context, stores []session.Store, fn func(context.Context, int, session.Store) (T, error)) []raceOutcome[T] {
	start := make(chan struct{})
	ready := make(chan struct{}, len(stores))
	out := make(chan raceOutcome[T], len(stores))
	for i, store := range stores {
		go func(i int, store session.Store) {
			ready <- struct{}{}
			<-start
			value, err := fn(ctx, i, store)
			out <- raceOutcome[T]{index: i, value: value, err: err}
		}(i, store)
	}
	for range stores {
		<-ready
	}
	close(start)
	result := make([]raceOutcome[T], 0, len(stores))
	for range stores {
		result = append(result, <-out)
	}
	return result
}

// canonicalRace requires every identical caller to receive the same result;
// contradictory requests must elect exactly one winner and one conflict.
func canonicalRace[T any](t *testing.T, f *raceFixture, conflict bool, fn func(context.Context, int, session.Store) (T, error)) raceOutcome[T] {
	t.Helper()
	outcomes := raceStores(f.ctx, f.stores, fn)
	var winner raceOutcome[T]
	successes := 0
	for _, outcome := range outcomes {
		if outcome.err == nil {
			successes++
			winner = outcome
		} else if !conflict || !errors.Is(outcome.err, session.ErrConflict) {
			t.Fatalf("race caller %d: %v", outcome.index, outcome.err)
		}
	}
	expected := 2
	if conflict {
		expected = 1
	}
	if successes != expected {
		t.Fatalf("race successes = %d, want %d", successes, expected)
	}
	if !conflict && !reflect.DeepEqual(outcomes[0].value, outcomes[1].value) {
		t.Fatal("identical callers received different canonical results")
	}
	return winner
}

func assertRejectedRetry(t *testing.T, f *raceFixture, retry func() error) {
	t.Helper()
	before := f.snapshot(t)
	if err := retry(); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("losing retry: %v", err)
	}
	if f.snapshot(t) != before {
		t.Fatal("losing retry changed durable rows or revisions")
	}
}

func testRunSettlementRace(t *testing.T, server *testpostgres.Server) {
	for _, mode := range []string{"identical", "contradictory"} {
		t.Run(mode, func(t *testing.T) {
			f := newRaceFixture(t, server, 2)
			f.seed(t)
			request := session.SettleRunRequest{Settlement: session.RunSettlement{Status: session.RunCompleted, FinishedAt: f.now.Add(time.Minute)}, Event: session.RunSettlementEvent{ID: "finished", Usage: session.Usage{InputTokens: 2, OutputTokens: 3}}}
			requests := []session.SettleRunRequest{request, request}
			conflict := mode == "contradictory"
			if conflict {
				requests[1].Settlement.Status, requests[1].Settlement.Error = session.RunFailed, "failed"
				requests[1].Event.Usage.OutputTokens = 7
			}
			winner := canonicalRace(t, f, conflict, func(ctx context.Context, i int, store session.Store) (session.RunSettlementResult, error) {
				return store.Execution(session.RunFence{RunID: "run", ClaimToken: "old"}).SettleRun(ctx, requests[i])
			})
			run, err := f.stores[0].GetRun(f.ctx, "run")
			if err != nil || !reflect.DeepEqual(run, winner.value.Run) {
				t.Fatalf("durable run differs from winner: %v", err)
			}
			events, err := f.stores[0].ListEvents(f.ctx, "session", session.EventCursor{})
			if err != nil || len(events.Events) != 1 || !reflect.DeepEqual(events.Events[0], winner.value.Event) {
				t.Fatalf("canonical run event mismatch: %v", err)
			}
			if conflict {
				assertRejectedRetry(t, f, func() error {
					_, err := f.stores[0].Execution(session.RunFence{RunID: "run", ClaimToken: "old"}).SettleRun(f.ctx, requests[1-winner.index])
					return err
				})
			}
		})
	}
}

func seedClaimedTool(t *testing.T, f *raceFixture) session.ToolCall {
	t.Helper()
	call := seedPendingTool(t, f)
	execution := f.stores[0].Execution(session.RunFence{RunID: "run", ClaimToken: "old"})
	messageAt := f.now.Add(time.Second)
	claimed, err := execution.ClaimToolCall(f.ctx, session.ClaimToolCallRequest{ID: call.ID, ClaimedBy: "worker", ClaimToken: "tool-old", StartedAt: messageAt.Add(time.Second), LeaseDuration: time.Minute, Event: session.ToolTransitionEvent{ID: "tool-running", CreatedAt: messageAt.Add(time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	return claimed.Call
}
func toolSettlement(call session.ToolCall, at time.Time, status session.ToolCallStatus, output json.RawMessage, errText string, eventID session.EventID) session.SettleToolCallRequest {
	return session.SettleToolCallRequest{
		Settlement: session.ToolSettlement{ID: call.ID, ClaimedBy: call.ClaimedBy, ClaimToken: call.ClaimToken, Status: status, Output: output, Error: errText, CompletedAt: at,
			ResultMessage: session.Message{ID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, ParentID: call.MessageID, Role: session.RoleTool, CreatedAt: at, UpdatedAt: at},
			ResultPart:    session.Part{ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, Kind: session.PartToolResult, Payload: output, CreatedAt: at, UpdatedAt: at}},
		Event: session.ToolTransitionEvent{ID: eventID, CreatedAt: at},
	}
}

func testToolSettlementRace(t *testing.T, server *testpostgres.Server) {
	for _, mode := range []string{"identical", "contradictory"} {
		t.Run(mode, func(t *testing.T) {
			f := newRaceFixture(t, server, 2)
			f.seed(t)
			call := seedClaimedTool(t, f)
			request := toolSettlement(call, f.now.Add(time.Minute), session.ToolCallCompleted, json.RawMessage(`{"ok":true}`), "", "terminal")
			requests := []session.SettleToolCallRequest{request, request}
			conflict := mode == "contradictory"
			if conflict {
				requests[1] = toolSettlement(call, f.now.Add(2*time.Minute), session.ToolCallFailed, json.RawMessage(`{"ok":false}`), "failure", "terminal")
			}
			winner := canonicalRace(t, f, conflict, func(ctx context.Context, i int, store session.Store) (session.ToolTransitionResult, error) {
				return store.Execution(session.RunFence{RunID: "run", ClaimToken: "old"}).SettleToolCall(ctx, requests[i])
			})
			assertCanonicalToolSettlement(t, f, winner.value, call)
			if conflict {
				assertRejectedRetry(t, f, func() error {
					_, err := f.stores[0].Execution(session.RunFence{RunID: "run", ClaimToken: "old"}).SettleToolCall(f.ctx, requests[1-winner.index])
					return err
				})
			}
		})
	}
}

func assertCanonicalToolSettlement(t *testing.T, f *raceFixture, result session.ToolTransitionResult, seeded session.ToolCall) {
	t.Helper()
	call, err := f.stores[0].GetToolCall(f.ctx, seeded.ID)
	if err != nil || !session.SameToolTransitionState(call, result.Call) {
		t.Fatalf("stored tool call does not match canonical winner: %v", err)
	}
	events, err := f.stores[0].ListEvents(f.ctx, "session", session.EventCursor{Limit: 10})
	if err != nil || len(events.Events) != 3 {
		t.Fatalf("tool events = %d, %v; want pending/running/terminal", len(events.Events), err)
	}
	var terminal int
	for _, event := range events.Events {
		if event.ToolTransition == session.ToolTransitionTerminal {
			terminal++
			if !reflect.DeepEqual(event, result.Event) {
				t.Fatal("stored terminal event does not match canonical winner")
			}
		}
	}
	if terminal != 1 {
		t.Fatalf("terminal events = %d, want 1", terminal)
	}
	messages, err := f.stores[0].ListMessages(f.ctx, "session", session.ReplayCursor{Limit: 10})
	if err != nil || len(messages.Messages) != 2 || len(messages.Parts) != 2 {
		t.Fatalf("settlement envelope counts invalid: %v", err)
	}
	var messageFound, partFound bool
	expectedMessage := session.Message{ID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, ParentID: call.MessageID, Role: session.RoleTool, CreatedAt: call.CompletedAt, UpdatedAt: call.CompletedAt}
	expectedPart := session.Part{ID: call.ResultPartID, MessageID: call.ResultMessageID, SessionID: call.SessionID, RunID: call.RunID, Kind: session.PartToolResult, Payload: call.Output, CreatedAt: call.CompletedAt, UpdatedAt: call.CompletedAt}
	for _, message := range messages.Messages {
		if message.ID == seeded.ResultMessageID {
			if messageFound || !reflect.DeepEqual(message, expectedMessage) {
				t.Fatal("stored result message is not the canonical envelope")
			}
			messageFound = true
		}
	}
	for _, part := range messages.Parts {
		if part.ID == seeded.ResultPartID {
			if partFound || !reflect.DeepEqual(part, expectedPart) {
				t.Fatal("stored result part is not the canonical envelope")
			}
			partFound = true
		}
	}
	if !messageFound || !partFound {
		t.Fatalf("settlement result envelope message=%t part=%t", messageFound, partFound)
	}
}

func testToolTransitionRace(t *testing.T, server *testpostgres.Server) {
	for _, phase := range []string{"pending", "running"} {
		for _, mode := range []string{"identical", "contradictory"} {
			t.Run(phase+"/"+mode, func(t *testing.T) {
				f := newRaceFixture(t, server, 2)
				f.seed(t)
				conflict := mode == "contradictory"
				create := pendingToolRequest(f.now, "pending")
				creates := []session.CreateToolCallRequest{create, create}
				claimAt := f.now.Add(2 * time.Second)
				claim := session.ClaimToolCallRequest{ID: "tool", ClaimedBy: "worker", ClaimToken: "tool-old", StartedAt: claimAt, LeaseDuration: time.Minute, Event: session.ToolTransitionEvent{ID: "running", CreatedAt: claimAt}}
				claims := []session.ClaimToolCallRequest{claim, claim}
				if conflict {
					creates[1].Call.Name = "other"
					creates[1].RequestPart.Payload = json.RawMessage(`{"id":"tool","name":"other","arguments":{"key":"value"}}`)
					claims[1].ClaimedBy, claims[1].ClaimToken = "other", "tool-other"
				}
				expectedEvents := 1
				if phase == "running" {
					seedPendingTool(t, f)
					expectedEvents = 2
				} else {
					appendToolMessage(t, f)
				}
				invoke := func(ctx context.Context, i int, store session.Store) (session.ToolTransitionResult, error) {
					ex := store.Execution(session.RunFence{RunID: "run", ClaimToken: "old"})
					if phase == "running" {
						return ex.ClaimToolCall(ctx, claims[i])
					}
					return ex.CreateToolCall(ctx, creates[i])
				}
				winner := canonicalRace(t, f, conflict, invoke)
				call, err := f.stores[0].GetToolCall(f.ctx, "tool")
				if err != nil || !reflect.DeepEqual(call, winner.value.Call) {
					t.Fatalf("durable tool differs from winner: %v", err)
				}
				events, err := f.stores[0].ListEvents(f.ctx, "session", session.EventCursor{})
				if err != nil || len(events.Events) != expectedEvents || !reflect.DeepEqual(events.Events[expectedEvents-1], winner.value.Event) {
					t.Fatalf("canonical phase event mismatch: %v", err)
				}
				batch, err := f.stores[0].ListMessages(f.ctx, "session", session.ReplayCursor{})
				if err != nil || len(batch.Messages) != 1 || len(batch.Parts) != 1 {
					t.Fatalf("transition envelope counts invalid: %v", err)
				}
				if phase == "pending" && !reflect.DeepEqual(batch.Parts[0], creates[winner.index].RequestPart) {
					t.Fatal("pending request part differs from winner")
				}
				if conflict {
					assertRejectedRetry(t, f, func() error { _, err := invoke(f.ctx, 1-winner.index, f.stores[0]); return err })
				}
			})
		}
	}
}

func pendingToolRequest(now time.Time, eventID session.EventID) session.CreateToolCallRequest {
	call := session.ToolCall{ID: "tool", SessionID: "session", RunID: "run", MessageID: "request-message", RequestPartID: "request-part", ResultMessageID: "result-message", ResultPartID: "result-part", Name: "lookup", Input: json.RawMessage(`{"key":"value"}`), Status: session.ToolCallPending, RetrySafe: true}
	part := session.Part{ID: call.RequestPartID, MessageID: call.MessageID, SessionID: call.SessionID, RunID: call.RunID, Kind: session.PartToolCall, Payload: json.RawMessage(`{"id":"tool","name":"lookup","arguments":{"key":"value"}}`), CreatedAt: now, UpdatedAt: now}
	return session.CreateToolCallRequest{Call: call, RequestPart: part, Event: session.ToolTransitionEvent{ID: eventID, CreatedAt: now}}
}

func seedPendingTool(t *testing.T, f *raceFixture) session.ToolCall {
	t.Helper()
	execution := f.stores[0].Execution(session.RunFence{RunID: "run", ClaimToken: "old"})
	at := f.now.Add(time.Second)
	appendToolMessage(t, f)
	request := pendingToolRequest(at, "tool-pending")
	if _, err := execution.CreateToolCall(f.ctx, request); err != nil {
		t.Fatal(err)
	}
	return request.Call
}

func appendToolMessage(t *testing.T, f *raceFixture) {
	t.Helper()
	at := f.now.Add(time.Second)
	if _, err := f.stores[0].Execution(session.RunFence{RunID: "run", ClaimToken: "old"}).AppendMessage(f.ctx, session.Message{ID: "request-message", SessionID: "session", RunID: "run", Role: session.RoleAssistant, CreatedAt: at, UpdatedAt: at}); err != nil {
		t.Fatal(err)
	}
}
