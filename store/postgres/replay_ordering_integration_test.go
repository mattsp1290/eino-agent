//go:build postgres_integration

package postgres_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
)

type replayItem struct {
	id string
	at time.Time
}

func testReplayMessagesParts(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	run := f.seed(t, "replay-session", "replay-run")
	base := f.now.Truncate(time.Second)
	items := []replayItem{
		{id: "z\x00", at: base.Add(2 * time.Nanosecond)},
		{id: "é", at: base.Add(2 * time.Nanosecond)},
		{id: "a\x00", at: base.Add(2 * time.Nanosecond)},
		{id: "\x00", at: base.Add(2 * time.Nanosecond)},
		{id: "adjacent", at: base.Add(time.Nanosecond)},
	}
	wantMessages := []session.MessageID{"adjacent", "\x00", "a\x00", "z\x00", "é"}
	wantParts, wantOwners := replayPartExpectations(wantMessages)
	execution := f.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	for _, index := range []int{3, 0, 4, 2, 1} {
		item := items[index]
		message := session.Message{ID: session.MessageID(item.id), SessionID: "replay-session", RunID: run.ID, Role: session.RoleUser, CreatedAt: item.at, UpdatedAt: item.at}
		if _, err := execution.AppendMessage(f.ctx, message); err != nil {
			t.Fatalf("append replay message: %v", err)
		}
		for _, partSpec := range []struct {
			suffix  string
			ordinal int64
		}{{"tail", 1}, {"z", 0}, {"a", 0}} {
			part := session.Part{ID: replayPartID(message.ID, partSpec.suffix), MessageID: message.ID, SessionID: message.SessionID, RunID: run.ID, Kind: session.PartText, Ordinal: partSpec.ordinal, Payload: json.RawMessage(fmt.Sprintf(`{"ordinal":%d}`, partSpec.ordinal)), CreatedAt: item.at, UpdatedAt: item.at}
			if _, err := execution.AppendPart(f.ctx, part); err != nil {
				t.Fatalf("append replay part: %v", err)
			}
		}
	}
	first, err := f.store.ListMessages(f.ctx, "replay-session", session.ReplayCursor{Limit: 2})
	if err != nil {
		t.Fatalf("first replay page: %v", err)
	}
	assertReplayMessagePage(t, first, wantMessages[:2], wantParts[:6], wantOwners[:6])
	if first.Next.AfterMessageID == "" {
		t.Fatal("first replay page did not return a continuation cursor")
	}
	f.reopen(t)
	secondCursor := first.Next
	secondCursor.Limit = 1
	second, err := f.store.ListMessages(f.ctx, "replay-session", secondCursor)
	if err != nil {
		t.Fatalf("second replay page after reopen: %v", err)
	}
	assertReplayMessagePage(t, second, wantMessages[2:3], wantParts[6:9], wantOwners[6:9])
	thirdCursor := second.Next
	thirdCursor.Limit = 0
	third, err := f.store.ListMessages(f.ctx, "replay-session", thirdCursor)
	if err != nil {
		t.Fatalf("final replay page: %v", err)
	}
	assertReplayMessagePage(t, third, wantMessages[3:], wantParts[9:], wantOwners[9:])
	if third.Next != (session.ReplayCursor{}) {
		t.Fatal("final replay page returned a nonzero cursor")
	}
	other := f.seed(t, "other-session", "other-run")
	if _, err := f.store.ListMessages(f.ctx, other.SessionID, session.ReplayCursor{AfterMessageID: wantMessages[0], Limit: 1}); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("cross-session message cursor error = %v, want ErrNotFound", err)
	}
}

func testReplayEventsModels(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	run := f.seed(t, "replay-session", "replay-run")
	base := f.now.Truncate(time.Second)
	eventItems := []replayItem{{id: "event-z\x00", at: base.Add(2 * time.Nanosecond)}, {id: "event-é", at: base.Add(2 * time.Nanosecond)}, {id: "event-a\x00", at: base.Add(2 * time.Nanosecond)}, {id: "event-\x00", at: base.Add(2 * time.Nanosecond)}, {id: "event-adjacent", at: base.Add(time.Nanosecond)}}
	modelItems := []replayItem{{id: "model-z\x00", at: base.Add(2 * time.Nanosecond)}, {id: "model-é", at: base.Add(2 * time.Nanosecond)}, {id: "model-a\x00", at: base.Add(2 * time.Nanosecond)}, {id: "model-\x00", at: base.Add(2 * time.Nanosecond)}, {id: "model-adjacent", at: base.Add(time.Nanosecond)}}
	wantEvents := []session.EventID{"event-adjacent", "event-\x00", "event-a\x00", "event-z\x00", "event-é"}
	wantModels := []session.ModelRequestID{"model-adjacent", "model-\x00", "model-a\x00", "model-z\x00", "model-é"}
	execution := f.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	for _, index := range []int{3, 0, 4, 2, 1} {
		item := eventItems[index]
		if _, err := execution.AppendEvent(f.ctx, session.EventRecord{ID: session.EventID(item.id), SessionID: run.SessionID, RunID: run.ID, Kind: "replay_order", Payload: json.RawMessage(`{"ok":true}`), CreatedAt: item.at}); err != nil {
			t.Fatalf("append replay event: %v", err)
		}
	}
	for index, order := range []int{3, 0, 4, 2, 1} {
		item := modelItems[order]
		record := session.ModelRequestRecord{ID: session.ModelRequestID(item.id), SessionID: run.SessionID, RunID: run.ID, AssistantMessageID: session.MessageID("assistant-" + item.id), Attempt: index, State: session.ModelRequestPrepared, Messages: json.RawMessage(`{"messages":[]}`), CreatedAt: item.at, UpdatedAt: item.at}
		if _, err := execution.CreateModelRequest(f.ctx, record); err != nil {
			t.Fatalf("append replay model request: %v", err)
		}
	}
	firstEvent, err := f.store.ListEvents(f.ctx, run.SessionID, session.EventCursor{Limit: 2})
	if err != nil {
		t.Fatalf("first replay event page: %v", err)
	}
	assertReplayEventPage(t, firstEvent, wantEvents[:2])
	f.reopen(t)
	eventCursor := firstEvent.Next
	eventCursor.Limit = 1
	secondEvent, err := f.store.ListEvents(f.ctx, run.SessionID, eventCursor)
	if err != nil {
		t.Fatalf("second replay event page after reopen: %v", err)
	}
	assertReplayEventPage(t, secondEvent, wantEvents[2:3])
	eventCursor = secondEvent.Next
	eventCursor.Limit = 0
	thirdEvent, err := f.store.ListEvents(f.ctx, run.SessionID, eventCursor)
	if err != nil {
		t.Fatalf("final replay event page: %v", err)
	}
	assertReplayEventPage(t, thirdEvent, wantEvents[3:])
	if thirdEvent.Next != (session.EventCursor{}) {
		t.Fatal("final replay event page returned a nonzero cursor")
	}
	firstModel, err := f.store.ListModelRequests(f.ctx, run.ID, session.ModelRequestCursor{Limit: 2})
	if err != nil {
		t.Fatalf("first replay model page: %v", err)
	}
	assertReplayModelPage(t, firstModel, wantModels[:2])
	f.reopen(t)
	modelCursor := firstModel.Next
	modelCursor.Limit = 1
	secondModel, err := f.store.ListModelRequests(f.ctx, run.ID, modelCursor)
	if err != nil {
		t.Fatalf("second replay model page after reopen: %v", err)
	}
	assertReplayModelPage(t, secondModel, wantModels[2:3])
	modelCursor = secondModel.Next
	modelCursor.Limit = 0
	thirdModel, err := f.store.ListModelRequests(f.ctx, run.ID, modelCursor)
	if err != nil {
		t.Fatalf("final replay model page: %v", err)
	}
	assertReplayModelPage(t, thirdModel, wantModels[3:])
	if thirdModel.Next != (session.ModelRequestCursor{}) {
		t.Fatal("final replay model page returned a nonzero cursor")
	}
	other := f.seed(t, "other-session", "other-run")
	if _, err := f.store.ListEvents(f.ctx, other.SessionID, session.EventCursor{AfterEventID: wantEvents[0], Limit: 1}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("cross-session event cursor error = %v, want ErrConflict", err)
	}
	if _, err := f.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken}).SettleRun(f.ctx, session.SettleRunRequest{Settlement: session.RunSettlement{Status: session.RunCompleted, FinishedAt: f.now.Add(time.Minute)}, Event: session.RunSettlementEvent{ID: "replay-finish"}}); err != nil {
		t.Fatalf("finish primary replay run: %v", err)
	}
	otherRun := f.seed(t, "replay-session", "same-session-run")
	if _, err := f.store.ListModelRequests(f.ctx, otherRun.ID, session.ModelRequestCursor{AfterID: wantModels[0], Limit: 1}); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("cross-run model cursor error = %v, want ErrNotFound", err)
	}
}

func replayPartID(messageID session.MessageID, suffix string) session.PartID {
	return session.PartID(string(messageID) + "-part-" + suffix)
}

func replayPartExpectations(messages []session.MessageID) ([]session.PartID, []session.MessageID) {
	parts := make([]session.PartID, 0, len(messages)*3)
	owners := make([]session.MessageID, 0, len(messages)*3)
	for _, message := range messages {
		for _, suffix := range []string{"a", "z", "tail"} {
			parts = append(parts, replayPartID(message, suffix))
			owners = append(owners, message)
		}
	}
	return parts, owners
}

func assertReplayMessagePage(t *testing.T, page session.ReplayBatch, messages []session.MessageID, parts []session.PartID, owners []session.MessageID) {
	t.Helper()
	if !slices.Equal(pageMessageIDs(page), messages) || !slices.Equal(pagePartIDs(page), parts) || !slices.Equal(page.PartOwnerMessageIDs, owners) {
		t.Fatalf("replay page ordering or owners mismatch: messages=%d parts=%d owners=%d", len(page.Messages), len(page.Parts), len(page.PartOwnerMessageIDs))
	}
}

func assertReplayEventPage(t *testing.T, page session.EventBatch, want []session.EventID) {
	t.Helper()
	if !slices.Equal(pageEventIDs(page), want) {
		t.Fatalf("replay event page count/order mismatch: got=%d want=%d", len(page.Events), len(want))
	}
}

func assertReplayModelPage(t *testing.T, page session.ModelRequestBatch, want []session.ModelRequestID) {
	t.Helper()
	if !slices.Equal(pageModelIDs(page), want) {
		t.Fatalf("replay model page count/order mismatch: got=%d want=%d", len(page.Records), len(want))
	}
}

func pageMessageIDs(page session.ReplayBatch) []session.MessageID {
	ids := make([]session.MessageID, len(page.Messages))
	for i := range page.Messages {
		ids[i] = page.Messages[i].ID
	}
	return ids
}

func pagePartIDs(page session.ReplayBatch) []session.PartID {
	ids := make([]session.PartID, len(page.Parts))
	for i := range page.Parts {
		ids[i] = page.Parts[i].ID
	}
	return ids
}

func pageEventIDs(page session.EventBatch) []session.EventID {
	ids := make([]session.EventID, len(page.Events))
	for i := range page.Events {
		ids[i] = page.Events[i].ID
	}
	return ids
}

func pageModelIDs(page session.ModelRequestBatch) []session.ModelRequestID {
	ids := make([]session.ModelRequestID, len(page.Records))
	for i := range page.Records {
		ids[i] = page.Records[i].ID
	}
	return ids
}
