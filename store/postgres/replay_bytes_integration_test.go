//go:build postgres_integration

package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
)

func testReplayProviderBytes(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	sessionID := session.ID("replay-\x00-東京")
	runID := session.RunID("run-\x00-δοκιμή")
	run := f.seed(t, sessionID, runID)

	metadata := session.Session{
		ID:          sessionID,
		ParentID:    session.ID("parent-\x00-親"),
		WorkspaceID: "workspace-\x00-рабочее",
		Directory:   "/tmp/\x00/工作",
		Title:       "title-\x00-😀",
		Metadata:    map[string]string{"ключ": "value-\x00-値"},
		CreatedAt:   f.now,
		UpdatedAt:   f.now.Add(time.Second),
	}
	if err := f.store.UpdateSession(f.ctx, metadata); err != nil {
		t.Fatalf("update arbitrary session metadata: %v", err)
	}

	execution := f.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	message := session.Message{
		ID:        session.MessageID("message-\x00-雪"),
		SessionID: sessionID,
		RunID:     runID,
		Role:      session.RoleUser,
		Agent:     "agent-\x00-агент",
		ModelID:   "model-\x00-模型",
		CreatedAt: f.now.Add(2 * time.Second),
		UpdatedAt: f.now.Add(2 * time.Second),
	}
	if _, err := execution.AppendMessage(f.ctx, message); err != nil {
		t.Fatalf("append arbitrary replay message: %v", err)
	}

	inner := json.RawMessage("{\n  \"z\": \"\\u263A\", \"a\": \"\\u0000\", \"k\": \"value\" \n}")
	state := replayProviderPart(t, f.ctx, execution, session.PartID("state-\x00-状態"), message, inner, 0)
	request := session.ModelRequestRecord{
		ID:                 session.ModelRequestID("request-\x00-要求"),
		SessionID:          sessionID,
		RunID:              runID,
		AssistantMessageID: message.ID,
		Attempt:            1,
		Step:               2,
		ProviderID:         "provider-\x00-提供者",
		ModelID:            "model-\x00-模型",
		State:              session.ModelRequestPrepared,
		Messages:           json.RawMessage(`{"messages":["\u0000","雪"]}`),
		System:             "system-\x00-システム",
		Tools:              json.RawMessage(`{"tools":[]}`),
		SafeCallConfig:     json.RawMessage(`{"mode":"safe"}`),
		ContentSHA256:      "hash-\x00-摘要",
		ExtensionPlanHash:  "plan-\x00-計画",
		CreatedAt:          f.now.Add(3 * time.Second),
		UpdatedAt:          f.now.Add(3 * time.Second),
	}
	if _, err := execution.CreateModelRequest(f.ctx, request); err != nil {
		t.Fatalf("create arbitrary provider metadata: %v", err)
	}

	f.reopen(t)
	gotSession, err := f.store.GetSession(f.ctx, sessionID)
	if err != nil || !reflect.DeepEqual(gotSession, metadata) {
		t.Fatalf("reopened session metadata mismatch: %v", err)
	}
	gotRequest, err := f.store.GetModelRequest(f.ctx, request.ID)
	if err != nil || !reflect.DeepEqual(gotRequest, request) {
		t.Fatalf("reopened model request metadata mismatch: err=%v", err)
	}

	batch, err := f.store.ListMessages(f.ctx, sessionID, session.ReplayCursor{})
	if err != nil {
		t.Fatalf("replay after reopen: %v", err)
	}
	var gotMessage session.Message
	var gotPart session.Part
	for _, candidate := range batch.Messages {
		if candidate.ID == message.ID {
			gotMessage = candidate
		}
	}
	for _, candidate := range batch.Parts {
		if candidate.ID == state.ID {
			gotPart = candidate
		}
	}
	if !reflect.DeepEqual(gotMessage, message) || !reflect.DeepEqual(gotPart, state) {
		t.Fatalf("reopened replay graph lost arbitrary bytes")
	}
	decoded, err := session.DecodeProviderStatePayload(gotPart.Payload)
	if err != nil || !bytes.Equal(decoded.Data, inner) {
		t.Fatalf("provider state inner bytes mismatch: %v", err)
	}
}

func testReplayPrivateBounds(t *testing.T, server *testpostgres.Server) {
	t.Run("items", func(t *testing.T) { testReplayProviderItemBound(t, server) })
	t.Run("bytes", func(t *testing.T) { testReplayProviderByteBound(t, server) })
	t.Run("owner", func(t *testing.T) { testReplayAuthoritativeOwner(t, server) })
}

func testReplayProviderItemBound(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	sessionID := session.ID("items-session")
	run := f.seed(t, sessionID, "items-run")
	execution, message := replayMessageGraph(t, f, run, "items-message")
	for i := 0; i < session.ProviderStateHardMaxItems; i++ {
		data := json.RawMessage(fmt.Sprintf(`{"item":%d}`, i))
		replayProviderPart(t, f.ctx, execution, session.PartID(fmt.Sprintf("item-part-%02d", i)), message, data, int64(i))
	}
	f.reopen(t)
	if batch, err := f.store.ListMessages(f.ctx, sessionID, session.ReplayCursor{}); err != nil || len(batch.Parts) != session.ProviderStateHardMaxItems {
		t.Fatalf("valid provider state item boundary replay: parts=%d err=%v", len(batch.Parts), err)
	}
	execution = f.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	replayProviderPart(t, f.ctx, execution, "item-part-overflow", message, json.RawMessage(`{"item":"overflow"}`), session.ProviderStateHardMaxItems)
	f.reopen(t)
	if _, err := f.store.ListMessages(f.ctx, sessionID, session.ReplayCursor{}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("%d provider state items replay error = %v, want ErrConflict", session.ProviderStateHardMaxItems+1, err)
	}
}

func testReplayProviderByteBound(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	sessionID := session.ID("bytes-session")
	run := f.seed(t, sessionID, "bytes-run")
	execution, message := replayMessageGraph(t, f, run, "bytes-message")
	var payloadBytes int
	for i := 0; i < 2; i++ {
		const itemBytes = 7 << 20
		data := json.RawMessage(`{"data":"` + strings.Repeat("x", itemBytes-10) + `"}`)
		part := replayProviderPart(t, f.ctx, execution, session.PartID(fmt.Sprintf("bytes-part-%d", i)), message, data, int64(i))
		payloadBytes += len(part.Payload)
	}
	if payloadBytes > session.ProviderStateHardMaxStoredMessageBytes {
		t.Fatalf("two valid provider states already exceed aggregate bound: %d", payloadBytes)
	}
	f.reopen(t)
	if batch, err := f.store.ListMessages(f.ctx, sessionID, session.ReplayCursor{}); err != nil || len(batch.Parts) != 2 {
		t.Fatalf("valid provider state aggregate boundary replay: parts=%d err=%v", len(batch.Parts), err)
	}
	execution = f.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	const itemBytes = 7 << 20
	data := json.RawMessage(`{"data":"` + strings.Repeat("x", itemBytes-10) + `"}`)
	third := replayProviderPart(t, f.ctx, execution, "bytes-part-2", message, data, 2)
	if payloadBytes+len(third.Payload) <= session.ProviderStateHardMaxStoredMessageBytes {
		t.Fatalf("three valid provider states do not exceed aggregate bound: %d", payloadBytes+len(third.Payload))
	}
	f.reopen(t)
	if _, err := f.store.ListMessages(f.ctx, sessionID, session.ReplayCursor{}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("oversized aggregate provider state replay error = %v, want ErrConflict", err)
	}
}

func testReplayAuthoritativeOwner(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	sessionID := session.ID("owner-session")
	run := f.seed(t, sessionID, "owner-run")
	execution := f.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	first := replayMessage(t, f.ctx, execution, session.MessageID("owner-message-a"), sessionID, run.ID, f.now)
	second := replayMessage(t, f.ctx, execution, session.MessageID("owner-message-b"), sessionID, run.ID, f.now.Add(time.Second))
	part := replayProviderPart(t, f.ctx, execution, "owner-part", first, json.RawMessage(`{"owner":"first"}`), 0)
	if _, err := f.store.ListMessages(f.ctx, sessionID, session.ReplayCursor{}); err != nil {
		t.Fatalf("valid owner replay: %v", err)
	}
	var secondKey int64
	if err := f.db.QueryRowContext(f.ctx, "SELECT row_key FROM public.messages WHERE id = $1", []byte(second.ID)).Scan(&secondKey); err != nil {
		t.Fatalf("find alternate message owner: %v", err)
	}
	if _, err := f.db.ExecContext(f.ctx, "UPDATE public.parts SET message_key = $1 WHERE id = $2", secondKey, []byte(part.ID)); err != nil {
		t.Fatalf("tamper replay part owner: %v", err)
	}
	f.reopen(t)
	if _, err := f.store.ListMessages(f.ctx, sessionID, session.ReplayCursor{}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("tampered replay owner error = %v, want ErrConflict", err)
	}
}

func replayMessageGraph(t *testing.T, f *replayFixture, run session.Run, id session.MessageID) (session.ExecutionStore, session.Message) {
	t.Helper()
	execution := f.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	message := replayMessage(t, f.ctx, execution, id, run.SessionID, run.ID, f.now)
	return execution, message
}

func replayMessage(t testing.TB, ctx context.Context, execution session.ExecutionStore, id session.MessageID, sessionID session.ID, runID session.RunID, created time.Time) session.Message {
	t.Helper()
	message := session.Message{ID: id, SessionID: sessionID, RunID: runID, Role: session.RoleUser, CreatedAt: created, UpdatedAt: created}
	if _, err := execution.AppendMessage(ctx, message); err != nil {
		t.Fatalf("append replay message: %v", err)
	}
	return message
}

func replayProviderPart(t testing.TB, ctx context.Context, execution session.ExecutionStore, id session.PartID, message session.Message, data json.RawMessage, ordinal int64) session.Part {
	t.Helper()
	payload, err := session.EncodeProviderStatePayload(session.ProviderStateEnvelope{
		CodecID: "replay-codec", Version: 1, ProviderID: "replay-provider", SourceModelID: "replay-model",
		CompatibilityKey: "replay-compat", ItemIndex: int(ordinal) % session.ProviderStateHardMaxItems, Data: data,
	})
	if err != nil {
		t.Fatalf("encode provider state: %v", err)
	}
	part := session.Part{ID: id, MessageID: message.ID, SessionID: message.SessionID, RunID: message.RunID, Kind: session.PartProviderState, Ordinal: ordinal, Payload: payload, CreatedAt: message.CreatedAt, UpdatedAt: message.UpdatedAt}
	if _, err := execution.AppendPart(ctx, part); err != nil {
		t.Fatalf("append provider state: %v", err)
	}
	return part
}
