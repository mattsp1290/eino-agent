package agui

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

// TestBridgeEmitLiveMessageCommittedProjectsDurableContent is W7 item 3's
// live counterpart to the replay tests in replay_test.go: on a
// session.MessageCommittedEventKind event (see runtime/message_commit_event.go),
// Bridge.Emit reloads and reprojects the named durable message and emits it
// through the observer emitter's committed-projection path with
// DeliveryModeLiveContinuation -- custom eino.agentic.v1 supplements only,
// no duplicate native TEXT_MESSAGE_* events -- when this same connection has
// already natively streamed that message's content via emitMessageDelta.
//
// The fourth W7 fix-pass review's P0-1 finding replaced the earlier
// connection-phase flag (Bridge.inReplaySweep) this mode selection used to
// key off with a per-message record (Bridge.nativeStreamed), because a
// phase flag cannot tell a message whose deltas genuinely streamed on this
// connection apart from one that merely committed after the phase ended
// (see Bridge.nativeStreamed's doc comment; reviews/w7-fixes3-2026-09-12/).
// This test drives the real bridge.Emit(EventMessageDelta) path first, so
// it fails if nativeStreamed's bookkeeping is ever broken or removed --
// unlike a version of this test that skipped straight to the
// message_committed event, which cannot distinguish "no native content
// streamed" from "native content streamed but not tracked".
func TestBridgeEmitLiveMessageCommittedProjectsDurableContent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-live-commit"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-live-commit", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-live-commit", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	const messageID session.MessageID = "assistant-live-commit"
	if _, err := execution.AppendMessage(ctx, session.Message{
		ID: messageID, SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, TurnID: "turn-live-commit", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	parts, err := session.EncodeContentParts(session.Content{
		Role:   session.RoleAssistant,
		Blocks: []session.ContentBlock{{ID: "b1", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "committed live"}}},
	}, func() session.PartID { return "part-live-commit" }, messageID, sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(ctx, parts[0]); err != nil {
		t.Fatalf("append part: %v", err)
	}

	sink := newSSESink()
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)
	// Simulate this connection's own live turn actually streaming the
	// message's content BEFORE it commits -- the real precondition
	// DeliveryModeLiveContinuation's "no duplicate native events" guarantee
	// depends on (Bridge.nativeStreamed). Without this, nativeStreamed
	// would be empty and the commit below would correctly (per
	// emitLiveMessageCommitted's doc comment) use DeliveryModeCommittedOnly
	// instead, defeating this test's purpose.
	bridge.Emit(ctx, session.EventRecord{
		Kind: runtime.EventMessageDelta, SessionID: sessionID, RunID: run.ID, MessageID: messageID,
		Payload: []byte(`{"content":"streaming preview","reasoning":""}`),
	})
	bridge.Emit(ctx, session.EventRecord{
		Kind: session.MessageCommittedEventKind, SessionID: sessionID, RunID: run.ID, MessageID: messageID,
		TurnID: "turn-live-commit", Payload: []byte(`{"revision":1}`),
	})
	if err := bridge.EncErr(); err != nil {
		t.Fatalf("emitter encoding error = %v", err)
	}
	if err := bridge.Err(); err != nil {
		t.Fatalf("emitter transport error = %v", err)
	}

	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	want := "TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT,CUSTOM"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s (the delta's own native frames, then a single CUSTOM content-block supplement for the commit -- no duplicate native events on the live path)", got, want)
	}
	raw := string(sink.Bytes())
	if !strings.Contains(raw, "committed live") {
		t.Fatalf("committed text missing from stream: %s", raw)
	}
	if !strings.Contains(raw, `"turnId":"turn-live-commit"`) {
		t.Fatalf("durable turn identity missing from stream: %s", raw)
	}
}

// TestBridgeEmitLiveMessageCommittedNoopWithoutStore proves the live
// committed-projection path is a documented no-op (not a panic) when Bridge
// was constructed without a store -- existing classic-only callers/tests.
func TestBridgeEmitLiveMessageCommittedNoopWithoutStore(t *testing.T) {
	t.Parallel()

	sink := newSSESink()
	bridge := NewBridge(context.Background(), nil, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	bridge.Emit(context.Background(), session.EventRecord{
		Kind: session.MessageCommittedEventKind, SessionID: "session-1", RunID: "run-1", MessageID: "message-1",
	})
	if len(sink.Bytes()) != 0 {
		t.Fatalf("expected no output without a store, got: %s", sink.Bytes())
	}
}

// TestEmitMessageSnapshotIncludeReasoningGate proves the W7 review's P0
// finding (A2/finding I1): durable reasoning content must default to
// excluded from replay -- matching the pre-W7 classic history pipeline's
// default and agui.GateProviderReasoningStorage -- and must appear only
// when a host explicitly opts in via the includeReasoning parameter.
// Before this fix, loadCommittedProjections passed
// history.Options{IncludeReasoning: true} unconditionally, so durable
// reasoning streamed to every reconnecting client regardless of host
// policy, and no test distinguished the two directions of the flag.
func TestEmitMessageSnapshotIncludeReasoningGate(t *testing.T) {
	t.Parallel()

	buildSession := func(t *testing.T) (session.Store, session.ID) {
		t.Helper()
		ctx := context.Background()
		store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
		if err != nil {
			t.Fatalf("open sqlite store: %v", err)
		}
		t.Cleanup(func() { _ = storePool.Close() })
		const sessionID session.ID = "session-reasoning-gate"
		now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
		if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatalf("create session: %v", err)
		}
		run, err := store.AdmitRun(ctx, session.Run{ID: "run-reasoning-gate", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-reasoning-gate", Status: session.RunPending, CreatedAt: now}, time.Minute)
		if err != nil {
			t.Fatalf("admit run: %v", err)
		}
		execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
		if _, err := execution.AppendMessage(ctx, session.Message{ID: "assistant-reasoning", SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant, TurnID: "turn-reasoning", CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatalf("append message: %v", err)
		}
		content := session.Content{Role: session.RoleAssistant, Blocks: []session.ContentBlock{
			{ID: "blk-reasoning", Kind: session.BlockKindReasoning, Reasoning: &session.ReasoningBlock{Text: "secret chain of thought"}},
			{ID: "blk-text", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "visible answer"}},
		}}
		n := 0
		nextPartID := func() session.PartID { n++; return session.PartID(fmt.Sprintf("part-reasoning-%d", n)) }
		parts, err := session.EncodeContentParts(content, nextPartID, "assistant-reasoning", sessionID, run.ID, now, session.DefaultContentLimits())
		if err != nil {
			t.Fatalf("EncodeContentParts: %v", err)
		}
		for _, part := range parts {
			if _, err := execution.AppendPart(ctx, part); err != nil {
				t.Fatalf("append part %s: %v", part.ID, err)
			}
		}
		return store, sessionID
	}

	t.Run("excluded by default", func(t *testing.T) {
		store, sessionID := buildSession(t)
		ctx := context.Background()
		sink := newSSESink()
		bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), "run-reasoning-gate", nil)
		if err := emitMessageSnapshot(ctx, bridge, store, sessionID, session.ContentLimits{}, false); err != nil {
			t.Fatalf("emitMessageSnapshot error = %v", err)
		}
		raw := string(sink.Bytes())
		if strings.Contains(raw, "secret chain of thought") {
			t.Fatalf("durable reasoning leaked into replay under the default (includeReasoning=false): %s", raw)
		}
		if !strings.Contains(raw, "visible answer") {
			t.Fatalf("non-reasoning content missing from replay: %s", raw)
		}
	})

	t.Run("included when host opts in", func(t *testing.T) {
		store, sessionID := buildSession(t)
		ctx := context.Background()
		sink := newSSESink()
		bridge := NewBridge(ctx, store, session.ContentLimits{}, true, sink.Writer(), sse.NewSSEWriter(), string(sessionID), "run-reasoning-gate", nil)
		if err := emitMessageSnapshot(ctx, bridge, store, sessionID, session.ContentLimits{}, true); err != nil {
			t.Fatalf("emitMessageSnapshot error = %v", err)
		}
		raw := string(sink.Bytes())
		if !strings.Contains(raw, "secret chain of thought") {
			t.Fatalf("durable reasoning missing from replay despite includeReasoning=true (host opted in): %s", raw)
		}
	})
}

// TestAgenticIdentityProjectsConsistentAgentPathAcrossTurn proves the W7
// review's A4 finding: every message in one turn must project the same
// agentPath. Before this fix, agenticIdentity keyed off
// session.Message.Agent (set to the configured agent's display name on
// assistant messages, but always left empty on user messages -- see
// admissionUserMessage/admissionAssistantMessage in the runtime package),
// so a user message projected "root" while the assistant message in the
// SAME turn projected the configured agent's own name: an inconsistency
// within a single turn a client would see as the agent path changing
// mid-conversation for no reason.
func TestAgenticIdentityProjectsConsistentAgentPathAcrossTurn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, storePool, err := openTestSQLite(ctx, filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = storePool.Close() })

	const sessionID session.ID = "session-agent-path"
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(ctx, session.Session{ID: sessionID, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: "run-agent-path", SessionID: sessionID, OwnerID: "owner", ClaimToken: "claim-agent-path", Status: session.RunPending, CreatedAt: now}, time.Minute)
	if err != nil {
		t.Fatalf("admit run: %v", err)
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})

	if _, err := execution.AppendMessage(ctx, session.Message{ID: "user-agent-path", SessionID: sessionID, RunID: run.ID, Role: session.RoleUser, TurnID: "turn-agent-path", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	userParts, err := session.EncodeContentParts(session.Content{Role: session.RoleUser, Blocks: []session.ContentBlock{
		{ID: "blk-user-text", Kind: session.BlockKindUserInputText, Text: &session.TextBlock{Text: "hi"}},
	}}, func() session.PartID { return "part-user-agent-path" }, "user-agent-path", sessionID, run.ID, now, session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(ctx, userParts[0]); err != nil {
		t.Fatalf("append part: %v", err)
	}

	// The assistant message in the SAME turn: Agent is set to the
	// configured agent's display name (see admissionAssistantMessage in
	// runtime), but AgentPath is left empty (subagent nesting is not wired
	// end to end yet).
	if _, err := execution.AppendMessage(ctx, session.Message{
		ID: "assistant-agent-path", SessionID: sessionID, RunID: run.ID, Role: session.RoleAssistant,
		Agent: "customer-support-agent", TurnID: "turn-agent-path", CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("append message: %v", err)
	}
	assistantParts, err := session.EncodeContentParts(session.Content{Role: session.RoleAssistant, Blocks: []session.ContentBlock{
		{ID: "blk-assistant-text", Kind: session.BlockKindAssistantGenText, Text: &session.TextBlock{Text: "hello back"}},
	}}, func() session.PartID { return "part-assistant-agent-path" }, "assistant-agent-path", sessionID, run.ID, now.Add(time.Second), session.DefaultContentLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.AppendPart(ctx, assistantParts[0]); err != nil {
		t.Fatalf("append part: %v", err)
	}

	sink := newSSESink()
	bridge := NewBridge(ctx, store, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), string(sessionID), string(run.ID), nil)
	if err := emitMessageSnapshot(ctx, bridge, store, sessionID, session.ContentLimits{}, false); err != nil {
		t.Fatalf("emitMessageSnapshot error = %v", err)
	}
	raw := string(sink.Bytes())
	if strings.Contains(raw, "customer-support-agent") {
		t.Fatalf("assistant message projected the configured agent's display name into agentPath instead of falling back to root, breaking cross-turn agent-path consistency with the user message: %s", raw)
	}
	if !strings.Contains(raw, `"name":"root"`) {
		t.Fatalf("expected both messages to project the root agent path: %s", raw)
	}
}
