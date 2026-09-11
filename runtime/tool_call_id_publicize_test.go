package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/cloudwego/eino/adk"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"
)

// seedToolCall writes a session.ToolCall directly into store.toolCalls,
// bypassing CreateToolCall's relational bookkeeping: these unit tests only
// need store.GetToolCall to resolve id -> the given record, not a fully
// admitted session/run/message/part graph.
func seedToolCall(store *admissionStore, call session.ToolCall) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.toolCalls[call.ID] = call
}

func toolCallBlockMessage(callID, name, arguments string) *einoschema.AgenticMessage {
	return agenticAssistantToolCalls(agenticToolCall(callID, name, arguments))
}

func toolResultBlockMessage(callID, name string) *einoschema.AgenticMessage {
	return &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeUser,
		ContentBlocks: []*einoschema.ContentBlock{{
			Type: einoschema.ContentBlockTypeFunctionToolResult,
			FunctionToolResult: &einoschema.FunctionToolResult{
				CallID: callID, Name: name,
				Content: []*einoschema.FunctionToolResultContentBlock{{Type: einoschema.FunctionToolResultContentBlockTypeText, Text: &einoschema.UserInputText{Text: "ok"}}},
			},
		}},
	}
}

func toolSearchResultBlockMessage(callID string) *einoschema.AgenticMessage {
	return &einoschema.AgenticMessage{
		Role: einoschema.AgenticRoleTypeUser,
		ContentBlocks: []*einoschema.ContentBlock{{
			Type:                         einoschema.ContentBlockTypeToolSearchResult,
			ToolSearchFunctionToolResult: &einoschema.ToolSearchFunctionToolResult{CallID: callID},
		}},
	}
}

// TestPublicizeToolCallIDsRewritesAllBlockKindsAndFallsBackWhenEmpty proves
// WC-I1/DI-I2's wire-coverage gap: function_tool_call, function_tool_result
// and tool_search_result blocks are all rewritten to the provider's id, and
// a call whose ProviderCallID is empty (the provider left CallID unset)
// keeps the durable id on the wire instead.
func TestPublicizeToolCallIDsRewritesAllBlockKindsAndFallsBackWhenEmpty(t *testing.T) {
	store := newAdmissionStore()
	seedToolCall(store, session.ToolCall{ID: "tool-call-1", SessionID: "session-1", ProviderCallID: "call_0", Name: "echo"})
	seedToolCall(store, session.ToolCall{ID: "tool-call-2", SessionID: "session-1", ProviderCallID: "", Name: "search"})

	messages := []*einoschema.AgenticMessage{
		toolCallBlockMessage("tool-call-1", "echo", `{}`),
		toolResultBlockMessage("tool-call-1", "echo"),
		toolSearchResultBlockMessage("tool-call-2"),
		toolCallBlockMessage("tool-call-2", "search", `{}`),
	}
	out, err := publicizeToolCallIDs(context.Background(), store, "session-1", nil, messages)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].ContentBlocks[0].FunctionToolCall.CallID != "call_0" {
		t.Fatalf("function_tool_call id = %q, want call_0", out[0].ContentBlocks[0].FunctionToolCall.CallID)
	}
	if out[1].ContentBlocks[0].FunctionToolResult.CallID != "call_0" {
		t.Fatalf("function_tool_result id = %q, want call_0", out[1].ContentBlocks[0].FunctionToolResult.CallID)
	}
	// tool-call-2 has no ProviderCallID: the wire keeps showing the durable
	// (minted) id, since there is no separate provider id to preserve.
	if out[2].ContentBlocks[0].ToolSearchFunctionToolResult.CallID != "tool-call-2" {
		t.Fatalf("tool_search_result id = %q, want tool-call-2 (empty ProviderCallID falls back to durable id)", out[2].ContentBlocks[0].ToolSearchFunctionToolResult.CallID)
	}
	if out[3].ContentBlocks[0].FunctionToolCall.CallID != "tool-call-2" {
		t.Fatalf("function_tool_call id = %q, want tool-call-2", out[3].ContentBlocks[0].FunctionToolCall.CallID)
	}
}

// TestPublicizeToolCallIDsDoesNotMutateInput proves the load-bearing
// non-mutation invariant DI-I2 flagged as unpinned: the original message/
// block objects ADK itself is tracking must be untouched, since
// durableProjection's sameIDSet check and ADK's own conversation state stay
// keyed on the durable id.
func TestPublicizeToolCallIDsDoesNotMutateInput(t *testing.T) {
	store := newAdmissionStore()
	seedToolCall(store, session.ToolCall{ID: "tool-call-1", SessionID: "session-1", ProviderCallID: "call_0", Name: "echo"})

	original := toolCallBlockMessage("tool-call-1", "echo", `{}`)
	originalBlock := original.ContentBlocks[0]
	originalCall := originalBlock.FunctionToolCall
	messages := []*einoschema.AgenticMessage{original}

	out, err := publicizeToolCallIDs(context.Background(), store, "session-1", nil, messages)
	if err != nil {
		t.Fatal(err)
	}
	if messages[0] != original || original.ContentBlocks[0] != originalBlock || originalBlock.FunctionToolCall != originalCall {
		t.Fatalf("input message/block/call pointers were replaced")
	}
	if originalCall.CallID != "tool-call-1" {
		t.Fatalf("input call id mutated in place: %q, want tool-call-1 unchanged", originalCall.CallID)
	}
	if out[0] == original {
		t.Fatalf("rewritten message reuses the same pointer as the input")
	}
	if out[0].ContentBlocks[0].FunctionToolCall.CallID != "call_0" {
		t.Fatalf("rewritten call id = %q, want call_0", out[0].ContentBlocks[0].FunctionToolCall.CallID)
	}
}

// TestPublicizeToolCallIDsKeepsDurableIDsOnCollisionWithinOneRequest proves
// WC-I3: when two distinct durable ids in the SAME outgoing request would
// resolve to the same provider-facing id -- whether from two calls in one
// response, or from two calls in different turns that both reused an
// indexed provider id -- both keep their own durable id on the wire instead
// of an ambiguous shared value.
func TestPublicizeToolCallIDsKeepsDurableIDsOnCollisionWithinOneRequest(t *testing.T) {
	store := newAdmissionStore()
	seedToolCall(store, session.ToolCall{ID: "tool-call-1", SessionID: "session-1", ProviderCallID: "call_0", Name: "echo"})
	seedToolCall(store, session.ToolCall{ID: "tool-call-2", SessionID: "session-1", ProviderCallID: "call_0", Name: "echo"})
	seedToolCall(store, session.ToolCall{ID: "tool-call-3", SessionID: "session-1", ProviderCallID: "call_1", Name: "echo"})

	t.Run("two calls in one response", func(t *testing.T) {
		messages := []*einoschema.AgenticMessage{{
			Role: einoschema.AgenticRoleTypeAssistant,
			ContentBlocks: []*einoschema.ContentBlock{
				{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &einoschema.FunctionToolCall{CallID: "tool-call-1", Name: "echo"}},
				{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &einoschema.FunctionToolCall{CallID: "tool-call-2", Name: "echo"}},
				{Type: einoschema.ContentBlockTypeFunctionToolCall, FunctionToolCall: &einoschema.FunctionToolCall{CallID: "tool-call-3", Name: "echo"}},
			},
		}}
		out, err := publicizeToolCallIDs(context.Background(), store, "session-1", nil, messages)
		if err != nil {
			t.Fatal(err)
		}
		got := []string{
			out[0].ContentBlocks[0].FunctionToolCall.CallID,
			out[0].ContentBlocks[1].FunctionToolCall.CallID,
			out[0].ContentBlocks[2].FunctionToolCall.CallID,
		}
		want := []string{"tool-call-1", "tool-call-2", "call_1"}
		if got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
			t.Fatalf("ids = %v, want %v (colliding pair keeps durable ids, unique one still rewrites)", got, want)
		}
	})

	t.Run("call_0 reused across two turns in the same request", func(t *testing.T) {
		messages := []*einoschema.AgenticMessage{
			toolCallBlockMessage("tool-call-1", "echo", `{}`),
			toolResultBlockMessage("tool-call-1", "echo"),
			toolCallBlockMessage("tool-call-2", "echo", `{}`),
			toolResultBlockMessage("tool-call-2", "echo"),
		}
		out, err := publicizeToolCallIDs(context.Background(), store, "session-1", nil, messages)
		if err != nil {
			t.Fatal(err)
		}
		if out[0].ContentBlocks[0].FunctionToolCall.CallID != "tool-call-1" || out[1].ContentBlocks[0].FunctionToolResult.CallID != "tool-call-1" ||
			out[2].ContentBlocks[0].FunctionToolCall.CallID != "tool-call-2" || out[3].ContentBlocks[0].FunctionToolResult.CallID != "tool-call-2" {
			t.Fatalf("cross-turn call_0 collision was not disambiguated: %#v", out)
		}
	})
}

// TestPublicizeToolCallIDsFailsClosedOnMissingRow proves WC-S1: a block
// whose durable CallID has no tool_calls row fails the rewrite with
// errToolCallIDUnresolved (not a bare not-found), so the retry/failover
// policies can refuse to retry it (see TestDefaultShouldRetryAndShouldFailoverRefuseUnresolvedToolCallID).
func TestPublicizeToolCallIDsFailsClosedOnMissingRow(t *testing.T) {
	store := newAdmissionStore()
	messages := []*einoschema.AgenticMessage{toolCallBlockMessage("not-in-store", "echo", `{}`)}
	_, err := publicizeToolCallIDs(context.Background(), store, "session-1", nil, messages)
	if err == nil || !errors.Is(err, errToolCallIDUnresolved) {
		t.Fatalf("err = %v, want errToolCallIDUnresolved", err)
	}
}

// TestPublicizeToolCallIDsFailsClosedOnCrossSessionRow proves the DI-S1
// session fence: GetToolCall is store-global, so a durable id that resolves
// to a row belonging to a different session must not publish that other
// session's provider id onto this session's wire request.
func TestPublicizeToolCallIDsFailsClosedOnCrossSessionRow(t *testing.T) {
	store := newAdmissionStore()
	seedToolCall(store, session.ToolCall{ID: "tool-call-1", SessionID: "other-session", ProviderCallID: "call_0", Name: "echo"})
	messages := []*einoschema.AgenticMessage{toolCallBlockMessage("tool-call-1", "echo", `{}`)}
	_, err := publicizeToolCallIDs(context.Background(), store, "session-1", nil, messages)
	if err == nil || !errors.Is(err, errToolCallIDUnresolved) {
		t.Fatalf("err = %v, want errToolCallIDUnresolved", err)
	}
}

// TestDefaultShouldRetryAndShouldFailoverRefuseUnresolvedToolCallID proves
// reconciliation item 4: a publicizeToolCallIDs failure is a deterministic,
// durable-consistency error, not a transient one, so neither the retry nor
// the failover policy should spend an attempt on it.
func TestDefaultShouldRetryAndShouldFailoverRefuseUnresolvedToolCallID(t *testing.T) {
	err := fmt.Errorf("resolve provider-facing tool call id for x: %w", errToolCallIDUnresolved)
	decision := defaultShouldRetry(context.Background(), &adk.TypedRetryContext[*einoschema.AgenticMessage]{Err: err})
	if decision == nil || decision.Retry {
		t.Fatalf("defaultShouldRetry decision = %#v, want Retry=false", decision)
	}
	if defaultShouldFailover(context.Background(), nil, err) {
		t.Fatal("defaultShouldFailover = true, want false")
	}
	// Sanity: an ordinary error is still retried/failed over (this sentinel
	// check must not have swallowed the general case).
	other := errors.New("transient")
	if d := defaultShouldRetry(context.Background(), &adk.TypedRetryContext[*einoschema.AgenticMessage]{Err: other}); d == nil || !d.Retry {
		t.Fatalf("defaultShouldRetry(other) = %#v, want Retry=true", d)
	}
	if !defaultShouldFailover(context.Background(), nil, other) {
		t.Fatal("defaultShouldFailover(other) = false, want true")
	}
}

// TestPublicizeToolCallIDsCacheAvoidsRepeatedStoreReadsPerDispatch proves
// reconciliation item 6: the durable-id -> provider-id mapping is resolved
// from the store at most once per distinct call over a turn's dispatches,
// not once per call on every dispatch.
func TestPublicizeToolCallIDsCacheAvoidsRepeatedStoreReadsPerDispatch(t *testing.T) {
	store := newAdmissionStore()
	seedToolCall(store, session.ToolCall{ID: "tool-call-1", SessionID: "session-1", ProviderCallID: "call_0", Name: "echo"})
	seedToolCall(store, session.ToolCall{ID: "tool-call-2", SessionID: "session-1", ProviderCallID: "call_1", Name: "echo"})

	var reads int
	counting := &countingGetToolCallStore{admissionStore: store, onGet: func() { reads++ }}
	var cache toolCallIDCache

	// Three dispatches, each resolving the whole growing history: dispatch
	// 3 sees both calls again. Without the cache this reads the store 3
	// times (1 + 2); with it, exactly once per distinct call (2 total).
	first := []*einoschema.AgenticMessage{toolCallBlockMessage("tool-call-1", "echo", `{}`)}
	second := []*einoschema.AgenticMessage{toolCallBlockMessage("tool-call-1", "echo", `{}`), toolCallBlockMessage("tool-call-2", "echo", `{}`)}
	third := []*einoschema.AgenticMessage{toolCallBlockMessage("tool-call-1", "echo", `{}`), toolCallBlockMessage("tool-call-2", "echo", `{}`)}
	for _, batch := range [][]*einoschema.AgenticMessage{first, second, third} {
		if _, err := publicizeToolCallIDs(context.Background(), counting, "session-1", &cache, batch); err != nil {
			t.Fatal(err)
		}
	}
	if reads != 2 {
		t.Fatalf("store reads = %d, want exactly 2 (one per distinct call, cached across dispatches)", reads)
	}
}

type countingGetToolCallStore struct {
	*admissionStore
	mu    sync.Mutex
	onGet func()
}

func (s *countingGetToolCallStore) GetToolCall(ctx context.Context, id session.ToolCallID) (session.ToolCall, error) {
	s.mu.Lock()
	if s.onGet != nil {
		s.onGet()
	}
	s.mu.Unlock()
	return s.admissionStore.GetToolCall(ctx, id)
}
