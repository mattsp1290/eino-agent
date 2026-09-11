package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cloudwego/eino/adk"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
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
// reconciliation item 1: when two or more distinct durable ids in the SAME
// outgoing request would resolve to the same provider-facing id -- whether
// from two calls in one response, from two calls in different turns that
// both reused an indexed provider id, or from a provider id that equals
// another call's durable id -- every call still gets a unique wire id.
// Processing happens in request order: the earliest call with a given
// provider id keeps it verbatim, and every later, colliding call falls back
// to its own durable id, which is reserved store-wide and therefore can
// never in turn be claimed as anyone else's wire id.
func TestPublicizeToolCallIDsKeepsDurableIDsOnCollisionWithinOneRequest(t *testing.T) {
	store := newAdmissionStore()
	seedToolCall(store, session.ToolCall{ID: "tool-call-1", SessionID: "session-1", ProviderCallID: "call_0", Name: "echo"})
	seedToolCall(store, session.ToolCall{ID: "tool-call-2", SessionID: "session-1", ProviderCallID: "call_0", Name: "echo"})
	seedToolCall(store, session.ToolCall{ID: "tool-call-3", SessionID: "session-1", ProviderCallID: "call_1", Name: "echo"})
	seedToolCall(store, session.ToolCall{ID: "tool-call-search", SessionID: "session-1", ProviderCallID: "call_2", Name: "search"})
	seedToolCall(store, session.ToolCall{ID: "tool-call-echo2", SessionID: "session-1", ProviderCallID: "call_2", Name: "echo"})

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
		// tool-call-1 is earliest in request order, so it keeps "call_0"
		// verbatim; tool-call-2 collides with it and falls back to its own
		// durable id; tool-call-3's "call_1" is unique and still rewrites.
		want := []string{"call_0", "tool-call-2", "call_1"}
		if got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
			t.Fatalf("ids = %v, want %v (earliest keeps the provider id, later collision falls back)", got, want)
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
		// tool-call-1 appears first: it keeps "call_0" on both its call and
		// result blocks. tool-call-2 collides and falls back to its own
		// durable id on both its blocks.
		if out[0].ContentBlocks[0].FunctionToolCall.CallID != "call_0" || out[1].ContentBlocks[0].FunctionToolResult.CallID != "call_0" ||
			out[2].ContentBlocks[0].FunctionToolCall.CallID != "tool-call-2" || out[3].ContentBlocks[0].FunctionToolResult.CallID != "tool-call-2" {
			t.Fatalf("cross-turn call_0 collision was not disambiguated: %#v", out)
		}
	})

	t.Run("tool_search_result collision", func(t *testing.T) {
		messages := []*einoschema.AgenticMessage{
			toolCallBlockMessage("tool-call-search", "search", `{}`),
			toolSearchResultBlockMessage("tool-call-search"),
			toolCallBlockMessage("tool-call-echo2", "echo", `{}`),
			toolResultBlockMessage("tool-call-echo2", "echo"),
		}
		out, err := publicizeToolCallIDs(context.Background(), store, "session-1", nil, messages)
		if err != nil {
			t.Fatal(err)
		}
		call := out[0].ContentBlocks[0].FunctionToolCall.CallID
		search := out[1].ContentBlocks[0].ToolSearchFunctionToolResult.CallID
		call2 := out[2].ContentBlocks[0].FunctionToolCall.CallID
		result2 := out[3].ContentBlocks[0].FunctionToolResult.CallID
		if call != search || call2 != result2 || call == call2 {
			t.Fatalf("collision not internally consistent: call=%q search=%q call2=%q result2=%q", call, search, call2, result2)
		}
		if call != "call_2" || call2 != "tool-call-echo2" {
			t.Fatalf("ids = call=%q call2=%q, want call_2/tool-call-echo2 (the search call is earliest, so it keeps call_2)", call, call2)
		}
	})
}

// TestPublicizeToolCallIDsFallsBackWhenProviderIDEqualsAnotherCallsDurableID
// proves LC-I1/OC-S3 (folded into reconciliation item 1's request-order
// rule): a provider id is refused not only when an earlier call has already
// sent it, but also when it is some other call's durable id -- durable ids
// are reserved for their own call from the start of the request, regardless
// of processing order, so a provider (or gateway) that echoes back an id it
// saw minted earlier in history can never collide with that call's own
// fallback.
func TestPublicizeToolCallIDsFallsBackWhenProviderIDEqualsAnotherCallsDurableID(t *testing.T) {
	t.Run("LC-I1: a fallback id can collide with a later call's provider id", func(t *testing.T) {
		store := newAdmissionStore()
		seedToolCall(store, session.ToolCall{ID: "tool-call-1", SessionID: "session-1", ProviderCallID: "call_0", Name: "echo"})
		seedToolCall(store, session.ToolCall{ID: "tool-call-2", SessionID: "session-1", ProviderCallID: "call_0", Name: "echo"})
		seedToolCall(store, session.ToolCall{ID: "tool-call-3", SessionID: "session-1", ProviderCallID: "tool-call-1", Name: "echo"})
		messages := []*einoschema.AgenticMessage{
			toolCallBlockMessage("tool-call-1", "echo", `{}`), toolResultBlockMessage("tool-call-1", "echo"),
			toolCallBlockMessage("tool-call-2", "echo", `{}`), toolResultBlockMessage("tool-call-2", "echo"),
			toolCallBlockMessage("tool-call-3", "echo", `{}`), toolResultBlockMessage("tool-call-3", "echo"),
		}
		out, err := publicizeToolCallIDs(context.Background(), store, "session-1", nil, messages)
		if err != nil {
			t.Fatal(err)
		}
		assertPairwiseDistinctWireIDs(t, out)
		if out[0].ContentBlocks[0].FunctionToolCall.CallID != "call_0" {
			t.Fatalf("tool-call-1 (earliest) = %q, want call_0", out[0].ContentBlocks[0].FunctionToolCall.CallID)
		}
	})

	t.Run("OC-S3: a call's own provider id equals another call's durable id", func(t *testing.T) {
		store := newAdmissionStore()
		seedToolCall(store, session.ToolCall{ID: "tool-call-a", SessionID: "session-1", ProviderCallID: "tool-call-b", Name: "echo"})
		seedToolCall(store, session.ToolCall{ID: "tool-call-b", SessionID: "session-1", ProviderCallID: "call_0", Name: "echo"})
		seedToolCall(store, session.ToolCall{ID: "tool-call-c", SessionID: "session-1", ProviderCallID: "call_0", Name: "echo"})
		messages := []*einoschema.AgenticMessage{
			toolCallBlockMessage("tool-call-a", "echo", `{}`), toolResultBlockMessage("tool-call-a", "echo"),
			toolCallBlockMessage("tool-call-b", "echo", `{}`), toolResultBlockMessage("tool-call-b", "echo"),
			toolCallBlockMessage("tool-call-c", "echo", `{}`), toolResultBlockMessage("tool-call-c", "echo"),
		}
		out, err := publicizeToolCallIDs(context.Background(), store, "session-1", nil, messages)
		if err != nil {
			t.Fatal(err)
		}
		assertPairwiseDistinctWireIDs(t, out)
		// tool-call-a's provider id ("tool-call-b") is reserved as
		// tool-call-b's own durable id from the start of the request, so
		// tool-call-a falls back even though it is processed first.
		if out[0].ContentBlocks[0].FunctionToolCall.CallID != "tool-call-a" {
			t.Fatalf("tool-call-a = %q, want tool-call-a (its own durable id, reserved by tool-call-b)", out[0].ContentBlocks[0].FunctionToolCall.CallID)
		}
		if out[2].ContentBlocks[0].FunctionToolCall.CallID != "call_0" {
			t.Fatalf("tool-call-b (earliest claimant of call_0) = %q, want call_0", out[2].ContentBlocks[0].FunctionToolCall.CallID)
		}
	})
}

// assertPairwiseDistinctWireIDs asserts every call/result pair in messages
// (built as alternating call/result messages, one block each) has matching
// call/result ids, and that every pair's id is distinct from every other
// pair's.
func assertPairwiseDistinctWireIDs(t *testing.T, messages []*einoschema.AgenticMessage) {
	t.Helper()
	seen := map[string]bool{}
	for i := 0; i+1 < len(messages); i += 2 {
		call := messages[i].ContentBlocks[0].FunctionToolCall.CallID
		result := messages[i+1].ContentBlocks[0].FunctionToolResult.CallID
		if call != result || seen[call] {
			t.Fatalf("pair %d wire ids call=%q result=%q (seen=%v): ambiguous on the wire", i/2, call, result, seen)
		}
		seen[call] = true
	}
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

// transientOnceOnCompletedToolCallStore returns a transient (non-sentinel)
// error the first time GetToolCall is asked about a completed tool call,
// then succeeds on every subsequent read of that same call.
type transientOnceOnCompletedToolCallStore struct {
	*admissionStore
	mu     sync.Mutex
	failed bool
}

func (s *transientOnceOnCompletedToolCallStore) GetToolCall(ctx context.Context, id session.ToolCallID) (session.ToolCall, error) {
	call, err := s.admissionStore.GetToolCall(ctx, id)
	if err != nil || call.Status != session.ToolCallCompleted {
		return call, err
	}
	s.mu.Lock()
	already := s.failed
	s.failed = true
	s.mu.Unlock()
	if !already {
		return session.ToolCall{}, errors.New("transient connection reset")
	}
	return call, nil
}

// TestRunRetriesTransientToolCallIDLookupFailureAndCompletes proves
// reconciliation item 2: a store.GetToolCall error that is not
// session.ErrNotFound/session.ErrConflict is NOT wrapped in
// errToolCallIDUnresolved, so it stays fully retryable under the run's
// normal WithAttempts policy instead of failing the run outright.
func TestRunRetriesTransientToolCallIDLookupFailureAndCompletes(t *testing.T) {
	store := &transientOnceOnCompletedToolCallStore{admissionStore: newAdmissionStore()}
	streamer := scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		last := request.Messages[len(request.Messages)-1]
		if last != nil && last.Role == einoschema.AgenticRoleTypeUser && isFunctionToolResultMessage(last) {
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call_0", "echo", `{}`))}, nil
	})
	orch := mustConfiguredOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: streamer}),
		WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(staticToolRegistry{tools: []Tool{{
			Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096},
			Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "hi"}, nil }),
		}}})}),
		WithAttempts(3),
	)
	result := startAndWaitRequest(t, orch, Request{SessionID: "transient-toolcallid-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v, want RunCompleted (a transient GetToolCall error must stay retryable, not fail closed)", result)
	}
}

// unresolvedToolCallStore returns session.ErrNotFound for a completed tool
// call once armed, counting how many times that injected failure is
// actually read.
type unresolvedToolCallStore struct {
	*admissionStore
	armed atomic.Bool
	reads atomic.Int64
}

func (s *unresolvedToolCallStore) GetToolCall(ctx context.Context, id session.ToolCallID) (session.ToolCall, error) {
	call, err := s.admissionStore.GetToolCall(ctx, id)
	if err == nil && s.armed.Load() && call.Status == session.ToolCallCompleted {
		s.reads.Add(1)
		return session.ToolCall{}, session.ErrNotFound
	}
	return call, err
}

// TestRunFailsClosedOnceOnDeterministicToolCallIDLookupFailure proves LC-S1:
// once a deterministic id-lookup failure (session.ErrNotFound) has struck,
// it costs exactly one store read and one streamer dispatch -- neither the
// retry policy (WithAttempts) nor a configured FailoverPolicy spends a
// second attempt on it, because errToolCallIDUnresolved is refused by both
// defaultShouldRetry and defaultShouldFailover.
func TestRunFailsClosedOnceOnDeterministicToolCallIDLookupFailure(t *testing.T) {
	store := &unresolvedToolCallStore{admissionStore: newAdmissionStore()}
	var dispatches atomic.Int64
	streamer := scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		dispatches.Add(1)
		last := request.Messages[len(request.Messages)-1]
		if last != nil && last.Role == einoschema.AgenticRoleTypeUser && isFunctionToolResultMessage(last) {
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call_0", "echo", `{}`))}, nil
	})
	plan := mustTestRunPlan(RunPlanSpec{
		Components: []PlanComponent{{Component: testPlanComponent("test-tools"), Tools: testPlanTools(staticToolRegistry{tools: []Tool{{
			Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096},
			Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
				// Arm the injected failure only once this call has actually
				// settled: the dispatch that created it must not be
				// affected, only the later continuation dispatch that reads
				// it back.
				store.armed.Store(true)
				return ToolResult{Output: "hi"}, nil
			}),
		}}})}},
		Failover: &FailoverPolicy{Models: []model.Selection{{ProviderID: "fake", ModelID: "failover-model"}}},
	})
	orch := mustConfiguredOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: streamer}),
		WithIDGenerator(&sequenceIDs{}),
		WithRunPlanProvider(staticRunPlanProvider{plan: plan}),
		WithAttempts(3),
	)
	result := startAndWaitRequest(t, orch, Request{SessionID: "unresolved-toolcallid-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if result.Status != session.RunFailed || !errors.Is(result.Error, errToolCallIDUnresolved) {
		t.Fatalf("result = %+v, want RunFailed with errToolCallIDUnresolved", result)
	}
	if got := store.reads.Load(); got != 1 {
		t.Fatalf("store reads = %d, want exactly 1 (a deterministic failure must not burn the retry/failover budget)", got)
	}
	if got := dispatches.Load(); got != 1 {
		t.Fatalf("streamer dispatches = %d, want exactly 1 (only the tool-call-producing dispatch reached the model)", got)
	}
}
