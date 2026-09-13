package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// functionCallAndResultBlocks returns, in message order, the CallID of
// every function_tool_call block (calls) and every function_tool_result
// block (results) across messages -- the wire-level view a scripted
// streamer captures from the model.Request it actually received.
func functionCallAndResultBlocks(messages []*einoschema.AgenticMessage) (calls, results []string) {
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		for _, block := range msg.ContentBlocks {
			if block == nil {
				continue
			}
			switch block.Type {
			case einoschema.ContentBlockTypeFunctionToolCall:
				if block.FunctionToolCall != nil {
					calls = append(calls, block.FunctionToolCall.CallID)
				}
			case einoschema.ContentBlockTypeFunctionToolResult:
				if block.FunctionToolResult != nil {
					results = append(results, block.FunctionToolResult.CallID)
				}
			}
		}
	}
	return calls, results
}

// TestProviderCallIDReuseAcrossRunsProducesDistinctRowsAndCollisionSafeWire
// proves reconciliation item 2's "provider reuses call_0 ... across two
// runs in one session" regression, combined with item 1's duplicate-id
// disambiguation: on base 48b9558, prepareToolCalls reused the provider's
// own CallID verbatim as the durable, store-wide-unique tool_calls.id, so
// the second run's reuse of "call_0" collided with the first run's row and
// failed with session.ErrConflict. Fixed, both runs complete with distinct
// minted rows; the provider still sees its own id back whenever it is
// unambiguous (the first run's completed call, before the second run's own
// call exists), and once both calls (from different runs, same session,
// same provider id) coexist in one outgoing request, the EARLIEST one in
// request order (run "one"'s, since it is earlier in session history) keeps
// "call_0" verbatim -- unchanged from the earlier request that already sent
// it -- while the later, colliding call falls back to its own durable id.
// See TestPublicizeToolCallIDsKeepsDurableIDsOnCollisionWithinOneRequest.
func TestProviderCallIDReuseAcrossRunsProducesDistinctRowsAndCollisionSafeWire(t *testing.T) {
	store := newAdmissionStore()
	var mu sync.Mutex
	var requests []model.Request
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		clone, err := request.Clone()
		if err != nil {
			return nil, err
		}
		mu.Lock()
		requests = append(requests, clone)
		mu.Unlock()
		last := request.Messages[len(request.Messages)-1]
		if last != nil && last.Role == einoschema.AgenticRoleTypeUser && isFunctionToolResultMessage(last) {
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call_0", "echo", `{"text":"hi"}`))}, nil
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "hi"}, nil }),
	}}})
	for _, prompt := range []string{"one", "two"} {
		if r := startAndWaitRequest(t, orch, Request{SessionID: "reuse-session", Message: TextUserMessage(prompt), Config: orchestratorConfig()}); r.Status != session.RunCompleted {
			t.Fatalf("run %q = %+v", prompt, r)
		}
	}
	if len(requests) != 4 {
		t.Fatalf("captured %d requests, want 4 (two dispatches per run)", len(requests))
	}

	store.mu.Lock()
	var calls []session.ToolCall
	for _, call := range store.toolCalls {
		calls = append(calls, call)
	}
	store.mu.Unlock()
	if len(calls) != 2 || calls[0].ID == calls[1].ID || calls[0].ProviderCallID != "call_0" || calls[1].ProviderCallID != "call_0" {
		t.Fatalf("tool calls = %#v, want 2 distinct ids both ProviderCallID=call_0", calls)
	}

	// requests[2]: run "two"'s first dispatch. Only run "one"'s completed
	// call is in history yet (run "two" has not called the tool), so
	// "call_0" is unambiguous and the provider sees its own id verbatim.
	firstCalls, firstResults := functionCallAndResultBlocks(requests[2].Messages)
	if len(firstCalls) != 1 || firstCalls[0] != "call_0" || len(firstResults) != 1 || firstResults[0] != "call_0" {
		t.Fatalf("requests[2] call/result ids = %v/%v, want [call_0]/[call_0]", firstCalls, firstResults)
	}

	// requests[3]: run "two"'s continuation. History now carries BOTH run
	// "one"'s and run "two"'s completed calls, both ProviderCallID
	// "call_0" -- an outright collision within this one outgoing request.
	// Run "one"'s call is earliest in request order, so it keeps "call_0"
	// verbatim; run "two"'s call, appearing later, falls back to its own
	// durable id instead of sending the ambiguous shared value.
	lastCalls, lastResults := functionCallAndResultBlocks(requests[3].Messages)
	if len(lastCalls) != 2 || len(lastResults) != 2 {
		t.Fatalf("requests[3] = %d calls, %d results, want 2 and 2", len(lastCalls), len(lastResults))
	}
	if lastCalls[0] != "call_0" || lastCalls[1] == "call_0" || lastCalls[0] == lastCalls[1] {
		t.Fatalf("requests[3] colliding provider id was not disambiguated (earliest-keeps): calls=%v", lastCalls)
	}
	if lastCalls[0] != lastResults[0] || lastCalls[1] != lastResults[1] {
		t.Fatalf("requests[3] call ids %v do not pair with result ids %v", lastCalls, lastResults)
	}
	// Earliest-keeps prefix property (reconciliation item 1): requests[2]
	// already sent run "one"'s call under "call_0"; that value must not
	// change in requests[3] just because a new, colliding "call_0" entered
	// history -- otherwise a provider's prompt-prefix cache over that
	// earlier history would go stale.
	if lastCalls[0] != firstCalls[0] {
		t.Fatalf("earlier request's wire id changed once a later collision appeared: requests[2]=%v requests[3]=%v", firstCalls, lastCalls)
	}
}

// TestProviderCallIDReuseAcrossTurnsWithinOneRunProducesDistinctRowsAndCollisionSafeWire
// is the within-one-run counterpart of the above (reconciliation item 2's
// "across two turns" case, DI-I2): a provider that reuses "call_0" for two
// consecutive tool calls inside the SAME run must still complete with two
// distinct durable rows -- base 48b9558 fails the second with
// "session store conflict" (the durable id collision) -- and once both
// calls coexist in one outgoing request, the collision is disambiguated the
// same earliest-keeps way as the cross-run case: the first call keeps
// "call_0", the second falls back to its own durable id.
func TestProviderCallIDReuseAcrossTurnsWithinOneRunProducesDistinctRowsAndCollisionSafeWire(t *testing.T) {
	var mu sync.Mutex
	var requests []model.Request
	dispatches := 0
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(_ context.Context, req model.Request) ([]*einoschema.AgenticMessage, error) {
		clone, err := req.Clone()
		if err != nil {
			return nil, err
		}
		mu.Lock()
		requests = append(requests, clone)
		dispatches++
		n := dispatches
		mu.Unlock()
		if n <= 2 {
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call_0", "echo", `{"text":"hi"}`))}, nil
		}
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	defer cleanup()
	var executedMu sync.Mutex
	var executed []ToolCall
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			executedMu.Lock()
			executed = append(executed, call)
			executedMu.Unlock()
			return ToolResult{Output: "ok"}, nil
		}),
	}}})
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result) // base 48b9558: session store conflict
	}
	if len(executed) != 2 || executed[0].ID == executed[1].ID || executed[0].ProviderCallID != "call_0" || executed[1].ProviderCallID != "call_0" {
		t.Fatalf("executed = %#v", executed)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 3 {
		t.Fatalf("captured %d requests, want 3", len(requests))
	}
	// requests[1]: second dispatch, only the first call has settled --
	// "call_0" is unambiguous.
	calls1, results1 := functionCallAndResultBlocks(requests[1].Messages)
	if len(calls1) != 1 || calls1[0] != "call_0" || len(results1) != 1 || results1[0] != "call_0" {
		t.Fatalf("requests[1] = %v/%v, want [call_0]/[call_0]", calls1, results1)
	}
	// requests[2]: third dispatch, both calls now share "call_0" in one
	// request. The first call (earliest in request order) keeps "call_0"
	// verbatim -- unchanged from requests[1] -- and the second falls back to
	// its own durable id.
	calls2, results2 := functionCallAndResultBlocks(requests[2].Messages)
	if len(calls2) != 2 || len(results2) != 2 || calls2[0] != "call_0" || calls2[1] == "call_0" || calls2[0] == calls2[1] {
		t.Fatalf("requests[2] = %v/%v, want [call_0, <distinct durable id>] (earliest-keeps)", calls2, results2)
	}
	if calls2[0] != results2[0] || calls2[1] != results2[1] {
		t.Fatalf("requests[2] call ids %v do not pair with result ids %v", calls2, results2)
	}
	// Earliest-keeps prefix property: requests[1] already sent the first
	// call under "call_0"; that value must not change in requests[2] just
	// because a second, colliding "call_0" entered history.
	if calls2[0] != calls1[0] {
		t.Fatalf("earlier request's wire id changed once a later collision appeared: requests[1]=%v requests[2]=%v", calls1, calls2)
	}
}

// selectionAwareModel resolves a distinct model.Resolved (and streamer) per
// selection.ModelID, unlike resolvedModel (which ignores the selection
// entirely): needed so a failover attempt's ledger row and wire dispatch
// are actually distinguishable from the primary attempt's -- with
// resolvedModel, both report the same fake/test identity, so a test could
// pass even if the "failover" attempt were silently dispatched through the
// primary model again (LC-S3).
type selectionAwareModel struct {
	streamers map[model.ID]model.Streamer
}

func (r selectionAwareModel) Resolve(_ context.Context, selection model.Selection, _ model.Runtime) (model.Resolved, error) {
	streamer, ok := r.streamers[selection.ModelID]
	if !ok {
		return model.Resolved{}, fmt.Errorf("selectionAwareModel: no streamer configured for model %q", selection.ModelID)
	}
	return model.Resolved{
		Provider: model.Provider{ID: selection.ProviderID},
		Model:    model.Descriptor{ID: selection.ModelID, ProviderID: selection.ProviderID},
		Streamer: streamer,
	}, nil
}

// TestModelRequestLedgerMatchesWireRequestForToolCallStep proves
// reconciliation item 1: for EVERY physical attempt in a turn -- the
// original dispatch, a retried attempt, and a failed-over attempt, not just
// the happy two-step path -- the request ledger's ContentSHA256 (and the
// Messages it was derived from) must equal auditModelRequest's hash of the
// exact request the streamer actually received, never a request rebuilt
// from different (durable-id) call ids, as happened when
// publicizeToolCallIDs ran a second time inside dispatch() after begin()
// had already audited the pre-rewrite input.
func TestModelRequestLedgerMatchesWireRequestForToolCallStep(t *testing.T) {
	for _, tc := range []struct {
		name           string
		attempts       int
		failover       bool
		wantDispatches int
	}{
		{name: "primary", attempts: 1, wantDispatches: 2},
		{name: "retry", attempts: 2, wantDispatches: 3},
		{name: "failover", attempts: 1, failover: true, wantDispatches: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, storePool, err := openTestSQLite(context.Background(), filepath.Join(t.TempDir(), "store.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = storePool.Close() }()

			var mu sync.Mutex
			var submitted []model.Request
			record := func(request model.Request) {
				clone, cloneErr := request.Clone()
				if cloneErr != nil {
					t.Fatal(cloneErr)
				}
				mu.Lock()
				submitted = append(submitted, clone)
				mu.Unlock()
			}

			// Primary/retry: step 1 mints a tool call ("call_0"); step 2
			// (retry variant) fails once with a transient error before the
			// retried attempt succeeds with text. Failover: step 1 is the
			// same tool call; step 2 (the primary model's own continuation
			// attempt) fails outright, triggering ADK's failover wrapper --
			// the failover model's own first dispatch then succeeds.
			primaryCalls := 0
			primaryStreamer := scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
				primaryCalls++
				record(request)
				switch {
				case primaryCalls == 1:
					return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call_0", "echo", `{"text":"hi"}`))}, nil
				case tc.name == "retry" && primaryCalls == 2:
					return nil, errors.New("transient")
				case tc.name == "failover" && primaryCalls == 2:
					return nil, errors.New("primary model unavailable")
				default:
					return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
				}
			})
			failoverStreamer := scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
				record(request)
				return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
			})
			resolver := selectionAwareModel{streamers: map[model.ID]model.Streamer{
				"test":           primaryStreamer,
				"failover-model": failoverStreamer,
			}}

			toolRegistry := staticToolRegistry{tools: []Tool{{
				Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096},
				Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "hi"}, nil }),
			}}}
			plan := newTestToolPlan(toolRegistry)
			if tc.failover {
				plan = mustTestRunPlan(RunPlanSpec{
					Components: []PlanComponent{{Component: testPlanComponent("test-tools"), Tools: testPlanTools(toolRegistry)}},
					Failover:   &FailoverPolicy{Models: []model.Selection{{ProviderID: "fake", ModelID: "failover-model"}}},
				})
			}

			options := []Option{
				WithStore(store), WithModelResolver(resolver), WithIDGenerator(&sequenceIDs{}),
				WithRunPlanProvider(staticRunPlanProvider{plan: plan}),
				WithClock(func() time.Time { return time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC) }),
			}
			if tc.attempts > 1 {
				options = append(options, WithAttempts(tc.attempts))
			}
			orchestrator, err := NewStreamingOrchestrator(options...)
			if err != nil {
				t.Fatal(err)
			}
			result := startAndWaitRequest(t, orchestrator, Request{SessionID: session.ID("ledger-tool-session-" + tc.name), Message: TextUserMessage("hello"), Config: orchestratorConfig()})
			if result.Status != session.RunCompleted {
				t.Fatalf("result = %+v", result)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(submitted) != tc.wantDispatches {
				t.Fatalf("submitted = %d requests, want %d", len(submitted), tc.wantDispatches)
			}
			batch, err := store.ListModelRequests(context.Background(), result.RunID, session.ModelRequestCursor{Limit: 10})
			if err != nil || len(batch.Records) != tc.wantDispatches {
				t.Fatalf("records = %#v, %v", batch, err)
			}
			sort.Slice(batch.Records, func(i, j int) bool { return batch.Records[i].Step < batch.Records[j].Step })
			for i, record := range batch.Records {
				_, _, hash, err := auditModelRequest(submitted[i], nil, 0)
				if err != nil {
					t.Fatal(err)
				}
				if record.ContentSHA256 != hash {
					t.Fatalf("record[%d].ContentSHA256 = %s, want %s (hash of the request the streamer actually received): ledger and wire diverged", i, record.ContentSHA256, hash)
				}
			}
			// Positive check that this actually exercises a tool-call step
			// (would otherwise vacuously pass for a no-tool run).
			calls, results := functionCallAndResultBlocks(submitted[1].Messages)
			if len(calls) != 1 || calls[0] != "call_0" || len(results) != 1 || results[0] != "call_0" {
				t.Fatalf("submitted[1] call/result ids = %v/%v, want [call_0]/[call_0]", calls, results)
			}
			if tc.failover {
				// LC-S3: prove the failover attempt actually dispatched
				// through the failover model, not a resolver that silently
				// ignores the selection and reuses the primary's identity.
				if got := batch.Records[len(batch.Records)-1].ModelID; got != "failover-model" {
					t.Fatalf("last ledger row ModelID = %q, want failover-model", got)
				}
			}
		})
	}
}

// TestPrepareToolCallsTreatsInvalidOrOversizedProviderCallIDAsAbsent proves
// reconciliation item 5 (WC-S4): a provider id that is not valid UTF-8, or
// that exceeds session.DiscoveryMaxIdentityBytes, is never persisted as
// ProviderCallID -- the call is treated as if the provider had left CallID
// empty, so publicizeToolCallIDs sends the minted durable id instead.
func TestPrepareToolCallsTreatsInvalidOrOversizedProviderCallIDAsAbsent(t *testing.T) {
	store := newAdmissionStore()
	invalidUTF8 := "call-\xff\xfe"
	oversized := strings.Repeat("x", session.DiscoveryMaxIdentityBytes+1)
	dispatches := 0
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		dispatches++
		switch dispatches {
		case 1:
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall(invalidUTF8, "echo", `{}`))}, nil
		case 2:
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall(oversized, "echo", `{}`))}, nil
		default:
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "ok"}, nil }),
	}}})
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	store.mu.Lock()
	var calls []session.ToolCall
	for _, call := range store.toolCalls {
		calls = append(calls, call)
	}
	store.mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("tool calls = %d, want 2", len(calls))
	}
	for _, call := range calls {
		if call.ProviderCallID != "" {
			t.Fatalf("call %s ProviderCallID = %q, want empty (invalid/oversized provider id must be treated as absent)", call.ID, call.ProviderCallID)
		}
	}
}

// TestReplayAfterSQLiteRestartSendsProviderToolCallIDOnWire proves
// reconciliation item 2's restart-replay requirement: a follow-up run
// against a reopened SQLite store must show the provider its own id for a
// tool call completed by a prior process, not the durable id
// history.LoadAgentic's projection uses internally.
func TestReplayAfterSQLiteRestartSendsProviderToolCallIDOnWire(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "store.db")
	store, storePool, err := openTestSQLite(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = storePool.Close()
		}
	}()
	toolRegistry := staticToolRegistry{tools: []Tool{{
		Name: "echo", Retention: RetentionPolicy{MaxInlineBytes: 4096},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "echoed"}, nil }),
	}}}
	// ids is shared across both orchestrator instances (before and after
	// the reopen): a real IDGenerator must mint store-wide-unique ids
	// across process restarts (see IDGenerator's doc comment), which a
	// fresh *sequenceIDs{} per instance -- restarting its counter from
	// zero -- does not honor. Using two independent counters here would be
	// a test-harness collision, not evidence of a production bug.
	ids := &sequenceIDs{}
	orchestrator := mustConfiguredOrchestrator(
		WithStore(store),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
			last := request.Messages[len(request.Messages)-1]
			if last != nil && last.Role == einoschema.AgenticRoleTypeUser && isFunctionToolResultMessage(last) {
				return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
			}
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-restart", "echo", `{"text":"hi"}`))}, nil
		})}),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(toolRegistry)}),
		WithIDGenerator(ids),
		WithOwnerID("restart-owner"),
	)
	const sessionID session.ID = "restart-session"
	handle, err := orchestrator.Start(ctx, Request{SessionID: sessionID, Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatal(err)
	}
	if result := <-handle.Done(); result.Error != nil || result.Status != session.RunCompleted {
		t.Fatalf("first run result = %+v", result)
	}
	if err := storePool.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	reopened, reopenedPool, err := reopenTestSQLite(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopenedPool.Close() }()

	var mu sync.Mutex
	var secondRunRequests []model.Request
	reopenedOrchestrator := mustConfiguredOrchestrator(
		WithStore(reopened),
		WithModelResolver(resolvedModel{streamer: scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
			mu.Lock()
			secondRunRequests = append(secondRunRequests, request)
			mu.Unlock()
			return []*einoschema.AgenticMessage{agenticAssistantText("still here")}, nil
		})}),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(toolRegistry)}),
		WithIDGenerator(ids),
		WithOwnerID("restart-owner"),
	)
	handle2, err := reopenedOrchestrator.Start(ctx, Request{SessionID: sessionID, Message: TextUserMessage("again"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatal(err)
	}
	if result := <-handle2.Done(); result.Error != nil || result.Status != session.RunCompleted {
		t.Fatalf("second run result = %+v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(secondRunRequests) != 1 {
		t.Fatalf("second run dispatches = %d, want 1", len(secondRunRequests))
	}
	calls, results := functionCallAndResultBlocks(secondRunRequests[0].Messages)
	if len(calls) != 1 || calls[0] != "call-restart" || len(results) != 1 || results[0] != "call-restart" {
		t.Fatalf("replayed history call/result ids = %v/%v, want [call-restart]/[call-restart]", calls, results)
	}
}

// TestResumeRunAfterToolInterruptSendsProviderToolCallIDOnWire proves
// reconciliation item 2's resume requirement: the physical dispatch that
// follows a decided ResumeRun (after a tool interrupt/approval pause) must
// still show the provider its own id for the interrupted call's request and
// result blocks.
func TestResumeRunAfterToolInterruptSendsProviderToolCallIDOnWire(t *testing.T) {
	gate := Tool{
		Name: "gate", Info: &einoschema.ToolInfo{Name: "gate", Desc: "needs approval"},
		InterruptPolicy: pausingInterruptPolicy{},
		Executor: orchestratorToolExecutorFunc(func(_ context.Context, call ToolCall) (ToolResult, error) {
			return ToolResult{Output: "decision:" + call.ResumeDecision}, nil
		}),
	}
	var mu sync.Mutex
	var resumeRequest model.Request
	dispatches := 0
	orch, cleanup := newSQLiteTestOrchestrator(t, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		dispatches++
		if dispatches == 1 {
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-interrupt", "gate", `{}`))}, nil
		}
		mu.Lock()
		resumeRequest = request
		mu.Unlock()
		return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
	}))
	defer cleanup()
	configureTestTools(orch, staticToolRegistry{tools: []Tool{gate}})

	handle, err := orch.Start(context.Background(), Request{SessionID: "resume-interrupt-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunPaused || !result.Interrupted {
		t.Fatalf("result = %+v", result)
	}
	pause, ok := <-handle.AwaitPause()
	if !ok || len(pause.InterruptContexts) != 1 {
		t.Fatalf("pause = %+v, ok=%v", pause, ok)
	}
	resumeHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed result = %+v", resumed)
	}
	mu.Lock()
	defer mu.Unlock()
	wireCalls, wireResults := functionCallAndResultBlocks(resumeRequest.Messages)
	if len(wireCalls) != 1 || wireCalls[0] != "call-interrupt" || len(wireResults) != 1 || wireResults[0] != "call-interrupt" {
		t.Fatalf("resume continuation call/result ids = %v/%v, want [call-interrupt]/[call-interrupt]", wireCalls, wireResults)
	}
}

// TestApprovalContinuationSendsProviderToolCallIDOnWire proves
// reconciliation item 2's approval-continuation requirement: the dispatch
// following a decided MCP tool-approval pause must still show the provider
// its own id for an unrelated, already-completed tool call earlier in the
// same session's history.
func TestApprovalContinuationSendsProviderToolCallIDOnWire(t *testing.T) {
	store := newAdmissionStore()
	echo := Tool{
		Name: "echo", Info: &einoschema.ToolInfo{Name: "echo", Desc: "echo"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "hi"}, nil }),
	}
	var mu sync.Mutex
	var continuationRequest model.Request
	dispatches := 0
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		dispatches++
		switch dispatches {
		case 1:
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-approval-history", "echo", `{}`))}, nil
		case 2:
			return []*einoschema.AgenticMessage{{
				Role: einoschema.AgenticRoleTypeAssistant,
				ContentBlocks: []*einoschema.ContentBlock{
					{Type: einoschema.ContentBlockTypeMCPToolApprovalRequest, MCPToolApprovalRequest: &einoschema.MCPToolApprovalRequest{ID: "apr-1", Name: "remote_write", ServerLabel: "srv", Arguments: `{}`}},
				},
			}}, nil
		default:
			mu.Lock()
			continuationRequest = request
			mu.Unlock()
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	}))
	configureTestTools(orch, staticToolRegistry{tools: []Tool{echo}})

	handle, err := orch.Start(context.Background(), Request{SessionID: "approval-continuation-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunPaused || !result.Interrupted {
		t.Fatalf("result = %+v", result)
	}
	pause, ok := <-handle.AwaitPause()
	if !ok || len(pause.InterruptContexts) != 1 {
		t.Fatalf("pause = %+v, ok=%v", pause, ok)
	}
	resumeHandle, err := orch.ResumeRun(context.Background(), result.RunID, ResumeRequest{
		Targets: map[string]any{pause.InterruptContexts[0].ID: "approve"},
	})
	if err != nil {
		t.Fatalf("ResumeRun error = %v", err)
	}
	resumed := <-resumeHandle.Done()
	if resumed.Status != session.RunCompleted || resumed.Error != nil {
		t.Fatalf("resumed result = %+v", resumed)
	}
	mu.Lock()
	defer mu.Unlock()
	wireCalls, wireResults := functionCallAndResultBlocks(continuationRequest.Messages)
	if len(wireCalls) != 1 || wireCalls[0] != "call-approval-history" || len(wireResults) != 1 || wireResults[0] != "call-approval-history" {
		t.Fatalf("approval continuation call/result ids = %v/%v, want [call-approval-history]/[call-approval-history]", wireCalls, wireResults)
	}
}

// TestFailoverAttemptSendsProviderToolCallIDOnWire proves reconciliation
// item 2's failover requirement: when the primary model's continuation
// attempt fails and ADK fails over to an alternate model
// (buildFailoverConfig), the failover attempt's own physical dispatch must
// still show the provider its own id for the already-completed tool call.
func TestFailoverAttemptSendsProviderToolCallIDOnWire(t *testing.T) {
	store := newAdmissionStore()
	echo := Tool{
		Name: "echo", Info: &einoschema.ToolInfo{Name: "echo", Desc: "echo"},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "hi"}, nil }),
	}
	var mu sync.Mutex
	var failoverRequest model.Request
	dispatches := 0
	streamer := scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.AgenticMessage, error) {
		dispatches++
		switch dispatches {
		case 1:
			return []*einoschema.AgenticMessage{agenticAssistantToolCalls(agenticToolCall("call-failover", "echo", `{}`))}, nil
		case 2:
			// The primary model's continuation attempt fails -- triggers
			// ADK's failover wrapper (defaultShouldFailover refuses only
			// the graph-interrupt/already-committed sentinels).
			return nil, errors.New("primary model unavailable")
		default:
			mu.Lock()
			failoverRequest = request
			mu.Unlock()
			return []*einoschema.AgenticMessage{agenticAssistantText("done")}, nil
		}
	})
	plan := mustTestRunPlan(RunPlanSpec{
		Components: []PlanComponent{{Component: testPlanComponent("test-tools"), Tools: testPlanTools(staticToolRegistry{tools: []Tool{echo}})}},
		Failover:   &FailoverPolicy{Models: []model.Selection{{ProviderID: "fake", ModelID: "failover-model"}}},
	})
	orch := newTestOrchestrator(store, streamer, WithRunPlanProvider(staticRunPlanProvider{plan: plan}))

	result := startAndWaitRequest(t, orch, Request{SessionID: "failover-session", Message: TextUserMessage("hello"), Config: orchestratorConfig()})
	if result.Status != session.RunCompleted || result.Error != nil {
		t.Fatalf("result = %+v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	wireCalls, wireResults := functionCallAndResultBlocks(failoverRequest.Messages)
	if len(wireCalls) != 1 || wireCalls[0] != "call-failover" || len(wireResults) != 1 || wireResults[0] != "call-failover" {
		t.Fatalf("failover attempt call/result ids = %v/%v, want [call-failover]/[call-failover]", wireCalls, wireResults)
	}
}
