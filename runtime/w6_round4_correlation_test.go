package runtime

import (
	"reflect"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

// TestRepositionMidTurnCompactionBoundaryMovesBeforeSplitGroup is round-four
// W6 correlation-and-seal review Important #4 (eino-agent-0wb's true root
// cause) proven directly against repositionMidTurnCompactionBoundary: given
// a durable reload where a compaction boundary durably landed between a
// function_tool_call and its own function_tool_result -- exactly what
// happens when summarization fires on a turn's own first cycle, since that
// cycle's assistant placeholder row is reserved at admission (an early
// durable position) while its eventual content and result both commit
// later, straddling anything Finalize commits in between -- the fix must
// move the boundary to sit immediately BEFORE the call, never leaving it
// (or moving it) anywhere that still separates the two.
//
// Mutation check performed by hand while authoring this test (not automated
// here, since the fix lives in a pure function with no feature flag): with
// repositionMidTurnCompactionBoundary's body replaced by `return full,
// fullSourceIDs, fullState` unconditionally (i.e. reverting the fix to a
// no-op), this test fails with the boundary still reported at its original
// split position; restoring the real body makes it pass again.
func TestRepositionMidTurnCompactionBoundaryMovesBeforeSplitGroup(t *testing.T) {
	const (
		userMsgID     session.MessageID = "m-user"
		callMsgID     session.MessageID = "m-call"
		boundaryMsgID session.MessageID = "m-boundary"
		resultMsgID   session.MessageID = "m-result"
	)
	full := []*einoschema.AgenticMessage{
		agenticUserText("hi"),
		toolCallBlockMessage("call-A", "ping", `{}`),
		agenticSystemText("compaction summary text"),
		toolResultBlockMessage("call-A", "ping"),
	}
	fullSourceIDs := []session.MessageID{userMsgID, callMsgID, boundaryMsgID, resultMsgID}
	fullState := []model.ProviderMessageState{{MessageIndex: 1, MessageID: string(callMsgID)}}

	gotMessages, gotSourceIDs, gotState := repositionMidTurnCompactionBoundary(full, fullSourceIDs, fullState, boundaryMsgID)

	wantSourceIDs := []session.MessageID{userMsgID, boundaryMsgID, callMsgID, resultMsgID}
	if !reflect.DeepEqual(gotSourceIDs, wantSourceIDs) {
		t.Fatalf("sourceIDs after reposition = %v, want %v (boundary moved before the call)", gotSourceIDs, wantSourceIDs)
	}
	// The messages themselves must have moved in lockstep with their ids.
	if gotMessages[1] != full[2] {
		t.Fatalf("messages[1] after reposition = %+v, want the boundary message (full[2])", gotMessages[1])
	}
	if gotMessages[2] != full[1] {
		t.Fatalf("messages[2] after reposition = %+v, want the call message (full[1])", gotMessages[2])
	}
	// fullState's MessageIndex must be remapped to the call's NEW position
	// (2, since the boundary now sits at 1) -- a stale index would make a
	// later dispatch attribute captured provider state to the wrong message.
	if len(gotState) != 1 || gotState[0].MessageIndex != 2 {
		t.Fatalf("provider state after reposition = %+v, want MessageIndex remapped to 2", gotState)
	}

	// The boundary must genuinely no longer split the group: nothing
	// between the call and its result may be anything but the call's own
	// result.
	callIdx, resultIdx := -1, -1
	for i, id := range gotSourceIDs {
		if id == callMsgID {
			callIdx = i
		}
		if id == resultMsgID {
			resultIdx = i
		}
	}
	if resultIdx != callIdx+1 {
		t.Fatalf("call at %d, result at %d -- not adjacent after reposition (sourceIDs=%v)", callIdx, resultIdx, gotSourceIDs)
	}
}

// TestRepositionMidTurnCompactionBoundaryNoOpWhenAlreadySafe proves the
// function never moves a boundary that does not split any group -- the
// common case on every cycle before summarization has fired, and every
// cycle after it fires when the trigger happened before any call in this
// turn (e.g. right at turn admission, before that cycle's own dispatch).
func TestRepositionMidTurnCompactionBoundaryNoOpWhenAlreadySafe(t *testing.T) {
	const (
		userMsgID     session.MessageID = "m-user"
		boundaryMsgID session.MessageID = "m-boundary"
		callMsgID     session.MessageID = "m-call"
		resultMsgID   session.MessageID = "m-result"
	)
	full := []*einoschema.AgenticMessage{
		agenticUserText("hi"),
		agenticSystemText("compaction summary text"),
		toolCallBlockMessage("call-A", "ping", `{}`),
		toolResultBlockMessage("call-A", "ping"),
	}
	fullSourceIDs := []session.MessageID{userMsgID, boundaryMsgID, callMsgID, resultMsgID}

	gotMessages, gotSourceIDs, _ := repositionMidTurnCompactionBoundary(full, fullSourceIDs, nil, boundaryMsgID)
	if !reflect.DeepEqual(gotSourceIDs, fullSourceIDs) {
		t.Fatalf("sourceIDs = %v, want unchanged %v (boundary already safe)", gotSourceIDs, fullSourceIDs)
	}
	for i := range full {
		if gotMessages[i] != full[i] {
			t.Fatalf("messages[%d] changed despite an already-safe boundary", i)
		}
	}
}

// TestRepositionMidTurnCompactionBoundaryHandlesToolSearchResult proves the
// same fix for a call answered by a tool_search_result block (toolsearch's
// own dynamictool discovery call), a distinct content block type from an
// ordinary function_tool_result -- moveTailStartToGroupBoundary checks the
// same two durable session.PartKind values for the identical reason.
// Without this, the composed example's own patchtoolcalls + summarization
// scenario (examples/agentic-middleware/composed_test.go) still failed: an
// EARLIER, still-open tool_search call was mistaken for already closed,
// leaving open empty and skipping the reposition entirely.
func TestRepositionMidTurnCompactionBoundaryHandlesToolSearchResult(t *testing.T) {
	const (
		userMsgID     session.MessageID = "m-user"
		callMsgID     session.MessageID = "m-call"
		boundaryMsgID session.MessageID = "m-boundary"
		resultMsgID   session.MessageID = "m-result"
	)
	full := []*einoschema.AgenticMessage{
		agenticUserText("hi"),
		toolCallBlockMessage("call-search", "tool_search", `{}`),
		agenticSystemText("compaction summary text"),
		toolSearchResultBlockMessage("call-search", "hidden_capability"),
	}
	fullSourceIDs := []session.MessageID{userMsgID, callMsgID, boundaryMsgID, resultMsgID}

	_, gotSourceIDs, _ := repositionMidTurnCompactionBoundary(full, fullSourceIDs, nil, boundaryMsgID)
	wantSourceIDs := []session.MessageID{userMsgID, boundaryMsgID, callMsgID, resultMsgID}
	if !reflect.DeepEqual(gotSourceIDs, wantSourceIDs) {
		t.Fatalf("sourceIDs after reposition = %v, want %v (boundary moved before the tool_search call)", gotSourceIDs, wantSourceIDs)
	}
}

// TestCorrelateDurableSubsequenceFailsClosedOnGenuinelyAbsentMessage is
// round-four W6 correlation-and-seal review Important #3: the fail-closed
// "unresolved > 0" check had no regression test at all -- neutralizing it
// survived the full runtime and examples/agentic-middleware suites. This
// constructs the exact shape the check exists to catch: a durable baseline
// message (id1) that this cycle's projected messages do not contain under
// ANY resolvable identity (no pointer match, no content match) -- simulating
// an earlier handler that dropped or unrecognizably rewrote it -- and
// asserts correlateDurableSubsequence refuses to proceed rather than
// silently treating the cycle as fully accounted for.
//
// Mutation check performed by hand: with the `if unresolved > 0 { return
// nil, nil, err }` guard in correlateDurableSubsequence commented out, this
// test fails (no error, and only 1 of the 2 real baseline messages present
// in durableOriginalIndices) instead of failing closed; restoring the guard
// passes it again.
func TestCorrelateDurableSubsequenceFailsClosedOnGenuinelyAbsentMessage(t *testing.T) {
	m0 := agenticUserText("m0 content")
	m1 := agenticUserText("m1 content")
	baselineMsgs := []*einoschema.AgenticMessage{m0, m1}
	baselineIDs := []session.MessageID{"id0", "id1"}
	sourceMessageID := func(msg *einoschema.AgenticMessage) (session.MessageID, bool) {
		if msg == m0 {
			return "id0", true
		}
		return "", false
	}
	// m1 is genuinely absent from this cycle's own projection -- not merely
	// under a new pointer (content differs too, so the fallback cannot
	// mistake anything here for it).
	originalMessages := []*einoschema.AgenticMessage{m0}

	_, _, err := correlateDurableSubsequence(originalMessages, sourceMessageID, baselineMsgs, baselineIDs)
	if err == nil {
		t.Fatal("correlateDurableSubsequence succeeded despite baseline message id1 having no resolvable identity in this cycle -- want a fail-closed error")
	}
}

// TestCorrelateDurableSubsequenceTwoPassRejectsCoincidentalDuplicate is
// round-four W6 correlation-and-seal review Important #1: the fallback must
// not let an ephemeral (non-durable) message that coincidentally matches a
// baseline entry's content steal that entry's slot before the REAL durable
// message (present later in originalMessages, resolvable by pointer) claims
// it. ephemeral has the SAME content as m0 but no durable id at all --
// exactly the shape of an agentsmd/skill-injected message that happens to
// echo real conversational text -- and appears BEFORE the real m0 in
// originalMessages, the position that defeated the old single, interleaved
// pass (it would greedily match ephemeral against baselineMsgs[0] before
// the real m0 got a chance to claim id0 by pointer, producing id0 claimed
// by TWO different messages with no error at all).
//
// Mutation check performed by hand: reverting correlateDurableSubsequence
// to a single interleaved pass (resolve-or-fallback per message, in
// original order, instead of two strict passes) makes this test fail:
// sourceIDs ends up ["id0", "id0", "id1"] (id0 claimed twice, ephemeral
// wrongly resolved) instead of ephemeral being correctly left unresolved.
func TestCorrelateDurableSubsequenceTwoPassRejectsCoincidentalDuplicate(t *testing.T) {
	m0 := agenticUserText("m0 content")
	m1 := agenticUserText("m1 content")
	ephemeral := agenticUserText("m0 content") // distinct pointer, coincidentally identical content to m0
	baselineMsgs := []*einoschema.AgenticMessage{m0, m1}
	baselineIDs := []session.MessageID{"id0", "id1"}
	sourceMessageID := func(msg *einoschema.AgenticMessage) (session.MessageID, bool) {
		switch msg {
		case m0:
			return "id0", true
		case m1:
			return "id1", true
		default:
			return "", false
		}
	}
	originalMessages := []*einoschema.AgenticMessage{ephemeral, m0, m1}

	sourceIDs, durableOriginalIndices, err := correlateDurableSubsequence(originalMessages, sourceMessageID, baselineMsgs, baselineIDs)
	if err != nil {
		t.Fatalf("correlateDurableSubsequence error = %v, want success (both real baseline messages ARE present, just alongside a coincidental ephemeral duplicate)", err)
	}
	if sourceIDs[0] != "" {
		t.Fatalf("ephemeral (index 0) resolved to durable id %q, want unresolved (\"\") -- it must never steal a real baseline slot", sourceIDs[0])
	}
	if sourceIDs[1] != "id0" || sourceIDs[2] != "id1" {
		t.Fatalf("real messages resolved to sourceIDs=%v, want [_, id0, id1]", sourceIDs)
	}
	if !reflect.DeepEqual(durableOriginalIndices, []int{1, 2}) {
		t.Fatalf("durableOriginalIndices = %v, want [1 2] (only the two REAL durable messages, ephemeral excluded)", durableOriginalIndices)
	}
	// No durable id may be claimed by more than one entry.
	seen := map[session.MessageID]int{}
	for _, idx := range durableOriginalIndices {
		seen[sourceIDs[idx]]++
	}
	for id, count := range seen {
		if count > 1 {
			t.Fatalf("durable id %q claimed by %d different messages, want at most 1", id, count)
		}
	}
}
