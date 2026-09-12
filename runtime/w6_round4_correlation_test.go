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
