package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

// TestResolveTurnHistoryOptionsReadsBackTheCommittedEpoch is round-two W6
// review item 11's epoch-read-back protection proof, as a direct,
// mutation-provable unit test of resolveTurnHistoryOptions itself (the
// live multi-turn integration test's own narrowing assertions are
// satisfied by summarization's in-memory Finalize effect on the SAME
// cycle that triggers it, so they do not, on their own, prove a LATER
// turn's fresh admission actually reads the durably committed epoch back
// -- this test isolates exactly that mechanism).
func TestResolveTurnHistoryOptionsReadsBackTheCommittedEpoch(t *testing.T) {
	store := newAdmissionStore()
	sessionID := session.ID("read-back-session")
	execution := testFencedExecutionStore(t, store, sessionID)
	now := time.Unix(4000, 0).UTC()

	epoch := session.ContextEpoch{
		ID: "epoch-committed", SessionID: sessionID, SummarizedFromID: "m1", SummarizedToID: "m2",
		Trigger: "summarization", Reason: "context_budget", NextAction: session.EpochNextAutoContinue, CreatedAt: now,
	}
	if _, err := execution.StartContextEpoch(context.Background(), epoch); err != nil {
		t.Fatal(err)
	}
	epoch.SummaryMessageID = "summary-message"
	epoch.ClosedAt = now
	if err := execution.FinishContextEpoch(context.Background(), epoch); err != nil {
		t.Fatal(err)
	}

	resolved, err := resolveTurnHistoryOptions(context.Background(), store, sessionID, history.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Epoch == nil {
		t.Fatal("resolveTurnHistoryOptions did not read back the durably committed epoch at all")
	}
	if resolved.Epoch.ID != epoch.ID {
		t.Fatalf("resolved.Epoch.ID = %v, want %v", resolved.Epoch.ID, epoch.ID)
	}

	// A host-pinned static Options.Epoch always wins outright -- the
	// read-back only fills in when the caller left it unset.
	pinned := history.Options{Epoch: &session.ContextEpoch{ID: "host-pinned"}}
	resolvedPinned, err := resolveTurnHistoryOptions(context.Background(), store, sessionID, pinned)
	if err != nil {
		t.Fatal(err)
	}
	if resolvedPinned.Epoch == nil || resolvedPinned.Epoch.ID != "host-pinned" {
		t.Fatalf("resolvedPinned.Epoch = %+v, want the host-pinned epoch preserved untouched", resolvedPinned.Epoch)
	}
}
