package compaction

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

// BoundaryIDs are caller-owned durable IDs for one compaction summary boundary.
type BoundaryIDs struct {
	MessageID session.MessageID
	PartID    session.PartID
	// TurnID stamps the boundary message with the durable turn that
	// triggered compaction, mirroring session.Message.TurnID. Without it,
	// the boundary message carries an empty TurnID forever, which forces
	// agui's identity projection (agui.agenticIdentity) to fall back to a
	// synthetic turn id on every replay of every compacted session (W7
	// review finding). Optional: a caller that leaves it empty gets that
	// same synthetic-fallback behavior, just like any other unstamped
	// message.
	TurnID session.TurnID
	// AgentPath mirrors session.Message.AgentPath for the same reason.
	AgentPath string
}

// SummaryPayload is stored in a PartCompaction record and replayed as text.
type SummaryPayload struct {
	Text             string            `json:"text"`
	EpochID          session.EpochID   `json:"epoch_id"`
	SummarizedFromID session.MessageID `json:"summarized_from_id,omitempty"`
	SummarizedToID   session.MessageID `json:"summarized_to_id,omitempty"`
	TailStartID      session.MessageID `json:"tail_start_id,omitempty"`
	Redacted         bool              `json:"redacted"`
}

// Boundary is the replayable message/part pair for a compaction summary.
type Boundary struct {
	Message session.Message
	Part    session.Part
}

// NewBoundary builds the replayable records for a compaction summary. The raw
// prompts covered by the epoch are identified by IDs only; their content is not
// copied into the boundary payload.
func NewBoundary(epoch session.ContextEpoch, ids BoundaryIDs, runID session.RunID, now time.Time, summary string) (Boundary, error) {
	if epoch.ID == "" {
		return Boundary{}, fmt.Errorf("epoch id required")
	}
	if epoch.SessionID == "" {
		return Boundary{}, fmt.Errorf("session id required")
	}
	if ids.MessageID == "" {
		return Boundary{}, fmt.Errorf("message id required")
	}
	if ids.PartID == "" {
		return Boundary{}, fmt.Errorf("part id required")
	}
	payload, err := json.Marshal(SummaryPayload{
		Text:             summary,
		EpochID:          epoch.ID,
		SummarizedFromID: epoch.SummarizedFromID,
		SummarizedToID:   epoch.SummarizedToID,
		TailStartID:      epoch.TailStartID,
		Redacted:         true,
	})
	if err != nil {
		return Boundary{}, err
	}
	return Boundary{
		Message: session.Message{
			ID:        ids.MessageID,
			SessionID: epoch.SessionID,
			RunID:     runID,
			Role:      session.RoleSystem,
			TurnID:    ids.TurnID,
			AgentPath: ids.AgentPath,
			CreatedAt: now,
			UpdatedAt: now,
		},
		Part: session.Part{
			ID:        ids.PartID,
			MessageID: ids.MessageID,
			SessionID: epoch.SessionID,
			RunID:     runID,
			Kind:      session.PartCompaction,
			Ordinal:   0,
			Payload:   payload,
			CreatedAt: now,
			UpdatedAt: now,
		},
	}, nil
}

// AppendBoundary appends the replayable summary message and compaction part
// in its own transaction.
func AppendBoundary(ctx context.Context, store session.ExecutionStore, epoch session.ContextEpoch, ids BoundaryIDs, runID session.RunID, now time.Time, summary string) (Boundary, error) {
	if store == nil {
		return Boundary{}, fmt.Errorf("store required")
	}
	var boundary Boundary
	err := store.WithinTx(ctx, func(ctx context.Context, tx session.ExecutionStore) error {
		var err error
		boundary, err = AppendBoundaryTx(ctx, tx, epoch, ids, runID, now, summary)
		return err
	})
	return boundary, err
}

// AppendBoundaryTx is AppendBoundary without its own transaction: store is
// expected to already be inside a transaction the caller controls (for
// example one branch of a larger session.ExecutionStore.WithinTx that also
// calls StartContextEpoch), so the whole epoch swap -- start, boundary
// append, finish -- commits atomically as a single unit instead of as two
// separate writes.
func AppendBoundaryTx(ctx context.Context, store session.ExecutionStore, epoch session.ContextEpoch, ids BoundaryIDs, runID session.RunID, now time.Time, summary string) (Boundary, error) {
	if store == nil {
		return Boundary{}, fmt.Errorf("store required")
	}
	return appendBoundaryRecords(ctx, store, epoch, ids, runID, now, summary)
}

func appendBoundaryRecords(ctx context.Context, store session.ExecutionStore, epoch session.ContextEpoch, ids BoundaryIDs, runID session.RunID, now time.Time, summary string) (Boundary, error) {
	boundary, err := NewBoundary(epoch, ids, runID, now, summary)
	if err != nil {
		return Boundary{}, err
	}
	message, err := store.AppendMessage(ctx, boundary.Message)
	if err != nil {
		return Boundary{}, err
	}
	part, err := store.AppendPart(ctx, boundary.Part)
	if err != nil {
		return Boundary{}, err
	}
	epoch.SummaryMessageID = message.ID
	if epoch.ClosedAt.IsZero() {
		epoch.ClosedAt = now
	}
	if err := store.FinishContextEpoch(ctx, epoch); err != nil {
		return Boundary{}, err
	}
	boundary.Message = message
	boundary.Part = part
	return boundary, nil
}
