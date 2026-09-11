package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/compaction"
	"github.com/mattsp1290/eino-agent/session/history"
)

// contextEpochCapability is the narrow, unexported durable capability this
// package's OWN summarization recipe uses to correlate ADK's in-memory
// summary input with durable history and commit a compaction boundary. It
// is deliberately never reachable through an exported HandlerBuildContext
// field: an arbitrary host-registered HandlerFactory (of ANY Kind,
// including a host's own third-party middleware) receives the exact same
// HandlerBuildContext value this package's summarization recipe does, so no
// exported field on that type may carry durable claim/settle authority or
// checkpoint-byte access (session.Store, session.ExecutionStore) -- see
// HandlerBuildContext's doc comment and C1's fix in the W6 round-1 review.
//
// This capability grants exactly three things, and nothing else:
//  1. minting the identities a compaction boundary needs;
//  2. a read-only reload of this session's durable conversational history,
//     for Finalize's position correlation only (session.Store's read
//     surface still technically includes ReadPromotedCheckpoint, but that
//     is unreachable here: this type is unexported, so it can never be
//     read from outside this package regardless of what session.Store
//     itself exposes -- the isolation is at the Go visibility boundary,
//     the same mechanism authorizeRewrite already relies on);
//  3. atomically starting a new session.ContextEpoch and appending its
//     replayable summary boundary in ONE fenced transaction.
//
// It grants no ToolCall claim/settle authority and no arbitrary
// AppendMessage/AppendPart access outside that one atomic boundary-commit
// path.
type contextEpochCapability struct {
	sessionID     session.ID
	runID         session.RunID
	store         session.Store
	execution     session.ExecutionStore
	ids           IDGenerator
	now           func() time.Time
	contentLimits session.ContentLimits
}

// ready reports whether this capability was actually populated for this
// turn (buildAgentHandlers always constructs one, but a factory that is not
// this package's own summarization recipe should never depend on it, and
// summarization itself fails construction closed when its own durable
// dependencies are unavailable).
func (c contextEpochCapability) ready() bool {
	return c.store != nil && c.execution != nil && c.ids != nil && c.now != nil
}

// loadConversationalHistory reloads this session's full durable
// message/part history (NOT epoch-filtered), for Finalize's role/part-kind
// lookups by message ID only -- see summarizationFinalize.
func (c contextEpochCapability) loadConversationalHistory(ctx context.Context) (session.ReplayBatch, error) {
	if !c.ready() {
		return session.ReplayBatch{}, fmt.Errorf("%w: context epoch capability unavailable", errHandlerMissingBackend)
	}
	return history.LoadBatch(ctx, c.store, c.sessionID)
}

// activeEpoch returns the currently active summarization epoch for this
// session (see latestFinishedSummarizationEpoch), or nil if none.
func (c contextEpochCapability) activeEpoch(ctx context.Context) (*session.ContextEpoch, error) {
	if !c.ready() {
		return nil, fmt.Errorf("%w: context epoch capability unavailable", errHandlerMissingBackend)
	}
	return latestFinishedSummarizationEpoch(ctx, c.store, c.sessionID)
}

// loadCurrentAgenticProjection reloads this session's durable history
// projected through activeEpoch exactly the way ADK's own current turn
// input was built (see adkEngine.buildDurableBaseline), so its
// SourceMessageIDs correlates 1:1, in order, with what ADK's Finalize hands
// back as originalMessages -- including when a PRIOR summarization epoch is
// already active (a second summarization on the same session must
// correlate against the ALREADY-COMPACTED view, not raw unfiltered
// history, or the correlation check always mismatches once more than one
// epoch has ever been created for a session).
func (c contextEpochCapability) loadCurrentAgenticProjection(ctx context.Context, activeEpoch *session.ContextEpoch) (history.AgenticProjection, error) {
	if !c.ready() {
		return history.AgenticProjection{}, fmt.Errorf("%w: context epoch capability unavailable", errHandlerMissingBackend)
	}
	return history.LoadAgentic(ctx, c.store, c.sessionID, history.Options{Epoch: activeEpoch, ContentLimits: c.contentLimits})
}

// commitSummaryEpoch durably starts epoch and appends its replayable
// summary boundary in ONE fenced transaction: StartContextEpoch and the
// boundary append (AppendMessage + AppendPart + FinishContextEpoch) commit
// together, so a mid-sequence failure can never leave a started epoch row
// with no SummaryMessageID (see compaction.AppendBoundaryTx).
func (c contextEpochCapability) commitSummaryEpoch(ctx context.Context, epoch session.ContextEpoch, ids compaction.BoundaryIDs, summaryText string) (session.ContextEpoch, compaction.Boundary, error) {
	if !c.ready() {
		return session.ContextEpoch{}, compaction.Boundary{}, fmt.Errorf("%w: context epoch capability unavailable", errHandlerMissingBackend)
	}
	var started session.ContextEpoch
	var boundary compaction.Boundary
	err := c.execution.WithinTx(ctx, func(ctx context.Context, tx session.ExecutionStore) error {
		var err error
		started, err = tx.StartContextEpoch(ctx, epoch)
		if err != nil {
			return err
		}
		boundary, err = compaction.AppendBoundaryTx(ctx, tx, started, ids, c.runID, c.now(), summaryText)
		return err
	})
	if err != nil {
		return session.ContextEpoch{}, compaction.Boundary{}, err
	}
	return started, boundary, nil
}

// latestFinishedSummarizationEpoch returns the most recently created,
// finished (SummaryMessageID set) summarization-triggered epoch for
// sessionID, or nil if the store does not expose session.ContextEpochReader
// or none exists -- a bounded, best-effort lookup: a store that cannot list
// epochs degrades to "no active epoch" rather than failing every turn.
// buildDurableBaseline consults this on every cycle so the provider
// projection actually narrows once a summarization epoch commits, without
// requiring a host to configure a static Options.Epoch override.
func latestFinishedSummarizationEpoch(ctx context.Context, store session.Store, sessionID session.ID) (*session.ContextEpoch, error) {
	reader, ok := store.(session.ContextEpochReader)
	if !ok {
		return nil, nil
	}
	epochs, err := reader.ListContextEpochs(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	var latest *session.ContextEpoch
	for i := range epochs {
		e := epochs[i]
		if e.Trigger != "summarization" || e.SummaryMessageID == "" {
			continue
		}
		if latest == nil || e.CreatedAt.After(latest.CreatedAt) {
			cp := e
			latest = &cp
		}
	}
	return latest, nil
}
