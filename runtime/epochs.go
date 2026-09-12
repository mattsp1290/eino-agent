package runtime

import (
	"context"
	"time"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/session/history"
)

// resolveTurnHistoryOptions resolves options.Epoch for one turn's admission:
// when the host has not pinned a static Epoch override, it looks up the
// most recently finished summarization epoch for sessionID (see
// latestFinishedSummarizationEpoch) and uses that instead, so the provider
// projection actually narrows once a summarization epoch commits, without
// requiring a host to configure Options.Epoch by hand. This is resolved
// exactly ONCE per turn admission -- the returned options are then used
// consistently for both that turn's baseMessageCount computation and every
// one of that turn's own ReAct-cycle reloads (adkEngine.buildDurableBaseline)
// -- rather than re-resolved per cycle: baseMessageCount's admission-time
// prefix and a later cycle's fresh reload must agree on the same epoch view
// for adkEngine.buildDurableBaseline's splice math to stay correct. A
// summarization epoch this turn's own recipe commits mid-turn therefore
// takes effect starting the NEXT turn admitted on this session, not later
// cycles of the same turn that created it -- a documented, bounded scoping
// choice, not an oversight.
func resolveTurnHistoryOptions(ctx context.Context, store session.Store, sessionID session.ID, options history.Options) (history.Options, error) {
	if options.Epoch != nil {
		return options, nil
	}
	resolved, err := latestFinishedSummarizationEpoch(ctx, store, sessionID)
	if err != nil {
		return history.Options{}, err
	}
	options.Epoch = resolved
	return options, nil
}

func admissionContextEpoch(request admissionRequest, sessionID session.ID, now time.Time) session.ContextEpoch {
	return session.ContextEpoch{
		ID:         request.IDs.ContextEpochID,
		SessionID:  sessionID,
		ModelID:    string(request.Model.Model.ID),
		ProviderID: string(request.Model.Provider.ID),
		Trigger:    "turn",
		Reason:     "run_admission",
		NextAction: session.EpochNextStop,
		CreatedAt:  now,
	}
}
