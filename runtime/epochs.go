package runtime

import (
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

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
