package runtime

import (
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func admissionContextEpoch(request AdmissionRequest, sessionID session.ID, now time.Time) session.ContextEpoch {
	return session.ContextEpoch{
		ID:         request.IDs.ContextEpochID,
		SessionID:  sessionID,
		ModelID:    admissionModelID(request),
		ProviderID: admissionProviderID(request),
		Trigger:    "turn",
		Reason:     "run_admission",
		NextAction: session.EpochNextStop,
		CreatedAt:  now,
	}
}
