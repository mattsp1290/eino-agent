package runtime

import (
	"context"

	"github.com/mattsp1290/eino-agent/session"
)

// messageCommittedPayload is the Payload shape for
// session.MessageCommittedEventKind. EventRecord.MessageID already carries
// the committed message's id; Revision is this session's observation
// watermark at the moment of commit (see session.ObservationReader), so a
// watcher can tell a stale notification apart from the current one.
type messageCommittedPayload struct {
	Revision int64 `json:"revision"`
}

// publishMessageCommitted durably records and publishes one
// message_committed notification for messageID, immediately after its
// content committed (persistAssistantTurn) or a tool call settled
// (persistToolSettlement). It is deliberately best-effort: messageID's
// content is already committed regardless of whether this notification is
// ever observed (a watcher can always fall back to polling
// ReadObservationRevision or a later replay), so a failure to append or
// publish this event must never be treated as though the commit itself
// failed.
func (o *StreamingOrchestrator) publishMessageCommitted(ctx context.Context, execution *runExecution, sessionID session.ID, runID session.RunID, messageID session.MessageID, epochID session.EpochID, turnID session.TurnID, agentPath string) {
	if o == nil || execution == nil || messageID == "" {
		return
	}
	var revision int64
	if reader, ok := o.store.(session.ObservationReader); ok {
		if watermark, err := reader.ReadObservationRevision(ctx, sessionID); err == nil {
			revision = watermark.Revision
		}
	}
	event := session.EventRecord{
		ID: o.ids.NewEventID(), SessionID: sessionID, RunID: runID, MessageID: messageID,
		EpochID: epochID, TurnID: turnID, AgentPath: agentPath, Kind: session.MessageCommittedEventKind,
		Payload: mustJSON(messageCommittedPayload{Revision: revision}), CreatedAt: o.now(),
	}
	committed, err := execution.store.AppendEvent(ctx, event)
	if err != nil {
		return
	}
	execution.publishPersisted(ctx, committed)
}
