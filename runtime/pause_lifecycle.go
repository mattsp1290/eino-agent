package runtime

import (
	"encoding/json"
	"fmt"

	"github.com/cloudwego/eino/adk"

	"github.com/mattsp1290/eino-agent/session"
)

// pauseLifecyclePayload builds the immutable, redacted payload stored in the
// same transaction as the pause event. It snapshots only target identity and
// address, never adk.InterruptCtx.Info or a host's resume decision.
func pauseLifecyclePayload(event session.EventRecord, revision int64, agentPath string, targets []*adk.InterruptCtx) (json.RawMessage, error) {
	if agentPath == "" {
		agentPath = "root"
	}
	value := session.PauseLifecycleV1{
		Version: session.PauseLifecycleVersion, PauseID: string(event.ID), CheckpointRevision: revision,
		Generation: revision, AgentPath: agentPath, MessageID: session.MessageID("pause:" + string(event.RunID)),
		AttemptID: "pause:" + string(event.RunID), EventRevision: string(event.ID),
	}
	for _, target := range targets {
		if target == nil {
			return nil, fmt.Errorf("nil pause interrupt target")
		}
		value.Targets = append(value.Targets, session.PauseInterruptTarget{ID: target.ID, Address: target.Address.String()})
	}
	if err := session.ValidatePauseLifecycle(session.RunPausedEventKind, value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}
