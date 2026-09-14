package session

import (
	"errors"
	"fmt"
)

// PauseLifecycleV1 is the immutable, redacted lifecycle fact carried by a
// run_paused or run_resumed EventRecord. It is deliberately record payload
// data rather than checkpoint state: a checkpoint is engine-private and may
// be replaced, while this fact remains the stable AG-UI/replay identity.
//
// A paused fact has PauseID, checkpoint and targets. A resumed fact references
// that PauseID and names the real successor turn/attempt only once redrive has
// admitted them. Decision values are never included.
type PauseLifecycleV1 struct {
	Version            int                    `json:"version"`
	PauseID            string                 `json:"pause_id"`
	CheckpointRevision int64                  `json:"checkpoint_revision"`
	Generation         int64                  `json:"generation"`
	Targets            []PauseInterruptTarget `json:"targets,omitempty"`
	Approval           *PauseApprovalLink     `json:"approval,omitempty"`
	AgentPath          string                 `json:"agent_path"`
	MessageID          MessageID              `json:"message_id"`
	AttemptID          string                 `json:"attempt_id"`
	EventRevision      string                 `json:"event_revision"`
	ResumedPauseID     string                 `json:"resumed_pause_id,omitempty"`
	ResumedTargetIDs   []string               `json:"resumed_target_ids,omitempty"`
	ResumeMode         string                 `json:"resume_mode,omitempty"`
	NewTurnID          TurnID                 `json:"new_turn_id,omitempty"`
	NewAttemptID       string                 `json:"new_attempt_id,omitempty"`
	ResumePhase        string                 `json:"resume_phase,omitempty"`
}

type PauseInterruptTarget struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type PauseApprovalLink struct {
	RequestID string `json:"request_id"`
	TargetID  string `json:"target_id"`
	Address   string `json:"address"`
}

const (
	PauseLifecycleVersion = 1
	ResumeModeFull        = "full"
	ResumeModeTargeted    = "targeted"
	ResumePhaseFact       = "post_redrive"
)

// ValidatePauseLifecycle validates the public, non-sensitive lifecycle
// identity before it is atomically stored with its owning event.
func ValidatePauseLifecycle(kind string, value PauseLifecycleV1) error {
	if value.Version != PauseLifecycleVersion || value.PauseID == "" || value.EventRevision == "" ||
		value.AgentPath == "" || value.MessageID == "" || value.AttemptID == "" || value.Generation <= 0 {
		return errors.New("invalid pause lifecycle identity")
	}
	targets := make(map[string]string, len(value.Targets))
	for _, target := range value.Targets {
		if target.ID == "" || target.Address == "" {
			return errors.New("pause lifecycle target requires id and address")
		}
		if _, duplicate := targets[target.ID]; duplicate {
			return fmt.Errorf("duplicate pause lifecycle target %q", target.ID)
		}
		targets[target.ID] = target.Address
	}
	if value.Approval != nil {
		if value.Approval.RequestID == "" || value.Approval.TargetID == "" || value.Approval.Address == "" || targets[value.Approval.TargetID] != value.Approval.Address {
			return errors.New("invalid pause lifecycle approval link")
		}
	}
	switch kind {
	case RunPausedEventKind:
		// Operational pauses (for example a stop/recovery boundary) have no
		// interactive target. Approval pauses must have one through the
		// validated Approval link above.
		if value.CheckpointRevision <= 0 || value.ResumedPauseID != "" {
			return errors.New("invalid paused lifecycle fact")
		}
	case RunResumedEventKind:
		if value.ResumedPauseID == "" || (value.ResumeMode != ResumeModeFull && value.ResumeMode != ResumeModeTargeted) ||
			value.ResumePhase != ResumePhaseFact || value.NewTurnID == "" || value.NewAttemptID == "" {
			return errors.New("invalid resumed lifecycle fact")
		}
		for _, id := range value.ResumedTargetIDs {
			if id == "" {
				return errors.New("empty resumed target id")
			}
		}
	default:
		return fmt.Errorf("pause lifecycle unsupported event kind %q", kind)
	}
	return nil
}
