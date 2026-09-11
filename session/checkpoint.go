package session

import (
	"errors"
	"time"
)

// CheckpointKind classifies which durable engine wrote a checkpoint.
type CheckpointKind string

const (
	// CheckpointKindRunner marks a checkpoint written by the ADK runner.
	CheckpointKindRunner CheckpointKind = "runner"
	// CheckpointKindLoop marks a checkpoint written between turns by the
	// TurnLoop (a "queued continuation" pause with no runner state).
	CheckpointKindLoop CheckpointKind = "loop"
)

// DefaultMaxCheckpointBytes bounds one checkpoint's Bytes when a caller does
// not configure a narrower limit.
const DefaultMaxCheckpointBytes = 16 << 20

// ErrCheckpointTooLarge reports that a checkpoint's Bytes exceeds the
// configured (or default) MaxCheckpointBytes.
var ErrCheckpointTooLarge = errors.New("session checkpoint exceeds configured limit")

// Checkpoint is one durable, versioned engine-state snapshot for a run.
// Revisions are staged unpromoted and only become the durable pause boundary
// once PromotePause marks one promoted.
type Checkpoint struct {
	RunID            RunID
	Revision         int64
	Kind             CheckpointKind
	AgentFingerprint string
	EinoVersion      string
	CodecVersion     int
	CheckpointID     string
	Bytes            []byte
	Promoted         bool
	CreatedAt        time.Time
}

// StageCheckpointRequest stages one unpromoted checkpoint revision under the
// fence. MaxBytes overrides DefaultMaxCheckpointBytes when positive.
type StageCheckpointRequest struct {
	Checkpoint Checkpoint
	MaxBytes   int
}

// PromotePauseRequest atomically promotes a staged checkpoint revision into
// the durable pause boundary.
type PromotePauseRequest struct {
	Revision int64
	TurnID   TurnID
	InboxIDs []InboxID
	Event    EventRecord
}

// PromotePauseResult is the canonical run/turn/checkpoint/event set committed
// by promotion.
type PromotePauseResult struct {
	Run        Run
	Turn       Turn
	Checkpoint Checkpoint
	Event      EventRecord
}

// effectiveMaxCheckpointBytes resolves the configured bound, defaulting to
// DefaultMaxCheckpointBytes and requiring a positive override when given.
func effectiveMaxCheckpointBytes(maxBytes int) (int, error) {
	if maxBytes == 0 {
		return DefaultMaxCheckpointBytes, nil
	}
	if maxBytes < 0 {
		return 0, ErrConflict
	}
	return maxBytes, nil
}

// ValidateStageCheckpoint checks the caller-owned shape of a staging request
// against the currently fenced run and resolves the effective byte bound.
func ValidateStageCheckpoint(run Run, request StageCheckpointRequest) (int, error) {
	maxBytes, err := effectiveMaxCheckpointBytes(request.MaxBytes)
	if err != nil {
		return 0, err
	}
	c := request.Checkpoint
	if c.RunID != run.ID || c.Revision <= 0 || c.CheckpointID == "" || c.EinoVersion == "" ||
		c.AgentFingerprint == "" || (c.Kind != CheckpointKindRunner && c.Kind != CheckpointKindLoop) ||
		c.CodecVersion <= 0 || c.Promoted || c.CreatedAt.IsZero() {
		return 0, ErrConflict
	}
	if len(c.Bytes) > maxBytes {
		return 0, ErrCheckpointTooLarge
	}
	return maxBytes, nil
}

// ValidatePromotePause checks the caller-owned shape of a promotion request
// against the currently fenced run before any store mutation.
func ValidatePromotePause(run Run, request PromotePauseRequest) error {
	if request.Revision <= 0 || request.Event.ID == "" || request.Event.Kind != RunPausedEventKind ||
		request.Event.RunID != run.ID || request.Event.SessionID != run.SessionID {
		return ErrConflict
	}
	return nil
}
