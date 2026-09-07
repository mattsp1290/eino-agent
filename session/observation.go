package session

import (
	"context"
	"errors"
	"math"
	"time"
)

// ObservationWatermark orders committed state within one store and session.
// It is not a history cursor or a live-text resume token.
type ObservationWatermark struct {
	StoreID   string
	SessionID ID
	Revision  int64
}

// ObservationLimits bounds selected records and a conservative encoded byte
// budget (256 bytes per record plus six times each string's UTF-8 length).
// MaxTextBytes is cumulative across all included display text.
type ObservationLimits struct {
	MaxMessages, MaxTools, MaxParts int
	MaxSnapshotBytes, MaxTextBytes  int
}

func (l ObservationLimits) Validate() error {
	for _, n := range []int{l.MaxMessages, l.MaxTools, l.MaxParts, l.MaxSnapshotBytes, l.MaxTextBytes} {
		if n <= 0 || n > math.MaxInt/16 {
			return ErrObservationLimits
		}
	}
	return nil
}

var (
	ErrObservationLimits   = errors.New("observation: invalid limits")
	ErrObservationTooLarge = errors.New("observation: snapshot too large")
	ErrObservationInvalid  = errors.New("observation: inconsistent data")
	ErrObservationReader   = errors.New("observation: committed root reader required")
	ErrObservationStore    = errors.New("observation: store read failed")
)

// ObservationReader returns detached committed values, never caller-transaction
// state. Each snapshot reads its watermark and every field in one transaction.
type ObservationReader interface {
	ReadObservationRevision(context.Context, ID) (ObservationWatermark, error)
	ReadObservationSnapshot(context.Context, ID, ObservationLimits) (ObservationSnapshot, error)
}

type ObservationSnapshot struct {
	Watermark            ObservationWatermark
	Exists               bool
	Messages             []ObservationMessage
	Runs                 []ObservationRun
	Tools                []ObservationTool
	OmittedOlderMessages bool
}
type ObservationMessage struct {
	ID        MessageID
	RunID     RunID
	Role      Role
	Text      string
	Finalized bool
}
type ObservationRun struct {
	ID                  RunID
	Status              RunStatus
	ProviderID, ModelID string
	LeaseUntil          time.Time
}

func (r ObservationRun) Terminal() bool {
	return r.Status == RunCompleted || r.Status == RunFailed || r.Status == RunInterrupted
}

type ObservationTool struct {
	ID        ToolCallID
	RunID     RunID
	MessageID MessageID
	Name      string
	Status    ToolCallStatus
}

// Clone returns independently owned collections.
func (s ObservationSnapshot) Clone() ObservationSnapshot {
	s.Messages = append([]ObservationMessage(nil), s.Messages...)
	s.Runs = append([]ObservationRun(nil), s.Runs...)
	s.Tools = append([]ObservationTool(nil), s.Tools...)
	return s
}
