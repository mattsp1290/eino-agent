// Package watch provides bounded same-process session state observation.
// Durable replacements coalesce revisions; transient text has separate identity.
// It owns neither execution nor the store.
package watch

import (
	"errors"
	"math"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

var (
	ErrOptions        = errors.New("watch: invalid options or reader")
	ErrClosed         = errors.New("watch: closed")
	ErrCapacity       = errors.New("watch: capacity exceeded")
	ErrBusy           = errors.New("watch: concurrent read")
	ErrResyncRequired = errors.New("watch: resync required")
)

type Options struct {
	Snapshot                                                                            session.ObservationLimits
	PollInterval, ReadTimeout                                                           time.Duration
	MaxSubscriptions, MaxWatchedSessions, MaxLiveRuns, MaxLiveTextBytes, PendingUpdates int
}

func (o Options) validate() error {
	if o.Snapshot.Validate() != nil || o.PollInterval <= 0 || o.ReadTimeout <= 0 {
		return ErrOptions
	}
	for _, n := range []int{o.MaxSubscriptions, o.MaxWatchedSessions, o.MaxLiveRuns, o.MaxLiveTextBytes, o.PendingUpdates} {
		if n <= 0 || n > math.MaxInt/16 {
			return ErrOptions
		}
	}
	if o.MaxSubscriptions > math.MaxInt/o.PendingUpdates || o.PendingUpdates > math.MaxInt/o.Snapshot.MaxSnapshotBytes {
		return ErrOptions
	}
	return nil
}

type Kind uint8

const (
	Durable Kind = iota + 1
	Live
	LiveUnavailable
)

// LiveIdentity identifies a model attempt within one service incarnation.
type LiveIdentity struct {
	ServiceID     string
	SessionID     session.ID
	RunID         session.RunID
	MessageID     session.MessageID
	RequestID     session.ModelRequestID
	Attempt, Step int
}
type LiveText struct {
	Identity           LiveIdentity
	Sequence           uint64
	PublicationVersion uint64
	Text               string
}

// Update is a closed union: Durable carries Snapshot; Live and LiveUnavailable
// carry Live. Unavailable removes any older overlay for that qualified message.
// Only Durable advances a store watermark.
type Update struct {
	Kind             Kind
	DeliverySequence uint64
	Snapshot         session.ObservationSnapshot
	Live             LiveText
}

func (u Update) clone() Update { u.Snapshot = u.Snapshot.Clone(); return u }

// Eligible prevents delayed live work from resurrecting removed/finalized views.
func Eligible(s session.ObservationSnapshot, id LiveIdentity) bool {
	if id.SessionID != s.Watermark.SessionID {
		return false
	}
	active := false
	for _, r := range s.Runs {
		if r.ID == id.RunID && !r.Terminal() {
			active = true
			break
		}
	}
	if !active {
		return false
	}
	for _, m := range s.Messages {
		if m.ID == id.MessageID && m.RunID == id.RunID && !m.Finalized {
			return true
		}
	}
	return false
}
