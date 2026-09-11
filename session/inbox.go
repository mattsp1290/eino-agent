package session

import "time"

// InboxState is the durable lifecycle of one queued submission.
type InboxState string

const (
	// InboxQueued means the item is durable but not yet claimed by a turn.
	InboxQueued InboxState = "queued"
	// InboxConsumed means AdmitTurn claimed the item into a turn.
	InboxConsumed InboxState = "consumed"
	// InboxCompleted means the claiming turn completed normally.
	InboxCompleted InboxState = "completed"
	// InboxInterrupted means the claiming turn was interrupted or its run
	// was paused before completion.
	InboxInterrupted InboxState = "interrupted"
)

// InboxItem is one durable, idempotent submission queued for a session ahead
// of (or between) turns. Blocks are the submission's content, validated as
// RoleUser content on write.
type InboxItem struct {
	ID             InboxID
	SessionID      ID
	IdempotencyKey string
	Blocks         []ContentBlock
	State          InboxState
	TurnID         TurnID
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ValidateEnqueueInbox checks a freshly queued item's shape and validates its
// content against limits before any store mutation.
func ValidateEnqueueInbox(item InboxItem, limits ContentLimits) error {
	if item.ID == "" || item.SessionID == "" || item.IdempotencyKey == "" ||
		item.State != InboxQueued || item.TurnID != "" || item.CreatedAt.IsZero() {
		return ErrConflict
	}
	return Content{Role: RoleUser, Blocks: item.Blocks}.Validate(limits)
}
