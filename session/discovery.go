package session

import (
	"context"
	"errors"
	"time"
	"unicode/utf8"
)

const (
	DiscoveryDefaultLimit     = 50
	DiscoveryMaxLimit         = 100
	DiscoveryMaxIdentityBytes = 1024
	DiscoveryMaxTitleBytes    = 16384
	DiscoveryMaxCursorBytes   = 8192
)

var (
	ErrDiscoveryQuery    = errors.New("discovery: invalid query")
	ErrDiscoveryCursor   = errors.New("discovery: invalid cursor")
	ErrDiscoveryTooLarge = errors.New("discovery: summary too large")
	ErrDiscoveryInvalid  = errors.New("discovery: inconsistent data")
	ErrDiscoveryReader   = errors.New("discovery: committed root reader required")
	ErrDiscoveryStore    = errors.New("discovery: store read failed")
)

// SessionDiscoveryQuery selects one exact workspace, never a global listing.
// Hosts must authorize the selector on every call; it is not a credential.
// Limit zero means 50; 1–100 is accepted. Cursor is opaque, at most 8192 bytes.
// WorkspaceID must be nonempty valid UTF-8, at most 1024 bytes, without normalization.
type SessionDiscoveryQuery struct {
	WorkspaceID string `json:"workspace_id"`
	Limit       int    `json:"limit"`
	Cursor      string `json:"cursor"`
}

// Validate checks input bounds before any cursor decoding or store access.
func (q SessionDiscoveryQuery) Validate() error {
	if q.WorkspaceID == "" || len(q.WorkspaceID) > DiscoveryMaxIdentityBytes || !utf8.ValidString(q.WorkspaceID) || q.Limit < 0 || q.Limit > DiscoveryMaxLimit {
		return ErrDiscoveryQuery
	}
	if len(q.Cursor) > DiscoveryMaxCursorBytes {
		return ErrDiscoveryCursor
	}
	return nil
}

// SessionSummary contains only detached, allowlisted conversation metadata.
// ID and WorkspaceID have 1024-byte ceilings; Title has a 16384-byte ceiling.
// All strings are UTF-8. Titles may be empty and require host display escaping.
// Times are UTC with nanosecond precision; zero times are supported. UpdatedAt
// is stored metadata time, not message activity.
type SessionSummary struct {
	ID          ID        `json:"id"`
	WorkspaceID string    `json:"workspace_id"`
	Title       string    `json:"title"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// SessionDiscoveryPage orders summaries by CreatedAt descending, then ID
// descending in bytewise UTF-8 order. Zero creation times sort last.
// NextCursor is empty exactly when this page observed no further rows, even
// for a full final page. Passing an empty cursor starts a fresh traversal.
type SessionDiscoveryPage struct {
	Sessions   []SessionSummary `json:"sessions"`
	NextCursor string           `json:"next_cursor"`
}

// SessionDiscoveryReader is an optional read capability independent of Store
// and execution authority. It includes empty and completed conversations.
// Each call returns one committed view and releases all read resources before
// returning. Transaction-scoped readers are rejected; do not call the root
// reader while holding its sole connection in a transaction.
//
// Cursors bind the store incarnation, workspace and last returned ordering key,
// survive reopening the same store, and allow Limit changes. They are neither
// credentials nor tamper-proof. No anchor lookup or retained snapshot is needed.
// With fixed keys, returned rows never repeat: inserts ahead require refresh;
// inserts behind may appear later. Titles reflect each page's view. Mutating
// workspace/creation keys may miss or repeat rows and requires restarting.
//
// Cancellation takes priority at entry, followed by query validation and store
// work. Errors return a zero page and content-free ErrDiscovery* classes (or
// context errors), supporting errors.Is. Unknown workspaces return empty pages.
// Oversized included scalars fail the whole page with ErrDiscoveryTooLarge;
// invalid UTF-8 or timestamp projections give ErrDiscoveryInvalid. Lookahead
// only tests existence and does not validate the next page's summary. Nil or
// transaction-scoped readers give ErrDiscoveryReader; closed stores and other
// store failures give ErrDiscoveryStore. Listing performs no execution,
// recovery, lease renewal, or event publication.
type SessionDiscoveryReader interface {
	ListSessions(context.Context, SessionDiscoveryQuery) (SessionDiscoveryPage, error)
}
