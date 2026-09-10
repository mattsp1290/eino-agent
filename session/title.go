package session

import (
	"errors"
	"time"
	"unicode/utf8"
)

var (
	// ErrSessionTitleInvalid reports an invalid title mutation request.
	ErrSessionTitleInvalid = errors.New("session title: invalid request")
	// ErrSessionTitleStore reports a title mutation storage failure without
	// exposing database details or rejected values.
	ErrSessionTitleStore = errors.New("session title: store failure")
)

// SessionTitleRequest selects one session in its exact expected workspace and
// supplies the title to store unchanged. WorkspaceID is an authorization
// selector owned by the host, not a credential or wildcard.
type SessionTitleRequest struct {
	SessionID   ID     `json:"session_id"`
	WorkspaceID string `json:"workspace_id"`
	Title       string `json:"title"`
}

// SessionTitleResult is the bounded result of a title mutation.
type SessionTitleResult struct {
	Title     string    `json:"title"`
	UpdatedAt time.Time `json:"updated_at"`
	Changed   bool      `json:"changed"`
}

// ValidateSessionTitle accepts valid UTF-8 display titles up to the discovery
// ceiling. Empty and whitespace-only titles are valid and remain unchanged.
func ValidateSessionTitle(title string) error {
	if len(title) > DiscoveryMaxTitleBytes || !utf8.ValidString(title) {
		return ErrSessionTitleInvalid
	}
	return nil
}

// Validate checks all bounded selectors and the title before store access.
func (r SessionTitleRequest) Validate() error {
	if r.SessionID == "" || len(r.SessionID) > DiscoveryMaxIdentityBytes || !utf8.ValidString(string(r.SessionID)) ||
		len(r.WorkspaceID) > DiscoveryMaxIdentityBytes || !utf8.ValidString(r.WorkspaceID) {
		return ErrSessionTitleInvalid
	}
	return ValidateSessionTitle(r.Title)
}
