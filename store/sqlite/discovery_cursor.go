package sqlite

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"time"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/session"
)

const discoveryTimeLayout = "2006-01-02T15:04:05.000000000Z"

// Identity fields use inner base64 to bound JSON expansion even for control bytes.
type discoveryCursor struct {
	Version   int    `json:"version"`
	Store     string `json:"store"`
	Workspace string `json:"workspace"`
	ID        string `json:"id"`
	Created   string `json:"created"`
}

func encodeDiscoveryCursor(store, workspace string, last session.SessionSummary) string {
	enc := base64.RawURLEncoding.EncodeToString
	raw, _ := json.Marshal(discoveryCursor{1, enc([]byte(store)), enc([]byte(workspace)), enc([]byte(last.ID)), timeText(last.CreatedAt)})
	return enc(raw)
}

func decodeDiscoveryCursor(value, workspace string) (discoveryCursor, error) {
	var c discoveryCursor
	invalid := func() (discoveryCursor, error) { return discoveryCursor{}, session.ErrDiscoveryCursor }
	if len(value) > session.DiscoveryMaxCursorBytes {
		return invalid()
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || json.Unmarshal(raw, &c) != nil || c.Version != 1 {
		return invalid()
	}
	dec := base64.RawURLEncoding.DecodeString
	store, e1 := dec(c.Store)
	ws, e2 := dec(c.Workspace)
	id, e3 := dec(c.ID)
	created, e4 := discoveryTime(c.Created)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || !validDiscoveryIncarnation(string(store)) || string(ws) != workspace || len(id) == 0 || len(id) > session.DiscoveryMaxIdentityBytes || !utf8.Valid(id) {
		return invalid()
	}
	if encodeDiscoveryCursor(string(store), string(ws), session.SessionSummary{ID: session.ID(id), CreatedAt: created}) != value {
		return invalid()
	}
	c.Store, c.Workspace, c.ID = string(store), string(ws), string(id)
	return c, nil
}

func validDiscoveryIncarnation(value string) bool {
	if len(value) != 32 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func discoveryTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	if len(value) != 30 {
		return time.Time{}, session.ErrDiscoveryInvalid
	}
	parsed, err := time.Parse(discoveryTimeLayout, value)
	if err != nil || timeText(parsed) != value {
		return time.Time{}, session.ErrDiscoveryInvalid
	}
	return parsed, nil
}
