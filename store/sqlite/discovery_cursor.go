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
type discoveryCursorEnvelope struct {
	Version   int    `json:"version"`
	Store     string `json:"store"`
	Workspace string `json:"workspace"`
	ID        string `json:"id"`
	Created   string `json:"created"`
}

// A position has one meaning: validated identifiers and a UTC creation time.
// Wire version and workspace binding are checked before constructing it.
type discoveryPosition struct {
	storeID   string
	id        session.ID
	createdAt time.Time
}

func encodeDiscoveryCursor(store, workspace string, last session.SessionSummary) string {
	enc := base64.RawURLEncoding.EncodeToString
	raw, _ := json.Marshal(discoveryCursorEnvelope{
		Version: 1, Store: enc([]byte(store)), Workspace: enc([]byte(workspace)),
		ID: enc([]byte(last.ID)), Created: timeText(last.CreatedAt),
	})
	return enc(raw)
}

func decodeDiscoveryCursor(value, workspace string) (discoveryPosition, error) {
	var c discoveryCursorEnvelope
	invalid := func() (discoveryPosition, error) { return discoveryPosition{}, session.ErrDiscoveryCursor }
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
	return discoveryPosition{storeID: string(store), id: session.ID(id), createdAt: created}, nil
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
