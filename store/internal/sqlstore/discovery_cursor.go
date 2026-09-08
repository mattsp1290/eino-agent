package sqlstore

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"time"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/session"
)

// Identity fields use inner base64 to bound JSON expansion even for control bytes.
type discoveryCursorEnvelope struct {
	Version   int    `json:"version"`
	Store     string `json:"store"`
	Workspace string `json:"workspace"`
	ID        string `json:"id"`
	Created   string `json:"created"`
}

// DiscoveryPosition contains validated cursor identity and its UTC creation time.
type DiscoveryPosition struct {
	StoreID   string
	ID        session.ID
	CreatedAt time.Time
}

// EncodeDiscoveryCursor returns the canonical opaque discovery cursor.
func EncodeDiscoveryCursor(store, workspace string, last session.SessionSummary) string {
	enc := base64.RawURLEncoding.EncodeToString
	raw, _ := json.Marshal(discoveryCursorEnvelope{
		Version: 1, Store: enc([]byte(store)), Workspace: enc([]byte(workspace)),
		ID: enc([]byte(last.ID)), Created: TimeText(last.CreatedAt),
	})
	return enc(raw)
}

// DecodeDiscoveryCursor validates a cursor, including its workspace and canonical encoding.
func DecodeDiscoveryCursor(value, workspace string) (DiscoveryPosition, error) {
	var c discoveryCursorEnvelope
	invalid := func() (DiscoveryPosition, error) { return DiscoveryPosition{}, session.ErrDiscoveryCursor }
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
	created, e4 := DiscoveryTime(c.Created)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || !ValidDiscoveryIncarnation(string(store)) || string(ws) != workspace || len(id) == 0 || len(id) > session.DiscoveryMaxIdentityBytes || !utf8.Valid(id) {
		return invalid()
	}
	if EncodeDiscoveryCursor(string(store), string(ws), session.SessionSummary{ID: session.ID(id), CreatedAt: created}) != value {
		return invalid()
	}
	return DiscoveryPosition{StoreID: string(store), ID: session.ID(id), CreatedAt: created}, nil
}

// ValidDiscoveryIncarnation checks the stable lowercase hexadecimal store identity.
func ValidDiscoveryIncarnation(value string) bool {
	if len(value) != 32 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}
