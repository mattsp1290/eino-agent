package sqlstore

import (
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

const discoveryTimeLayout = "2006-01-02T15:04:05.000000000Z"

// TimeText is the canonical fixed-width UTC representation used by SQL sort keys.
func TimeText(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(discoveryTimeLayout)
}

// DiscoveryTime parses and validates a canonical discovery timestamp.
func DiscoveryTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	if len(value) != 30 {
		return time.Time{}, session.ErrDiscoveryInvalid
	}
	parsed, err := time.Parse(discoveryTimeLayout, value)
	if err != nil || TimeText(parsed) != value {
		return time.Time{}, session.ErrDiscoveryInvalid
	}
	return parsed, nil
}
