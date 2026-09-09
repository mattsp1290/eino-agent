package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-agent/session"
)

func TestNilStoreDiscoveryReaderPreservesValidationPrecedence(t *testing.T) {
	var store *Store
	var reader session.SessionDiscoveryReader = store
	if _, err := reader.ListSessions(context.Background(), session.SessionDiscoveryQuery{}); !errors.Is(err, session.ErrDiscoveryQuery) {
		t.Fatalf("nil reader invalid query error = %v, want ErrDiscoveryQuery", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reader.ListSessions(ctx, session.SessionDiscoveryQuery{WorkspaceID: "workspace"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("nil reader canceled error = %v, want context.Canceled", err)
	}
}
