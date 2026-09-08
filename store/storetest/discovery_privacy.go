package storetest

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func discoveryPrivate(t *testing.T, factory Factory) {
	st, reader := discoverySubject(t, factory)
	ctx := t.Context()
	sessionIDs := []session.ID{"live", "expired", "completed"}
	for _, id := range sessionIDs {
		seedPrivateDiscovery(t, st, id)
	}
	before := captureDiscoveryFacts(t, st, sessionIDs)
	for range 3 {
		q := session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: 1}
		var ids []session.ID
		for {
			p := discoveryList(t, reader, q)
			ids = append(ids, discoveryIDs(p)...)
			raw, _ := json.Marshal(p)
			decoded, _ := base64.RawURLEncoding.DecodeString(p.NextCursor)
			if strings.Contains(string(raw), "PRIVATE_") || strings.Contains(string(decoded), "PRIVATE_") {
				t.Fatal("private data disclosed")
			}
			for _, s := range p.Sessions {
				raw, _ := json.Marshal(s)
				var keys map[string]any
				if err := json.Unmarshal(raw, &keys); err != nil {
					t.Fatal(err)
				}
				if len(keys) != 5 || keys["id"] == nil || keys["workspace_id"] == nil || keys["title"] == nil || keys["created_at"] == nil || keys["updated_at"] == nil {
					t.Fatal(keys)
				}
			}
			p.Sessions[0].Title = "local mutation"
			if p.NextCursor == "" {
				break
			}
			q.Cursor = p.NextCursor
		}
		if !reflect.DeepEqual(ids, []session.ID{"live", "expired", "completed"}) {
			t.Fatal(ids)
		}
	}
	if string(before) != string(captureDiscoveryFacts(t, st, sessionIDs)) {
		t.Fatal("discovery mutated durable state")
	}
	if _, err := st.ClaimRun(ctx, session.RunClaim{RunID: "live-run", OwnerID: "new", ClaimToken: "new", LeaseDuration: time.Minute}); !errors.Is(err, session.ErrSessionBusy) {
		t.Fatal("live claim changed", err)
	}
	if _, err := st.ClaimRun(ctx, session.RunClaim{RunID: "expired-run", OwnerID: "new", ClaimToken: "new", LeaseDuration: time.Minute}); err != nil {
		t.Fatal("expired recovery failed", err)
	}
}
