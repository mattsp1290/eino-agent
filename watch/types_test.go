package watch

import (
	"encoding/json"
	"testing"

	"github.com/mattsp1290/eino-agent/session"
)

// FuzzObservationUpdate exercises malformed identities, kinds and watermarks
// against whole-window eligibility. Inputs are finite and contain no real data.
func FuzzObservationUpdate(f *testing.F) {
	for _, seed := range []string{
		`{"Kind":2,"Live":{"Identity":{"SessionID":"s","RunID":"r","MessageID":"m"}}}`,
		`{"Kind":255,"Live":{"Identity":{"SessionID":"foreign","RunID":"r","MessageID":"m"}}}`,
		`{"Snapshot":{"Watermark":{"Revision":9223372036854775807}}}`,
		"{\"Live\":{\"Text\":\"\xff\"}}",
	} {
		f.Add([]byte(seed), false, false)
	}
	f.Fuzz(func(t *testing.T, raw []byte, finalized, terminal bool) {
		if len(raw) > 4096 {
			return
		}
		var u Update
		if json.Unmarshal(raw, &u) != nil {
			return
		}
		s := session.ObservationSnapshot{Watermark: session.ObservationWatermark{StoreID: "store", SessionID: "s", Revision: 1}, Exists: true, Messages: []session.ObservationMessage{{ID: "m", RunID: "r", Role: session.RoleAssistant, Finalized: finalized}}, Runs: []session.ObservationRun{{ID: "r", Status: session.RunRunning}}}
		if terminal {
			s.Runs[0].Status = session.RunFailed
		}
		eligible := Eligible(s, u.Live.Identity)
		if eligible && (finalized || terminal || u.Live.Identity.SessionID != "s" || u.Live.Identity.MessageID != "m" || u.Live.Identity.RunID != "r") {
			t.Fatal("foreign or retired overlay eligible")
		}
		s.Messages = nil
		if Eligible(s, u.Live.Identity) {
			t.Fatal("overlay resurrected absent message")
		}
	})
}
