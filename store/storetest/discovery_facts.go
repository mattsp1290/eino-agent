package storetest

import (
	"encoding/json"
	"testing"

	"github.com/mattsp1290/eino-agent/session"
)

// Named facts make the read-only contract's coverage explicit. The optional
// revision augments the same checks for backends implementing observation.
type discoveryDurableFacts struct {
	Session  session.Session
	Run      session.Run
	History  session.ReplayBatch
	Events   session.EventBatch
	Tool     session.ToolCall
	Requests session.ModelRequestBatch
	Revision *session.ObservationWatermark
}

func captureDiscoveryFacts(t *testing.T, st session.Store, ids []session.ID) []byte {
	t.Helper()
	ctx := t.Context()
	facts := make([]discoveryDurableFacts, 0, len(ids))
	for _, id := range ids {
		var f discoveryDurableFacts
		var err error
		if f.Session, err = st.GetSession(ctx, id); err != nil {
			t.Fatal(err)
		}
		if f.Run, err = st.GetRun(ctx, session.RunID(id)+"-run"); err != nil {
			t.Fatal(err)
		}
		if f.History, err = st.ListMessages(ctx, id, session.ReplayCursor{Limit: 100}); err != nil {
			t.Fatal(err)
		}
		if f.Events, err = st.ListEvents(ctx, id, session.EventCursor{Limit: 100}); err != nil {
			t.Fatal(err)
		}
		if f.Tool, err = st.GetToolCall(ctx, session.ToolCallID(id)+"-tool"); err != nil {
			t.Fatal(err)
		}
		if f.Requests, err = st.ListModelRequests(ctx, f.Run.ID, session.ModelRequestCursor{Limit: 100}); err != nil {
			t.Fatal(err)
		}
		if observer, ok := st.(session.ObservationReader); ok {
			w, err := observer.ReadObservationRevision(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			f.Revision = &w
		}
		facts = append(facts, f)
	}
	// Freeze the before-view so a backend's aliased maps cannot mask mutation.
	raw, err := json.Marshal(facts)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
