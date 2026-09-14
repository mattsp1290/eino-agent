package runtime

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/mattsp1290/eino-agent/session"
)

func TestLoadPauseLifecycleCorrelatedCompensation(t *testing.T) {
	t.Parallel()

	original := session.PauseLifecycleV1{
		Version: session.PauseLifecycleVersion, PauseID: "pause-original", CheckpointRevision: 7, Generation: 7,
		AgentPath: "root", MessageID: "pause:run-1", AttemptID: "pause:run-1", EventRevision: "event-original",
		Targets: []session.PauseInterruptTarget{{ID: "target-a", Address: "root,a"}, {ID: "target-b", Address: "root,b"}},
	}
	replacement := original
	replacement.PauseID = "pause-replacement"
	replacement.EventRevision = "event-replacement"
	replacement.Targets = []session.PauseInterruptTarget{{ID: "target-a", Address: "root,new-a"}}
	differentRevision := replacement
	differentRevision.PauseID = "pause-different-revision"
	differentRevision.EventRevision = "event-different-revision"
	differentRevision.CheckpointRevision = 8
	differentRevision.Generation = 8

	pauseEvent := func(t *testing.T, lifecycle session.PauseLifecycleV1) session.EventRecord {
		t.Helper()
		payload, err := json.Marshal(lifecycle)
		if err != nil {
			t.Fatal(err)
		}
		return session.EventRecord{ID: session.EventID(lifecycle.EventRevision), SessionID: "session-1", RunID: "run-1", Kind: session.RunPausedEventKind, Payload: payload}
	}
	compensation := func(id session.EventID, correlation string) session.EventRecord {
		return session.EventRecord{ID: id, SessionID: "session-1", RunID: "run-1", Kind: session.RunPausedEventKind, Correlation: correlation}
	}

	tests := []struct {
		name       string
		later      []session.EventRecord
		want       *session.PauseLifecycleV1
		wantTarget []string
		wantMode   string
	}{
		{name: "one exact compensation", later: []session.EventRecord{compensation("compensation-1", original.EventRevision)}, want: &original, wantTarget: []string{"target-a"}, wantMode: session.ResumeModeTargeted},
		{name: "two exact compensations including durable null", later: []session.EventRecord{compensation("compensation-1", original.EventRevision), {ID: "compensation-2", SessionID: "session-1", RunID: "run-1", Kind: session.RunPausedEventKind, Correlation: original.EventRevision, Payload: json.RawMessage("null")}}, want: &original, wantTarget: []string{"target-a"}, wantMode: session.ResumeModeTargeted},
		{name: "empty correlation clears", later: []session.EventRecord{compensation("compensation-1", "")}},
		{name: "mismatched correlation clears", later: []session.EventRecord{compensation("compensation-1", "another-event")}},
		{name: "malformed correlated payload clears", later: []session.EventRecord{{ID: "malformed", SessionID: "session-1", RunID: "run-1", Kind: session.RunPausedEventKind, Correlation: original.EventRevision, Payload: []byte(`{"version":`)}}},
		{name: "later matching revision replaces", later: []session.EventRecord{pauseEvent(t, replacement)}, want: &replacement, wantTarget: []string{"target-a"}, wantMode: session.ResumeModeFull},
		{name: "later different revision clears", later: []session.EventRecord{pauseEvent(t, differentRevision)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := newAdmissionStore()
			store.putEvent(pauseEvent(t, original))
			for _, event := range tt.later {
				store.putEvent(event)
			}
			got, err := loadPauseLifecycle(context.Background(), store, "session-1", "run-1", 7, map[string]any{"target-a": "private-decision"})
			if err != nil {
				t.Fatalf("loadPauseLifecycle error = %v", err)
			}
			if tt.want == nil {
				if got != nil {
					t.Fatalf("loadPauseLifecycle = %#v, want no stale lifecycle", got)
				}
				return
			}
			if got == nil || !reflect.DeepEqual(got.paused, *tt.want) || !reflect.DeepEqual(got.targetIDs, tt.wantTarget) || got.mode != tt.wantMode {
				t.Fatalf("loadPauseLifecycle = %#v, want paused=%#v targets=%v mode=%q", got, *tt.want, tt.wantTarget, tt.wantMode)
			}
		})
	}
}
