package session

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestValidatePromotePausePayload(t *testing.T) {
	t.Parallel()

	run := Run{ID: "run-1", SessionID: "session-1"}
	baseEvent := EventRecord{ID: "pause-1", SessionID: run.SessionID, RunID: run.ID, Kind: RunPausedEventKind}
	lifecycle := PauseLifecycleV1{
		Version:            PauseLifecycleVersion,
		PauseID:            string(baseEvent.ID),
		CheckpointRevision: 1,
		Generation:         1,
		AgentPath:          "root",
		MessageID:          "message-1",
		AttemptID:          "attempt-1",
		EventRevision:      "event-revision-1",
	}
	validPayload, err := json.Marshal(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	mismatchedPause := lifecycle
	mismatchedPause.PauseID = "another-pause"
	mismatchedPausePayload, err := json.Marshal(mismatchedPause)
	if err != nil {
		t.Fatal(err)
	}
	mismatchedRevision := lifecycle
	mismatchedRevision.CheckpointRevision = 2
	mismatchedRevisionPayload, err := json.Marshal(mismatchedRevision)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		payload json.RawMessage
		wantErr bool
	}{
		{name: "absent operational payload"},
		{name: "malformed lifecycle object", payload: json.RawMessage(`{}`), wantErr: true},
		{name: "valid lifecycle", payload: validPayload},
		{name: "mismatched pause id", payload: mismatchedPausePayload, wantErr: true},
		{name: "mismatched checkpoint revision", payload: mismatchedRevisionPayload, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			event := baseEvent
			event.Payload = tt.payload
			err := ValidatePromotePause(run, PromotePauseRequest{Revision: 1, TurnID: "turn-1", Event: event})
			if tt.wantErr && !errors.Is(err, ErrConflict) {
				t.Fatalf("ValidatePromotePause() error = %v, want ErrConflict", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ValidatePromotePause() error = %v, want nil", err)
			}
		})
	}
}
