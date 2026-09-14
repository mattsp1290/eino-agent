package session

import "testing"

func TestValidatePauseLifecycle(t *testing.T) {
	pause := PauseLifecycleV1{Version: PauseLifecycleVersion, PauseID: "pause-1", CheckpointRevision: 1, Generation: 1, AgentPath: "root", MessageID: "message-1", AttemptID: "attempt-1", EventRevision: "event-1", Targets: []PauseInterruptTarget{{ID: "target-1", Address: "root/0"}}}
	if err := ValidatePauseLifecycle(RunPausedEventKind, pause); err != nil {
		t.Fatalf("paused lifecycle error = %v", err)
	}
	pause.Targets = append(pause.Targets, pause.Targets[0])
	if err := ValidatePauseLifecycle(RunPausedEventKind, pause); err == nil {
		t.Fatal("duplicate target accepted")
	}
	resume := PauseLifecycleV1{Version: PauseLifecycleVersion, PauseID: "pause-1", Generation: 1, AgentPath: "root", MessageID: "message-1", AttemptID: "attempt-1", EventRevision: "event-2", ResumedPauseID: "pause-1", ResumeMode: ResumeModeFull, ResumePhase: ResumePhaseFact, NewTurnID: "turn-2", NewAttemptID: "attempt-2"}
	if err := ValidatePauseLifecycle(RunResumedEventKind, resume); err != nil {
		t.Fatalf("resumed lifecycle error = %v", err)
	}
}
