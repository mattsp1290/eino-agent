package ensembleadapter

import (
	"testing"

	"github.com/mattsp1290/eino-agent/runtime"
)

func TestMapRunEventProjectsTerminalFailure(t *testing.T) {
	t.Parallel()

	mapped := MapRunEvent(RunEvent{
		Kind:          EventRunFailed,
		RunAttemptID:  "42",
		ThreadID:      "thread-1",
		Error:         "worker failed",
		ErrorCategory: "unknown",
	})
	if mapped.Disposition != DispositionDurable {
		t.Fatalf("Disposition = %q, want durable", mapped.Disposition)
	}
	if mapped.RuntimeEvent.Kind != runtime.EventRunFinished {
		t.Fatalf("Kind = %q, want run_finished", mapped.RuntimeEvent.Kind)
	}
	if mapped.RuntimeEvent.Error.Message != "worker failed" {
		t.Fatalf("Error = %#v", mapped.RuntimeEvent.Error)
	}
}

func TestMapRunEventTreatsNotificationAsLiveOnlyDelta(t *testing.T) {
	t.Parallel()

	mapped := MapRunEvent(RunEvent{
		Kind:     EventNotification,
		ThreadID: "thread-1",
		Message:  "visible progress",
	})
	if mapped.Disposition != DispositionLiveOnly {
		t.Fatalf("Disposition = %q, want live_only", mapped.Disposition)
	}
	if mapped.RuntimeEvent.Kind != runtime.EventMessageDelta || !mapped.RuntimeEvent.LiveOnly {
		t.Fatalf("Runtime event = %#v", mapped.RuntimeEvent)
	}
}

func TestMapRunEventOmitsForensicOnlyMessages(t *testing.T) {
	t.Parallel()

	mapped := MapRunEvent(RunEvent{Kind: EventOtherMessage, Message: "reasoning"})
	if mapped.Disposition != DispositionOmit {
		t.Fatalf("Disposition = %q, want omit", mapped.Disposition)
	}
}

func TestMapRunEventDurableMappingsHaveRuntimeKind(t *testing.T) {
	t.Parallel()

	durableKinds := []RunEventKind{
		EventRunStarted,
		EventSessionStarted,
		EventTurnStarted,
		EventTurnCompleted,
		EventToolCallStarted,
		EventToolCallFinished,
		EventUnsupportedToolCall,
		EventMalformedToolCall,
		EventTurnFailed,
		EventTurnCancelled,
		EventRunFinalized,
		EventRunFailed,
	}
	for _, kind := range durableKinds {
		mapped := MapRunEvent(RunEvent{Kind: kind, ToolCallID: "tool-1"})
		if mapped.Disposition != DispositionDurable {
			t.Fatalf("%s disposition = %q, want durable", kind, mapped.Disposition)
		}
		if mapped.RuntimeEvent.Kind == "" {
			t.Fatalf("%s mapped to empty runtime kind", kind)
		}
	}
}
