package ensembleadapter

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

func TestMapRunEventProjectsTerminalFailure(t *testing.T) {
	t.Parallel()

	mapped := MapRunEvent(RunEvent{
		Kind:          EventRunFailed,
		IssueID:       "ISSUE-1",
		RunAttemptID:  "42",
		ThreadID:      "thread-1",
		Time:          time.Unix(1, 0),
		Error:         "worker failed",
		ErrorCategory: "unknown",
		FinalStatus:   "failed",
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

func TestMapRunEventProjectsModelFallbackDurably(t *testing.T) {
	t.Parallel()

	mapped := MapRunEvent(RunEvent{
		Kind:         EventModelFallbackEngaged,
		IssueID:      "ISSUE-1",
		RunAttemptID: "42",
		ThreadID:     "thread-1",
		Time:         time.Unix(1, 0),
		FromModel:    "primary-model",
		ToModel:      "fallback-model",
	})
	if mapped.Disposition != DispositionDurable {
		t.Fatalf("Disposition = %q, want durable", mapped.Disposition)
	}
	if mapped.RuntimeEvent.Kind != runtime.EventModelFallbackEngaged {
		t.Fatalf("Kind = %q, want model_fallback_engaged", mapped.RuntimeEvent.Kind)
	}
	if mapped.RuntimeEvent.ModelID != "fallback-model" {
		t.Fatalf("ModelID = %q, want fallback-model", mapped.RuntimeEvent.ModelID)
	}
	var payload runtime.ModelFallbackPayload
	if err := json.Unmarshal(mapped.RuntimeEvent.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.FromModelID != "primary-model" || payload.ToModelID != "fallback-model" {
		t.Fatalf("payload transition = %+v", payload)
	}
}

func TestMapRunEventTreatsNotificationAsLiveOnlyDelta(t *testing.T) {
	t.Parallel()

	mapped := MapRunEvent(RunEvent{
		Kind:         EventNotification,
		IssueID:      "ISSUE-1",
		RunAttemptID: "42",
		ThreadID:     "thread-1",
		Time:         time.Unix(1, 0),
		Message:      "visible progress",
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
		EventRunFinalized,
		EventRunFailed,
	}
	for _, kind := range durableKinds {
		mapped := MapRunEvent(RunEvent{
			Kind:          kind,
			IssueID:       "ISSUE-1",
			RunAttemptID:  "42",
			ThreadID:      "thread-1",
			Time:          time.Unix(1, 0),
			ToolCallID:    "tool-1",
			FinalStatus:   "completed",
			Error:         "failed",
			ErrorCategory: "unknown",
		})
		if mapped.Disposition != DispositionDurable {
			t.Fatalf("%s disposition = %q, want durable", kind, mapped.Disposition)
		}
		if mapped.RuntimeEvent.Kind == "" {
			t.Fatalf("%s mapped to empty runtime kind", kind)
		}
	}
}

func TestMapRunEventTurnFailuresAreNotTerminalRunFinishes(t *testing.T) {
	t.Parallel()

	for _, kind := range []RunEventKind{EventTurnFailed, EventTurnCancelled} {
		mapped := MapRunEvent(RunEvent{
			Kind:          kind,
			IssueID:       "ISSUE-1",
			RunAttemptID:  "42",
			ThreadID:      "thread-1",
			TurnID:        "turn-1",
			Time:          time.Unix(1, 0),
			Error:         "turn failed",
			ErrorCategory: "unknown",
		})
		if mapped.RuntimeEvent.Kind == runtime.EventRunFinished {
			t.Fatalf("%s mapped to terminal run finish", kind)
		}
		if mapped.Disposition != DispositionOmit {
			t.Fatalf("%s disposition = %q, want omit until a nonterminal status event exists", kind, mapped.Disposition)
		}
	}
}

func TestMapRunEventUsesThreadIDAsStableSessionKey(t *testing.T) {
	t.Parallel()

	mapped := MapRunEvent(RunEvent{
		Kind:         EventRunStarted,
		IssueID:      "ISSUE-1",
		RunAttemptID: "42",
		SessionID:    "thread-1-turn-1",
		ThreadID:     "thread-1",
		Time:         time.Unix(1, 0),
	})
	if mapped.RuntimeEvent.SessionID != session.ID("thread-1") {
		t.Fatalf("SessionID = %q, want stable thread id", mapped.RuntimeEvent.SessionID)
	}
}

func TestMapRunEventDoesNotUseToolNameAsToolCallID(t *testing.T) {
	t.Parallel()

	first := MapRunEvent(RunEvent{
		Kind:         EventToolCallStarted,
		IssueID:      "ISSUE-1",
		RunAttemptID: "42",
		ThreadID:     "thread-1",
		TurnID:       "turn-1",
		Time:         time.Unix(1, 0),
		ToolName:     "search",
	})
	second := MapRunEvent(RunEvent{
		Kind:         EventToolCallStarted,
		IssueID:      "ISSUE-1",
		RunAttemptID: "42",
		ThreadID:     "thread-1",
		TurnID:       "turn-1",
		Time:         time.Unix(2, 0),
		ToolName:     "search",
	})
	if first.Disposition != DispositionOmit || second.Disposition != DispositionOmit {
		t.Fatalf("uncorrelated same-name tool calls = %q/%q, want omit", first.Disposition, second.Disposition)
	}
}

func TestMapRunEventOmitsUnsupportedAndMalformedToolCalls(t *testing.T) {
	t.Parallel()

	for _, kind := range []RunEventKind{EventUnsupportedToolCall, EventMalformedToolCall} {
		mapped := MapRunEvent(RunEvent{
			Kind:         kind,
			IssueID:      "ISSUE-1",
			RunAttemptID: "42",
			ThreadID:     "thread-1",
			TurnID:       "turn-1",
			Time:         time.Unix(1, 0),
			ToolName:     "search",
		})
		if mapped.Disposition != DispositionOmit {
			t.Fatalf("%s disposition = %q, want omit", kind, mapped.Disposition)
		}
	}
}
