// Package ensembleadapter sketches a future adapter from ensemble dispatcher
// events to eino-agent runtime events. It deliberately mirrors only the small
// event shape needed for the migration sketch and does not import ensemble
// internal packages.
package ensembleadapter

import (
	"encoding/json"
	"time"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

// RunEventKind mirrors the current ensemble dispatcher.RunEventKind strings
// relevant to AG-UI replay/tail and observability planning.
type RunEventKind string

const (
	EventRunStarted           RunEventKind = "run_started"
	EventSessionStarted       RunEventKind = "session_started"
	EventTurnStarted          RunEventKind = "turn_started"
	EventTurnCompleted        RunEventKind = "turn_completed"
	EventTurnFailed           RunEventKind = "turn_failed"
	EventTurnCancelled        RunEventKind = "turn_cancelled"
	EventToolCallStarted      RunEventKind = "tool_call_started"
	EventToolCallFinished     RunEventKind = "tool_call_finished"
	EventUnsupportedToolCall  RunEventKind = "unsupported_tool_call"
	EventMalformedToolCall    RunEventKind = "malformed_tool_call"
	EventModelFallbackEngaged RunEventKind = "model_fallback_engaged"
	EventNotification         RunEventKind = "notification"
	EventOtherMessage         RunEventKind = "other_message"
	EventRunFinalized         RunEventKind = "run_finalized"
	EventRunFailed            RunEventKind = "run_failed"
)

// RunEvent is the adapter-facing projection of ensemble's dispatcher.RunEvent.
type RunEvent struct {
	Kind          RunEventKind
	Time          time.Time
	IssueID       string
	RunAttemptID  string
	SessionID     string
	ThreadID      string
	TurnID        string
	ToolCallID    string
	ToolName      string
	ToolOutcome   string
	Message       string
	Error         string
	ErrorCategory string
	FinalStatus   string
	InputTokens   int64
	OutputTokens  int64
	FromModel     string
	ToModel       string
}

// Disposition describes how the adapter should treat one ensemble event.
type Disposition string

const (
	DispositionDurable  Disposition = "durable"
	DispositionLiveOnly Disposition = "live_only"
	DispositionOmit     Disposition = "omit"
)

// MappedEvent is the planned eino-agent projection for one ensemble event.
type MappedEvent struct {
	RuntimeEvent runtime.Event
	Disposition  Disposition
	Observation  string
}

// MapRunEvent maps ensemble's dispatcher event vocabulary to the eino-agent
// runtime event vocabulary. It is illustrative: a production adapter must add
// ID generation, store persistence, and AG-UI bridge wiring around this mapping.
func MapRunEvent(event RunEvent) MappedEvent {
	base := runtime.Event{
		SessionID: session.ID(first(event.ThreadID, event.SessionID)),
		RunID:     session.RunID(event.RunAttemptID),
		Usage: runtime.Usage{
			InputTokens:  event.InputTokens,
			OutputTokens: event.OutputTokens,
		},
		Payload: payload(event),
		Time:    event.Time,
	}
	switch event.Kind {
	case EventRunStarted:
		base.Kind = runtime.EventRunStarted
		return MappedEvent{RuntimeEvent: base, Disposition: DispositionDurable, Observation: "run"}
	case EventSessionStarted, EventTurnStarted, EventTurnCompleted:
		base.Kind = runtime.EventContextEpochChanged
		return MappedEvent{RuntimeEvent: base, Disposition: DispositionDurable, Observation: "turn"}
	case EventNotification:
		base.Kind = runtime.EventMessageDelta
		base.Payload = payload(map[string]string{"content": event.Message})
		base.LiveOnly = true
		return MappedEvent{RuntimeEvent: base, Disposition: DispositionLiveOnly, Observation: "message"}
	case EventOtherMessage, EventModelFallbackEngaged:
		return MappedEvent{RuntimeEvent: base, Disposition: DispositionOmit, Observation: "forensic"}
	case EventToolCallStarted, EventToolCallFinished:
		if event.ToolCallID == "" {
			return MappedEvent{RuntimeEvent: base, Disposition: DispositionOmit, Observation: "tool_uncorrelated"}
		}
		base.Kind = runtime.EventToolCallUpdated
		base.ToolCallID = session.ToolCallID(event.ToolCallID)
		return MappedEvent{RuntimeEvent: base, Disposition: DispositionDurable, Observation: "tool"}
	case EventUnsupportedToolCall, EventMalformedToolCall:
		return MappedEvent{RuntimeEvent: base, Disposition: DispositionOmit, Observation: "tool_validation"}
	case EventTurnFailed, EventTurnCancelled:
		return MappedEvent{RuntimeEvent: base, Disposition: DispositionOmit, Observation: "turn_error"}
	case EventRunFinalized:
		base.Kind = runtime.EventRunFinished
		return MappedEvent{RuntimeEvent: base, Disposition: DispositionDurable, Observation: "run"}
	case EventRunFailed:
		base.Kind = runtime.EventRunFinished
		base.Error.Message = first(event.Error, string(event.Kind))
		base.Error.Code = event.ErrorCategory
		return MappedEvent{RuntimeEvent: base, Disposition: DispositionDurable, Observation: "run"}
	default:
		return MappedEvent{RuntimeEvent: base, Disposition: DispositionOmit, Observation: "unknown"}
	}
}

func payload(value any) json.RawMessage {
	raw, _ := json.Marshal(value)
	return raw
}

func first(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
