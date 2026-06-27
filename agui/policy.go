package agui

import "github.com/mattsp1290/eino-agent/session"

// Disposition states how an AG-UI-facing event family is handled by the
// runtime and durable store.
type Disposition string

const (
	// DispositionPersist means the event produces durable session facts.
	DispositionPersist Disposition = "persist"
	// DispositionReplay means the event is projected during history replay.
	DispositionReplay Disposition = "replay"
	// DispositionLive means the event is emitted only to the active live tail.
	DispositionLive Disposition = "live"
	// DispositionAudit means the event may be stored as an EventRecord but is not
	// replayed as conversation content.
	DispositionAudit Disposition = "audit"
	// DispositionOmit means the event is not persisted or replayed.
	DispositionOmit Disposition = "omit"
)

// EventFamily groups AG-UI events by durability behavior.
type EventFamily string

const (
	EventRunLifecycle       EventFamily = "run_lifecycle"
	EventText               EventFamily = "text"
	EventReasoning          EventFamily = "reasoning"
	EventToolCall           EventFamily = "tool_call"
	EventToolResult         EventFamily = "tool_result"
	EventStateSnapshot      EventFamily = "state_snapshot"
	EventStateDelta         EventFamily = "state_delta"
	EventMessagesSnapshot   EventFamily = "messages_snapshot"
	EventActivity           EventFamily = "activity"
	EventStep               EventFamily = "step"
	EventCustom             EventFamily = "custom"
	EventError              EventFamily = "error"
	EventEncryptedReasoning EventFamily = "encrypted_reasoning"
)

// Rule describes durability and replay behavior for one AG-UI event family.
type Rule struct {
	Family       EventFamily
	Persist      Disposition
	Replay       Disposition
	LiveTail     Disposition
	SessionPart  session.PartKind
	EventKind    string
	Redaction    session.RedactionClass
	SnapshotSafe bool
	Notes        string
}

// Rules returns the default AG-UI durability policy.
func Rules() []Rule {
	return []Rule{
		{
			Family:    EventRunLifecycle,
			Persist:   DispositionAudit,
			Replay:    DispositionOmit,
			LiveTail:  DispositionLive,
			EventKind: "run_lifecycle",
			Redaction: session.RedactionMetadata,
			Notes:     "RUN_STARTED/RUN_FINISHED are durable audit events and live-tail events; replay reconstructs current run state separately.",
		},
		{
			Family:       EventText,
			Persist:      DispositionPersist,
			Replay:       DispositionReplay,
			LiveTail:     DispositionLive,
			SessionPart:  session.PartText,
			EventKind:    "message_delta",
			Redaction:    session.RedactionContent,
			SnapshotSafe: true,
			Notes:        "Live text deltas are emitted immediately; replay uses settled message text parts, not stored SSE frames.",
		},
		{
			Family:       EventReasoning,
			Persist:      DispositionPersist,
			Replay:       DispositionReplay,
			LiveTail:     DispositionLive,
			SessionPart:  session.PartReasoning,
			EventKind:    "reasoning_delta",
			Redaction:    session.RedactionContent,
			SnapshotSafe: true,
			Notes:        "Plain reasoning may be stored when provider policy allows it; encrypted reasoning is governed by the encrypted reasoning rule.",
		},
		{
			Family:    EventEncryptedReasoning,
			Persist:   DispositionOmit,
			Replay:    DispositionOmit,
			LiveTail:  DispositionOmit,
			Redaction: session.RedactionContent,
			Notes:     "Encrypted reasoning is never persisted, replayed, or included in snapshots.",
		},
		{
			Family:      EventToolCall,
			Persist:     DispositionPersist,
			Replay:      DispositionReplay,
			LiveTail:    DispositionLive,
			SessionPart: session.PartToolCall,
			EventKind:   "tool_call_updated",
			Redaction:   session.RedactionContent,
			Notes:       "Tool call starts/args/ends settle into durable tool-call records and replayable tool-call parts.",
		},
		{
			Family:      EventToolResult,
			Persist:     DispositionPersist,
			Replay:      DispositionReplay,
			LiveTail:    DispositionLive,
			SessionPart: session.PartToolResult,
			EventKind:   "tool_result",
			Redaction:   session.RedactionContent,
			Notes:       "Tool results replay from bounded durable tool-result parts; live emission uses eino-agui emitter helpers.",
		},
		{
			Family:       EventStateSnapshot,
			Persist:      DispositionPersist,
			Replay:       DispositionReplay,
			LiveTail:     DispositionLive,
			SessionPart:  session.PartState,
			EventKind:    "state_snapshot",
			Redaction:    session.RedactionContent,
			SnapshotSafe: true,
			Notes:        "State snapshots are durable only when host policy marks them replay-safe.",
		},
		{
			Family:    EventStateDelta,
			Persist:   DispositionAudit,
			Replay:    DispositionOmit,
			LiveTail:  DispositionLive,
			EventKind: "state_delta",
			Redaction: session.RedactionContent,
			Notes:     "State deltas are live-tail events; replay uses the latest durable snapshot or host-projected state.",
		},
		{
			Family:    EventMessagesSnapshot,
			Persist:   DispositionOmit,
			Replay:    DispositionReplay,
			LiveTail:  DispositionLive,
			EventKind: "messages_snapshot",
			Redaction: session.RedactionContent,
			Notes:     "Message snapshots are projected from durable messages and parts; raw snapshot SSE frames are not stored.",
		},
		{
			Family:    EventActivity,
			Persist:   DispositionAudit,
			Replay:    DispositionOmit,
			LiveTail:  DispositionLive,
			EventKind: "activity",
			Redaction: session.RedactionMetadata,
			Notes:     "Activity is live UI state plus optional audit metadata, not replayable conversation content.",
		},
		{
			Family:      EventStep,
			Persist:     DispositionPersist,
			Replay:      DispositionReplay,
			LiveTail:    DispositionLive,
			SessionPart: session.PartStep,
			EventKind:   "step",
			Redaction:   session.RedactionMetadata,
			Notes:       "Step boundaries are durable for audit, replay annotations, and observability correlation.",
		},
		{
			Family:    EventCustom,
			Persist:   DispositionAudit,
			Replay:    DispositionOmit,
			LiveTail:  DispositionLive,
			EventKind: "custom",
			Redaction: session.RedactionContent,
			Notes:     "Custom events are live and audit-only unless a future typed custom replay contract is added.",
		},
		{
			Family:    EventError,
			Persist:   DispositionAudit,
			Replay:    DispositionReplay,
			LiveTail:  DispositionLive,
			EventKind: "error",
			Redaction: session.RedactionMetadata,
			Notes:     "Errors are durable audit events and may replay as terminal run/message status rather than raw RUN_ERROR frames.",
		},
	}
}
