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

// Gate names a host/provider decision that must pass before a rule can be
// persisted, replayed, or included in snapshots.
type Gate string

const (
	// GateProviderReasoningStorage requires provider and host permission to store
	// non-encrypted reasoning.
	GateProviderReasoningStorage Gate = "provider_reasoning_storage"
	// GateHostReplaySafeState requires the host to mark state as replay-safe.
	GateHostReplaySafeState Gate = "host_replay_safe_state"
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
	EventProviderState      EventFamily = "provider_state"
)

// Rule describes durability and replay behavior for one AG-UI event family.
type Rule struct {
	Family       EventFamily
	Persist      Disposition
	Replay       Disposition
	LiveTail     Disposition
	SessionPart  session.PartKind
	AuditKind    string
	Redaction    session.RedactionClass
	SnapshotSafe bool
	Gates        []Gate
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
			AuditKind: "run_lifecycle",
			Redaction: session.RedactionMetadata,
			Notes:     "RUN_STARTED/RUN_FINISHED are durable audit events and live-tail events; replay reconstructs current run state separately.",
		},
		{
			Family:       EventText,
			Persist:      DispositionPersist,
			Replay:       DispositionReplay,
			LiveTail:     DispositionLive,
			SessionPart:  session.PartAssistantGenText,
			AuditKind:    "message_delta",
			Redaction:    session.RedactionContent,
			SnapshotSafe: true,
			Notes:        "Live text deltas are emitted immediately; replay uses settled assistant_gen_text message parts, not stored SSE frames.",
		},
		{
			Family:      EventReasoning,
			Persist:     DispositionPersist,
			Replay:      DispositionReplay,
			LiveTail:    DispositionLive,
			SessionPart: session.PartReasoning,
			AuditKind:   "reasoning_delta",
			Redaction:   session.RedactionContent,
			Gates:       []Gate{GateProviderReasoningStorage},
			Notes:       "Plain reasoning may be stored and replayed only after provider and host policy allow reasoning storage.",
		},
		{
			Family:    EventEncryptedReasoning,
			Persist:   DispositionOmit,
			Replay:    DispositionOmit,
			LiveTail:  DispositionOmit,
			Redaction: session.RedactionContent,
			Notes:     "Encrypted reasoning is never persisted as an AG-UI event, replayed, or included in snapshots; a provider adapter may separately retain bounded opaque continuity state in a runtime-private provider_state part.",
		},
		{
			Family:    EventProviderState,
			Persist:   DispositionOmit,
			Replay:    DispositionOmit,
			LiveTail:  DispositionOmit,
			Redaction: session.RedactionContent,
			Notes:     "Provider-private continuity state is never an AG-UI event or snapshot field; runtime-owned provider_state parts are visible only at the durable-store operator boundary and matching provider adapter.",
		},
		{
			Family:      EventToolCall,
			Persist:     DispositionPersist,
			Replay:      DispositionReplay,
			LiveTail:    DispositionLive,
			SessionPart: session.PartFunctionToolCall,
			AuditKind:   "tool_call_updated",
			Redaction:   session.RedactionContent,
			Notes:       "Tool call starts/args/ends settle into durable tool-call records and replayable function_tool_call parts.",
		},
		{
			Family:      EventToolResult,
			Persist:     DispositionPersist,
			Replay:      DispositionReplay,
			LiveTail:    DispositionLive,
			SessionPart: session.PartFunctionToolResult,
			AuditKind:   "tool_result",
			Redaction:   session.RedactionContent,
			Notes:       "Tool results replay from bounded durable function_tool_result parts; live emission uses eino-agui emitter helpers.",
		},
		{
			Family:    EventStateSnapshot,
			Persist:   DispositionOmit,
			Replay:    DispositionOmit,
			LiveTail:  DispositionLive,
			AuditKind: "state_snapshot",
			Redaction: session.RedactionContent,
			Gates:     []Gate{GateHostReplaySafeState},
			Notes:     "State snapshots are live-tail only today: no durable PartKind backs this yet -- the W2 content contract's 20 block kinds cover model-authored content, not arbitrary host-visible app state -- so persistence and replay remain unimplemented pending a future work package. Once a PartKind is added, Persist/Replay should become DispositionPersist/DispositionReplay and this rule should gain a SessionPart, still gated on host replay-safety.",
		},
		{
			Family:    EventStateDelta,
			Persist:   DispositionAudit,
			Replay:    DispositionOmit,
			LiveTail:  DispositionLive,
			AuditKind: "state_delta",
			Redaction: session.RedactionContent,
			Notes:     "State deltas are live-tail events; replay uses the latest durable snapshot or host-projected state.",
		},
		{
			Family:    EventMessagesSnapshot,
			Persist:   DispositionOmit,
			Replay:    DispositionReplay,
			LiveTail:  DispositionLive,
			AuditKind: "messages_snapshot",
			Redaction: session.RedactionContent,
			Notes:     "Message snapshots are projected from durable messages and parts; raw snapshot SSE frames are not stored.",
		},
		{
			Family:    EventActivity,
			Persist:   DispositionAudit,
			Replay:    DispositionOmit,
			LiveTail:  DispositionLive,
			AuditKind: "activity",
			Redaction: session.RedactionMetadata,
			Notes:     "Activity is live UI state plus optional audit metadata, not replayable conversation content.",
		},
		{
			Family:    EventStep,
			Persist:   DispositionOmit,
			Replay:    DispositionOmit,
			LiveTail:  DispositionLive,
			AuditKind: "step",
			Redaction: session.RedactionMetadata,
			Notes:     "Step boundaries are live-tail only today: Bridge.StepStarted/StepFinished are pure passthroughs to the eino-agui emitter and write no durable part or EventRecord (session.AttemptReplacedEventKind is a separate, unrelated model-dispatch-retry audit trail, not a record of AG-UI step boundaries). No durable PartKind backs step boundaries yet, so persistence, audit, and replay remain unimplemented pending a future work package.",
		},
		{
			Family:    EventCustom,
			Persist:   DispositionAudit,
			Replay:    DispositionOmit,
			LiveTail:  DispositionLive,
			AuditKind: "custom",
			Redaction: session.RedactionContent,
			Notes:     "Custom events are live and audit-only unless a future typed custom replay contract is added.",
		},
		{
			Family:    EventError,
			Persist:   DispositionAudit,
			Replay:    DispositionReplay,
			LiveTail:  DispositionLive,
			AuditKind: "error",
			Redaction: session.RedactionMetadata,
			Notes:     "Errors are durable audit events and may replay as terminal run/message status rather than raw RUN_ERROR frames.",
		},
	}
}
