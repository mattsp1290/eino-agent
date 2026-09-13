package session

// Durable event kind constants beyond the pre-existing run_started,
// run_finished (RunSettlementEventKind), tool_call_updated
// (ToolTransitionEventKind), and message_delta kinds.
//
// TurnStartedEventKind, TurnCompletedEventKind, and RunPausedEventKind are
// canonical: only AdmitTurn, CompleteTurn/InterruptTurn, and PromotePause may
// persist records with these kinds (see ExecutionStore.AppendEvent, which
// rejects them). The remaining kinds are ordinary AppendEvent payloads
// produced by the runtime engine (not part of this package's atomic
// contract) and are declared here only so every durable event kind has one
// canonical string constant.
const (
	// TurnStartedEventKind marks one turn's atomic admission.
	TurnStartedEventKind = "turn_started"
	// TurnCompletedEventKind marks one turn's atomic normal completion.
	TurnCompletedEventKind = "turn_completed"
	// RunPausedEventKind marks a run's atomic transition into RunPaused.
	RunPausedEventKind = "run_paused"
	// RunResumedEventKind marks a run's transition out of RunPaused back to
	// running via ClaimRun.
	RunResumedEventKind = "run_resumed"
	// SubagentStartedEventKind marks a child agent path starting.
	SubagentStartedEventKind = "subagent_started"
	// SubagentFinishedEventKind marks a child agent path finishing normally.
	SubagentFinishedEventKind = "subagent_finished"
	// SubagentErrorEventKind marks a child agent path finishing with an error.
	SubagentErrorEventKind = "subagent_error"
	// AttemptReplacedEventKind marks a retried or failed-over model
	// invocation being replaced by a new attempt.
	AttemptReplacedEventKind = "attempt_replaced"
	// AuthorizedToolResultRewriteEventKind durably records one sanctioned
	// content-management rewrite of a function_tool_result's model-visible
	// content (patchtoolcalls patching a dangling call, reduction
	// truncating/clearing a settled result): which sealed handler instance
	// made the change, its Kind, the call ID, and the content digest before
	// and after -- see runtime.authorizedRewriteRecord. Correlation carries
	// the call ID.
	AuthorizedToolResultRewriteEventKind = "authorized_tool_result_rewrite"
	// SkillActivatedEventKind durably records one skill activation: the
	// skill's name and a content digest of what was actually loaded, so a
	// SKILL.md later edited between activation and any later point can be
	// told apart from the version this run actually used. Correlation
	// carries the skill name.
	SkillActivatedEventKind = "skill_activated"
	// MessageCommittedEventKind marks a durable message becoming readable:
	// emitted once after an assistant turn's content commits
	// (persistAssistantTurn) and once after each tool call settles
	// (persistToolSettlement), always carrying the committed message's ID
	// (EventRecord.MessageID) and the session's observation-watermark
	// revision at the moment of commit (see the "revision" field in
	// Payload). It is the notification an AG-UI bridge or other watcher
	// uses to know a durable projection is now safe to build and emit --
	// the underlying content is already committed regardless of whether
	// this notification itself is ever observed, so publishing it is
	// best-effort and its own failure never unwinds the commit it
	// describes. Ordinary AppendEvent payload, not canonical.
	MessageCommittedEventKind = "message_committed"
)
