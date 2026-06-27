package obs

// FieldClass describes whether a value is safe for default export.
type FieldClass string

const (
	// FieldAllowed is exported by default.
	FieldAllowed FieldClass = "allowed"
	// FieldSummaryOnly is exported only as an opt-in bounded summary.
	FieldSummaryOnly FieldClass = "summary_only"
	// FieldForbidden is never exported.
	FieldForbidden FieldClass = "forbidden"
)

// ObservationKind describes the runtime boundary that creates an observation.
type ObservationKind string

const (
	ObservationSession  ObservationKind = "session"
	ObservationRun      ObservationKind = "run"
	ObservationModel    ObservationKind = "model_call"
	ObservationStream   ObservationKind = "stream"
	ObservationTool     ObservationKind = "tool_call"
	ObservationAGUI     ObservationKind = "agui_event"
	ObservationRecovery ObservationKind = "recovery"
	ObservationError    ObservationKind = "error"
)

// FieldPolicy describes one attribute or payload family.
type FieldPolicy struct {
	Name        string
	Class       FieldClass
	Attribute   string
	Cardinality string
	Notes       string
}

// CorrelationField describes one propagated correlation identifier.
type CorrelationField struct {
	Name        string
	Attribute   string
	Cardinality string
	Required    bool
	Notes       string
}

// SummaryPolicy describes opt-in bounded summary behavior.
type SummaryPolicy struct {
	EnabledByDefault bool
	MaxBytesDefault  int
	AllowedKinds     []ObservationKind
	ForbiddenInputs  []string
}

// DefaultSummaryPolicy returns the safe summary default: disabled and bounded
// only when explicitly enabled by host configuration.
func DefaultSummaryPolicy() SummaryPolicy {
	return SummaryPolicy{
		EnabledByDefault: false,
		MaxBytesDefault:  0,
		AllowedKinds: []ObservationKind{
			ObservationModel,
			ObservationStream,
			ObservationTool,
		},
		ForbiddenInputs: []string{
			"raw_prompt",
			"raw_output",
			"raw_tool_payload",
			"attachment_bytes",
			"attachment_urls",
			"attachment_paths",
			"attachment_media_content",
			"stdout",
			"stderr",
			"reasoning",
			"encrypted_reasoning",
			"compaction_summary",
			"state_snapshot_payload",
			"agui_custom_event_payload",
			"secrets",
			"environment_dump",
			"headers",
			"tokens",
			"cookies",
			"api_keys",
			"auth_metadata",
		},
	}
}

// DefaultFields returns the default redaction policy for observations.
func DefaultFields() []FieldPolicy {
	return []FieldPolicy{
		{Name: "service.name", Class: FieldAllowed, Attribute: "service.name", Cardinality: "low"},
		{Name: "service.env", Class: FieldAllowed, Attribute: "service.env", Cardinality: "low"},
		{Name: "service.version", Class: FieldAllowed, Attribute: "service.version", Cardinality: "low"},
		{Name: "operation.name", Class: FieldAllowed, Attribute: "gen_ai.operation.name", Cardinality: "low"},
		{Name: "provider", Class: FieldAllowed, Attribute: "genai.provider", Cardinality: "bounded"},
		{Name: "model", Class: FieldAllowed, Attribute: "genai.model", Cardinality: "bounded"},
		{Name: "status", Class: FieldAllowed, Attribute: "status", Cardinality: "low"},
		{Name: "latency.total_ms", Class: FieldAllowed, Attribute: "genai.latency.total_ms", Cardinality: "numeric"},
		{Name: "latency.first_token_ms", Class: FieldAllowed, Attribute: "genai.latency.first_token_ms", Cardinality: "numeric"},
		{Name: "usage.input_tokens", Class: FieldAllowed, Attribute: "genai.usage.input_tokens", Cardinality: "numeric"},
		{Name: "usage.output_tokens", Class: FieldAllowed, Attribute: "genai.usage.output_tokens", Cardinality: "numeric"},
		{Name: "usage.reasoning_tokens", Class: FieldAllowed, Attribute: "genai.usage.reasoning_tokens", Cardinality: "numeric"},
		{Name: "usage.cached_input_tokens", Class: FieldAllowed, Attribute: "genai.usage.cached_input_tokens", Cardinality: "numeric"},
		{Name: "tool.name", Class: FieldAllowed, Attribute: "tool.name", Cardinality: "bounded"},
		{Name: "tool.kind", Class: FieldAllowed, Attribute: "tool.kind", Cardinality: "low"},
		{Name: "tool.status", Class: FieldAllowed, Attribute: "tool.status", Cardinality: "low"},
		{Name: "error.classification", Class: FieldAllowed, Attribute: "error.classification", Cardinality: "low"},
		{Name: "error.retryable", Class: FieldAllowed, Attribute: "error.retryable", Cardinality: "low"},
		{Name: "session.id", Class: FieldAllowed, Attribute: "session.id", Cardinality: "high"},
		{Name: "run.id", Class: FieldAllowed, Attribute: "run.id", Cardinality: "high"},
		{Name: "assistant_message.id", Class: FieldAllowed, Attribute: "assistant_message.id", Cardinality: "high"},
		{Name: "tool_call.id", Class: FieldAllowed, Attribute: "tool_call.id", Cardinality: "high"},
		{Name: "agui.thread.id", Class: FieldAllowed, Attribute: "agui.thread.id", Cardinality: "high"},
		{Name: "agui.run.id", Class: FieldAllowed, Attribute: "agui.run.id", Cardinality: "high"},
		{Name: "input.summary", Class: FieldSummaryOnly, Attribute: "genai.request.summary", Cardinality: "bounded"},
		{Name: "output.summary", Class: FieldSummaryOnly, Attribute: "genai.response.summary", Cardinality: "bounded"},
		{Name: "tool.input.summary", Class: FieldSummaryOnly, Attribute: "tool.input.summary", Cardinality: "bounded"},
		{Name: "tool.output.summary", Class: FieldSummaryOnly, Attribute: "tool.output.summary", Cardinality: "bounded"},
		{Name: "raw_prompt", Class: FieldForbidden, Notes: "Never export raw prompt or message content by default."},
		{Name: "raw_output", Class: FieldForbidden, Notes: "Never export raw model output by default."},
		{Name: "raw_tool_payload", Class: FieldForbidden, Notes: "Never export raw tool input or output payloads by default."},
		{Name: "attachments", Class: FieldForbidden, Notes: "Never export attachment content by default."},
		{Name: "attachment_bytes", Class: FieldForbidden, Notes: "Never export attachment bytes by default."},
		{Name: "attachment_urls", Class: FieldForbidden, Notes: "Never export attachment URLs by default."},
		{Name: "attachment_paths", Class: FieldForbidden, Notes: "Never export attachment file paths by default."},
		{Name: "attachment_media_content", Class: FieldForbidden, Notes: "Never export extracted image, PDF, audio, video, or other media content by default."},
		{Name: "stdout", Class: FieldForbidden, Notes: "Never export raw tool stdout beyond opt-in bounded summaries."},
		{Name: "stderr", Class: FieldForbidden, Notes: "Never export raw tool stderr beyond opt-in bounded summaries."},
		{Name: "reasoning", Class: FieldForbidden, Notes: "Never export reasoning content by default."},
		{Name: "encrypted_reasoning", Class: FieldForbidden, Notes: "Never export encrypted reasoning."},
		{Name: "compaction_summary", Class: FieldForbidden, Notes: "Never export compaction summary content by default."},
		{Name: "state_snapshot_payload", Class: FieldForbidden, Notes: "Never export host state snapshot payloads by default."},
		{Name: "agui_custom_event_payload", Class: FieldForbidden, Notes: "Never export AG-UI custom event payloads by default."},
		{Name: "secrets", Class: FieldForbidden, Notes: "Never export secrets."},
		{Name: "environment_dump", Class: FieldForbidden, Notes: "Never export environment dumps."},
		{Name: "headers", Class: FieldForbidden, Notes: "Never export raw headers."},
		{Name: "tokens", Class: FieldForbidden, Notes: "Never export bearer/session/API tokens."},
		{Name: "cookies", Class: FieldForbidden, Notes: "Never export cookies."},
		{Name: "api_keys", Class: FieldForbidden, Notes: "Never export API keys."},
		{Name: "auth_metadata", Class: FieldForbidden, Notes: "Never export auth metadata."},
	}
}

// DefaultCorrelationFields returns the correlation IDs propagated through
// eino-obs contexts and observations.
func DefaultCorrelationFields() []CorrelationField {
	return []CorrelationField{
		{Name: "trace_id", Attribute: "trace.id", Cardinality: "high", Notes: "Trace correlation, not a metric label."},
		{Name: "observation_id", Attribute: "observation.id", Cardinality: "high"},
		{Name: "parent_observation_id", Attribute: "observation.parent_id", Cardinality: "high"},
		{Name: "session_id", Attribute: "session.id", Cardinality: "high", Required: true},
		{Name: "run_id", Attribute: "run.id", Cardinality: "high", Required: true},
		{Name: "agent_id", Attribute: "agent.id", Cardinality: "bounded"},
		{Name: "assistant_message_id", Attribute: "assistant_message.id", Cardinality: "high"},
		{Name: "tool_call_id", Attribute: "tool_call.id", Cardinality: "high"},
		{Name: "provider", Attribute: "genai.provider", Cardinality: "bounded"},
		{Name: "model", Attribute: "genai.model", Cardinality: "bounded"},
		{Name: "agui_thread_id", Attribute: "agui.thread.id", Cardinality: "high"},
		{Name: "agui_run_id", Attribute: "agui.run.id", Cardinality: "high"},
	}
}
