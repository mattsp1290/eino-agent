package obs

import "testing"

func TestDefaultSummaryPolicyIsOptIn(t *testing.T) {
	t.Parallel()

	policy := DefaultSummaryPolicy()
	if policy.EnabledByDefault {
		t.Fatal("summaries must be disabled by default")
	}
	if policy.MaxBytesDefault != 0 {
		t.Fatalf("MaxBytesDefault = %d, want 0", policy.MaxBytesDefault)
	}
	for _, name := range forbiddenPayloadFamilies() {
		if !contains(policy.ForbiddenInputs, name) {
			t.Fatalf("summary policy missing forbidden input %s: %#v", name, policy.ForbiddenInputs)
		}
	}
	wantKinds := []ObservationKind{ObservationModel, ObservationStream, ObservationTool}
	for _, kind := range wantKinds {
		if !containsKind(policy.AllowedKinds, kind) {
			t.Fatalf("summary policy missing allowed kind %s: %#v", kind, policy.AllowedKinds)
		}
	}
}

func TestDefaultFieldsForbidRawContent(t *testing.T) {
	t.Parallel()

	fields := fieldsByName(DefaultFields())
	for _, name := range forbiddenPolicyFields() {
		if fields[name].Class != FieldForbidden {
			t.Fatalf("%s class = %q, want forbidden", name, fields[name].Class)
		}
	}
	for _, name := range []string{"input.summary", "output.summary", "tool.input.summary", "tool.output.summary"} {
		if fields[name].Class != FieldSummaryOnly {
			t.Fatalf("%s class = %q, want summary_only", name, fields[name].Class)
		}
	}
}

func TestRequiredCorrelationFields(t *testing.T) {
	t.Parallel()

	fields := correlationByName(DefaultCorrelationFields())
	for _, name := range []string{"session_id", "run_id"} {
		if !fields[name].Required {
			t.Fatalf("%s should be required", name)
		}
	}
	for _, name := range []string{"trace_id", "observation_id", "tool_call_id", "agui_thread_id", "agui_run_id"} {
		if fields[name].Cardinality != "high" {
			t.Fatalf("%s cardinality = %q, want high", name, fields[name].Cardinality)
		}
	}
}

func fieldsByName(fields []FieldPolicy) map[string]FieldPolicy {
	result := make(map[string]FieldPolicy, len(fields))
	for _, field := range fields {
		result[field.Name] = field
	}
	return result
}

func correlationByName(fields []CorrelationField) map[string]CorrelationField {
	result := make(map[string]CorrelationField, len(fields))
	for _, field := range fields {
		result[field.Name] = field
	}
	return result
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func containsKind(values []ObservationKind, want ObservationKind) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func forbiddenPayloadFamilies() []string {
	return []string{
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
	}
}

func forbiddenPolicyFields() []string {
	return append([]string{"attachments"}, forbiddenPayloadFamilies()...)
}
