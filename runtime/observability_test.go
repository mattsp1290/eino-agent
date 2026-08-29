package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"
	einoobs "github.com/mattsp1290/eino-obs"

	agentcontext "github.com/mattsp1290/eino-agent/context"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/permissions"
	"github.com/mattsp1290/eino-agent/session"
)

func TestStreamingOrchestratorRecordsNoNetworkObservations(t *testing.T) {
	t.Parallel()

	observer := einoobs.New(einoobs.Config{Service: "eino-agent-test"})
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(ctx context.Context, request model.Request) ([]*einoschema.Message, error) {
		request.Observer.OnProviderEnd(ctx, model.Response{Usage: model.Usage{InputTokens: 3, OutputTokens: 2, ReasoningTokens: 1, CacheReadTokens: 4}})
		return []*einoschema.Message{einoschema.AssistantMessage("hello", nil)}, nil
	}))
	orch.observer = observer
	orch.trace = agentcontext.TraceContext{TraceID: "trace-1"}
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	observations := observer.Snapshot().Observations
	stream := assertObservation(t, observations, "stream", "ok", "trace-1")
	if stream.Attributes["genai.usage.input_tokens"] != int64(3) || stream.Attributes["genai.usage.output_tokens"] != int64(2) {
		t.Fatalf("stream usage attrs = %#v", stream.Attributes)
	}
	run := assertObservation(t, observations, "run", "ok", "trace-1")
	sessionObs := assertObservation(t, observations, "session", "ok", "trace-1")
	if run.ParentID != sessionObs.ID {
		t.Fatalf("run parent = %q, want session %q", run.ParentID, sessionObs.ID)
	}
	if observationContains(observations, "hello") {
		t.Fatalf("observations leaked raw model content: %#v", observations)
	}
}

func TestStreamingOrchestratorRecordsRetryAndProviderError(t *testing.T) {
	t.Parallel()

	observer := einoobs.New(einoobs.Config{Service: "eino-agent-test"})
	store := newAdmissionStore()
	var calls int
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		calls++
		return nil, model.Error{Code: "rate_limited", Message: "SECRET prompt retry me", Retryable: true, Cause: model.ErrProviderRateLimited}
	}))
	orch.attemptsValue = 2
	orch.observer = observer
	result := startAndWait(t, orch)
	if result.Status != session.RunFailed || calls != 2 {
		t.Fatalf("result = %+v calls=%d", result, calls)
	}
	observations := observer.Snapshot().Observations
	retry := assertObservation(t, observations, "retry", "ok", "")
	if retry.Attributes["error.classification"] != "rate_limited" {
		t.Fatalf("retry attrs = %#v", retry.Attributes)
	}
	failed := assertObservation(t, observations, "run", "error", "")
	if failed.Error == nil || failed.Error.Classification != "rate_limited" || !failed.Error.Retryable {
		t.Fatalf("failed run error = %#v", failed.Error)
	}
	assertObservation(t, observations, "error", "error", "")
	streams := observationsByKind(observations, "stream")
	if len(streams) != 2 || streams[0].ID == streams[1].ID {
		t.Fatalf("retry streams = %#v", streams)
	}
	if streams[0].Attributes["genai.retry.attempt"] != int64(1) || streams[1].Attributes["genai.retry.attempt"] != int64(2) {
		t.Fatalf("retry stream attrs = %#v / %#v", streams[0].Attributes, streams[1].Attributes)
	}
	if observationContains(observations, "SECRET prompt") {
		t.Fatalf("observations leaked provider error text: %#v", observations)
	}
}

func TestStreamingOrchestratorRecordsCancellation(t *testing.T) {
	t.Parallel()

	observer := einoobs.New(einoobs.Config{Service: "eino-agent-test"})
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return nil, context.Canceled
	}))
	orch.observer = observer
	result := startAndWait(t, orch)
	if result.Status != session.RunInterrupted || !result.Interrupted {
		t.Fatalf("result = %+v", result)
	}
	observations := observer.Snapshot().Observations
	run := assertObservation(t, observations, "run", "canceled", "")
	if run.Error == nil || run.Error.Classification != "canceled" {
		t.Fatalf("run error = %#v", run.Error)
	}
	assertObservation(t, observations, "cancellation", "canceled", "")
}

func TestStreamingOrchestratorRecordsInterrupt(t *testing.T) {
	t.Parallel()

	observer := einoobs.New(einoobs.Config{Service: "eino-agent-test"})
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(ctx context.Context, _ model.Request) ([]*einoschema.Message, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	orch.observer = observer
	handle, err := orch.Start(context.Background(), Request{
		SessionID: "session-1",
		ParentID:  "user-1",
		Input:     []*einoschema.Message{einoschema.UserMessage("SECRET prompt")},
		Config:    orchestratorConfig(),
	})
	if err != nil {
		t.Fatalf("Start error = %v", err)
	}
	if err := handle.Interrupt(context.Background(), "disconnect"); err != nil {
		t.Fatalf("Interrupt error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunInterrupted {
		t.Fatalf("result = %+v", result)
	}
	observations := observer.Snapshot().Observations
	interrupt := assertObservation(t, observations, "interrupt", "ok", "")
	if interrupt.Attributes["interrupt.reason"] != "disconnect" {
		t.Fatalf("interrupt attrs = %#v", interrupt.Attributes)
	}
	assertObservation(t, observations, "cancellation", "canceled", "")
	if observationContains(observations, "SECRET prompt") {
		t.Fatalf("observations leaked raw prompt: %#v", observations)
	}
}

func TestStreamingOrchestratorRecordsResume(t *testing.T) {
	t.Parallel()

	observer := einoobs.New(einoobs.Config{Service: "eino-agent-test"})
	store, run := resumeStoreWithTool(t, "dead-owner", session.ToolCallPending)
	defer func() {
		_ = store.Close()
	}()
	now := time.Date(2026, 6, 28, 14, 0, 0, 0, time.UTC)
	toolRegistry := staticToolRegistry{tools: []Tool{{Name: "echo", Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) { return ToolResult{Output: "ok"}, nil })}}}
	orch := mustConfiguredOrchestrator(
		WithStore(store),
		WithOwnerID("owner-1"),
		WithClock(func() time.Time { return now }),
		WithObserver(observer),
		WithRunPlanProvider(staticRunPlanProvider{plan: newTestToolPlan(toolRegistry)}),
	)
	handle, err := orch.Resume(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("Resume error = %v", err)
	}
	result := <-handle.Done()
	if result.Status != session.RunInterrupted {
		t.Fatalf("result = %+v", result)
	}
	resume := assertObservation(t, observer.Snapshot().Observations, "resume", "ok", "")
	if resume.Attributes["resume.status"] != "started" {
		t.Fatalf("resume attrs = %#v", resume.Attributes)
	}
	observations := observer.Snapshot().Observations
	assertObservation(t, observations, "tool.materialized", "ok", "")
	toolSpan := assertObservation(t, observations, "tool_call", "ok", "")
	if toolSpan.Attributes["tool.call_id"] != "call-resume" || toolSpan.Attributes["tool.status"] != "succeeded" {
		t.Fatalf("tool span attrs = %#v", toolSpan.Attributes)
	}
	settled := assertObservation(t, observations, "tool.settled", "ok", "")
	if settled.Attributes["tool.call_id"] != "call-resume" || settled.Attributes["tool.status"] != "succeeded" {
		t.Fatalf("settled attrs = %#v", settled.Attributes)
	}
	assertObservation(t, observations, "run", "canceled", "")
}

func TestStreamingOrchestratorRecordsToolLifecycleWithoutPayloadLeak(t *testing.T) {
	t.Parallel()

	observer := einoobs.New(einoobs.Config{Service: "eino-agent-test"})
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, msg := range request.Messages {
			if msg.Role == einoschema.Tool {
				return []*einoschema.Message{einoschema.AssistantMessage("done", nil)}, nil
			}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:   "call-1",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "echo",
				Arguments: `{"text":"SECRET tool input"}`,
			},
		}})}, nil
	}))
	orch.observer = observer
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "SECRET tool output"}, nil
		}),
	}}})
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	observations := observer.Snapshot().Observations
	assertObservation(t, observations, "tool.registered", "ok", "")
	assertObservation(t, observations, "tool.materialized", "ok", "")
	toolSpan := assertObservation(t, observations, "tool_call", "ok", "")
	if toolSpan.Attributes["tool.call_id"] != "call-1" || toolSpan.Attributes["tool.status"] != "succeeded" {
		t.Fatalf("tool span attrs = %#v", toolSpan.Attributes)
	}
	settled := assertObservation(t, observations, "tool.settled", "ok", "")
	if settled.Attributes["tool.status"] != "succeeded" {
		t.Fatalf("settled attrs = %#v", settled.Attributes)
	}
	want := readObservationGolden(t, "../testdata/obs/tool_lifecycle_observations.json")
	requireGoldenEqual(t, goldenToolObservations(observations), want)
	if observationContains(observations, "SECRET tool input") || observationContains(observations, "SECRET tool output") {
		t.Fatalf("observations leaked tool payloads: %#v", observations)
	}
}

func TestStreamingOrchestratorRecordsPermissionDeniedToolAsExpectedFailure(t *testing.T) {
	t.Parallel()

	observer := einoobs.New(einoobs.Config{Service: "eino-agent-test"})
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, msg := range request.Messages {
			if msg.Role == einoschema.Tool {
				return []*einoschema.Message{einoschema.AssistantMessage("handled", nil)}, nil
			}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:   "call-denied",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "echo",
				Arguments: `{"target":"SECRET danger pattern"}`,
			},
		}})}, nil
	}))
	orch.observer = observer
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name:    "echo",
		Pattern: permissionPatternField("target"),
		Scope: ToolScope{
			Permissions: []string{"shell"},
		},
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			t.Fatal("executor should not run")
			return ToolResult{}, nil
		}),
	}}})
	orch.permissions = permissions.PolicyFunc(func(_ context.Context, request permissions.Request) (permissions.Decision, error) {
		if request.Pattern != "SECRET danger pattern" {
			t.Fatalf("permission pattern = %q", request.Pattern)
		}
		return permissions.Decision{Action: permissions.ActionDeny, Message: "blocked"}, nil
	})
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	toolSpan := assertObservation(t, observer.Snapshot().Observations, "tool_call", "error", "")
	if toolSpan.Error == nil || toolSpan.Error.Classification != "permission_denied" {
		t.Fatalf("tool span error = %#v attrs=%#v", toolSpan.Error, toolSpan.Attributes)
	}
	settled := assertObservation(t, observer.Snapshot().Observations, "tool.settled", "error", "")
	if settled.Attributes["tool.status"] != "failed" {
		t.Fatalf("settled attrs = %#v", settled.Attributes)
	}
	if observationContains(observer.Snapshot().Observations, "SECRET danger pattern") {
		t.Fatalf("observations leaked permission pattern: %#v", observer.Snapshot().Observations)
	}
}

func TestStreamingOrchestratorRecordsOperationalToolFailure(t *testing.T) {
	t.Parallel()

	observer := einoobs.New(einoobs.Config{Service: "eino-agent-test"})
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(_ context.Context, request model.Request) ([]*einoschema.Message, error) {
		for _, msg := range request.Messages {
			if msg.Role == einoschema.Tool {
				return []*einoschema.Message{einoschema.AssistantMessage("handled", nil)}, nil
			}
		}
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:   "call-fail",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "echo",
				Arguments: `{}`,
			},
		}})}, nil
	}))
	orch.observer = observer
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{}, errors.New("SECRET operational detail")
		}),
	}}})
	result := startAndWait(t, orch)
	if result.Status != session.RunCompleted {
		t.Fatalf("result = %+v", result)
	}
	toolSpan := assertObservation(t, observer.Snapshot().Observations, "tool_call", "error", "")
	if toolSpan.Error == nil || toolSpan.Error.Classification != "operational_failure" {
		t.Fatalf("tool span error = %#v attrs=%#v", toolSpan.Error, toolSpan.Attributes)
	}
	if observationContains(observer.Snapshot().Observations, "SECRET operational detail") {
		t.Fatalf("observations leaked tool error: %#v", observer.Snapshot().Observations)
	}
}

func TestStreamingOrchestratorRecordsUnavailableToolFailureWithoutPayloadLeak(t *testing.T) {
	t.Parallel()

	observer := einoobs.New(einoobs.Config{Service: "eino-agent-test"})
	store := newAdmissionStore()
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:   "call-missing",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "missing",
				Arguments: `{"text":"SECRET missing tool input"}`,
			},
		}})}, nil
	}))
	orch.observer = observer
	configureTestTools(orch, staticToolRegistry{})
	result := startAndWait(t, orch)
	if result.Status != session.RunFailed {
		t.Fatalf("result = %+v", result)
	}
	observations := observer.Snapshot().Observations
	settled := assertObservation(t, observations, "tool.settled", "error", "")
	if settled.Attributes["tool.call_id"] != "call-missing" || settled.Attributes["tool.status"] != "failed" {
		t.Fatalf("settled attrs = %#v", settled.Attributes)
	}
	if settled.Error == nil || settled.Error.Classification != "operational_failure" {
		t.Fatalf("settled error = %#v", settled.Error)
	}
	if observationContains(observations, "SECRET missing tool input") {
		t.Fatalf("observations leaked missing tool payload: %#v", observations)
	}
}

func TestStreamingOrchestratorRecordsSettlementFailure(t *testing.T) {
	t.Parallel()

	observer := einoobs.New(einoobs.Config{Service: "eino-agent-test"})
	store := newAdmissionStore()
	store.settleToolCallErr = errors.New("SECRET settlement detail")
	orch := newTestOrchestrator(store, scriptedStreamer(func(context.Context, model.Request) ([]*einoschema.Message, error) {
		return []*einoschema.Message{einoschema.AssistantMessage("", []einoschema.ToolCall{{
			ID:   "call-settle",
			Type: "function",
			Function: einoschema.FunctionCall{
				Name:      "echo",
				Arguments: `{}`,
			},
		}})}, nil
	}))
	orch.observer = observer
	configureTestTools(orch, staticToolRegistry{tools: []Tool{{
		Name: "echo",
		Executor: orchestratorToolExecutorFunc(func(context.Context, ToolCall) (ToolResult, error) {
			return ToolResult{Output: "ok"}, nil
		}),
	}}})
	result := startAndWait(t, orch)
	if result.Status != session.RunFailed {
		t.Fatalf("result = %+v", result)
	}
	observations := observer.Snapshot().Observations
	toolSpan := assertObservation(t, observations, "tool_call", "error", "")
	if toolSpan.Error == nil || toolSpan.Error.Classification != "operational_failure" {
		t.Fatalf("tool span error = %#v attrs=%#v", toolSpan.Error, toolSpan.Attributes)
	}
	settled := assertObservation(t, observations, "tool.settled", "error", "")
	if settled.Error == nil || settled.Error.Classification != "operational_failure" {
		t.Fatalf("settled error = %#v attrs=%#v", settled.Error, settled.Attributes)
	}
	if observationContains(observations, "SECRET settlement detail") {
		t.Fatalf("observations leaked settlement error: %#v", observations)
	}
}

func assertObservation(t *testing.T, observations []einoobs.Observation, kind string, status string, traceID string) einoobs.Observation {
	t.Helper()
	for _, observation := range observations {
		if observation.Kind != kind || observation.Status != status {
			continue
		}
		if traceID != "" && observation.TraceID != traceID {
			t.Fatalf("%s trace = %q, want %q", kind, observation.TraceID, traceID)
		}
		return observation
	}
	t.Fatalf("missing observation kind=%s status=%s in %#v", kind, status, observations)
	return einoobs.Observation{}
}

func observationContains(observations []einoobs.Observation, needle string) bool {
	for _, observation := range observations {
		if strings.Contains(observation.Name, needle) || attrsContain(observation.Attributes, needle) {
			return true
		}
		if observation.Error != nil && strings.Contains(observation.Error.Error(), needle) {
			return true
		}
		for _, event := range observation.Events {
			if strings.Contains(event.Name, needle) || attrsContain(event.Attributes, needle) {
				return true
			}
			if event.Error != nil && strings.Contains(event.Error.Error(), needle) {
				return true
			}
		}
	}
	return false
}

func observationsByKind(observations []einoobs.Observation, kind string) []einoobs.Observation {
	var result []einoobs.Observation
	for _, observation := range observations {
		if observation.Kind == kind {
			result = append(result, observation)
		}
	}
	return result
}

type goldenObservation struct {
	Kind                string `json:"kind"`
	Status              string `json:"status"`
	ToolName            string `json:"tool_name,omitempty"`
	ToolKind            string `json:"tool_kind,omitempty"`
	ToolCallID          string `json:"tool_call_id,omitempty"`
	ToolStatus          string `json:"tool_status,omitempty"`
	ErrorClassification string `json:"error_classification,omitempty"`
}

func goldenToolObservations(observations []einoobs.Observation) []goldenObservation {
	kinds := map[string]bool{
		"tool.registered":   true,
		"tool.materialized": true,
		"tool_call":         true,
		"tool.settled":      true,
	}
	result := make([]goldenObservation, 0, len(observations))
	for _, observation := range observations {
		if !kinds[observation.Kind] {
			continue
		}
		item := goldenObservation{
			Kind:                observation.Kind,
			Status:              observation.Status,
			ToolName:            stringAttr(observation.Attributes, "tool.name"),
			ToolKind:            stringAttr(observation.Attributes, "tool.kind"),
			ToolCallID:          stringAttr(observation.Attributes, "tool.call_id"),
			ToolStatus:          stringAttr(observation.Attributes, "tool.status"),
			ErrorClassification: stringAttr(observation.Attributes, "error.classification"),
		}
		result = append(result, item)
	}
	return result
}

func readObservationGolden(t *testing.T, path string) []goldenObservation {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read observation golden: %v", err)
	}
	var result []goldenObservation
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("decode observation golden: %v", err)
	}
	return result
}

func stringAttr(attrs map[string]any, key string) string {
	value, ok := attrs[key]
	if !ok {
		return ""
	}
	text, ok := value.(string)
	if ok {
		return text
	}
	return ""
}

func requireGoldenEqual[T any](t *testing.T, got, want T) {
	t.Helper()
	if reflect.DeepEqual(got, want) {
		return
	}
	gotJSON, _ := json.MarshalIndent(got, "", "  ")
	wantJSON, _ := json.MarshalIndent(want, "", "  ")
	t.Fatalf("golden mismatch\n--- got ---\n%s\n--- want ---\n%s", gotJSON, wantJSON)
}

func attrsContain(attrs map[string]any, needle string) bool {
	for _, value := range attrs {
		if strings.Contains(valueString(value), needle) {
			return true
		}
	}
	return false
}

func valueString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case map[string]string:
		var builder strings.Builder
		for key, item := range v {
			builder.WriteString(key)
			builder.WriteString(item)
		}
		return builder.String()
	default:
		return ""
	}
}
