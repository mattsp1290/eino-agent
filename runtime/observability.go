package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	einoobs "github.com/mattsp1290/eino-obs"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

type observedRun struct {
	session *einoobs.Session
	run     *einoobs.Run
}

type ObservabilitySink struct {
	Observer *einoobs.Observer
}

func (s ObservabilitySink) Emit(ctx context.Context, event Event) error {
	if s.Observer == nil {
		return nil
	}
	switch event.Kind {
	case EventContextEpochChanged:
		s.Observer.Compaction(ctx, einoobs.CompactionEvent{
			Correlation: eventCorrelation(event, "compaction"),
			Reason:      "context_epoch_changed",
			Time:        event.Time,
			Metadata: einoobs.Metadata{
				"epoch_id": string(event.EpochID),
			},
		})
	case EventRunFinished:
		if event.Error.Code != "" || event.Error.Message != "" {
			s.Observer.Error(ctx, einoobs.ErrorEvent{
				Correlation:    eventCorrelation(event, "error"),
				Operation:      "run",
				Classification: firstNonEmpty(event.Error.Code, "run_error"),
				Err:            observationErrorFromClassification(firstNonEmpty(event.Error.Code, "run_error")),
				Retryable:      event.Error.Retryable,
				Time:           event.Time,
			})
		}
	}
	return nil
}

func (o *StreamingOrchestrator) startObservedRun(ctx context.Context, run session.Run, messageID session.MessageID, start time.Time) observedRun {
	if o == nil || o.Observer == nil {
		return observedRun{}
	}
	corr := o.runCorrelation(run, messageID, "session")
	corr.ObservationID = observationID("session", string(run.SessionID), string(run.ID))
	obsSession := o.Observer.StartSession(ctx, einoobs.SessionStart{
		Correlation: corr,
		Name:        "session",
		StartTime:   start,
		Metadata: einoobs.Metadata{
			"workspace_id": run.Config["workspace_id"],
		},
	})
	runCorr := o.runCorrelation(run, messageID, "run")
	runCorr.ParentObservationID = corr.ObservationID
	obsRun := obsSession.StartRun(einoobs.RunStart{
		Correlation: runCorr,
		Name:        "run",
		StartTime:   start,
		Metadata: einoobs.Metadata{
			"agent":            run.Agent,
			"context_epoch_id": string(run.ContextEpoch),
		},
	})
	return observedRun{session: obsSession, run: obsRun}
}

func (o *StreamingOrchestrator) finishObservedRun(observed observedRun, result Result, end time.Time) {
	if observed.run == nil && observed.session == nil {
		return
	}
	if result.Error != nil || result.Status == session.RunFailed {
		event := einoobs.RunError{
			Err:            observationError(result.Error, "run_failed"),
			Classification: errorClassification(result.Error, "run_failed"),
			Canceled:       result.Interrupted,
			Retryable:      retryable(result.Error),
			Metadata:       einoobs.Metadata{"status": string(result.Status)},
		}
		observed.run.Error(event)
		observed.session.Error(einoobs.SessionError{Err: event.Err, Classification: event.Classification, Canceled: result.Interrupted})
		return
	}
	if result.Interrupted || result.Status == session.RunInterrupted {
		err := observationError(result.Error, "interrupted")
		observed.run.Error(einoobs.RunError{Err: err, Classification: "interrupted", Canceled: true, Metadata: einoobs.Metadata{"status": string(result.Status)}})
		observed.session.Error(einoobs.SessionError{Err: err, Classification: "interrupted", Canceled: true})
		return
	}
	observed.run.End(einoobs.RunEnd{EndTime: end, Metadata: einoobs.Metadata{"status": string(result.Status)}})
	observed.session.End(einoobs.SessionEnd{EndTime: end})
}

func (o *StreamingOrchestrator) startObservedStream(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, attempt int) *einoobs.Stream {
	if o == nil || o.Observer == nil {
		return nil
	}
	corr := o.snapshotCorrelation(snapshot, messageID, "stream")
	corr.ObservationID = observationID("stream", string(snapshot.RunID), string(messageID), fmt.Sprintf("attempt-%d", attempt))
	return o.Observer.StartStream(ctx, einoobs.StreamStart{
		Correlation: corr,
		ProviderModel: einoobs.ProviderModel{
			Provider: string(snapshot.Model.Provider.ID),
			Model:    string(snapshot.Model.Model.ID),
		},
		Name:         "model.stream",
		StartTime:    o.now(),
		RetryAttempt: int64(attempt),
		Metadata: einoobs.Metadata{
			"agent":            snapshot.Config.Agent.Name,
			"context_epoch_id": string(snapshot.EpochID),
		},
	})
}

func (o *StreamingOrchestrator) observeRetry(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, attempt int, attempts int, err error) {
	if o == nil || o.Observer == nil {
		return
	}
	o.Observer.Retry(ctx, einoobs.RetryEvent{
		Correlation:    o.snapshotCorrelation(snapshot, messageID, "retry"),
		Attempt:        int64(attempt),
		MaxAttempts:    int64(attempts),
		Classification: errorClassification(err, "retryable"),
		Reason:         "provider_retry",
		Time:           o.now(),
		Metadata: einoobs.Metadata{
			"error": safeErrorMessage(err),
		},
	})
}

func (o *StreamingOrchestrator) observeError(ctx context.Context, snapshot TurnSnapshot, messageID session.MessageID, operation string, err error) {
	if o == nil || o.Observer == nil || err == nil {
		return
	}
	if errors.Is(err, context.Canceled) {
		o.Observer.Cancellation(ctx, einoobs.CancellationEvent{
			Correlation:    o.snapshotCorrelation(snapshot, messageID, "cancellation"),
			Operation:      operation,
			Classification: "canceled",
			Reason:         "context_canceled",
			Err:            observationError(err, "canceled"),
			Time:           o.now(),
		})
		return
	}
	o.Observer.Error(ctx, einoobs.ErrorEvent{
		Correlation:    o.snapshotCorrelation(snapshot, messageID, "error"),
		Operation:      operation,
		Classification: errorClassification(err, "error"),
		Err:            observationError(err, "error"),
		Retryable:      retryable(err),
		Time:           o.now(),
	})
}

func (o *StreamingOrchestrator) observeInterrupt(ctx context.Context, run session.Run, messageID session.MessageID, reason string) {
	if o == nil || o.Observer == nil {
		return
	}
	o.Observer.Interrupt(ctx, einoobs.InterruptEvent{
		Correlation: o.runCorrelation(run, messageID, "interrupt"),
		Reason:      reason,
		Status:      "requested",
		Time:        o.now(),
	})
}

func (o *StreamingOrchestrator) observeResume(ctx context.Context, run session.Run, reason string) {
	if o == nil || o.Observer == nil {
		return
	}
	o.Observer.Resume(ctx, einoobs.ResumeEvent{
		Correlation: o.runCorrelation(run, "", "resume"),
		Reason:      reason,
		Status:      "started",
		Time:        o.now(),
	})
}

func (o *StreamingOrchestrator) observeToolsResolved(ctx context.Context, snapshot TurnSnapshot, tools []Tool) {
	if o == nil || o.Observer == nil {
		return
	}
	for _, tool := range tools {
		corr := o.snapshotCorrelation(snapshot, "", "tool.registered")
		corr.ObservationID = observationID("tool.registered", string(snapshot.RunID), tool.Name)
		o.Observer.ToolRegistered(ctx, einoobs.ToolRegistered{
			Correlation: corr,
			ToolName:    tool.Name,
			ToolKind:    "server",
			Time:        o.now(),
			Metadata: einoobs.Metadata{
				"source": tool.Metadata["source"],
			},
		})
	}
}

func (o *StreamingOrchestrator) observeToolMaterialized(ctx context.Context, snapshot TurnSnapshot, tool Tool, call ToolCall) {
	if o == nil || o.Observer == nil {
		return
	}
	o.Observer.ToolMaterialized(ctx, einoobs.ToolMaterialized{
		Correlation: o.toolCorrelation(snapshot, call, "tool.materialized"),
		ToolCallID:  string(call.ID),
		ToolName:    tool.Name,
		ToolKind:    "server",
		Time:        o.now(),
	})
}

func (o *StreamingOrchestrator) startObservedToolCall(ctx context.Context, snapshot TurnSnapshot, tool Tool, call ToolCall) *einoobs.ToolCall {
	if o == nil || o.Observer == nil {
		return nil
	}
	return o.Observer.StartToolCall(ctx, einoobs.ToolCallStart{
		Correlation: o.toolCorrelation(snapshot, call, "tool.call"),
		ToolCallID:  string(call.ID),
		ToolName:    tool.Name,
		ToolKind:    "server",
		StartTime:   o.now(),
	})
}

func (o *StreamingOrchestrator) finishObservedToolCall(observed *einoobs.ToolCall, status session.ToolCallStatus, err error, metadata map[string]string) {
	if observed == nil {
		return
	}
	classification := toolClassification(status, err, metadata)
	if status == session.ToolCallCompleted && classification == "" {
		observed.End(einoobs.ToolCallEnd{EndTime: o.now(), Metadata: toolMetadata(metadata)})
		return
	}
	observed.Error(einoobs.ToolCallError{
		Err:            observationErrorFromClassification(firstNonEmpty(classification, string(status), "tool_error")),
		Classification: firstNonEmpty(classification, string(status), "tool_error"),
		Canceled:       toolCanceled(status, classification),
		Retryable:      false,
		EndTime:        o.now(),
		Metadata:       toolMetadata(metadata),
	})
}

func (o *StreamingOrchestrator) observeToolSettled(ctx context.Context, snapshot TurnSnapshot, tool Tool, call ToolCall, status session.ToolCallStatus, latency time.Duration, err error, metadata map[string]string) {
	if o == nil || o.Observer == nil {
		return
	}
	classification := toolClassification(status, err, metadata)
	event := einoobs.ToolSettled{
		Correlation:    o.toolCorrelation(snapshot, call, "tool.settled"),
		ToolCallID:     string(call.ID),
		ToolName:       tool.Name,
		ToolKind:       "server",
		Status:         settledToolStatus(status, classification),
		Time:           o.now(),
		Latency:        latency,
		LatencyKnown:   latency != 0,
		Classification: classification,
		Retryable:      false,
		Metadata:       toolMetadata(metadata),
	}
	if classification != "" {
		event.Error = einoobs.ObservationError{
			Operation:      "tool_call",
			Classification: classification,
			Err:            observationErrorFromClassification(classification),
			Retryable:      false,
		}
	}
	o.Observer.ToolSettled(ctx, event)
}

func (o *StreamingOrchestrator) observeStreamChunk(stream *einoobs.Stream, index int64) {
	if stream == nil {
		return
	}
	now := o.now()
	if index == 0 {
		stream.FirstToken(einoobs.StreamFirstToken{Time: now})
	}
	stream.Chunk(einoobs.StreamChunk{Index: index, Time: now})
}

func (o *StreamingOrchestrator) endObservedStream(stream *einoobs.Stream, usage model.Usage) {
	if stream == nil {
		return
	}
	stream.End(einoobs.StreamEnd{EndTime: o.now(), Usage: tokenUsage(usage)})
}

func (o *StreamingOrchestrator) errorObservedStream(stream *einoobs.Stream, err error, usage model.Usage) {
	if stream == nil || err == nil {
		return
	}
	stream.Error(einoobs.StreamError{
		Err:            observationError(err, "stream_error"),
		Classification: errorClassification(err, "stream_error"),
		Canceled:       errors.Is(err, context.Canceled),
		Retryable:      retryable(err),
		EndTime:        o.now(),
		PartialUsage:   tokenUsage(usage),
	})
}

func (o *StreamingOrchestrator) runCorrelation(run session.Run, messageID session.MessageID, kind string) einoobs.Correlation {
	return einoobs.Correlation{
		SessionID:          string(run.SessionID),
		RunID:              string(run.ID),
		AgentID:            run.Agent,
		AssistantMessageID: string(messageID),
		Provider:           run.ProviderID,
		Model:              run.ModelID,
		TraceID:            o.Trace.TraceID,
		ObservationID:      observationID(kind, string(run.ID)),
	}
}

func (o *StreamingOrchestrator) snapshotCorrelation(snapshot TurnSnapshot, messageID session.MessageID, kind string) einoobs.Correlation {
	return einoobs.Correlation{
		SessionID:           string(snapshot.SessionID),
		RunID:               string(snapshot.RunID),
		AgentID:             snapshot.Config.Agent.Name,
		AssistantMessageID:  string(messageID),
		Provider:            string(snapshot.Model.Provider.ID),
		Model:               string(snapshot.Model.Model.ID),
		TraceID:             o.Trace.TraceID,
		ObservationID:       observationID(kind, string(snapshot.RunID), string(messageID)),
		ParentObservationID: observationID("run", string(snapshot.RunID)),
	}
}

func (o *StreamingOrchestrator) toolCorrelation(snapshot TurnSnapshot, call ToolCall, kind string) einoobs.Correlation {
	corr := o.snapshotCorrelation(snapshot, call.MessageID, kind)
	corr.ToolCallID = string(call.ID)
	corr.ObservationID = observationID(kind, string(snapshot.RunID), string(call.MessageID), string(call.ID))
	return corr
}

func observationID(parts ...string) string {
	kept := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			kept = append(kept, part)
		}
	}
	return strings.Join(kept, ":")
}

func eventCorrelation(event Event, kind string) einoobs.Correlation {
	return einoobs.Correlation{
		SessionID:           string(event.SessionID),
		RunID:               string(event.RunID),
		AssistantMessageID:  string(event.MessageID),
		ToolCallID:          string(event.ToolCallID),
		Provider:            event.ProviderID,
		Model:               event.ModelID,
		TraceID:             event.Correlation,
		ObservationID:       observationID(kind, string(event.RunID), string(event.MessageID), string(event.EpochID), string(event.ToolCallID)),
		ParentObservationID: observationID("run", string(event.RunID)),
	}
}

func tokenUsage(usage model.Usage) einoobs.TokenUsage {
	return einoobs.TokenUsage{
		InputTokens:            usage.InputTokens,
		OutputTokens:           usage.OutputTokens,
		ReasoningTokens:        usage.ReasoningTokens,
		CachedInputTokens:      usage.CacheReadTokens,
		InputTokensKnown:       usage.InputTokens != 0,
		OutputTokensKnown:      usage.OutputTokens != 0,
		ReasoningTokensKnown:   usage.ReasoningTokens != 0,
		CachedInputTokensKnown: usage.CacheReadTokens != 0,
	}
}

func toolClassification(status session.ToolCallStatus, err error, metadata map[string]string) string {
	if value := metadata["permission_status"]; value != "" {
		if value == "interrupted" {
			return "interrupted"
		}
		return "permission_" + value
	}
	if status == session.ToolCallInterrupted || errors.Is(err, context.Canceled) {
		return "interrupted"
	}
	if status == session.ToolCallFailed {
		return "operational_failure"
	}
	return ""
}

func settledToolStatus(status session.ToolCallStatus, classification string) string {
	switch {
	case toolCanceled(status, classification):
		return "canceled"
	case status == session.ToolCallFailed || classification != "":
		return "failed"
	default:
		return "succeeded"
	}
}

func toolCanceled(status session.ToolCallStatus, classification string) bool {
	return status == session.ToolCallInterrupted || classification == "interrupted"
}

func toolMetadata(metadata map[string]string) einoobs.Metadata {
	result := einoobs.Metadata{}
	if value := metadata["permission_status"]; value != "" {
		result["permission_status"] = value
	}
	return result
}

func errorClassification(err error, fallback string) string {
	if err == nil {
		return fallback
	}
	var providerErr model.Error
	if errors.As(err, &providerErr) && providerErr.Code != "" {
		return providerErr.Code
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return fallback
}

func observationError(err error, fallback string) error {
	return observationErrorFromClassification(errorClassification(err, fallback))
}

func observationErrorFromClassification(classification string) error {
	if classification == "" {
		classification = "error"
	}
	return errors.New(classification)
}

func safeErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return errorClassification(err, "error")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
