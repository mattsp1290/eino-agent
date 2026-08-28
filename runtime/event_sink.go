package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

// runEventSink keeps durability inside the fenced runtime capability. External
// sinks receive transport/observability copies and cannot mutate session state.
type runEventSink struct {
	execution      *runExecution
	infrastructure EventSink
	plan           *extension.Plan
}

func (s runEventSink) Emit(ctx context.Context, event Event) error {
	if !event.LiveOnly && event.Kind != EventRunStarted && event.Kind != EventRunFinished && s.execution.store != nil {
		if event.EventID == "" {
			if s.execution.host == nil || s.execution.host.ids == nil {
				return fmt.Errorf("persist runtime event: event id generator required")
			}
			event.EventID = s.execution.host.ids.NewEventID()
		}
		if _, err := s.execution.store.AppendEvent(ctx, durableEventRecord(event, s.execution.host.now)); err != nil {
			return err
		}
	}
	var err error
	if s.infrastructure != nil {
		err = s.infrastructure.Emit(ctx, cloneEvent(event))
	}
	extension.Notify(s.plan, ctx, EventPublishedPoint, event)
	return err
}

// publishPersisted delivers an event that was already committed as part of a
// state transition. Transport and extension failures cannot roll back durable
// state and are therefore intentionally best-effort.
func (s runEventSink) publishPersisted(ctx context.Context, record session.EventRecord) {
	event := runtimeEventRecord(record)
	if s.infrastructure != nil {
		_ = s.infrastructure.Emit(ctx, cloneEvent(event))
	}
	extension.Notify(s.plan, ctx, EventPublishedPoint, event)
}

func runtimeEventRecord(record session.EventRecord) Event {
	return Event{
		Kind: EventKind(record.Kind), EventID: record.ID, SessionID: record.SessionID, RunID: record.RunID,
		MessageID: record.MessageID, PartID: record.PartID, ToolCallID: record.ToolCallID,
		EpochID: record.EpochID, ProviderID: record.ProviderID, ModelID: record.ModelID,
		ParentID: record.ParentID, Correlation: record.Correlation,
		Usage:     Usage{InputTokens: record.Usage.InputTokens, OutputTokens: record.Usage.OutputTokens, ReasoningTokens: record.Usage.ReasoningTokens, CacheReadTokens: record.Usage.CacheReadTokens, CacheWriteTokens: record.Usage.CacheWriteTokens, Cost: record.Usage.Cost},
		Error:     EventError{Code: record.Error.Code, Message: record.Error.Message, Retryable: record.Error.Retryable},
		Redaction: RedactionClass(record.Redaction), Payload: cloneJSON(record.Payload), LiveOnly: record.LiveOnly, Time: record.CreatedAt,
	}
}

func durableEventRecord(event Event, now func() time.Time) session.EventRecord {
	createdAt := event.Time
	if createdAt.IsZero() {
		createdAt = now().UTC()
	}
	return session.EventRecord{
		ID: event.EventID, SessionID: event.SessionID, RunID: event.RunID,
		MessageID: event.MessageID, PartID: event.PartID, ToolCallID: event.ToolCallID,
		EpochID: event.EpochID, ProviderID: event.ProviderID, ModelID: event.ModelID,
		ParentID: event.ParentID, Kind: string(event.Kind), Correlation: event.Correlation,
		Usage: session.Usage{
			InputTokens: event.Usage.InputTokens, OutputTokens: event.Usage.OutputTokens,
			ReasoningTokens: event.Usage.ReasoningTokens, CacheReadTokens: event.Usage.CacheReadTokens,
			CacheWriteTokens: event.Usage.CacheWriteTokens, Cost: event.Usage.Cost,
		},
		Error: session.EventError{
			Code: event.Error.Code, Message: event.Error.Message, Retryable: event.Error.Retryable,
		},
		Redaction: session.RedactionClass(event.Redaction), Payload: cloneJSON(event.Payload),
		LiveOnly: event.LiveOnly, CreatedAt: createdAt,
	}
}
