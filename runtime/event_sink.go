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
			if s.execution.host == nil || s.execution.host.IDs == nil {
				return fmt.Errorf("persist runtime event: event id generator required")
			}
			event.EventID = s.execution.host.IDs.NewEventID()
		}
		if _, err := s.execution.store.AppendEvent(ctx, durableEventRecord(event, s.execution.host.now)); err != nil {
			return err
		}
	}
	var err error
	if s.infrastructure != nil {
		err = s.infrastructure.Emit(ctx, cloneEvent(event))
	}
	_ = extension.Notify(s.plan, ctx, EventPublishedPoint, event)
	return err
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
