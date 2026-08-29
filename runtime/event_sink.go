package runtime

import (
	"context"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/session"
)

// runEventSink fans out transport and observability copies. Durable records are
// committed by the state transition that owns them before reaching this sink.
type runEventSink struct {
	infrastructure EventSink
	plan           *extension.Plan
}

func (s runEventSink) Emit(ctx context.Context, event Event) error {
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
func (s runEventSink) publishPersisted(infrastructureCtx, notificationCtx context.Context, record session.EventRecord) {
	event := runtimeEventRecord(record)
	if s.infrastructure != nil {
		_ = s.infrastructure.Emit(infrastructureCtx, cloneEvent(event))
	}
	extension.Notify(s.plan, notificationCtx, EventPublishedPoint, event)
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
