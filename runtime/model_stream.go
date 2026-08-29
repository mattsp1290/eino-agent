package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func (o *StreamingOrchestrator) streamModel(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, messages []*einoschema.Message, attempt, step int, usage *model.Usage) (message *einoschema.Message, err error) {
	queue := newEventQueue(ctx, o.queueSize, execution.eventSink(o.events))
	defer queue.close()
	obsStream := o.startObservedStream(ctx, snapshot, messageID, attempt)
	var streamUsage model.Usage
	var streamErr error
	var requestRecord session.ModelRequestRecord
	var modelRequested bool
	defer func() {
		if recovered := recover(); recovered != nil {
			streamErr = fmt.Errorf("provider stream panic: %v", recovered)
			err = streamErr
			message = nil
		}
		if usage != nil {
			*usage = addUsage(*usage, streamUsage)
		}
		ledgerTransitionOK := true
		if requestRecord.ID != "" {
			state := session.ModelRequestCompleted
			if streamErr != nil {
				state = session.ModelRequestFailed
			}
			if transitionErr := updateModelRequest(ctx, execution.store, &requestRecord, state, streamErr, o.now()); transitionErr != nil {
				streamErr = transitionErr
				err = transitionErr
				message = nil
				ledgerTransitionOK = false
			}
		}
		if streamErr != nil {
			o.errorObservedStream(obsStream, streamErr, streamUsage)
		} else {
			o.endObservedStream(obsStream, streamUsage)
		}
		if modelRequested && ledgerTransitionOK {
			extension.Notify(execution.dispatch(), context.WithoutCancel(ctx), ModelCompletedPoint, ModelCompletedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Attempt: attempt, Step: step, Usage: runtimeUsage(streamUsage), Error: classifyExtensionError(streamErr)})
		}
	}()
	request := snapshot.ProviderRequest(messageID, o.trace, messages)
	request.System, err = o.renderSystemPrompt(ctx, execution.plan, snapshot, attempt, step)
	if err != nil {
		streamErr = err
		return nil, err
	}
	request, audited, contentHash, err := auditModelRequest(request, o.modelRequestSafeOptions, o.modelRequestMaxBytes)
	if err != nil {
		streamErr = err
		return nil, err
	}
	requestRecord, err = o.prepareModelRequest(ctx, execution, snapshot, request, audited, contentHash, messageID, attempt, step)
	if err != nil {
		streamErr = err
		return nil, err
	}
	request.IdempotencyKey = string(requestRecord.ID)
	if err = updateModelRequest(ctx, execution.store, &requestRecord, session.ModelRequestDispatchStarted, nil, o.now()); err != nil {
		streamErr = err
		return nil, err
	}
	extension.Notify(execution.dispatch(), ctx, ModelRequestedPoint, ModelRequestedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Attempt: attempt, Step: step, ProviderID: string(request.Identity.ProviderID), ModelID: string(request.Identity.ModelID), RequestRecordID: requestRecord.ID, MessageCount: len(request.Messages), ToolCount: len(request.Tools), ContentHash: contentHash})
	modelRequested = true
	reader, err := extension.InvokeAround(execution.dispatch(), ctx, ModelStreamPoint, ModelStreamInput{ProviderID: string(request.Identity.ProviderID), ModelID: string(request.Identity.ModelID), Audited: audited, ContentHash: contentHash}, func(ctx context.Context, _ ModelStreamInput) (*einoschema.StreamReader[model.StreamDelta], error) {
		return snapshot.Model.Streamer.StreamProvider(ctx, request)
	})
	if err != nil {
		streamErr = err
		return nil, err
	}
	if reader == nil {
		streamErr = model.Error{Code: "nil_provider_stream", Message: "provider returned nil stream"}
		return nil, streamErr
	}
	defer reader.Close()
	var chunks []*einoschema.Message
	for {
		if err := ctx.Err(); err != nil {
			streamErr = err
			return nil, err
		}
		delta, err := reader.Recv()
		streamUsage = mergeUsage(streamUsage, delta.Usage)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			streamErr = err
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			streamErr = err
			return nil, err
		}
		if delta.Message == nil {
			streamErr = model.Error{Code: "malformed_provider_stream", Message: "provider returned nil message chunk"}
			return nil, streamErr
		}
		o.observeStreamChunk(obsStream, int64(len(chunks)))
		chunks = append(chunks, delta.Message)
		if err := queue.emit(Event{
			Kind: EventMessageDelta, SessionID: snapshot.SessionID, RunID: snapshot.RunID,
			MessageID: messageID, EpochID: snapshot.EpochID,
			ProviderID: string(request.Identity.ProviderID), ModelID: string(request.Identity.ModelID),
			Payload:  mustJSON(map[string]string{"content": delta.Message.Content, "reasoning": delta.Message.ReasoningContent}),
			LiveOnly: true, Time: o.now(),
		}); err != nil {
			streamErr = err
			return nil, err
		}
	}
	if len(chunks) == 0 {
		return einoschema.AssistantMessage("", nil), nil
	}
	msg, err := einoschema.ConcatMessages(chunks)
	if err != nil {
		streamErr = model.Error{Code: "malformed_provider_stream", Message: err.Error(), Cause: err}
		return nil, streamErr
	}
	streamUsage = resolveStreamUsage(streamUsage, msg)
	return msg, nil
}

func resolveStreamUsage(observed model.Usage, msg *einoschema.Message) model.Usage {
	return mergeUsage(model.UsageFromMessage(msg), observed)
}

func mergeUsage(current model.Usage, next model.Usage) model.Usage {
	if next.InputTokens != 0 {
		current.InputTokens = next.InputTokens
	}
	if next.OutputTokens != 0 {
		current.OutputTokens = next.OutputTokens
	}
	if next.ReasoningTokens != 0 {
		current.ReasoningTokens = next.ReasoningTokens
	}
	if next.CacheReadTokens != 0 {
		current.CacheReadTokens = next.CacheReadTokens
	}
	if next.CacheWriteTokens != 0 {
		current.CacheWriteTokens = next.CacheWriteTokens
	}
	if next.Cost != 0 {
		current.Cost = next.Cost
	}
	return current
}
