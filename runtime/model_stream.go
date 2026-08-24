package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

func (o *StreamingOrchestrator) streamModel(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, messages []*einoschema.Message, attempt, step int, usage *model.Usage) (message *einoschema.Message, err error) {
	queue := newEventQueue(ctx, o.QueueSize, execution.eventSink(o.Events))
	defer queue.close()
	obsStream := o.startObservedStream(ctx, snapshot, messageID, attempt)
	var streamUsage model.Usage
	var streamErr error
	var requestRecord *session.ModelRequestRecord
	var requestStore session.ModelRequestStore
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
		if requestRecord != nil {
			state := session.ModelRequestCompleted
			if streamErr != nil {
				state = session.ModelRequestFailed
			}
			if transitionErr := updateModelRequest(ctx, requestStore, requestRecord, state, streamErr, o.now()); transitionErr != nil {
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
			_ = extension.Notify(execution.dispatch(), context.WithoutCancel(ctx), ModelCompletedPoint, ModelCompletedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Attempt: attempt, Step: step, Usage: runtimeUsage(streamUsage), Error: classifyExtensionError(streamErr)})
		}
	}()
	observer := &streamObserver{queue: queue, base: snapshot, messageID: messageID, now: o.now}
	request := snapshot.ProviderRequest(messageID, o.Trace, observer)
	request.Messages = cloneMessages(messages)
	request.System, err = o.renderSystemPrompt(ctx, execution.plan, snapshot, attempt, step)
	if err != nil {
		streamErr = err
		return nil, err
	}
	request, err = request.Clone()
	if err != nil {
		streamErr = err
		return nil, err
	}
	audited, contentHash, err := AuditModelRequest(request, o.ModelRequestSafeOptions, o.ModelRequestMaxBytes)
	if err != nil {
		streamErr = err
		return nil, err
	}
	requestRecord, requestStore, err = o.prepareModelRequest(ctx, execution, snapshot, request, audited, contentHash, messageID, attempt, step)
	if err != nil {
		streamErr = err
		return nil, err
	}
	if requestRecord != nil {
		request.IdempotencyKey = string(requestRecord.ID)
		if err = updateModelRequest(ctx, requestStore, requestRecord, session.ModelRequestDispatchStarted, nil, o.now()); err != nil {
			streamErr = err
			return nil, err
		}
	}
	if execution.dispatch() != nil {
		requestRecordID := session.ModelRequestID("")
		if requestRecord != nil {
			requestRecordID = requestRecord.ID
		}
		_ = extension.Notify(execution.dispatch(), ctx, ModelRequestedPoint, ModelRequestedNotice{SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Attempt: attempt, Step: step, ProviderID: string(request.Identity.ProviderID), ModelID: string(request.Identity.ModelID), RequestRecordID: requestRecordID, MessageCount: len(request.Messages), ToolCount: len(request.Tools), ContentHash: contentHash})
		modelRequested = true
	}
	var reader *einoschema.StreamReader[*einoschema.Message]
	if execution.dispatch() != nil {
		reader, err = extension.Invoke(execution.dispatch(), ctx, ModelStreamPoint, ModelStreamInput{ProviderID: string(request.Identity.ProviderID), ModelID: string(request.Identity.ModelID), Audited: audited, ContentHash: contentHash}, func(ctx context.Context, _ ModelStreamInput) (*einoschema.StreamReader[*einoschema.Message], error) {
			return openStream(ctx, snapshot.Model, request)
		})
	} else {
		reader, err = openStream(ctx, snapshot.Model, request)
	}
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
		chunk, err := reader.Recv()
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
		if chunk == nil {
			streamErr = model.Error{Code: "malformed_provider_stream", Message: "provider returned nil message chunk"}
			return nil, streamErr
		}
		o.observeStreamChunk(obsStream, int64(len(chunks)))
		chunks = append(chunks, chunk)
		if err := queue.emit(Event{
			Kind: EventMessageDelta, SessionID: snapshot.SessionID, RunID: snapshot.RunID,
			MessageID: messageID, EpochID: snapshot.EpochID,
			ProviderID: string(request.Identity.ProviderID), ModelID: string(request.Identity.ModelID),
			Payload:  mustJSON(map[string]string{"content": chunk.Content, "reasoning": chunk.ReasoningContent}),
			LiveOnly: true, Time: o.now(),
		}); err != nil {
			streamErr = err
			return nil, err
		}
	}
	if len(chunks) == 0 {
		streamUsage = observer.usageSnapshot()
		return einoschema.AssistantMessage("", nil), nil
	}
	msg, err := einoschema.ConcatMessages(chunks)
	if err != nil {
		streamErr = model.Error{Code: "malformed_provider_stream", Message: err.Error(), Cause: err}
		return nil, streamErr
	}
	streamUsage = resolveStreamUsage(observer.usageSnapshot(), msg)
	return msg, nil
}

func openStream(ctx context.Context, resolved model.Resolved, request model.Request) (*einoschema.StreamReader[*einoschema.Message], error) {
	if resolved.Streamer != nil {
		if request.IdempotencyKey != "" {
			if streamer, ok := resolved.Streamer.(model.IdempotentStreamer); ok {
				return streamer.StreamProviderWithIdempotencyKey(ctx, request, request.IdempotencyKey)
			}
		}
		return resolved.Streamer.StreamProvider(ctx, request)
	}
	client := resolved.Client
	if client == nil {
		return nil, model.Error{Code: "model_client_missing", Message: "resolved model has no client", Cause: model.ErrProviderUnavailable}
	}
	if len(request.Tools) > 0 {
		withTools, err := client.WithTools(request.Tools)
		if err != nil {
			return nil, err
		}
		client = withTools
	}
	messages := cloneMessages(request.Messages)
	if request.System != "" {
		messages = append([]*einoschema.Message{einoschema.SystemMessage(request.System)}, messages...)
	}
	return client.Stream(ctx, messages, einomodel.WithTools(request.Tools))
}

type streamObserver struct {
	queue     *eventQueue
	base      TurnSnapshot
	messageID session.MessageID
	now       func() time.Time
	mu        sync.Mutex
	usage     model.Usage
}

func (o *streamObserver) OnProviderStart(context.Context, model.Request) {}
func (o *streamObserver) OnProviderDelta(_ context.Context, delta model.StreamDelta) {
	o.setUsage(delta.Usage)
}
func (o *streamObserver) OnProviderError(context.Context, model.Error) {}
func (o *streamObserver) OnProviderEnd(_ context.Context, response model.Response) {
	o.setUsage(response.Usage)
}

func (o *streamObserver) setUsage(usage model.Usage) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.usage = mergeUsage(o.usage, usage)
	o.mu.Unlock()
}

func (o *streamObserver) usageSnapshot() model.Usage {
	if o == nil {
		return model.Usage{}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.usage
}

func resolveStreamUsage(observed model.Usage, msg *einoschema.Message) model.Usage {
	if observed.InputTokens != 0 || observed.OutputTokens != 0 {
		return observed
	}
	if msg != nil && msg.ResponseMeta != nil && msg.ResponseMeta.Usage != nil {
		u := msg.ResponseMeta.Usage
		return model.Usage{InputTokens: int64(u.PromptTokens), OutputTokens: int64(u.CompletionTokens)}
	}
	return observed
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
