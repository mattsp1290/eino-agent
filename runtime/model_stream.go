package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	einoschema "github.com/cloudwego/eino/schema"
	einoobs "github.com/mattsp1290/eino-obs"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/watch"
)

type modelStreamResult struct {
	message       *einoschema.AgenticMessage
	usage         model.Usage
	receivedDelta bool
	err           error
}

const (
	providerStreamPanicCode    = "provider_stream_panic"
	providerStreamPanicMessage = "provider stream failed"
)

func newProviderStreamPanicError() error {
	return model.Error{Code: providerStreamPanicCode, Message: providerStreamPanicMessage}
}

func streamLimitError(reason string) error {
	return model.Error{Code: "stream_limit_exceeded", Message: reason, Cause: model.ErrProviderRejected}
}

type modelStreamReader interface {
	Recv() (model.StreamDelta, error)
	Close()
}

type modelStreamAttempt struct {
	live        watch.LiveIdentity
	execution   *runExecution
	snapshot    TurnSnapshot
	messageID   session.MessageID
	attempt     int
	step        int
	observation *einoobs.Stream
	record      session.ModelRequestRecord
	providerID  string
	modelID     string
}

func (o *StreamingOrchestrator) streamModel(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, messages []*einoschema.AgenticMessage, attempt, step int, usage *model.Usage) (result modelStreamResult) {
	state := modelStreamAttempt{
		execution: execution, snapshot: snapshot, messageID: messageID, attempt: attempt, step: step,
		observation: o.startObservedStream(ctx, snapshot, messageID, attempt),
	}
	defer func() {
		if recover() != nil {
			result.message = nil
			result.err = newProviderStreamPanicError()
		}
		if usage != nil {
			*usage = addUsage(*usage, result.usage)
		}
		o.sessionObserver.FinishAttempt(state.live)
		state.finalize(ctx, o, &result)
	}()
	request := snapshot.ProviderRequest(messageID, o.trace, messages, execution.discoveredSnapshot())
	state.providerID, state.modelID = string(request.Identity.ProviderID), string(request.Identity.ModelID)
	request.System, result.err = o.renderSystemPrompt(ctx, execution.plan, snapshot, attempt, step)
	if result.err != nil {
		return result
	}
	var audited AuditedModelInput
	var contentHash string
	request, audited, contentHash, result.err = auditModelRequest(request, o.modelRequestSafeOptions, o.modelRequestMaxBytes)
	if result.err != nil {
		return result
	}
	state.record, result.err = o.prepareModelRequest(ctx, execution, snapshot, request, audited, contentHash, messageID, modelRequestIdentity{
		InvocationID: o.ids.NewInvocationID(), Attempt: attempt, Step: step,
	})
	if result.err != nil {
		return result
	}
	state.live = o.sessionObserver.BeginAttempt(watch.LiveIdentity{SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, RequestID: state.record.ID, Attempt: attempt, Step: step})
	request.IdempotencyKey = string(state.record.ID)
	result.err = updateModelRequest(ctx, execution.store, &state.record, session.ModelRequestDispatchStarted, nil, o.now())
	if result.err != nil {
		return result
	}
	extension.Notify(execution.dispatch(), ctx, ModelRequestedPoint, ModelRequestedNotice{
		SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Attempt: attempt, Step: step,
		ProviderID: state.providerID, ModelID: state.modelID, RequestRecordID: state.record.ID,
		MessageCount: len(request.Messages), ToolCount: len(request.Controls.Tools), ContentHash: contentHash,
	})
	reader, invokeErr := extension.InvokeAround(execution.dispatch(), ctx, ModelStreamPoint, ModelStreamInput{
		ProviderID: state.providerID, ModelID: state.modelID, Audited: audited, ContentHash: contentHash,
	}, func(ctx context.Context) (*einoschema.StreamReader[model.StreamDelta], error) {
		return snapshot.Model.Streamer.StreamProvider(ctx, request)
	})
	if invokeErr != nil {
		result.err = invokeErr
		if reader != nil {
			reader.Close()
		}
		return result
	}
	if reader == nil {
		result.err = model.Error{Code: "nil_provider_stream", Message: "provider returned nil stream"}
		return result
	}
	receiveModelStream(ctx, reader, o.streamLimits, &result, func(index int64, message *einoschema.AgenticMessage) {
		state.observeDelta(ctx, o, index, message)
	})
	return result
}

func (a *modelStreamAttempt) observeDelta(ctx context.Context, host *StreamingOrchestrator, index int64, message *einoschema.AgenticMessage) {
	content, reasoning := deltaText(message)
	if content != "" {
		host.sessionObserver.AppendText(a.live, content)
	}
	host.observeStreamChunk(a.observation, index)
	a.execution.eventSink().Emit(ctx, session.EventRecord{
		Kind: EventMessageDelta, SessionID: a.snapshot.SessionID, RunID: a.snapshot.RunID,
		MessageID: a.messageID, EpochID: a.snapshot.EpochID, ProviderID: a.providerID, ModelID: a.modelID,
		Payload:  mustJSON(map[string]string{"content": content, "reasoning": reasoning}),
		LiveOnly: true, CreatedAt: host.now(),
	})
}

// deltaText extracts the live-visible text of one streamed agentic chunk:
// the concatenation of every assistant_gen_text block's Text as content, and
// every reasoning block's Text as reasoning.
func deltaText(message *einoschema.AgenticMessage) (content, reasoning string) {
	if message == nil {
		return "", ""
	}
	for _, block := range message.ContentBlocks {
		if block == nil {
			continue
		}
		switch block.Type {
		case einoschema.ContentBlockTypeAssistantGenText:
			if block.AssistantGenText != nil {
				content += block.AssistantGenText.Text
			}
		case einoschema.ContentBlockTypeReasoning:
			if block.Reasoning != nil {
				reasoning += block.Reasoning.Text
			}
		}
	}
	return content, reasoning
}

func (a *modelStreamAttempt) finalize(ctx context.Context, host *StreamingOrchestrator, result *modelStreamResult) {
	switch a.record.State {
	case session.ModelRequestPrepared:
		if err := updateModelRequest(ctx, a.execution.store, &a.record, session.ModelRequestFailed, result.err, host.now()); err != nil {
			result.message, result.err = nil, err
		}
	case session.ModelRequestDispatchStarted:
		state := session.ModelRequestCompleted
		if result.err != nil {
			state = session.ModelRequestFailed
		}
		if err := updateModelRequest(ctx, a.execution.store, &a.record, state, result.err, host.now()); err != nil {
			result.message, result.err = nil, err
			host.errorObservedStream(a.observation, result.err, result.usage)
			return
		}
		a.observe(host, result)
		extension.Notify(a.execution.dispatch(), context.WithoutCancel(ctx), ModelCompletedPoint, ModelCompletedNotice{
			SessionID: a.snapshot.SessionID, RunID: a.snapshot.RunID, MessageID: a.messageID,
			Attempt: a.attempt, Step: a.step, Usage: runtimeUsage(result.usage), Error: classifyExtensionError(result.err),
		})
		return
	}
	a.observe(host, result)
}

func (a *modelStreamAttempt) observe(host *StreamingOrchestrator, result *modelStreamResult) {
	if result.err != nil {
		host.errorObservedStream(a.observation, result.err, result.usage)
		return
	}
	host.endObservedStream(a.observation, result.usage)
}

// receiveModelStream drains reader to completion, accumulating chunks under
// the configured StreamLimits. reader.Close is called exactly once, via
// defer, regardless of which return path is taken (including a panic
// unwinding through this frame): Eino's StreamReader.Close is single-use, so
// no other path in this function may call it.
func receiveModelStream(ctx context.Context, reader modelStreamReader, limits StreamLimits, result *modelStreamResult, onDelta func(int64, *einoschema.AgenticMessage)) {
	defer reader.Close()
	var chunks []*einoschema.AgenticMessage
	totalBytes := 0
	for {
		if err := ctx.Err(); err != nil {
			result.err = err
			return
		}
		delta, err := reader.Recv()
		result.usage = mergeUsage(result.usage, delta.Usage)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			result.err = err
			return
		}
		if err := ctx.Err(); err != nil {
			result.err = err
			return
		}
		if delta.Message == nil {
			result.err = model.Error{Code: "malformed_provider_stream", Message: "provider returned nil message chunk"}
			return
		}
		if len(chunks) >= limits.MaxChunks {
			result.err = streamLimitError("provider stream exceeded the maximum chunk count")
			return
		}
		encoded, marshalErr := json.Marshal(delta.Message)
		if marshalErr != nil {
			result.err = model.Error{Code: "malformed_provider_stream", Message: marshalErr.Error(), Cause: marshalErr}
			return
		}
		totalBytes += len(encoded)
		if totalBytes > limits.MaxBytes {
			result.err = streamLimitError("provider stream exceeded the maximum byte budget")
			return
		}
		result.receivedDelta = true
		if onDelta != nil {
			onDelta(int64(len(chunks)), delta.Message)
		}
		chunks = append(chunks, delta.Message)
	}
	if len(chunks) == 0 {
		result.message = &einoschema.AgenticMessage{Role: einoschema.AgenticRoleTypeAssistant}
		return
	}
	message, err := einoschema.ConcatAgenticMessages(chunks)
	if err != nil {
		result.err = model.Error{Code: "malformed_provider_stream", Message: err.Error(), Cause: err}
		return
	}
	// ConcatAgenticMessages short-circuits and returns the single chunk
	// as-is when len(chunks) == 1, without clearing StreamingMeta the way
	// its multi-chunk path does. The finalized message must never carry
	// streaming metadata (model.Request.Clone and session.ContentFromAgenticMessage
	// both reject it), so strip it defensively regardless of chunk count.
	clearStreamingMeta(message)
	result.message = message
	result.usage = resolveStreamUsage(result.usage, message)
}

// clearStreamingMeta nils every content block's StreamingMeta on msg.
func clearStreamingMeta(msg *einoschema.AgenticMessage) {
	if msg == nil {
		return
	}
	for _, block := range msg.ContentBlocks {
		if block != nil {
			block.StreamingMeta = nil
		}
	}
}

func resolveStreamUsage(observed model.Usage, msg *einoschema.AgenticMessage) model.Usage {
	return mergeUsage(model.UsageFromAgenticMessage(msg), observed)
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
