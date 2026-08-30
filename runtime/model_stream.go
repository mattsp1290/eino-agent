package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"

	einoschema "github.com/cloudwego/eino/schema"
	einoobs "github.com/mattsp1290/eino-obs"

	"github.com/mattsp1290/eino-agent/extension"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/session"
)

type modelStreamResult struct {
	message       *einoschema.Message
	usage         model.Usage
	receivedDelta bool
	err           error
}

type modelStreamReader interface {
	Recv() (model.StreamDelta, error)
	Close()
}

type modelStreamAttempt struct {
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

func (o *StreamingOrchestrator) streamModel(ctx context.Context, execution *runExecution, snapshot TurnSnapshot, messageID session.MessageID, messages []*einoschema.Message, attempt, step int, usage *model.Usage) (result modelStreamResult) {
	state := modelStreamAttempt{
		execution: execution, snapshot: snapshot, messageID: messageID, attempt: attempt, step: step,
		observation: o.startObservedStream(ctx, snapshot, messageID, attempt),
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			result.message = nil
			result.err = fmt.Errorf("provider stream panic: %v", recovered)
		}
		if usage != nil {
			*usage = addUsage(*usage, result.usage)
		}
		state.finalize(ctx, o, &result)
	}()
	request := snapshot.ProviderRequest(messageID, o.trace, messages)
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
	state.record, result.err = o.prepareModelRequest(ctx, execution, snapshot, request, audited, contentHash, messageID, attempt, step)
	if result.err != nil {
		return result
	}
	request.IdempotencyKey = string(state.record.ID)
	result.err = updateModelRequest(ctx, execution.store, &state.record, session.ModelRequestDispatchStarted, nil, o.now())
	if result.err != nil {
		return result
	}
	extension.Notify(execution.dispatch(), ctx, ModelRequestedPoint, ModelRequestedNotice{
		SessionID: snapshot.SessionID, RunID: snapshot.RunID, MessageID: messageID, Attempt: attempt, Step: step,
		ProviderID: state.providerID, ModelID: state.modelID, RequestRecordID: state.record.ID,
		MessageCount: len(request.Messages), ToolCount: len(request.Tools), ContentHash: contentHash,
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
	receiveModelStream(ctx, reader, &result, func(index int64, message *einoschema.Message) {
		state.observeDelta(ctx, o, index, message)
	})
	return result
}

func (a *modelStreamAttempt) observeDelta(ctx context.Context, host *StreamingOrchestrator, index int64, message *einoschema.Message) {
	host.observeStreamChunk(a.observation, index)
	a.execution.eventSink().Emit(ctx, session.EventRecord{
		Kind: EventMessageDelta, SessionID: a.snapshot.SessionID, RunID: a.snapshot.RunID,
		MessageID: a.messageID, EpochID: a.snapshot.EpochID, ProviderID: a.providerID, ModelID: a.modelID,
		Payload:  mustJSON(map[string]string{"content": message.Content, "reasoning": message.ReasoningContent}),
		LiveOnly: true, CreatedAt: host.now(),
	})
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

func receiveModelStream(ctx context.Context, reader modelStreamReader, result *modelStreamResult, onDelta func(int64, *einoschema.Message)) {
	defer reader.Close()
	var chunks []*einoschema.Message
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
		result.receivedDelta = true
		if onDelta != nil {
			onDelta(int64(len(chunks)), delta.Message)
		}
		chunks = append(chunks, delta.Message)
	}
	if len(chunks) == 0 {
		result.message = einoschema.AssistantMessage("", nil)
		return
	}
	message, err := einoschema.ConcatMessages(chunks)
	if err != nil {
		result.err = model.Error{Code: "malformed_provider_stream", Message: err.Error(), Cause: err}
		return
	}
	result.message = message
	result.usage = resolveStreamUsage(result.usage, message)
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
