package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
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
