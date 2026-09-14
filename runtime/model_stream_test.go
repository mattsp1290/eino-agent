package runtime

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	einomodel "github.com/cloudwego/eino/components/model"
	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
)

type testModelStreamReader struct {
	chunks     []model.StreamDelta
	index      int
	panicAt    int
	panicValue any
	closePanic bool
	closes     int
}

// nativeAdapterStreamModel provides native Eino streams so these tests cross
// model.NewAgenticStreamer rather than testing receiveModelStream with a
// hand-written model.StreamReader.
type nativeAdapterStreamModel struct {
	chunks    []*einoschema.AgenticMessage
	streamErr error
}

func (m *nativeAdapterStreamModel) Generate(context.Context, []*einoschema.AgenticMessage, ...einomodel.Option) (*einoschema.AgenticMessage, error) {
	return nil, errors.New("unused")
}

func (m *nativeAdapterStreamModel) Stream(context.Context, []*einoschema.AgenticMessage, ...einomodel.Option) (*einoschema.StreamReader[*einoschema.AgenticMessage], error) {
	if m.streamErr == nil {
		return einoschema.StreamReaderFromArray(m.chunks), nil
	}
	reader, writer := einoschema.Pipe[*einoschema.AgenticMessage](1)
	go func() {
		defer writer.Close()
		writer.Send(nil, m.streamErr)
	}()
	return reader, nil
}

func (r *testModelStreamReader) Recv() (model.StreamDelta, error) {
	if r.index == r.panicAt {
		panic(r.panicValue)
	}
	if r.index == len(r.chunks) {
		return model.StreamDelta{}, io.EOF
	}
	chunk := r.chunks[r.index]
	r.index++
	return chunk, nil
}

func (r *testModelStreamReader) Close() {
	r.closes++
	if r.closePanic {
		panic(r.panicValue)
	}
}

func TestReceiveModelStreamPreservesPartialStateAcrossPanic(t *testing.T) {
	const secret = "provider-secret-receive-value"
	reader := &testModelStreamReader{
		chunks:     []model.StreamDelta{{Message: agenticAssistantText("first"), Usage: model.Usage{InputTokens: 3, OutputTokens: 1}}},
		panicAt:    1,
		panicValue: secret,
	}
	var result modelStreamResult
	var indexes []int64
	func() {
		defer func() {
			if recover() != nil {
				result.message = nil
				result.err = newProviderStreamPanicError()
			}
		}()
		receiveModelStream(context.Background(), reader, defaultStreamLimits(), &result, func(index int64, _ *einoschema.AgenticMessage) {
			indexes = append(indexes, index)
		})
	}()
	if result.message != nil || !result.receivedDelta || result.usage != (model.Usage{InputTokens: 3, OutputTokens: 1}) {
		t.Fatalf("result = %#v", result)
	}
	if result.err == nil || result.err.Error() != providerStreamPanicMessage || strings.Contains(result.err.Error(), secret) {
		t.Fatalf("error = %v", result.err)
	}
	if len(indexes) != 1 || indexes[0] != 0 || reader.closes != 1 {
		t.Fatalf("indexes=%v closes=%d", indexes, reader.closes)
	}
}

func TestReceiveModelStreamClosePanicSupersedesSuccess(t *testing.T) {
	const secret = "provider-secret-close-value"
	reader := &testModelStreamReader{
		chunks:     []model.StreamDelta{{Message: agenticAssistantText("done"), Usage: model.Usage{InputTokens: 4, OutputTokens: 2}}},
		panicAt:    -1,
		panicValue: secret,
		closePanic: true,
	}
	var result modelStreamResult
	func() {
		defer func() {
			if recover() != nil {
				result.message = nil
				result.err = newProviderStreamPanicError()
			}
		}()
		receiveModelStream(context.Background(), reader, defaultStreamLimits(), &result, nil)
	}()
	if result.message != nil || !result.receivedDelta || result.usage != (model.Usage{InputTokens: 4, OutputTokens: 2}) {
		t.Fatalf("result = %#v", result)
	}
	if result.err == nil || result.err.Error() != providerStreamPanicMessage || strings.Contains(result.err.Error(), secret) || reader.closes != 1 {
		t.Fatalf("error=%v closes=%d", result.err, reader.closes)
	}
}

func TestReceiveModelStreamEmitsZeroBasedDeltaOrder(t *testing.T) {
	reader := &testModelStreamReader{
		chunks: []model.StreamDelta{
			{Message: agenticTextChunk(0, "a")},
			{Message: agenticTextChunk(0, "b")},
		},
		panicAt: -1,
	}
	var result modelStreamResult
	var indexes []int64
	receiveModelStream(context.Background(), reader, defaultStreamLimits(), &result, func(index int64, _ *einoschema.AgenticMessage) {
		indexes = append(indexes, index)
	})
	if result.err != nil || result.message == nil || agenticMessageText(result.message) != "ab" {
		t.Fatalf("result = %#v", result)
	}
	if len(indexes) != 2 || indexes[0] != 0 || indexes[1] != 1 || reader.closes != 1 {
		t.Fatalf("indexes=%v closes=%d", indexes, reader.closes)
	}
}

func TestReceiveModelStreamRejectsNilChunk(t *testing.T) {
	reader := &testModelStreamReader{
		chunks:  []model.StreamDelta{{Message: nil}},
		panicAt: -1,
	}
	var result modelStreamResult
	receiveModelStream(context.Background(), reader, defaultStreamLimits(), &result, nil)
	var providerErr model.Error
	if !errors.As(result.err, &providerErr) || providerErr.Code != "malformed_provider_stream" {
		t.Fatalf("error = %v, want malformed_provider_stream", result.err)
	}
	if reader.closes != 1 {
		t.Fatalf("closes = %d, want 1", reader.closes)
	}
}

func TestReceiveModelStreamWithNativeAgenticAdapterCompletesAndPropagatesSourceCancellation(t *testing.T) {
	t.Run("clean completion", func(t *testing.T) {
		client := &nativeAdapterStreamModel{chunks: []*einoschema.AgenticMessage{agenticAssistantText("done")}}
		reader, err := model.NewAgenticStreamer(client).StreamProvider(context.Background(), model.Request{Identity: model.Identity{ProviderID: "fake", ModelID: "m1"}})
		if err != nil {
			t.Fatal(err)
		}
		var result modelStreamResult
		receiveModelStream(context.Background(), reader, defaultStreamLimits(), &result, nil)
		if result.err != nil || result.message == nil || agenticMessageText(result.message) != "done" {
			t.Fatalf("result = %#v", result)
		}
		if !result.receivedDelta {
			t.Fatal("clean adapter stream did not produce a real delta")
		}
	})

	t.Run("source-reported cancellation", func(t *testing.T) {
		client := &nativeAdapterStreamModel{streamErr: context.Canceled}
		reader, err := model.NewAgenticStreamer(client).StreamProvider(context.Background(), model.Request{Identity: model.Identity{ProviderID: "fake", ModelID: "m1"}})
		if err != nil {
			t.Fatal(err)
		}
		var result modelStreamResult
		receiveModelStream(context.Background(), reader, defaultStreamLimits(), &result, nil)
		if !errors.Is(result.err, context.Canceled) || result.receivedDelta || result.message != nil {
			t.Fatalf("result = %#v, want source context.Canceled without a final delta", result)
		}
	})
}

func TestReceiveModelStreamEnforcesMaxChunks(t *testing.T) {
	reader := &testModelStreamReader{
		chunks: []model.StreamDelta{
			{Message: agenticTextChunk(0, "a")},
			{Message: agenticTextChunk(1, "b")},
			{Message: agenticTextChunk(2, "c")},
		},
		panicAt: -1,
	}
	var result modelStreamResult
	receiveModelStream(context.Background(), reader, StreamLimits{MaxChunks: 2, MaxBytes: 1 << 20}, &result, nil)
	var providerErr model.Error
	if !errors.As(result.err, &providerErr) || providerErr.Code != "stream_limit_exceeded" {
		t.Fatalf("error = %v, want stream_limit_exceeded", result.err)
	}
	if reader.closes != 1 {
		t.Fatalf("closes = %d, want 1", reader.closes)
	}
}

func TestReceiveModelStreamEnforcesMaxBytes(t *testing.T) {
	reader := &testModelStreamReader{
		chunks: []model.StreamDelta{
			{Message: agenticTextChunk(0, strings.Repeat("x", 128))},
		},
		panicAt: -1,
	}
	var result modelStreamResult
	receiveModelStream(context.Background(), reader, StreamLimits{MaxChunks: 1 << 20, MaxBytes: 8}, &result, nil)
	var providerErr model.Error
	if !errors.As(result.err, &providerErr) || providerErr.Code != "stream_limit_exceeded" {
		t.Fatalf("error = %v, want stream_limit_exceeded", result.err)
	}
	if reader.closes != 1 {
		t.Fatalf("closes = %d, want 1", reader.closes)
	}
}

// TestReceiveModelStreamConcatenatesFunctionToolCallChunks proves a
// function_tool_call block streamed as several argument-fragment chunks at
// the same StreamingMeta index accumulates into one complete call, the same
// way TestReceiveModelStreamEmitsZeroBasedDeltaOrder proves it for text.
func TestReceiveModelStreamConcatenatesFunctionToolCallChunks(t *testing.T) {
	reader := &testModelStreamReader{
		chunks: []model.StreamDelta{
			{Message: agenticToolCallChunk(0, "call-1", "search", `{"query":`)},
			{Message: agenticToolCallChunk(0, "call-1", "search", `"weather"}`)},
		},
		panicAt: -1,
	}
	var result modelStreamResult
	var indexes []int64
	receiveModelStream(context.Background(), reader, defaultStreamLimits(), &result, func(index int64, _ *einoschema.AgenticMessage) {
		indexes = append(indexes, index)
	})
	if result.err != nil || result.message == nil {
		t.Fatalf("result = %#v", result)
	}
	calls := agenticToolCallsOf(result.message)
	if len(calls) != 1 || calls[0].CallID != "call-1" || calls[0].Name != "search" || calls[0].Arguments != `{"query":"weather"}` {
		t.Fatalf("calls = %#v", calls)
	}
	if len(indexes) != 2 || indexes[0] != 0 || indexes[1] != 1 || reader.closes != 1 {
		t.Fatalf("indexes=%v closes=%d", indexes, reader.closes)
	}
}

func TestWithStreamLimitsRejectsNonPositiveValues(t *testing.T) {
	if _, err := NewStreamingOrchestrator(WithStore(newAdmissionStore()), WithModelResolver(resolvedModel{}), WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()), WithStreamLimits(StreamLimits{MaxChunks: 0, MaxBytes: 1})); err == nil {
		t.Fatal("zero MaxChunks was accepted")
	}
	if _, err := NewStreamingOrchestrator(WithStore(newAdmissionStore()), WithModelResolver(resolvedModel{}), WithIDGenerator(&sequenceIDs{}), WithRunPlanProvider(emptyTestRunPlanProvider()), WithStreamLimits(StreamLimits{MaxChunks: 1, MaxBytes: 0})); err == nil {
		t.Fatal("zero MaxBytes was accepted")
	}
}
