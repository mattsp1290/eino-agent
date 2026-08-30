package runtime

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
)

type testModelStreamReader struct {
	chunks     []model.StreamDelta
	index      int
	panicAt    int
	closePanic bool
	closes     int
}

func (r *testModelStreamReader) Recv() (model.StreamDelta, error) {
	if r.index == r.panicAt {
		panic("receive panic")
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
		panic("close panic")
	}
}

func TestReceiveModelStreamPreservesPartialStateAcrossPanic(t *testing.T) {
	reader := &testModelStreamReader{
		chunks:  []model.StreamDelta{{Message: einoschema.AssistantMessage("first", nil), Usage: model.Usage{InputTokens: 3, OutputTokens: 1}}},
		panicAt: 1,
	}
	var result modelStreamResult
	var indexes []int64
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				result.message = nil
				result.err = fmt.Errorf("provider stream panic: %v", recovered)
			}
		}()
		receiveModelStream(context.Background(), reader, &result, func(index int64, _ *einoschema.Message) {
			indexes = append(indexes, index)
		})
	}()
	if result.message != nil || !result.receivedDelta || result.usage != (model.Usage{InputTokens: 3, OutputTokens: 1}) {
		t.Fatalf("result = %#v", result)
	}
	if result.err == nil || !strings.Contains(result.err.Error(), "provider stream panic: receive panic") {
		t.Fatalf("error = %v", result.err)
	}
	if len(indexes) != 1 || indexes[0] != 0 || reader.closes != 1 {
		t.Fatalf("indexes=%v closes=%d", indexes, reader.closes)
	}
}

func TestReceiveModelStreamClosePanicSupersedesSuccess(t *testing.T) {
	reader := &testModelStreamReader{
		chunks:     []model.StreamDelta{{Message: einoschema.AssistantMessage("done", nil), Usage: model.Usage{InputTokens: 4, OutputTokens: 2}}},
		panicAt:    -1,
		closePanic: true,
	}
	var result modelStreamResult
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				result.message = nil
				result.err = fmt.Errorf("provider stream panic: %v", recovered)
			}
		}()
		receiveModelStream(context.Background(), reader, &result, nil)
	}()
	if result.message != nil || !result.receivedDelta || result.usage != (model.Usage{InputTokens: 4, OutputTokens: 2}) {
		t.Fatalf("result = %#v", result)
	}
	if result.err == nil || !strings.Contains(result.err.Error(), "provider stream panic: close panic") || reader.closes != 1 {
		t.Fatalf("error=%v closes=%d", result.err, reader.closes)
	}
}

func TestReceiveModelStreamEmitsZeroBasedDeltaOrder(t *testing.T) {
	reader := &testModelStreamReader{
		chunks: []model.StreamDelta{
			{Message: einoschema.AssistantMessage("a", nil)},
			{Message: einoschema.AssistantMessage("b", nil)},
		},
		panicAt: -1,
	}
	var result modelStreamResult
	var indexes []int64
	receiveModelStream(context.Background(), reader, &result, func(index int64, _ *einoschema.Message) {
		indexes = append(indexes, index)
	})
	if result.err != nil || result.message == nil || result.message.Content != "ab" {
		t.Fatalf("result = %#v", result)
	}
	if len(indexes) != 2 || indexes[0] != 0 || indexes[1] != 1 || reader.closes != 1 {
		t.Fatalf("indexes=%v closes=%d", indexes, reader.closes)
	}
}
