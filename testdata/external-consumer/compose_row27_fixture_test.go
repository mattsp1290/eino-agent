package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

// row27CheckpointStore is deliberately small: the fixture needs to prove the
// public compose checkpoint boundary, not an eino-agent persistence adapter.
type row27CheckpointStore struct {
	mu       sync.Mutex
	m        map[string][]byte
	setCalls atomic.Int32
}

func newRow27CheckpointStore() *row27CheckpointStore {
	return &row27CheckpointStore{m: make(map[string][]byte)}
}

func (s *row27CheckpointStore) Get(_ context.Context, id string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[id]
	return append([]byte(nil), v...), ok, nil
}

func (s *row27CheckpointStore) Set(_ context.Context, id string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[id] = append([]byte(nil), value...)
	s.setCalls.Add(1)
	return nil
}

type row27InvokeResult struct {
	output string
	err    error
}

func row27Graph(t *testing.T, node *compose.Lambda) compose.Runnable[string, string] {
	t.Helper()
	g := compose.NewGraph[string, string]()
	if err := g.AddLambdaNode("node", node); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(compose.START, "node"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("node", compose.END); err != nil {
		t.Fatal(err)
	}
	r, err := g.Compile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestComposeRow27NodePanicReturnsErrorAndRunnableIsReusable(t *testing.T) {
	var calls atomic.Int32
	r := row27Graph(t, compose.InvokableLambda(func(_ context.Context, input string) (string, error) {
		if calls.Add(1) == 1 {
			panic("row27 node panic sentinel")
		}
		return input + "-recovered", nil
	}))

	output, err := r.Invoke(t.Context(), "first")
	if err == nil || output != "" || !strings.Contains(err.Error(), "row27 node panic sentinel") {
		t.Fatalf("first Invoke = (%q, %v), want empty output and converted sentinel error", output, err)
	}
	output, err = r.Invoke(t.Context(), "second")
	if err != nil || output != "second-recovered" || calls.Load() != 2 {
		t.Fatalf("reused Invoke = (%q, %v), calls = %d", output, err, calls.Load())
	}
}

func TestComposeRow27OrdinaryContextCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once
	var releaseOnce sync.Once
	releaseWorker := func() { releaseOnce.Do(func() { close(release) }) }
	r := row27Graph(t, compose.InvokableLambda(func(ctx context.Context, _ string) (string, error) {
		once.Do(func() { close(started) })
		defer close(finished)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-release:
			return "released", nil
		}
	}))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan row27InvokeResult, 1)
	resultRead := false
	t.Cleanup(func() {
		releaseWorker()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Errorf("controlled cancellation node did not finish during cleanup")
			return
		}
		if !resultRead {
			select {
			case <-result:
			case <-time.After(2 * time.Second):
				t.Errorf("controlled cancellation Invoke did not return during cleanup")
			}
		}
	})
	go func() {
		output, err := r.Invoke(ctx, "input")
		result <- row27InvokeResult{output: output, err: err}
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("node did not start")
	}
	cancel()
	select {
	case got := <-result:
		resultRead = true
		if !errors.Is(got.err, context.Canceled) || got.output != "" {
			t.Fatalf("Invoke = (%q, %v), want empty output and context.Canceled", got.output, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled Invoke did not return")
	}
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled node did not finish")
	}
	releaseWorker()
}

func TestComposeRow27NestedGraphInterruptCheckpointResume(t *testing.T) {
	store := newRow27CheckpointStore()
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	var starts atomic.Int32
	var releaseOnce sync.Once
	releaseWorker := func() { releaseOnce.Do(func() { close(release) }) }

	inner := compose.NewGraph[string, string]()
	if err := inner.AddLambdaNode("inner", compose.InvokableLambda(func(_ context.Context, input string) (string, error) {
		if starts.Add(1) == 1 {
			close(started)
			<-release // Graph interruption is independent of ordinary context cancellation.
			close(finished)
		}
		return input + "-inner", nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge(compose.START, "inner"); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge("inner", compose.END); err != nil {
		t.Fatal(err)
	}
	outer := compose.NewGraph[string, string]()
	if err := outer.AddGraphNode("child", inner); err != nil {
		t.Fatal(err)
	}
	if err := outer.AddEdge(compose.START, "child"); err != nil {
		t.Fatal(err)
	}
	if err := outer.AddEdge("child", compose.END); err != nil {
		t.Fatal(err)
	}
	r, err := outer.Compile(t.Context(), compose.WithCheckPointStore(store))
	if err != nil {
		t.Fatal(err)
	}

	interruptCtx, interrupt := compose.WithGraphInterrupt(t.Context())
	result := make(chan row27InvokeResult, 1)
	resultRead := false
	t.Cleanup(func() {
		releaseWorker()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Errorf("controlled interrupted node did not finish during cleanup")
			return
		}
		if !resultRead {
			select {
			case <-result:
			case <-time.After(2 * time.Second):
				t.Errorf("controlled interrupted Invoke did not return during cleanup")
			}
		}
	})
	go func() {
		output, err := r.Invoke(interruptCtx, "input", compose.WithCheckPointID("nested"))
		result <- row27InvokeResult{output: output, err: err}
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("nested node did not start")
	}
	interrupt(compose.WithGraphInterruptTimeout(0))
	select {
	case got := <-result:
		resultRead = true
		if got.output != "" {
			t.Fatalf("interrupted Invoke output = %q, want empty", got.output)
		}
		info, ok := compose.ExtractInterruptInfo(got.err)
		if !ok || info == nil || info.SubGraphs["child"] == nil {
			t.Fatalf("interrupt = %v, info = %#v; want parent-visible child checkpoint", got.err, info)
		}
		if store.setCalls.Load() == 0 {
			t.Fatal("graph interrupt did not write a checkpoint")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("graph interrupt did not return")
	}
	releaseWorker()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("interrupted original task did not finish after release")
	}
	output, err := r.Invoke(t.Context(), "", compose.WithCheckPointID("nested"))
	if err != nil || output != "input-inner" || starts.Load() != 2 {
		t.Fatalf("resume = (%q, %v), starts = %d", output, err, starts.Load())
	}
}

func TestComposeRow27NestedStreamReinterruptResumeConcats(t *testing.T) {
	store := newRow27CheckpointStore()
	var effects atomic.Int32
	interrupting := compose.StreamableLambda(func(ctx context.Context, _ string) (*schema.StreamReader[string], error) {
		interrupted, hasState, state := compose.GetInterruptState[string](ctx)
		if !interrupted {
			return nil, compose.StatefulInterrupt(ctx, "first", "first-state")
		}
		if !hasState {
			return nil, errors.New("missing interrupt state")
		}
		switch state {
		case "first-state":
			r, w := schema.Pipe[string](1)
			go func() { defer w.Close(); w.Send("", compose.StatefulInterrupt(ctx, "second", "second-state")) }()
			return r, nil
		case "second-state":
			effects.Add(1)
			return schema.StreamReaderFromArray([]string{"done"}), nil
		default:
			return nil, fmt.Errorf("unexpected interrupt state %q", state)
		}
	})
	inner := compose.NewGraph[string, string]()
	if err := inner.AddLambdaNode("tool", interrupting); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddLambdaNode("collect", compose.CollectableLambda(func(_ context.Context, input *schema.StreamReader[string]) (string, error) {
		defer input.Close()
		var chunks []string
		for {
			value, err := input.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return "", err
			}
			chunks = append(chunks, value)
		}
		return fmt.Sprint(chunks), nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge(compose.START, "tool"); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge("tool", "collect"); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge("collect", compose.END); err != nil {
		t.Fatal(err)
	}
	outer := compose.NewGraph[string, string]()
	if err := outer.AddGraphNode("child", inner); err != nil {
		t.Fatal(err)
	}
	if err := outer.AddEdge(compose.START, "child"); err != nil {
		t.Fatal(err)
	}
	if err := outer.AddEdge("child", compose.END); err != nil {
		t.Fatal(err)
	}
	r, err := outer.Compile(t.Context(), compose.WithCheckPointStore(store))
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Stream(t.Context(), "input", compose.WithCheckPointID("stream"))
	first, ok := compose.ExtractInterruptInfo(err)
	if !ok || first == nil || first.SubGraphs["child"] == nil || len(first.InterruptContexts) != 1 {
		t.Fatalf("first stream interrupt = %v, info = %#v", err, first)
	}
	_, err = r.Stream(compose.ResumeWithData(t.Context(), first.InterruptContexts[0].ID, "approved"), "", compose.WithCheckPointID("stream"))
	second, ok := compose.ExtractInterruptInfo(err)
	if !ok || second == nil || second.SubGraphs["child"] == nil || len(second.InterruptContexts) != 1 {
		t.Fatalf("second stream interrupt = %v, info = %#v", err, second)
	}
	stream, err := r.Stream(compose.ResumeWithData(t.Context(), second.InterruptContexts[0].ID, "approved"), "", compose.WithCheckPointID("stream"))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	value, err := stream.Recv()
	if err != nil || value != "[done]" {
		t.Fatalf("final stream first value = (%q, %v)", value, err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("final stream terminal error = %v, want EOF", err)
	}
	if effects.Load() != 1 {
		t.Fatalf("side effects = %d, want 1", effects.Load())
	}
}
