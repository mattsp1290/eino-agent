# Suggestions

## Replace timing-based event-count polling with an explicit completion signal

- Reference: `runtime/orchestrator_tool_test.go:112`
- Rationale: The repaired test now configures sufficient queue capacity, which correctly tests three best-effort delivery attempts. Its final assertion still polls an atomic counter every millisecond for up to one second. A channel closed on the third observed tool event gives the test a deterministic happens-before edge, produces faster failures, and avoids dependence on scheduler timing under heavily loaded CI.

```go
type selectiveToolPanickingSink struct {
	toolEvents atomic.Int32
	delivered  chan struct{}
	once       sync.Once
}

func (s *selectiveToolPanickingSink) Emit(_ context.Context, event session.EventRecord) {
	if event.Kind != EventToolCallUpdated {
		return
	}
	if s.toolEvents.Add(1) == 3 {
		s.once.Do(func() { close(s.delivered) })
	}
	panic("transport unavailable")
}

select {
case <-sink.delivered:
case <-time.After(time.Second):
	t.Fatalf("tool event delivery attempts = %d, want 3", sink.toolEvents.Load())
}
```

## Make infrastructure-queue drops observable without changing best-effort semantics

- Reference: `runtime/event_queue.go:43`
- Rationale: `eventQueue.emit` deliberately drops new work when full and reports that fact with a boolean, but every production caller discards the result. Durable records remain replayable, so this is not a correctness blocker, but operators cannot distinguish a healthy live path from an undersized or blocked infrastructure sink. A bounded counter or observer hook would preserve the non-blocking contract while making queue sizing actionable.

```go
type eventQueue struct {
	// existing fields
	dropped atomic.Uint64
}

// In the full-queue branch:
q.dropped.Add(1)
return false
```

Expose the count through the existing observability path or a shutdown diagnostic rather than adding synchronous sink work to the enqueue path.
