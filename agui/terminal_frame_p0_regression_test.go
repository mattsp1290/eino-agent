package agui

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

// TestEmitToolCallUpdatedMalformedPayloadEmitsExactlyOneRedactedTerminalFrame
// proves the W7 fifth fix-pass review's P0-3 finding: the
// at-most-one-terminal-frame invariant must hold for EVERY RUN_ERROR
// producer, not just Emit's own runtime.EventRunFinished case.
//
// Before this fix, emitToolCallUpdated's malformed-payload branch called
// b.emit.RunError(err.Error()) directly: it neither checked nor set
// Bridge.terminated, and it put the raw encoding/json parser error (e.g.
// `invalid character 'n' looking for beginning of object key string`) on
// the wire, defeating both the terminal-frame invariant and
// TerminalErrorMessage's redaction contract. A single malformed tool
// payload also killed the connection via replay()/Reconnect's
// bridge.LiveErr() check, so this is reachable on any tool payload this
// bridge cannot unmarshal, not a contrived edge case.
func TestEmitToolCallUpdatedMalformedPayloadEmitsExactlyOneRedactedTerminalFrame(t *testing.T) {
	t.Parallel()

	sink := newSSESink()
	bridge := NewBridge(context.Background(), nil, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	bridge.Emit(context.Background(), session.EventRecord{
		Kind: runtime.EventToolCallUpdated, MessageID: "assistant-1", ToolCallID: "call-1",
		Payload: []byte(`not-json`),
	})
	// A SECOND malformed payload on the same connection must not add a
	// second terminal frame either -- emitTerminalError's own terminated
	// check must fire on a second call through the SAME function, not only
	// when a different terminal-frame producer (Terminate) runs after it.
	bridge.Emit(context.Background(), session.EventRecord{
		Kind: runtime.EventToolCallUpdated, MessageID: "assistant-1", ToolCallID: "call-1",
		Payload: []byte(`also-not-json`),
	})

	if err := bridge.LiveErr(); err == nil {
		t.Fatalf("LiveErr() = nil, want the malformed-payload error preserved for a host (it must not be silently dropped just because it is redacted on the wire)")
	}

	// A terminal frame already reached the wire above -- a later Terminate
	// call for an unrelated failure must be a no-op, not a second frame.
	if ok := bridge.Terminate(context.Background(), errors.New("boom")); ok {
		t.Fatalf("Terminate() = true, want false: a terminal frame already reached the wire for this connection")
	}

	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	want := "RUN_ERROR"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s (exactly one terminal frame, and Terminate must not add a second one)", got, want)
	}
	raw := string(sink.Bytes())
	if !strings.Contains(raw, TerminalErrorMessage) {
		t.Fatalf("wire missing the fixed TerminalErrorMessage: %s", raw)
	}
	if strings.Contains(raw, "invalid character") {
		t.Fatalf("raw encoding/json parser error text leaked to the wire: %s", raw)
	}
}

// TestBridgeErrorEnforcesTerminatedInvariant proves the exported Bridge.Error
// method -- dead in-repo (transport/http.go calls Terminate instead) but
// still exported for a host embedding Bridge directly -- is now
// policy-consistent with Terminate and emitToolCallUpdated's
// malformed-payload branch: it checks/sets Bridge.terminated and never puts
// its caller-supplied message on the wire.
func TestBridgeErrorEnforcesTerminatedInvariant(t *testing.T) {
	t.Parallel()

	sink := newSSESink()
	bridge := NewBridge(context.Background(), nil, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	bridge.Error("internal detail that must never reach the wire")
	// A second call must be a no-op: at most one terminal frame ever.
	bridge.Error("a second internal detail")

	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	want := "RUN_ERROR"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s (Bridge.Error must enforce at-most-one-terminal-frame)", got, want)
	}
	raw := string(sink.Bytes())
	if strings.Contains(raw, "internal detail") {
		t.Fatalf("caller-supplied message text leaked to the wire: %s", raw)
	}
	if !strings.Contains(raw, TerminalErrorMessage) {
		t.Fatalf("wire missing the fixed TerminalErrorMessage: %s", raw)
	}
}

// TestTerminateClosesOpenSpansThroughFallbackEmitter proves the W7 fifth
// fix-pass review's I1 finding: Terminate must close any open
// TEXT_MESSAGE/REASONING span before writing its terminal frame, through
// the SAME fallback emitter the terminal frame itself uses (not b.emit,
// whose ctx is exactly the dead/expiring one Terminate exists to work
// around).
func TestTerminateClosesOpenSpansThroughFallbackEmitter(t *testing.T) {
	t.Parallel()

	sink := newSSESink()
	bridge := NewBridge(context.Background(), nil, session.ContentLimits{}, false, sink.Writer(), sse.NewSSEWriter(), "thread-1", "run-1", nil)
	bridge.Emit(context.Background(), session.EventRecord{
		Kind: runtime.EventMessageDelta, MessageID: "assistant-1",
		Payload: []byte(`{"content":"still streaming","reasoning":""}`),
	})
	if ok := bridge.Terminate(context.Background(), errors.New("host deadline")); !ok {
		t.Fatalf("Terminate() = false, want true")
	}

	frames := frameData(t, sink.Bytes())
	got := typesFromFrames(frames)
	want := "TEXT_MESSAGE_START,TEXT_MESSAGE_CONTENT,TEXT_MESSAGE_END,RUN_ERROR"
	if stringsJoined(got) != want {
		t.Fatalf("event types = %#v, want %s (Terminate must close the open text span before its terminal frame)", got, want)
	}
}
