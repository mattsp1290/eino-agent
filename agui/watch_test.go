package agui

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"
	"github.com/mattsp1290/eino-agui/emitter"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/watch"
)

func TestWatchBridgeReplacementsAndDelayedRemoval(t *testing.T) {
	var output bytes.Buffer
	emit := emitter.NewEmitter(context.Background(), bufio.NewWriter(&output), sse.NewSSEWriter(), "s", "", nil)
	initial := session.ObservationSnapshot{Watermark: session.ObservationWatermark{StoreID: "store", SessionID: "s", Revision: 1}, Exists: true, Messages: []session.ObservationMessage{{ID: "m", RunID: "r", Role: session.RoleAssistant}}, Runs: []session.ObservationRun{{ID: "r", Status: session.RunRunning}}}
	bridge := NewWatchBridge(emit, initial)
	if err := bridge.Initial(); err != nil {
		t.Fatal(err)
	}
	id := watch.LiveIdentity{ServiceID: "svc", SessionID: "s", RunID: "r", MessageID: "m", RequestID: "req"}
	if err := bridge.Apply(watch.Update{Kind: watch.Live, DeliverySequence: 1, Live: watch.LiveText{Identity: id, PublicationVersion: 1, Text: "prefix"}}); err != nil {
		t.Fatal(err)
	}
	if err := bridge.Apply(watch.Update{Kind: watch.LiveUnavailable, DeliverySequence: 2, Live: watch.LiveText{Identity: id, PublicationVersion: 3}}); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := bridge.Apply(watch.Update{Kind: watch.Live, DeliverySequence: 3, Live: watch.LiveText{Identity: id, PublicationVersion: 2, Text: "stale"}}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "stale") {
		t.Fatal(output.String())
	}
	final := initial.Clone()
	final.Watermark.Revision++
	final.Messages[0].Finalized = true
	final.Messages[0].Text = "final"
	if err := bridge.Apply(watch.Update{Kind: watch.Durable, DeliverySequence: 4, Snapshot: final}); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := bridge.Apply(watch.Update{Kind: watch.Live, DeliverySequence: 5, Live: watch.LiveText{Identity: id, PublicationVersion: 99, Text: "stale"}}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "stale") || !strings.Contains(output.String(), `"id":"m"`) || !strings.Contains(output.String(), `"content":"final"`) {
		t.Fatal(output.String())
	}
	moved := initial.Clone()
	moved.Watermark.Revision = 10
	moved.Messages = []session.ObservationMessage{{ID: "new", RunID: "new-run", Role: session.RoleUser, Finalized: true}}
	moved.Runs = []session.ObservationRun{{ID: "new-run", Status: session.RunRunning}}
	if err := bridge.Apply(watch.Update{Kind: watch.Durable, DeliverySequence: 6, Snapshot: moved}); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := bridge.Apply(watch.Update{Kind: watch.Live, DeliverySequence: 7, Live: watch.LiveText{Identity: id, PublicationVersion: 100, Text: "resurrected"}}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "resurrected") || strings.Contains(output.String(), `"id":"m"`) {
		t.Fatal(output.String())
	}
}

// FuzzWatchBridgeApply feeds bounded malformed updates through the actual
// durable/live application path, including version and ownership validation.
func FuzzWatchBridgeApply(f *testing.F) {
	for _, raw := range []string{`{"Kind":255,"DeliverySequence":1}`, `{"Kind":1,"DeliverySequence":1,"Snapshot":{"Watermark":{"StoreID":"store","SessionID":"s","Revision":-1}}}`, `{"Kind":2,"DeliverySequence":1,"Live":{"Identity":{"SessionID":"foreign","RunID":"r","MessageID":"m"},"Text":"bad"}}`} {
		f.Add([]byte(raw))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 4096 {
			return
		}
		var update watch.Update
		if json.Unmarshal(raw, &update) != nil {
			return
		}
		var output bytes.Buffer
		emit := emitter.NewEmitter(context.Background(), bufio.NewWriter(&output), sse.NewSSEWriter(), "s", "", nil)
		initial := session.ObservationSnapshot{Watermark: session.ObservationWatermark{StoreID: "store", SessionID: "s", Revision: 1}, Exists: true, Messages: []session.ObservationMessage{{ID: "m", RunID: "r", Role: session.RoleAssistant, Finalized: true, Text: "final"}}, Runs: []session.ObservationRun{{ID: "r", Status: session.RunCompleted}}}
		bridge := NewWatchBridge(emit, initial)
		_ = bridge.Apply(update)
		if update.Kind == watch.Live || update.Kind == watch.LiveUnavailable {
			if len(bridge.overlays) != 0 {
				t.Fatal("terminal/finalized snapshot accepted overlay")
			}
		}
		if len(bridge.overlays) > len(bridge.snapshot.Messages) {
			t.Fatal("overlay escaped bounded window")
		}
	})
}

func TestWatchBridgeRunUnavailableDoesNotInventMessage(t *testing.T) {
	var output bytes.Buffer
	emit := emitter.NewEmitter(context.Background(), bufio.NewWriter(&output), sse.NewSSEWriter(), "s", "", nil)
	initial := session.ObservationSnapshot{Watermark: session.ObservationWatermark{StoreID: "store", SessionID: "s", Revision: 1}, Exists: true, Messages: []session.ObservationMessage{{ID: "user", RunID: "r", Role: session.RoleUser, Finalized: true, Text: "hello"}}, Runs: []session.ObservationRun{{ID: "r", Status: session.RunRunning}}}
	bridge := NewWatchBridge(emit, initial)
	notice := watch.Update{Kind: watch.LiveUnavailable, DeliverySequence: 1, Live: watch.LiveText{Identity: watch.LiveIdentity{ServiceID: "svc", SessionID: "s", RunID: "r"}, PublicationVersion: 1}}
	if err := bridge.Apply(notice); err != nil {
		t.Fatal(err)
	}
	if len(bridge.overlays) != 1 || !strings.Contains(output.String(), `"Available":false`) || strings.Contains(output.String(), `"id":""`) {
		t.Fatal(output.String())
	}
	placeholder := initial.Clone()
	placeholder.Watermark.Revision = 2
	placeholder.Messages = append(placeholder.Messages, session.ObservationMessage{ID: "assistant", RunID: "r", Role: session.RoleAssistant})
	output.Reset()
	if err := bridge.Apply(watch.Update{Kind: watch.Durable, DeliverySequence: 2, Snapshot: placeholder}); err != nil {
		t.Fatal(err)
	}
	if len(bridge.overlays) != 0 || !strings.Contains(output.String(), `"Live":[]`) {
		t.Fatal(output.String())
	}
	notice.DeliverySequence = 3
	notice.Live.PublicationVersion = 99
	if err := bridge.Apply(notice); err != nil {
		t.Fatal(err)
	}
	if len(bridge.overlays) != 0 {
		t.Fatal("delayed run notice survived eligible placeholder")
	}
}
