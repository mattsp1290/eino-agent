package agui

import (
	"bufio"
	"bytes"
	"context"
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
