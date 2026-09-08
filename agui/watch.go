package agui

import (
	"errors"
	"unicode/utf8"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"
	"github.com/mattsp1290/eino-agui/emitter"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/watch"
)

// WatchState is the allowlisted STATE_SNAPSHOT shape of the observation adapter.
// Text is carried only by MESSAGES_SNAPSHOT, with stable durable message IDs.
type WatchState struct {
	Watermark            session.ObservationWatermark
	Exists               bool
	OmittedOlderMessages bool
	Runs                 []session.ObservationRun
	Tools                []session.ObservationTool
	Live                 []WatchLiveState
}
type WatchLiveState struct {
	Identity           watch.LiveIdentity
	Available          bool
	PublicationVersion uint64
}

// WatchBridge consumes one subscription in delivery order. It emits current
// state replacements, not historical run events. Use a new bridge on reconnect.
type WatchBridge struct {
	emit     *emitter.Emitter
	snapshot session.ObservationSnapshot
	overlays map[session.MessageID]watch.Update
	sequence uint64
}

func NewWatchBridge(emit *emitter.Emitter, initial session.ObservationSnapshot) *WatchBridge {
	return &WatchBridge{emit: emit, snapshot: initial.Clone(), overlays: make(map[session.MessageID]watch.Update)}
}

// Initial emits the durable baseline before transient updates.
func (b *WatchBridge) Initial() error { return b.render() }
func (b *WatchBridge) Apply(u watch.Update) error {
	if u.DeliverySequence == 0 {
		return watch.ErrResyncRequired
	}
	if u.DeliverySequence <= b.sequence {
		return nil
	}
	switch u.Kind {
	case watch.Durable:
		w, old := u.Snapshot.Watermark, b.snapshot.Watermark
		if w.StoreID != old.StoreID || w.SessionID != old.SessionID || w.Revision <= old.Revision {
			return watch.ErrResyncRequired
		}
		b.snapshot = u.Snapshot.Clone()
		for id, overlay := range b.overlays {
			if !watchOverlayEligible(b.snapshot, overlay) {
				delete(b.overlays, id)
			}
		}
	case watch.Live, watch.LiveUnavailable:
		id := u.Live.Identity
		if id.SessionID != b.snapshot.Watermark.SessionID {
			return watch.ErrResyncRequired
		}
		if !utf8.ValidString(u.Live.Text) {
			return watch.ErrResyncRequired
		}
		if watchOverlayEligible(b.snapshot, u) {
			previous, exists := b.overlays[id.MessageID]
			if !exists || u.Live.PublicationVersion > previous.Live.PublicationVersion {
				b.overlays[id.MessageID] = u
			}
		}
	default:
		return watch.ErrResyncRequired
	}
	b.sequence = u.DeliverySequence
	return b.render()
}
func (b *WatchBridge) render() error {
	if b == nil || b.emit == nil {
		return watch.ErrOptions
	}
	messages := make([]types.Message, 0, len(b.snapshot.Messages))
	state := WatchState{Watermark: b.snapshot.Watermark, Exists: b.snapshot.Exists, OmittedOlderMessages: b.snapshot.OmittedOlderMessages, Runs: b.snapshot.Runs, Tools: b.snapshot.Tools, Live: make([]WatchLiveState, 0, len(b.overlays))}
	for _, m := range b.snapshot.Messages {
		text := m.Text
		if overlay, ok := b.overlays[m.ID]; ok {
			if overlay.Kind == watch.Live {
				text = overlay.Live.Text
			}
			state.Live = append(state.Live, WatchLiveState{Identity: overlay.Live.Identity, Available: overlay.Kind == watch.Live, PublicationVersion: overlay.Live.PublicationVersion})
		}
		messages = append(messages, types.Message{ID: string(m.ID), Role: types.Role(m.Role), Content: text})
	}
	if runNotice, ok := b.overlays[""]; ok {
		state.Live = append(state.Live, WatchLiveState{Identity: runNotice.Live.Identity, Available: false, PublicationVersion: runNotice.Live.PublicationVersion})
	}
	b.emit.MessagesSnapshot(messages)
	b.emit.StateSnapshot(state)
	return errors.Join(b.emit.Err(), b.emit.EncErr())
}

// A run-qualified unavailable notice can precede a visible placeholder. It
// communicates availability in state without inventing a conversation message.
func watchOverlayEligible(snapshot session.ObservationSnapshot, u watch.Update) bool {
	id := u.Live.Identity
	if u.Kind != watch.LiveUnavailable || id.MessageID != "" {
		return watch.Eligible(snapshot, id)
	}
	if id.SessionID != snapshot.Watermark.SessionID {
		return false
	}
	for _, message := range snapshot.Messages {
		if message.RunID == id.RunID && !message.Finalized {
			return false
		}
	}
	for _, run := range snapshot.Runs {
		if run.ID == id.RunID && !run.Terminal() {
			return true
		}
	}
	return false
}
