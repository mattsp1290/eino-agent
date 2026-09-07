package watch

import (
	"strings"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/session"
)

type runKey struct {
	session session.ID
	run     session.RunID
}
type liveEntry struct {
	value     LiveText
	available bool
}

// BeginAttempt is a trusted runtime tap. Capacity failure affects observation
// availability only. A new attempt replaces any retained prefix for the run.
func (s *Service) BeginAttempt(id LiveIdentity) LiveIdentity {
	if s == nil {
		return LiveIdentity{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id.ServiceID = s.incarnation
	if s.closed || id.SessionID == "" || id.RunID == "" || id.MessageID == "" || id.RequestID == "" || id.Attempt < 0 || id.Step < 0 {
		return LiveIdentity{}
	}
	if len(id.SessionID)+len(id.RunID)+len(id.MessageID)+len(id.RequestID) > s.options.Snapshot.MaxSnapshotBytes/6 {
		return LiveIdentity{}
	}
	key := runKey{id.SessionID, id.RunID}
	if old := s.live[key]; old != nil {
		s.liveBytes -= len(old.value.Text)
		delete(s.live, key)
	}
	s.version++
	if len(s.live) < s.options.MaxLiveRuns {
		s.live[key] = &liveEntry{value: LiveText{Identity: id, PublicationVersion: s.version}, available: true}
	}
	s.seedSessionLocked(id.SessionID)
	return id
}
func (s *Service) AppendText(id LiveIdentity, text string) {
	if s == nil || text == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.live[runKey{id.SessionID, id.RunID}]
	if s.closed || entry == nil || entry.value.Identity != id || !entry.available {
		return
	}
	s.version++
	entry.value.Sequence++
	entry.value.PublicationVersion = s.version
	if !utf8.ValidString(text) || len(text) > s.options.MaxLiveTextBytes-s.liveBytes {
		s.liveBytes -= len(entry.value.Text)
		entry.value.Text = ""
		entry.available = false
	} else {
		entry.value.Text += strings.Clone(text)
		s.liveBytes += len(text)
	}
	s.seedSessionLocked(id.SessionID)
}
func (s *Service) FinishAttempt(id LiveIdentity) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := runKey{id.SessionID, id.RunID}
	if entry := s.live[key]; entry != nil && entry.value.Identity == id {
		s.liveBytes -= len(entry.value.Text)
		delete(s.live, key)
		s.version++
		s.seedSessionLocked(id.SessionID)
	}
}
func (s *Service) ReleaseRun(id session.ID, run session.RunID) {
	if s == nil {
		return
	}
	s.mu.Lock()
	key := runKey{id, run}
	if entry := s.live[key]; entry != nil {
		s.liveBytes -= len(entry.value.Text)
		delete(s.live, key)
	}
	if !s.closed {
		s.version++
		s.seedSessionLocked(id)
	}
	s.mu.Unlock()
	s.Hint(id)
}
func (s *Service) seedSessionLocked(id session.ID) {
	if group := s.sessions[id]; group != nil {
		for sub := range group.subs {
			if sub.ready {
				s.seedLocked(sub)
			}
		}
	}
}
func (s *Service) seedLocked(sub *Subscription) {
	for _, m := range sub.snapshot.Messages {
		id := LiveIdentity{ServiceID: s.incarnation, SessionID: sub.group.id, RunID: m.RunID, MessageID: m.ID}
		if !Eligible(sub.snapshot, id) {
			continue
		}
		u := Update{Kind: LiveUnavailable, Live: LiveText{Identity: id, PublicationVersion: s.version}}
		if entry := s.live[runKey{id.SessionID, id.RunID}]; entry != nil && entry.value.Identity.MessageID == m.ID {
			u.Live = entry.value
			if entry.available {
				u.Kind = Live
			}
		}
		s.enqueueLocked(sub, u)
	}
}
