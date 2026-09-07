package watch

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

type Service struct {
	mu            sync.Mutex
	reader        session.ObservationReader
	options       Options
	incarnation   string
	closed        bool
	sessions      map[session.ID]*watchedSession
	subscriptions map[*Subscription]bool
	live          map[runKey]*liveEntry
	liveBytes     int
	version       uint64
	workers       sync.WaitGroup
	done          chan struct{}
}
type watchedSession struct {
	id        session.ID
	ctx       context.Context
	cancel    context.CancelFunc
	wake      chan struct{}
	subs      map[*Subscription]bool
	watermark session.ObservationWatermark
}

func NewService(reader session.ObservationReader, options Options) (*Service, error) {
	if reader == nil || options.validate() != nil {
		return nil, ErrOptions
	}
	v := reflect.ValueOf(reader)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Interface, reflect.Slice:
		if v.IsNil() {
			return nil, ErrOptions
		}
	}
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return nil, ErrOptions
	}
	return &Service{reader: reader, options: options, incarnation: hex.EncodeToString(bytes[:]), sessions: make(map[session.ID]*watchedSession), subscriptions: make(map[*Subscription]bool), live: make(map[runKey]*liveEntry), done: make(chan struct{})}, nil
}
func (s *Service) Watch(ctx context.Context, id session.ID) (*Subscription, error) {
	if s == nil || ctx == nil || id == "" {
		return nil, ErrOptions
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	if len(s.subscriptions) >= s.options.MaxSubscriptions {
		s.mu.Unlock()
		return nil, ErrCapacity
	}
	group := s.sessions[id]
	if group == nil {
		if len(s.sessions) >= s.options.MaxWatchedSessions {
			s.mu.Unlock()
			return nil, ErrCapacity
		}
		workerCtx, cancel := context.WithCancel(context.Background())
		group = &watchedSession{id: id, ctx: workerCtx, cancel: cancel, wake: make(chan struct{}, 1), subs: make(map[*Subscription]bool)}
		s.sessions[id] = group
		s.workers.Add(1)
		go s.poll(group)
	}
	lifetime, cancel := context.WithCancel(ctx)
	sub := &Subscription{service: s, group: group, ctx: lifetime, cancel: cancel, notify: make(chan struct{}, 1)}
	s.subscriptions[sub] = true
	group.subs[sub] = true
	sub.stop = context.AfterFunc(lifetime, func() { sub.Close() })
	s.workers.Add(1)
	defer s.workers.Done()
	s.mu.Unlock()
	readCtx, readCancel := context.WithTimeout(lifetime, s.options.ReadTimeout)
	snap, err := s.reader.ReadObservationSnapshot(readCtx, id, s.options.Snapshot)
	readCancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	if sub.err != nil {
		return nil, sub.err
	}
	if err != nil {
		s.terminateLocked(sub, safeReadError(err))
		return nil, sub.err
	}
	if !validSnapshot(snap, id) {
		s.terminateLocked(sub, ErrResyncRequired)
		return nil, sub.err
	}
	sub.initial = snap.Clone()
	sub.snapshot = snap.Clone()
	sub.ready = true
	s.seedLocked(sub)
	if sub.err != nil {
		return nil, sub.err
	}
	return sub, nil
}
func validSnapshot(s session.ObservationSnapshot, id session.ID) bool {
	return s.Watermark.StoreID != "" && s.Watermark.SessionID == id && s.Watermark.Revision >= 0 && ((s.Exists && s.Watermark.Revision > 0) || (!s.Exists && s.Watermark.Revision == 0 && len(s.Messages) == 0 && len(s.Runs) == 0 && len(s.Tools) == 0))
}
func safeReadError(err error) error {
	for _, safe := range []error{session.ErrObservationTooLarge, session.ErrObservationInvalid} {
		if errors.Is(err, safe) {
			return safe
		}
	}
	return session.ErrObservationStore
}

// Hint is optional latency assistance; periodic reads establish durable truth.
func (s *Service) Hint(id session.ID) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if group := s.sessions[id]; group != nil {
		select {
		case group.wake <- struct{}{}:
		default:
		}
	}
}
func (s *Service) poll(g *watchedSession) {
	defer s.workers.Done()
	ticker := time.NewTicker(s.options.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
		case <-g.wake:
		}
		s.mu.Lock()
		baseline := g.watermark
		for sub := range g.subs {
			if sub.ready && (baseline.StoreID == "" || sub.snapshot.Watermark.Revision > baseline.Revision) {
				baseline = sub.snapshot.Watermark
			}
		}
		s.mu.Unlock()
		ctx, cancel := context.WithTimeout(g.ctx, s.options.ReadTimeout)
		revision, err := s.reader.ReadObservationRevision(ctx, g.id)
		var snap session.ObservationSnapshot
		s.mu.Lock()
		need := err == nil && (g.watermark.StoreID == "" || revision != g.watermark)
		if err == nil {
			for sub := range g.subs {
				if sub.ready && sub.snapshot.Watermark != revision {
					need = true
					break
				}
			}
		}
		s.mu.Unlock()
		if need {
			snap, err = s.reader.ReadObservationSnapshot(ctx, g.id, s.options.Snapshot)
		}
		cancel()
		s.mu.Lock()
		if g.ctx.Err() != nil {
			s.mu.Unlock()
			return
		}
		if err != nil {
			for sub := range g.subs {
				s.terminateLocked(sub, safeReadError(err))
			}
			s.mu.Unlock()
			return
		}
		if revision.SessionID != g.id || revision.StoreID == "" || revision.Revision < 0 ||
			(baseline.StoreID != "" && (revision.StoreID != baseline.StoreID || revision.Revision < baseline.Revision)) {
			for sub := range g.subs {
				s.terminateLocked(sub, ErrResyncRequired)
			}
			s.mu.Unlock()
			return
		}
		if need {
			if !validSnapshot(snap, g.id) || snap.Watermark.StoreID != revision.StoreID || snap.Watermark.Revision < revision.Revision {
				for sub := range g.subs {
					s.terminateLocked(sub, ErrResyncRequired)
				}
				s.mu.Unlock()
				return
			}
			g.watermark = snap.Watermark
			for sub := range g.subs {
				if !sub.ready {
					continue
				}
				if snap.Watermark.StoreID != sub.snapshot.Watermark.StoreID {
					s.terminateLocked(sub, ErrResyncRequired)
					continue
				}
				if snap.Watermark.Revision <= sub.snapshot.Watermark.Revision {
					continue
				}
				sub.snapshot = snap.Clone()
				sub.pending = nil // whole-window replacement invalidates every queued overlay
				s.enqueueLocked(sub, Update{Kind: Durable, Snapshot: snap})
				s.seedLocked(sub)
			}
		}
		s.mu.Unlock()
	}
}
func (s *Service) terminateLocked(sub *Subscription, err error) {
	if sub.err != nil {
		return
	}
	sub.err = err
	sub.pending = nil
	sub.cancel()
	if sub.stop != nil {
		sub.stop()
	}
	delete(s.subscriptions, sub)
	delete(sub.group.subs, sub)
	if len(sub.group.subs) == 0 {
		sub.group.cancel()
		if s.sessions[sub.group.id] == sub.group {
			delete(s.sessions, sub.group.id)
		}
	}
	signal(sub.notify)
}
func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
func (s *Service) enqueueLocked(sub *Subscription, u Update) {
	if sub.err != nil || !sub.ready {
		return
	}
	for i := range sub.pending {
		old := sub.pending[i]
		if old.Kind != Durable && u.Kind != Durable && old.Live.Identity.MessageID == u.Live.Identity.MessageID && old.Live.Identity.RunID == u.Live.Identity.RunID {
			sub.pending[i] = u
			signal(sub.notify)
			return
		}
	}
	if len(sub.pending) >= s.options.PendingUpdates {
		s.terminateLocked(sub, ErrResyncRequired)
		return
	}
	sub.pending = append(sub.pending, u)
	signal(sub.notify)
}

// Close rejects new observers, cancels its reads and workers, and releases live
// resources. It never interrupts a run or closes the reader. Repeat after timeout.
func (s *Service) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		for sub := range s.subscriptions {
			s.terminateLocked(sub, ErrClosed)
		}
		s.live = nil
		s.liveBytes = 0
		go func() { s.workers.Wait(); close(s.done) }()
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
