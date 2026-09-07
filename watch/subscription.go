package watch

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/mattsp1290/eino-agent/session"
)

type Subscription struct {
	service   *Service
	group     *watchedSession
	ctx       context.Context
	cancel    context.CancelFunc
	stop      func() bool
	notify    chan struct{}
	operation sync.Mutex
	reading   atomic.Bool
	// Below protected by service.mu.
	ready             bool
	initial, snapshot session.ObservationSnapshot
	pending           []Update
	sequence          uint64
	generation        uint64
	err               error
}

func (sub *Subscription) Initial() session.ObservationSnapshot {
	sub.service.mu.Lock()
	defer sub.service.mu.Unlock()
	return sub.initial.Clone()
}
func (sub *Subscription) Next(ctx context.Context) (Update, error) {
	if !sub.reading.CompareAndSwap(false, true) {
		return Update{}, ErrBusy
	}
	defer sub.reading.Store(false)
	for {
		sub.operation.Lock()
		s := sub.service
		s.mu.Lock()
		if sub.err != nil {
			err := sub.err
			s.mu.Unlock()
			sub.operation.Unlock()
			return Update{}, err
		}
		if len(sub.pending) > 0 {
			u := sub.pending[0]
			sub.pending[0] = Update{}
			sub.pending = sub.pending[1:]
			sub.sequence++
			u.DeliverySequence = sub.sequence
			s.mu.Unlock()
			sub.operation.Unlock()
			return u.clone(), nil
		}
		s.mu.Unlock()
		sub.operation.Unlock()
		select {
		case <-ctx.Done():
			return Update{}, ctx.Err()
		case <-sub.notify:
		}
	}
}
func (sub *Subscription) Resnapshot(ctx context.Context) (session.ObservationSnapshot, error) {
	sub.operation.Lock()
	defer sub.operation.Unlock()
	s := sub.service
	s.mu.Lock()
	if sub.err != nil {
		err := sub.err
		s.mu.Unlock()
		return session.ObservationSnapshot{}, err
	}
	s.workers.Add(1)
	defer s.workers.Done()
	s.mu.Unlock()
	readCtx, cancel := context.WithTimeout(ctx, s.options.ReadTimeout)
	stop := context.AfterFunc(sub.ctx, cancel)
	defer func() { stop(); cancel() }()
	snap, err := s.reader.ReadObservationSnapshot(readCtx, sub.group.id, s.options.Snapshot)
	s.mu.Lock()
	defer s.mu.Unlock()
	if sub.err != nil {
		return session.ObservationSnapshot{}, sub.err
	}
	if err != nil {
		s.terminateLocked(sub, safeReadError(err))
		return session.ObservationSnapshot{}, sub.err
	}
	if !validSnapshot(snap, sub.group.id) || snap.Watermark.StoreID != sub.snapshot.Watermark.StoreID || snap.Watermark.Revision < sub.snapshot.Watermark.Revision {
		s.terminateLocked(sub, ErrResyncRequired)
		return session.ObservationSnapshot{}, sub.err
	}
	sub.generation++
	sub.pending = nil
	sub.snapshot = snap.Clone()
	s.seedLocked(sub)
	if sub.err != nil {
		return session.ObservationSnapshot{}, sub.err
	}
	return snap.Clone(), nil
}
func (sub *Subscription) Close() {
	if sub == nil {
		return
	}
	s := sub.service
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terminateLocked(sub, ErrClosed)
}
