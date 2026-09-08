package watch

import (
	"context"
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
	operation chan struct{}
	reading   atomic.Bool
	// Below protected by service.mu.
	ready             bool
	initial, snapshot session.ObservationSnapshot
	pending           []Update
	sequence          uint64
	err               error
}

func (sub *Subscription) Initial() session.ObservationSnapshot {
	sub.service.mu.Lock()
	defer sub.service.mu.Unlock()
	return sub.initial.Clone()
}

// Admission must remain cancelable while another operation is reading the
// store. The gate still serializes snapshot replacement with update delivery.
func (sub *Subscription) acquireOperation(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sub.ctx.Done():
		sub.service.mu.Lock()
		defer sub.service.mu.Unlock()
		if sub.err != nil {
			return sub.err
		}
		return ErrClosed
	case sub.operation <- struct{}{}:
		if err := ctx.Err(); err != nil {
			sub.releaseOperation()
			return err
		}
		return nil
	}
}

func (sub *Subscription) releaseOperation() { <-sub.operation }

func (sub *Subscription) Next(ctx context.Context) (Update, error) {
	if !sub.reading.CompareAndSwap(false, true) {
		return Update{}, ErrBusy
	}
	defer sub.reading.Store(false)
	for {
		if err := sub.acquireOperation(ctx); err != nil {
			return Update{}, err
		}
		s := sub.service
		s.mu.Lock()
		if sub.err != nil {
			err := sub.err
			s.mu.Unlock()
			sub.releaseOperation()
			return Update{}, err
		}
		if len(sub.pending) > 0 {
			u := sub.pending[0]
			sub.pending[0] = Update{}
			sub.pending = sub.pending[1:]
			sub.sequence++
			u.DeliverySequence = sub.sequence
			s.mu.Unlock()
			sub.releaseOperation()
			return u.clone(), nil
		}
		s.mu.Unlock()
		sub.releaseOperation()
		select {
		case <-ctx.Done():
			return Update{}, ctx.Err()
		case <-sub.notify:
		}
	}
}
func (sub *Subscription) Resnapshot(ctx context.Context) (session.ObservationSnapshot, error) {
	if err := sub.acquireOperation(ctx); err != nil {
		return session.ObservationSnapshot{}, err
	}
	defer sub.releaseOperation()
	s := sub.service
	s.mu.Lock()
	if sub.err != nil {
		err := sub.err
		s.mu.Unlock()
		return session.ObservationSnapshot{}, err
	}
	baseline := sub.snapshot.Watermark
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
	if !validSnapshot(snap, sub.group.id) || snap.Watermark.StoreID != baseline.StoreID || snap.Watermark.Revision < baseline.Revision {
		s.terminateLocked(sub, ErrResyncRequired)
		return session.ObservationSnapshot{}, sub.err
	}
	if sub.snapshot.Watermark.Revision > snap.Watermark.Revision {
		snap = sub.snapshot.Clone()
	}
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
