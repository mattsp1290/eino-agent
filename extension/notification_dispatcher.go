package extension

import "context"

const notificationQueueSize = 128

type notificationTask struct {
	run     func()
	release func()
}

func (p *Plan) startNotificationWorker() {
	if p == nil {
		return
	}
	p.notifications = make(chan notificationTask, notificationQueueSize)
	p.notifyDone = make(chan struct{})
	go func() {
		defer close(p.notifyDone)
		for task := range p.notifications {
			func() {
				defer task.release()
				task.run()
			}()
		}
	}()
}

func (p *Plan) enqueueNotification(entry plannedEntry, run func()) bool {
	if p == nil || run == nil || entry.retain == nil {
		return false
	}
	p.notifyMu.Lock()
	defer p.notifyMu.Unlock()
	if p.notifyClosed || p.notifications == nil {
		return false
	}
	release := entry.retain()
	select {
	case p.notifications <- notificationTask{run: run, release: release}:
		return true
	default:
		release()
		return false
	}
}

// Flush waits until every notification accepted before the call has finished.
// It is intended for bounded shutdown and deterministic tests; normal runtime
// execution must not wait for observer callbacks.
func (p *Plan) Flush(ctx context.Context) error {
	if p == nil {
		return nil
	}
	done := make(chan struct{})
	p.notifyMu.Lock()
	if p.notifyClosed || p.notifications == nil {
		drained := p.notifyDone
		p.notifyMu.Unlock()
		if drained == nil {
			return nil
		}
		select {
		case <-drained:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case p.notifications <- notificationTask{run: func() { close(done) }, release: func() {}}:
		p.notifyMu.Unlock()
	case <-ctx.Done():
		p.notifyMu.Unlock()
		return ctx.Err()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
