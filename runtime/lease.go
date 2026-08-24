package runtime

import (
	"context"
	"sync"
	"time"
)

type runLeaseHeartbeat struct {
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	cancel   context.CancelFunc
	mu       sync.Mutex
	err      error
}

func (e *runExecution) startLease(ctx context.Context, duration time.Duration) context.Context {
	if e == nil || e.store == nil || duration <= 0 {
		return ctx
	}
	runCtx, cancel := context.WithCancel(ctx)
	interval := duration / 3
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	heartbeat := &runLeaseHeartbeat{stop: make(chan struct{}), done: make(chan struct{}), cancel: cancel}
	e.lease = heartbeat
	go func() {
		defer close(heartbeat.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeat.stop:
				return
			case <-runCtx.Done():
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(context.WithoutCancel(runCtx), interval)
				_, err := e.store.RenewRunLease(renewCtx, duration)
				renewCancel()
				if err != nil {
					heartbeat.mu.Lock()
					heartbeat.err = err
					heartbeat.mu.Unlock()
					cancel()
					return
				}
			}
		}
	}()
	return runCtx
}

func (e *runExecution) stopLease() error {
	if e == nil || e.lease == nil {
		return nil
	}
	heartbeat := e.lease
	heartbeat.stopOnce.Do(func() { close(heartbeat.stop) })
	<-heartbeat.done
	heartbeat.cancel()
	heartbeat.mu.Lock()
	defer heartbeat.mu.Unlock()
	return heartbeat.err
}
