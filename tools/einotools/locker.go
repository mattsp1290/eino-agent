package einotools

import (
	"context"
	"sync"
)

var standardLocks keyedLocker

type keyedLocker struct {
	mu      sync.Mutex
	entries map[string]*lockEntry
}

type lockEntry struct {
	token chan struct{}
	refs  int
}

func (l *keyedLocker) Do(ctx context.Context, key string, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entry := l.retain(key)
	select {
	case entry.token <- struct{}{}:
		defer func() {
			<-entry.token
			l.release(key, entry)
		}()
	case <-ctx.Done():
		l.release(key, entry)
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn()
}

func (l *keyedLocker) retain(key string) *lockEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.entries == nil {
		l.entries = make(map[string]*lockEntry)
	}
	entry := l.entries[key]
	if entry == nil {
		entry = &lockEntry{token: make(chan struct{}, 1)}
		l.entries[key] = entry
	}
	entry.refs++
	return entry
}

func (l *keyedLocker) release(key string, entry *lockEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry.refs--
	if entry.refs == 0 && l.entries[key] == entry {
		delete(l.entries, key)
	}
}

func (l *keyedLocker) idleEntries() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}
