package workspace

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
)

var ErrInvalidRoot = errors.New("invalid workspace root")

// CanonicalRoot returns the symlink-resolved absolute workspace root used for
// tool construction and per-root serialization.
func CanonicalRoot(root string) (string, error) {
	if root == "" {
		return "", fmt.Errorf("%w: root required", ErrInvalidRoot)
	}
	if !filepath.IsAbs(root) {
		return "", fmt.Errorf("%w: root must be absolute: %s", ErrInvalidRoot, root)
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("%w: resolve %s: %v", ErrInvalidRoot, root, err)
	}
	return resolved, nil
}

// Locker serializes work by canonical workspace key while allowing independent
// roots to proceed concurrently.
type Locker struct {
	mu    sync.Mutex
	locks map[string]chan struct{}
}

// Do runs fn while holding the lock for key.
func (l *Locker) Do(ctx context.Context, key string, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lock := l.lockFor(key)
	select {
	case lock <- struct{}{}:
		defer func() { <-lock }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn()
}

func (l *Locker) lockFor(key string) chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.locks == nil {
		l.locks = map[string]chan struct{}{}
	}
	lock := l.locks[key]
	if lock == nil {
		lock = make(chan struct{}, 1)
		l.locks[key] = lock
	}
	return lock
}
