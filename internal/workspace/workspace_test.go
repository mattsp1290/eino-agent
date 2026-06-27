package workspace

import (
	"context"
	"testing"
)

func TestLockerDoReturnsWhenCanceledWhileWaiting(t *testing.T) {
	t.Parallel()

	locker := &Locker{}
	locked := make(chan struct{})
	release := make(chan struct{})
	go func() {
		err := locker.Do(context.Background(), "root", func() error {
			close(locked)
			<-release
			return nil
		})
		if err != nil {
			t.Errorf("first lock error = %v", err)
		}
	}()
	<-locked
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := locker.Do(ctx, "root", func() error {
		t.Fatal("canceled waiter entered critical section")
		return nil
	}); err != context.Canceled {
		t.Fatalf("canceled wait error = %v, want context.Canceled", err)
	}
	close(release)
}
