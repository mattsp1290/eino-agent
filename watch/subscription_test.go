package watch

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/sqlite"
)

type operationAdmissionReader struct {
	*sqlite.Store
	reads            atomic.Int32
	entered, release chan struct{}
}

func (r *operationAdmissionReader) ReadObservationSnapshot(ctx context.Context, id session.ID, limits session.ObservationLimits) (session.ObservationSnapshot, error) {
	snapshot, err := r.Store.ReadObservationSnapshot(ctx, id, limits)
	if r.reads.Add(1) == 2 {
		close(r.entered)
		// Hold even after cancellation to model a store still unwinding its
		// read. Other callers must not need this operation to finish.
		<-r.release
	}
	return snapshot, err
}

func TestOperationAdmissionCancellation(t *testing.T) {
	for _, operation := range []string{"Next", "Resnapshot"} {
		for _, terminate := range []string{"deadline", "close"} {
			t.Run(operation+"/"+terminate, func(t *testing.T) {
				store, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "admission.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = store.Close() }()
				reader := &operationAdmissionReader{Store: store, entered: make(chan struct{}), release: make(chan struct{})}
				options := testOptions()
				options.PollInterval = time.Hour
				service, err := NewService(reader, options)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = service.Close(context.Background()) }()
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(reader.release) }) }
				defer release()
				sub, err := service.Watch(t.Context(), "s")
				if err != nil {
					t.Fatal(err)
				}
				holder := make(chan error, 1)
				go func() { _, err := sub.Resnapshot(t.Context()); holder <- err }()
				select {
				case <-reader.entered:
				case <-time.After(time.Second):
					t.Fatal("resnapshot did not enter its read")
				}
				ctx := t.Context()
				want := ErrClosed
				if terminate == "deadline" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
					defer cancel()
					want = context.DeadlineExceeded
				}
				waiting := make(chan error, 1)
				go func() {
					if operation == "Next" {
						_, err := sub.Next(ctx)
						waiting <- err
					} else {
						_, err := sub.Resnapshot(ctx)
						waiting <- err
					}
				}()
				if terminate == "close" {
					sub.Close()
				}
				select {
				case err := <-waiting:
					if !errors.Is(err, want) {
						t.Fatalf("waiting operation: got %v, want %v", err, want)
					}
				case <-time.After(time.Second):
					t.Fatal("waiting operation needed the unrelated read to finish")
				}
				if got := reader.reads.Load(); got != 2 {
					t.Fatalf("canceled admission started a store read: %d", got)
				}
				release()
				err = <-holder
				if terminate == "close" {
					if !errors.Is(err, ErrClosed) {
						t.Fatalf("active resnapshot after close: %v", err)
					}
				} else {
					if err != nil {
						t.Fatalf("canceled waiter affected active resnapshot: %v", err)
					}
					if _, err := sub.Resnapshot(t.Context()); err != nil {
						t.Fatalf("canceled waiter terminated subscription: %v", err)
					}
				}
			})
		}
	}
}
