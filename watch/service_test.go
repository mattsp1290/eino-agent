package watch

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/sqlite"
)

func testOptions() Options {
	return Options{Snapshot: session.ObservationLimits{MaxMessages: 10, MaxTools: 10, MaxParts: 20, MaxSnapshotBytes: 64000, MaxTextBytes: 1000}, PollInterval: time.Millisecond * 5, ReadTimeout: time.Second, MaxSubscriptions: 10, MaxWatchedSessions: 5, MaxLiveRuns: 2, MaxLiveTextBytes: 100, PendingUpdates: 10}
}
func setup(t *testing.T, o Options) (*Service, *sqlite.Store) {
	t.Helper()
	st, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewService(st, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
		_ = st.Close()
	})
	return s, st
}
func admit(t *testing.T, st *sqlite.Store, id session.ID, rid session.RunID, mid session.MessageID) session.ExecutionStore {
	t.Helper()
	ctx := t.Context()
	if _, err := st.CreateSession(ctx, session.Session{ID: id}); err != nil {
		t.Fatal(err)
	}
	r, err := st.AdmitRun(ctx, session.Run{ID: rid, SessionID: id, Status: session.RunRunning, ClaimToken: "f"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ex := st.Execution(session.RunFence{RunID: r.ID, ClaimToken: r.ClaimToken})
	if _, err = ex.AppendMessage(ctx, session.Message{ID: mid, SessionID: id, RunID: rid, Role: session.RoleAssistant}); err != nil {
		t.Fatal(err)
	}
	return ex
}
func next(t *testing.T, sub *Subscription) Update {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	u, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
func durable(t *testing.T, sub *Subscription, predicate func(session.ObservationSnapshot) bool) Update {
	t.Helper()
	for {
		u := next(t, sub)
		if u.Kind == Durable && predicate(u.Snapshot) {
			return u
		}
	}
}
func TestWatchAdmissionPollingAndFinalization(t *testing.T) {
	s, st := setup(t, testOptions())
	sub, err := s.Watch(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if sub.Initial().Exists {
		t.Fatal("watch created session")
	}
	ex := admit(t, st, "s", "r", "m")
	u := durable(t, sub, func(v session.ObservationSnapshot) bool { return len(v.Messages) == 1 })
	id := s.BeginAttempt(LiveIdentity{SessionID: "s", RunID: "r", MessageID: "m", RequestID: "req", Attempt: 1})
	s.AppendText(id, "partial")
	live := next(t, sub)
	if live.Kind != Live || live.Live.Text != "partial" || live.DeliverySequence <= u.DeliverySequence {
		t.Fatal(live)
	}
	if err := ex.WithinTx(t.Context(), func(ctx context.Context, tx session.ExecutionStore) error {
		if _, err := tx.AppendPart(ctx, session.Part{ID: "p", SessionID: "s", RunID: "r", MessageID: "m", Kind: session.PartText, Payload: []byte(`{"text":"final"}`)}); err != nil {
			return err
		}
		return tx.FinalizeAssistantMessage(ctx, "m")
	}); err != nil {
		t.Fatal(err)
	}
	final := durable(t, sub, func(v session.ObservationSnapshot) bool { return v.Messages[0].Finalized })
	if final.Snapshot.Messages[0].Text != "final" {
		t.Fatal(final)
	}
	s.AppendText(id, "delayed")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err = sub.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
func TestResnapshotReseedsPausedTextAndFinishRemoves(t *testing.T) {
	s, st := setup(t, testOptions())
	admit(t, st, "s", "r", "m")
	id := s.BeginAttempt(LiveIdentity{SessionID: "s", RunID: "r", MessageID: "m", RequestID: "one", Attempt: 1})
	s.AppendText(id, "paused")
	sub, err := s.Watch(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = sub.Resnapshot(t.Context()); err != nil {
		t.Fatal(err)
	}
	if u := next(t, sub); u.Kind != Live || u.Live.Text != "paused" {
		t.Fatal(u)
	}
	id2 := s.BeginAttempt(LiveIdentity{SessionID: "s", RunID: "r", MessageID: "m", RequestID: "two", Attempt: 2})
	s.AppendText(id, "stale")
	s.AppendText(id2, "fresh")
	if u := next(t, sub); u.Kind != Live || u.Live.Text != "fresh" || u.Live.Identity != id2 {
		t.Fatal(u)
	}
	s.FinishAttempt(id2)
	removed := next(t, sub)
	if removed.Kind != LiveUnavailable {
		t.Fatal(removed)
	}
	s.AppendText(id2, "late")
	if _, err = sub.Resnapshot(t.Context()); err != nil {
		t.Fatal(err)
	}
	if u := next(t, sub); u.Kind != LiveUnavailable || u.Live.PublicationVersion < removed.Live.PublicationVersion {
		t.Fatal(u)
	}
}
func TestCacheBeforeVisibilityAndWindowEviction(t *testing.T) {
	o := testOptions()
	o.Snapshot.MaxMessages = 1
	s, st := setup(t, o)
	sub, err := s.Watch(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	id := s.BeginAttempt(LiveIdentity{SessionID: "s", RunID: "r", MessageID: "a", RequestID: "one"})
	s.AppendText(id, "early")
	ex := admit(t, st, "s", "r", "a")
	durable(t, sub, func(v session.ObservationSnapshot) bool { return len(v.Messages) == 1 })
	if u := next(t, sub); u.Kind != Live || u.Live.Text != "early" {
		t.Fatal(u)
	}
	if _, err = ex.AppendMessage(t.Context(), session.Message{ID: "z", SessionID: "s", RunID: "r", Role: session.RoleUser, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	durable(t, sub, func(v session.ObservationSnapshot) bool { return v.Messages[0].ID == "z" })
	s.AppendText(id, "delayed")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err = sub.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
func TestOverflowTerminatesWithoutLaterPublication(t *testing.T) {
	o := testOptions()
	o.PendingUpdates = 1
	s, st := setup(t, o)
	sub, err := s.Watch(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	admit(t, st, "s", "r", "m")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	// A replacement and eligible unavailable notice exceed this budget. Terminal
	// status lives outside the full queue and must release the worker immediately.
	for {
		_, err = sub.Next(ctx)
		if err != nil {
			break
		}
	}
	if !errors.Is(err, ErrResyncRequired) {
		t.Fatal(err)
	}
	if _, err = sub.Resnapshot(ctx); !errors.Is(err, ErrResyncRequired) {
		t.Fatal(err)
	}
	s.mu.Lock()
	count := len(s.sessions)
	s.mu.Unlock()
	if count != 0 {
		t.Fatal(count)
	}
	reattached, err := s.Watch(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	defer reattached.Close()
	if !reattached.Initial().Exists {
		t.Fatal("missing recovered state")
	}
}
func TestCacheLimitsIsolationAndDetach(t *testing.T) {
	o := testOptions()
	o.MaxLiveRuns = 1
	o.MaxLiveTextBytes = 4
	s, st := setup(t, o)
	ex := admit(t, st, "s", "r", "m")
	admit(t, st, "other", "r2", "m2")
	id := s.BeginAttempt(LiveIdentity{SessionID: "s", RunID: "r", MessageID: "m", RequestID: "one"})
	s.AppendText(id, "1234")
	s.AppendText(id, "5")
	sub, err := s.Watch(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if u := next(t, sub); u.Kind != LiveUnavailable || u.Live.Text != "" {
		t.Fatal(u)
	}
	other := s.BeginAttempt(LiveIdentity{SessionID: "other", RunID: "r2", MessageID: "m2", RequestID: "two"})
	s.AppendText(other, "no")
	otherSub, err := s.Watch(t.Context(), "other")
	if err != nil {
		t.Fatal(err)
	}
	if u := next(t, otherSub); u.Kind != LiveUnavailable || u.Live.Identity.SessionID != "other" {
		t.Fatal(u)
	}
	sub.Close()
	if err = ex.FinalizeAssistantMessage(t.Context(), "m"); err != nil {
		t.Fatal("detach changed execution", err)
	}
	if err = s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = ex.FinalizeAssistantMessage(t.Context(), "m"); err != nil {
		t.Fatal("service close changed execution", err)
	}
	s.AppendText(id, "after close")
}

type errorReader struct {
	entered chan struct{}
	closed  chan struct{}
}

func (r *errorReader) ReadObservationRevision(ctx context.Context, _ session.ID) (session.ObservationWatermark, error) {
	<-ctx.Done()
	return session.ObservationWatermark{}, ctx.Err()
}
func (r *errorReader) ReadObservationSnapshot(ctx context.Context, _ session.ID, _ session.ObservationLimits) (session.ObservationSnapshot, error) {
	close(r.entered)
	<-ctx.Done()
	close(r.closed)
	return session.ObservationSnapshot{}, errors.New("SECRET raw database error")
}
func TestCloseDuringInitialRead(t *testing.T) {
	r := &errorReader{entered: make(chan struct{}), closed: make(chan struct{})}
	s, err := NewService(r, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := s.Watch(t.Context(), "s"); done <- err }()
	<-r.entered
	if err = s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-r.closed
	if err = <-done; !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestNextCancellationBusyAndDetachedCopies(t *testing.T) {
	s, _ := setup(t, testOptions())
	sub, err := s.Watch(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = sub.Next(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	sub.reading.Store(true)
	if _, err = sub.Next(t.Context()); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	sub.reading.Store(false)
	if _, err = sub.Resnapshot(t.Context()); err != nil {
		t.Fatal(err)
	}
	sub.Close()
	sub.Close()
	if _, err = sub.Next(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
