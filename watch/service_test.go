package watch

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync/atomic"
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
	st, stPool, err := openTestSQLite(t.Context(), filepath.Join(t.TempDir(), "store.db"))
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
		_ = stPool.Close()
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
	if notice := next(t, sub); notice.Kind != LiveUnavailable || notice.Live.Identity.MessageID != "" {
		t.Fatal(notice)
	}
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
	if notice := next(t, sub); notice.Kind != LiveUnavailable || notice.Live.Identity.MessageID != "" {
		t.Fatal(notice)
	}
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

type pausedInitialReader struct {
	*sqlite.Store
	snapshots                                                    atomic.Int32
	revisions                                                    atomic.Int32
	initialRead, releaseInitial, workerSnapshot, workerRechecked chan struct{}
}

func (r *pausedInitialReader) ReadObservationSnapshot(ctx context.Context, id session.ID, l session.ObservationLimits) (session.ObservationSnapshot, error) {
	n := r.snapshots.Add(1)
	snapshot, err := r.Store.ReadObservationSnapshot(ctx, id, l)
	switch n {
	case 1:
		close(r.initialRead)
		select {
		case <-r.releaseInitial:
		case <-ctx.Done():
			return session.ObservationSnapshot{}, ctx.Err()
		}
	case 2:
		close(r.workerSnapshot)
	}
	return snapshot, err
}
func (r *pausedInitialReader) ReadObservationRevision(ctx context.Context, id session.ID) (session.ObservationWatermark, error) {
	if r.revisions.Add(1) == 2 {
		close(r.workerRechecked)
	}
	return r.Store.ReadObservationRevision(ctx, id)
}
func TestInitialReadCannotLoseNewerWorkerSnapshot(t *testing.T) {
	st, stPool, err := openTestSQLite(t.Context(), filepath.Join(t.TempDir(), "initial-race.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stPool.Close() }()
	reader := &pausedInitialReader{Store: st, initialRead: make(chan struct{}), releaseInitial: make(chan struct{}), workerSnapshot: make(chan struct{}), workerRechecked: make(chan struct{})}
	options := testOptions()
	options.PollInterval = time.Hour
	service, err := NewService(reader, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.Close(context.Background()) }()
	attached := make(chan *Subscription, 1)
	failed := make(chan error, 1)
	go func() {
		sub, err := service.Watch(t.Context(), "s")
		if err != nil {
			failed <- err
			return
		}
		attached <- sub
	}()
	<-reader.initialRead
	if _, err = st.CreateSession(t.Context(), session.Session{ID: "s"}); err != nil {
		t.Fatal(err)
	}
	service.Hint("s")
	<-reader.workerSnapshot
	service.Hint("s")
	<-reader.workerRechecked
	// The worker has accepted the new revision while the subscriber still has
	// only its earlier committed read. Releasing that read must not strand it.
	close(reader.releaseInitial)
	var sub *Subscription
	select {
	case sub = <-attached:
	case err = <-failed:
		t.Fatal(err)
	}
	defer sub.Close()
	if sub.Initial().Exists {
		t.Fatal("barrier failed to hold old initial state")
	}
	service.Hint("s")
	u := next(t, sub)
	if u.Kind != Durable || !u.Snapshot.Exists {
		t.Fatal(u)
	}
}

type pausedResnapshotReader struct {
	*sqlite.Store
	reads             atomic.Int32
	captured, release chan struct{}
}

func (r *pausedResnapshotReader) ReadObservationSnapshot(ctx context.Context, id session.ID, l session.ObservationLimits) (session.ObservationSnapshot, error) {
	n := r.reads.Add(1)
	snap, err := r.Store.ReadObservationSnapshot(ctx, id, l)
	if n == 2 {
		close(r.captured)
		select {
		case <-r.release:
		case <-ctx.Done():
			return session.ObservationSnapshot{}, ctx.Err()
		}
	}
	return snap, err
}
func TestResnapshotAcceptsConcurrentNewerPoll(t *testing.T) {
	st, stPool, err := openTestSQLite(t.Context(), filepath.Join(t.TempDir(), "resnapshot-race.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stPool.Close() }()
	reader := &pausedResnapshotReader{Store: st, captured: make(chan struct{}), release: make(chan struct{})}
	options := testOptions()
	options.PollInterval = time.Hour
	service, err := NewService(reader, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.Close(context.Background()) }()
	sub, err := service.Watch(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	done := make(chan error, 1)
	result := make(chan session.ObservationSnapshot, 1)
	go func() { snap, err := sub.Resnapshot(t.Context()); result <- snap; done <- err }()
	<-reader.captured
	if _, err = st.CreateSession(t.Context(), session.Session{ID: "s"}); err != nil {
		t.Fatal(err)
	}
	service.Hint("s")
	// A notification is sent under the publication mutex only after the newer
	// snapshot has been accepted. Next stays serialized behind Resnapshot.
	select {
	case <-sub.notify:
	case <-time.After(time.Second):
		t.Fatal("worker did not publish")
	}
	close(reader.release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	snap := <-result
	if !snap.Exists || snap.Watermark.Revision == 0 {
		t.Fatal(snap)
	}
	if _, err = sub.Resnapshot(t.Context()); err != nil {
		t.Fatal("healthy forward read terminated subscription", err)
	}
}

func TestSlowAndFastSubscribersConvergeIndependently(t *testing.T) {
	service, st := setup(t, testOptions())
	admit(t, st, "s", "r", "m")
	slow, err := service.Watch(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	fast, err := service.Watch(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()
	id := service.BeginAttempt(LiveIdentity{SessionID: "s", RunID: "r", MessageID: "m", RequestID: "req"})
	for range 50 {
		service.AppendText(id, "x")
		u := next(t, fast)
		if u.Kind != Live {
			t.Fatal(u)
		}
	}
	u := next(t, slow)
	if u.Kind != Live || len(u.Live.Text) != 50 {
		t.Fatal("slow consumer lost full replacement", u)
	}
	if _, err := fast.Resnapshot(t.Context()); err != nil {
		t.Fatal(err)
	}
	if u := next(t, fast); u.Kind != Live || len(u.Live.Text) != 50 {
		t.Fatal(u)
	}
}

type noncooperativeReader struct{ entered, release chan struct{} }

func (r *noncooperativeReader) ReadObservationRevision(context.Context, session.ID) (session.ObservationWatermark, error) {
	return session.ObservationWatermark{}, errors.New("PRIVATE_ERROR")
}
func (r *noncooperativeReader) ReadObservationSnapshot(context.Context, session.ID, session.ObservationLimits) (session.ObservationSnapshot, error) {
	close(r.entered)
	<-r.release
	return session.ObservationSnapshot{}, errors.New("PRIVATE_ERROR")
}
func TestServiceCloseTimeoutCanBeRetried(t *testing.T) {
	reader := &noncooperativeReader{entered: make(chan struct{}), release: make(chan struct{})}
	options := testOptions()
	options.PollInterval = time.Hour
	service, err := NewService(reader, options)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := service.Watch(t.Context(), "s"); done <- err }()
	<-reader.entered
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err = service.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(reader.release)
	if err = <-done; !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err = service.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type failingRevisionReader struct {
	*sqlite.Store
	fail atomic.Bool
}

func (r *failingRevisionReader) ReadObservationRevision(ctx context.Context, id session.ID) (session.ObservationWatermark, error) {
	if r.fail.Load() {
		return session.ObservationWatermark{}, errors.New("PRIVATE_DATABASE_ERROR")
	}
	return r.Store.ReadObservationRevision(ctx, id)
}
func TestReadFailureTerminatesWithoutRawError(t *testing.T) {
	_, st := setup(t, testOptions())
	reader := &failingRevisionReader{Store: st}
	service, err := NewService(reader, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.Close(context.Background()) }()
	sub, err := service.Watch(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	reader.fail.Store(true)
	service.Hint("s")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err = sub.Next(ctx); !errors.Is(err, session.ErrObservationStore) {
		t.Fatal(err)
	}
	if _, err = sub.Resnapshot(ctx); !errors.Is(err, session.ErrObservationStore) {
		t.Fatal(err)
	}
}
func TestServiceCapacityAndOptions(t *testing.T) {
	service, st := setup(t, testOptions())
	for _, options := range []Options{{}, func() Options { o := testOptions(); o.PendingUpdates = 0; return o }(), func() Options { o := testOptions(); o.Snapshot.MaxMessages = -1; return o }()} {
		if _, err := NewService(st, options); !errors.Is(err, ErrOptions) {
			t.Fatal(err)
		}
	}
	var nilStore *sqlite.Store
	if _, err := NewService(nilStore, testOptions()); !errors.Is(err, ErrOptions) {
		t.Fatal(err)
	}
	if _, err := service.Watch(t.Context(), ""); !errors.Is(err, ErrOptions) {
		t.Fatal(err)
	}
	service.options.MaxSubscriptions = 1
	sub, err := service.Watch(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Watch(t.Context(), "s"); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	sub.Close()
	again, err := service.Watch(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	again.Close()
}

func TestStoreIncarnationAndRevisionRegressionRequireReattach(t *testing.T) {
	for _, change := range []string{"incarnation", "revision"} {
		t.Run(change, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "watermark.db")
			st, stPool, err := openTestSQLite(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = stPool.Close() }()
			admit(t, st, "s", "r", "m")
			service, err := NewService(st, testOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = service.Close(context.Background()) }()
			sub, err := service.Watch(t.Context(), "s")
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			raw, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = raw.Close() }()
			raw.SetMaxOpenConns(1)
			if _, err = raw.ExecContext(t.Context(), "PRAGMA busy_timeout=5000"); err != nil {
				t.Fatal(err)
			}
			query := "UPDATE observation_store SET incarnation = lower(hex(randomblob(16)))"
			if change == "revision" {
				query = "UPDATE observation_revisions SET revision = 1 WHERE session_id = CAST('s' AS BLOB)"
			}
			if _, err = raw.ExecContext(t.Context(), query); err != nil {
				t.Fatal(err)
			}
			service.Hint("s")
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			for {
				_, err = sub.Next(ctx)
				if err != nil {
					break
				}
			}
			if !errors.Is(err, ErrResyncRequired) {
				t.Fatal(err)
			}
		})
	}
}
