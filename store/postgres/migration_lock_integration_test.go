//go:build postgres_integration

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
)

func testConcurrentMigration(t *testing.T, server *testpostgres.Server) {
	database := server.Database(t)
	first, second, observer := database.Open(t), database.Open(t), database.Open(t)
	entered := make(chan int, 2)
	acquired, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	files := migrationFS(t)
	results := make(chan error, 2)
	go func() {
		results <- migrate(t.Context(), first, files, gateLocker{migrationDelegate(t), entered, acquired, release})
	}()
	firstPID := receiveMigration(t, entered)
	receiveMigration(t, acquired)
	go func() {
		results <- migrate(t.Context(), second, files, gateLocker{migrationDelegate(t), entered, nil, nil})
	}()
	if secondPID := receiveMigration(t, entered); firstPID == secondPID {
		t.Fatal("migrators share a backend")
	}
	if schemaHistory(t, observer) != "absent" {
		t.Fatal("history written before validating lock returned")
	}
	select {
	case err := <-results:
		t.Fatalf("migrator escaped held lock: %v", err)
	default:
	}
	close(release)
	for range 2 {
		if err := receiveMigration(t, results); err != nil {
			t.Fatal(err)
		}
	}
	assertMigrationState(t, observer, schemaCurrent)
}

// testMigrationCancelBeforeBindAbortsPoolWaitPromptly guards the liveness
// regression d4f325b introduced: migrationContext.interrupt stopped
// canceling m.Context at all, so a parent cancellation became invisible to
// everything that runs before SessionLock ever binds a connection -
// concretely, goose's own p.db.Conn(m.Context) pool wait. With the pool
// saturated by a connection this test holds open, and the parent already
// canceled before migrate starts, migrate must return promptly with
// context.Canceled; at d4f325b it instead blocked until the held connection
// was released (here, until the test's own cleanup). See
// TestMigrationContextInterruptCancelsOnlyWhenUnbound for the matching
// white-box proof that interrupt only cancels m.Context while unbound, and
// never once a *sql.Conn is bound.
func testMigrationCancelBeforeBindAbortsPoolWaitPromptly(t *testing.T, server *testpostgres.Server) {
	database := server.Database(t)
	db := database.Open(t)
	db.SetMaxOpenConns(1)
	held, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := held.Close(); err != nil {
			t.Error(err)
		}
	})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result := make(chan error, 1)
	go func() { result <- migrate(ctx, db, migrationFS(t), migrationDelegate(t)) }()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled parent with a saturated pool: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("migrate ignored a canceled parent while waiting on a saturated pool " +
			"(interrupt must cancel m.Context while nothing is bound yet)")
	}
}

// testMigrationDiscardAfterInterruptWithNoFurtherIOAvoidsMisleadingError
// reproduces the ownership-model review's S1 finding directly against a
// real connection: on a success-shaped stop() after an interrupt where the
// owner attempted no further I/O in between, pgx has not yet observed the
// transport failure, so native.IsClosed() is false. discardPGXConnection
// used to call native.Close over that already-dead socket unconditionally
// and report "use of closed network connection" - the exact misleading
// error the code comments claim to avoid - alongside the real cause. bind
// -> interrupt -> stop, with no query in between, forces exactly that
// ordering; testCanceledMigration and friends do not, because the owner's
// own query is what observes the failure there.
func testMigrationDiscardAfterInterruptWithNoFurtherIOAvoidsMisleadingError(t *testing.T, server *testpostgres.Server) {
	database := server.Database(t)
	db := database.Open(t)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
			t.Error(err)
		}
	})

	m := newMigrationContext(t.Context())
	if err := m.bind(conn); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("probe interrupt")
	m.interrupt(cause)
	stopErr := m.stop()
	if !errors.Is(stopErr, cause) {
		t.Fatalf("stop did not report the interrupt cause: %v", stopErr)
	}
	if stopErr != nil && strings.Contains(stopErr.Error(), "use of closed network connection") {
		t.Fatalf("stop reported a misleading transport error alongside the real cause: %v", stopErr)
	}
}

func testCanceledMigration(t *testing.T, server *testpostgres.Server) {
	database := server.Database(t)
	holderDB, waiterDB := database.Open(t), database.Open(t)
	holder := schemaConn(t, holderDB)
	delegate := migrationDelegate(t)
	if err := delegate.SessionLock(t.Context(), holder); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := delegate.SessionUnlock(context.Background(), holder); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, result := make(chan int, 1), make(chan error, 1)
	files := migrationFS(t)
	go func() { result <- migrate(ctx, waiterDB, files, gateLocker{migrationDelegate(t), entered, nil, nil}) }()
	receiveMigration(t, entered)
	cancel()
	if err := receiveMigration(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter: %v", err)
	}
	if schemaHistory(t, waiterDB) != "absent" {
		t.Fatal("canceled waiter wrote history")
	}
	if err := waiterDB.PingContext(t.Context()); err != nil {
		t.Fatal("waiter pool unusable", err)
	}
}

// testCanceledMigrationRace repeats the canceled-waiter scenario against one
// shared server and, using a rendezvous hook rather than scheduling luck,
// asserts that no two discards of the same *sql.Conn ever overlap, and that
// discardObserved actually fired for every iteration (so the assertion
// cannot pass vacuously).
//
// Historically a canceled waiter's connection could be discarded from two
// places at once: the parent-context watcher inside its migrationContext
// (newMigrationContext's context.AfterFunc, firing on the test's cancel())
// discarded the bound *sql.Conn concurrently with
// validatingSessionLocker.SessionLock's own error path, which discarded the
// same *sql.Conn once the delegate's blocked advisory-lock query unblocked
// with that same cancellation error - both calling discardPGXConnection, and
// so conn.Raw, on the same *sql.Conn at once. database/sql's Conn.grabConn
// checks Conn.done before taking Conn.closemu for read, and Conn.close sets
// Conn.done before taking Conn.closemu for write and clearing its
// driverConn - a gap between ANY grab and ANY close, not just two discards.
// A grabConn call that reads done as false just ahead of a concurrent
// close's CAS, then blocks on closemu until that close finishes, resumes
// with a nil driverConn and a nil error, and Conn.Raw dereferences it
// unconditionally - panicking (github.com/mattsp1290/eino-agent issue
// eino-agent-f3i).
//
// Since the ownership-model fix (see migration_context.go), interrupt never
// calls discardPGXConnection at all - only the goroutine that owns the
// *sql.Conn does, from migrationContext.discard or stop. So this specific
// two-discards-overlap can no longer happen structurally, not just by luck:
// the rendezvous hook below holds the first discard call for a *sql.Conn
// open for up to 250ms waiting for a second one that, post-fix, never comes.
// This is a cheap regression guard against something reintroducing a
// non-owner discard call. testCanceledMigrationOwnerRace below targets the
// distinct race that survived the mutex-only fix: owner SQL racing
// interrupt's transport close directly, without going through
// discardPGXConnection at all - and testMigrationLockDelegateError covers
// the delegate-error discard path with no cancellation involved.
func testCanceledMigrationRace(t *testing.T, server *testpostgres.Server) {
	type rendezvous struct {
		inFlight atomic.Int32
		second   chan struct{}
	}
	var states sync.Map // *sql.Conn -> *rendezvous
	var overlapped atomic.Bool
	var entries atomic.Int32
	previous := discardObserved.Load()
	hook := func(conn *sql.Conn, entering bool) {
		v, _ := states.LoadOrStore(conn, &rendezvous{second: make(chan struct{}, 1)})
		st := v.(*rendezvous)
		if !entering {
			st.inFlight.Add(-1)
			return
		}
		entries.Add(1)
		if st.inFlight.Add(1) > 1 {
			overlapped.Store(true)
			select {
			case st.second <- struct{}{}:
			default:
			}
			return
		}
		select {
		case <-st.second: // pre-fix: a second discard for this conn arrived.
		case <-time.After(250 * time.Millisecond): // post-fix: nothing else ever discards this conn.
		}
	}
	discardObserved.Store(&hook)
	t.Cleanup(func() { discardObserved.Store(previous) })

	const iterations = 5
	for i := range iterations {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			testCanceledMigration(t, server)
		})
	}
	if overlapped.Load() {
		t.Fatal("two discards of the same canceled waiter connection overlapped: " +
			"validatingSessionLocker no longer routes every discard through the owning goroutine")
	}
	if got := entries.Load(); got != iterations {
		t.Fatalf("discardObserved observed %d discards, want exactly %d "+
			"(more would mean a canceled waiter's conn is discarded more than once, "+
			"fewer would make the overlap assertion above vacuous)", got, iterations)
	}
}

// failedLock makes SessionLock acquire the real advisory lock and then fail
// with a fixed error, with no cancellation involved anywhere. It exists to
// cover validatingSessionLocker.SessionLock's l.operation.discard() call
// after a plain delegate error (migration_lock.go), which
// testCanceledMigration and testCanceledMigrationRace do not exercise: there,
// the delegate never succeeds and the failure is always a cancellation.
type failedLock struct {
	lock.SessionLocker
	err error
	pid *int
}

func (l failedLock) SessionLock(ctx context.Context, conn *sql.Conn) error {
	if err := l.SessionLocker.SessionLock(ctx, conn); err != nil {
		return err
	}
	if err := conn.QueryRowContext(ctx, `SELECT pg_catalog.pg_backend_pid()`).Scan(l.pid); err != nil {
		return err
	}
	return l.err
}

// testMigrationLockDelegateError covers validatingSessionLocker.SessionLock's
// discard of a *sql.Conn after the delegate acquires the real advisory lock
// and then returns a plain error - no interrupt, no cancellation. Without
// that discard the connection would go back to the pool with the advisory
// lock still held, blocking every future migration.
func testMigrationLockDelegateError(t *testing.T, server *testpostgres.Server) {
	database := server.Database(t)
	waiterDB, observer := database.Open(t), database.Open(t)
	failure := errors.New("injected lock delegate failure")
	var pid int
	locker := failedLock{SessionLocker: migrationDelegate(t), err: failure, pid: &pid}
	if err := migrate(t.Context(), waiterDB, migrationFS(t), locker); !errors.Is(err, failure) {
		t.Fatalf("delegate error lost: %v", err)
	}
	if schemaHistory(t, waiterDB) != "absent" {
		t.Fatal("failed delegate wrote history")
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var exists bool
		if err := observer.QueryRowContext(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$1)`, pid).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("delegate-error backend survived discard")
		case <-tick.C:
		}
	}
	if err := Migrate(t.Context(), observer); err != nil {
		t.Fatal("independent migration blocked after delegate error", err)
	}
}

// gatedSessionLock lets a test control exactly when a SessionLocker's lock
// query is issued. It signals ready once it is about to block, then waits
// for proceed before calling through - so a test can release it at a chosen
// moment instead of relying on scheduling luck.
type gatedSessionLock struct {
	lock.SessionLocker
	ready   chan<- struct{}
	proceed <-chan struct{}
}

func (l gatedSessionLock) SessionLock(ctx context.Context, conn *sql.Conn) error {
	close(l.ready)
	<-l.proceed
	return l.SessionLocker.SessionLock(ctx, conn)
}

// testCanceledMigrationOwnerRace forces the owning goroutine to issue SQL on
// a bound *sql.Conn at the exact moment interrupt has just closed that
// connection's transport, reproducing the interleaving behind
// github.com/mattsp1290/eino-agent issue eino-agent-f3i directly: a foreign
// goroutine (the parent-context watcher) interrupting a *sql.Conn
// concurrently with the owning goroutine's own use of it.
//
// interruptObserved, a test-only hook in migration_context.go, fires inside
// migrationContext.interrupt immediately after it closes the transport,
// while interrupt is still holding migrationContext.mu. The hook signals the
// test and then holds the interrupt path open until released. The test then
// releases both the held interrupt path and a gated delegate whose lock
// query is about to run on the same *sql.Conn at once, so the query is
// dispatched against an already-closed socket while interrupt is still
// mid-flight, and any surviving overlap between the two goroutines'
// database/sql access is exercised rather than avoided by construction.
// Releasing both together does not itself force that overlap - the
// scheduler still decides how the two goroutines interleave - but post-fix
// there is nothing left to overlap with: interrupt only closes the
// transport and never touches database/sql.
//
// This test asserts the ownership invariant two different ways, and they
// are not equally strong:
//
//  1. Deterministic, every iteration: discardObserved's hook below captures
//     the calling goroutine's stack for every discardPGXConnection call and
//     fails immediately if it was reached from migrationContext.interrupt
//     or the parent-context watcher's AfterFunc callback, rather than from
//     migrate's own goroutine. That is a property of who called
//     discardPGXConnection, not of timing, so it cannot pass by luck -
//     confirmed by hand-reverting interrupt to discard directly and
//     watching this assertion fail on iteration 0, both before and after
//     moving the reverted discard ahead of the interruptObserved hook (see
//     the fix's commit message for the exact revert used). This is the
//     runtime counterpart to TestMigrationOwnershipGuard's static check.
//  2. Probabilistic, across the whole loop: the forced interleaving itself
//     (owner SQL dispatched the instant interrupt closes the socket) can
//     still reach database/sql's actual nil-driverConn panic if some other
//     regression this hook's stack check does not name reintroduces the
//     race. Measured against interrupt hand-reverted to discard directly
//     (the pre-fix shape): panics appeared roughly once per 200 iterations
//     across six runs (15, 81, 126, 224, 226, 529) - about 0.5% per
//     iteration, far worse under -race. iterations below is sized to give
//     this secondary signal a real chance without making the suite slow; it
//     is not, by itself, a reliable detector, and the stack assertion above
//     is what actually guarantees a reintroduction fails this test.
func testCanceledMigrationOwnerRace(t *testing.T, server *testpostgres.Server) {
	const iterations = 200
	var hookFired atomic.Int32
	var released, timedOut atomic.Int32

	// Deterministic half of the check (see doc above): a discard reached
	// from a non-owner goroutine fails immediately, regardless of whether
	// this run's forced interleaving happens to panic.
	var foreignDiscard atomic.Bool
	var foreignStack atomic.Pointer[string]
	previousDiscard := discardObserved.Load()
	dhook := func(conn *sql.Conn, entering bool) {
		if !entering {
			return
		}
		buf := make([]byte, 16<<10)
		trace := string(buf[:runtime.Stack(buf, false)])
		if strings.Contains(trace, "migrationContext).interrupt") ||
			strings.Contains(trace, "newMigrationContext.func") {
			foreignDiscard.Store(true)
			foreignStack.Store(&trace)
		}
	}
	discardObserved.Store(&dhook)
	t.Cleanup(func() { discardObserved.Store(previousDiscard) })

	for i := range iterations {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			database := server.Database(t)
			waiterDB := database.Open(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			ready := make(chan struct{})
			proceed := make(chan struct{})
			socketClosed := make(chan struct{})
			release := make(chan struct{})
			var closeSocketClosedOnce sync.Once
			hook := func() {
				// sync.Once, not a bare close: a future change to interrupt
				// (or a second bound context inside this window) firing this
				// hook twice must not turn a test-only double-close into a
				// panic that looks like a product crash.
				closeSocketClosedOnce.Do(func() { close(socketClosed) })
				hookFired.Add(1)
				select {
				case <-release:
					released.Add(1)
				case <-time.After(250 * time.Millisecond):
					timedOut.Add(1)
				}
			}
			previous := interruptObserved.Load()
			interruptObserved.Store(&hook)
			t.Cleanup(func() { interruptObserved.Store(previous) })

			delegate := gatedSessionLock{SessionLocker: migrationDelegate(t), ready: ready, proceed: proceed}
			files := migrationFS(t)
			result := make(chan error, 1)
			go func() { result <- migrate(ctx, waiterDB, files, delegate) }()

			receiveMigration(t, ready) // owner is bound and about to issue its lock query.
			cancel()
			receiveMigration(t, socketClosed) // interrupt has closed the transport and is holding the path open.
			close(proceed)                    // release the owner's query against the now-closed socket...
			close(release)                    // ...and interrupt's held path, together, so any surviving overlap is exercised.

			if err := receiveMigration(t, result); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled waiter with a closed transport: %v", err)
			}
			if schemaHistory(t, waiterDB) != "absent" {
				t.Fatal("canceled waiter wrote history")
			}
			if err := waiterDB.PingContext(t.Context()); err != nil {
				t.Fatal("waiter pool unusable", err)
			}
		})
	}
	if foreignDiscard.Load() {
		t.Fatalf("discardPGXConnection ran on a non-owner goroutine - the exact "+
			"ownership violation behind eino-agent-f3i:\n%s", *foreignStack.Load())
	}
	if got := hookFired.Load(); got < iterations {
		t.Fatalf("interrupt hook observed %d aborts, want >= %d (test is vacuous)", got, iterations)
	}
	if got := released.Load(); got < iterations {
		t.Fatalf("only %d/%d iterations released the interrupt in step with the owner's "+
			"query (the rest hit the 250ms fallback and proved nothing that iteration); "+
			"released=%d timedOut=%d", got, iterations, released.Load(), timedOut.Load())
	}
}

type failedUnlock struct {
	lock.SessionLocker
	pid    int
	err    error
	cancel context.CancelFunc
	expire bool
}

func (l *failedUnlock) SessionLock(ctx context.Context, conn *sql.Conn) error {
	if err := conn.QueryRowContext(ctx, `SELECT pg_catalog.pg_backend_pid()`).Scan(&l.pid); err != nil {
		return err
	}
	return l.SessionLocker.SessionLock(ctx, conn)
}

func (l *failedUnlock) SessionUnlock(ctx context.Context, conn *sql.Conn) error {
	if l.expire {
		var value any
		// Block server-side until cleanupTimeout's interrupt closes the
		// transport out from under this in-flight query, reproducing a
		// genuine I/O failure mid-unlock rather than a context-cancellation
		// shortcut: after the ownership-model fix (migration_context.go),
		// interrupt never cancels the pgx-visible context itself, only the
		// transport, so a delegate can no longer rely on ctx.Done() firing
		// on its own - only a real read/write against the closed socket
		// unblocks it.
		//
		// pg_sleep(8) - not something much longer - matters for
		// testPhysicalMigrationCleanup's poll: cleanupTimeout (5s) closes
		// the transport while this query is still running, but pg_sleep
		// itself does not read the socket, so PostgreSQL only discovers the
		// dead connection when the query finishes and tries to write its
		// result back, around t=8s. Depending on the host/container network
		// path noticing the closed transport sooner (a plain client FIN can
		// otherwise sit unnoticed for a long time, with
		// client_connection_check_interval=0) would make the poll's timing
		// hold only by accident; 8s guarantees the write-failure path fires
		// with margin inside the poll's own 5s budget after migrate returns.
		return errors.Join(l.err, conn.QueryRowContext(ctx, "SELECT pg_catalog.pg_sleep(8)").Scan(&value))
	}
	l.cancel()
	return l.err // Deliberately leave the real session lock held.
}

func testPhysicalMigrationCleanup(t *testing.T, server *testpostgres.Server) {
	for _, test := range []struct {
		state  schemaState
		expire bool
	}{{schemaEmpty, false}, {schemaCurrent, false}, {schemaCurrent, true}} {
		state := test.state
		database := server.Database(t)
		observer := database.Open(t)
		prepareSchema(t, observer, state)
		pool := database.OpenPGX(t)
		wrapper := stdlib.OpenDBFromPool(pool)
		t.Cleanup(func() {
			if err := wrapper.Close(); err != nil {
				t.Error(err)
			}
		})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		failure := errors.New("injected advisory unlock failure")
		locker := &failedUnlock{SessionLocker: migrationDelegate(t), err: failure, cancel: cancel, expire: test.expire}
		err := migrate(ctx, wrapper, migrationFS(t), locker)
		if !errors.Is(err, failure) {
			t.Fatalf("cleanup failure lost: %v", err)
		}
		if state == schemaCurrent && !errors.Is(err, goose.ErrAlreadyApplied) {
			t.Fatalf("no-op error missing: %v", err)
		}
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			var exists bool
			if err := observer.QueryRowContext(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$1)`, locker.pid).Scan(&exists); err != nil {
				t.Fatal(err)
			}
			if !exists {
				break
			}
			select {
			case <-deadline.C:
				t.Fatal("contaminated physical backend survived cleanup")
			case <-tick.C:
			}
		}
		// An independent pool can acquire the same lock and validate the completed
		// baseline; neither closing the wrapper alone nor releasing to pgxpool suffices.
		if err := Migrate(t.Context(), observer); err != nil {
			t.Fatal("independent migration blocked after cleanup", err)
		}
		if err := pool.Ping(t.Context()); err != nil {
			t.Fatal("host native pool closed", err)
		}
		if err := Migrate(t.Context(), wrapper); err != nil {
			t.Fatal("host wrapper unusable", err)
		}
		assertMigrationState(t, observer, schemaCurrent)
	}
}

func receiveMigration[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatal("migration synchronization timed out")
	}
	var zero T
	return zero
}
