//go:build postgres_integration

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/postgres"
)

func TestPostgresFencing(t *testing.T) {
	server := testpostgres.Start(t)
	for _, tc := range []struct {
		name string
		run  func(*testing.T, *testpostgres.Server)
	}{
		{"admission", testAdmissionRace}, {"reclaim", testReclaimRace},
		{"live_clock", testClaimLiveClock}, {"stale_methods", testStaleMethods},
		{"delayed_writer", testDelayedWriter}, {"run_settlement", testRunSettlementRace},
		{"tool_settlement", testToolSettlementRace}, {"tool_transitions", testToolTransitionRace},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, server) })
	}
}

type raceFixture struct {
	ctx      context.Context
	dbs      []*sql.DB
	stores   []session.Store
	pids     []int
	observer *sql.DB
	now      time.Time
}

func newRaceFixture(t *testing.T, server *testpostgres.Server, n int) *raceFixture {
	t.Helper()
	database := server.Database(t)
	first, st := migratePostgresDatabase(t, database)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	f := &raceFixture{ctx: ctx, dbs: []*sql.DB{first}, stores: []session.Store{st}, now: time.Now().UTC()}
	for range n - 1 {
		db := database.Open(t)
		st, err := postgres.New(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		f.dbs = append(f.dbs, db)
		f.stores = append(f.stores, st)
	}
	// Hold all connections at once to prove distinct physical backends. One idle
	// connection per pool keeps these PIDs observable without limiting open work.
	held := make([]*sql.Conn, 0, n)
	defer func() {
		for _, conn := range held {
			_ = conn.Close()
		}
	}()
	seen := make(map[int]bool)
	for _, db := range f.dbs {
		db.SetMaxIdleConns(1)
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
		var pid int
		if err := conn.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
			t.Fatal(err)
		}
		if seen[pid] {
			t.Fatalf("independent pools reused backend %d", pid)
		}
		seen[pid] = true
		f.pids = append(f.pids, pid)
	}
	f.observer = database.Open(t)
	return f
}

func (f *raceFixture) seed(t *testing.T) session.Run {
	t.Helper()
	if _, err := f.stores[0].CreateSession(f.ctx, session.Session{ID: "session", CreatedAt: f.now, UpdatedAt: f.now}); err != nil {
		t.Fatal(err)
	}
	run, err := f.stores[0].AdmitRun(f.ctx, session.Run{ID: "run", SessionID: "session", OwnerID: "old", ClaimToken: "old", Status: session.RunPending, CreatedAt: f.now}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func (f *raceFixture) expire(t *testing.T, run session.Run) {
	t.Helper()
	if _, err := f.stores[0].Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken}).RenewRunLease(f.ctx, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	waitRace(t, f.ctx, func() (bool, error) {
		var expired bool
		err := f.observer.QueryRowContext(f.ctx, "SELECT lease_until <= (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint FROM public.runs WHERE id = $1", []byte(run.ID)).Scan(&expired)
		return expired, err
	})
}

func waitRace(t *testing.T, ctx context.Context, predicate func() (bool, error)) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		done, err := predicate()
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}

// Include projections, serialized records, and observation revisions: an
// ErrConflict alone cannot prove that an earlier statement was rolled back.
func (f *raceFixture) snapshot(t *testing.T) string {
	t.Helper()
	var result strings.Builder
	for _, table := range []string{"observation_store", "observation_revisions", "sessions", "runs", "messages", "parts", "tool_calls", "context_epochs", "model_requests", "events"} {
		var rows string
		query := "SELECT COALESCE(jsonb_agg(r ORDER BY r::text), '[]'::jsonb)::text FROM (SELECT to_jsonb(t) AS r FROM public." + table + " t) rows"
		if err := f.observer.QueryRowContext(f.ctx, query).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		result.WriteString(table + ":" + rows + "\n")
	}
	return result.String()
}

func testAdmissionRace(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 8)
	if _, err := f.stores[0].CreateSession(f.ctx, session.Session{ID: "session", CreatedAt: f.now, UpdatedAt: f.now}); err != nil {
		t.Fatal(err)
	}
	outcomes := raceStores(f.ctx, f.stores, func(ctx context.Context, i int, st session.Store) (session.Run, error) {
		return st.AdmitRun(ctx, session.Run{ID: session.RunID(fmt.Sprintf("run-%d", i)), SessionID: "session", OwnerID: "owner", ClaimToken: "token", Status: session.RunPending, CreatedAt: f.now}, time.Minute)
	})
	var winner session.Run
	wins := 0
	for _, r := range outcomes {
		if r.err == nil {
			winner = r.value
			wins++
		} else if !errors.Is(r.err, session.ErrSessionBusy) {
			t.Errorf("admission error: %v", r.err)
		}
	}
	if wins != 1 {
		t.Fatalf("admission winners = %d", wins)
	}
	runs, err := f.stores[0].ListUnfinishedRuns(f.ctx)
	if err != nil || len(runs) != 1 || runs[0].ID != winner.ID {
		t.Fatalf("durable admission winner mismatch: %v", err)
	}
	var count int
	if err := f.observer.QueryRowContext(f.ctx, "SELECT count(*) FROM public.runs").Scan(&count); err != nil || count != 1 {
		t.Fatalf("admission rows = %d: %v", count, err)
	}
	before := f.snapshot(t)
	if _, err := f.stores[1].AdmitRun(f.ctx, winner, time.Minute); !errors.Is(err, session.ErrConflict) || f.snapshot(t) != before {
		t.Fatalf("duplicate admission changed state: %v", err)
	}

}

func testReclaimRace(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 2)
	old := f.seed(t)
	f.expire(t, old)
	outcomes := raceStores(f.ctx, f.stores, func(ctx context.Context, i int, st session.Store) (session.Run, error) {
		token := fmt.Sprintf("new-%d", i)
		return st.ClaimRun(ctx, session.RunClaim{RunID: old.ID, OwnerID: token, ClaimToken: token, LeaseDuration: time.Minute})
	})
	var winner session.Run
	wins := 0
	for _, r := range outcomes {
		if r.err == nil {
			winner = r.value
			wins++
		} else if !errors.Is(r.err, session.ErrSessionBusy) {
			t.Errorf("reclaim error: %v", r.err)
		}
	}
	if wins != 1 || winner.ClaimToken == old.ClaimToken {
		t.Fatalf("reclaim winners = %d", wins)
	}
	durable, err := f.stores[0].GetRun(f.ctx, old.ID)
	if err != nil || durable.ClaimToken != winner.ClaimToken || durable.OwnerID != winner.OwnerID {
		t.Fatalf("durable reclaim winner mismatch: %v", err)
	}
	before := f.snapshot(t)
	_, err = f.stores[0].Execution(session.RunFence{RunID: old.ID, ClaimToken: old.ClaimToken}).StartRun(f.ctx, f.now)
	if !errors.Is(err, session.ErrConflict) || f.snapshot(t) != before {
		t.Fatalf("reclaimed fence changed state: %v", err)
	}
}

func testClaimLiveClock(t *testing.T, server *testpostgres.Server) {
	f := newRaceFixture(t, server, 2)
	old := f.seed(t)
	err := f.stores[1].WithinTx(f.ctx, func(ctx context.Context, tx session.Store) error {
		// Establish the transaction before the new lease expires. A transaction-
		// start timestamp would keep this claim busy after the live clock expires.
		if _, err := tx.GetRun(ctx, old.ID); err != nil {
			return err
		}
		f.expire(t, old)
		_, err := tx.ClaimRun(ctx, session.RunClaim{RunID: old.ID, OwnerID: "new", ClaimToken: "new", LeaseDuration: time.Minute})
		return err
	})
	if err != nil {
		t.Fatalf("live-clock claim: %v", err)
	}
}
