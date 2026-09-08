package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func observationLimits() session.ObservationLimits {
	return session.ObservationLimits{MaxMessages: 10, MaxTools: 10, MaxParts: 10, MaxSnapshotBytes: 64000, MaxTextBytes: 1000}
}
func TestObservationLimitsPrivacyAndIndex(t *testing.T) {
	st, ex, _, now := setupToolTransitionTest(t)
	defer func() { _ = st.db.Close() }()
	ctx := context.Background()
	l := observationLimits()
	appendPart := func(id string, kind session.PartKind, payload string) {
		t.Helper()
		_, err := ex.AppendPart(ctx, session.Part{ID: session.PartID(id), MessageID: "msg-tool", SessionID: "session-tool", RunID: "run-tool", Kind: kind, Payload: []byte(payload), CreatedAt: now})
		if err != nil {
			t.Fatal(err)
		}
	}
	secret := strings.Repeat("HIDDEN_SENTINEL", 10000)
	appendPart("provider", session.PartProviderState, fmt.Sprintf("%q", secret))
	appendPart("reasoning", session.PartReasoning, fmt.Sprintf("%q", secret))
	appendPart("text", session.PartText, `{"text":"hi","private":"HIDDEN_SENTINEL"}`)
	snap, err := st.ReadObservationSnapshot(ctx, "session-tool", l)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(snap)
	if strings.Contains(string(raw), "HIDDEN") || snap.Messages[0].Text != "hi" {
		t.Fatal("unsafe projection")
	}
	l.MaxTextBytes = 1
	if _, err = st.ReadObservationSnapshot(ctx, "session-tool", l); !errors.Is(err, session.ErrObservationTooLarge) {
		t.Fatal(err)
	}
	l = observationLimits()
	l.MaxParts = 1
	appendPart("empty", session.PartText, `{"text":""}`)
	if _, err = st.ReadObservationSnapshot(ctx, "session-tool", l); !errors.Is(err, session.ErrObservationTooLarge) {
		t.Fatal(err)
	}
	l = observationLimits()
	appendPart("bad", session.PartText, `{"text":42}`)
	if _, err = st.ReadObservationSnapshot(ctx, "session-tool", l); !errors.Is(err, session.ErrObservationInvalid) {
		t.Fatal(err)
	}
	for _, query := range []string{
		"EXPLAIN QUERY PLAN SELECT id FROM messages WHERE session_key = (SELECT row_key FROM sessions WHERE id = x'73657373696f6e2d746f6f6c') AND role IN ('user','assistant') ORDER BY created_at DESC,id DESC LIMIT 2",
		"EXPLAIN QUERY PLAN SELECT id FROM parts WHERE session_key = (SELECT row_key FROM sessions WHERE id = x'73657373696f6e2d746f6f6c') AND message_key = (SELECT row_key FROM messages WHERE id = x'6d73672d746f6f6c') AND kind = 'text' ORDER BY ordinal,id LIMIT 2",
	} {
		rows, err := st.db.QueryContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		var plan string
		for rows.Next() {
			var a, b, c int
			var detail string
			if err = rows.Scan(&a, &b, &c, &detail); err != nil {
				t.Fatal(err)
			}
			plan += detail
		}
		_ = rows.Close()
		if !strings.Contains(plan, "observation_idx") || strings.Contains(plan, "SCAN ") {
			t.Fatal(plan)
		}
	}
}
func TestObservationRootReaderAndOverflow(t *testing.T) {
	st, ex, _, _ := setupToolTransitionTest(t)
	defer func() { _ = st.db.Close() }()
	ctx := context.Background()
	if err := st.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
		r := tx.(session.ObservationReader)
		if _, err := r.ReadObservationSnapshot(ctx, "session-tool", observationLimits()); !errors.Is(err, session.ErrObservationReader) {
			t.Fatal(err)
		}
		if _, err := r.ReadObservationRevision(ctx, "session-tool"); !errors.Is(err, session.ErrObservationReader) {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := st.ReadObservationSnapshot(canceled, "session-tool", observationLimits()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := st.ReadObservationSnapshot(ctx, "session-tool", observationLimits()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, "UPDATE observation_revisions SET revision = ? WHERE session_id = ?", int64(math.MaxInt64), []byte("session-tool")); err != nil {
		t.Fatal(err)
	}
	if err := ex.FinalizeAssistantMessage(ctx, "msg-tool"); err == nil {
		t.Fatal("revision overflow accepted")
	}
	var finalized bool
	if err := st.db.QueryRowContext(ctx, "SELECT finalized FROM messages WHERE id = x'6d73672d746f6f6c'").Scan(&finalized); err != nil || finalized {
		t.Fatal(err, finalized)
	}
}
func TestObservationReopenAndRecreation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	st, err := openSQLiteFixture(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	before, err := st.ReadObservationSnapshot(ctx, "absent", observationLimits())
	if err != nil {
		t.Fatal(err)
	}
	_ = st.db.Close()
	st, err = reopenSQLiteFixture(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	after, err := st.ReadObservationSnapshot(ctx, "absent", observationLimits())
	if err != nil {
		t.Fatal(err)
	}
	_ = st.db.Close()
	if !reflect.DeepEqual(before, after) {
		t.Fatal(before, after)
	}
	st, err = openSQLiteFixture(ctx, filepath.Join(t.TempDir(), "new.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.db.Close() }()
	recreated, err := st.ReadObservationSnapshot(ctx, "absent", observationLimits())
	if err != nil {
		t.Fatal(err)
	}
	if recreated.Watermark.StoreID == before.Watermark.StoreID {
		t.Fatal("incarnation reused")
	}
}
func TestObservationConcurrentCommittedSnapshot(t *testing.T) {
	for _, mode := range []string{"DELETE", "WAL"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "store.db")
			writer, err := openSQLiteFixture(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = writer.db.Close() }()
			if _, err = writer.db.ExecContext(ctx, "PRAGMA journal_mode="+mode); err != nil {
				t.Fatal(err)
			}
			_, err = writer.CreateSession(ctx, session.Session{ID: "s"})
			if err != nil {
				t.Fatal(err)
			}
			run, err := writer.AdmitRun(ctx, session.Run{ID: "r", SessionID: "s", Status: session.RunRunning, ClaimToken: "f"}, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			ex := writer.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
			_, err = ex.AppendMessage(ctx, session.Message{ID: "m", SessionID: "s", RunID: "r", Role: session.RoleAssistant})
			if err != nil {
				t.Fatal(err)
			}
			reader, err := openSQLiteFixture(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.db.Close() }()
			ready, commit := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- ex.WithinTx(ctx, func(ctx context.Context, tx session.ExecutionStore) error {
					if _, err := tx.AppendPart(ctx, session.Part{ID: "p", MessageID: "m", SessionID: "s", RunID: "r", Kind: session.PartText, Payload: []byte(`{"text":"atomic"}`)}); err != nil {
						return err
					}
					if err := tx.FinalizeAssistantMessage(ctx, "m"); err != nil {
						return err
					}
					close(ready)
					<-commit
					return nil
				})
			}()
			<-ready
			before, err := reader.ReadObservationSnapshot(ctx, "s", observationLimits())
			if err != nil {
				t.Fatal(err)
			}
			if before.Messages[0].Text != "" || before.Messages[0].Finalized {
				t.Fatal(before)
			}
			close(commit)
			if err = <-done; err != nil {
				t.Fatal(err)
			}
			after, err := reader.ReadObservationSnapshot(ctx, "s", observationLimits())
			if err != nil {
				t.Fatal(err)
			}
			if after.Messages[0].Text != "atomic" || !after.Messages[0].Finalized || after.Watermark.Revision <= before.Watermark.Revision {
				t.Fatal(after)
			}
		})
	}
}

func TestObservationWindowCumulativeLimitsAndExcludedPopulations(t *testing.T) {
	st, ex, _, now := setupToolTransitionTest(t)
	defer func() { _ = st.db.Close() }()
	ctx := t.Context()
	limit := observationLimits()
	limit.MaxMessages = 2
	// Equal timestamps and opaque IDs exercise the durable tie ordering.
	for _, id := range []string{"visible-z", "visible-a"} {
		if _, err := ex.AppendMessage(ctx, session.Message{ID: session.MessageID(id), SessionID: "session-tool", RunID: "run-tool", Role: session.RoleUser, CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
		if _, err := ex.AppendPart(ctx, session.Part{ID: session.PartID(id), MessageID: session.MessageID(id), SessionID: "session-tool", RunID: "run-tool", Kind: session.PartText, Payload: []byte(`{"text":"é"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	read := func() session.ObservationSnapshot {
		t.Helper()
		snap, err := st.ReadObservationSnapshot(ctx, "session-tool", limit)
		if err != nil {
			t.Fatal(err)
		}
		return snap
	}
	before := read()
	if len(before.Messages) != 2 || before.Messages[0].ID != "visible-a" || before.Messages[1].ID != "visible-z" || !before.OmittedOlderMessages {
		t.Fatal(before)
	}
	baselineAllocs := testing.AllocsPerRun(5, func() { read() })
	huge := fmt.Sprintf("%q", strings.Repeat("PRIVATE_EXCLUDED", 1024))
	if err := ex.WithinTx(ctx, func(ctx context.Context, tx session.ExecutionStore) error {
		for i := range 1000 {
			id := session.MessageID(fmt.Sprintf("excluded-%04d", i))
			if _, err := tx.AppendMessage(ctx, session.Message{ID: id, SessionID: "session-tool", RunID: "run-tool", Role: session.RoleSystem, CreatedAt: now.Add(time.Hour)}); err != nil {
				return err
			}
			if _, err := tx.AppendPart(ctx, session.Part{ID: session.PartID(id), MessageID: "visible-z", SessionID: "session-tool", RunID: "run-tool", Kind: session.PartReasoning, Payload: []byte(huge)}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	after := read()
	if !reflect.DeepEqual(before.Messages, after.Messages) {
		t.Fatal("excluded rows changed window")
	}
	populatedAllocs := testing.AllocsPerRun(5, func() { read() })
	if populatedAllocs > baselineAllocs+100 {
		t.Fatalf("excluded population materialized: allocations before=%g after=%g", baselineAllocs, populatedAllocs)
	}
	limit.MaxTextBytes = 3
	if _, err := st.ReadObservationSnapshot(ctx, "session-tool", limit); !errors.Is(err, session.ErrObservationTooLarge) {
		t.Fatal("cumulative text budget", err)
	}
	limit.MaxTextBytes = 4
	if got := read(); got.Messages[0].Text != "é" {
		t.Fatal(got)
	}
	limit.MaxSnapshotBytes = 1
	if _, err := st.ReadObservationSnapshot(ctx, "session-tool", limit); !errors.Is(err, session.ErrObservationTooLarge) {
		t.Fatal("snapshot budget", err)
	}
}
func TestObservationForeignOwnershipAndScalarUTF8(t *testing.T) {
	st, ex, _, _ := setupToolTransitionTest(t)
	defer func() { _ = st.db.Close() }()
	ctx := t.Context()
	if _, err := ex.AppendPart(ctx, session.Part{ID: "p", MessageID: "msg-tool", SessionID: "session-tool", RunID: "run-tool", Kind: session.PartText, Payload: []byte(`{"text":"safe"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateSession(ctx, session.Session{ID: "foreign-session"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AdmitRun(ctx, session.Run{ID: "foreign-run", SessionID: "foreign-session", Status: session.RunRunning, ClaimToken: "foreign-token"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, "UPDATE parts SET run_key = (SELECT row_key FROM runs WHERE id = ?) WHERE id = ?", []byte("foreign-run"), []byte("p")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ReadObservationSnapshot(ctx, "session-tool", observationLimits()); !errors.Is(err, session.ErrObservationInvalid) {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, "UPDATE parts SET run_key = (SELECT row_key FROM runs WHERE id = ?) WHERE id = ?", []byte("run-tool"), []byte("p")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, "UPDATE runs SET provider_id=?, model_id=? WHERE id=?", []byte{0xc3}, []byte{0xa9}, []byte("run-tool")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ReadObservationSnapshot(ctx, "session-tool", observationLimits()); !errors.Is(err, session.ErrObservationInvalid) {
		t.Fatal("individually malformed fields accepted", err)
	}
}
func TestObservationAssistantToolFinalizationAtomic(t *testing.T) {
	st, ex, call, now := setupToolTransitionTest(t)
	defer func() { _ = st.db.Close() }()
	ctx := t.Context()
	call.RequestPartID = "request"
	request := session.CreateToolCallRequest{Call: call, RequestPart: session.Part{ID: call.RequestPartID, MessageID: call.MessageID, SessionID: call.SessionID, RunID: call.RunID, Kind: session.PartToolCall, Payload: []byte(`{"id":"call-tool","name":"tool","arguments":{"ok":true}}`)}, Event: session.ToolTransitionEvent{ID: "pending", CreatedAt: now}}
	rollback := errors.New("rollback")
	write := func(ctx context.Context, tx session.ExecutionStore) error {
		if _, err := tx.AppendPart(ctx, session.Part{ID: "text", MessageID: call.MessageID, SessionID: call.SessionID, RunID: call.RunID, Kind: session.PartText, Payload: []byte(`{"text":"atomic"}`)}); err != nil {
			return err
		}
		if _, err := tx.CreateToolCall(ctx, request); err != nil {
			return err
		}
		return tx.FinalizeAssistantMessage(ctx, call.MessageID)
	}
	before, err := st.ReadObservationSnapshot(ctx, call.SessionID, observationLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err = ex.WithinTx(ctx, func(ctx context.Context, tx session.ExecutionStore) error {
		if err := write(ctx, tx); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	rolledBack, err := st.ReadObservationSnapshot(ctx, call.SessionID, observationLimits())
	if err != nil || !reflect.DeepEqual(before, rolledBack) {
		t.Fatal(rolledBack, err)
	}
	if err = ex.WithinTx(ctx, write); err != nil {
		t.Fatal(err)
	}
	after, err := st.ReadObservationSnapshot(ctx, call.SessionID, observationLimits())
	if err != nil {
		t.Fatal(err)
	}
	if !after.Messages[0].Finalized || after.Messages[0].Text != "atomic" || len(after.Tools) != 1 || after.Tools[0].Status != session.ToolCallPending || after.Watermark.Revision <= before.Watermark.Revision {
		t.Fatal(after)
	}
}
