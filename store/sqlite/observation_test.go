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
	defer func() { _ = st.Close() }()
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
		"EXPLAIN QUERY PLAN SELECT id FROM messages WHERE session_id = 'session-tool' AND role IN ('user','assistant') ORDER BY created_at DESC,id DESC LIMIT 2",
		"EXPLAIN QUERY PLAN SELECT id FROM parts WHERE session_id = 'session-tool' AND message_id = 'msg-tool' AND kind = 'text' ORDER BY ordinal,id LIMIT 2",
	} {
		rows, err := st.query(ctx, query)
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
	defer func() { _ = st.Close() }()
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
	if _, err := st.exec(ctx, "UPDATE observation_revisions SET revision = ? WHERE session_id = ?", int64(math.MaxInt64), "session-tool"); err != nil {
		t.Fatal(err)
	}
	if err := ex.FinalizeAssistantMessage(ctx, "msg-tool"); err == nil {
		t.Fatal("revision overflow accepted")
	}
	var finalized bool
	if err := st.queryRow(ctx, "SELECT finalized FROM messages WHERE id = 'msg-tool'").Scan(&finalized); err != nil || finalized {
		t.Fatal(err, finalized)
	}
}
func TestObservationReopenAndRecreation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")
	st, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	before, err := st.ReadObservationSnapshot(ctx, "absent", observationLimits())
	if err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	st, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	after, err := st.ReadObservationSnapshot(ctx, "absent", observationLimits())
	if err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	if !reflect.DeepEqual(before, after) {
		t.Fatal(before, after)
	}
	st, err = Open(ctx, filepath.Join(t.TempDir(), "new.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
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
			writer, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = writer.Close() }()
			if _, err = writer.exec(ctx, "PRAGMA journal_mode="+mode); err != nil {
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
			reader, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Close() }()
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

func FuzzObservationText(f *testing.F) {
	for _, seed := range []string{`{"text":"hello"}`, `{"text":42}`, `{"text":null}`, `{"text":""}`, "{\"text\":\"\xff\"}"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 4096 {
			return
		}
		value, valid := observationText(session.Part{Kind: session.PartText, Payload: raw})
		if valid {
			if !json.Valid(raw) {
				t.Fatal("accepted malformed JSON")
			}
			var decoded struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal(raw, &decoded); err != nil || decoded.Text != value {
				t.Fatal("text changed")
			}
		}
		hidden, ok := observationText(session.Part{Kind: session.PartProviderState, Payload: raw})
		if hidden != "" || !ok {
			t.Fatal("hidden state inspected")
		}
	})
}
