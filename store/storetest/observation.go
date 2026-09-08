package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func observationContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("observation revision finalization and rollback", func(t *testing.T) {
		subject := setup(t, factory)
		reader, ok := subject.Store.(session.ObservationReader)
		if !ok {
			t.Fatal("store must implement committed ObservationReader")
		}
		ctx := context.Background()
		limits := session.ObservationLimits{MaxMessages: 10, MaxTools: 10, MaxParts: 10, MaxSnapshotBytes: 64000, MaxTextBytes: 1000}
		read := func() session.ObservationSnapshot {
			t.Helper()
			v, err := reader.ReadObservationSnapshot(ctx, "observed", limits)
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
		missing := read()
		if missing.Exists || missing.Watermark.Revision != 0 || missing.Watermark.StoreID == "" {
			t.Fatal(missing)
		}
		createSession(t, ctx, subject.Store, "observed")
		r, err := subject.Store.AdmitRun(ctx, run("opaque-z", "observed", "owner"), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		ex := executionFor(subject.Store, r)
		m := session.Message{ID: "opaque-a", SessionID: r.SessionID, RunID: r.ID, Role: session.RoleAssistant, CreatedAt: time.Unix(100, 0)}
		appendMessage(t, ctx, ex, m)
		before := read()
		if len(before.Messages) != 1 || before.Messages[0].Finalized || before.Watermark.Revision <= missing.Watermark.Revision {
			t.Fatal(before)
		}
		rollback := errors.New("rollback")
		part := session.Part{ID: "text", MessageID: m.ID, SessionID: r.SessionID, RunID: r.ID, Kind: session.PartText, Payload: []byte(`{"text":"hello"}`)}
		if err = ex.WithinTx(ctx, func(ctx context.Context, tx session.ExecutionStore) error {
			if _, err := tx.AppendPart(ctx, part); err != nil {
				return err
			}
			if err := tx.FinalizeAssistantMessage(ctx, m.ID); err != nil {
				return err
			}
			return rollback
		}); !errors.Is(err, rollback) {
			t.Fatal(err)
		}
		after := read()
		if after.Watermark != before.Watermark || after.Messages[0].Finalized || after.Messages[0].Text != "" {
			t.Fatal(after)
		}
		if err = ex.WithinTx(ctx, func(ctx context.Context, tx session.ExecutionStore) error {
			if _, err := tx.AppendPart(ctx, part); err != nil {
				return err
			}
			return tx.FinalizeAssistantMessage(ctx, m.ID)
		}); err != nil {
			t.Fatal(err)
		}
		final := read()
		if final.Watermark.Revision <= before.Watermark.Revision || !final.Messages[0].Finalized || final.Messages[0].Text != "hello" {
			t.Fatal(final)
		}
		if err = ex.FinalizeAssistantMessage(ctx, m.ID); err != nil {
			t.Fatal(err)
		}
		if err = subject.Store.Execution(session.RunFence{RunID: r.ID, ClaimToken: "wrong"}).FinalizeAssistantMessage(ctx, m.ID); err == nil {
			t.Fatal("accepted stale fence")
		}
		user := m
		user.ID = "user"
		user.Role = session.RoleUser
		appendMessage(t, ctx, ex, user)
		if err = ex.FinalizeAssistantMessage(ctx, user.ID); err == nil {
			t.Fatal("finalized user as assistant")
		}
		empty := m
		empty.ID = "empty"
		appendMessage(t, ctx, ex, empty)
		if err = ex.FinalizeAssistantMessage(ctx, empty.ID); err != nil {
			t.Fatal(err)
		}
		detached := read()
		detached.Messages[0].Text = "mutated"
		if read().Messages[0].Text == "mutated" {
			t.Fatal("aliased snapshot")
		}
	})
}

func boundedObservationContract(t *testing.T, factory Factory) {
	t.Run("bounded allowlisted observation", func(t *testing.T) {
		subject := setup(t, factory)
		reader, ok := subject.Store.(session.ObservationReader)
		if !ok {
			t.Fatal("missing ObservationReader")
		}
		ctx := context.Background()
		createSession(t, ctx, subject.Store, "bounded")
		r, err := subject.Store.AdmitRun(ctx, run("bounded-run", "bounded", "owner"), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		ex := executionFor(subject.Store, r)
		for i, role := range []session.Role{session.RoleUser, session.RoleAssistant, session.RoleSystem} {
			m := session.Message{ID: session.MessageID(fmt.Sprintf("m%d", i)), SessionID: r.SessionID, RunID: r.ID, Role: role, CreatedAt: time.Unix(int64(i), 0)}
			appendMessage(t, ctx, ex, m)
			text := "visible"
			if role == session.RoleSystem {
				text = "PRIVATE_SYSTEM"
			}
			raw, _ := json.Marshal(map[string]string{"text": text})
			appendPart(t, ctx, ex, session.Part{ID: session.PartID(fmt.Sprintf("p%d", i)), SessionID: r.SessionID, RunID: r.ID, MessageID: m.ID, Kind: session.PartText, Payload: raw})
		}
		appendPart(t, ctx, ex, session.Part{ID: "reasoning", SessionID: r.SessionID, RunID: r.ID, MessageID: "m1", Kind: session.PartReasoning, Payload: []byte(`{"text":"PRIVATE_REASONING"}`)})
		limits := session.ObservationLimits{MaxMessages: 1, MaxTools: 1, MaxParts: 1, MaxSnapshotBytes: 64000, MaxTextBytes: 7}
		snapshot, err := reader.ReadObservationSnapshot(ctx, r.SessionID, limits)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(snapshot)
		if strings.Contains(string(raw), "PRIVATE") || len(snapshot.Messages) != 1 || snapshot.Messages[0].ID != "m1" || snapshot.Messages[0].Text != "visible" || !snapshot.OmittedOlderMessages {
			t.Fatal(snapshot)
		}
		limits.MaxTextBytes = 6
		if _, err = reader.ReadObservationSnapshot(ctx, r.SessionID, limits); !errors.Is(err, session.ErrObservationTooLarge) {
			t.Fatal(err)
		}
		limits.MaxTextBytes = 7
		appendPart(t, ctx, ex, session.Part{ID: "empty", SessionID: r.SessionID, RunID: r.ID, MessageID: "m1", Kind: session.PartText, Payload: []byte(`{"text":""}`)})
		if _, err = reader.ReadObservationSnapshot(ctx, r.SessionID, limits); !errors.Is(err, session.ErrObservationTooLarge) {
			t.Fatal("empty part escaped cumulative count", err)
		}
	})
}
