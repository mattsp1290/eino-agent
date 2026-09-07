package storetest

import (
	"context"
	"errors"
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
