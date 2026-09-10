package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func titleContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("session title mutation", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		now := time.Now().UTC().Add(-time.Hour)
		original := session.Session{ID: "named", WorkspaceID: "workspace", Directory: "/workspace", Title: "original", Metadata: map[string]string{"version": "one"}, CreatedAt: now, UpdatedAt: now}
		if _, err := subject.Store.CreateSession(ctx, original); err != nil {
			t.Fatal(err)
		}

		stale := original
		current := original
		current.Metadata = map[string]string{"version": "two"}
		if err := subject.Store.UpdateSession(ctx, current); err != nil {
			t.Fatal(err)
		}
		result, err := subject.Store.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: original.ID, WorkspaceID: original.WorkspaceID, Title: "renamed"})
		if err != nil || !result.Changed || result.Title != "renamed" || result.UpdatedAt.Before(current.UpdatedAt) {
			t.Fatalf("rename result = %#v, error = %v", result, err)
		}
		got, err := subject.Store.GetSession(ctx, original.ID)
		if err != nil || got.Title != "renamed" || got.Metadata["version"] != "two" || got.Directory != stale.Directory || !got.CreatedAt.Equal(stale.CreatedAt) {
			t.Fatalf("stored session = %#v, error = %v", got, err)
		}
		noOp, err := subject.Store.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: original.ID, WorkspaceID: original.WorkspaceID, Title: "renamed"})
		if err != nil || noOp.Changed || noOp.Title != result.Title || !noOp.UpdatedAt.Equal(result.UpdatedAt) {
			t.Fatalf("no-op result = %#v, error = %v", noOp, err)
		}
		if _, err := subject.Store.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: original.ID, WorkspaceID: "wrong", Title: "forbidden"}); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("wrong workspace error = %v", err)
		}
		if _, err := subject.Store.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: "missing", WorkspaceID: "workspace", Title: "missing"}); !errors.Is(err, session.ErrNotFound) {
			t.Fatalf("missing session error = %v", err)
		}
	})

	t.Run("fenced title authority", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		record := sessionRecord("fenced-title")
		record.WorkspaceID = "workspace"
		if _, err := subject.Store.CreateSession(ctx, record); err != nil {
			t.Fatal(err)
		}
		r := admitRun(t, ctx, subject.Store, run("fenced-title-run", record.ID, "owner"))
		request := session.SessionTitleRequest{SessionID: record.ID, WorkspaceID: record.WorkspaceID, Title: "agent title"}
		result, err := executionFor(subject.Store, r).SetSessionTitle(ctx, request)
		if err != nil || !result.Changed {
			t.Fatalf("fenced rename result = %#v, error = %v", result, err)
		}
		if _, err := subject.Store.Execution(session.RunFence{RunID: r.ID, ClaimToken: "stale"}).SetSessionTitle(ctx, request); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("stale fence error = %v", err)
		}
		wrong := request
		wrong.SessionID = "other"
		if _, err := executionFor(subject.Store, r).SetSessionTitle(ctx, wrong); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("wrong session error = %v", err)
		}
		wrong = request
		wrong.WorkspaceID = "wrong"
		if _, err := executionFor(subject.Store, r).SetSessionTitle(ctx, wrong); !errors.Is(err, session.ErrConflict) {
			t.Fatalf("wrong workspace error = %v", err)
		}
	})

	t.Run("title rollback and serialization", func(t *testing.T) {
		subject := setup(t, factory)
		ctx := context.Background()
		record := sessionRecord("serialized-title")
		if _, err := subject.Store.CreateSession(ctx, record); err != nil {
			t.Fatal(err)
		}
		rollback := errors.New("rollback")
		if err := subject.Store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
			if _, err := tx.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: record.ID, Title: "rolled back"}); err != nil {
				return err
			}
			return rollback
		}); !errors.Is(err, rollback) {
			t.Fatal(err)
		}
		if got, _ := subject.Store.GetSession(ctx, record.ID); got.Title != record.Title {
			t.Fatalf("rollback title = %q", got.Title)
		}

		locked := make(chan struct{})
		release := make(chan struct{})
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- subject.Store.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
				if _, err := tx.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: record.ID, Title: "alpha"}); err != nil {
					return err
				}
				close(locked)
				<-release
				return nil
			})
		}()
		<-locked
		secondStarted := make(chan struct{})
		secondDone := make(chan error, 1)
		go func() {
			close(secondStarted)
			_, err := subject.Store.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: record.ID, Title: "beta"})
			secondDone <- err
		}()
		<-secondStarted
		close(release)
		if err := <-firstDone; err != nil {
			t.Fatal(err)
		}
		if err := <-secondDone; err != nil {
			t.Fatal(err)
		}
		got, err := subject.Store.GetSession(ctx, record.ID)
		if err != nil || got.Title != "beta" {
			t.Fatalf("serialized title = %q, error = %v", got.Title, err)
		}
	})
}
