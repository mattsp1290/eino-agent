package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func TestSessionTitleReopenRevisionAndTransactionVisibility(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "title.db")
	st, err := openSQLiteFixture(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	record := session.Session{ID: "session", WorkspaceID: "workspace", Title: "initial", Metadata: map[string]string{"preserve": "yes"}, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if _, err := st.CreateSession(ctx, record); err != nil {
		t.Fatal(err)
	}
	before, err := st.ReadObservationRevision(ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	request := session.SessionTitleRequest{SessionID: record.ID, WorkspaceID: record.WorkspaceID, Title: "renamed"}
	changed, err := st.SetSessionTitle(ctx, request)
	if err != nil || !changed.Changed {
		t.Fatalf("changed = %#v, error = %v", changed, err)
	}
	after, err := st.ReadObservationRevision(ctx, record.ID)
	if err != nil || after.Revision <= before.Revision {
		t.Fatalf("revision before=%#v after=%#v error=%v", before, after, err)
	}
	if _, err := st.SetSessionTitle(ctx, request); err != nil {
		t.Fatal(err)
	}
	noOp, err := st.ReadObservationRevision(ctx, record.ID)
	if err != nil || noOp != after {
		t.Fatalf("no-op revision = %#v, want %#v, error=%v", noOp, after, err)
	}
	if _, err := st.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: record.ID, WorkspaceID: "wrong", Title: "rejected"}); !errors.Is(err, session.ErrConflict) {
		t.Fatal(err)
	}
	rejected, _ := st.ReadObservationRevision(ctx, record.ID)
	if rejected != after {
		t.Fatalf("rejected revision = %#v, want %#v", rejected, after)
	}

	other, err := reopenSQLiteFixture(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.db.Close() }()
	rollback := errors.New("rollback")
	if err := st.WithinTx(ctx, func(ctx context.Context, tx session.Store) error {
		if _, err := tx.SetSessionTitle(ctx, session.SessionTitleRequest{SessionID: record.ID, WorkspaceID: record.WorkspaceID, Title: "uncommitted"}); err != nil {
			return err
		}
		if got, err := tx.GetSession(ctx, record.ID); err != nil || got.Title != "uncommitted" {
			t.Fatalf("transaction read = %#v, error = %v", got, err)
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if got, err := other.GetSession(ctx, record.ID); err != nil || got.Title != "renamed" || got.Metadata["preserve"] != "yes" {
		t.Fatalf("committed read = %#v, error = %v", got, err)
	}
	if err := st.db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := reopenSQLiteFixture(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.db.Close() }()
	if got, err := reopened.GetSession(context.Background(), record.ID); err != nil || got.Title != "renamed" || got.Metadata["preserve"] != "yes" {
		t.Fatalf("reopened session = %#v, error = %v", got, err)
	}
}

func TestSessionTitleErrorPrivacyAndPrecedence(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	invalid := session.SessionTitleRequest{Title: string([]byte{0xff})}
	var nilStore *Store
	if result, err := nilStore.SetSessionTitle(canceled, invalid); !errors.Is(err, context.Canceled) || result != (session.SessionTitleResult{}) {
		t.Fatalf("canceled result=%#v error=%v", result, err)
	}
	if result, err := nilStore.SetSessionTitle(context.Background(), invalid); !errors.Is(err, session.ErrSessionTitleInvalid) || result != (session.SessionTitleResult{}) {
		t.Fatalf("invalid result=%#v error=%v", result, err)
	}
	valid := session.SessionTitleRequest{SessionID: "session", Title: "title"}
	if result, err := nilStore.SetSessionTitle(context.Background(), valid); !errors.Is(err, session.ErrSessionTitleStore) || err.Error() != session.ErrSessionTitleStore.Error() || result != (session.SessionTitleResult{}) {
		t.Fatalf("nil result=%#v error=%v", result, err)
	}

	st, err := openSQLiteFixture(t.Context(), filepath.Join(t.TempDir(), "privacy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.db.Close() }()
	now := time.Now().UTC()
	if _, err := st.CreateSession(t.Context(), session.Session{ID: "private", Title: "old", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(t.Context(), "UPDATE sessions SET record = ? WHERE id = ?", []byte(`{"private":"STORED_SECRET"}`), []byte("private")); err != nil {
		t.Fatal(err)
	}
	privateRequest := session.SessionTitleRequest{SessionID: "private", Title: "REJECTED_SECRET"}
	if result, err := st.SetSessionTitle(t.Context(), privateRequest); !errors.Is(err, session.ErrSessionTitleStore) || err.Error() != session.ErrSessionTitleStore.Error() || result != (session.SessionTitleResult{}) {
		t.Fatalf("corrupt result=%#v error=%v", result, err)
	}
}
