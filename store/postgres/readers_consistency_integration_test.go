//go:build postgres_integration

package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/postgres"
)

func testReadersConsistency(t *testing.T, server *testpostgres.Server) {
	t.Run("observation", func(t *testing.T) { testObservationConsistency(t, server) })
	t.Run("discovery", func(t *testing.T) { testDiscoveryConsistency(t, server) })
}

func testObservationConsistency(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	run := f.seed(t, "reader-session", "reader-run")
	writerDB := f.database.Open(t)
	writer, err := postgres.New(f.ctx, writerDB)
	if err != nil {
		t.Fatal(err)
	}
	message := session.Message{ID: "reader-assistant", SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: f.now, UpdatedAt: f.now}
	fence := session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken}
	if _, err := writer.Execution(fence).AppendMessage(f.ctx, message); err != nil {
		t.Fatalf("seed observation message: %v", err)
	}
	baseline, err := writer.ReadObservationSnapshot(f.ctx, run.SessionID, readerLimits())
	if err != nil {
		t.Fatalf("baseline observation: %v", err)
	}
	var hooked bool
	var hookErr error
	queries := 0
	sawFollowingField := false
	reader, err := postgres.NewReaderTestStore(f.db, func(_ context.Context, query string) error {
		if !hooked && strings.Contains(strings.ToLower(query), "from public.messages") {
			hooked = true
			hookErr = writer.WithinTx(f.ctx, func(ctx context.Context, tx session.Store) error {
				execution := tx.Execution(fence)
				part := session.Part{ID: "reader-text", MessageID: message.ID, SessionID: message.SessionID, RunID: message.RunID, Kind: session.PartText, Payload: json.RawMessage(`{"text":"hooked"}`), CreatedAt: f.now, UpdatedAt: f.now}
				if _, err := execution.AppendPart(ctx, part); err != nil {
					return err
				}
				request := fencingToolCreateRequest(run, message.ID, "reader-tool", "reader-tool-pending", f.now.Add(time.Second))
				if _, err := execution.CreateToolCall(ctx, request); err != nil {
					return err
				}
				return execution.FinalizeAssistantMessage(ctx, message.ID)
			})
			return hookErr
		}
		return nil
	}, func(query string, _ []any) {
		queries++
		if hooked && strings.Contains(strings.ToLower(query), "from public.parts") {
			sawFollowingField = true
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := reader.ReadObservationSnapshot(f.ctx, run.SessionID, readerLimits())
	if hookErr != nil {
		t.Fatalf("observation writer callback: %v", hookErr)
	}
	if err != nil {
		t.Fatalf("hooked observation: %v", err)
	}
	if !hooked || queries == 0 || !sawFollowingField {
		t.Fatalf("reader hook state hooked=%t queries=%d following_field=%t", hooked, queries, sawFollowingField)
	}
	if !reflect.DeepEqual(got, baseline) {
		t.Fatal("repeatable-read observation changed after synchronous writer commit")
	}
	fresh, err := writer.ReadObservationSnapshot(f.ctx, run.SessionID, readerLimits())
	if err != nil {
		t.Fatalf("fresh observation: %v", err)
	}
	if fresh.Watermark.Revision <= baseline.Watermark.Revision || len(fresh.Messages) != 1 || fresh.Messages[0].Text != "hooked" || !fresh.Messages[0].Finalized || len(fresh.Tools) != 1 || fresh.Tools[0].ID != "reader-tool" {
		t.Fatalf("fresh observation did not include committed fields: revision=%d messages=%d tools=%d", fresh.Watermark.Revision, len(fresh.Messages), len(fresh.Tools))
	}
	if f.db.Stats().InUse != 0 || writerDB.Stats().InUse != 0 {
		t.Fatalf("reader pools retained in-use connections: reader=%d writer=%d", f.db.Stats().InUse, writerDB.Stats().InUse)
	}
}

func testDiscoveryConsistency(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	old := session.Session{ID: "discover-new", WorkspaceID: "workspace", Title: "old-title", CreatedAt: f.now, UpdatedAt: f.now}
	other := session.Session{ID: "discover-old", WorkspaceID: "workspace", Title: "other-title", CreatedAt: f.now.Add(-time.Second), UpdatedAt: f.now.Add(-time.Second)}
	for _, record := range []session.Session{other, old} {
		if _, err := f.store.CreateSession(f.ctx, record); err != nil {
			t.Fatalf("seed discovery session: %v", err)
		}
	}
	writerDB := f.database.Open(t)
	var hooked bool
	var hookErr error
	reader, err := postgres.NewReaderTestStore(f.db, func(_ context.Context, query string) error {
		if !hooked && strings.Contains(strings.ToLower(query), "from public.sessions") {
			hooked = true
			hookErr = updateDiscoveryState(f.ctx, writerDB, old.ID)
		}
		return nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := reader.ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: "workspace", Limit: 1})
	if hookErr != nil {
		t.Fatalf("discovery writer callback: %v", hookErr)
	}
	if err != nil || len(first.Sessions) != 1 || first.Sessions[0].ID != old.ID || first.Sessions[0].Title != old.Title || first.NextCursor == "" {
		t.Fatalf("initial discovery page did not preserve old view: count=%d err=%v", len(first.Sessions), err)
	}
	if !hooked || hookErr != nil {
		t.Fatalf("discovery hook state hooked=%t callback=%v", hooked, hookErr)
	}
	if _, err := reader.ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: "workspace", Limit: 1, Cursor: first.NextCursor}); !errors.Is(err, session.ErrDiscoveryCursor) {
		t.Fatalf("old discovery cursor error = %v, want ErrDiscoveryCursor", err)
	}
	fresh, err := reader.ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: "workspace", Limit: 1})
	if err != nil || len(fresh.Sessions) != 1 || fresh.Sessions[0].ID != old.ID || fresh.Sessions[0].Title != "new-title" || fresh.NextCursor == "" {
		t.Fatalf("fresh discovery page did not observe committed state: count=%d err=%v", len(fresh.Sessions), err)
	}
	second, err := reader.ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: "workspace", Limit: 1, Cursor: fresh.NextCursor})
	if err != nil || len(second.Sessions) != 1 || second.Sessions[0].ID != other.ID {
		t.Fatalf("fresh discovery continuation mismatch: count=%d err=%v", len(second.Sessions), err)
	}
}

func updateDiscoveryState(ctx context.Context, db *sql.DB, id session.ID) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE public.sessions SET title = $1 WHERE id = $2", []byte("new-title"), []byte(id)); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE public.observation_store SET incarnation = $1 WHERE singleton = 1", "11111111111111111111111111111111"); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
