package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func TestDiscoveryIndependentCommittedConnections(t *testing.T) {
	for _, mode := range []string{"DELETE", "WAL"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			path := filepath.Join(t.TempDir(), "store.db")
			reader := discoveryStore(t, path)
			if _, err := reader.db.Exec("PRAGMA journal_mode=" + mode); err != nil {
				t.Fatal(err)
			}
			writer := discoveryStore(t, path)
			for _, id := range []session.ID{"b", "d"} {
				putDiscovery(t, writer, session.Session{ID: id, WorkspaceID: "A", Title: string(id)})
			}
			q := session.SessionDiscoveryQuery{WorkspaceID: "A", Limit: 1}
			first := listDiscovery(t, reader, q)
			q.Cursor = first.NextCursor
			q.Limit = 100
			tx, err := writer.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			child := &Store{db: writer.db, tx: tx}
			for _, id := range []session.ID{"a", "z"} {
				putDiscovery(t, child, session.Session{ID: id, WorkspaceID: "A"})
			}
			for _, id := range []session.ID{"b", "d"} {
				s, err := child.GetSession(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				s.Title = "edited-" + string(id)
				if err = child.UpdateSession(ctx, s); err != nil {
					t.Fatal(err)
				}
			}
			page, err := reader.ListSessions(ctx, q)
			if err != nil || len(page.Sessions) != 1 || page.Sessions[0].Title != "b" {
				t.Fatal("uncommitted changes visible", page, err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			page, err = reader.ListSessions(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Sessions) != 2 || page.Sessions[0].ID != "b" || page.Sessions[0].Title != "edited-b" || page.Sessions[1].ID != "a" || page.NextCursor != "" {
				t.Fatal(page)
			}
			refreshed := listDiscovery(t, reader, session.SessionDiscoveryQuery{WorkspaceID: "A"})
			var ids []session.ID
			for _, s := range refreshed.Sessions {
				ids = append(ids, s.ID)
			}
			if !reflect.DeepEqual(ids, []session.ID{"z", "d", "b", "a"}) || refreshed.Sessions[1].Title != "edited-d" {
				t.Fatal(refreshed)
			}
		})
	}
}
func TestDiscoveryWorkspaceIndexPlan(t *testing.T) {
	s := discoveryStore(t, filepath.Join(t.TempDir(), "store.db"))
	if err := s.WithinTx(t.Context(), func(ctx context.Context, tx session.Store) error {
		for i := 0; i < 300; i++ {
			ws := "B"
			if i < 3 {
				ws = "A"
			}
			if _, err := tx.CreateSession(ctx, session.Session{ID: session.ID(fmt.Sprintf("id-%03d", i)), WorkspaceID: ws}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, continuation := range []bool{false, true} {
		query := discoverySQL(continuation)
		// The SQL projection, not substring absence in an output, proves private
		// record JSON never crosses the driver boundary.
		if strings.Contains(query, "record") || strings.Contains(query, "OFFSET") || strings.Count(query, "CASE WHEN") != 5 {
			t.Fatal(query)
		}
		args := []any{1024, 1024, 16384, 30, 30, "A"}
		if continuation {
			args = append(args, "", "id-002")
		}
		args = append(args, 2)
		rows, err := s.db.Query("EXPLAIN QUERY PLAN "+query, args...)
		if err != nil {
			t.Fatal(err)
		}
		var plans []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plans = append(plans, detail)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err = rows.Close(); err != nil {
			t.Fatal(err)
		}
		plan := strings.Join(plans, " ")
		if !strings.Contains(plan, "SEARCH sessions USING INDEX sessions_workspace_created_idx") || !strings.Contains(plan, "workspace_id=?") || strings.Contains(plan, "TEMP B-TREE") || strings.Contains(plan, "SCAN sessions") {
			t.Fatal(plan)
		}
		if continuation && !strings.Contains(plan, "(created_at,id)<(?,?)") {
			t.Fatal("missing tuple range", plan)
		}
	}
}
