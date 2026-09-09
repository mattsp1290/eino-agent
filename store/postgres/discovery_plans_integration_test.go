//go:build postgres_integration

package postgres_test

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/postgres"
)

// testDiscoveryIndexPlans proves the plan used by the public reader, rather
// than a hand-written approximation of its projection and predicates.
func testDiscoveryIndexPlans(t *testing.T, server *testpostgres.Server) {
	t.Helper()
	f := newReplayFixture(t, server)
	const workspace = "workspace-42"

	// Keep the fixture large enough to distinguish the bounded workspace index
	// path while leaving the selected workspace with several pages of results.
	mustExec(t, f.db, `INSERT INTO public.sessions
		(row_key,id,record,workspace_id,title,created_at,updated_at)
	SELECT g+100, convert_to('fixture-'||g,'UTF8'), '{}'::bytea,
		convert_to('workspace-'||(g%100),'UTF8'), convert_to('title-'||g,'UTF8'),
		to_char(timestamp '2020-01-01 00:00:00 UTC' + g * interval '1 minute',
			'YYYY-MM-DD"T"HH24:MI:SS') || '.000000000Z',
		to_char(timestamp '2020-01-01 00:00:00 UTC' + g * interval '1 minute',
			'YYYY-MM-DD"T"HH24:MI:SS') || '.000000000Z'
	FROM generate_series(1,5000) AS g`)
	mustExec(t, f.db, `ANALYZE public.sessions`)
	var traces []readerQuery
	reader, err := postgres.NewReaderTestStore(f.db, nil, func(query string, args []any) {
		if strings.Contains(strings.ToLower(query), "from public.sessions") {
			traces = append(traces, readerQuery{sql: query, args: cloneReaderArgs(args)})
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := reader.ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: workspace, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Sessions) != 10 || first.NextCursor == "" {
		t.Fatalf("first discovery page: sessions=%d cursor=%q", len(first.Sessions), first.NextCursor)
	}
	second, err := reader.ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: workspace, Limit: 10, Cursor: first.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Sessions) != 10 || second.NextCursor == "" {
		t.Fatalf("second discovery page: sessions=%d cursor=%q", len(second.Sessions), second.NextCursor)
	}
	seen := make(map[session.ID]bool, len(first.Sessions)+len(second.Sessions))
	for _, page := range []session.SessionDiscoveryPage{first, second} {
		for _, item := range page.Sessions {
			if seen[item.ID] {
				t.Fatalf("cursor repeated session %q", item.ID)
			}
			seen[item.ID] = true
			if item.WorkspaceID != workspace {
				t.Fatalf("session %q escaped workspace %q", item.ID, workspace)
			}
		}
	}
	if len(seen) != 20 {
		t.Fatalf("distinct page results=%d, want 20", len(seen))
	}
	if len(traces) != 2 {
		t.Fatalf("captured %d production discovery queries, want 2", len(traces))
	}
	for i, trace := range traces {
		plan := explainDiscoveryQuery(t, f.db, trace)
		assertDiscoveryPlan(t, plan, i == 1)
	}
}

type discoveryPlan struct {
	NodeType     string          `json:"Node Type"`
	RelationName string          `json:"Relation Name"`
	IndexName    string          `json:"Index Name"`
	IndexCond    string          `json:"Index Cond"`
	Filter       string          `json:"Filter"`
	ActualRows   float64         `json:"Actual Rows"`
	Plans        []discoveryPlan `json:"Plans"`
}

func explainDiscoveryQuery(t *testing.T, db *sql.DB, trace readerQuery) discoveryPlan {
	t.Helper()
	var raw string
	if err := db.QueryRowContext(t.Context(), "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) "+trace.sql, trace.args...).Scan(&raw); err != nil {
		t.Fatalf("explain production discovery query: %v", err)
	}
	var envelope []struct {
		Plan discoveryPlan `json:"Plan"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || len(envelope) != 1 {
		t.Fatalf("decode discovery plan: %v", err)
	}
	return envelope[0].Plan
}

func assertDiscoveryPlan(t *testing.T, root discoveryPlan, continuation bool) {
	t.Helper()
	var index *discoveryPlan
	var limit *discoveryPlan
	var walk func(discoveryPlan)
	walk = func(node discoveryPlan) {
		if strings.Contains(node.NodeType, "Seq Scan") || strings.Contains(node.NodeType, "Sort") {
			t.Fatalf("unwanted node in discovery plan: %s", node.NodeType)
		}
		if node.Filter != "" {
			t.Fatalf("unbounded discovery filter: %q", node.Filter)
		}
		if node.NodeType == "Limit" {
			limit = &node
		}
		if node.IndexName == "sessions_workspace_created_idx" {
			copy := node
			index = &copy
		}
		for _, child := range node.Plans {
			walk(child)
		}
	}
	walk(root)
	if index == nil || (index.NodeType != "Index Scan" && index.NodeType != "Index Only Scan") {
		t.Fatalf("discovery did not use workspace index: %+v", root)
	}
	cond := strings.ToLower(index.IndexCond)
	if !strings.Contains(cond, "workspace_id") {
		t.Fatalf("workspace predicate absent from Index Cond: %q", index.IndexCond)
	}
	if continuation && !strings.Contains(strings.ReplaceAll(cond, " ", ""), "row(created_at,id)<row(") {
		t.Fatalf("tuple keyset predicate absent from Index Cond: %q", index.IndexCond)
	}
	if index.ActualRows != 11 {
		t.Fatalf("index scan returned %.0f rows, want exactly 10 page rows plus one lookahead", index.ActualRows)
	}
	if limit == nil || limit.ActualRows != 11 {
		t.Fatalf("unbounded discovery limit: %v", limit)
	}
}
