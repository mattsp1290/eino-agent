//go:build postgres_integration

package postgres_test

import (
	"database/sql"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
)

func testPendingToolReopen(t *testing.T, server *testpostgres.Server) {
	database := server.Database(t)
	db := database.Open(t)
	directExecDDL(t, db)
	incarnation := queryStrings(t, db, `SELECT incarnation FROM public.observation_store`)
	insertPGSession(t, db, 1, "session", "workspace", pgTime)
	mustExec(t, db, `UPDATE public.sessions SET record=$1`, []byte(`{"ID":"session","ParentID":"missing-session"}`))
	insertPGRun(t, db, 2, "run", 1, "pending")
	mustExec(t, db, `UPDATE public.runs SET record=$1`, []byte(`{"ID":"run","ParentRunID":"missing-run","ParentMsgID":"future-user","ContextEpoch":"epoch"}`))
	// Admission persists the run before its epoch and user message.
	mustExec(t, db, `INSERT INTO public.context_epochs(row_key,id,session_key,record,created_at,closed_at) VALUES(3,$1,1,$2,$3,$4)`, []byte("epoch"), []byte(`{"ID":"epoch"}`), pgTime, "")
	insertPGMessage(t, db, 4, "future-user", 1, 2, "user")
	mustExec(t, db, `UPDATE public.messages SET record=$1`, []byte(`{"ID":"future-user","ParentID":"missing-message"}`))
	insertPGModelRequest(t, db, "model-request", 1, 2, "future-assistant", 0, 0)
	insertPGMessage(t, db, 5, "request", 1, 2, "assistant")
	insertPGPart(t, db, 6, "request-part", 5, 1, 2, 0, "tool_call")
	insertPGTool(t, db, 7, "tool", 1, 2, 5, 6, "pending")
	insertPGEvent(t, db, "pending", 1, 2, 7, "tool_pending", "pending")
	insertPGEvent(t, db, "optional", 1, 2, nil, "custom", nil)
	mustExec(t, db, `UPDATE public.events SET record=$1 WHERE id=$2`, []byte(`{"MessageID":"missing-message","PartID":"missing-part","EpochID":"missing-epoch"}`), []byte("optional"))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = database.Open(t)
	if got := queryStrings(t, db, `SELECT incarnation FROM public.observation_store`); !reflect.DeepEqual(got, incarnation) {
		t.Fatalf("incarnation changed: %v -> %v", incarnation, got)
	}
	if got := queryStrings(t, db, `SELECT status FROM public.tool_calls`); !reflect.DeepEqual(got, []string{"pending"}) {
		t.Fatalf("pending tool status: %v", got)
	}
	for col, want := range map[string]string{"result_message_id": "future-message", "result_part_id": "future-part"} {
		if got := queryStrings(t, db, "SELECT "+col+" FROM public.tool_calls"); !reflect.DeepEqual(got, []string{want}) {
			t.Fatalf("reserved %s: %v", col, got)
		}
	}
	for table, id := range map[string]string{"messages": "future-message", "parts": "future-part"} {
		if got := queryStrings(t, db, "SELECT id FROM public."+table+" WHERE id=$1", []byte(id)); len(got) != 0 {
			t.Fatalf("premature %s output: %v", table, got)
		}
	}
	if got := queryStrings(t, db, `SELECT id FROM public.messages WHERE id=$1`, []byte("future-assistant")); len(got) != 0 {
		t.Fatalf("premature assistant correlation: %v", got)
	}
	// Settlement materializes the exact reserved outputs before the terminal row/event.
	insertPGMessage(t, db, 8, "future-message", 1, 2, "tool")
	insertPGPart(t, db, 9, "future-part", 8, 1, 2, 0, "tool_result")
	mustExec(t, db, `UPDATE public.tool_calls SET status='completed'`)
	insertPGEvent(t, db, "terminal", 1, 2, 7, "tool_completed", "terminal")
	if got := queryStrings(t, db, `SELECT m.id FROM public.tool_calls t JOIN public.messages m ON m.id=t.result_message_id JOIN public.parts p ON p.id=t.result_part_id AND p.message_key=m.row_key WHERE t.status='completed'`); !reflect.DeepEqual(got, []string{"future-message"}) {
		t.Fatalf("settled output owners: %v", got)
	}
}

// Xorshift supplies deterministic, non-repeating bytes, including invalid UTF-8.
// The fixtures exercise byte storage directly; record codec tests cover JSON.
func incompressibleBytes(size int, state uint32) []byte {
	b := make([]byte, size)
	for i := range b {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		b[i] = byte(state >> 24)
	}
	return b
}

func testLargeIdentityIndexes(t *testing.T, server *testpostgres.Server) {
	db := server.Database(t).Open(t)
	directExecDDL(t, db)
	tables := []string{"sessions", "runs", "context_epochs", "messages", "parts", "tool_calls", "model_requests", "events"}
	for n, size := range []int{1024, 1025} {
		key := n + 1
		ids := make(map[string]string)
		for i, table := range tables {
			ids[table] = string(incompressibleBytes(size, uint32(0x13579bdf+n*97+i)))
		}
		workspace := incompressibleBytes(size, uint32(0x2468ace1+n))
		workspace[0] = 0
		insertPGSession(t, db, key, ids["sessions"], string(workspace), pgTime)
		insertPGRun(t, db, key, ids["runs"], key, "pending")
		mustExec(t, db, `INSERT INTO public.context_epochs(row_key,id,session_key,record,created_at,closed_at) VALUES($1,$2,$3,'{}'::bytea,$4,'')`, key, []byte(ids["context_epochs"]), key, pgTime)
		insertPGMessage(t, db, key, ids["messages"], key, key, "assistant")
		insertPGPart(t, db, key, ids["parts"], key, key, key, 0, "tool_call")
		insertPGTool(t, db, key, ids["tool_calls"], key, key, key, key, "pending")
		insertPGModelRequest(t, db, ids["model_requests"], key, key, string(incompressibleBytes(size, uint32(77+n))), 0, 0)
		insertPGEvent(t, db, ids["events"], key, key, nil, "arbitrary\x00kind", nil)
		for _, table := range tables {
			got := queryStrings(t, db, "SELECT id FROM public."+table+" WHERE id=$1", []byte(ids[table]))
			if !reflect.DeepEqual(got, []string{ids[table]}) {
				t.Fatalf("%s %d-byte ID changed", table, size)
			}
		}
		if got := queryStrings(t, db, `SELECT workspace_id FROM public.sessions WHERE row_key=$1`, key); !reflect.DeepEqual(got, []string{string(workspace)}) {
			t.Fatalf("%d-byte workspace changed", size)
		}
		for i, transition := range []string{"pending", "running", "terminal"} {
			insertPGEvent(t, db, string(incompressibleBytes(size, uint32(0xabc000+n*97+i))), key, key, key, "tool_transition", transition)
		}
		mustExec(t, db, `UPDATE public.runs SET status='completed' WHERE row_key=$1`, key)
		insertPGEvent(t, db, string(incompressibleBytes(size, uint32(0xdef000+n))), key, key, nil, "run_finished", nil)
	}
	for _, table := range tables {
		ids := queryStrings(t, db, "SELECT id FROM public."+table)
		sort.Strings(ids) // Go string order compares bytes, including NUL/invalid UTF-8.
		if got := queryStrings(t, db, "SELECT id FROM public."+table+" ORDER BY id"); !reflect.DeepEqual(got, ids) {
			t.Fatalf("%s lost byte ordering", table)
		}
	}
	// Timestamp keys compare at nanosecond precision across year zero and a year
	// boundary; equal keys break ties on public ID bytes (including embedded NUL).
	for i, row := range []struct{ id, stamp string }{
		{"z", pgTime}, {"a\x00z", "0000-01-01T00:00:00.000000001Z"},
		{"a", "0000-01-01T00:00:00.000000001Z"}, {"next-year", "0001-01-01T00:00:00.000000000Z"},
	} {
		insertPGSession(t, db, 10+i, row.id, "ordering\x00", row.stamp)
	}
	if got := queryStrings(t, db, `SELECT id FROM public.sessions WHERE workspace_id=$1 ORDER BY created_at,id`, []byte("ordering\x00")); !reflect.DeepEqual(got, []string{"z", "a", "a\x00z", "next-year"}) {
		t.Fatalf("time/byte ordering: %q", got)
	}
	assertPGIndexFootprints(t, db)
	assertPGDiscoveryPlans(t, db)
}

func assertPGIndexFootprints(t *testing.T, db *sql.DB) {
	t.Helper()
	blockSize, err := strconv.Atoi(queryStrings(t, db, `SELECT current_setting('block_size')`)[0])
	if err != nil {
		t.Fatal(err)
	}
	for table, indexes := range pgIndexes {
		for _, index := range indexes {
			// Actual inserts above exercise every index with incompressible values.
			// Composite record sizes include headers and provide a diagnostic check
			// below one third of a database page; they are not exact btree tuple sizes.
			size, err := strconv.Atoi(queryStrings(t, db, "SELECT coalesce(max(pg_column_size(ROW("+index.columns+"))),0)::text FROM public."+table)[0])
			if err != nil {
				t.Fatal(err)
			}
			if size == 0 || size*3 >= blockSize {
				t.Fatalf("%s record footprint %d versus page %d", index.name, size, blockSize)
			}
			if got := queryStrings(t, db, `SELECT (pg_relation_size($1::regclass)>0)::text`, "public."+index.name); !reflect.DeepEqual(got, []string{"true"}) {
				t.Fatalf("%s has no physical index pages", index.name)
			}
		}
	}
}
func assertPGDiscoveryPlans(t *testing.T, db *sql.DB) {
	t.Helper()
	mustExec(t, db, `INSERT INTO public.sessions(row_key,id,record,workspace_id,title,created_at,updated_at)
 SELECT g+100,convert_to('fixture-'||g,'UTF8'),'{}'::bytea,convert_to('bucket-'||(g%100),'UTF8'),'title'::bytea,$1,$1 FROM generate_series(1,5000) g`, pgTime)
	mustExec(t, db, `ANALYZE public.sessions`)
	for _, query := range []string{
		`EXPLAIN (FORMAT JSON) SELECT id FROM public.sessions WHERE workspace_id=$1 ORDER BY created_at,id LIMIT 11`,
		`EXPLAIN (FORMAT JSON) SELECT id FROM public.sessions WHERE workspace_id=$1 AND (created_at,id)>('0000-01-01T00:00:00.000000000Z','fixture-1000'::bytea) ORDER BY created_at,id LIMIT 11`,
	} {
		plan := queryStrings(t, db, query, []byte("bucket-42"))[0]
		if !strings.Contains(plan, "sessions_workspace_created_idx") || strings.Contains(plan, "Seq Scan") || strings.Contains(plan, `"Sort"`) {
			t.Fatalf("discovery requires ordered bounded index path: %s", plan)
		}
	}
}
