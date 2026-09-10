//go:build postgres_integration

package postgres_test

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
)

//go:embed migrations/00001_initial.sql
var baselineSQL []byte

const pgTime = "0000-01-01T00:00:00.000000000Z"

func TestPostgresBaseline(t *testing.T) {
	server := testpostgres.Start(t)
	t.Run("catalog", func(t *testing.T) { testCatalog(t, server) })
	t.Run("constraints", func(t *testing.T) { testConstraints(t, server) })
	t.Run("revisions", func(t *testing.T) { testRevisions(t, server) })
	t.Run("pending_reopen", func(t *testing.T) { testPendingToolReopen(t, server) })
	t.Run("large_indexes", func(t *testing.T) { testLargeIdentityIndexes(t, server) })
}

func directExecDDL(t *testing.T, db *sql.DB) { t.Helper(); mustExec(t, db, string(baselineSQL)) }
func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}
func queryStrings(t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
	}()
	var out []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		out = append(out, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
func expectPGError(t *testing.T, db *sql.DB, code, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, query, args...)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != code {
		t.Fatalf("got %v, want SQLSTATE %s", err, code)
	}
}
func assertStrings(t *testing.T, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q want %q", got, want)
	}
}

type pgIndex struct {
	name, columns   string
	unique, partial bool
}

var pgIndexes = map[string][]pgIndex{
	"observation_store": {
		{name: "observation_store_pkey", columns: "singleton", unique: true},
	},
	"observation_revisions": {
		{name: "observation_revisions_pkey", columns: "session_id", unique: true},
	},
	"sessions": {
		{name: "sessions_pkey", columns: "row_key", unique: true},
		{name: "sessions_id_key", columns: "id", unique: true},
		{name: "sessions_workspace_created_idx", columns: "workspace_id,created_at,id"},
	},
	"runs": {
		{name: "runs_pkey", columns: "row_key", unique: true},
		{name: "runs_id_key", columns: "id", unique: true},
		{name: "runs_session_status_idx", columns: "session_key,status"},
		{name: "runs_session_active_unique_idx", columns: "session_key", unique: true, partial: true},
	},
	"messages": {
		{name: "messages_pkey", columns: "row_key", unique: true},
		{name: "messages_id_key", columns: "id", unique: true},
		{name: "messages_replay_idx", columns: "session_key,created_at,id"},
		{name: "messages_observation_idx", columns: "session_key,created_at,id,run_key,role,finalized", partial: true},
		{name: "messages_run_key_idx", columns: "run_key"},
	},
	"admission_receipts": {
		{name: "admission_receipts_pkey", columns: "row_key", unique: true},
		{name: "admission_receipts_run_key_key", columns: "run_key", unique: true},
		{name: "admission_receipts_session_key_admission_key_key", columns: "session_key,admission_key", unique: true},
		{name: "admission_receipts_run_key_idx", columns: "run_key"},
		{name: "admission_receipts_user_message_key_idx", columns: "user_message_key"},
		{name: "admission_receipts_assistant_message_key_idx", columns: "assistant_message_key"},
	},
	"parts": {
		{name: "parts_pkey", columns: "row_key", unique: true},
		{name: "parts_id_key", columns: "id", unique: true},
		{name: "parts_replay_idx", columns: "session_key,message_key,ordinal,id"},
		{name: "parts_observation_idx", columns: "session_key,message_key,kind,ordinal,id,run_key,text_valid"},
		{name: "parts_message_key_idx", columns: "message_key"},
		{name: "parts_run_key_idx", columns: "run_key"},
	},
	"tool_calls": {
		{name: "tool_calls_pkey", columns: "row_key", unique: true},
		{name: "tool_calls_id_key", columns: "id", unique: true},
		{name: "tools_unfinished_idx", columns: "run_key,status"},
		{name: "tools_observation_idx", columns: "session_key,run_key,id"},
		{name: "tool_calls_request_message_key_idx", columns: "request_message_key"},
		{name: "tool_calls_request_part_key_idx", columns: "request_part_key"},
	},
	"context_epochs": {
		{name: "context_epochs_pkey", columns: "row_key", unique: true},
		{name: "context_epochs_id_key", columns: "id", unique: true},
		{name: "context_epochs_session_created_idx", columns: "session_key,created_at,id"},
	},
	"model_requests": {
		{name: "model_requests_pkey", columns: "row_key", unique: true},
		{name: "model_requests_id_key", columns: "id", unique: true},
		{name: "model_requests_run_attempt_step_idx", columns: "run_key,attempt,step", unique: true},
		{name: "model_requests_run_created_idx", columns: "run_key,created_at,id"},
		{name: "model_requests_session_key_idx", columns: "session_key"},
	},
	"events": {
		{name: "events_pkey", columns: "row_key", unique: true},
		{name: "events_id_key", columns: "id", unique: true},
		{name: "events_replay_idx", columns: "session_key,created_at,id"},
		{name: "events_tool_transition_unique_idx", columns: "tool_key,tool_transition", unique: true, partial: true},
		{name: "events_run_finished_unique_idx", columns: "run_key,kind", unique: true, partial: true},
		{name: "events_run_key_idx", columns: "run_key"},
		{name: "events_tool_key_idx", columns: "tool_key"},
	},
}
var pgOwners = map[string][]string{
	"runs": {"session_key:sessions"}, "messages": {"run_key:runs", "session_key:sessions"},
	"admission_receipts": {"assistant_message_key:messages", "run_key:runs", "session_key:sessions", "user_message_key:messages"},
	"parts":              {"message_key:messages", "run_key:runs", "session_key:sessions"},
	"tool_calls":         {"request_message_key:messages", "request_part_key:parts", "run_key:runs", "session_key:sessions"},
	"context_epochs":     {"session_key:sessions"}, "model_requests": {"run_key:runs", "session_key:sessions"},
	"events": {"run_key:runs", "session_key:sessions", "tool_key:tool_calls"},
}

func testCatalog(t *testing.T, server *testpostgres.Server) {
	db := server.Database(t).Open(t)
	directExecDDL(t, db)
	var tables []string
	for table := range pgIndexes {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	assertStrings(t, queryStrings(t, db, `SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public' ORDER BY tablename`), tables)
	assertStrings(t, queryStrings(t, db, `SELECT datcollate FROM pg_catalog.pg_database WHERE datname=current_database()`), []string{"en_US.utf8"})
	for table, expected := range pgIndexes {
		got := queryStrings(t, db, `SELECT ic.relname || ':' || string_agg(coalesce(a.attname,'<expression>'),',' ORDER BY k.ord)
   FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class ic ON ic.oid=i.indexrelid
   CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,ord)
   LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
   WHERE i.indrelid=$1::regclass GROUP BY ic.relname ORDER BY ic.relname`, "public."+table)
		want := make([]string, 0, len(expected))
		for _, index := range expected {
			want = append(want, index.name+":"+index.columns)
		}
		sort.Strings(want)
		assertStrings(t, got, want)
		pk := "row_key:bigint"
		if table == "observation_store" {
			pk = "singleton:integer"
		}
		if table == "observation_revisions" {
			pk = "session_id:bytea"
		}
		assertStrings(t, queryStrings(t, db, `SELECT a.attname || ':' || pg_catalog.format_type(a.atttypid,a.atttypmod) FROM pg_catalog.pg_index i JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0] WHERE i.indrelid=$1::regclass AND i.indisprimary`, "public."+table), []string{pk})
		for _, index := range expected {
			assertStrings(t, queryStrings(t, db, `SELECT indisunique::text || ':' || (indpred IS NOT NULL)::text FROM pg_catalog.pg_index WHERE indexrelid=$1::regclass`, "public."+index.name), []string{strconv.FormatBool(index.unique) + ":" + strconv.FormatBool(index.partial)})
		}
	}
	// Every time projection uses C; bytea does not use a text collation.
	assertStrings(t, queryStrings(t, db, `SELECT c.relname || '.' || a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND a.attname IN ('created_at','updated_at','closed_at') AND a.attcollation<>'pg_catalog."C"'::regcollation`), nil)
	for table, want := range pgOwners {
		assertStrings(t, queryStrings(t, db, `SELECT a.attname || ':' || parent.relname FROM pg_catalog.pg_constraint f JOIN pg_catalog.pg_attribute a ON a.attrelid=f.conrelid AND a.attnum=f.conkey[1] JOIN pg_catalog.pg_class parent ON parent.oid=f.confrelid WHERE f.conrelid=$1::regclass AND f.contype='f' ORDER BY a.attname`, "public."+table), want)
	}
	assertStrings(t, queryStrings(t, db, `SELECT f.conname FROM pg_catalog.pg_constraint f JOIN pg_catalog.pg_namespace n ON n.oid=f.connamespace WHERE n.nspname='public' AND f.contype='f' AND NOT EXISTS (SELECT FROM pg_catalog.pg_index i WHERE i.indrelid=f.conrelid AND i.indkey[0]=f.conkey[1] AND i.indpred IS NULL AND i.indisvalid)`), nil)
}

func populatePG(t *testing.T, db *sql.DB) {
	t.Helper()
	insertPGSession(t, db, 1, "s1", "w", pgTime)
	insertPGRun(t, db, 3, "r1", 1, "pending")
	mustExec(t, db, `INSERT INTO public.context_epochs(row_key,id,session_key,record,created_at,closed_at) VALUES(7,'epoch',1,'{}',$1,'')`, pgTime)
	insertPGMessage(t, db, 4, "m1", 1, 3, "assistant")
	insertPGAdmissionReceipt(t, db, 1, 3, 4, 4)
	insertPGPart(t, db, 5, "p1", 4, 1, 3, 0, "tool_call")
	insertPGTool(t, db, 6, "t1", 1, 3, 4, 5, "pending")
	insertPGModelRequest(t, db, "q1", 1, 3, "future-assistant", 0, 0)
	insertPGEvent(t, db, "e1", 1, 3, 6, "tool_pending", "pending")
}
func testConstraints(t *testing.T, server *testpostgres.Server) {
	db := server.Database(t).Open(t)
	directExecDDL(t, db)
	populatePG(t, db)
	for table, owners := range pgOwners {
		for _, owner := range owners {
			col := strings.Split(owner, ":")[0]
			expectPGError(t, db, "23503", "UPDATE public."+table+" SET "+col+"=999")
			if col != "tool_key" {
				expectPGError(t, db, "23502", "UPDATE public."+table+" SET "+col+"=NULL")
			}
		}
	}
	for _, q := range []string{`UPDATE public.messages SET finalized=2`, `UPDATE public.parts SET text_valid=2`, `UPDATE public.runs SET status='unknown'`, `UPDATE public.events SET tool_transition=NULL`, `UPDATE public.observation_revisions SET revision=-1`} {
		expectPGError(t, db, "23514", q)
	}
	insertPGRun(t, db, 8, "r2", 1, "completed")
	expectPGError(t, db, "23505", `UPDATE public.runs SET status='running' WHERE row_key=8`)
	insertPGModelRequest(t, db, "q2", 1, 3, "future", 1, 0)
	expectPGError(t, db, "23505", `UPDATE public.model_requests SET attempt=0 WHERE id=$1`, []byte("q2"))
	for _, transition := range []string{"pending", "running", "terminal"} {
		if transition != "pending" {
			insertPGEvent(t, db, transition, 1, 3, 6, "tool_transition", transition)
		}
		expectPGError(t, db, "23505", `INSERT INTO public.events(id,session_key,run_key,tool_key,kind,tool_transition,record,created_at) VALUES('duplicate',1,3,6,'tool_transition',$1,'{}',$2)`, transition, pgTime)
	}
	insertPGEvent(t, db, "finished", 1, 3, nil, "run_finished", nil)
	expectPGError(t, db, "23505", `INSERT INTO public.events(id,session_key,run_key,kind,record,created_at) VALUES('duplicate',1,3,'run_finished','{}',$1)`, pgTime)
	insertPGEvent(t, db, "custom", 1, 3, nil, "run_finished\x00suffix", nil)
}
func testRevisions(t *testing.T, server *testpostgres.Server) {
	db := server.Database(t).Open(t)
	directExecDDL(t, db)
	revision := func(id string, want int) {
		t.Helper()
		assertStrings(t, queryStrings(t, db, `SELECT revision::text FROM public.observation_revisions WHERE session_id=$1`, []byte(id)), []string{strconv.Itoa(want)})
	}
	insertPGSession(t, db, 1, "s1", "w", pgTime)
	revision("s1", 1)
	insertPGSession(t, db, 2, "s2", "w", pgTime)
	revision("s2", 1)
	mustExec(t, db, `UPDATE public.sessions SET id='renamed' WHERE row_key=1`)
	revision("s1", 2)
	revision("renamed", 1)
	insertPGRun(t, db, 3, "r1", 1, "pending")
	revision("renamed", 2)
	insertPGMessage(t, db, 4, "m1", 1, 3, "assistant")
	revision("renamed", 3)
	insertPGPart(t, db, 5, "p1", 4, 1, 3, 0, "tool_call")
	revision("renamed", 4)
	insertPGTool(t, db, 6, "t1", 1, 3, 4, 5, "pending")
	revision("renamed", 5)
	for i, table := range []string{"runs", "messages", "parts", "tool_calls"} {
		mustExec(t, db, "UPDATE public."+table+" SET session_key=2")
		revision("renamed", 6+i)
		revision("s2", 2+i)
	}
	for i, table := range []string{"sessions", "runs", "messages", "parts", "tool_calls"} {
		where := ""
		if table == "sessions" {
			where = " WHERE row_key=2"
		}
		mustExec(t, db, "UPDATE public."+table+" SET record=record"+where)
		revision("s2", 7+2*i)
	}
	for i, table := range []string{"tool_calls", "parts", "messages", "runs", "sessions"} {
		where := ""
		if table == "sessions" {
			where = " WHERE row_key=2"
		}
		mustExec(t, db, "DELETE FROM public."+table+where)
		revision("s2", 16+i)
	}
	revision("renamed", 9)
	revision("s1", 2)
	mustExec(t, db, `UPDATE public.observation_revisions SET revision=9223372036854775807 WHERE session_id='renamed'`)
	expectPGError(t, db, "22003", `UPDATE public.sessions SET title='overflow' WHERE row_key=1`)
	assertStrings(t, queryStrings(t, db, `SELECT title FROM public.sessions WHERE row_key=1`), []string{"title"})
}

func insertPGSession(t *testing.T, db *sql.DB, key int, id, workspace, created string) {
	t.Helper()
	mustExec(t, db, `INSERT INTO public.sessions(row_key,id,record,workspace_id,title,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7)`, key, []byte(id), []byte("{}"), []byte(workspace), []byte("title"), created, created)
}
func insertPGRun(t *testing.T, db *sql.DB, key int, id string, sessionKey int, status string) {
	t.Helper()
	mustExec(t, db, `INSERT INTO public.runs(row_key,id,session_key,status,provider_id,model_id,owner_id,claim_token,lease_until,record,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, key, []byte(id), sessionKey, status, []byte("p"), []byte("m"), []byte("owner"), []byte("token"), 0, []byte("{}"), pgTime)
}
func insertPGMessage(t *testing.T, db *sql.DB, key int, id string, sessionKey, runKey int, role string) {
	t.Helper()
	mustExec(t, db, `INSERT INTO public.messages(row_key,id,session_key,run_key,role,finalized,record,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, key, []byte(id), sessionKey, runKey, role, 0, []byte("{}"), pgTime)
}

func insertPGAdmissionReceipt(t *testing.T, db *sql.DB, sessionKey, runKey, userMessageKey, assistantMessageKey int) {
	t.Helper()
	mustExec(t, db, `INSERT INTO public.admission_receipts(session_key,admission_key,run_key,user_message_key,assistant_message_key,fingerprint_version,fingerprint,created_at) VALUES($1,$2,$3,$4,$5,1,$6,$7)`, sessionKey, []byte("admission"), runKey, userMessageKey, assistantMessageKey, make([]byte, 32), pgTime)
}

func insertPGPart(t *testing.T, db *sql.DB, key int, id string, messageKey, sessionKey, runKey, ordinal int, kind string) {
	t.Helper()
	mustExec(t, db, `INSERT INTO public.parts(row_key,id,message_key,session_key,run_key,ordinal,kind,display_text,text_valid,record,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, key, []byte(id), messageKey, sessionKey, runKey, ordinal, kind, []byte{}, 0, []byte("{}"), pgTime)
}
func insertPGTool(t *testing.T, db *sql.DB, key int, id string, sessionKey, runKey, messageKey, partKey int, status string) {
	t.Helper()
	mustExec(t, db, `INSERT INTO public.tool_calls(row_key,id,session_key,run_key,request_message_key,request_part_key,result_message_id,result_part_id,status,name,claimed_by,claim_token,record) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, key, []byte(id), sessionKey, runKey, messageKey, partKey, []byte("future-message"), []byte("future-part"), status, []byte("tool"), []byte{}, []byte{}, []byte("{}"))
}
func insertPGModelRequest(t *testing.T, db *sql.DB, id string, sessionKey, runKey int, assistant string, attempt, step int) {
	t.Helper()
	mustExec(t, db, `INSERT INTO public.model_requests(id,session_key,run_key,assistant_message_id,attempt,step,state,record,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, []byte(id), sessionKey, runKey, []byte(assistant), attempt, step, "prepared", []byte("{}"), pgTime)
}
func insertPGEvent(t *testing.T, db *sql.DB, id string, sessionKey, runKey int, tool any, kind string, transition any) {
	t.Helper()
	mustExec(t, db, `INSERT INTO public.events(id,session_key,run_key,tool_key,kind,tool_transition,record,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, []byte(id), sessionKey, runKey, tool, []byte(kind), transition, []byte("{}"), pgTime)
}
