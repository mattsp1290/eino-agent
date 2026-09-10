package sqlite

import (
	"database/sql"
	_ "embed"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The explicit baseline is shared by production migration and schema proofs.
//
//go:embed migrations/00001_initial.sql
var baselineSQL []byte

//go:embed testdata/legacy_schema.sql
var currentSchema string

const (
	baselineTime                  = "0000-01-01T00:00:00.000000000Z"
	baselineIdentityBytes         = 1024
	baselineOversizeIdentityBytes = baselineIdentityBytes + 1
	baselineTimestampBytes        = len(baselineTime)
	baselineRowIDBytes            = 8
	baselineMaxIndexValueBytes    = 2*baselineOversizeIdentityBytes + baselineTimestampBytes + baselineRowIDBytes
)

func openBaseline(t *testing.T) *sql.DB {
	t.Helper()
	db := openBaselinePool(t, filepath.Join(t.TempDir(), "baseline.db"))
	execBaseline(t, db, string(baselineSQL))
	return db
}

func baselineStrings(t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
	}()
	var result []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestBaselineSchemaTablesAndIndexes(t *testing.T) {
	db := openBaseline(t)
	indexes := map[string][]string{
		"observation_store":     nil,
		"observation_revisions": {"sqlite_autoindex_observation_revisions_1:session_id"},
		"sessions":              {"sqlite_autoindex_sessions_1:id", "sessions_workspace_created_idx:workspace_id,created_at,id"},
		"runs":                  {"sqlite_autoindex_runs_1:id", "runs_session_status_idx:session_key,status", "runs_session_active_unique_idx:session_key"},
		"messages":              {"messages_run_key_idx:run_key", "sqlite_autoindex_messages_1:id", "messages_replay_idx:session_key,created_at,id", "messages_observation_idx:session_key,created_at,id,run_key,role,finalized"},
		"admission_receipts":    {"admission_receipts_assistant_message_key_idx:assistant_message_key", "admission_receipts_run_key_idx:run_key", "admission_receipts_user_message_key_idx:user_message_key", "sqlite_autoindex_admission_receipts_1:run_key", "sqlite_autoindex_admission_receipts_2:session_key,admission_key"},
		"parts":                 {"parts_message_key_idx:message_key", "parts_run_key_idx:run_key", "sqlite_autoindex_parts_1:id", "parts_replay_idx:session_key,message_key,ordinal,id", "parts_observation_idx:session_key,message_key,kind,ordinal,id,run_key,text_valid"},
		"tool_calls":            {"tool_calls_request_message_key_idx:request_message_key", "tool_calls_request_part_key_idx:request_part_key", "sqlite_autoindex_tool_calls_1:id", "tools_unfinished_idx:run_key,status", "tools_observation_idx:session_key,run_key,id"},
		"context_epochs":        {"sqlite_autoindex_context_epochs_1:id", "context_epochs_session_created_idx:session_key,created_at,id"},
		"model_requests":        {"model_requests_session_key_idx:session_key", "sqlite_autoindex_model_requests_1:id", "model_requests_run_attempt_step_idx:run_key,attempt,step", "model_requests_run_created_idx:run_key,created_at,id"},
		"events":                {"events_run_key_idx:run_key", "events_tool_key_idx:tool_key", "sqlite_autoindex_events_1:id", "events_replay_idx:session_key,created_at,id", "events_tool_transition_unique_idx:tool_key,tool_transition", "events_run_finished_unique_idx:run_key,kind"},
	}
	var wantTables []string
	for table := range indexes {
		wantTables = append(wantTables, table)
	}
	sort.Strings(wantTables)
	if got := baselineStrings(t, db, `SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name`); !reflect.DeepEqual(got, wantTables) {
		t.Fatalf("tables: got %v want %v", got, wantTables)
	}
	for table, want := range indexes {
		t.Run(table, func(t *testing.T) {
			pk := "row_key"
			if table == "observation_store" {
				pk = "singleton"
			}
			if table == "observation_revisions" {
				pk = "session_id"
			}
			wantPK := pk + ":INTEGER"
			if pk == "session_id" {
				wantPK = pk + ":BLOB"
			}
			if got := baselineStrings(t, db, `SELECT name || ':' || type FROM pragma_table_info(?) WHERE pk>0`, table); !reflect.DeepEqual(got, []string{wantPK}) {
				t.Fatalf("primary key: %v want %s", got, wantPK)
			}
			var got []string
			for _, name := range baselineStrings(t, db, `SELECT name FROM pragma_index_list(?)`, table) {
				cols := baselineStrings(t, db, `SELECT name FROM pragma_index_xinfo(?) WHERE cid>=0 ORDER BY seqno`, name)
				got = append(got, name+":"+strings.Join(cols, ","))
				// Include all stored columns, even auxiliary index payload. The largest
				// fixture key has two one-byte-over identities, a fixed time, and
				// a rowid; its value-byte bound is derived above. SQLite has no PostgreSQL btree tuple ceiling; the latter
				// needs its own physical-size proof in the PostgreSQL slice.
				valueBytes := baselineRowIDBytes // implicit rowid
				for _, col := range cols {
					switch col {
					case "id", "session_id", "workspace_id":
						valueBytes += baselineOversizeIdentityBytes
					case "admission_key":
						valueBytes += 256
					case "created_at":
						valueBytes += baselineTimestampBytes
					case "status", "role", "kind", "tool_transition":
						valueBytes += 14
					case "session_key", "run_key", "message_key", "tool_key", "request_message_key", "request_part_key", "user_message_key", "assistant_message_key", "ordinal", "attempt", "step", "finalized", "text_valid":
						valueBytes += baselineRowIDBytes
					default:
						t.Fatalf("%s indexes private or unbounded projection %s", name, col)
					}
				}
				if valueBytes > baselineMaxIndexValueBytes {
					t.Fatalf("%s key values exceed fixture bound: %d", name, valueBytes)
				}
			}
			sort.Strings(got)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("indexes: got %v want %v", got, want)
			}
		})
	}
}

func populateBaseline(t *testing.T, db *sql.DB) {
	t.Helper()
	insertBaselineSession(t, db, 1, "s1", "w", "0000-01-01T00:00:00.000000000Z")
	insertBaselineRun(t, db, 3, "r1", 1, "pending")
	execBaseline(t, db, `INSERT INTO context_epochs(row_key,id,session_key,record,created_at,closed_at) VALUES(7,x'6570',1,x'7b7d',?,'')`, "0000-01-01T00:00:00.000000000Z")
	insertBaselineMessage(t, db, 4, "m1", 1, 3, "assistant")
	insertBaselinePart(t, db, 5, "p1", 4, 1, 3, 0, "tool_call")
	insertBaselineTool(t, db, 6, "t1", 1, 3, 4, 5, "pending")
	insertBaselineModelRequest(t, db, "q1", 1, 3, "future-assistant", 0, 0)
	insertBaselineEvent(t, db, "e1", 1, 3, 6, "tool_pending", "pending")
}

func expectBaselineError(t *testing.T, db *sql.DB, fragment, query string, args ...any) {
	t.Helper()
	_, err := db.Exec(query, args...)
	if err == nil || !strings.Contains(err.Error(), fragment) {
		t.Fatalf("%s: got %v, want %s", query, err, fragment)
	}
}

func assertBaselineIndexedLookup(t *testing.T, db *sql.DB, table, column string) {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN SELECT rowid FROM "+table+" WHERE "+column+"=?", 999)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
	}()
	count := 0
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(detail, "SEARCH ") {
			t.Errorf("%s.%s foreign-key lookup scans: %s", table, column, detail)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("%s.%s lookup produced %d plan steps, want one index search", table, column, count)
	}
}

func TestBaselineSchemaForeignKeys(t *testing.T) {
	db := openBaseline(t)
	populateBaseline(t, db)
	owners := map[string][]string{
		"runs":           {"session_key:sessions"},
		"messages":       {"run_key:runs", "session_key:sessions"},
		"parts":          {"message_key:messages", "run_key:runs", "session_key:sessions"},
		"tool_calls":     {"request_message_key:messages", "request_part_key:parts", "run_key:runs", "session_key:sessions"},
		"context_epochs": {"session_key:sessions"},
		"model_requests": {"run_key:runs", "session_key:sessions"},
		"events":         {"run_key:runs", "session_key:sessions", "tool_key:tool_calls"},
	}
	for table, want := range owners {
		if got := baselineStrings(t, db, `SELECT "from" || ':' || "table" FROM pragma_foreign_key_list(?) WHERE "to"='row_key' ORDER BY "from"`, table); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s foreign keys: got %v want %v", table, got, want)
		}
		for _, owner := range want {
			col := strings.Split(owner, ":")[0]
			if got := baselineStrings(t, db, `SELECT type FROM pragma_table_info(?) WHERE name=?`, table, col); !reflect.DeepEqual(got, []string{"INTEGER"}) {
				t.Fatalf("%s.%s type: %v", table, col, got)
			}
			assertBaselineIndexedLookup(t, db, table, col)
			expectBaselineError(t, db, "FOREIGN KEY constraint failed", "UPDATE "+table+" SET "+col+"=999")
			if col != "tool_key" {
				expectBaselineError(t, db, "NOT NULL constraint failed", "UPDATE "+table+" SET "+col+"=NULL")
			}
		}
	}
	// A second live connection must enforce the same host-provided DSN setting.
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()
	var enabled int
	if err := db.QueryRow("PRAGMA foreign_keys").Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled != 1 {
		t.Fatal("second connection has foreign keys disabled")
	}
}

func TestBaselineSchemaChecksAndUniqueness(t *testing.T) {
	db := openBaseline(t)
	populateBaseline(t, db)
	for _, query := range []string{
		`UPDATE sessions SET id='text'`, `UPDATE sessions SET workspace_id='text'`,
		`UPDATE sessions SET record='text'`, `UPDATE sessions SET title='text'`,
		`UPDATE sessions SET created_at=x'00'`, `UPDATE runs SET status='unknown'`,
		`UPDATE runs SET lease_until=1.5`, `UPDATE messages SET finalized=2`,
		`UPDATE parts SET text_valid=2`, `UPDATE parts SET ordinal=1.5`,
		`UPDATE events SET kind='text'`, `UPDATE events SET tool_transition=NULL`,
		`UPDATE events SET tool_key=NULL`, `UPDATE observation_revisions SET revision=-1`,
		`UPDATE observation_store SET incarnation='a000000000000000000000000000000g'`,
	} {
		expectBaselineError(t, db, "CHECK constraint failed", query)
	}
	insertBaselineRun(t, db, 8, "r2", 1, "completed")
	expectBaselineError(t, db, "UNIQUE constraint failed", `UPDATE runs SET status='running' WHERE row_key=8`)
	insertBaselineModelRequest(t, db, "q2", 1, 3, "future", 1, 0)
	expectBaselineError(t, db, "UNIQUE constraint failed", `UPDATE model_requests SET attempt=0 WHERE id=?`, []byte("q2"))
	for _, transition := range []string{"pending", "running", "terminal"} {
		if transition != "pending" {
			insertBaselineEvent(t, db, transition, 1, 3, 6, "tool_transition", transition)
		}
		expectBaselineError(t, db, "UNIQUE constraint failed", `INSERT INTO events(id,session_key,run_key,tool_key,kind,tool_transition,record,created_at) VALUES(?,1,3,6,?,?,x'7b7d','')`, []byte("duplicate"), []byte("tool_transition"), transition)
	}
	insertBaselineEvent(t, db, "finished", 1, 3, nil, "run_finished", nil)
	expectBaselineError(t, db, "UNIQUE constraint failed", `INSERT INTO events(id,session_key,run_key,kind,record,created_at) VALUES(?,1,3,?,x'7b7d','')`, []byte("duplicate"), []byte("run_finished"))
	insertBaselineEvent(t, db, "custom", 1, 3, nil, "run_finished\x00suffix", nil)
}

func TestBaselineObservationTriggers(t *testing.T) {
	db := openBaseline(t)
	assertRevision := func(id string, want int) {
		t.Helper()
		var got int
		if err := db.QueryRow(`SELECT revision FROM observation_revisions WHERE session_id=?`, []byte(id)).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s revision=%d, want %d", id, got, want)
		}
	}
	insertBaselineSession(t, db, 1, "s1", "w", "0000-01-01T00:00:00.000000000Z")
	assertRevision("s1", 1)
	insertBaselineSession(t, db, 2, "s2", "w", "0000-01-01T00:00:00.000000000Z")
	assertRevision("s2", 1)
	execBaseline(t, db, `UPDATE sessions SET id=? WHERE row_key=1`, []byte("renamed"))
	assertRevision("s1", 2)
	assertRevision("renamed", 1)
	insertBaselineRun(t, db, 3, "r1", 1, "pending")
	assertRevision("renamed", 2)
	insertBaselineMessage(t, db, 4, "m1", 1, 3, "assistant")
	assertRevision("renamed", 3)
	insertBaselinePart(t, db, 5, "p1", 4, 1, 3, 0, "tool_call")
	assertRevision("renamed", 4)
	insertBaselineTool(t, db, 6, "t1", 1, 3, 4, 5, "pending")
	assertRevision("renamed", 5)
	for i, table := range []string{"runs", "messages", "parts", "tool_calls"} {
		execBaseline(t, db, "UPDATE "+table+" SET session_key=2")
		assertRevision("renamed", 6+i)
		assertRevision("s2", 2+i)
	}
	// Updates within one owner invalidate both OLD and NEW projections.
	for i, table := range []string{"sessions", "runs", "messages", "parts", "tool_calls"} {
		where := ""
		if table == "sessions" {
			where = " WHERE row_key=2"
		}
		execBaseline(t, db, "UPDATE "+table+" SET record=record"+where)
		assertRevision("s2", 7+2*i)
	}
	for i, table := range []string{"tool_calls", "parts", "messages", "runs", "sessions"} {
		where := ""
		if table == "sessions" {
			where = " WHERE row_key=2"
		}
		execBaseline(t, db, "DELETE FROM "+table+where)
		assertRevision("s2", 16+i)
	}
	// Deletion leaves the revision tombstone and does not bump a former owner.
	assertRevision("renamed", 9)
	assertRevision("s1", 2)
}

func insertBaselineSession(t *testing.T, db *sql.DB, key int, id, workspace, created string) {
	t.Helper()
	execBaseline(t, db, `INSERT INTO sessions(row_key,id,record,workspace_id,title,created_at,updated_at) VALUES(?,?,?,?,?,?,?)`, key, []byte(id), []byte("{}"), []byte(workspace), []byte("title"), created, created)
}
func insertBaselineRun(t *testing.T, db *sql.DB, key int, id string, sessionKey int, status string) {
	t.Helper()
	execBaseline(t, db, `INSERT INTO runs(row_key,id,session_key,status,provider_id,model_id,owner_id,claim_token,lease_until,record,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, key, []byte(id), sessionKey, status, []byte("p"), []byte("m"), []byte("owner"), []byte("token"), 0, []byte("{}"), baselineTime)
}
func insertBaselineMessage(t *testing.T, db *sql.DB, key int, id string, sessionKey, runKey int, role string) {
	t.Helper()
	execBaseline(t, db, `INSERT INTO messages(row_key,id,session_key,run_key,role,finalized,record,created_at) VALUES(?,?,?,?,?,?,?,?)`, key, []byte(id), sessionKey, runKey, role, 0, []byte("{}"), baselineTime)
}
func insertBaselinePart(t *testing.T, db *sql.DB, key int, id string, messageKey, sessionKey, runKey, ordinal int, kind string) {
	t.Helper()
	execBaseline(t, db, `INSERT INTO parts(row_key,id,message_key,session_key,run_key,ordinal,kind,display_text,text_valid,record,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, key, []byte(id), messageKey, sessionKey, runKey, ordinal, kind, []byte{}, 0, []byte("{}"), baselineTime)
}
func insertBaselineTool(t *testing.T, db *sql.DB, key int, id string, sessionKey, runKey, messageKey, partKey int, status string) {
	t.Helper()
	execBaseline(t, db, `INSERT INTO tool_calls(row_key,id,session_key,run_key,request_message_key,request_part_key,result_message_id,result_part_id,status,name,claimed_by,claim_token,record) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, key, []byte(id), sessionKey, runKey, messageKey, partKey, []byte("future-message"), []byte("future-part"), status, []byte("tool"), []byte{}, []byte{}, []byte("{}"))
}
func insertBaselineModelRequest(t *testing.T, db *sql.DB, id string, sessionKey, runKey int, assistant string, attempt, step int) {
	t.Helper()
	execBaseline(t, db, `INSERT INTO model_requests(id,session_key,run_key,assistant_message_id,attempt,step,state,record,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, []byte(id), sessionKey, runKey, []byte(assistant), attempt, step, "prepared", []byte("{}"), baselineTime)
}
func insertBaselineEvent(t *testing.T, db *sql.DB, id string, sessionKey, runKey int, tool any, kind string, transition any) {
	t.Helper()
	execBaseline(t, db, `INSERT INTO events(id,session_key,run_key,tool_key,kind,tool_transition,record,created_at) VALUES(?,?,?,?,?,?,?,?)`, []byte(id), sessionKey, runKey, tool, []byte(kind), transition, []byte("{}"), baselineTime)
}
func execBaseline(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}
