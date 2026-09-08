package sqlite

import (
	"database/sql"
	"net/url"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func openBaselinePool(t *testing.T, path string) *sql.DB {
	t.Helper()
	u := url.URL{Scheme: "file", Path: path, RawQuery: "_pragma=foreign_keys(1)"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestBaselinePendingToolReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pending.db")
	db := openBaselinePool(t, path)
	execBaseline(t, db, string(baselineSQL))
	incarnation := baselineStrings(t, db, `SELECT incarnation FROM observation_store`)
	insertBaselineSession(t, db, 1, "session", "workspace", baselineTime)
	execBaseline(t, db, `UPDATE sessions SET record=?`, []byte(`{"ID":"session","ParentID":"missing-session"}`))
	insertBaselineRun(t, db, 2, "run", 1, "pending")
	execBaseline(t, db, `UPDATE runs SET record=?`, []byte(`{"ID":"run","ParentRunID":"missing-run","ParentMsgID":"future-user","ContextEpoch":"epoch"}`))
	// Admission persists the run before its epoch and user message.
	execBaseline(t, db, `INSERT INTO context_epochs(row_key,id,session_key,record,created_at,closed_at) VALUES(3,?,1,?,?,?)`, []byte("epoch"), []byte(`{"ID":"epoch"}`), baselineTime, "")
	insertBaselineMessage(t, db, 4, "future-user", 1, 2, "user")
	execBaseline(t, db, `UPDATE messages SET record=?`, []byte(`{"ID":"future-user","ParentID":"missing-message"}`))
	insertBaselineModelRequest(t, db, "model-request", 1, 2, "future-assistant", 0, 0)
	insertBaselineMessage(t, db, 5, "request", 1, 2, "assistant")
	insertBaselinePart(t, db, 6, "request-part", 5, 1, 2, 0, "tool_call")
	insertBaselineTool(t, db, 7, "tool", 1, 2, 5, 6, "pending")
	insertBaselineEvent(t, db, "pending", 1, 2, 7, "tool_pending", "pending")
	insertBaselineEvent(t, db, "optional", 1, 2, nil, "custom", nil)
	execBaseline(t, db, `UPDATE events SET record=? WHERE id=?`, []byte(`{"MessageID":"missing-message","PartID":"missing-part","EpochID":"missing-epoch"}`), []byte("optional"))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openBaselinePool(t, path)
	if got := baselineStrings(t, db, `SELECT incarnation FROM observation_store`); !reflect.DeepEqual(got, incarnation) {
		t.Fatalf("incarnation changed: %v -> %v", incarnation, got)
	}
	if got := baselineStrings(t, db, `SELECT status FROM tool_calls`); !reflect.DeepEqual(got, []string{"pending"}) {
		t.Fatalf("pending tool status: %v", got)
	}
	for col, want := range map[string]string{"result_message_id": "future-message", "result_part_id": "future-part"} {
		if got := baselineStrings(t, db, "SELECT "+col+" FROM tool_calls"); !reflect.DeepEqual(got, []string{want}) {
			t.Fatalf("reserved %s: %v", col, got)
		}
	}
	for table, id := range map[string]string{"messages": "future-message", "parts": "future-part"} {
		if got := baselineStrings(t, db, "SELECT id FROM "+table+" WHERE id=?", []byte(id)); len(got) != 0 {
			t.Fatalf("premature %s output: %v", table, got)
		}
	}
	if got := baselineStrings(t, db, `SELECT id FROM messages WHERE id=?`, []byte("future-assistant")); len(got) != 0 {
		t.Fatalf("premature assistant correlation: %v", got)
	}
	// Settlement materializes the exact reserved outputs before the terminal row/event.
	insertBaselineMessage(t, db, 8, "future-message", 1, 2, "tool")
	insertBaselinePart(t, db, 9, "future-part", 8, 1, 2, 0, "tool_result")
	execBaseline(t, db, `UPDATE tool_calls SET status='completed'`)
	insertBaselineEvent(t, db, "terminal", 1, 2, 7, "tool_completed", "terminal")
	if got := baselineStrings(t, db, `SELECT m.id FROM tool_calls t JOIN messages m ON m.id=t.result_message_id JOIN parts p ON p.id=t.result_part_id AND p.message_key=m.row_key WHERE t.status='completed'`); !reflect.DeepEqual(got, []string{"future-message"}) {
		t.Fatalf("settled output owners: %v", got)
	}
}

// Xorshift supplies deterministic, non-repeating bytes, including invalid UTF-8.
// The fixtures exercise byte storage directly; record codec tests cover JSON.
func identityBytes(size int, state uint32) []byte {
	b := make([]byte, size)
	for i := range b {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		b[i] = byte(state >> 24)
	}
	return b
}

func TestBaselineLargeIdentityIndexes(t *testing.T) {
	db := openBaseline(t)
	tables := []string{"sessions", "runs", "context_epochs", "messages", "parts", "tool_calls", "model_requests", "events"}
	for n, size := range []int{1024, 1025} {
		key := n + 1
		ids := make(map[string]string)
		for i, table := range tables {
			ids[table] = string(identityBytes(size, uint32(0x13579bdf+n*97+i)))
		}
		workspace := identityBytes(size, uint32(0x2468ace1+n))
		workspace[0] = 0
		insertBaselineSession(t, db, key, ids["sessions"], string(workspace), baselineTime)
		insertBaselineRun(t, db, key, ids["runs"], key, "pending")
		execBaseline(t, db, `INSERT INTO context_epochs(row_key,id,session_key,record,created_at,closed_at) VALUES(?,?,?,x'7b7d',?,'')`, key, []byte(ids["context_epochs"]), key, baselineTime)
		insertBaselineMessage(t, db, key, ids["messages"], key, key, "assistant")
		insertBaselinePart(t, db, key, ids["parts"], key, key, key, 0, "tool_call")
		insertBaselineTool(t, db, key, ids["tool_calls"], key, key, key, key, "pending")
		insertBaselineModelRequest(t, db, ids["model_requests"], key, key, string(identityBytes(size, uint32(77+n))), 0, 0)
		insertBaselineEvent(t, db, ids["events"], key, key, nil, "arbitrary\x00kind", nil)
		for _, table := range tables {
			got := baselineStrings(t, db, "SELECT id FROM "+table+" WHERE id=?", []byte(ids[table]))
			if !reflect.DeepEqual(got, []string{ids[table]}) {
				t.Fatalf("%s %d-byte ID changed", table, size)
			}
		}
		if got := baselineStrings(t, db, `SELECT workspace_id FROM sessions WHERE row_key=?`, key); !reflect.DeepEqual(got, []string{string(workspace)}) {
			t.Fatalf("%d-byte workspace changed", size)
		}
		for i, transition := range []string{"pending", "running", "terminal"} {
			insertBaselineEvent(t, db, string(identityBytes(size, uint32(0xabc000+n*97+i))), key, key, key, "tool_transition", transition)
		}
		execBaseline(t, db, `UPDATE runs SET status='completed' WHERE row_key=?`, key)
		insertBaselineEvent(t, db, string(identityBytes(size, uint32(0xdef000+n))), key, key, nil, "run_finished", nil)
	}
	for _, table := range tables {
		ids := baselineStrings(t, db, "SELECT id FROM "+table)
		sort.Strings(ids) // Go string order compares bytes, including NUL/invalid UTF-8.
		if got := baselineStrings(t, db, "SELECT id FROM "+table+" ORDER BY id"); !reflect.DeepEqual(got, ids) {
			t.Fatalf("%s lost byte ordering", table)
		}
	}
	// Timestamp keys compare at nanosecond precision across year zero and a year
	// boundary; equal keys break ties on public ID bytes (including embedded NUL).
	for i, row := range []struct{ id, stamp string }{
		{"z", baselineTime}, {"a\x00z", "0000-01-01T00:00:00.000000001Z"},
		{"a", "0000-01-01T00:00:00.000000001Z"}, {"next-year", "0001-01-01T00:00:00.000000000Z"},
	} {
		insertBaselineSession(t, db, 10+i, row.id, "ordering\x00", row.stamp)
	}
	if got := baselineStrings(t, db, `SELECT id FROM sessions WHERE workspace_id=? ORDER BY created_at,id`, []byte("ordering\x00")); !reflect.DeepEqual(got, []string{"z", "a", "a\x00z", "next-year"}) {
		t.Fatalf("time/byte ordering: %q", got)
	}
}
