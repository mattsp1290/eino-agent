//go:build postgres_integration

package postgres_test

import (
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

type readerQuery struct {
	sql  string
	args []any
}

func testReadersPrivacy(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	run, message, call, textPart := privacyGraph(t, f)
	private := []byte("PRIVATE_RECORD_BLOB" + strings.Repeat("x", 1<<20))
	privacyExec(t, f, "UPDATE public.sessions SET record = $1 WHERE id = $2", private, []byte(message.SessionID))
	privacyExec(t, f, "UPDATE public.runs SET record = $1 WHERE id = $2", private, []byte(run.ID))
	privacyExec(t, f, "UPDATE public.messages SET record = $1 WHERE id = $2", private, []byte(message.ID))
	privacyExec(t, f, "UPDATE public.parts SET record = $1 WHERE session_key = (SELECT row_key FROM public.sessions WHERE id = $2)", private, []byte(textPart.SessionID))
	privacyExec(t, f, "UPDATE public.tool_calls SET record = $1 WHERE id = $2", private, []byte(call.ID))

	var traces []readerQuery
	reader, err := postgres.NewReaderTestStore(f.db, nil, func(query string, args []any) {
		traces = append(traces, readerQuery{sql: query, args: cloneReaderArgs(args)})
	})
	if err != nil {
		t.Fatal(err)
	}
	observation, err := reader.ReadObservationSnapshot(f.ctx, message.SessionID, readerLimits())
	if err != nil || !observation.Exists || len(observation.Messages) != 1 || len(observation.Runs) != 1 || len(observation.Tools) != 1 || observation.Messages[0].Text != "visible text" {
		t.Fatalf("private records changed public observation: err=%v messages=%d runs=%d tools=%d", err, len(observation.Messages), len(observation.Runs), len(observation.Tools))
	}
	page, err := reader.ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: "privacy-workspace"})
	if err != nil || len(page.Sessions) != 1 || page.Sessions[0].Title != "visible title" {
		t.Fatalf("private records changed public discovery: err=%v sessions=%d", err, len(page.Sessions))
	}
	assertReaderProjectionSQL(t, traces, []string{"observation_store", "sessions", "messages", "parts", "runs", "tool_calls"})
}

func testReadersBounds(t *testing.T, server *testpostgres.Server) {
	t.Run("discovery_title", func(t *testing.T) { testReaderDiscoveryTitleBound(t, server) })
	t.Run("discovery_identity", func(t *testing.T) { testReaderDiscoveryIdentityBound(t, server) })
	t.Run("discovery_order", func(t *testing.T) { testReaderDiscoveryOrder(t, server) })
	t.Run("observation_display_text", func(t *testing.T) { testReaderDisplayTextBound(t, server) })
	t.Run("observation_provider_id", func(t *testing.T) { testReaderProviderBound(t, server) })
	t.Run("observation_tool_name", func(t *testing.T) { testReaderToolNameBound(t, server) })
}

func testReaderDiscoveryTitleBound(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	reader := f.store.(session.SessionDiscoveryReader)
	exact := session.Session{ID: "title-exact", WorkspaceID: "title-exact", Title: strings.Repeat("t", session.DiscoveryMaxTitleBytes)}
	if _, err := f.store.CreateSession(f.ctx, exact); err != nil {
		t.Fatal(err)
	}
	page, err := reader.ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: exact.WorkspaceID})
	if err != nil || len(page.Sessions) != 1 || len(page.Sessions[0].Title) != session.DiscoveryMaxTitleBytes {
		t.Fatalf("exact title bound: sessions=%d err=%v", len(page.Sessions), err)
	}
	tooLarge := session.Session{ID: "title-too-large", WorkspaceID: "title-too-large", Title: strings.Repeat("t", session.DiscoveryMaxTitleBytes+1)}
	if _, err := f.store.CreateSession(f.ctx, tooLarge); err != nil {
		t.Fatal(err)
	}
	var traces []readerQuery
	instrumented, err := postgres.NewReaderTestStore(f.db, nil, func(query string, args []any) {
		traces = append(traces, readerQuery{sql: query, args: cloneReaderArgs(args)})
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err = instrumented.ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: tooLarge.WorkspaceID})
	if !errors.Is(err, session.ErrDiscoveryTooLarge) || !reflect.DeepEqual(page, session.SessionDiscoveryPage{}) {
		t.Fatalf("oversized title: sessions=%d err=%v", len(page.Sessions), err)
	}
	assertGuardedNull(t, f, traces, "title", 2, "from public.sessions")
}

func testReaderDiscoveryIdentityBound(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	reader := f.store.(session.SessionDiscoveryReader)
	id := session.ID(strings.Repeat("i", session.DiscoveryMaxIdentityBytes+1))
	if _, err := f.store.CreateSession(f.ctx, session.Session{ID: id, WorkspaceID: "identity"}); err != nil {
		t.Fatal(err)
	}
	var traces []readerQuery
	instrumented, err := postgres.NewReaderTestStore(f.db, nil, func(query string, args []any) {
		traces = append(traces, readerQuery{sql: query, args: cloneReaderArgs(args)})
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := instrumented.ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: "identity"})
	if !errors.Is(err, session.ErrDiscoveryTooLarge) || !reflect.DeepEqual(page, session.SessionDiscoveryPage{}) {
		t.Fatalf("oversized identity: sessions=%d err=%v", len(page.Sessions), err)
	}
	assertGuardedNull(t, f, traces, "id", 0, "from public.sessions")
	page, err = reader.ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: strings.Repeat("w", session.DiscoveryMaxIdentityBytes+1)})
	if !errors.Is(err, session.ErrDiscoveryQuery) || !reflect.DeepEqual(page, session.SessionDiscoveryPage{}) {
		t.Fatalf("invalid query identity precedence: sessions=%d err=%v", len(page.Sessions), err)
	}
}

func testReaderDiscoveryOrder(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	workspace := "workspace-\x00-界"
	base := time.Date(2026, 9, 8, 1, 2, 3, 4, time.UTC)
	cases := []struct {
		id session.ID
		at time.Time
	}{
		{"year-max", time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)},
		{"é", base}, {"z\x00", base}, {"a\x00", base}, {"\x00", base},
		{"year-zero", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"zero", time.Time{}},
	}
	for i := len(cases) - 1; i >= 0; i-- {
		item := cases[i]
		if _, err := f.store.CreateSession(f.ctx, session.Session{ID: item.id, WorkspaceID: workspace, Title: "title-" + string(item.id), CreatedAt: item.at, UpdatedAt: item.at}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := f.store.(session.SessionDiscoveryReader).ListSessions(f.ctx, session.SessionDiscoveryQuery{WorkspaceID: workspace, Limit: 100})
	if err != nil || len(page.Sessions) != len(cases) {
		t.Fatalf("ordered discovery count=%d err=%v", len(page.Sessions), err)
	}
	for i, item := range cases {
		if page.Sessions[i].ID != item.id || page.Sessions[i].WorkspaceID != workspace || !page.Sessions[i].CreatedAt.Equal(item.at) || !page.Sessions[i].UpdatedAt.Equal(item.at) {
			t.Fatalf("discovery order/value mismatch at %d", i)
		}
	}
}

func testReaderDisplayTextBound(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	_, message, _, textPart := privacyGraph(t, f)
	privacyExec(t, f, "UPDATE public.parts SET display_text = $1 WHERE id = $2", []byte(strings.Repeat("x", readerLimits().MaxTextBytes+1)), []byte(textPart.ID))
	assertObservationTooLarge(t, f, message.SessionID, "display_text", 0, "from public.parts")
}

func testReaderProviderBound(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	run, message, _, _ := privacyGraph(t, f)
	privacyExec(t, f, "UPDATE public.runs SET provider_id = $1 WHERE id = $2", []byte(strings.Repeat("p", 1<<20)), []byte(run.ID))
	assertObservationTooLarge(t, f, message.SessionID, "provider_id", 1, "from public.runs")
}

func testReaderToolNameBound(t *testing.T, server *testpostgres.Server) {
	f := newReplayFixture(t, server)
	_, message, call, _ := privacyGraph(t, f)
	privacyExec(t, f, "UPDATE public.tool_calls SET name = $1 WHERE id = $2", []byte(strings.Repeat("n", 1<<20)), []byte(call.ID))
	assertObservationTooLarge(t, f, message.SessionID, "name", 2, "from public.tool_calls")
}

func privacyGraph(t *testing.T, f *replayFixture) (session.Run, session.Message, session.ToolCall, session.Part) {
	t.Helper()
	run := f.seed(t, "privacy-session", "privacy-run")
	metadata, err := f.store.GetSession(f.ctx, run.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	metadata.WorkspaceID, metadata.Title = "privacy-workspace", "visible title"
	if err := f.store.UpdateSession(f.ctx, metadata); err != nil {
		t.Fatal(err)
	}
	execution := f.store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	message := session.Message{ID: "privacy-message", SessionID: run.SessionID, RunID: run.ID, Role: session.RoleAssistant, CreatedAt: f.now, UpdatedAt: f.now}
	if _, err := execution.AppendMessage(f.ctx, message); err != nil {
		t.Fatal(err)
	}
	textPart := session.Part{ID: "privacy-text", MessageID: message.ID, SessionID: run.SessionID, RunID: run.ID, Kind: session.PartText, Ordinal: 0, Payload: json.RawMessage(`{"text":"visible text"}`), CreatedAt: f.now, UpdatedAt: f.now}
	if _, err := execution.AppendPart(f.ctx, textPart); err != nil {
		t.Fatal(err)
	}
	request := fencingToolCreateRequest(run, message.ID, "privacy-tool", "privacy-tool-event", f.now)
	if _, err := execution.CreateToolCall(f.ctx, request); err != nil {
		t.Fatal(err)
	}
	replayProviderPart(t, f.ctx, execution, "private-provider", message, json.RawMessage(`{"private":"provider state"}`), 2)

	return run, message, request.Call, textPart
}

func assertObservationTooLarge(t *testing.T, f *replayFixture, id session.ID, field string, index int, source string) {
	t.Helper()
	var traces []readerQuery
	reader, err := postgres.NewReaderTestStore(f.db, nil, func(query string, args []any) {
		traces = append(traces, readerQuery{sql: query, args: cloneReaderArgs(args)})
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.ReadObservationSnapshot(f.ctx, id, readerLimits())
	if !errors.Is(err, session.ErrObservationTooLarge) || !reflect.DeepEqual(snapshot, session.ObservationSnapshot{}) {
		t.Fatalf("oversized %s: nonzero snapshot or wrong error: %v", field, err)
	}
	assertGuardedNull(t, f, traces, field, index, source)
}

func assertGuardedNull(t *testing.T, f *replayFixture, traces []readerQuery, field string, index int, source string) {
	t.Helper()
	for _, item := range traces {
		lower := strings.ToLower(item.sql)
		if !strings.Contains(lower, "case when pg_catalog.octet_length(") || !strings.Contains(lower, field) || !strings.Contains(lower, source) {
			continue
		}
		rows, err := f.db.QueryContext(f.ctx, item.sql, item.args...)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		if !rows.Next() {
			_ = rows.Close()
			t.Fatalf("guarded %s query returned no rows", field)
		}
		values := make([]any, len(columns))
		pointers := make([]any, len(values))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if index >= len(values) || values[index] != nil {
			t.Fatalf("guarded %s reached driver as a value", field)
		}
		return
	}
	t.Fatalf("no guarded %s projection captured", field)
}

func assertReaderProjectionSQL(t *testing.T, traces []readerQuery, tables []string) {
	t.Helper()
	seen := make(map[string]bool)
	for _, item := range traces {
		query := strings.ToLower(item.sql)
		if !strings.HasPrefix(strings.TrimSpace(query), "select") {
			continue
		}
		if strings.Contains(query, "select *") || strings.Contains(query, "record") || strings.Contains(query, "private") {
			t.Fatalf("private projection in reader SQL")
		}
		if !strings.Contains(query, "case when pg_catalog.octet_length(") {
			t.Fatalf("reader projection missing SQL byte guard")
		}
		for _, table := range tables {
			if strings.Contains(query, "public."+table) {
				seen[table] = true
			}
		}
	}
	for _, table := range tables {
		if !seen[table] {
			t.Fatalf("reader never queried projection table %s", table)
		}
	}
}

func privacyExec(t *testing.T, f *replayFixture, query string, args ...any) {
	t.Helper()
	if _, err := f.db.ExecContext(f.ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}

func cloneReaderArgs(args []any) []any {
	cloned := make([]any, len(args))
	for i, arg := range args {
		if bytes, ok := arg.([]byte); ok {
			cloned[i] = append([]byte(nil), bytes...)
		} else {
			cloned[i] = arg
		}
	}
	return cloned
}
