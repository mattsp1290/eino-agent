package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	sqliteDriver "modernc.org/sqlite"

	"github.com/mattsp1290/eino-agent/session"
)

// This test-only driver pauses QueryRow's Close after the production reader has
// consumed its watermark, while the production read transaction is still open.
type watermarkConnector struct {
	path              string
	captured, release chan struct{}
	once              sync.Once
}

func (c *watermarkConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := (&sqliteDriver.Driver{}).Open(c.path)
	if err != nil {
		return nil, err
	}
	return &watermarkConn{Conn: conn, connector: c}, nil
}
func (c *watermarkConnector) Driver() driver.Driver { return &sqliteDriver.Driver{} }

type watermarkConn struct {
	driver.Conn
	connector *watermarkConnector
}

func (c *watermarkConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
	if err != nil {
		return nil, err
	}
	if strings.Contains(q, "SELECT incarnation") {
		return &watermarkRows{Rows: rows, after: func() {
			c.connector.once.Do(func() {
				close(c.connector.captured)
				select {
				case <-c.connector.release:
				case <-ctx.Done():
				}
			})
		}}, nil
	}
	return rows, nil
}

type watermarkRows struct {
	driver.Rows
	after func()
}

func (r *watermarkRows) Close() error { err := r.Rows.Close(); r.after(); return err }

func TestObservationCommitBetweenWatermarkAndFields(t *testing.T) {
	for _, mode := range []string{"DELETE", "WAL"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			path := filepath.Join(t.TempDir(), "isolation.db")
			writer, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = writer.Close() }()
			if _, err = writer.exec(ctx, "PRAGMA journal_mode="+mode); err != nil {
				t.Fatal(err)
			}
			if _, err = writer.CreateSession(ctx, session.Session{ID: "s"}); err != nil {
				t.Fatal(err)
			}
			run, err := writer.AdmitRun(ctx, session.Run{ID: "r", SessionID: "s", Status: session.RunRunning, ClaimToken: "f"}, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			ex := writer.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
			if _, err = ex.AppendMessage(ctx, session.Message{ID: "m", SessionID: "s", RunID: "r", Role: session.RoleAssistant}); err != nil {
				t.Fatal(err)
			}
			connector := &watermarkConnector{path: path, captured: make(chan struct{}), release: make(chan struct{})}
			reader := &Store{db: sql.OpenDB(connector)}
			reader.db.SetMaxOpenConns(1)
			defer func() { _ = reader.Close() }()
			snapshots := make(chan session.ObservationSnapshot, 1)
			readErrors := make(chan error, 1)
			go func() {
				snap, err := reader.ReadObservationSnapshot(ctx, "s", observationLimits())
				snapshots <- snap
				readErrors <- err
			}()
			select {
			case <-connector.captured:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			written, committed := make(chan struct{}), make(chan error, 1)
			go func() {
				committed <- ex.WithinTx(ctx, func(ctx context.Context, tx session.ExecutionStore) error {
					if _, err := tx.AppendPart(ctx, session.Part{ID: "p", MessageID: "m", SessionID: "s", RunID: "r", Kind: session.PartText, Payload: []byte(`{"text":"atomic"}`)}); err != nil {
						return err
					}
					call := session.ToolCall{ID: "tool", SessionID: "s", RunID: "r", MessageID: "m", RequestPartID: "request", ResultMessageID: "result", ResultPartID: "result-part", Name: "echo", Pattern: "echo", Input: []byte("{}"), Status: session.ToolCallPending}
					if _, err := tx.CreateToolCall(ctx, session.CreateToolCallRequest{Call: call, RequestPart: session.Part{ID: "request", MessageID: "m", SessionID: "s", RunID: "r", Kind: session.PartToolCall, Payload: []byte(`{"id":"tool","name":"echo","arguments":{}}`)}, Event: session.ToolTransitionEvent{ID: "event", CreatedAt: time.Now()}}); err != nil {
						return err
					}
					if err := tx.FinalizeAssistantMessage(ctx, "m"); err != nil {
						return err
					}
					close(written)
					return nil
				})
			}()
			commitObserved := false
			select {
			case <-written:
			case err = <-committed:
				if err != nil {
					t.Fatal(err)
				}
				commitObserved = true
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if mode == "WAL" && !commitObserved {
				// Commit actually completes before the paused root reader selects messages,
				// parts and tools. A transactionless implementation would mix old/new fields.
				select {
				case err = <-committed:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			close(connector.release)
			if err = <-readErrors; err != nil {
				t.Fatal(err)
			}
			old := <-snapshots
			if old.Messages[0].Text != "" || old.Messages[0].Finalized || len(old.Tools) != 0 {
				t.Fatal("mixed revision snapshot", old)
			}
			if mode == "DELETE" && !commitObserved {
				select {
				case err = <-committed:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			current, err := reader.ReadObservationSnapshot(ctx, "s", observationLimits())
			if err != nil {
				t.Fatal(err)
			}
			if current.Watermark.Revision <= old.Watermark.Revision || current.Messages[0].Text != "atomic" || !current.Messages[0].Finalized || len(current.Tools) != 1 {
				t.Fatal(current)
			}
		})
	}
}
