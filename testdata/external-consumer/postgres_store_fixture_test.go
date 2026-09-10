//go:build postgres_integration

package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/mattsp1290/eino-agent/session"
	storepostgres "github.com/mattsp1290/eino-agent/store/postgres"
)

const consumerPostgresImage = "postgres:17.9-bookworm@sha256:47f917f7409eacd22fc5dfb1dee634e1b55cf0c01d1a7eb701be2227a03e0641"

type consumerPostgres struct {
	dsn   string
	pools []*sql.DB
}

func newConsumerPostgres(t *testing.T) *consumerPostgres {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	c, err := postgres.Run(ctx, consumerPostgresImage,
		postgres.WithDatabase("eino_consumer"), postgres.WithUsername("postgres"), postgres.WithPassword("postgres"),
		testcontainers.WithAdditionalWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(5*time.Minute),
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(5*time.Minute),
		),
	)
	f := &consumerPostgres{}
	if c != nil {
		t.Cleanup(func() {
			for _, db := range f.pools {
				_ = db.Close()
			}
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := c.Terminate(cleanupCtx); err != nil {
				t.Errorf("postgres container cleanup: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatalf("postgres container startup failed: %v", err)
	}
	if c == nil {
		t.Fatal("postgres container startup returned no container")
	}
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres endpoint lookup failed: %v", err)
	}
	f.dsn = dsn
	// Register the pool with the already-installed cleanup before Ping: a
	// failed startup still closes the host-owned pool before termination.
	return f
}

func (f *consumerPostgres) open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", f.dsn)
	if err != nil {
		t.Fatalf("postgres host pool open failed: %v", err)
	}
	f.pools = append(f.pools, db)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("postgres host pool ping failed: %v", err)
	}
	return db
}

func TestPostgresConsumer(t *testing.T) {
	f := newConsumerPostgres(t)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	db := f.open(t)

	var before sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT to_regclass('public.eino_agent_goose_version')").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before.Valid {
		t.Fatalf("fresh database unexpectedly has migration table %q", before.String)
	}
	if _, err := storepostgres.New(ctx, db); err == nil {
		t.Fatal("postgres.New before Migrate unexpectedly succeeded")
	}
	var after sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT to_regclass('public.eino_agent_goose_version')").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after.Valid {
		t.Fatalf("New before Migrate performed DDL: %q", after.String)
	}
	if err := storepostgres.Migrate(ctx, db); err != nil {
		t.Fatalf("postgres.Migrate: %v", err)
	}
	store, err := storepostgres.New(ctx, db)
	if err != nil {
		t.Fatalf("postgres.New after Migrate: %v", err)
	}
	if _, err := store.Execution(session.RunFence{}).AppendMessage(ctx, session.Message{}); !errors.Is(err, session.ErrConflict) {
		t.Fatalf("invalid store operation = %v, want conflict", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("host pool after invalid store operation: %v", err)
	}
	// Constructing another store proves that abandoning the first instance does
	// not consume or close the host pool.
	store, err = storepostgres.New(ctx, db)
	if err != nil {
		t.Fatalf("postgres.New after abandoned store: %v", err)
	}

	at := time.Now().UTC().Truncate(time.Microsecond)
	sid, rid := session.ID("consumer-session"), session.RunID("consumer-run")
	if _, err := store.CreateSession(ctx, session.Session{ID: sid, WorkspaceID: "consumer", Title: "public fixture", CreatedAt: at, UpdatedAt: at}); err != nil {
		t.Fatal(err)
	}
	run, err := store.AdmitRun(ctx, session.Run{ID: rid, SessionID: sid, OwnerID: "consumer", ClaimToken: "consumer-claim", Status: session.RunPending, ProviderID: "fixture", ModelID: "fixture", CreatedAt: at}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ex := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})
	message := session.Message{ID: "consumer-message", SessionID: sid, RunID: rid, Role: session.RoleUser, CreatedAt: at, UpdatedAt: at}
	if _, err := ex.AppendMessage(ctx, message); err != nil {
		t.Fatal(err)
	}
	part := session.Part{ID: "consumer-text", MessageID: message.ID, SessionID: sid, RunID: rid, Kind: session.PartText, Payload: json.RawMessage(`{"text":"hello consumer"}`), CreatedAt: at, UpdatedAt: at}
	if _, err := ex.AppendPart(ctx, part); err != nil {
		t.Fatal(err)
	}
	if _, err := ex.AppendEvent(ctx, session.EventRecord{ID: "consumer-event", SessionID: sid, RunID: rid, Kind: "consumer_fixture", Payload: json.RawMessage(`{"ok":true}`), CreatedAt: at}); err != nil {
		t.Fatal(err)
	}
	finished := at.Add(time.Second)
	if _, err := ex.SettleRun(ctx, session.SettleRunRequest{Settlement: session.RunSettlement{Status: session.RunCompleted, FinishedAt: finished}, Event: session.RunSettlementEvent{ID: "consumer-finished", MessageID: message.ID}}); err != nil {
		t.Fatal(err)
	}
	assertConsumerReplay(t, ctx, store, sid)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedDB := f.open(t)
	reopened, err := storepostgres.New(ctx, reopenedDB)
	if err != nil {
		t.Fatalf("reopen postgres store without Migrate: %v", err)
	}
	assertConsumerReplay(t, ctx, reopened, sid)
}

func assertConsumerReplay(t *testing.T, ctx context.Context, store session.Store, sid session.ID) {
	t.Helper()
	run, err := store.GetRun(ctx, "consumer-run")
	if err != nil || run.SessionID != sid || run.Status != session.RunCompleted {
		t.Fatalf("run replay = %+v, err=%v", run, err)
	}
	messages, err := store.ListMessages(ctx, sid, session.ReplayCursor{})
	if err != nil || len(messages.Messages) != 1 || len(messages.Parts) != 1 || messages.Messages[0].ID != "consumer-message" || string(messages.Parts[0].Payload) != `{"text":"hello consumer"}` {
		t.Fatalf("message replay = %+v, err=%v", messages, err)
	}
	events, err := store.ListEvents(ctx, sid, session.EventCursor{})
	if err != nil || len(events.Events) != 2 || events.Events[0].ID != "consumer-event" || events.Events[1].ID != "consumer-finished" {
		t.Fatalf("event replay = %+v, err=%v", events, err)
	}
}
