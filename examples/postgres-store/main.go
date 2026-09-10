// Command postgres-store demonstrates the host-owned PostgreSQL lifecycle.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/store/postgres"
)

const dsnEnv = "EINO_AGENT_POSTGRES_DSN"

func main() {
	migrate := flag.Bool("migrate", false, "apply the version-1 schema, then exit")
	flag.Parse()
	if err := run(*migrate); err != nil {
		// Do not print err: database/sql and pgx errors can contain the DSN.
		fmt.Fprintln(os.Stderr, "postgres-store: operation failed")
		os.Exit(1)
	}
}

func run(migrate bool) error {
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		return errors.New("missing PostgreSQL configuration")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }() // The host owns this pool and its shutdown.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	if migrate {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			return err
		}
		// Run this setup command with application writers quiesced.
		return postgres.Migrate(ctx, db)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	store, err := postgres.New(ctx, db) // New validates; it does not run DDL.
	if err != nil {
		return err
	}

	token := rand.Text()
	id := func(kind string) string { return kind + "-" + token }
	sessionID := session.ID(id("session"))
	runID := session.RunID(id("run"))
	messageID := session.MessageID(id("message"))
	partID := session.PartID(id("part"))
	eventID := session.EventID(id("event"))
	now := time.Now().UTC()
	sess, err := store.CreateSession(ctx, session.Session{
		ID: sessionID, Title: "PostgreSQL example", CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		return err
	}
	run, err := store.AdmitRun(ctx, session.Run{
		ID: runID, SessionID: sess.ID, OwnerID: "postgres-example", ClaimToken: id("claim"),
		Status: session.RunPending, CreatedAt: now,
	}, time.Minute)
	if err != nil {
		return err
	}
	execution := store.Execution(session.RunFence{RunID: run.ID, ClaimToken: run.ClaimToken})

	message, err := execution.AppendMessage(ctx, session.Message{
		ID: messageID, SessionID: sess.ID, RunID: run.ID, Role: session.RoleUser,
		CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		return err
	}
	if _, err := execution.AppendPart(ctx, session.Part{
		ID: partID, MessageID: message.ID, SessionID: sess.ID, RunID: run.ID,
		Kind: session.PartText, Payload: json.RawMessage(`{"text":"hello from PostgreSQL"}`),
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return err
	}
	if _, err := execution.AppendEvent(ctx, session.EventRecord{
		ID: eventID, SessionID: sess.ID, RunID: run.ID, MessageID: message.ID,
		Kind: "example.lifecycle", Redaction: session.RedactionMetadata, CreatedAt: now,
	}); err != nil {
		return err
	}
	finished := now.Add(time.Second)
	if _, err := execution.SettleRun(ctx, session.SettleRunRequest{
		Settlement: session.RunSettlement{Status: session.RunCompleted, FinishedAt: finished},
		Event:      session.RunSettlementEvent{ID: session.EventID(id("run-finished")), MessageID: message.ID},
	}); err != nil {
		return err
	}

	replay, err := store.ListMessages(ctx, sess.ID, session.ReplayCursor{Limit: 10})
	if err != nil {
		return err
	}
	if len(replay.Messages) != 1 || len(replay.Parts) != 1 || replay.Messages[0].ID != message.ID || replay.Parts[0].Kind != session.PartText {
		return errors.New("replay verification failed")
	}
	fmt.Printf("session=%s replay_messages=%d\n", sess.ID, len(replay.Messages))
	return nil
}
