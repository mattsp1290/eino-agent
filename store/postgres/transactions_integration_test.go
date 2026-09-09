//go:build postgres_integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/mattsp1290/eino-agent/internal/testpostgres"
)

var (
	errTransactionAbort = errors.New("transaction callback abort")
	errTransactionPanic = errors.New("transaction callback panic")
)

func TestPostgresTransactions(t *testing.T) {
	server := testpostgres.Start(t)
	t.Run("store", func(t *testing.T) { testStoreTransactions(t, server) })
	t.Run("execution", func(t *testing.T) { testExecutionTransactions(t, server) })
}

func finishTransaction(outcome string, cancel context.CancelFunc) error {
	switch outcome {
	case "commit":
		return nil
	case "error":
		return errTransactionAbort
	case "panic":
		panic(errTransactionPanic)
	case "cancel":
		cancel()
		return nil
	default:
		panic("unknown transaction outcome: " + outcome)
	}
}

func assertTransactionOutcome(t *testing.T, outcome string, invoke func() error) {
	t.Helper()
	var err error
	var caught any
	func() {
		defer func() { caught = recover() }()
		err = invoke()
	}()
	if outcome == "panic" {
		if caught != errTransactionPanic {
			t.Fatalf("transaction panic identity changed: %T", caught)
		}
		return
	}
	if caught != nil {
		t.Fatalf("unexpected transaction panic: %T", caught)
	}
	switch outcome {
	case "commit":
		if err != nil {
			t.Fatalf("transaction commit: %v", err)
		}
	case "error":
		if !errors.Is(err, errTransactionAbort) {
			t.Fatalf("transaction lost callback error: %v", err)
		}
	case "cancel":
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("transaction lost cancellation: %v", err)
		}
	default:
		t.Fatalf("unknown transaction outcome: %s", outcome)
	}
}
