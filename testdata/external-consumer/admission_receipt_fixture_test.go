package consumer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
)

// TestPublicAdmissionReceiptRecovery proves that a consumer needs only public
// store and runtime APIs to recover a committed keyed admission after losing
// its original result.
func TestPublicAdmissionReceiptRecovery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "admission-receipt.db")
	store, pool, err := openTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	selection := model.Selection{ProviderID: "discovery", ModelID: "deterministic"}
	request := runtime.Request{SessionID: "receipt-session", AdmissionKey: "consumer-event-1", Message: runtime.UserMessage{Content: "hello"}, Config: config.Snapshot{Agent: config.Agent{Name: "receipt", Model: selection}, Model: selection, Metadata: map[string]string{"workspace_id": "consumer"}}}
	owner := discoveryRuntime(t, store, &discoveryModel{})
	first, err := owner.Start(ctx, request)
	if err != nil || first.Disposition != runtime.AdmissionNew || first.Handle == nil {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	<-first.Handle.Done()
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, reopenedPool, err := reopenTestSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopenedPool.Close() }()
	retry := discoveryRuntime(t, reopened, &discoveryModel{})
	duplicate, err := retry.Start(ctx, request)
	if err != nil || duplicate.Disposition != runtime.AdmissionExisting || duplicate.Handle != nil || duplicate.Receipt != first.Receipt {
		t.Fatalf("duplicate=%#v err=%v", duplicate, err)
	}
	record, err := retry.LookupAdmission(ctx, request.SessionID, request.AdmissionKey)
	if err != nil || record.Receipt != first.Receipt {
		t.Fatalf("lookup=%#v err=%v", record, err)
	}

	request.Message.Content = "changed"
	if _, err := retry.Start(ctx, request); !errors.Is(err, session.ErrAdmissionConflict) {
		t.Fatalf("conflict=%v", err)
	}
}
