package runtime

import (
	"context"
	"errors"

	"github.com/mattsp1290/eino-agent/session"
)

// AdmissionDisposition states whether this invocation owns a newly admitted
// execution or resolved an immutable receipt created by an earlier one.
type AdmissionDisposition string

const (
	AdmissionNew      AdmissionDisposition = "new"
	AdmissionExisting AdmissionDisposition = "existing"
)

// AdmissionResult is returned by Start. Handle is non-nil only for a new
// admission; duplicate callers use the receipt and committed readers.
type AdmissionResult struct {
	Receipt     session.AdmissionReceipt
	Disposition AdmissionDisposition
	Handle      Handle
}

func admissionResultForExisting(record session.AdmissionRecord, fingerprint [32]byte) (AdmissionResult, error) {
	if record.FingerprintVersion != session.AdmissionFingerprintVersion {
		return AdmissionResult{}, session.ErrAdmissionUnknown
	}
	if record.Fingerprint != fingerprint {
		return AdmissionResult{}, session.AdmissionConflictError{}
	}
	return AdmissionResult{Receipt: record.Receipt, Disposition: AdmissionExisting}, nil
}

func (o *StreamingOrchestrator) lookupExistingAdmission(ctx context.Context, sessionID session.ID, key string, fingerprint [32]byte) (AdmissionResult, bool, error) {
	record, err := o.store.LookupAdmission(ctx, sessionID, key)
	if errors.Is(err, session.ErrNotFound) {
		return AdmissionResult{}, false, nil
	}
	if err != nil {
		return AdmissionResult{}, false, err
	}
	result, err := admissionResultForExisting(record, fingerprint)
	return result, true, err
}

// LookupAdmission resolves a keyed receipt without model, plan, observer, or
// execution construction.
func (o *StreamingOrchestrator) LookupAdmission(ctx context.Context, sessionID session.ID, key string) (session.AdmissionRecord, error) {
	if err := o.validateConfigured(); err != nil {
		return session.AdmissionRecord{}, err
	}
	return o.store.LookupAdmission(ctx, sessionID, key)
}
