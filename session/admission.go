package session

import (
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
)

const (
	// AdmissionFingerprintVersion is the version persisted for the current
	// runtime admission fingerprint encoding.
	AdmissionFingerprintVersion uint32 = 1
	maxAdmissionKeyBytes        int    = 256
)

var (
	// ErrAdmissionConflict reports that a key is already bound to different
	// immutable caller input. It intentionally carries no caller content.
	ErrAdmissionConflict = errors.New("admission conflict")
	// ErrAdmissionInvalid reports malformed receipt or key input.
	ErrAdmissionInvalid = errors.New("invalid admission")
	// ErrAdmissionUnknown reports an outcome which cannot safely be treated as
	// absence (for example a failed commit acknowledgement).
	ErrAdmissionUnknown = errors.New("admission outcome unknown")
	// ErrAdmissionStore reports use of an admission reader in the wrong store
	// context or another safe store-boundary failure.
	ErrAdmissionStore = errors.New("admission store error")
)

// AdmissionConflictError is content-free so transport callers can expose its
// classification without leaking prompts, metadata, keys, or driver details.
type AdmissionConflictError struct{}

func (AdmissionConflictError) Error() string { return "admission conflict" }
func (AdmissionConflictError) Unwrap() error { return ErrAdmissionConflict }

// AdmissionReceipt is the immutable identity committed with one keyed run.
// Its empty Key form is returned for unkeyed Start calls but is not persisted.
type AdmissionReceipt struct {
	SessionID          ID
	Key                string
	RunID              RunID
	UserMessageID      MessageID
	AssistantMessageID MessageID
	CreatedAt          time.Time
}

// AdmissionRecord combines an immutable receipt with the opaque comparison
// digest and the current committed run-status projection.
type AdmissionRecord struct {
	Receipt            AdmissionReceipt
	FingerprintVersion uint32
	Fingerprint        [32]byte
	// RunStatus is a point-in-time projection from the same committed read as
	// the receipt. Callers must read again to observe later run transitions.
	RunStatus RunStatus
}

// ValidateAdmissionKey validates an externally supplied idempotency key.
func ValidateAdmissionKey(key string) error {
	if len(key) == 0 || len(key) > maxAdmissionKeyBytes || !utf8.ValidString(key) {
		return ErrAdmissionInvalid
	}
	for i := range len(key) {
		c := key[i]
		alphanumeric := c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
		valid := alphanumeric || c == '.' || c == '_' || c == ':' || c == '-'
		if !valid || i == 0 && !alphanumeric {
			return ErrAdmissionInvalid
		}
	}
	return nil
}

// ValidateAdmissionRecord validates the public immutable receipt shape.
func ValidateAdmissionRecord(record AdmissionRecord) error {
	r := record.Receipt
	if err := ValidateAdmissionKey(r.Key); err != nil {
		return err
	}
	if r.SessionID == "" || r.RunID == "" || r.UserMessageID == "" || r.AssistantMessageID == "" || r.CreatedAt.IsZero() ||
		!utf8.ValidString(string(r.SessionID)) || !utf8.ValidString(string(r.RunID)) || !utf8.ValidString(string(r.UserMessageID)) || !utf8.ValidString(string(r.AssistantMessageID)) ||
		record.FingerprintVersion != AdmissionFingerprintVersion {
		return fmt.Errorf("%w: receipt shape", ErrAdmissionInvalid)
	}
	return nil
}
