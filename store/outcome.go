package store

import "errors"

type transactionOutcomeUnknownError struct{ cause error }

func (e transactionOutcomeUnknownError) Error() string { return e.cause.Error() }
func (e transactionOutcomeUnknownError) Unwrap() error { return e.cause }

// MarkTransactionOutcomeUnknown identifies an error returned after a commit
// may have reached durable storage even though its acknowledgement was lost.
func MarkTransactionOutcomeUnknown(err error) error {
	if err == nil || IsTransactionOutcomeUnknown(err) {
		return err
	}
	return transactionOutcomeUnknownError{cause: err}
}

// IsTransactionOutcomeUnknown reports whether durable commit success cannot
// be distinguished from rollback using the transaction result alone.
func IsTransactionOutcomeUnknown(err error) bool {
	var marked transactionOutcomeUnknownError
	return errors.As(err, &marked)
}
