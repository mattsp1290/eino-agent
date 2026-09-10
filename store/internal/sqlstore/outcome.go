package sqlstore

import "errors"

type transactionOutcomeUnknownError struct{ cause error }

func (e transactionOutcomeUnknownError) Error() string                 { return e.cause.Error() }
func (e transactionOutcomeUnknownError) Unwrap() error                 { return e.cause }
func (transactionOutcomeUnknownError) TransactionOutcomeUnknown() bool { return true }

// MarkTransactionOutcomeUnknown identifies an error returned after a commit
// may have reached durable storage even though its acknowledgement was lost.
func MarkTransactionOutcomeUnknown(err error) error {
	if err == nil {
		return nil
	}
	var marked interface{ TransactionOutcomeUnknown() bool }
	if errors.As(err, &marked) && marked.TransactionOutcomeUnknown() {
		return err
	}
	return transactionOutcomeUnknownError{cause: err}
}
