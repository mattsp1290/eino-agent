// Package runtime defines orchestration contracts for admitting and executing
// Eino agent runs over durable sessions.
//
// A fresh Start request contains exactly one current UserMessage. Runtime owns
// transcript history: admission loads prior committed messages, appends the
// current user text to the provider snapshot, and atomically persists that user
// message and part before provider execution. Callers must not resend history
// or write a second copy of admitted or settled messages.
//
// Once admission commits, failed and interrupted executions retain the user
// message and their assistant placeholder for durable replay. A synchronous
// admission failure commits none of the attempted run's transcript records.
//
// A nonempty Request.AdmissionKey commits an immutable receipt with that graph.
// Start returns AdmissionNew with a live Handle only to the successful owner;
// a matching later Start returns AdmissionExisting with the original receipt
// and no handle. LookupAdmission reads a committed receipt without constructing
// a provider or a run plan. A changed keyed payload returns the content-free
// session.ErrAdmissionConflict classification. Receipts have no expiry and do
// not grant execution authority; hosts observe state through committed readers
// and explicitly Resume stranded work when appropriate.
package runtime
