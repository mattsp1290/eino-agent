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
package runtime
