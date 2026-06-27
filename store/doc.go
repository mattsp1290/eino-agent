// Package store documents durable store implementation expectations.
//
// Concrete stores implement the interfaces in package session. The reusable
// contract tests live under store/storetest so each backend can verify the same
// admission, transaction, replay, claim, and recovery semantics.
package store
