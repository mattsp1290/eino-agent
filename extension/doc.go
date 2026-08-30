// Package extension provides typed, deterministic runtime extension points,
// atomic mount ownership, immutable scoped plans, and quiescent teardown.
//
// It deliberately contains no agent-runtime domain types. Packages declaring
// points own cloning and validation for their payloads.
package extension
