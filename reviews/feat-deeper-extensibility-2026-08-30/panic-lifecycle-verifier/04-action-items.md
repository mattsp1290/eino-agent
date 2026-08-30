# Action Items

## Critical

- [x] No Critical findings.

## Important

- [x] No Important findings.

## Suggestions

- [ ] Add a race-focused Wasm lifecycle test at `wasmext/wasmext_test.go:220` that queues a second call and begins close while the first invocation is about to panic, then verifies bounded termination and exactly-once finalization.
