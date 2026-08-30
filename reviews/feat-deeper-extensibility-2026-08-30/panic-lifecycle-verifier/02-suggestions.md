# Suggestions

## Add a panic-versus-close contention regression

- Reference: `wasmext/wasmext_test.go:220`
- Rationale: The new regression proves panic classification, sequential gate reuse, and successful close. A complementary contention test could pin the harder schedule where a second caller is already queued on `callGate` while the first invocation panics and `Close` begins. The current locking and reference accounting are correct under inspection and race testing, so this is additional hardening rather than a blocker.

```go
firstEntered := make(chan struct{})
releasePanic := make(chan struct{})
// First fake invocation closes firstEntered, waits for releasePanic, then panics.
// Start a second Decide call after firstEntered, race Close, then releasePanic.
// Assert both calls terminate with ErrorTrap or ErrorClosed, Close completes,
// component.Close runs once, and no call succeeds after shutdown starts.
```

Run this test under `-race -count=100` to preserve the shutdown/reference invariant against future changes to gate ownership.
