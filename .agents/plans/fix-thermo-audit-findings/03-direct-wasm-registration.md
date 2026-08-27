# Direct Wasm Registration

Issue: `eino-agent-7hv`

## Objective

Replace the public load-handle-register ceremony with direct loader-owned registration while keeping module ownership explicit and failure-safe.

## API shape

Add these methods (names may follow established package conventions):

```go
func (l *Loader) RegisterContextSource(
    ctx context.Context,
    registrar extension.Registrar,
    registration extension.Registration,
    config ModuleConfig,
) error

func (l *Loader) RegisterHook(
    ctx context.Context,
    registrar extension.Registrar,
    registration extension.Registration,
    config ModuleConfig,
) error

func (l *Loader) RegisterToolMiddleware(
    ctx context.Context,
    registrar extension.Registrar,
    registration extension.Registration,
    config ModuleConfig,
) error
```

Use the actual registrar/registration names already present in the package if they differ. Do not retain deprecated overloads.

## Implementation changes

### `wasmext/wrappers.go`, `wasmext/points.go`, and `wasmext/loader.go`

- Make concrete loaded context-source, hook, and middleware wrappers private.
- Remove exported opaque `Loaded*` handles, including any exported loaded wrappers not intended as independently useful consumer APIs.
- Move existing load and callback adaptation logic behind the direct `Loader.Register*` methods.
- Open the private wrapper, attach one idempotent cleanup/untrack token with `registrar.Defer`, stage callbacks, then track the module before returning. The registrar has no commit hook, so tracking cannot wait for successful preparation or commit.
- If instantiation or staging fails, close the untracked module and return the error.
- On later installer failure or commit failure, registrar rollback invokes the token, atomically untracks, and finalizes the module so the failed mount retains no resource.
- `Mount.Close`, rollback, and `Loader.Close` may all reach the token/module; make the ownership transition race-safe and idempotent.
- Preserve idempotent close behavior and avoid double-close paths.

### Installer/composition integration

- Replace load-then-register sequences with direct loader calls.
- Use the actual composition call shape in consumers: `loader.RegisterHook(ctx, registrar.Extensions(), spec, cfg)` (and analogous methods). Do not add composition forwarding APIs solely for convenience.
- Keep registrar staging atomic: a failed stage aborts the mount before publication.
- If a later operation in the same installer fails after a direct registration succeeded, the failed mount is not published and rollback releases the module rather than retaining it in `Loader`.
- Document that `Loader` must outlive active mounts created from its modules. `Mount.Close` releases and untracks the module through its deferred token; `Mount.Deactivate` alone does not run cleanup.

### Documentation

Update current consumer-facing material, including `docs/architecture/extension-points.md`, `docs/architecture/extensibility.md`, `docs/consumer-guide.md`, and `examples/wasm-extensions/README.md`. Historical prompts/plans are left untouched.

## Required behavior tests

- Successful direct registration for each supported extension point.
- Instantiation failure closes or leaves no owned module.
- Registrar staging failure closes the new module and publishes no registration.
- A later installer failure publishes no mount and leaves no retained loader ownership.
- A duplicate-instance commit failure rolls back and leaves no retained loader ownership.
- A composition mount test performs successful direct Wasm registration, then fails a later capability registration, and proves neither mount nor module remains.
- Registrar rollback racing with `Loader.Close` closes the module exactly once without deadlock.
- `Loader.Close` closes registered modules once and remains idempotent.
- Consumer examples compile against only the direct API; exported opaque handles are absent.

## Verification

- Search current code/docs for exported `Loaded*`, old load/register choreography, and stale examples.
- Run focused `wasmext`, `composition`, and integration tests before the full quality gate.
