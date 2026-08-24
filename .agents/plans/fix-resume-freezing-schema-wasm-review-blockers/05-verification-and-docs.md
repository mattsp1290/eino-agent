# Verification and Documentation

## Goal

Verify the four changes as one coherent runtime and document the authority, schema, and ABI boundaries that future work must preserve.

## Documentation surface

- `docs/architecture/runtime.md`: describe claim-token authority, store-authoritative lease time, atomic resume, heartbeat cancellation, and stale-writer rejection.
- `docs/architecture/storage.md`: specify conditional `ClaimRun`, token-fenced renewal/finish, and the relationship between run and tool-call leases.
- `docs/architecture/extensibility.md`: state that schemas freeze through checked clones and component calls cross typed world interfaces.
- `docs/extension-points.md`: update any Wasm wrapper/API examples affected by error-returning definitions without adding compatibility notes.
- `store/sqlite/doc.go`: document the one current bootstrap schema and non-mutating rejection of initialized non-current databases.
- `docs/consumer-guide.md`: replace forward-migration ordering advice with the current no-compatibility/recreate workflow.
- `README.md`: update only if its public quick-start or guarantee statements become inaccurate.

## Cross-cutting verification

Run focused packages after each work package, then run:

```text
gofmt on changed Go files
go test ./...
CGO_ENABLED=0 go test ./wasmext
go vet ./...
make check
git diff --check
```

Also run race-focused resume tests with `go test -race` when the environment supports cgo/race builds. Repeat the concurrent-claim and short-lease heartbeat tests to expose timing flakes.

## Review-specific audit

- Search for owner-string authorization, unchecked schema clone fallbacks, old schema/version tokens, and generic Wasm dispatch patterns listed in the preceding work files.
- Re-run the thermo-nuclear review rubric against the final diff, with special attention to newly enlarged runtime/store files and replacement switchboards.
- Confirm all new failure paths preserve plan release and terminal semantics while refusing event, model-ledger, history, context, and tool writes from a stale run fence.
- Confirm removed SQL files and APIs have no live documentation links, embeds, or build references.

## Acceptance

- Every command exits zero, except an unavailable optional race environment which must be reported with the concrete reason.
- Tests observe token fencing and clone failures rather than merely checking new fields or method existence.
- Documentation calls claim tokens authoritative and owner IDs diagnostic.
- No compatibility, rollout, or feature-flag path is introduced.
- `git status --short` contains only files belonging to this plan and Beads metadata expected by repository workflow.

## Verification result

Completed at `2026-08-24T03:43:46Z`:

- `make check` passed, including `go vet ./...`, `go test ./...`, `go test -race ./...`, module-tidy checks, `golangci-lint`, and generated-WIT diff verification.
- `CGO_ENABLED=0 go test ./wasmext` passed.
- `git diff --check` passed.
- Obsolete SQLite/version tokens and unchecked production clone/generic Wasm dispatch patterns were absent in targeted searches.
- The final thermo-nuclear audit found no remaining blocking maintainability issue; the largest changed production files remain below 1,000 lines, with SQLite schema validation and execution fencing separated into focused modules.
