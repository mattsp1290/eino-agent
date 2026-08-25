# Execution Handoff

## Dependency-ordered work packages

1. Implement the typed durable plan identities and migrate descriptor fixtures.
2. Collapse model resolution to one built streamer.
3. Enforce object input, add explicit/persisted permission patterns, and remove
   `FollowUp`.
4. Consolidate Wasm construction and closure under `Loader`.
5. Update architecture/consumer documentation and run structural checks.

Work packages 2 and 4 can proceed independently after package-level baselines.
Work package 3 follows identity fixture migration to reduce overlapping session
test edits.

## Review protocol

Before implementation, two independent subagents review the full plan directory
for correctness, omissions, testability, dependency ordering, and unjustified
compatibility. The implementation-plan skill also requires a fresh adversarial
review. Reviewers report findings only and do not edit files. The primary agent
records accepted corrections in the plan and changes status to reviewed before
code changes.

## Verification

Focused gates:

```text
go test ./model ./providers/fake ./runtime
go test ./session ./composition ./runtime
go test ./tools ./tools/session ./runtime ./store/sqlite
go test ./wasmext
go test -race ./model ./runtime ./session ./composition ./tools ./store/sqlite ./wasmext
```

Repository gates:

```text
make fmt
make check
git diff --check
```

Structural checks:

```text
rg -n 'Resolved\.Client|streamerFor|einomodel.WithTools|FollowUp|ErrUnsupportedOperation|toolPattern|permission_pattern|json:"pattern"|\.Required|CapabilityID|func Open(Tool|PermissionsPolicy|ContextSource|EventSink|Hook|ToolMiddleware)|func \([^)]*Loaded[^)]*\) Close' . --glob '*.go' --glob '*.md' --glob '!docs/prompts/**' --glob '!.agents/**' --glob '!wasmext/gen/**'
wc -l model/*.go runtime/*.go session/*.go composition/*.go tools/*.go wasmext/*.go
```

Expected matches from schema libraries or result enums must be inspected; no
production match for a removed path is accepted.

## Definition of done

- All accepted review corrections are present in this plan.
- Every success criterion in `00-overview.md` has a direct test or structural
  check.
- Current architecture and consumer docs describe one model transport, typed
  plan identity, explicit permission resolver, and loader-owned Wasm lifetime.
- SQLite JSON round-trip tests prove permission pattern survives create, claim,
  and settlement and participates in duplicate-create conflict detection.
- Rollback/recovery documentation states that incompatible pre-release local
  databases are recreated; no dual reader, migration, or feature flag exists.
- No compatibility issue or migration is filed because the user explicitly
  rejected backward compatibility for this undeployed code.
- `eino-agent-05m` is closed after all gates pass.
- Related files are selectively staged and committed.
- `git pull --rebase`, `bd dolt push`, and `git push` succeed.
- Final status is clean and up to date with origin.

Implementation completed on 2026-08-25. `make check` and `git diff --check`
passed with the accepted review corrections in place.

## Review record

Two independent reviews completed before implementation. All substantive
findings were accepted; overlapping findings were consolidated. See
[06-review-disposition.md](06-review-disposition.md). The required adversarial
review follows these corrections.
