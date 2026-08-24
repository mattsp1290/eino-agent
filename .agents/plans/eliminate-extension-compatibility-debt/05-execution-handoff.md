# Execution Handoff

## Dependency-ordered work packages

1. **Seal plan construction and descriptor semantics.** Change `session/extensions.go`, `runtime/extension_plan.go`, and `composition/registry.go`; migrate focused plan tests first.
2. **Remove the legacy extension pipeline.** Change orchestrator/admitter/options execution and migrate Wasm/examples/tests to composition-backed plans.
3. **Make settlement uniformly atomic.** Change `session.Store`, runtime fresh/resume tool paths, SQLite, and settlement tests.
4. **Make normalized input authoritative.** Protect `Pattern`, derive it once, and update permission tests.
5. **Install the checked model request boundary.** Change `model.Request.Clone`, canonical request projection, extension view, fake provider, and ledger tests.
6. **Update documentation and run final gates.** Remove stale production guidance, run structural searches, format, and execute `make check`.

Work packages 3 and 5 can proceed independently only after work packages 1 and 2 compile. Work package 4 should land with work package 3 because both alter the tool preparation contract.

## Per-package gates

- After packages 1 and 2: `go test ./extension ./composition ./runtime ./session ./wasmext`.
- After packages 3 and 4: `go test ./runtime ./session ./store/sqlite ./tools` and the focused race gate.
- After package 5: `go test ./model ./providers/fake ./runtime`.
- After package 6: `make fmt && make check && git diff --check`.

## Integration invariants

- Resume validates the current sealed descriptor before any durable mutation.
- Plan leases release exactly once on every exit.
- Identity and behavior are inseparable in plan construction, and descriptor clones cannot mutate sealed state.
- Every tool result is atomic and claim-fenced.
- Permission pattern and executed input share one source.
- Request canonicalization finishes before provider dispatch state becomes uncertain.
- Infrastructure events and permissions remain outside the extension plan.

## Definition of done

- Every success criterion in `00-overview.md` has a direct test or structural check.
- No compatibility path remains in production code or current architecture/consumer documentation.
- The plan status is updated to implemented only after gates pass.
- `eino-agent-26t` is closed with the final verification result.
- Related files are selectively staged and committed.
- `git pull --rebase`, `bd dolt push`, and `git push` succeed.
- Final `git status --short --branch` reports a clean branch up to date with origin.

## Deferred work

No deferred compatibility, migration, or rollout work is expected. File a new Beads issue for any newly discovered requirement that is outside these five accepted review findings.
