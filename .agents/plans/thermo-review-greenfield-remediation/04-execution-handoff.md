# Execution Handoff

Status: Implemented and verified.

## Dependency-ordered work packages

1. Model stream contract
   - Change `model.Streamer` to return `StreamDelta` readers.
   - Remove observer and dual idempotency APIs.
   - Convert the Eino and fake adapters plus repository test streamers.
   - Gate: `go test ./model ./providers/fake` and repository compilation.
2. Runtime lifecycle and ledger
   - Consume normalized deltas, merge usage before later failure points, and keep one terminal defer.
   - Convert the model-stream required-around point and validators to the delta reader without weakening delegated-reader identity.
   - Add SQLite cancellation and partial-failure/retry tests.
   - Gate: focused runtime stream, ledger, usage, and observability tests.
3. Run settlement contract and SQLite enforcement
   - Add terminal settlement, required event, result types, and canonical run/event construction from current fenced state.
   - Reserve one `run_finished` phase per run in schema version 1.
   - Expand reusable store contract and SQLite atomicity, replay, and concurrency tests.
   - Gate: `go test ./store/storetest ./store/sqlite`.
4. Execution capability construction
   - Construct `runExecution` from the admitted or claimed run.
   - Delete rebinding and recovery paths.
   - Adapt fresh, resume, tool, event, and extension lifecycle fixtures.
   - Gate: focused runtime fresh/resume/tool/extension tests and race tests.
5. Documentation and integration verification
   - Correct EventSink, run-plan-provider, idempotency, and settlement guidance.
   - Run all repository quality gates and stale-symbol searches.

## Ordering and parallelization constraints

- Packages 1 and 3 are independent architectural boundaries, but each must be internally complete before meaningful downstream compilation.
- Package 2 depends on package 1.
- Package 4 depends on package 3 and touches runtime files also changed by package 2; implement it after package 2 to reduce merge ambiguity.
- Package 5 depends on final symbol names and behavior from all earlier packages.
- The implementer owns all edits. Review agents must not modify the shared worktree.

## Integration gates

- Verify every `model.Streamer` implementation uses the single method and delta reader.
- Verify every `ExecutionStore` implementation and double uses required run settlement.
- Verify runtime never publishes a `run_finished` event that was not returned by durable settlement.
- Verify plan release counts and admission notification order remain unchanged.
- Verify partial usage is merged once per provider attempt and accumulated once per run.
- Verify `git diff --check` and `make check` pass from the repository root.

## Definition of done

- All six claimed Beads issues meet their acceptance criteria.
- Two independent plan reviews and one adversarial plan review are complete, with accepted corrections applied.
- No unresolved blocking decision remains.
- Focused tests and `make check` pass.
- No compatibility shim, migration, deprecated method, or feature flag was added.
- Changed files and plan documents are committed.
- The six Beads issues are closed.
- Beads state and the Git branch are pushed.
- `git status` reports a clean branch up to date with origin.

## Deferred work

No deferred work is planned. Create a Beads issue for any newly discovered out-of-scope defect before session completion.
