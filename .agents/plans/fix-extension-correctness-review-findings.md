# Fix Extension Correctness Review Findings

## Change information

- Change type: OTHER (correctness bug fixes)
- Description: Resolve all six supplied review findings covering durable tool settlement fencing and idempotency, required interceptor delegation, strict-plan resume capability checks, prompt/guard fingerprint ordering, and early WASM hook metadata caching.
- Relevant documentation: The supplied code review; `docs/architecture/tools.md`; `docs/architecture/extension-points.md`.
- Affected areas: `session`, `store/sqlite`, `extension`, `runtime`, `composition`, `wasmext`, and their focused tests.
- Success criteria: Each reported failure mode has a regression test; affected package tests and the full Go test suite pass; public behavior remains compatible except for the intentional rejection of stale settlements and swallowed required-delegation failures.
- Constraints: Preserve atomicity and retry idempotency, do not weaken strict-plan fingerprinting, apply settlement-store requirements only to strict descriptors containing required tools, preserve bounded/content-free WASM metadata, and avoid unrelated worktree changes.

## Implementation plan

1. Strengthen `session.ToolSettlement` with the expected claim owner and token. Require nonempty identity and an exact match before either first application or already-terminal idempotency matching, preserve it in SQLite reconciliation records, and populate it at every trusted runtime settlement call site. Keep fencing credentials out of `runtime.ToolCall`; instead make `tools.BuildToolSettlement` accept explicit claim identity, while source-compatible calls that omit it return a clear error rather than silently producing a settlement that conforming stores reject. Add unit, builder, and SQLite coverage for missing, mismatched, stale, and terminal-retry claim identities.
2. Normalize an omitted `CompletedAt` against an already-terminal call before equality checks, while continuing to generate a UTC completion time on the initial apply. Add unit and SQLite coverage for replaying the same zero-time settlement.
3. Track whether required interceptor delegation returned successfully. If an interceptor swallows a delegated error and returns success, propagate the delegated failure instead of accepting fabricated output. Cover both the generic dispatcher contract and the required model-stream point.
4. Centralize the settlement-store predicate as `strict mode && descriptor contains required tools`, and use it consistently for fresh-plan acquisition, resume-plan acquisition, and `resumeRun` reconciliation. Test partial-legacy tool plans and strict callback-only plans as well as strict tool rejection.
5. Add a capability-order field to durable plan entries, define schema v2 as recording prompt/guard order (including explicit zero), and emit v2 descriptors from composition. Preserve schema-v1 fingerprint compatibility by omitting/ignoring the new field for v1 callback/tool-only descriptors, but reject fresh or resumed v1 descriptors containing prompt/guard capabilities because their admitted order is unverifiable. Test v1 callback/tool fingerprint compatibility, v1 prompt/guard rejection, v2 zero-order descriptor semantics, and fingerprint divergence for changed prompt and guard order.
6. Cache bounded admission metadata when a registered WASM hook receives `RunAdmittedPoint`; allow the resolved before-turn projection to replace it. Deliver the contained admission notification only after abortible legacy `BeforeRun` hooks succeed (or otherwise add equivalent explicit cleanup) so a partial-legacy admission failure cannot leak the cache entry. Test settlement before turn preparation, after an early assembly failure, and a mixed partial-legacy `BeforeRun` failure so `AfterTurn`/`AfterRun` receive the best available projection without retaining abandoned entries.
7. Run formatting, focused package tests, `go test ./...`, and any repository lint/build gates discovered locally. Review the final diff for scope, close the Beads issue, commit, rebase, push Beads data and Git changes, then verify the branch is clean and up to date.

## Risk checks

- Adding claim identity must not copy credentials from the current database record into an untrusted settlement or disclose claim tokens through tool/guard/middleware/interceptor inputs; trusted orchestration attaches the exact identity of the claim that fenced the work.
- Reconciliation settlements generated from terminal records must retain their original claim identity.
- Already-terminal idempotent retries must validate claim identity before comparing terminal output.
- Zero-time normalization must not make settlements with different status, output, error, metadata, or claim identity compare equal.
- Required interceptors that return the delegated error unchanged must preserve the original error chain.
- Descriptor sorting may remain canonical, but schema v2 must fingerprint explicit prompt/guard order. Schema v1 remains compatible only where no order-sensitive capability is present.
- Admission metadata must remain bounded and content-free, and cache cleanup must still occur in `AfterRun`.
- Admission-time caching must not retain entries for starts that abort before asynchronous execution and `RunSettledPoint` cleanup can occur.
