# Execution handoff

## Operating context

- Status: implementation-ready.
- Active users or external consumers: no.
- Backward compatibility required: no.
- Feature flags: not applicable.
- Stored-data migration: none; development databases may be recreated.
- Implementation has not occurred.

## Dependency-ordered work packages

### 0. Claim implementation tracking

Result: implementation, release, and response evidence have one Beads owner.

Change surface:

- The implementation Beads feature created by the planning session (existing
  issue at handoff): claim it and record this plan directory.

Prerequisites and parallelization:

- The planning issue `eino-agent-64r` must close only after this plan is
  reviewed, force-staged if ignored, committed, and synchronized.
- No source or release work starts before the implementation issue is claimed.

Verification:

```text
bd show eino-agent-j62
bd update eino-agent-j62 --claim
```

Acceptance: the issue is in progress and points to
`.agents/plans/durable-user-message-admission/`.

### 1. Replace the public input contract

Result: `Start` accepts one proposed `runtime.UserMessage`, loads prior history
itself, and rejects blank or invalid-UTF-8 submissions before downstream work.

Change surface:

- `runtime/types.go` and `runtime/orchestrator.go` (existing).
- `runtime/admission.go` private request/ID plumbing (existing).
- Existing runtime and example request call sites enumerated in
  [01-public-admission-contract.md](01-public-admission-contract.md).
- `tools/einotools/einotools_test.go`, `wasmext/phase_b_test.go`, and
  `wasmext/wasmext_test.go` (existing breaking-API call sites).

Prerequisites and parallelization:

- Depends on package 0.
- Blocks all persistence and lifecycle assertions.
- Mechanical test call-site updates may follow the production type edit in the
  same branch; do not split them into a period where repository compilation is
  intentionally broken.

Verification:

```text
go test ./runtime
go test ./examples/minimal-server ./examples/ag-ui-go-server-example ./examples/native-extension
go test ./tools/einotools ./wasmext
```

Acceptance: all criteria in
[01-public-admission-contract.md](01-public-admission-contract.md) pass.

### 2. Commit the durable admission pair

Result: the user message/part, assistant placeholder, and existing admission
records commit or roll back together under the private run fence; provider
history loads after that fence, and repeated runs reuse the session with
session-monotonic replay positions.

Change surface:

- `runtime/admission.go` and `runtime/admission_test.go` (existing).
- `runtime/admission_sqlite_test.go` (new under existing `runtime/`).
- `store/sqlite/sql_helpers.go` and `store/sqlite/store_test.go` (existing).

Prerequisites and parallelization:

- Depends on package 1's final API shape.
- SQLite timestamp and existing-session work can be developed alongside
  private admission builders, but the combined two-run replay and failed-
  second-admission tests must pass before either is merged.
- No store interface or schema change is planned.

Verification:

```text
go test ./runtime -run 'Admission|Admit|History|SecondRun|Interrupt|Failure'
go test ./store/sqlite -run 'ListMessages|Transaction|Rollback'
go test -race ./runtime ./store/sqlite
```

Acceptance: all criteria in
[02-atomic-persistence-and-ordering.md](02-atomic-persistence-and-ordering.md)
pass.

### 3. Preserve run-wide durable message order

Result: tool results and follow-up assistant placeholders remain strictly after
the assistant that requested the tool, even when the injected clock is frozen
and generated IDs sort in the opposite direction.

Change surface:

- `runtime/orchestrator.go`, `runtime/tool_execution.go`, and
  `runtime/tool_settlement.go` (existing).
- Existing runtime execution state and focused runtime/SQLite tests needed to
  prove close/reopen and one-record pagination order.
- The revised plan files in this directory, which record the human-approved
  review disposition and mandatory follow-up slice.

Prerequisites and parallelization:

- Depends on package 2 and its exact reviewed admission-pair head.
- Must merge before consumer, full verification, or release work.
- Do not change public APIs, store interfaces, schema, tool content, status,
  parentage, or telemetry duration semantics.

Verification:

```text
go test ./runtime -run 'Tool.*History|History.*Tool|Frozen.*Tool|Tool.*Frozen'
go test ./store/sqlite -run 'ListMessages'
go test -race ./runtime ./store/sqlite
git diff --check
```

Acceptance: the run-wide ordering criteria in
[03-verification-and-consumer-updates.md](03-verification-and-consumer-updates.md)
pass after a real SQLite restart.

### 4. Prove lifecycle behavior and update consumers

Result: public tests, examples, architecture docs, and the external fixture all
use runtime-owned admission and prove no duplicate history.

Change surface:

- Existing lifecycle tests listed in the verification matrix.
- `examples/minimal-server/main.go`, its tests, and
  `docs/examples/minimal-server.md` (existing).
- `examples/ag-ui-go-server-example/sketch.go` and
  `docs/integrations/ag-ui-go-server-example.md` (existing), plus
  `examples/ag-ui-go-server-example/sketch_test.go` (new under the existing
  example directory).
- `runtime/doc.go`, `docs/consumer-guide.md`,
  `docs/architecture/runtime.md`, and `docs/architecture/storage.md` (existing).
- `testdata/external-consumer/consumer.go` (existing).

Prerequisites and parallelization:

- Depends on package 3.
- Documentation and adapter work may proceed in parallel after behavior and
  names are fixed.
- Do not update supported release claims yet.

Verification:

```text
go test ./runtime ./session/history ./store/sqlite ./store/storetest
go test ./examples/minimal-server ./examples/ag-ui-go-server-example ./examples/native-extension
go test ./tools/einotools ./wasmext
make external-consumer-check
git diff --check
```

Acceptance: all criteria in
[03-verification-and-consumer-updates.md](03-verification-and-consumer-updates.md)
pass.

### 5. Integrate, publish, verify, and respond

Result: one public module pin implements the contract and the external response
unblocks consumer verification.

Change surface:

- `README.md`, `docs/consumer-guide.md`, and `docs/dependency-status.md`
  (existing).
- `refs/tags/v0.2.0` (proposed annotated tag).
- `$HOME/.agents/projects/eino-agent/responses/2026-08-30-durable-user-message-admission.md`
  (proposed external response).

Prerequisites and parallelization:

- Depends on packages 1 through 4 and the full repository gate.
- Release commit, tag push, public verification, documentation evidence, and
  response writing are serial stop/go steps.
- No feature-flag decision is required.

Verification:

- Run the integration gates below on the intended tag target.
- Push the tag and confirm its peeled remote SHA.
- Run the published consumer command from
  [04-release-and-response.md](04-release-and-response.md).
- Compare response claims to recorded command output before writing it.

Acceptance: all criteria in
[04-release-and-response.md](04-release-and-response.md) pass.

## Integration and regression gates

Run from the repository root after all repository changes are assembled:

```text
make fmt
make check
git diff --check
git status --short
```

`make check` covers formatting, vet, the complete test suite, the race suite,
module-tidy checks, lint, Windows compilation, WIT regeneration, and the local
external-consumer fixture. The final status command must show no generated,
manifest, sum, fixture, or formatting drift. Preserve unrelated user changes.

## Stop/go decisions

1. API gate: stop if any repository consumer can still pass historical
   messages or choose the admitted user-message ID.
2. Atomicity gate: stop if any injected failure exposes only user or assistant
   state or mutates prior session/transcript state.
3. Replay gate: stop if two-run SQLite order depends on clock advancement,
   lexical IDs, reasoning options, or live events.
4. Tool-loop replay gate: stop if tool results or follow-up assistants can sort
   before the assistant that requested the tool under a frozen clock.
5. Second-run gate: stop if the first prompt reaches the provider twice.
6. Consistency gate: stop if a contender can read history before `AdmitRun` and
   later succeed with a stale snapshot.
7. Release gate: reuse `v0.2.0` only when it peels to the intended commit;
   otherwise select the next unused minor version and update all pins.
8. Response gate: do not write `completed` until public resolution and the
   peeled SHA are independently verified.

## Final definition of done

- `runtime.Request` accepts only one current `runtime.UserMessage`.
- Runtime generates the user message/part identities and preserves the run
  claim-token boundary.
- Repeated admissions reuse a matching session and reject immutable identity
  drift without rewriting the existing record.
- Provider history projection and `AdmitRun` share the admission transaction,
  so no admitted run can use a stale pre-fence transcript.
- Admission commits session, run, epoch, user, part, assistant, and start event
  atomically.
- Run and assistant parentage use the generated user message ID.
- Completion, failure, interruption, rollback, second-run, exact replay, and
  SQLite restart tests pass.
- Frozen-clock tool loops replay chronologically after restart and cursor
  pagination without relying on generated ID order.
- Repository examples do not submit transcript history.
- Public docs state ownership, ordering, compatibility, failure, and replay
  behavior.
- `make check` and `git diff --check` pass on the release commit.
- The verified release tag and peeled SHA are on `origin`.
- The fresh published consumer compiles the new API without a replacement.
- The external response exists and names only the verified pin.
- The implementation Beads issue is closed with verification evidence.
- The implementer runs the repository's required synchronization protocol,
  pushes Git and Beads state, and verifies the branch is up to date with its
  remote.

## Deferred work and follow-up

- Multimodal user-message admission needs a separate plan that defines durable
  file/media parts and provider projection.
- Session branching and explicit previous-message parent selection remain out
  of scope.
- No other deferred work is planned. The review-discovered tool-loop ordering
  defect is approved in-scope package 3, not deferred work.
