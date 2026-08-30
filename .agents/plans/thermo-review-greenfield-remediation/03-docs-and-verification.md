# Documentation and Verification

## Goal and prerequisites

Make public guidance describe the implemented greenfield contracts and verify that interface changes did not leave hidden compatibility branches.

Prerequisites: complete model/runtime and session/store changes before final documentation wording and repository-wide gates.

## Work package E: consumer and architecture documentation

Change surface:

- `docs/integrations/ag-ui-go-server-example.md`
  - Tell consumers to pass `stream.Tail` directly as the runtime `EventSink` when live fanout is desired.
  - State that runtime persists replayable events through `session.ExecutionStore` before forwarding copies.
  - Remove the composite durable-plus-live sink instruction and the claim that the minimal server demonstrates one.
- `docs/consumer-guide.md`
  - List Store, ModelResolver, RunPlanProvider, and IDGenerator as required constructor dependencies.
  - Keep EventSink, permissions policy, owner ID override, queue sizing, and lease tuning optional.
  - Replace `FinishRun` wording with required atomic `SettleRun` state/event semantics.
- `docs/architecture/extension-points.md`
  - Remove `model.IdempotentStreamer`.
  - State that adapters read `Request.IdempotencyKey` if their provider transport accepts such a key.
- `docs/architecture/storage.md`
  - State that `run_finished` is one reserved canonical event per run and is committed only by `SettleRun`.
- Update model/provider comments and affected examples so public prose uses the same terms.

Acceptance criteria:

- `rg 'IdempotentStreamer|StreamObserver|durable-plus-live|FinishRun'` finds no stale public contract references, except deliberate historical plan artifacts outside product documentation.
- `examples/minimal-server` wiring and integration prose agree that `EventSink` is observation/live delivery only.
- Constructor documentation matches `runtime.NewStreamingOrchestrator` validation.

## Work package F: verification and cleanup

Focused gates during implementation:

- `go test ./model ./providers/fake`
- `go test ./runtime -run 'Stream|Ledger|Usage|RunExecution|Resume|ExtensionPlan'`
- `go test ./store/storetest ./store/sqlite`
- `go test ./examples/... ./tools/... ./wasmext/...`

Repository gates after all edits:

- `gofmt` on changed Go files.
- `git diff --check`.
- `make check`.
- `go test -race ./runtime ./store/sqlite` if `make check` does not already cover those packages under race detection.
- Search for removed interfaces and mutable fallback symbols.

Observable final checks:

- One `run_finished` event exists after fresh completion, resumed completion, interrupted completion, and identical settlement replay.
- Concurrent identical run settlement returns one canonical result to both callers; concurrent conflicting settlement elects one winner and stores one event.
- Settlement after lease renewal retains the store's renewed lease in the canonical terminal run and replays without conflict.
- Failed-attempt provider usage survives a successful retry and appears in the final result and durable event.
- Cancellation after dispatch leaves no nonterminal ledger record.
- The worktree contains no compatibility adapter or unused deprecated symbol.

## Risks and exclusions

- Compile errors are expected while changing public interfaces. Finish one package boundary before interpreting downstream errors as defects.
- Do not weaken exact SQLite schema verification to make tests pass.
- Do not rewrite unrelated documentation or examples.
- If a required follow-up is discovered but cannot be completed in this session, create a Beads issue before handoff.
