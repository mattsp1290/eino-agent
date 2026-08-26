# Settlement Ownership, Documentation, and Verification

## Goal and prerequisites

Delete the tool-output compatibility facade, retain coverage at the runtime owner, and make all public documentation describe the final architecture.

Prerequisites: complete the storage/ledger and tool-composition work so documentation and integration tests target stable final APIs.

## Settlement ownership

Delete:

- `tools/output.go`.
- `tools/output_test.go` after transferring any unique canonical-encoding assertions to an existing runtime test or `runtime/tool_settlement_test.go` (proposed new file under existing `runtime/`).

Move the intent of `tools/output_sqlite_test.go` into `runtime/tool_settlement_sqlite_test.go` (proposed new file under existing `runtime/`):

- Call `runtime.BuildToolSettlement` directly.
- Set up the run and tool claim through `session.ExecutionStore`.
- Settle through the same fenced execution store.
- Assert the terminal tool call and reserved result message/part are committed atomically.

Retain runtime coverage for:

- completed, expected-failure, operational-failure, and interrupted output classes;
- truncation at UTF-8 boundaries;
- redaction and external-output flags;
- exclusion of tool-controlled metadata and attachment locations;
- required claim identity and reserved result IDs;
- invalid disposition and explicit completion-time errors.

Acceptance criteria:

- `rg` finds no `tools.ModelOutput`, `tools.EncodeModelOutput`, or `tools.BuildToolSettlement` references.
- Runtime remains the only package defining canonical output encoding and settlement construction.
- The SQLite integration test proves the runtime-produced envelope is accepted by fenced atomic settlement.

## Documentation changes

Update these existing files:

- `README.md`: describe `tools` as typed definitions and materialization, not a registry.
- `docs/consumer-guide.md`: state that model-request ledger persistence is opt-in, model-request reads are on `session.Store`, writes are execution-scoped, and session tools mount through composition. Replace the direct `s.store.AppendEvent` event-sink example: runtime persists eligible durable events internally through its fenced execution sink, while external sinks receive transport/observability copies and cannot mutate session state.
- `docs/architecture/tools.md`: remove mutable registry generations/snapshots and thin output adapters; document direct definition materialization, composition-owned selection, fenced settlement, and the session tool mount.
- `docs/architecture/extension-points.md`: retain `WithModelRequestLedger`; describe execution-scoped writer transitions and the disabled non-persistence path.
- `docs/architecture/runtime.md`: replace legacy typed-registry wording where it means `tools.Registry`.
- `docs/architecture/storage.md`: state that run-owned and model-request writes require `ExecutionStore` and that model-request reads are top-level.
- `docs/architecture/security.md`: assign canonical output encoding solely to runtime and update evidence links from deleted `tools.TestEncodeModelOutput*` tests to their final runtime test names.
- `docs/prompts/eino-agent-functional-options-wasm-extensibility.md`: replace or remove instructions that construct `tools.NewRegistry`, call its `Register`, or resolve it directly; use composition mounts and run plans.

Correct the stale `session.Store.SettleToolCall` reference to `session.ExecutionStore.SettleToolCall`.

Documentation acceptance criteria:

- `rg` finds no references to deleted registry/output symbols. Optional ledger enablement remains documented as an explicit privacy and retention choice.
- Examples name `composition.Registry` as the only registry for tool publication.
- Storage documentation distinguishes admission/session writes from run-fenced writes.
- Documentation contains no direct concrete-store calls to privatized run-owned writers; intentionally fenced examples call through a `session.ExecutionStore` value.

## Final verification

Run focused tests while editing, then run all repository gates:

```bash
make fmt
make check
git diff --check
```

If `make check` regenerates WIT bindings, accept no diff unless the implementation intentionally changed WIT contracts, which is out of scope.

Also inspect:

```bash
rg -n 'ModelRequestStore|tools\.Registry|tools\.NewRegistry|tools\.Snapshot|tools\.Registration|NewSnapshot|ErrStaleRegistration|sessiontools\.Register|tools\.ModelOutput|tools\.EncodeModelOutput|tools\.BuildToolSettlement|tools\.TestEncodeModelOutput' .
rg -n '\.(AppendMessage|AppendPart|UpdatePart|AppendEvent|CreateToolCall|ClaimToolCall|SettleToolCall|StartContextEpoch|FinishContextEpoch|CreateModelRequest|UpdateModelRequest)\(' README.md docs examples
git status --short
```

The deleted-symbol search must return no live code or documentation references except historical plan artifacts. The writer search may return only examples whose receiver is visibly a `session.ExecutionStore`. Also inspect generic `registry.Register` and `ResolveTools` hits to distinguish canonical composition/run-plan use from the deleted tool registry. Exclude `.agents/plans/` when evaluating those results.

## Completion protocol

1. Review the diff for accidental compatibility wrappers or unrelated changes.
2. Close `eino-agent-0go` after all acceptance criteria pass.
3. Commit the implementation and plan together with a focused message.
4. Run `git pull --rebase`.
5. Run `bd dolt push`.
6. Run `git push`.
7. Verify the branch is clean and up to date with origin.
