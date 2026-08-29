# Extensibility Integrity Remediation

Status: Implemented and verified.

Tracking issue: `eino-agent-igd`

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "80626998584de4bbadc463e9091e284d0cae922bc2992e0c4ddcc6ca0f91cf84",
    "confirmed_at": "2026-08-29T18:46:15Z"
  }
}
```

The user confirmed that this repository has no users and backward compatibility is dead code. Replace weak APIs and stored-schema version 1 directly. Do not add aliases, migrations, feature flags, dual paths, or deprecated shims.

## Change classification and affected areas

This is a greenfield integrity and lifecycle refactor across four boundaries:

- `runtime`, `session`, and `store/sqlite`: make one assistant tool-call turn atomic before execution and terminalize every persisted call before run settlement.
- `extension`, `runtime`, and `composition`: make the host declare extension-point authority before components mount.
- `extension` and `runtime`: move observer-only notifications off the run's synchronous control path while retaining component leases until queued work drains.
- `session` and `store/sqlite`: delete the unused part cursor and page only parts belonging to the selected messages.

`docs/` and `examples/` are excluded from design review. Compile-only edits there are allowed only if a breaking API prevents the repository gate from building.

## Requested outcome

Resolve the four thermo-nuclear review findings without compatibility scaffolding. The implementation must remove the invalid states instead of adding recovery branches around them.

## Success criteria

- An assistant response and every pending tool-call record/event it requests commit in one transaction or not at all.
- A cancellation or tool panic cannot settle the run while another persisted tool call remains pending or running.
- Every store rejects terminal run settlement while any tool call for that run is unfinished.
- A component cannot claim or replace a host point definition by mounting first.
- Equivalent durable contracts with a different point handle fail against the host catalog before publication; dispatch remains tied to the declared typed handle.
- Notification callbacks and best-effort event observers cannot delay tool execution, run settlement, `Handle.Done`, or plan release.
- Queued notification work retains mount leases until it completes; mount close remains context-bounded when a callback never returns.
- `ReplayCursor.AfterPartID` is deleted and SQLite decodes only parts for the current message page.
- Focused tests cover partial tool-batch failure, terminalization, point-catalog conflicts, blocked notification callbacks, notification ordering/drop behavior, and multi-page replay.
- Non-doc/example package tests, race tests, vet, lint, formatting, module-tidy, and diff checks pass.

## Repository-grounded findings

- `runtime.persistAssistant` appends every `PartToolCall` before `runtime.executePreparedTools` creates the corresponding `session.ToolCall` records one at a time.
- `runtime.executePreparedTools` returns immediately on a panic or cancellation, while `runtime.executeLifecycle` still settles the run terminal. `runtime.Resume` short-circuits terminal runs.
- `session.ToolSettlement` already owns the result message and part atomically. Tool creation does not own the matching assistant request part.
- `extension.Registry.CommitMount` installs the first observed point-definition pointer in `pointDefinitions`. `extension.matchingEntries` dispatches only exact pointer identity.
- `extension.Notify`, `runtime.runEventSink`, and `runtime.emitBestEffort` execute observers synchronously. Several persisted-event paths remove cancellation before dispatch.
- `extension.Plan` already owns mount-release functions, so it is the canonical owner for a bounded notification dispatcher and deferred lease release.
- `session.ReplayCursor.AfterPartID` has no reader or writer. `store/sqlite.ListMessages` selects every part for the session and filters after JSON decoding.

## Key decisions

1. Persist the complete assistant turn through one outer `ExecutionStore.WithinTx` call. Extend tool creation so the store validates and writes the canonical request part with the call and event.
2. Separate batch persistence from execution. Execute only canonical records returned by the committed transaction.
3. After a fatal tool outcome, use a detached lifecycle context to transition every remaining committed call to interrupted before run settlement. Do not invoke tool behavior or transforms for skipped calls. Enforce zero unfinished calls as a transactional store precondition for terminal run settlement so a cleanup write failure leaves the run reclaimable instead of corrupt.
4. Construct registries from a host-owned point catalog. Registry publication validates registrations against that immutable catalog and never elects authority from mounted components.
5. Give each snapshot plan a bounded, ordered notification dispatcher. `Notify` clones and enqueues without waiting. Accepted tasks take a lease only on their target mount; `Plan.Release` stops admission and releases the plan's baseline leases immediately.
6. Keep gates, hooks, transforms, and around callbacks synchronous because they change behavior. Only `Notification` uses the dispatcher.
7. Give each run one bounded, nonblocking infrastructure event dispatcher, created before admission publication and closed without waiting for native callbacks.
8. Delete the unused replay field and use a SQLite page CTE for parts. Do not preserve the dead cursor shape.

Rejected alternatives:

- Creating all calls before execution without terminalizing skipped calls leaves unfinished records under a terminal run.
- Executing every tool concurrently changes permission and callback ordering without being required to remove the invalid state.
- Letting the first mount declare a point keeps behavior dependent on mount order.
- Adding callback timeouts still lets native code ignore context and block the run goroutine.
- Spawning unowned goroutines per notification loses ordering and can release a mount while its callback is running.
- Retaining `AfterPartID` as an ignored compatibility field contradicts the confirmed greenfield constraint.

## Target flow

```text
model response
  -> prepare all tool calls and reserve request/result identities
  -> one transaction: assistant parts + pending calls + pending events
  -> publish committed pending events
  -> execute canonical calls in deterministic order
       -> fatal outcome: interrupt every remaining canonical call
  -> assert no unfinished calls
  -> settle run

host point catalog -> immutable registry authority -> component validation
snapshot -> plan-owned bounded notification queue -> ordered callbacks
                          \-> per-task mount lease -> release on callback return

admitted run -> run-owned bounded infrastructure queue -> best-effort sink

message page CTE -> decode page messages -> decode only page parts
```

## Scope and constraints

In scope:

- Direct public Go API changes and schema-version-1 edits required by the four findings.
- Production and focused test changes outside `docs/` and `examples/`.
- Mechanical compilation fixes outside review scope when required by deleted APIs.
- Existing deterministic tool execution order and durable event ordering.

Out of scope:

- Parallel tool execution.
- Reliable external event delivery, retries, or unbounded buffering.
- New extension kinds or new storage backends.
- Documentation or example quality work.
- Migration support for existing SQLite files.

## Risks and gates

- The tool transition transaction must not publish events until the outer transaction commits.
- Cleanup after cancellation must not execute component-provided tool code or result transforms.
- The notification dispatcher must not send on a closed queue and must release each task-specific mount reference exactly once.
- Queue overflow is best-effort loss. Tests must prove it never blocks the caller and preserves FIFO order for accepted work.
- A blocked native observer may retain its own mount lease indefinitely, but it must not retain unrelated mounts, the run goroutine, or `Handle.Done`.
- No unresolved blocking decisions remain.

## Review disposition

Two independent reviewers found the same settlement invariant and queue-ownership gaps. All findings were accepted:

- enforce unfinished-call rejection in the store and exercise the same cleanup path on fresh and resumed runs;
- reject lifecycle-owned tool parts through generic `AppendPart`;
- replace the ambiguous infrastructure-delivery choice with one run-owned dispatcher;
- use per-task mount leases rather than retaining every selected mount behind one blocked callback;
- prove page-bounded SQLite decoding with malformed JSON outside the selected page;
- use one consistent implementation order.

## Implemented outcome

- Tool request parts, pending calls, and pending events now commit through one transition-owned envelope inside the assistant-turn transaction; result parts remain settlement-owned.
- SQLite refuses terminal run settlement while any call is pending or running, and fresh/resumed fatal paths share lifecycle-only terminalization.
- Registry construction freezes a host catalog; runtime exports its canonical points and composition explicitly accepts custom host points.
- Plan notifications use a bounded FIFO worker with per-task mount references, while each run owns one bounded nonblocking infrastructure dispatcher.
- Replay cursors no longer contain part state, cross-session cursors fail, and SQLite decodes parts only for the truncated message page.
- `make check`, including vet, the full test suite, the full race suite, module-tidy checks, lint, and generated-WIT verification, passes.

## Document map

- [01-atomic-tool-turns.md](01-atomic-tool-turns.md): make assistant tool-call persistence and terminalization one coherent lifecycle.
- [02-point-authority-and-notifications.md](02-point-authority-and-notifications.md): establish host-declared point authority and plan-owned observer dispatch.
- [03-replay-pagination.md](03-replay-pagination.md): delete the dead cursor field and bound SQLite part loading to the message page.
- [04-execution-handoff.md](04-execution-handoff.md): dependency order, gates, issue completion, commit, and push.
