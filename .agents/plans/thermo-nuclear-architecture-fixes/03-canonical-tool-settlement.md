# Canonical Tool Execution And Settlement

## Goal

Use one runtime-owned implementation for post-claim execution, transformation, output encoding, durable settlement, notification, and observation.

## Repository evidence

- `runtime.executePreparedTools` and `runtime.resumeRunWithSettlement` duplicate the terminal tool lifecycle.
- `runtime.encodeToolOutput` and `tools.EncodeModelOutput` define different model-facing payloads.
- `tools.BuildToolSettlement` has no production caller, uses wall-clock time internally, and cannot be imported into runtime without a package cycle.
- Strict plans require `session.ToolSettlementStore`; partial-legacy plans preserve the existing non-atomic compatibility path.

## Exact change surface

- `runtime/tool_settlement.go` (new): canonical `ToolOutput` data model, proposed exported `ToolSettlementInput`, lower-level terminal-envelope builder, executed-outcome adapter, running-call interruption adapter, and proposed unexported `settledTool` result.
- `runtime/tool_execution.go` (new): proposed `runExecution.executeAndSettleClaimedTool` and the shared non-atomic/atomic commit boundary.
- `runtime/orchestrator.go`: retain fresh-only create/claim and model-message accumulation; delegate post-claim work.
- `runtime/interrupt.go`: retain reconciliation and pending/running classification; delegate pending post-claim work.
- `runtime/types.go`: add only the minimum exported data type or builder contract needed by `tools`; prefer unexported execution types.
- `tools/output.go`: make `ModelOutput` an alias or lossless wrapper around the runtime canonical output, and delegate `EncodeModelOutput` and `BuildToolSettlement` to runtime.
- Runtime, tools, and SQLite integration tests: prove identical envelopes and state transitions.

## Intended design

The proposed exported `ToolSettlementInput` accepts explicit inputs rather than consulting global time or store state:

- authoritative `Tool`, `ToolCall`, and durable claimed `session.ToolCall`;
- protected `ToolOutcome` disposition, result, and error;
- claim owner/token;
- reserved or newly allocated result message/part IDs;
- model ID;
- one caller-supplied UTC completion time.

It returns one immutable settlement envelope containing the terminal call state, model-visible output, result message, and result part. Preserve runtime's current restrictive payload fields: `tool_call_id`, `status`, `content`, `structured`, `truncated`, `original_size`, `inline_size`, `external`, and `redacted`. Do not newly expose tool-controlled attachment or metadata values. Canonical `ToolSettlement.Metadata` contains host-owned call metadata plus `output_status`, `output_truncated`, `output_external`, `output_redacted`, `output_original_size`, and `output_inline_size`; tools wrappers must produce the same keys.

A lower-level envelope builder accepts an already classified terminal status, encoded payload, diagnostic error, claim identity, reserved IDs, and completion time. The executed-outcome adapter and the resumed-running-call interruption adapter both delegate to it. The interruption adapter preserves an existing durable output when present and synthesizes the canonical interrupted payload only when absent.

`executeAndSettleClaimedTool` performs:

1. materialization observation;
2. guards, permissions, and protected execution unless a fresh prepare error already exists;
3. legacy after middleware when execution occurred;
4. protected result transformation;
5. canonical encoding and envelope construction at one timestamp;
6. create one `settlementCtx := context.WithoutCancel(ctx)` and use it for `ToolSettlementStore.SettleToolCall` or every ordered partial-legacy terminal write;
7. settled extension notification and observability finalization.

Fresh execution remains responsible for durable creation, reserved IDs, pending/running transport events, appending the returned model tool message, and publishing exactly one fresh-only terminal `EventToolCallUpdated` after successful durable settlement. The shared operation returns canonical status and payload for that publication. Pending resume preserves its current lack of terminal transport publication. Resume remains responsible for unreconciled settlement replay, running-call interruption through the canonical envelope adapter, pending claim conflict mapping, and final run settlement.

Recover panics from host executors and legacy tool middleware inside `executeAndSettleClaimedTool`. Convert them to bounded operational-failure outcomes, encode and settle them with `settlementCtx`, then finalize notifications and observations. The outer run panic guard remains a last-resort boundary rather than the durable tool-call mechanism.

## Invariants and error paths

- Claim owner/token and reserved IDs are validated before strict settlement.
- Durable completion uses one timestamp across call, message, and part.
- Once an outcome exists, every terminal strict and partial-legacy write uses the same `context.WithoutCancel`-derived context.
- Permission disposition and protected metadata cannot be changed by result transforms.
- Operational errors are persisted for diagnostics but not copied into model-visible content.
- Redaction and truncation behavior remains fail-closed.
- A settlement failure emits failure observation but never emits `ToolSettledPoint` as authoritative success.
- Fresh and resume paths return the same status/output for an equivalent claimed call.

## Tests and acceptance criteria

- Table-test completed, denied, approval-required, interrupted, executor-failed, prepare-failed, middleware-failed, and transform-failed outcomes through the canonical builder.
- Run equivalent fresh and pending-resume executions and compare terminal call output, result message, result part, status, error, and metadata, allowing only expected run-level differences.
- Test cancellation after executor return and prove strict settlement commits using the uncancelled context.
- Add the same cancellation-aware partial-legacy test and prove call, message, and part persist before `ToolSettledPoint`.
- Test strict claim fencing and reserved-ID failures.
- Test partial-legacy ordered writes preserve current behavior.
- Test fresh panic and pending-resume panic for executors and legacy middleware; the call/result records become terminal, the run fails, and the plan releases once.
- Keep running-call resume non-reexecution tests for both preexisting and absent output; assert claim/ID fencing, one completion timestamp, and canonical envelope use.
- Preserve fresh pending/running/terminal transport events exactly once and pending resume's current event behavior.
- Compare the public tools wrapper with runtime's builder for raw output, status, diagnostic error, metadata, model ID, all timestamps, claim identity, message, and part.
- `rg -n 'session.ToolSettlement\{' runtime --glob '*.go' --glob '!**/*_test.go'` finds construction only in the lower-level canonical envelope builder and the store-reconciliation decoder.
- `rg -n 'type toolOutputPayload|func encodeToolOutput|func applyToolOutputBounds' runtime tools` finds no duplicate encoder.

## Dependencies and exclusions

- Requires explicit `runExecution` from work package 1.
- Change `tools.BuildToolSettlement` to accept the complete runtime input contract or delegate from equivalent explicit arguments. No compatibility guarantee requires preserving the variadic claim signature. Deprecate or remove richer attachment/metadata output fields and update GoDoc/tests rather than exposing them on runtime's path.
- Do not parallelize tool calls or change concurrency-key scheduling in this refactor.
