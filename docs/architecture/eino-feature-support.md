# Eino Feature Support

This document is the maintained capability and verification matrix for the
CloudWeGo Eino dependency. It replaces the planning inventory that produced the
v0.9.19 adoption and is updated whenever a package of that adoption lands.

## Pin

| Field | Value |
| --- | --- |
| Module | `github.com/cloudwego/eino` |
| Version | `v0.9.19` (exact, no `replace`) |
| Origin | `https://github.com/cloudwego/eino` at `9d983b36a5112a1c233056b1a099825298fafb8f` (`refs/tags/v0.9.19`) |
| Module checksum | `h1:i71YUBK3nwY4L53dkzRgZpAcPSZ4v4eRponN7W9sDtk=` |
| Guard | `internal/deps/eino_pin_test.go` asserts the exact version, the absence of a replacement and the `go.sum` line |

The nested modules `wasmext/gen`, `examples/wasm-extensions` and
`internal/tools` do not consume Eino and were not changed by the pin.

Previous pin `v0.8.13` compiled unchanged against `v0.9.19`. One existing test
changed behavior: `schema.ToolInfo` now marshals `ParamsOneOf` natively and
distinguishes the parameter-map form from the JSON-schema form, so the
protected tool-info comparison in `runtime/extension_tool.go` compares the
converted schema separately from the remaining fields.

## W1: durable interception proof

Status: passed. The proof lives in `runtime/adk_proof_test.go`,
`runtime/adk_approval_proof_test.go`, `runtime/adk_boundary_test.go`,
`runtime/adk_recovery_test.go`, `runtime/adk_turnloop_test.go`,
`runtime/adk_approval_test.go` and `runtime/adk_wrapping_test.go`. It uses the
real upstream `adk.TypedChatModelAgent[*schema.AgenticMessage]`,
`adk.TypedRunner`, `compose.AgenticToolsNode`, checkpoint protocol,
`adk.TurnLoop`, retry and failover wrappers, and `adk.NewTypedAgentTool`, with a
scripted `model.AgenticModel`, SQLite and the existing fenced execution store.
It is test-only scaffolding: W5 promotes the proven adapters into the runtime
and deletes the scaffolding while keeping the regression tests.

Verified invariants (each is an executable assertion, run with `-race -count=10`):

- Every physical model call, including retries, failover attempts and child
  agent dispatches, creates one model request ledger row before dispatch and
  settles it completed or failed afterwards.
- The model result is committed (text and reasoning parts, canonical pending
  tool-call records, finalized assistant message) before ADK emits the model
  event, in both generate and streaming mode. Streaming chunks carrying
  `StreamingMeta.Index` are concatenated with `schema.ConcatAgenticMessages`.
- ADK schedules function calls but only the durable adapter executes them:
  claim under the run fence, existing permission and settlement pipeline,
  then the settled output is the only model-visible and event-visible result.
- A tool interrupt (`compose.StatefulInterrupt` from the tool) produces a real
  ADK checkpoint that is written before the interrupt event reaches the
  consumer. After closing and reopening SQLite under a new run fence, a
  targeted `ResumeWithParams` executes only the addressed leaf; settled
  siblings are never re-executed; a call left `running` by a crashed process
  is settled interrupted without rerun; a call settled after a stale
  checkpoint replays its recorded output without running the executor.
- An untargeted resume keeps the leaf paused, checkpoints again and dispatches
  nothing. Stale fences and competing live claims are rejected.
- Cancellation: before dispatch produces no ledger row; `CancelAfterChatModel`
  commits the model result and checkpoints without running tools;
  `CancelAfterToolCalls` settles the tool then checkpoints; immediate
  cancellation during a tool ends the run with a `CancelError`.
- `adk.TurnLoop` runs two successive turns under one live run fence with
  distinct assistant messages and ledger rows, without settling the run. The
  outer store is written only at loop exit, after `OnAgentEvents` returned and
  before `Wait` returns; a failed outer `Set` is reported through
  `TurnLoopExitState.CheckpointErr` without a promoted checkpoint; a
  between-turn stop checkpoints only the queue and is replanned through
  `GenInput`, not `GenResume`; a clean idle exit writes no checkpoint and
  retires a loaded one; a preempting push cancels only the captured turn.
- A completed model result carrying `MCPToolApprovalRequest` pauses at the
  model boundary through `compose.StatefulInterrupt`, before the ReAct loop can
  schedule sibling function calls. The approval record is durable, resume
  after reopen CASes it once, dispatches exactly one continuation whose input
  is the original input, the committed transcript and the user-role
  `MCPToolApprovalResponse` bound to the request ID, and a duplicate decision
  cannot dispatch again. Approval-only results also pause instead of
  completing normally.
- Middleware-added tools cannot evade settlement: the model adapter refuses
  calls to tools outside the frozen registry, and a last-registered
  `BeforeAgent` handler wraps registry tools injected by earlier middleware.

Findings that shape later packages:

- `model_requests` is unique on `(run, attempt, step)`. Concurrent adapters on
  one run (child agents, failover models) and resumed processes must draw
  from one per-run invocation sequence; W5 introduces an explicit invocation
  identity as planned.
- SQL stores return an unsettled call's absent output as JSON `null`.
  `settleInterruptedTool` now treats `null` as absent so interrupted
  settlements carry a real bounded output.
- Executed sibling tools are served from the ADK checkpoint state on resume;
  SQL remains authoritative when the checkpoint is stale. Interrupt IDs are ADK
  addresses and are distinct from native approval IDs.
- The `TypedChatModelAgent` doc comment describes the agentic variant as
  single-shot, but v0.9.19 builds an agentic ReAct graph with a real
  `AgenticToolsNode`; local function tools do execute.
- The runner writes the checkpoint before sending the interrupt event, so
  public pause promotion must wait for the iterator to drain.

## W2 through W8

Not started. Each package adds its rows here when it lands.
