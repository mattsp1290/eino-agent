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
  event, in both generate and streaming mode. The scaffolding drains the
  provider stream to EOF, concatenates chunks carrying `StreamingMeta.Index`
  with `schema.ConcatAgenticMessages`, and republishes one committed chunk;
  no live delta reaches ADK. Live transport of deltas is W3/W7 work. Results
  containing any block kind the scaffolding cannot record (server, MCP,
  tool-search and generated media blocks) fail closed before ADK sees them.
- ADK schedules function calls but only the durable adapter executes them:
  claim under the run fence, the existing permission policy (a real
  `permissions.Policy` asking and denying settles `expected_failure` rows
  without running the executor) and the settlement pipeline, then the settled
  output is the only model-visible and event-visible result. Replayed
  terminal rows are checked against the current run and session.
- A tool interrupt (`compose.StatefulInterrupt` from the tool) produces a real
  ADK checkpoint that is written before the interrupt event reaches the
  consumer. After closing and reopening SQLite under a new run fence, a
  targeted `ResumeWithParams` executes only the addressed leaf; settled
  siblings are never re-executed; a call left `running` by a crashed process
  is settled interrupted without rerun; a call settled after a stale
  checkpoint replays its recorded output without running the executor.
- An untargeted resume keeps the leaf paused, checkpoints again and dispatches
  nothing. Stale fences and competing live claims are rejected. A runner
  checkpoint `Set` failure emits an error event and still emits the interrupt
  event, so public pause promotion requires the interrupt and an error-free
  drain, never the interrupt alone.
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
  schedule sibling function calls, in generate and streaming mode and with a
  retry wrapper configured. The approval record is durable; resume after
  reopen updates it once under the run fence, dispatches exactly one
  continuation whose input
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
  SQL remains authoritative when the checkpoint is stale.
- Eino mints a fresh UUID interrupt ID on every pause and re-pause, while the
  `InterruptCtx.Address` (for example `agent:proof;tool:gate:call-9`) is the
  stable identity. `ResumeWithParams` with an ID that no longer resolves is a
  silent no-op re-pause. Public pause identity in W5 must therefore be the
  address or a durable runtime record that is resolved to the current
  interrupt ID from the freshly loaded checkpoint at resume time, and native
  approval IDs remain distinct from both.
- The scaffolding's ledger uses attempt `1` for every dispatch and a shared
  per-run step sequence, so the `attempt` axis of the unique index is inert
  until W5 defines invocation identity; audited safe call configuration
  covers temperature, top-p, max tokens, stop and tool choice only.
- Upstream retry and failover wrappers only recognize graph-level interrupt
  errors (`compose.ExtractInterruptInfo`); a `compose.StatefulInterrupt`
  returned by the model itself is treated as an ordinary failure by the
  deprecated `IsRetryAble` path and re-dispatched. Host-provided
  `ShouldRetry`/`ShouldFailover` decisions must refuse errors matched by
  `compose.IsInterruptRerunError`; the proof covers both guarded wrappers and
  the unguarded hazard.
- The approval record's decision is a read-then-update under the run fence
  and an in-process mutex, not a store-level conditional write; W2/W5 give
  the typed content contract a conditional update.
- The `TypedChatModelAgent` doc comment describes the agentic variant as
  single-shot, but v0.9.19 builds an agentic ReAct graph with a real
  `AgenticToolsNode`; local function tools do execute.
- The runner writes the checkpoint before sending the interrupt event, so
  public pause promotion must wait for the iterator to drain.

## W2: durable ordered rich content

Status: landed (commits 0574474, 3865683, 5b2922f); dual review applied.

- `session/content.go` is the versioned public content contract: 20 block
  kinds string-identical to Eino's `ContentBlockType`, closed typed payloads,
  a public response-meta projection, bounds (8 MiB per message, 1024 blocks,
  1 MiB per block by default, configurable through
  `runtime.WithContentLimits`), strict canonical decoding (unknown fields,
  duplicate keys, trailing values and unknown kinds are rejected) and exact
  conversion to and from `schema.AgenticMessage`. Content that validates is
  always projectable back to Eino.
- Private material (reasoning signatures, Claude encrypted citation indexes,
  Gemini SDK blobs, provider response and continuation IDs, `Extra`,
  `Extension`) is split into block-addressed `PrivateBlockState` and never
  enters public parts; provider-state envelopes and items carry a `BlockID`.
- Parts: one part per block with the block kind as `PartKind`, plus one
  `response_meta` part per assistant message; block parts need strictly
  increasing ordinals so provider-state parts may interleave. Both SQL
  baselines accept the new kinds; schema fingerprints were regenerated;
  observation text is projected from user and assistant text kinds only.
- Store contract: every kind and every nested function-result variant round
  trips through SQLite and PostgreSQL, including a close/reopen of the SQLite
  file, mixed ordering with response meta, observation exclusion of non-text
  kinds, and a provider-state sentinel scan in verbatim and base64 forms.
  The PostgreSQL required-suite list names every new subtest.
- History: `history.ProjectAgentic`/`LoadAgentic` project durable parts into
  agentic messages with `BlockRef` to part identity; legacy kinds map into
  agentic form; the classic projector understands the new kinds, projects
  user media into multi-content input and fails with `ErrClassicUnsupported`
  on anything it cannot represent.
- Admission: `runtime.UserMessage` is ordered content blocks
  (`runtime.TextUserMessage` for text). Block and part IDs are runtime-owned,
  caller-supplied block IDs are rejected, the submission is deep-cloned before
  validation, and every block part is persisted atomically with the run.
  Media-only submissions are admitted and a session that started with media
  continues on later turns.

Superseded part kinds (`text`, `tool_call`, `tool_result`, `file`, `step`,
`state`) remain declared until the runtime cutover replaces the assistant
persistence path in W3/W5.

## W3: agentic model boundary

Status: landed (commits e4dd842, 84b7bdd); dual review applied.

- `model.Request` carries `[]*schema.AgenticMessage` and typed
  `RequestControls` (tools, deferred tools, tool-search tool, agentic tool
  choice, temperature, top-p, max tokens, stop). `ValidateControls` rejects
  bad partitions and selectors before any dispatch. `NewAgenticStreamer`
  translates controls into Eino call options in a fixed order and always
  sends the tool list, so an empty list clears the client's tools.
- Provider-private state is block-bound: `NewTypedExtensionStateCodec`
  captures reasoning signatures, Claude encrypted citation indexes, Gemini
  SDK blobs and provider response/continuation IDs into `ProviderStateItem`s
  keyed by block ID and restores them only into the dispatched clone; the
  restored message is re-captured and any codec that alters public content
  is rejected. The codec's item shapes are proven byte-equal to the W2
  content split for every private kind.
- `NewClassicStreamer` and `NewClassicStreamerWithProviderState` adapt
  classic `ToolCallingChatModel` providers: faithful translation of text,
  multi-content user input, assistant text/reasoning/calls and tool results,
  typed `capability_unsupported` rejection before dispatch for server, MCP
  and tool-search blocks, deferred tools, tool search, assistant media and
  MCP/server tool-choice selectors, and stable block indices for streamed
  classic tool calls. Native provider translation remains an eino-providers
  deliverable (`eino-agent-td8`); the runtime consumes `model.AgenticModel`.
- Runtime: turn snapshots, admission, context assembly, streaming (bounded
  by `StreamLimits`), audit and the tool loop run on agentic messages. Each
  physical dispatch is one ledger row whose messages, tools, deferred tools,
  search tool, tool choice and scalar controls are persisted and hashed;
  private fields never reach the row. Assistant results persist as rich
  content parts plus response meta and block-bound provider-state parts in
  one transaction; tool results persist as user-role `function_tool_result`
  blocks whose text is the unchanged model-visible `ToolOutput` JSON, with
  retention clamped to the content block budget. Provider-state restore
  follows the projected message, dropping items bound to blocks the
  projection excluded (for example reasoning under default history
  options). Store envelope identity checks and replay decode under hard
  ceilings, not admission defaults.

Findings: Eino's `StreamReader.Close` is single-use; the classic adapter
round-trips classic `Extra` through a transient agentic `Extra` so the
existing Extra-key codecs keep working, and `Request.Clone` rejects any
`Extra` so that transient state can never re-enter a request.

## W4: structured tools, aliases, deferred search, composition

Status: landed; W1 scaffolding kept green.

- `tools.Definition` gains `Aliases []string`, `ArgumentAliases
  map[string][]string` (canonical key -> aliases) and `Deferred bool`.
  `ValidateDefinition` rejects an alias equal to the tool's own name or to
  another alias, and an argument alias equal to its own canonical key or
  reused across canonical keys. Mirroring upstream Eino's
  `compose.applyArgsAliases` (`compose/tool_node.go`), it also rejects an
  argument alias that collides with a declared parameter-schema property
  (derived from `Definition.Parameters` via `ParamsOneOf.ToJSONSchema()`,
  which would otherwise silently steal that property's value), a canonical
  argument key containing `"."` (nested field matching unsupported), and an
  alias that is itself another entry's canonical key (order-dependent under
  Go map iteration otherwise). `Materialize` copies all three onto
  `runtime.Tool`. `composition.composedToolSchemaHash` folds aliases,
  argument aliases and the deferred flag into the tool's schema identity, and
  `session.ToolPlanIdentity` gains `Aliases`/`Deferred` so a sealed plan's
  fingerprint changes whenever they do.
- `runtime.RunPlan` compiles a plan-wide alias index at `NewRunPlan` time and
  rejects (`ErrExtensionPlanMismatch`) an alias that collides with any tool's
  canonical name or with another tool's alias; `RunPlan.ResolveToolName`
  exposes the compiled index. `runtime/adk_tools.go`'s `resolveToolCall`
  re-derives the same collision-checked index per turn from
  `TurnSnapshot.Tools` and resolves a model-requested name (canonical or
  alias) before normalization, remapping argument aliases with the exact
  semantics of upstream Eino's `compose.remapArgs` (an alias key is renamed
  to its canonical key unless the canonical key is already present, in which
  case the alias key is left as an unrecognized field). `session.ToolCall`
  gains `RequestedName` (the model-facing name actually used, persisted in
  the JSON tool-call record with no DDL change); both the durable
  `function_tool_result` block and the same-turn model-visible message use
  `RequestedName`, so a live turn and a later replay always show the model
  the name it actually called. `store/internal/sqlstore.ValidToolRequestEnvelope`
  compares the persisted request block's name against `RequestedName` (falling
  back to `Name` for pre-alias records).
- Enhanced results: `runtime.ToolResult.Parts []ToolResultPart`
  (text/image/audio/video/file/tool_search) is authoritative over
  `Output`/`Structured` when non-empty. `validateToolResult` (the
  `ToolExecutePoint` output validator, so it runs on the tool's own return
  value) rejects a malformed part before it ever reaches settlement: a media
  part must carry `Media` with exactly one of `URL`/`Base64Data` (strict
  base64 when set), a `Name` only on a `file` part, a text part carries only
  `Text`, a `tool_search` part's payload must be valid JSON, and any other
  type is rejected — closing the failure mode where a malformed part (e.g.
  both `URL` and `Base64Data` set, or neither) would otherwise fail
  `session.EncodeContentParts` deep inside settlement and abort the whole
  run instead of degrading the one call. `buildTerminalToolEnvelope` also
  has a belt-and-braces fallback: if a validated part still fails to encode
  (a future encoder rule change), every part degrades to an omission record
  rather than failing the settlement. `runtime.ToolOutput.Parts` bounds each
  part independently against `RetentionPolicy`: an oversized or redacted part
  becomes an omission record (`{Type, Omitted:true, OriginalSize}`) rather
  than a truncated or corrupt payload. `toolOutputToResultContent` is the one
  function that turns a settled `ToolOutput` into the durable/model-visible
  `session.ResultContent` list, used by both `buildTerminalToolEnvelope` and
  the same-turn outgoing message builder: a scalar result keeps the exact
  historical single-text-part shape (the full `ToolOutput` JSON as text); an
  enhanced result carries one content item per bounded part.
  `store/internal/sqlstore.ValidToolResultEnvelope` re-checks this shape
  without importing `runtime`: it decodes a minimal `{"parts":[{"type",
  "omitted"}]}` view from `settlement.Output` and requires the durable
  content to agree in count, per-index type (an omitted or text/tool_search
  part always degrades to a text content item; a non-omitted media part
  becomes its matching content type) and order; the classic scalar shape
  keeps the exact historical invariant (one text item equal to the recorded
  Output JSON). The `tool_search_result` envelope check (below) is
  similarly tightened to cross-check the block's `Name` against the call and
  its discovered tool names against `discovered_tool_names` in the recorded
  Output.
- `tools.Definition.ExecuteRich` (preferred over `Execute` by `Materialize`
  when both are set) and `tools/einotools.ExecuteEnhancedLeaf` adapt an
  `EnhancedInvokableTool` leaf's `schema.ToolResult` into `RichResult`.
  Eino v0.9.19's `tool.InvokableTool` and `tool.EnhancedInvokableTool` both
  declare a differently-typed `InvokableRun` method, so no concrete leaf can
  satisfy both; `catalog.Definition.New` is statically typed to return
  `tool.InvokableTool`, so no standard `eino-tools` catalog leaf can ever be
  enhanced through that path today — `ExecuteEnhancedLeaf` exists for a
  directly-typed `tool.EnhancedInvokableTool` (host-authored, or a future
  catalog variant). `tools.WrapEnhanced` adapts a `runtime.Tool` into an Eino
  `tool.EnhancedInvokableTool` whose `InvokableRun` always goes through an
  injected `tools.Dispatch` callback (the durable, claim-fenced
  implementation is runtime-owned work for a later package); see
  `examples/agentic-graph`.
- Deferred tools and runtime-implemented tool search: `RunPlanSpec.ToolSearch
  *ToolSearchConfig{Name, Description}` (name defaults to `tool_search`) comes
  from `composition.Registrar.ToolSearch`; at most one may be active across an
  assembled plan (rejected at `Registry.AcquireRunPlan`, before
  `runtime.NewRunPlan`). `NewRunPlan` rejects (`ErrExtensionPlanMismatch`) a
  search name that collides with any owned tool's canonical name or alias —
  without this, the colliding tool would be silently unreachable, since
  `prepareToolCalls` tests `isToolSearchCall` before resolving the call
  against the tool registry. `ToolSearch`'s `Description` still carries no
  execution authority (every tool it can surface is still validated against
  the frozen registry at claim time) and stays out of the sealed descriptor,
  but its configured `Name` is sealed into
  `ExtensionPlanDescriptor.ToolSearch` (empty when no search is configured)
  before `NewRunPlan` calls `SealExtensionPlanForSession`: without a stable
  key, `composition.Registry.AcquireResumePlan` re-derives `ToolSearch` from
  the live registry on every resume, so a renamed or removed registration
  would otherwise resume successfully with the wrong (or no) search tool and
  fail much later, deep inside resume, with `tool "tool_search" unavailable`
  instead of the standard plan-mismatch error. A tool restriction
  (`Allowed`/`Denied`) is resolved through the plan's alias index before
  matching, so an entry naming a tool's alias denies/allows the tool exactly
  as naming its canonical name would (an entry naming neither is inert, not
  an error — restriction sets may be authored for a wider registry); for the
  search tool specifically, only an explicit `Denied` entry naming its
  configured name disables search for the plan entirely
  (`RunPlan.ToolSearch()` returns nil) — an `Allowed` list is restriction
  vocabulary for ordinary tools and never implicitly disables search merely
  by omitting the search name (`planToolDenied`, not `planToolAllowed`, gates
  this decision). When a plan has no `ToolSearch` (none registered, or
  denied), a `Deferred` tool has no mechanism by which it could ever be
  discovered: `TurnSnapshot.ProviderRequest` excludes it from both
  `Controls.Tools` and `Controls.DeferredTools` rather than advertising it
  with no way to call it, and a model call to it settles as the terminal,
  model-visible denial `tool %q is deferred and no tool search is
  configured` (naming the configured search tool instead, `call %s first`,
  whenever one is present).
  `TurnSnapshot.ProviderRequest` partitions `snapshot.Tools` into
  `Controls.Tools` (non-deferred, plus any deferred tool already in the
  per-execution `discovered` set) and `Controls.DeferredTools` (the rest),
  and sets `Controls.ToolSearchTool` to a `schema.ToolInfo` advertising a
  required `query` string and optional `max_results` integer parameter
  (mirroring upstream Eino's `getToolSearchToolInfo`,
  `adk/middlewares/dynamictool/toolsearch/toolsearch.go`), with a default
  description documenting the `select:<tool_name>` direct-selection protocol
  when the plan didn't configure one; the search tool itself is never listed
  in `Controls.Tools`/`Controls.DeferredTools`. The search tool is not a leaf
  executor: a call to it is recognized by name in `prepareToolCalls` and
  settled via `buildTerminalToolSearchEnvelope` into a `tool_search_result`
  content block (`session.BlockKindToolSearchResult`) carrying the exact
  frozen `schema.ToolInfo` JSON of each matched tool — never a name outside
  the frozen registry. `searchDeferredTools` implements two query modes: a
  `select:<name>[,<name>...]` query returns exactly the named deferred
  tool(s), matched by canonical name or alias (`max_results` does not bound
  direct selection — the model already named the exact tools it wants);
  anything else is a case-insensitive substring match on name/description,
  bounded by `max_results` (clamped to `[1, 8]`). A search call runs through
  the same observability points as an ordinary tool call
  (`ToolStartedPoint`/`ToolSettledPoint` notifications,
  `observeToolMaterialized`/`startObservedToolCall`/`finishObservedToolCall`/
  `observeToolSettled`) and the mounted guard chain (not permissions, which
  are pattern/scope based) on a synthetic search `Tool`; a guard denial or
  evaluation error degrades to zero discovered tools rather than aborting
  the run — a guard that wants to hard-stop a specific tool should deny that
  *discovered* tool's own calls instead, which still goes through the
  ordinary guard-checked execution pipeline.
  `executeTurn` seeds `execution.discovered` from every
  `tool_search_result` block in the turn's own projected messages
  (`discoveredToolsFromMessages`, scanning `snapshot.Messages`) at the start
  of every turn — fresh run or a later run in the same session — mirroring
  upstream Eino's client-side forward selection
  (`toolsearch.BeforeModelRewriteState`, which rescans the whole
  conversation on every model call rather than trusting a per-run in-memory
  set alone); a deferred tool discovered in run *N* is therefore callable in
  run *N+1* without rediscovery. Because discovery is derived from durable
  `tool_search_result` content, that content's only legitimate writer must
  be the settlement path: `session.PartToolSearchResult` is a reserved kind
  on the fenced `executionStore.AppendPart` (alongside the `tool_call`/
  `tool_result`/`function_tool_call`/`function_tool_result` kinds it is
  written next to by `settleToolCall`'s reserved `ResultPart`), and
  `StreamingOrchestrator.validate` rejects any caller-supplied block kind
  outside the true caller-submittable set (`user_input_*`,
  `mcp_tool_approval_response`) before admission — a hand-authored
  `tool_search_result` block in a `Start` submission can never seed
  `discovered` with no claim, no guard evaluation, and no durable tool-call
  record behind it. A call to a deferred tool not yet in `discovered`
  settles as a terminal, model-visible denied call ("call `tool_search`
  first") and the turn continues — exactly like a guard denial — rather than
  aborting the run; an unknown/hallucinated tool name stays fail-closed
  (aborts the run), unchanged from before. On `Resume`,
  `discoveredToolsFromHistoryPaged` rebuilds the advertised set from the
  session's *full* durable history (paged until exhausted, decoded with
  `session.MaxContentLimits()`, with store errors propagated — not a single
  1000-message page with an unpaged, error-swallowing decode), and
  `resumeTools` also resolves the plan's `ToolSearch()` configuration onto
  the resume snapshot; an interrupted `tool_search` call resumes by
  re-executing the search (it is side-effect free) when pending, or settling
  as interrupted when running — never `tool "tool_search" unavailable`,
  since the search tool is deliberately not a plan tool and a naive
  `tools[call.Name]` lookup would otherwise miss it.
- `examples/agentic-graph` builds a real `compose.Graph` (also a `Chain` with
  `AddBranch` and chain-parallel `AddAgenticToolsNode`) from
  `AddAgenticChatTemplateNode`, `AddAgenticModelNode` and
  `AddAgenticToolsNode` over `tools.WrapEnhanced` adapters with a
  `compose.ToolAliasConfig`, and an audit wrapper around the model node using
  `runtime.AuditAgenticRequest` (exported for this purpose). Its test asserts
  exactly one audited request and one call through the example's own fake
  `Dispatch` closure per tool invocation, and that alias resolution succeeds
  through the real `AgenticToolsNode` — it does not exercise a durable
  settlement (`dispatchFor`'s counters live entirely in-process; wiring a
  real `runtime.BuildToolSettlement` + store settle through this example is
  tracked as follow-up work, not implemented here).

## W5: typed ADK runtime, checkpoints and turn control

Status: phase 1 (single execution engine, checkpoints, turn control) and
phase 2 (retry/failover invocation semantics, durable model-boundary
projection, the concurrent tool-settlement-vs-run-finalization race) landed
and verified per the gate list below. Known gaps below are not yet closed:
approval is not proven against a real `TypedChatModelAgent`, tool search and
enhanced tool results are not byte-for-byte parity with the classic engine,
and the full acceptance-test matrix (multi-turn restart, concurrent-resume,
checkpoint-failure-mode coverage) has not been written.

- Single execution engine: `runtime/adk_model.go` (`adkModel`), `runtime/adk_execution.go`
  (`adkEngine`, `adkTool`, `adkToolSearch`, `AgentFactory`/`AgentBuildContext`,
  `DefaultChatModelAgentFactory`, `durableGuard`), `runtime/adk_checkpoint.go`
  (`adkCheckpointStore`), `runtime/adk_approval.go` (`adkApprovalBinding`) and
  `runtime/turn_loop.go` (`turnLoopCoordinator`, the TurnLoop host API) replace
  `executeTurn`/`streamModelAttempts`/`executePreparedTools`/`streamModel`, which
  are deleted. `Start` now: admits the run's first turn atomically
  (`admission.go`'s `admitDurable` routes it through `ExecutionStore.AdmitTurn`
  instead of hand-appending messages/parts/event), builds one `adk.TurnLoop`
  per run, pushes the pre-admitted first turn via a sentinel item
  (`firstTurnSentinelID`) synchronously before returning (so a fast-following
  `Enqueue` from the caller cannot race ahead of it), and stops the loop with
  `adk.UntilIdleFor` once idle -- not `adk.WithGraceful`/`WithGracefulTimeout`,
  which are themselves *cancel* modes upstream (they set
  `CancelAfterChatModel|CancelAfterToolCalls`) and would spuriously interrupt
  the very turn Start just pushed.
- `runtime.AgentFactory`/`AgentBuildContext` are frozen on `RunPlan`
  (`RunPlanSpec.Agent`, deliberately excluded from the sealed fingerprint like
  `ToolSearch`: construction identity, not durable capability evidence).
  `adkEngine.buildAgent` hands the factory the mandatory `adkModel` and one
  `adkTool` per turn tool (plus `adkToolSearch` when `TurnSnapshot.ToolSearch`
  is configured) and a `durableGuard` `BeforeAgent` handler the factory must
  install last; the guard rejects any tool in the built agent's effective list
  that is not one of those adapters.
- `adkModel.commit` persists exactly the classic engine's per-step facts
  (`captureAssistantProviderState`, `normalizeToolCallIDs`,
  `prepareToolCalls`, `persistAssistantTurn`) before returning a result to
  ADK, so W2/W3/W4 content, provider-state and alias/tool-search machinery is
  reused unchanged. Two adapter-level findings this package hit:
  - ADK's own ReAct loop echoes back messages carrying framework-internal
    `Extra` bookkeeping as input on a later physical dispatch within the same
    turn; `model.Request.Clone` (used by `auditModelRequest`) rejects *any*
    non-empty `Extra` anywhere in a message as unauditable transient state, by
    design. `stripADKInternalExtra` deep-copies and clears every
    message/block `Extra` before a message ever reaches audit or dispatch.
  - `adk.NewAgenticToolsNode` panics (nil-pointer dereference in
    `compose.convTools`) if a registered `tool.BaseTool.Info` returns a nil
    `*schema.ToolInfo` -- the classic engine's convention for an "unadvertised"
    `Tool{Info: nil}` (still callable, just omitted from
    `TurnSnapshot.ProviderRequest`'s model-visible tool list). `adkTool.Info`/
    `adkToolSearch.Info` therefore synthesize a minimal name-only
    `*schema.ToolInfo` when `Tool.Info` is nil.
- `runtime.ToolInterruptPolicy` (`Tool.InterruptPolicy`) lets a tool pause via
  a durable ADK checkpoint before its first execution attempt: `adkTool`
  checks it on a still-`ToolCallPending` call (via `compose.GetInterruptState`/
  `GetResumeContext`) before claiming, raising `compose.StatefulInterrupt`
  with only the call ID as durable info/state. `runtime.ToolCall.ResumeDecision`
  carries the host's resume payload once targeted. `TestTurnLoopChecksPointsAndResumesInterruptedTool`
  (`runtime/turn_loop_checkpoint_test.go`) proves the full cycle against the
  production adapters: pause (tool never executes, checkpoint promoted,
  `Handle.AwaitPause` reports the current-generation `InterruptCtx`), durable
  `RunPaused` status, `ResumeRun` targeting that `InterruptCtx.ID`, and
  completion with the tool executing exactly once.
- Checkpoint store: `adkCheckpointStore` implements `adk.CheckPointStore`/
  `CheckPointDeleter` over the already-landed `session.Checkpoint` store
  methods (`StageCheckpoint`/`ReadPromotedCheckpoint`/`RetireCheckpoints`/
  `RetireRunCheckpoints`). `Set` only stages; promotion is the TurnLoop exit
  protocol's job, after `Wait()` returns with `CheckpointAttempted &&
  CheckpointErr == nil`. The envelope (`adkCheckpointEnvelope`) carries the
  pinned Eino version (`EinoPinnedVersion = "v0.9.19"`), a codec version
  constant, and a fingerprint (`planFingerprint`: the plan's sealed
  fingerprint plus the concrete `AgentFactory` type), all validated before any
  upstream gob bytes are trusted or resumed against.
- TurnLoop exit protocol (`finishTurnLoop`): interrupted/canceled with a
  promoted checkpoint calls `PromotePause` (run -> paused, no live lease,
  interrupted turn + inbox items, `run_paused` event); a checkpoint `Set`
  failure is conservative (no promotion, run left running for lease-expiry
  recovery, matching the plan's explicit requirement); a between-turn stop
  with queued input promotes a "queued continuation" pause via a degenerate,
  content-free admitted turn (`promoteQueuedContinuation`) purely to carry
  `PromotePause`'s required turn identity -- the queued inbox items
  themselves stay `queued` untouched, so a resume plans them through
  `GenInput`, never `GenResume`; a skip-checkpoint stop
  (`Handle.Interrupt`, which has never promised a resumable pause) settles
  the run terminally `RunInterrupted`; a clean idle exit settles
  `RunCompleted` and retires the run's checkpoints. Every exit path stops
  this process's lease heartbeat before returning (`defer
  c.execution.stopLease()`), and `execution.release()` (which stops the
  plan's extension-notification worker) runs *before* the result is sent on
  `Handle.Done()`/`AwaitPause` -- both were real, `-race`-reproducible bugs
  during development: a heartbeat left running after settlement raced a
  receiver's subsequent reads, and sending `Done()` before `release()` (the
  reverse of the classic engine's `executeLifecycle` defer order) let a
  receiver observe state while the plan's worker goroutine was still tearing
  down.
- `session.ApplyCompleteTurn` (`session/turn.go`) was extended to accept
  `TurnInterrupted`, not just `TurnAdmitted`/`TurnRunning`, as a valid
  starting state: a resumed turn's normal completion transitions
  interrupted -> completed through the same atomic path. Without this, every
  successful resume failed with `ErrConflict` on its own `CompleteTurn` call.
- Turn/inbox/invocation identity: `IDGenerator` gained `NewTurnID`/
  `NewInboxID`/`NewInvocationID`; `session.ModelRequestRecord.ID` is now a
  true per-dispatch `InvocationID` (via `runtime.modelRequestIdentity`),
  replacing the prior `run:message:attempt:step` composite. `Attempt`/`Step`
  remain informational.
- Retry and failover: `runtime.defaultRetryConfig` (`runtime/adk_retry.go`)
  maps the legacy `WithAttempts`/`attemptsValue` total-attempt count onto
  `adk.TypedModelRetryConfig`. `runtime.FailoverPolicy` (`RunPlanSpec.Failover`,
  like `Agent`/`ToolSearch` deliberately excluded from the sealed
  fingerprint) maps an ordered list of alternate `model.Selection`s onto
  `adk.ModelFailoverConfig`; `buildFailoverConfig`'s `GetFailoverModel`
  resolves each failover target through the same `model.Resolver` every
  primary dispatch uses (never a cached/pinned client) and dispatches it
  through a fresh `*adkModel` sharing the turn's engine/execution/approval
  binding, so a failover attempt is its own audited ledger row exactly like
  a retry attempt. `ShouldRetry`/`ShouldFailover` both refuse an error
  matched by `compose.IsInterruptRerunError` (upstream's wrappers only
  recognize graph-level interrupts; retrying over one would join a new
  attempt onto already-durable paused state), an error wrapping
  `errCommittedDispatchFailed` (this dispatch's assistant message/tool calls
  were already durably persisted -- retrying past that point has no durable
  slot left to commit a second physical result against), and an error
  wrapping `errPartialStreamObserved` (the provider stream had already
  delivered at least one chunk -- `modelStreamResult.receivedDelta` -- before
  failing, so usage was already charged and, for `Stream`, live text may
  already be visible to a watcher; retrying would double-count usage or show
  a second response spliced after a partial one). Each attempt is its own
  `adkModel` dispatch with its own ledger row/InvocationID (`Attempt` is
  pinned to 1 and purely informational; the engine-wide `Step` counter is
  what actually advances per physical dispatch), matching the plan's "every
  attempt is its own invocation ledger row" requirement. `adkModel.begin`
  emits the durable `session.AttemptReplacedEventKind` event (old/new
  invocation IDs) and an observability `Retry` event
  (`StreamingOrchestrator.observeRetry`) whenever it supersedes a
  `recordFailedAttempt`-marked prior attempt.
- Native MCP approval: `adkApprovalBinding` recognizes a completed
  `schema.MCPToolApprovalRequest` result at the model boundary before ADK's
  ReAct loop can schedule sibling function calls. Unlike the W1 proof (which
  predates the W2 content contract), the public approval request/response
  blocks are ordinary `session.BlockKindMCPToolApprovalRequest`/
  `MCPToolApprovalResponse` content -- `persistAssistantTurn` already
  understands both kinds, no special-casing needed. A private native
  continuation record (exact dispatch input + committed transcript, needed to
  reconstruct the paused call) lives in a `session.PartState` part and is
  read-then-updated once under the run fence and an in-process mutex on
  resume -- the same accepted simplification the W1 finding already flagged
  (a true store-level conditional update was not added; the run fence's CAS
  in `ClaimRun` already guarantees only one process holds the fence at a
  time, so this is safe against concurrent resumes, just not a defense
  against a bug inside one process resuming the same run object twice).
  **Not proven** against a real `TypedChatModelAgent` end-to-end in this
  phase (the W1 proof's approval scaffolding was deleted along with the rest
  of `adk_approval_proof_test.go`; no replacement production test was written
  given time constraints) -- treat as implemented-but-unverified.
- Tool search: `adkToolSearch` registers the runtime-implemented
  `tool_search` pseudo-tool as an ordinary `tool.BaseTool` so ADK's tools node
  can route a model call to it at all, then delegates to the unchanged
  `runtime/tool_search.go` (`executeToolSearchCall`). **Known gap**: ADK's
  tools node always represents *any* registered `tool.InvokableTool`'s result
  as a generic `function_tool_result` content block; it has no notion of this
  codebase's dedicated `tool_search_result` block kind. A model that inspects
  the conversation for a `tool_search_result`-shaped block (as several W4
  acceptance tests do) will not find one, and in the affected tests ADK's own
  `MaxIterations` guard eventually fails the run rather than looping forever.
  Achieving byte-for-byte parity with the classic engine's tool-search
  representation needs a deeper interception design (recognizing a
  `tool_search` call at the model boundary, like the approval binding, rather
  than letting ADK's tools node drive it) and is deferred as follow-up work.
- Enhanced (multi-part) tool results: **known gap**. `adkTool.InvokableRun`
  implements only `tool.InvokableTool` (a `string` return), so a settled
  `ToolResult.Parts`-bearing (enhanced) result cannot be represented as ADK
  expects one -- `tool.EnhancedInvokableTool` (`*schema.ToolResult`) would be
  needed. `TestOrchestratorEnhancedToolResultPersistedMatchesModelVisibleAndReplay`
  fails on this exactly (the model-visible content collapses to one text item
  instead of three). Deferred as follow-up work.
- Resume: `ResumeRun` claims the paused run (`ClaimRun`'s CAS on
  `status='paused'`), validates the checkpoint envelope, resolves the model
  through `model.Resolver.Resolve` (an earlier version of this code
  hand-constructed an unresolved `model.Resolved` with a nil `Streamer` and
  panicked on the first resumed dispatch -- caught by
  `TestTurnLoopChecksPointsAndResumesInterruptedTool`), and drives a fresh
  `TurnLoop` whose `GenResume` maps host-supplied targets straight into
  `adk.ResumeParams`. **Known simplification**: targets are current-generation
  `InterruptCtx.ID` values only (as exposed via `Handle.AwaitPause`'s
  `PauseInfo.InterruptContexts`); this package does not yet resolve a stable
  `InterruptCtx.Address` across multiple pause/resume generations the way the
  plan's "resolved to the current interrupt ID from the freshly loaded
  checkpoint at resume time" language ultimately calls for. Using a stale ID
  is safe (a silent no-op re-pause, per the W1 finding), just not
  address-portable across generations yet.
- Host API (`runtime/turn_loop.go`): `Enqueue` persists a durable
  `session.InboxItem` first (idempotent on key), then pushes its ID into this
  process's live loop if one is registered (`liveLoopFor`); a loop this
  process does not own leaves the item durably queued for a later
  `Start`/`ResumeRun`. `Stop(ctx, runID, StopPolicy)` maps graceful/immediate
  to `adk.WithGraceful`/`WithImmediate` against the registered loop.
  `Handle` gained `AwaitPause() <-chan PauseInfo` and
  `Status(ctx) (session.Run, error)`; `Done()` now fires with the run's
  Result whether it settled terminally or paused (both channels can deliver
  for the same event; a caller only needing the terminal/pause distinction
  reads `Result.Status`). `(*StreamingOrchestrator).Resume` (the legacy
  tool-only resume entry point, unchanged from W1-era behavior for a
  `RunInterrupted` run with no checkpoint) now delegates to `ResumeRun` when
  the run is durably `session.RunPaused`.
- Two engine-ordering races were root-caused and fixed rather than papered
  over. (1) Concurrent tool-call settlement vs. run finalization: ADK's
  tools node dispatches every call declared in one assistant message
  concurrently with no cancellation on a sibling's failure/interruption
  (`compose.parallelRunToolCall`), and `WithImmediate()` stop tears an agent
  turn down "without waiting for any safe point" -- an interrupted or
  panicking tool call's own settlement write (synchronous inside
  `adkTool.InvokableRun`, in ADK's own tool-node goroutine) could still be
  landing when `finishTurnLoop` tried to `SettleRun`, which refuses to
  settle a run with any non-terminal tool call. Fixed with
  `settleRunRetrying` (bounded retry on `session.ErrConflict` specifically)
  at every `finishTurnLoop` settlement call site, plus
  `adkEngine.registerToolBatch`/`awaitToolTurn`/`settleToolTurn`, which
  serialize sibling tool calls within one assistant message into their
  declared order and stop every not-yet-started sibling (settled
  `ToolCallInterrupted`, matching the resume path's
  `terminalizeUnfinishedTools`/`interruptPendingTool` treatment) once an
  earlier one fatally panics or is canceled -- restoring an invariant the
  live path had silently lost relative to resume. (2) `Handle.Interrupt`
  racing the driving goroutine's own startup: canceling the run's context
  before `TurnLoop.Run` ever dispatches anything left ADK with nothing "in
  flight" to report as interrupted, so it could exit with `ExitReason ==
  nil` (an ordinary empty completion) instead -- `finishTurnLoop`'s default
  branch now cross-checks `ctx.Err()` directly rather than trusting
  `ExitReason` alone, and `runFreshTurnLoop` has an equivalent upfront check
  for the narrower race window before a lease is even taken.
- Model-boundary durable projection: `adkModel.durableProjection` replaces
  ADK's in-memory transcript with the durable projection of this run's
  committed history (`loadProviderHistory`/`history.ProjectAgentic` of prior
  turns, spliced with this turn's own already-committed assistant/tool
  messages past `adkEngine.baseMessageCount`, preserving ephemeral
  extension-injected context that a durable reload can never see) before
  every physical dispatch, failing closed
  (`errADKProjectionDiverged`) if the set of tool-call IDs ADK's own input
  carries disagrees with the durable projection's. This closes the
  `tool_search_result`/enhanced-tool-result *model-input* shape gap noted
  above for what the model itself receives on replay/resume; the two
  documented gaps above are specifically about what ADK's tools node can
  represent as a *tool result* going the other direction (durable persistence
  is unaffected either way).
- Verification actually run for this phase: `go build ./...`, `go vet ./...`,
  `go vet -tags postgres_integration ./...`, `gofmt`/`goimports`, and
  `./.bin/golangci-lint run ./...` (repo-wide, 0 issues) all pass.
  `go test ./...` passes for every package, `runtime` included (0 known
  failures). `go test ./runtime -race -count=3` and several additional full
  `-race` passes (including the two fixture/timing-sensitive tests singled
  out above, stress-tested individually at `-count=30`-`50`) are clean.
  `make check` passes (`fmt-check vet test race mod-tidy-check lint
  windows-compile wit-check`) with one known exception:
  `external-consumer-check`'s `TestPublicSessionWatchConstructionExecutionAndReopen`
  times out waiting for its blocking `EventSink` to observe a first event;
  confirmed via `git stash` against the commit before this phase's work that
  this failure predates it and is not a regression introduced here -- left
  open as a genuine, unresolved gap (root cause not yet found: whether the
  extension-notification worker goroutine that drains a run's persisted
  events into the configured `EventSink` is starting/draining correctly in
  this specific external-module harness needs its own investigation).
  `TESTCONTAINERS_RYUK_DISABLED=true GOMAXPROCS=2 GOFLAGS='-p=1' make
  postgres-test` and `make postgres-race` both pass ("required suites
  passed; zero skips"); `TestPostgresRuntime/admission`
  (`runtime/postgres_admission_integration_test.go`) needed its event-count
  assertion updated from 2 to 4 events in sequence (`run_started`,
  `turn_started`, `turn_completed`, `run_finished`) to match the durable
  turn model a completed run now legitimately records -- also confirmed via
  `git stash` to be a pre-existing staleness in the test, not a new gap.
  `POSTGRES_REQUIRED_SUITES` (`Makefile`) already lists the turn/inbox/
  checkpoint/paused-run store contract suites
  (`store/contract/{turns,inbox,checkpoints,paused_runs}`); no further
  suites needed adding.
- Out of scope, not started: production approval-proof tests against a real
  `adk.NewTypedChatModelAgent` (generate/stream, approve/deny, reopen from a
  promoted checkpoint, mixed function-call+approval pausing -- see the
  approval bullet above), the broader acceptance-test matrix (multi-turn
  restart with reconstructed history, new-input-racing-idle-settlement,
  injected `AdmitTurn`/`CompleteTurn` write failures, stop-mode/timeout/
  recursive-cancel/preempt/idle-exit coverage, duplicate enqueue,
  checkpoint-failure-mode coverage, stale fence, malformed envelope, version
  mismatch, unsupported gob state, concurrent-resume-yields-one-owner,
  unsafe-tool-never-rerun), child agents (typed `AgentTool`/`DeepAgent`),
  removing the superseded classic `PartKind`s, W6/W7.
