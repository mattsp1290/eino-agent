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
  against the tool registry. With that collision impossible, `ToolSearch` is
  safe to keep out of the sealed durable `ExtensionPlanDescriptor`/
  fingerprint: the search tool's name/description carry no execution
  authority, since every tool it can surface is still validated against the
  frozen registry at claim time. A tool restriction (`Allowed`/`Denied`) is
  resolved through the plan's alias index before matching, so an entry
  naming a tool's alias denies/allows the tool exactly as naming its
  canonical name would (an entry naming neither is inert, not an error —
  restriction sets may be authored for a wider registry); a restriction
  denying the search tool's own configured name disables tool search for
  the plan entirely (`RunPlan.ToolSearch()` returns nil).
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
  run *N+1* without rediscovery. A call to a deferred tool not yet in
  `discovered` settles as a terminal, model-visible denied call ("call
  `tool_search` first") and the turn continues — exactly like a guard
  denial — rather than aborting the run; an unknown/hallucinated tool name
  stays fail-closed (aborts the run), unchanged from before. On `Resume`,
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

## W5 through W8

Not started. Each package adds its rows here when it lands.
