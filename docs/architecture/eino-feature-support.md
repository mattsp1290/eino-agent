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

Status: passed. The proof was carried by test-only scaffolding
(`runtime/adk_proof_test.go`, `adk_approval_proof_test.go`,
`adk_boundary_test.go`, `adk_recovery_test.go`, `adk_turnloop_test.go`,
`adk_approval_test.go`, `adk_wrapping_test.go`), which used the real upstream
`adk.TypedChatModelAgent[*schema.AgenticMessage]`, `adk.TypedRunner`,
`compose.AgenticToolsNode`, checkpoint protocol, `adk.TurnLoop`, retry and
failover wrappers, and `adk.NewTypedAgentTool`, with a scripted
`model.AgenticModel`, SQLite and the existing fenced execution store. W5
deleted this scaffolding after promoting the proven adapters into the
runtime; the surviving regression coverage lives in
`runtime/adk_approval_production_test.go` and the W5 acceptance matrix below.

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

The superseded classic part kinds (`text`, `tool_call`, `tool_result`,
`file`, `step`, `state`) are removed (W5 phase 2, commit history at HEAD):
the typed ADK engine (W5) writes only the 20 block kinds plus
`response_meta`/`compaction`/`provider_state`; both SQL baselines' `kind`
CHECK constraints were edited in place (no new migration) and schema
fingerprints regenerated. `runtime.adk_approval`'s decision-CAS record moved
off the removed `state` kind onto a new dedicated `PartApprovalDecision`
("approval_decision") kind rather than reusing `provider_state`, since
`runtime.loadProviderHistory` strictly decodes every `provider_state` part on
an active message as a `ProviderStateItem` envelope and would fail closed on
the CAS record's unrelated payload shape.

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
  `admitTurn` (`runtime/turn_loop.go`) and `prepareFreshTurn`
  (`runtime/orchestrator.go`) seed `execution.discovered` from every
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
- Durable tool-call ids are always runtime-minted, never trusted verbatim
  from the provider. `runtime.prepareToolCalls` used to mint a call's
  durable `ID` (the `tool_calls` table's UNIQUE-store-wide primary key) via
  `IDGenerator.NewToolCallID()` only when the model left its `CallID` empty;
  otherwise it reused the provider's own `CallID` string verbatim as `ID`.
  Providers that reissue the same indexed id across unrelated responses
  (`call_0`-style: llama.cpp, Ollama, vLLM OpenAI-compatible endpoints,
  replayed/deterministic fixtures) therefore collided on a later turn with a
  clean `session.ErrConflict`. `prepareToolCalls` now *always* mints a fresh
  `ID`, and the provider's original `CallID` (verbatim, possibly empty) is
  preserved separately as `ProviderCallID` on both `session.ToolCall` and
  `runtime.ToolCall` — persisted in the existing free-form JSON tool-call
  record with no DDL change, exactly like `RequestedName`. The durable
  content block's `CallID` (what `compose.GetToolCallID` returns, what
  `store.GetToolCall` is keyed by everywhere it is looked up —
  `runtime/adk_execution.go`'s `adkTool`/`adkToolSearch InvokableRun`,
  `runtime/adk_approval.go`'s sibling-orphan reconciliation,
  `runtime/interrupt.go`'s resume/crash-reconciliation replay, tool search)
  stays the minted `ID` throughout, unchanged from before this fix except
  that it is now unconditionally store-unique — this keeps every internal
  identity comparison (including `adkModel.durableProjection`'s
  adk-vs-durable id-set reconciliation) exactly as it was. Only the literal
  wire payload handed to the model provider needs to show the provider back
  its own id: `runtime.publicizeToolCallIDs` (`runtime/orchestrator.go`)
  rewrites `function_tool_call`/`function_tool_result`/`tool_search_result`
  block `CallID`s from the minted `ID` to `ProviderCallID` (falling back to
  `ID` when the provider supplied none or an invalid/oversized one, and,
  processing every call in request order, whenever sending `ProviderCallID`
  would collide with an earlier call's wire id in that same outgoing
  request — see below) in a copy of the messages, applied exactly
  once per physical dispatch, at the top of `adkModel.begin` — before that
  request is audited/ledgered, so the request the ledger describes and the
  `ProviderRequest` `adkModel.dispatch` actually sends are the same bytes —
  so both a live turn's continuation and a replayed/resumed turn's next
  dispatch present the provider its own ids for call/result correlation,
  while every durable record and internal lookup stays keyed on the minted,
  always-unique `ID`.
  AG-UI tool events and observability continue to report the durable `ID`
  (unchanged); tool-call observations (`tool.call`/`tool.settled`) also carry
  `ProviderCallID` as a bounded, content-free `provider_call_id` metadata
  attribute when the provider supplied one, so a failure can still be
  correlated against that provider's own logs.
  `store/storetest`'s tool-call contract test round-trips `ProviderCallID`
  through `CreateToolCall`/`GetToolCall`/`ClaimToolCall`/
  `ListUnfinishedToolCalls`/`SettleToolCall`, and asserts that a settle
  envelope whose result block is keyed on the provider id instead of the
  durable id is refused with `session.ErrConflict`.

  Fidelity, duplicates, and failure modes this fix (and the round-two
  reconciliation that followed it) guarantees:
  - **Ledger fidelity.** `publicizeToolCallIDs` runs exactly once per
    physical dispatch, at the top of `adkModel.begin` -- before
    `nextStep`/`currentMessageID`, so a resolution failure neither consumes
    a step nor appends an assistant placeholder row -- and its output
    (`adkDispatch.wireInput`) is both what `begin` audits/hashes into the
    request ledger (`ModelRequestRecord.Messages`/`ContentSHA256`, the
    `ModelRequestedNotice.ContentHash`) and what `adkModel.dispatch` sends
    over the wire. The two can never diverge: there is no second rewrite.
  - **Duplicate provider ids.** Every distinct durable id referenced
    anywhere in the outgoing request (across every message, not just the
    current turn's own calls) is processed in request order -- the order
    its first block appears. A call's provider id goes on the wire only if
    (a) no earlier call in that same request has already sent that exact
    string, and (b) the string is not the durable id of any call referenced
    in the request (every durable id is reserved for its own call from the
    start, so a provider id can never be mistaken for another call's
    fallback); otherwise the call sends its own durable id instead (on both
    its call block and its result block, deterministically), which the same
    reservation keeps from ever colliding with another call's wire id in
    turn. Processing in request order also means the earliest call with a
    given provider id keeps it: once a request has sent a call's provider id
    verbatim, a later request that introduces a colliding call never changes
    that already-sent value, which keeps provider-side prompt-prefix caches
    (llama.cpp, vLLM, Ollama) valid as history grows.
  - **Failing closed.** A tool-call block with no `tool_calls` row (only
    reachable via direct store writes or imported history, since
    `rejectNonCallerBlocks` refuses caller-authored tool blocks and context
    contributions are text-only), or one whose row belongs to a different
    session, fails the dispatch with `errToolCallIDUnresolved` -- but only
    when the underlying store lookup itself failed deterministically
    (`session.ErrNotFound` or `session.ErrConflict`, wrapped with `%w` so
    the cause stays inspectable); a sentinel `defaultShouldRetry`/
    `defaultShouldFailover` both refuse to retry or fail over, since a
    deterministic, durable-consistency failure would only burn the run's
    retry/failover budget for nothing. Any other store error (a transient
    read failure, for example) is returned unwrapped and stays fully
    retryable/failover-eligible under the run's normal policy.
  - **Provider id validation.** `prepareToolCalls` treats a captured
    provider id as absent (falls back to the minted id) unless it is valid
    UTF-8 and no longer than `session.DiscoveryMaxIdentityBytes` -- reused
    here as a convenient existing bound, not because a provider call id is
    itself a discovery identity -- so an invalid or oversized id from a
    misbehaving provider never round-trips altered through the SQL stores'
    JSON encoding.
  - **Model-visible body vs. wire id.** The model reads the provider's own
    id on the wire `function_tool_call`/`function_tool_result`/
    `tool_search_result` block's `CallID`. The result's model-visible body
    (`ToolOutput.tool_call_id` inside the JSON content, and the tool-search
    output) still carries the *durable* id -- it is never rewritten, since
    that body is also the durably persisted and replayed content, and both
    ids are legitimate: the block-level `CallID` is what a stateful
    provider correlates by, while the body's `tool_call_id` is this
    package's own durable identity for the call.
  - **Ordering.** `ListUnfinishedToolCalls` (used by both the legacy
    non-ADK `resumeRun` and by `terminalizeUnfinishedTools` crash
    reconciliation) orders by the request assistant message's own creation
    order and then the call's block position within it, not by the minted
    tool-call id -- a sequence-generator id like `tool-call-10` sorts before
    `tool-call-9`, which has no relationship to declared order.

## W5: typed ADK runtime, checkpoints and turn control

Status: phase 1 (single execution engine, checkpoints, turn control), phase 2
(retry/failover invocation semantics, durable model-boundary projection, the
concurrent tool-settlement-vs-run-finalization race), phase 3 (a real
live-streaming regression and an idempotency-key bug found and fixed,
production approval proof against a real `adk.NewTypedChatModelAgent`, and a
focused acceptance-test matrix), phase 4 (the W5(b) round-1 dual-review
reconciliation pass: `reviews/w5-engine-2026-09-11-013808-482da09897eb/reconciliation.md`,
items 1-16, all accepted, none rejected), and phase 5 (a *second* dual-review
pass against the phase-4 fix itself --
`reviews/w5-engine-2026-09-11-013808-482da09897eb/fix-pass/reconciliation.md`,
items 1-10, all accepted, none rejected -- both round-2 reviewers found real,
reproduced defects in phase 4's own fixes) landed and verified per the gate
list below, including `make check` and the PostgreSQL-backed gates
(`make postgres-test`/`postgres-race`, and the external-consumer check with
`EINO_AGENT_CONSUMER_POSTGRES=1`) in full. Phase 4 fixed five reviewer-verified
criticals (a rejected `ResumeRun` used to strand the run `running` with no
driver; a paused/interrupted run's own first-turn sentinel used to leak into
`PromotePause`/`AdmitTurn` as a fake inbox ID, failing every first-turn pause
against a real SQL store; a sibling tool call declared after a paused or
terminal-replay call used to hang forever in the tool-batch gate; `Enqueue`
used to silently strand durably-accepted input with no live loop to consume
it; and a never-consumed queued item used to get marked `interrupted`
instead of staying `queued`) plus nine important-severity defects. Phase 5
then found that phase 4's own `Enqueue`/idle-shutdown fix was itself broken
three different ways (a caller-visible panic on a sealed loop, a stranded
`running` run with no driver, and a double-delivered inbox item across a
resume), plus a guard gap, an approval-transaction hazard, and several
unmet "done when" test criteria -- see the bullets below for each, and the
Host API / `AgentBuildContext` bullets in particular for what changed
between phase 4 and phase 5. Known gaps below are not yet closed: tool
search and enhanced tool results are not byte-for-byte parity with the
classic engine, and several acceptance-test-matrix scenarios remain
unwritten (see that bullet for the exact, now-shorter list).

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
  that is not one of those adapters. **Engine-enforced, not just documented**:
  `durableGuard` tracks whether its `BeforeAgent` hook actually ran
  (`hasRun`), and `turnLoopCoordinator.onAgentEvents` fails the turn as an
  `ErrInvalidOrchestrator` construction error if it never did, once the
  agent has finished running for the turn (checked only after the
  interrupted/no-engine case is ruled out, so a legitimate cancellation
  is never misreported as a construction failure). **The guard is the
  stronger, structurally-binding constraint, not the dispatch check below
  (round-four reconciliation, MR suggestion)**: `AgentBuildContext.Guard`
  is typed `adk.TypedChatModelAgentMiddleware[*einoschema.AgenticMessage]`,
  installed by appending to `TypedChatModelAgentConfig.Handlers` -- so only
  a `TypedChatModelAgent` has anywhere to install it at all. A factory
  returning a pure workflow/sequential agent with no `TypedChatModelAgent`
  child therefore fails this guard check before the dispatch check below is
  ever consulted, and a workflow-pattern factory is consequently
  unsupported in this phase: every compliant turn must wrap at least one
  `TypedChatModelAgent` built from `build.Model`, and that agent must
  actually run and dispatch. This catches a factory
  that never wires `AgentBuildContext.Guard` in at all, but **not** one
  that installs the guard and separately substitutes its own model instead
  of `build.Model`: `durableGuard.BeforeAgent` only inspects the agent's
  *tool* list, never its model, so that case passed this check alone
  (**phase 5 finding**, reproduced: the run settled `RunCompleted` with a
  content-free assistant placeholder and zero durable adapter calls). Fixed
  with a second, complementary check: `adkEngine.dispatches`, an
  `atomic.Int64` incremented by `adkModel.begin` once a physical call's
  ledger row is durably committed, checked alongside `guardRan` -- a
  normally-completed turn that recorded zero dispatches fails the same way.
  This is a zero-check, not an every-call check: a *hybrid* factory whose
  agent routes exactly one call through `build.Model` and every other call
  through a substituted model in the same turn passes it (`dispatches >
  0`), while the substituted calls remain unledgered and the turn still
  settles `RunCompleted` -- narrowing this to verify every dispatch, not
  just the first, is still **not covered** (see the round-three
  reconciliation phase-6 bullet, which also names the "every compliant
  turn must dispatch at least once" constraint this check imposes on
  `AgentFactory` explicitly, since a workflow-pattern factory that
  legitimately never dispatches on some turns fails it today). Both checks
  are necessarily post-hoc (they run only after the agent's
  events iterator has fully drained, so a noncompliant factory's own
  unledgered provider call has already happened by the time either fires);
  this phase did not add construction-time verification of the returned
  agent's tool/model identity. Proven by
  `TestNonCompliantFactoryWithoutGuardFailsAsConstructionError` (missing
  guard) and `TestRogueModelWithGuardInstalledFailsTurn` (guard installed,
  model substituted) in `runtime/w5_reconciliation_test.go` and
  `runtime/w5_round2_test.go` respectively.
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
  **W5(b) reconciliation fix**: `adkEngine.registerToolBatch`/`awaitToolTurn`/
  `settleToolTurn` (see the engine-ordering-races bullet below) previously
  called `settleToolTurn` on only two of `adkTool.InvokableRun`'s exit paths
  and never at all from `adkToolSearch.InvokableRun` -- a `ToolInterruptPolicy`
  pause, a terminal-replay branch, or several error paths all left the next
  not-yet-started sibling in a multi-call batch parked in `awaitToolTurn`
  until the run context died, and a `tool_search` call declared in the same
  batch as an ordinary tool blocked that tool forever. `settleToolTurn` is
  now idempotent and both methods release the batch gate unconditionally via
  a `defer` covering every exit path, and `adkToolSearch` now participates in
  the same `awaitToolTurn`/`settleToolTurn` protocol. Proven with
  `-race -count=10` in `runtime/turn_loop_sqlite_test.go`
  (`TestToolBatchInterruptPolicyPauseDoesNotBlockSiblingAgainstSQLite`,
  `TestToolBatchToolSearchDeclaredFirstDoesNotBlockSiblingAgainstSQLite`).
- Checkpoint store: `adkCheckpointStore` implements `adk.CheckPointStore`/
  `CheckPointDeleter` over the already-landed `session.Checkpoint` store
  methods (`StageCheckpoint`/`ReadPromotedCheckpoint`/`RetireCheckpoints`/
  `RetireRunCheckpoints`). `Set` only stages; promotion is the TurnLoop exit
  protocol's job, after `Wait()` returns with `CheckpointAttempted &&
  CheckpointErr == nil`. The envelope (`adkCheckpointEnvelope`) carries the
  pinned Eino version (`EinoPinnedVersion = "v0.9.19"`), a codec version
  constant, and a fingerprint (`planFingerprint`: the plan's sealed
  fingerprint plus the concrete `AgentFactory` type), all validated before any
  upstream gob bytes are trusted or resumed against. `Delete` retires every
  staged revision up to the latest one this adapter staged *immediately*,
  under the still-live fence (`RetireCheckpoints`) -- not deferred until
  after the exit protocol finishes acting on the checkpoint; an earlier
  version of its doc comment incorrectly described retirement as deferred
  (**W5(b) reconciliation fix**, comment-only). `Set` also now probes past a
  stale staged-but-unpromoted revision from a different, crashed prior
  staging attempt (bounded retry on `session.ErrConflict`, only on a fresh
  adapter instance's first `Set`) instead of hard-failing every later `Set`
  for the run.
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
  **Two W5(b) reconciliation fixes to this protocol**: (1) `PromotePause`'s
  `InboxIDs` are now exactly `state.InterruptedItems` (the interrupted
  turn's own actually-*consumed* items), filtered through
  `interruptedItemIDs` to drop the first-turn sentinel
  (`"\x00first-turn"`, which has no durable inbox row at all -- `Start`'s
  first turn is admitted directly inside the admission transaction, never
  through the inbox). Previously `UnhandledItems` and `TakeLateItems()` were
  also included: those items were never consumed, so marking them
  `interrupted` made a resumed `GenInput -> AdmitTurn -> consumeInboxForTurn`
  (which accepts only `queued`) fail the resumed run with `ErrConflict`, and
  against a real SQL store (whose `interruptInboxItems` turns an unknown ID
  into `ErrConflict`, unlike the old in-memory test fixture) the sentinel
  leak failed *every* first-turn pause outright. `TakeLateItems()` is still
  drained exactly once to seal the late buffer; its items just stay
  `queued`. (2) the clean-exit ("default") branch now also drains
  `TakeLateItems()` before ever settling `RunCompleted`, diverting to the
  same queued-continuation pause when non-empty -- see the `Enqueue` bullet
  below for why. Both are proven against a real SQLite store in
  `runtime/turn_loop_sqlite_test.go`.
- `session.ApplyCompleteTurn` (`session/turn.go`) was extended to accept
  `TurnInterrupted`, not just `TurnAdmitted`/`TurnRunning`, as a valid
  starting state: a resumed turn's normal completion transitions
  interrupted -> completed through the same atomic path. Without this, every
  successful resume failed with `ErrConflict` on its own `CompleteTurn` call.
  **W5(b) reconciliation fix**: `CompleteTurn`'s inbox settlement
  (`settleInboxForTurn`) previously only picked up rows still `InboxConsumed`
  for the turn; a resumed turn's own in-flight items were `InboxInterrupted`
  by that point (via `PromotePause`), so they never reached `InboxCompleted`
  when the resumed turn went on to finish normally.
  `settleInboxForTurn` now takes an explicit `from` state list --
  `CompleteTurn` settles both `InboxConsumed` and `InboxInterrupted` rows,
  `InterruptTurn` still only `InboxConsumed`.
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
  understands both kinds, no special-casing needed; `adkModel.durableProjection`
  reconstructs the continuation from committed history like any other fact,
  so the binding carries no private transcript/input blob at all, only a
  minimal decision-CAS record (`{Type, ApprovalRequestID, Status, Decision}`)
  in a `session.PartState` part (now `session.PartApprovalDecision` -- see
  the W2 section's "superseded part kinds" note), read-then-updated once
  under the run fence and an in-process mutex on resume -- the same accepted
  simplification the W1 finding already flagged (a true store-level
  conditional update was not
  added; the run fence's CAS in `ClaimRun` already guarantees only one
  process holds the fence at a time, so this is safe against concurrent
  resumes, just not a defense against a bug inside one process resuming the
  same run object twice). A sibling function_tool_call on the same message as
  the approval request is permanently orphaned once the pause fires (ADK's
  tools node never dispatches it, and a decided resume's continuation is a
  fresh physical dispatch, not a resumption of that ReAct iteration): `pause`
  now terminalizes it interrupted immediately (reusing
  `interruptPendingTool`, the resume path's equivalent treatment of an
  unstarted call after a fatal outcome) rather than leaving it pending
  forever, and `durableProjection`'s ADK-vs-durable tool-call-ID
  divergence check (`approvalOrphanedToolCallIDs`) excludes it from the
  comparison, since ADK's own resumed input legitimately never mentions it
  again.
  **Now proven** against a real `adk.NewTypedChatModelAgent`
  (`DefaultChatModelAgentFactory`) end-to-end via
  `TestApprovalPausesBeforeSiblingToolExecutionAndResumesViaProductionAgent`
  (approve and deny, mixed function-call+approval pausing before any local
  tool execution, untargeted resume re-pausing at a fresh checkpoint
  generation, targeted `ResumeRun` completing, a duplicate decision refused)
  and `TestApprovalOnlyResponseStillPausesViaProductionAgent`
  (`runtime/adk_approval_production_test.go`), replacing the deleted W1
  `adk_approval_proof_test.go`/`adk_approval_test.go` assertions. Getting
  these production tests to pass surfaced and fixed three real bugs no
  fixture-level test had caught: (1) `session/history/agentic_projection.go`'s
  rich-vs-legacy content-family dispatcher counted a message's private
  `PartState` CAS part (now `PartApprovalDecision`) as "legacy" content, so
  any durable reload of a message carrying both an approval request and
  ordinary rich content (`ErrMixedContentKinds`) failed -- fixed by
  excluding it from the count, matching how both downstream projectors
  already treated it as ignorable by design. (2)
  `adkApprovalBinding.commitResponse` built its
  committed `MCPToolApprovalResponse` content block without ever assigning
  it a block ID (`ContentBlock.Validate` rejects an empty ID), so a decided
  resume's continuation dispatch always failed content validation -- fixed
  by routing it through `assignContentBlockIDs`, the same helper `Start`/
  `Enqueue` use. (3) the sibling-call orphaning described above (a run could
  never terminally settle while an orphaned pending tool call sat
  unterminalized forever, and `durableProjection`'s own divergence check
  independently rejected the resumed dispatch before that).
  `adkModel.Generate` is not reachable through the production entry point at
  all any more (see the `TypedAgentInput.EnableStreaming` fix below); Stream
  and Generate share the same approval `prepare`/`pause`/`durableProjection`/
  `begin`/`commit` call sequence, so this phase's Stream-path production
  coverage is what Generate would also run, and Generate has no
  approval-specific logic of its own left to prove independently: the two
  methods' only differences are the live-delta `onDelta` wiring (Stream
  only) and the final result assembly, neither of which the approval
  binding touches).
  **W5(b) reconciliation fix**: the decision CAS (`UpdatePart`) and the
  committed `MCPToolApprovalResponse` message (`commitResponse`'s
  `AppendMessage`+`AppendPart`s) were two independent, non-transactional
  writes; a crash or any transient error between them left the decision
  durably `decided` with no response message ever committed, and every
  later resume (including a retried dispatch) hit the always-fatal
  `"approval %s already decided"` branch -- permanently wedging the run with
  the host's approval spent. `prepare` now wraps both writes in one
  `session.ExecutionStore.WithinTx`, and a re-entry carrying the *same*
  already-committed decision (as ADK's own retry wrapper produces when it
  re-invokes `Generate`/`Stream` after a transient dispatch failure right
  after a successful decision) is a no-op continuation rather than a fatal
  error. Proven by `TestApprovalDecisionSurvivesTransientDispatchFailure`
  (`runtime/w5_reconciliation_test.go`): a dispatch failure injected
  immediately after a successful `approve`, retried via ADK's own
  `WithAttempts(2)` retry wrapper, still completes the run with exactly one
  committed `MCPToolApprovalResponse` block.
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
- Resume: `ResumeRun` now validates everything derivable from the durable
  `GetRun` record -- the promoted checkpoint envelope (fingerprint/Eino
  version/codec), the plan fingerprint, `model.Resolver.Resolve` (an earlier
  version of this code hand-constructed an unresolved `model.Resolved` with
  a nil `Streamer` and panicked on the first resumed dispatch -- caught by
  `TestTurnLoopChecksPointsAndResumesInterruptedTool`), and `ListTurns` --
  entirely *before* `ClaimRun`'s CAS on `status='paused'`. **W5(b)
  reconciliation fix**: previously `ClaimRun` ran first, so any later check
  failing (a version/fingerprint mismatch, a model-resolve failure) left the
  run durably `running` with a fresh claim token but no driver and no
  heartbeat -- unrecoverable, since `ResumeRun` itself then refuses it
  (`!run.Paused()`) and the legacy `Resume` path routes a non-paused,
  non-terminal run into the tool-only resume flow, which settles it
  terminally `RunInterrupted`, destroying the pause. `TestCheckpointVersionMismatchRejectedOnResume`/
  `TestCheckpointFingerprintMismatchRejectedOnResume`/
  `TestResumeRunModelResolveFailureLeavesRunPaused`
  (`runtime/acceptance_matrix_test.go`) now assert the run stays
  `session.RunPaused` after each rejected resume, not just that `ResumeRun`
  itself returns an error. It then drives a fresh `TurnLoop` whose
  `GenResume` maps host-supplied targets straight into `adk.ResumeParams`.
  **Known simplification**: targets are current-generation `InterruptCtx.ID`
  values only (as exposed via `Handle.AwaitPause`'s `PauseInfo.InterruptContexts`);
  this package does not yet resolve a stable `InterruptCtx.Address` across
  multiple pause/resume generations the way the plan's "resolved to the
  current interrupt ID from the freshly loaded checkpoint at resume time"
  language ultimately calls for. Using a stale ID is safe (a silent no-op
  re-pause, per the W1 finding), just not address-portable across
  generations yet.
  **Three more W5(b) reconciliation fixes**: (1) the reconstructed
  `config.Snapshot` previously carried no system prompt and no agent
  options at all (both were simply never persisted), so every post-resume
  model dispatch silently ran with an empty system prompt and a model
  resolved without the agent's options; `admissionConfig` now also persists
  `system_prompt` and a JSON-encoded `agent_options` on the durable
  `session.Run.Config` map (no schema/DDL change -- `Config` is already an
  opaque JSON-serialized field), and `ResumeRun` decodes them back
  (`decodeAgentOptions`). (2) the resumed run's `turnLoopHandle` had a nil
  `cancel` and its context was derived directly from the caller's (possibly
  request-scoped) `ctx`, so `Handle.Interrupt` on a resumed run degraded to
  a best-effort `loop.Stop` that could not win a startup race, and a
  resumed run died the moment an HTTP handler's context returned; `ResumeRun`
  now derives `context.WithCancel(context.WithoutCancel(ctx))` and gives the
  handle that `cancel`, matching `Start`. Proven by
  `TestResumedHandleInterruptCancelsRun` (`runtime/w5_reconciliation_test.go`).
  (3) `ResumeRun` now emits a durable `session.RunResumedEventKind` event
  (best-effort: a failure to append it does not fail the resume itself,
  since the claim has already committed by that point and the alternative
  -- a compensating re-pause -- is a much larger blast radius for an
  observability-only record); proven by `TestResumeRunEmitsRunResumedEvent`.
  A related, closely-adjacent bug found while testing the fixes above:
  `resumeEngine`'s freshly reconstructed `adkEngine` started with the
  zero-value `placeholderUsed=false`, even though a `TurnInterrupted` turn's
  `AssistantMessageID` placeholder *always* already carries durable content
  from before the pause (a tool interrupt or approval pause only ever fires
  from inside a dispatch the turn already committed) -- so the resumed
  continuation's first physical dispatch could reclaim that same message via
  `claimPlaceholder` and corrupt it with a second, mismatched content kind
  (e.g. `assistant_gen_text` appended onto a message that already carried
  `function_tool_call`), which a later history reload then rejected as
  `ErrMixedContentKinds`. `resumeEngine` now constructs the resumed engine
  with `placeholderUsed: true`.
- Host API (`runtime/turn_loop.go`): `Enqueue` first validates the target
  run belongs to the session and is not already terminal (fail-closed --
  **W5(b) phase-4 fix**: previously it persisted and acknowledged the item
  unconditionally, even against a run that had already settled, and nothing
  ever consumed a durably `queued` item left behind when no live loop was
  registered). It then persists a durable `session.InboxItem` via
  `EnqueueInboxForRun` (**phase 5 fix**, replacing the plain `EnqueueInbox`
  call: idempotent on key, and additionally checked -- atomically, under
  the same session-row lock a concurrent terminal `SettleRun` takes -- against
  the target run's terminal status, so a write racing the run's own terminal
  settlement either commits while the run is genuinely still active or is
  rejected with `session.ErrRunClosed` and never acknowledged) and pushes
  its ID into this process's live loop only when that call durably created
  a new row (`created`), not merely because a live loop exists -- an
  idempotent retry against a live loop is otherwise delivered twice. A loop
  this process does not own leaves the item durably `queued` for a later
  `Start`/`ResumeRun` to pick up via `drainQueuedInbox` (deliberately
  session-scoped, not run-scoped: an item accepted against a now-terminal
  run must still be picked up by a later run of the same session).

  **Phase 5 finding and fix, the idle-shutdown race**: phase 4's own fix
  above was reproduced broken three ways by the round-2 reviewers, using
  only a plain `session.Store` decorator from outside this package --
  disproving phase 4's claim that the race "depends on an ADK-internal
  timing window this package has no hook into from outside." (1) Upstream
  seals its late-item buffer the first time `TakeLateItems` is called and
  panics on any later `Push`; `runTurnLoop` used to unregister the loop in
  a `defer` that ran *after* `finishTurnLoop` (which calls `TakeLateItems`)
  -- so an `Enqueue` racing that whole window found the loop still
  registered and hit the panic. Fixed: `runTurnLoop` now unregisters the
  loop immediately after `loop.Wait()` returns, strictly before
  `finishTurnLoop` ever seals the buffer. (2) The new clean-exit
  queued-continuation pause (below) called `promoteQueuedContinuation` with
  no guard on whether upstream had staged a checkpoint; a clean idle exit
  never does (late items are not `unhandled`, so upstream's own `isIdle`
  is true), so this promoted revision 0 -- `ErrConflict` -- and left the
  run durably `running` with the heartbeat already stopped and no driver.
  Fixed: `promoteQueuedContinuation` now stages its own `Kind=loop`
  "between turns, no runner state" checkpoint (`adkCheckpointStore.stageLoopCheckpoint`)
  when nothing was staged, before promoting. (3) A buffered-but-unconsumed
  item that survives a pause (kept durably `queued`, see the next bullet)
  is restored by ADK's own checkpoint as `UnhandledItems` *and* pushed
  again by `ResumeRun`'s `drainQueuedInbox` -- nothing deduped either
  copy, so it could be admitted twice (two durable user messages for one
  inbox item) or, when the two deliveries landed in different `GenInput`
  calls, fail the resumed run outright with a store conflict. Fixed:
  `turnLoopCoordinator` now tracks every ID it has admitted
  (`admittedItems`/`claimItems`), deduping within and across `GenInput`
  calls for the loop's lifetime, and `loadInboxItems` additionally drops
  any ID whose durable state is no longer `queued` as a cross-process
  backstop.

  There is still one residual window `TakeLateItems` alone cannot close: an
  `Enqueue` whose item commits *after* the late-buffer check above but
  *before* the terminal `SettleRun` CAS actually lands. Closed at the store
  level instead (`store/internal/sqlstore/execution.go`'s `SettleRun`, both
  SQLite and PostgreSQL, since both share this code): a settlement to
  `RunCompleted` now refuses (`ErrConflict`) while its session has any
  durably `queued` inbox item, checked inside the same fenced transaction.
  With all three fixed, `finishTurnLoop`'s clean-exit path drains
  `TakeLateItems()` before ever settling `RunCompleted`, and now also
  retries once through `drainQueuedInbox` if `SettleRun` itself reports a
  conflict -- diverting to the same queued-continuation pause the
  between-turn-stop case uses whenever real queued work is found, so an
  `Enqueue` racing the loop's own `UntilIdleFor` shutdown can no longer be
  silently dropped by a
  `RunCompleted` settlement that never saw it, in any of the windows tested.
  Proven by `TestEnqueueRejectsTerminalRun`, `TestEnqueueDrainedByNextStart`
  (`runtime/acceptance_matrix_test.go`),
  `TestTurnLoopSecondEnqueuedItemSurvivesPauseAndResumeAgainstSQLite`
  (now also asserting exactly one durable user message per enqueued item),
  `TestAdmitTurnInjectedFailureRollsBackSecondTurn`, and, new in phase 5,
  `TestEnqueueRacingIdleTerminalSettlementNeverStrandsItem`,
  `TestResumeRunDedupesCheckpointRestoredAndDrainedItem`, and
  `TestEnqueueRetryAgainstLiveLoopIsNotDoubleDelivered`
  (`runtime/w5_round2_test.go`) -- the first two using store decorators
  (`settleRunGateStore`, `checkpointGateStore`) that force the exact races
  above deterministically, against a real SQLite store, disproving the
  phase-4 "no hook into from outside" claim directly.
  `Stop(ctx, runID, StopPolicy)` maps graceful/immediate
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
- Live streaming parity (`TypedAgentInput.EnableStreaming`): this was a real
  regression, not a pre-existing gap -- `TestPublicSessionWatchConstructionExecutionAndReopen`
  (`testdata/external-consumer`) passed against the classic engine
  (eino-v0-9-19 at 96f13fa) but hung/timed out against the new TurnLoop
  engine. Root cause: `turnLoopCoordinator.genInput`/`genResume`
  (`runtime/turn_loop.go`) never set `TypedAgentInput.EnableStreaming`, so
  ADK routed every physical dispatch through `adkModel.Generate` (which
  passes a `nil` live-delta callback into `dispatch`'s receive loop) instead
  of `adkModel.Stream` (which wires the session observer/`EventMessageDelta`
  publishing a watcher needs to see partial text while a provider chunk is
  still in flight, not only after the whole physical call returns) --
  unlike the classic engine, which always streamed every dispatch. Fixed by
  setting `EnableStreaming: true` on both `GenInputResult.Input`
  construction sites; `adkModel.Generate` is consequently unreachable
  through the production entry point at all now (ADK selects Stream vs.
  Generate once per run from that flag for every node in the composed
  graph, including tool-loop continuations) -- see the approval bullet
  above for what that means for approval's own Generate coverage.
- Idempotent retry (`Enqueue`'s `IdempotencyKey`): also a real, independently
  discovered bug, not specific to the new engine. `Enqueue`
  (`runtime/turn_loop.go`) mints a fresh block ID for every content block on
  every call via `assignContentBlockIDs`, including a genuine retry under
  the same `IdempotencyKey` -- but `EnqueueInbox`'s dedup check (both the
  SQLite/Postgres-shared `store/internal/sqlstore/inbox.go` and the
  in-memory test fixture) compared the *full* block slice, IDs included, so
  a real retry's freshly-minted IDs never matched the original and every
  retry spiked a false `ErrConflict` instead of returning the original
  durably-admitted item -- defeating the purpose of an idempotency key.
  Fixed with `session.ContentBlocksEqualIgnoringID`, a content-only
  equality helper, used by both the production and fixture dedup checks.
- Two engine-ordering races were root-caused and fixed rather than papered
  over, plus the two bugs above -- see `TestGracefulAndImmediateStopReachIdempotentTerminalStates`/
  `TestDuplicateEnqueueIsIdempotentOnKey` (`runtime/acceptance_matrix_test.go`).
- Turn/run usage (**W5(b) reconciliation fix**): `CompleteTurnRequest.Usage`
  was computed by the coordinator but never read by the store
  (`session.Turn` had no usage field at all), and terminal settlement took
  usage only from the *last* turn's `adkEngine`, so a multi-turn run's
  `run_finished` event under-reported every prior turn's tokens/cost.
  `session.Turn` gained a `Usage` field, written atomically by
  `ApplyCompleteTurn`/`CompleteTurn`; `turnLoopCoordinator` now accumulates a
  run-level total (`addRunUsage`/`runUsageSnapshot`) as each turn completes,
  and `finishTurnLoop`'s settlement branches use that accumulated total (plus
  the in-flight turn's own not-yet-folded usage on an interrupted/failed
  exit -- see `finishedRunUsage`) instead of a single engine's snapshot.
  Proven by `TestRunFinishedUsageSumsBothTurns` (`runtime/w5_reconciliation_test.go`):
  a two-turn run's `run_finished` usage equals the sum of both turns' own
  durably recorded usage.
- **Phase 6 (round-three reconciliation)**: the round-three reviewers
  reproduced two residual windows phase 5's own fixes narrowed but did not
  fully close, a regression phase 5 itself introduced, a genuine crash-
  recovery gap, and several documentation/test-coverage gaps.
  1. **`Enqueue`'s push-after-seal panic, closed completely**: phase 5's
     `unregisterLoop`-before-seal ordering closed the wide window, but
     `Enqueue` still called `liveLoopFor` (lock released) and then `Push`
     (unlocked) as two separate steps -- a preemption in between could still
     land a `Push` on a loop `TakeLateItems` had already sealed, panicking
     in the host's own `Enqueue` goroutine. `Enqueue` now pushes through
     `pushToLoop`, which holds `loopsMu` across the lookup and the `Push`,
     the same lock `unregisterLoop` takes strictly before the seal.
  2. **The split-delivery degenerate turn**: `runTurnLoop` pushed
     `pushIDs` *after* `loop.Run`, racing `tryLoadCheckpoint`'s
     `buffer.TakeAll()`; a losing push landed in a second `GenInput` call
     whose item `claimItems` had already admitted, and `genInput` still
     minted and dispatched a content-free continuation turn -- an extra
     assistant reply and provider charge for content already committed.
     Fixed by pushing before `loop.Run` (matching `Start`'s own sentinel
     push), which merges both deliveries into one `GenInput` batch
     `claimItems` can dedupe; `genInput` additionally refuses to admit a
     turn when the dedupe empties a batch of genuine (non-sentinel)
     duplicates, stopping the loop synchronously instead (via `loop.Stop`
     called from inside `GenInput`, which the run loop takes as license to
     discard the turn and push the raw items back to the buffer rather than
     ever call `PrepareAgent`) so the duplicate surfaces as a between-turn
     queued-continuation pause, never a second dispatch. This does NOT
     cover a cross-process duplicate that empties a batch only after
     `loadInboxItems`' own durable-state filter runs inside `admitTurn`
     (still absorbed as a degenerate, dispatching continuation turn, which
     is the correct behaviour for that specific case -- the content really
     is already committed by another process).
  3. **A phase-5 regression**: `ResumeRun`'s durable message floor was
     seeded from `o.now()` alone. That is not a safe floor -- message
     timestamps are allocated forward in nanosecond steps from a turn's own
     `now()`, so already-committed messages routinely carry times after it
     -- and could sort a host's approval response before the assistant
     message that requested it. Seeded now at
     `max(o.now(), latestAdmissionMessageTime)`, read unfenced before
     `ClaimRun` (so the "no non-transactional read inside the approval
     decision's `WithinTx`" property this seed exists for is unchanged).
  4. **Crash-recovery reconciliation** (new capability, not a fix to
     existing behaviour): a run the ADK engine was driving when its process
     crashed previously had no recovery path at all. `ClaimRun`'s existing
     lease-expiry reclaim already worked generically at the store layer,
     but `Resume` routed every non-paused run through the legacy tool-only
     resume, which has no notion of ADK turns or checkpoints -- it would
     settle the run interrupted, discarding any promoted checkpoint, and
     never touch a turn left `admitted`/`running` or its consumed inbox
     rows. `Resume` now checks whether a run was ever ADK-driven (any turn,
     or any promoted checkpoint) and, if so, reclaims and conservatively
     reconciles it instead: unfinished tool calls are terminalized (an
     unsafe running tool is never rerun, matching the existing
     `terminalizeUnfinishedTools` contract), a turn left dangling by the
     crash is settled interrupted (`ReconcileInterruptedTurn`, a new
     `session.ExecutionStore` method -- distinct from the existing
     `InterruptTurn`, which leaves inbox items `interrupted` for the
     *same*, checkpoint-resumable turn to later complete), and the run is
     then either repaused (`RepauseRun`, a new `session.ExecutionStore`
     method that reverts a claim to `paused` with no live lease -- used
     here, and also as `ResumeRun`'s own compensating write for a
     post-claim `StartRun` failure, which previously left the run
     `running` with no driver and no real recovery path despite that being
     documented as "recoverable only by lease expiry") if it has ever had
     a promoted checkpoint, or settled interrupted if it never did
     (nothing to resume). A run with no turns and no checkpoint at all
     still uses the unchanged legacy path.
     **Superseded by phase 7 item 1 (CR-C1) and phase 7's round-five
     follow-up (TR-I1)**: this phase's `ReconcileInterruptedTurn` requeued
     the dangling turn's consumed inbox items back to `InboxQueued` on the
     premise that a fresh `AdmitTurn` would need to re-consume them into a
     new turn -- see phase 7 item 1 for why that duplicated content in
     provider history, and what shipped instead (items carried forward as
     `InboxInterrupted`, never requeued; the same turn later redriven by
     `TurnID` via `ResumeInterruptedTurn`). `RepauseRun` also no longer
     unconditionally "touches no checkpoint": when reconciliation found a
     dangling turn, it now stages and promotes a fresh `Kind=loop`
     checkpoint recorded for that turn as part of the same repause, so a
     later `ResumeRun`'s `TurnID`-consistency check (round-five
     reconciliation item 2/TR-I1) reads a consistent state. **Further
     superseded by phase 8 (round-six reconciliation)**: "dangling turn" and
     "the run's newest non-completed turn" were two different selectors
     that could disagree; phase 8 unifies them into one `currentTurn`
     selector used everywhere a "what turn is this checkpoint for" decision
     is made -- see the phase-8 bullet below for the full mechanism.
  5. **Docs/test-coverage only**: the `turnLoopCheckpointShape` doc comment
     claimed a checkpoint gob-shape round-trip test that did not exist;
     `TestEmptyLoopCheckpointMatchesUpstreamGobShape` proved a round trip by
     decoding `marshalEmptyLoopCheckpoint`'s bytes into a field-identical
     mirror of eino's private `turnLoopCheckpoint` type declared in the same
     test file -- not a test that could catch a one-field rename in
     `turnLoopCheckpointShape` itself, since nothing referenced any real
     upstream type or code path (round-four reconciliation item 4/CR-I3
     found this and replaced it; see the phase-7 bullet below). The genuine
     `TakeLateItems` late-item window (an `Enqueue` reaching `Push` while
     the loop is registered but already stopped) had zero test coverage
     despite being listed as closed; `TestEnqueueDuringLateItemWindowSurvivesAsQueuedContinuation`
     now forces it deterministically with a store decorator gating
     `adkCheckpointStore.Delete`'s `ReadPromotedCheckpoint` lookup.
     `ResumeRun`'s rebuilt `config.Snapshot` still dropped `Agent.Mode` and
     `Tools.Enabled`/`Tools.Disabled` (both read on every post-resume turn,
     via `BoundedTurnMetadata.AgentMode` and `NewToolScopeContext`
     respectively) even after the system-prompt/agent-options fix two
     phases ago; both now round-trip through two more durable
     `run.Config` keys. `EnqueueInboxForRun` and `SettleRun`'s queued-inbox
     refusal were new SQL behaviours pinned by nothing but the runtime
     fixture the same commit that added them also wrote (so neither ran
     against PostgreSQL); both now have `store/storetest` contract coverage
     on the existing `inbox` suite. `settleRunRetrying` no longer burns its
     full 40x5ms retry budget on the queued-inbox conflict (a structural
     conflict only a *future* run's drain can clear, unlike the in-flight
     tool settlement conflict the retry loop exists for) --
     `session.ErrRunHasQueuedInput` (wrapping `ErrConflict`, so existing
     `errors.Is(err, session.ErrConflict)` callers are unaffected) lets it
     divert on the first attempt. The rogue-model check's "every turn must
     route at least one physical dispatch through `build.Model`" constraint
     is now stated explicitly on `AgentFactory`'s doc comment (a workflow
     factory that legitimately never dispatches on some turns would still
     fail this check today -- narrowing the predicate itself is still **not
     covered**). `messageHasParts`' doc comment now names how it
     deliberately differs from `dropUnfinalizedAssistantPlaceholders`'
     predicate (any `Part` row vs. decoded `ContentBlocks`) instead of
     letting a reader assume the two agree.
  Proven by `runtime/w5_round3_test.go`
  (`TestDuplicateDeliveryOfAlreadyAdmittedItemDoesNotDispatch`,
  `TestTurnLoopCheckpointShapeDecodesThroughRealTurnLoop` -- the round-four
  replacement for this phase's original `TestEmptyLoopCheckpointMatchesUpstreamGobShape`,
  see the phase-7 bullet below --
  `TestEnqueueDuringLateItemWindowSurvivesAsQueuedContinuation`,
  `TestSettleRunRetryingSkipsRetryOnQueuedInput`) and
  `runtime/w5_round3_reconciliation_test.go`
  (`TestResumeRunStartFailureRepauses`,
  `TestResumeReconcilesDanglingAdmittedTurnAfterCrash`,
  `TestResumeReclaimsLeaseExpiredRunningRunWithPromotedCheckpoint`), plus
  the extended `TestResumeRunRestoresSystemPromptAndAgentOptions`
  (`runtime/w5_round2_test.go`) and the new `store/storetest/w5_durable.go`
  contract cases.
- **Phase 7 (round-four reconciliation)**: a fourth dual-review pass
  against phase 6's own fixes -- `reviews/w5-engine-2026-09-11-013808-482da09897eb/fix-pass-3/reconciliation.md`,
  items 1-9, all accepted -- found one Critical defect phase 6's own new
  crash-recovery path introduced, plus important-severity gaps in the
  duplicate-delivery guard, the reconciliation goroutine's protections, the
  checkpoint gob-shape test's honesty, the reconcile path's `Handle`
  contract, and unmet "done when" criteria on two round-three tests.
  1. **Crash reconciliation no longer duplicates history (Critical,
     CR-C1)**: `ReconcileInterruptedTurn` requeued a crashed turn's
     consumed inbox items back to `InboxQueued` on the premise that a fresh
     `AdmitTurn` would need to re-consume them into a new turn. That premise
     was wrong -- `AdmitTurn` already durably commits the turn's user
     messages atomically with the inbox consumption, so requeuing let the
     next admission mint a *second* user message from the same content,
     duplicating it in provider history on every crash recovery of a turn
     that carried content. `ReconcileInterruptedTurn` now settles those
     items `InboxInterrupted` instead, exactly like `InterruptTurn` --
     never requeued. A new fenced `ExecutionStore` op, `ResumeInterruptedTurn`
     (with a `store/storetest` contract case on both stores), later resumes
     the SAME turn under the SAME `TurnID`: it moves the turn to
     `TurnRunning` and its inbox rows back to `InboxConsumed`, so
     `CompleteTurn` (which already accepted `TurnRunning`) settles it
     completed with the item completed exactly once. A later crash
     mid-redrive leaves the turn `TurnRunning`, which
     `reconcileCrashedRun`'s existing dangling-turn detection
     (`TurnAdmitted || TurnRunning`) already recognizes, so the cycle is
     safely repeatable. On the runtime side, `ResumeRun` decodes the
     promoted checkpoint's own payload
     (`decodeLoopCheckpointHasRunnerState`) before ever calling `loop.Run`
     to determine whether this resume will reach `GenInput` or `GenResume`;
     when it has no real ADK runner state and the run has a pending
     crash-reconciled turn with real content, it pushes a new
     `reconciledTurnSentinelID` so `genInput` resumes that exact turn
     (`resumeReconciledTurn`) instead of admitting a fresh one. A crash with
     no promoted checkpoint at all settles the run interrupted with the
     user's text left in history exactly once, unanswered by that
     particular run (the conservative outcome: nothing automatically
     produces a reply to it).
  2. **The duplicate-delivery guard now actually removes the spurious
     dispatch (Important, CR-I1)**: the guard stopped the loop and pushed
     raw items back, but a stale id restored into a *fresh* coordinator
     (across a checkpoint or a process restart) was not recognized by that
     coordinator's in-process `admittedItems` dedup, so `admitTurn` still
     ran and minted a content-free turn that dispatched the model again --
     deferring, not removing, the dispatch the guard exists to prevent.
     `genInput` now also checks durable inbox state before calling
     `admitTurn`, and `finishTurnLoop` filters `UnhandledItems`/late items
     against durable state (`queuedAmong`) before deciding to pause: when
     nothing is genuinely queued, it settles the run normally
     (`settleCleanRunCompletion`, shared with the default clean-exit path)
     instead of promoting a pointless pause-carrier turn.
  3. **`reconcileCrashedRun` has the same protections every other run
     driver has (Important, CR-I2)**: it ran in a bare goroutine with no
     panic recovery, no observed-run span, no `RunSettledNotice` on
     terminal settlement, and no lease heartbeat across its writes.
     `reclaimAndReconcile` now drives it through `runReconcileCrashedRun`:
     a deferred `recover()` converts a panic into `RunFailed` instead of
     taking down the host process, `startObservedRun`/`finishObservedRun`
     bracket the work, a lease heartbeat covers the writes, and
     `RunSettledPoint` is notified on every genuinely terminal outcome
     (never the nonterminal repause). It is deliberately not routed through
     the shared `executeLifecycle` helper, whose own deferred `settleRun`
     would double-settle a run this function already settled itself.
  4. **The checkpoint gob-shape test now decodes through a real
     `adk.TurnLoop` (Important, CR-I3)**: the round-three test (and this
     doc, both corrected here) claimed
     `TestEmptyLoopCheckpointMatchesUpstreamGobShape` "fails loudly on an
     upstream rename." It could not -- it decoded `turnLoopCheckpointShape`'s
     own bytes into a hand-written duplicate declared in the same test
     file, and a one-field rename still passed.
     `TestTurnLoopCheckpointShapeDecodesThroughRealTurnLoop`
     (`runtime/w5_round3_test.go`) replaces it: it gob-encodes a non-zero
     shape, stages it behind a minimal `adk.CheckPointStore`, and drives a
     real `adk.NewTurnLoop` through `Run` -- eino's own
     `unmarshalTurnLoopCheckpoint` + `tryLoadCheckpoint` -- asserting
     `GenInput` receives exactly the encoded `UnhandledItems`.
  5. **`Resume`'s reconcile path honours the `Handle` contract (Important,
     CR-I4)**: its `streamingHandle.AwaitPause()` returned a `nil` channel
     that never delivers or closes, violating `Handle`'s documented
     contract now that a `RunPaused` result is reachable from it. The
     handle is now a `turnLoopHandle` (the same implementation `Start`/
     `ResumeRun` use), whose real pause channel is closed without a value
     on a terminal outcome and carries `PauseInfo{RunID, StopCause:
     "crash-reconciled"}` on a repause.
  6. **The resumed-config test now observes `ResumeRun`'s real snapshot
     (Important, MR-1)**: reverting `ResumeRun`'s `Agent.Mode`/
     `Tools.Enabled`/`Tools.Disabled` restoration left `go test ./runtime`
     green, because `TestResumeRunRestoresSystemPromptAndAgentOptions`
     asserted against a `TurnSnapshot` it built itself rather than
     `ResumeRun`'s own rebuilt config. It now records the REAL
     `BoundedTurnMetadata` (via a `TurnPreparePoint` hook) and the REAL
     `ToolScopeContext` (via the "gate" tool capability's own `Resolve`)
     a prepared turn gets, pre- and post-resume, and compares the last
     recorded value against the first and against the audited config.
  7. **The plan's full process-restart acceptance bullet is now driven for
     real (MR-2/MR-3)**: see the phase-7 acceptance-test-matrix additions
     below.
  8. **The durable guard, not the dispatch count, is the actually-binding
     constraint on a workflow-pattern `AgentFactory` (docs only)**: see the
     `AgentBuildContext` bullet above, corrected here -- `Guard` is an
     `adk.TypedChatModelAgentMiddleware`, installable only inside a
     `TypedChatModelAgent`, so a factory returning a pure workflow/
     sequential agent with no such child fails `onAgentEvents`' guard check
     before the dispatch check is ever consulted; narrowing the dispatch
     predicate (as previously suggested) would not have lifted this.
  9. **Docs corrected to match**: this section and the acceptance-matrix
     phase-6 paragraph no longer claim the duplicate-delivery guard
     "surfaces as a between-turn queued-continuation pause, never a second
     dispatch" (item 2 shows the dispatch was deferred, not removed, before
     this phase's fix), no longer claim the phase-6 tests alone proved the
     process-restart acceptance bullet (item 7), and the gob-shape test's
     doc comment states plainly what it now proves (item 4) instead of a
     claim the previous version could not support.
  Proven by `runtime/w5_round3_reconciliation_test.go`
  (`TestReconcileCrashedRunSurvivesPanic`, the extended
  `TestResumeReconcilesDanglingAdmittedTurnAfterCrash` and
  `TestResumeReclaimsLeaseExpiredRunningRunWithPromotedCheckpoint`, both now
  admitting real `UserParts` and asserting the user's text appears exactly
  once in committed history), the extended
  `TestDuplicateDeliveryOfAlreadyAdmittedItemDoesNotDispatch` and the new
  `TestTurnLoopCheckpointShapeDecodesThroughRealTurnLoop`
  (`runtime/w5_round3_test.go`), the extended
  `TestResumeRunRestoresSystemPromptAndAgentOptions`
  (`runtime/w5_round2_test.go`), the new
  `runtime/w5_round4_acceptance_test.go`
  (`TestProcessRestartRecoversMultipleQueuedInputsAfterFirstCommittedTurn`,
  `TestTargetedMultiLeafResumeLeavesUntargetedLeafPaused`,
  `TestReconcileCrashedRunTerminalizesUnfinishedToolCall`), and the new
  `store/storetest/w5_durable.go` `ResumeInterruptedTurn` contract case.
- Verification actually run for this phase: `go build ./...`, `go vet ./...`,
  `go vet -tags postgres_integration ./...`, `gofmt`/`goimports`, and
  `./.bin/golangci-lint run ./...` (repo-wide, 0 issues) all pass.
  `go test ./...` passes for every package, `runtime` included (0 known
  failures). `go test ./runtime -race -count=3` and several additional full
  `-race` passes (including the two fixture/timing-sensitive tests singled
  out above, stress-tested individually at `-count=30`-`50`) are clean.
  `make check` passes in full, including `external-consumer-check`.
  `TESTCONTAINERS_RYUK_DISABLED=true GOMAXPROCS=2 GOFLAGS='-p=1' make
  postgres-test` and `make postgres-race` both pass ("required suites
  passed; zero skips"); `EINO_AGENT_CONSUMER_POSTGRES=1
  TESTCONTAINERS_RYUK_DISABLED=true testdata/external-consumer/check.sh`
  also passes in full. `TestPostgresRuntime/admission`
  (`runtime/postgres_admission_integration_test.go`) needed its event-count
  assertion updated from 2 to 4 events in sequence (`run_started`,
  `turn_started`, `turn_completed`, `run_finished`) to match the durable
  turn model a completed run now legitimately records -- confirmed
  pre-existing staleness in the test itself, not a new gap (unlike the
  streaming regression above, this one really did predate this phase's
  work). `POSTGRES_REQUIRED_SUITES` (`Makefile`) already listed the
  turn/inbox/checkpoint/paused-run store contract suites
  (`store/contract/{turns,inbox,checkpoints,paused_runs}`) as of phase 4;
  **phase 5 correction**: that was true of the suite *list*, but misleading
  about *coverage* -- `store/storetest/w5_durable.go`'s `pausedRunContract`
  only exercised `PromotePause` with a valid inbox ID, with no case for an
  unknown ID or one in a state that is neither `queued` nor `consumed`
  (exactly the behavior `runtime/admission_store_test.go`'s fixture was
  hand-aligned to, and exactly the divergence class that let phase-4's own
  round-one Critical 1 ship green in the first place). `pausedRunContract`
  now includes "promote pause rejects an inbox id with no durable row"
  (asserting `ErrConflict` and a clean rollback), run on both SQLite and
  PostgreSQL via the existing suite -- no `Makefile` change needed for it.
  Phase 5 did add one new required suite:
  `runtime:TestPostgresRuntime/turn_loop_pause_resume`
  (`runtime/postgres_turnloop_integration_test.go`), the production
  TurnLoop pause/promote/resume protocol proven against real PostgreSQL,
  not only SQLite.
  **Phase 4 (W5(b) reconciliation) verification**, re-run in full after the
  fixes above: `gofmt`/`goimports` clean; `go build ./...`,
  `go vet ./...`, `go vet -tags postgres_integration ./...`, and
  `./.bin/golangci-lint run ./...` all pass with 0 issues; `go test ./...`
  passes for every package; `go test ./runtime -race -count=3` and a
  combined `-race -count=10` pass over every new/changed focused test named
  in the bullets above are clean; `make check` passes in full (including
  `external-consumer-check`, local mode); `TESTCONTAINERS_RYUK_DISABLED=true
  GOMAXPROCS=2 GOFLAGS='-p=1' make postgres-test` and `make postgres-race`
  both report "required suites passed; zero skips"; and
  `EINO_AGENT_CONSUMER_POSTGRES=1 TESTCONTAINERS_RYUK_DISABLED=true
  testdata/external-consumer/check.sh` also passes in full.
  **Phase 5 (round-2 reconciliation) verification**, re-run in full after
  the phase-5 fixes above, same full gate list as phase 4 (`gofmt`/
  `goimports`; `go build ./...`; `go vet ./...` and `go vet -tags
  postgres_integration ./...`; `./.bin/golangci-lint run ./...` at 0
  issues; `go test ./...` for every package; `go test ./runtime -race
  -count=3`, plus `-race -count=10` over the new phase-5 focused tests in
  `runtime/w5_round2_test.go` and the updated
  `TestTurnLoopSecondEnqueuedItemSurvivesPauseAndResumeAgainstSQLite`;
  `make check`; `TESTCONTAINERS_RYUK_DISABLED=true GOMAXPROCS=2
  GOFLAGS='-p=1' make postgres-test` and `make postgres-race`;
  `EINO_AGENT_CONSUMER_POSTGRES=1 TESTCONTAINERS_RYUK_DISABLED=true
  testdata/external-consumer/check.sh`) -- all green; see this phase's
  fix-pass commits for the exact recorded run.
  **Phase 6 (round-three reconciliation) verification**, same full gate
  list as phases 4-5 (`gofmt`/`goimports`; `go build ./...`; `go vet ./...`
  and `go vet -tags postgres_integration ./...`; `./.bin/golangci-lint run
  ./...` at 0 issues; `go test ./...` for every package; `go test
  ./runtime -race -count=3`, plus `-race -count=10` over the new phase-6
  focused tests in `runtime/w5_round3_test.go` and
  `runtime/w5_round3_reconciliation_test.go`; `make check`;
  `TESTCONTAINERS_RYUK_DISABLED=true GOMAXPROCS=2 GOFLAGS='-p=1' make
  postgres-test` and `make postgres-race`; `EINO_AGENT_CONSUMER_POSTGRES=1
  TESTCONTAINERS_RYUK_DISABLED=true testdata/external-consumer/check.sh`)
  -- all green; see this phase's fix-pass commits for the exact recorded
  run.
- **Phase 8 (round-five and round-six reconciliation)**: round five
  (`reviews/w5-engine-2026-09-11-013808-482da09897eb/fix-pass-4/reconciliation.md`)
  gave the checkpoint envelope a required `TurnID`
  (`decodeCheckpointEnvelope` rejects one without it -- no codec-version
  bump needed, the envelope had never shipped), made `genInput` scan the
  *whole* GenInput batch for `reconciledTurnSentinelID` rather than only
  `items[0]` (upstream's `tryLoadCheckpoint` builds that batch as
  `cp.UnhandledItems ++ newItems`, and a promoted checkpoint carrying its
  own `UnhandledItems` -- the between-turn-stop-with-queued-input shape --
  puts those ids ahead of the sentinel), and gave `RepauseRun` a
  `PromoteRevision` parameter. It also made `completeTurn` retire its own
  promoted checkpoint immediately on completion and made `ResumeRun` refuse
  a promoted checkpoint whose `TurnID` disagreed with the run's "newest
  non-completed turn". Round six
  (`reviews/w5-engine-2026-09-11-013808-482da09897eb/fix-pass-5/reconciliation.md`)
  found that pairing unsound: crash reconciliation's own "dangling turn"
  selector (highest-ordinal `admitted`/`running` turn) and `ResumeRun`'s
  "newest non-completed turn" selector are not the same predicate --
  `TurnInterrupted` satisfies the second and not the first -- so a run
  holding a lower-ordinal dangling turn behind a higher-ordinal
  already-interrupted carrier (exactly what a graceful `Stop` with queued
  input produces) made reconciliation stage a checkpoint `ResumeRun` then
  refused, permanently (checkpoint-precedence-reviewer Critical 1).
  Separately, `completeTurn`'s immediate retirement destroyed the
  "this run is resumable" signal reconciliation's `hasCheckpoint` gate
  depended on: once a resumed run's turn completed, its promoted checkpoint
  was gone, so a crash on any *later* turn of that same run read as "never
  paused" and settled the run terminally interrupted with the later turn's
  content stranded (checkpoint-precedence-reviewer Critical 2). And because
  the retirement is best-effort (its own error deliberately swallowed) and
  runs in a window after `CompleteTurn` commits, a crash landing in that
  exact window left a promoted checkpoint recorded for a now-completed turn
  with no dangling turn to reconcile; reconciliation repaused over it
  unchanged, and `ResumeRun`'s own consistency check then refused it
  forever, with `Resume` routing straight back into the same refusal and
  `Start` failing `ErrSessionBusy` because the run stayed nonterminal --
  bricking the whole session with no operator escape at all
  (branch-approval-reviewer Critical 1). Round six replaces the pairing
  with one coherent design instead of patching each symptom separately:
  - `currentTurn(turns)` (`runtime/turn_loop.go`) is now the single
    selector shared by crash reconciliation, `RepauseRun`/`PromoteRevision`
    staging, and `ResumeRun`'s checkpoint-`TurnID` consistency check: the
    newest turn whose state is not `completed`/`failed` --
    `admitted`/`running`/`interrupted` all qualify. `ResumeRun` keeps its
    consistency check as a defensive assertion (a genuinely inconsistent
    state it can still catch), but the round-six tests prove it can no
    longer fire after reconciliation has run.
  - `completeTurn` no longer retires its own promoted checkpoint on
    completion. A checkpoint recorded for a since-completed turn is stale
    *by fact* (`currentTurn` no longer selects that turn), not absent by an
    explicit mid-run delete; retirement now happens at terminal settlement
    (`settleCleanRunCompletion`, `abandonPausedRun`), and -- on any clean
    loop exit that loaded a checkpoint -- through upstream's own `Delete`
    cleanup, which this adapter implements as `RetireCheckpoints` under the
    still-live fence. `reconcileCrashedRun` (`runtime/interrupt.go`) never
    retires anything itself: it supersedes a stale revision with a fresh
    promoted one instead.
  - Crash reconciliation (`reconcileCrashedRun`, `runtime/interrupt.go`) is
    now total: whenever a run's checkpoint machinery has ever been used
    (`hasCheckpoint`), it always repauses into a state `ResumeRun` can
    accept, never settles terminal underneath a still-resumable run. When
    nothing is currently in flight (`currentTurn(turns).ID == ""` -- e.g. a
    crash right after `CompleteTurn`, mirroring branch-approval-reviewer's
    C1 shape), it mints a fresh, content-free carrier turn purely to hold a
    valid pause (the same degenerate shape `promoteQueuedContinuation`
    already used between turns), so a checkpoint envelope -- whose `TurnID`
    can never be empty -- always has a real turn to name; `ResumeRun` then
    finds nothing left to redrive or drain and completes the run on its
    own. A run whose checkpoint machinery was never used at all (no
    promoted checkpoint ever existed, `hasCheckpoint` false) still settles
    interrupted with nothing to resume, unchanged from phase 6.
  - `StopPolicy.Abandon` ("Stop-with-abandon", `runtime/turn_loop.go`) is
    new: a documented operator escape for a durably paused run this process
    has no live loop for (whether refused by `ResumeRun`'s defensive check
    or simply one nobody intends to resume) -- it settles the run
    terminally `RunInterrupted`, retires its checkpoints (terminalizing any
    unfinished tool call first, since a genuine tool-interrupt pause's
    durable `ToolCall` row can still be `pending`), and frees the session
    for a fresh `Start`. `Stop` reports `ErrInvalidOrchestrator` instead of
    honoring `Abandon` when this process has a live loop for the run
    (`Graceful`/`Immediate`, or `Handle.Interrupt`, already cover that case;
    silently downgrading to an ordinary stop would return `nil` while
    leaving the run nonterminal -- round-seven fix-pass-6 item 1/MG-I1).
  Proven by `runtime/w5_round6_reconciliation_test.go`:
  `TestResumeRunAfterGracefulStopWithQueuedInputSurvivesCrashAgainstSQLite`
  (checkpoint-precedence-reviewer probe D: `Start`+`Enqueue`+graceful
  `Stop`, crash, `Resume` repauses, `ResumeRun` completes with the queued
  item consumed exactly once),
  `TestResumeRunRepausesAndRedrivesOnlyInFlightTurnAfterCrashAgainstSQLite`
  (probes A/B: a genuine tool-interrupt pause resumed and completed, a
  second turn admitted then crashed, `Resume` repauses -- not terminal --
  and `ResumeRun` redrives only the in-flight second turn, the first
  turn's completion untouched),
  `TestResumeRunAcceptsAfterCrashRightAfterCompleteTurnWithStaleCheckpointAgainstSQLite`
  (branch-approval-reviewer's C1 probe: crash immediately after
  `CompleteTurn`, `Resume` repauses via a fresh carrier turn, `ResumeRun`
  accepts and completes the run), and
  `TestStopAbandonSettlesPausedRunAndFreesSessionForNewStart` (a paused run
  abandoned reaches terminal `RunInterrupted` with checkpoints retired, and
  a fresh `Start` on the same session then succeeds). The round-five test
  that drove real `Start`/`Enqueue`/`ResumeRun`
  (`TestCompleteTurnRetiresItsOwnPromotedCheckpointImmediately`,
  `w5_round5_reconciliation_test.go`) is removed -- it asserted the
  now-removed immediate-retirement behavior directly;
  `TestResumeRunRedrivesReconciledTurnPastStaleCheckpointUnhandledItems`
  (the round-five test that hand-wrote an envelope naming a turn ahead of
  when it existed and hand-rolled reconciliation instead of calling
  `reconcileCrashedRun`) is relabeled a synthetic unit test of `genInput`'s
  sentinel scan alone (the `UnhandledItems`-plus-sentinel combination it
  constructs is unreachable through the real reconciliation path, since a
  real `Kind=loop` checkpoint is only ever staged by `stageLoopCheckpoint`'s
  always-empty shape); and
  `TestResumeRunRefusesCheckpointForATurnThatHasSinceCompleted` gained a
  `StopPolicy.Abandon` assertion proving the refusal it exercises has a
  documented way out. `adkCheckpointStore.currentTurnID` is now seeded
  synchronously by both `Start` (`orchestrator.go`) and `ResumeRun`
  (`turn_loop.go`), before their `TurnLoop` is even constructed, so `stage`
  can never observe an empty `currentTurnID` ahead of the first
  `GenInput`/`GenResume` call (branch-approval-reviewer S3);
  `TestAdkCheckpointStoreStageSucceedsOnceSeededWithNoPriorGenInput`
  (`w5_round6_reconciliation_test.go`) proves it deterministically (a live
  race against a real `TurnLoop` was not reproducible even after 120
  iterations in the reviewer's own probe, so racing it in CI would be
  equally flaky and not actually discriminate a regression).
- **Phase 9 (round-seven reconciliation)**: `Stop{Abandon: true}` refuses
  with `ErrInvalidOrchestrator` while a loop is live in-process (abandon a
  run only after it has paused; `TestStopAbandonAgainstLiveLoopReportsErrInvalidOrchestrator`).
  Terminal settlement now terminalizes turns: `SettleRun` forces every
  turn still admitted/running/interrupted (including content-free carrier
  turns) to `TurnFailed` via `session.ApplyFailTurn` in the same fenced
  transaction, carrying consumed/interrupted inbox rows forward as
  `interrupted`; `TurnFailed` therefore has a writer, and no turn is left
  non-terminal under a terminal run (storetest `paused_runs`/turn contract
  on both stores). `resumeStartFailureRepause` delivers a `PauseInfo` on
  `AwaitPause` whenever `Done` reports `RunPaused`. Source-visible interface
  growth for embedders: `runtime.IDGenerator` gained `NewTurnID`,
  `NewInboxID` and `NewInvocationID`; `runtime.Handle` gained `AwaitPause`
  and `Status`; `session.Store`/`ExecutionStore` gained the turn, inbox,
  checkpoint, repause and resume-interrupted-turn operations (see
  `docs/consumer-guide.md`). Known follow-up, not a W5 defect: provider-
  supplied tool-call ids are used verbatim as the store-wide unique
  `tool_calls.id`, so a provider that reuses ids across responses (indexed
  `call_0`-style ids) fails a later turn with a clean store conflict;
  tracked as a separate bead.
- Acceptance-test matrix (`runtime/acceptance_matrix_test.go`,
  `runtime/turn_loop_sqlite_test.go`, `runtime/w5_reconciliation_test.go`,
  `runtime/w5_round2_test.go`, `runtime/w5_round3_test.go`,
  `runtime/w5_round3_reconciliation_test.go`,
  `runtime/w5_round4_acceptance_test.go`; focused cases clean under
  `-race -count=10`): checkpoint envelope
  malformed-rejection (empty/non-JSON/missing-required-field), checkpoint
  row version mismatch, plan-fingerprint mismatch, and a `model.Resolve`
  failure all rejected by `ResumeRun` *before* claiming the run's fence, and
  proven to leave the run `RunPaused` (not just that `ResumeRun` itself
  errors), a checkpoint `Set` failure leaving the run `RunRunning` for
  lease-expiry recovery rather than promoting or corrupting state, two
  concurrent `ResumeRun` calls against the same paused run yielding exactly
  one owner (via `ClaimRun`'s CAS) with no duplicate tool execution,
  `Enqueue` idempotency-key replay (now against a paused, not a completed,
  run -- `Enqueue` fails closed against a terminal run), `Enqueue` rejecting
  a terminal run outright and a `queued` item left with no live loop being
  drained and completed by the next `ResumeRun`, an injected `CompleteTurn`
  write failure failing the run without leaving the durable turn looking
  completed, an injected `AdmitTurn` write failure on a run's second turn
  failing the run without consuming the durable inbox item it would have
  claimed, graceful vs. immediate `Stop` (graceful lets an in-flight tool
  call finish without ever canceling its own context; immediate reaches a
  terminal result without waiting for it), a first-turn pause/resume and a
  between-turn `Stop` landing before the very first dispatch then resuming
  to completion against a real SQLite store, a second `Enqueue`d item
  surviving a pause and completing as the run's second turn after resume, a
  tool-batch sibling settling correctly past a `ToolInterruptPolicy` pause
  and past `tool_search` declared first, a factory that never installs the
  durable guard failing as a construction error, an approval decision
  surviving a transient dispatch failure via ADK's own retry with exactly
  one committed response block, a two-turn run's `run_finished` usage
  summing both turns, and `ResumeRun` emitting a durable `run_resumed`
  event. **Phase 5 additions** (`runtime/w5_round2_test.go` unless noted):
  an `Enqueue` racing a run's own idle-shutdown terminal settlement,
  against a real SQLite store with a `settleRunGateStore` decorator forcing
  the exact window, ending paused-with-the-item-queued (never a panic,
  never `RunCompleted` with the item stranded) and that pause's
  `ResumeRun` actually completing it; a checkpoint-restored and a
  drain-pushed copy of the same inbox item, forced into one `GenInput`
  batch via a `checkpointGateStore` decorator, admitted exactly once (not
  twice); an idempotent `Enqueue` retry against a still-live loop pushing
  its ID at most once; a factory that installs the durable guard but
  substitutes its own model still failing the turn (zero durable
  dispatches recorded); the approval decide-then-continue transaction
  completing against a real SQLite store within a bounded timeout (proving
  no non-transactional-read deadlock/hang hazard); and a resumed
  dispatch's `model.Request.System`/agent options equaling the fresh-run
  values, with `run.Config["system_prompt"]` asserted durable. **Phase 6
  additions** (see the phase-6 bullet above for the full mechanism):
  `TestResumeReconcilesDanglingAdmittedTurnAfterCrash` and
  `TestResumeReclaimsLeaseExpiredRunningRunWithPromotedCheckpoint`
  (`runtime/w5_round3_reconciliation_test.go`) prove conservative
  reconciliation of a turn (and its consumed inbox items) a crashed process
  left `admitted`/`running`, whether that is the run's first turn (settles
  interrupted, nothing to resume) or a later one following an earlier
  successful pause (repauses, genuinely resumable -- proven by a further
  `ResumeRun` actually completing it). **This pair alone did not cover the
  plan's full acceptance bullet** (round-four reconciliation MR-2/MR-3):
  both hand-build the crashed state directly via `AdmitTurn`/`StageCheckpoint`/
  `PromotePause`, with one queued item and no turn genuinely driven to
  completion by the engine -- see the phase-7 bullet below for the test
  that actually closes it. **Phase 7 (round-four reconciliation) additions**
  (see the phase-7 bullet below for the full mechanism):
  `TestProcessRestartRecoversMultipleQueuedInputsAfterFirstCommittedTurn`
  (`runtime/w5_round4_acceptance_test.go`) drives the plan's acceptance
  bullet for real -- a real `Start` completes turn 1, two more messages are
  enqueued and land as a genuine between-turn queued-continuation pause, the
  first orchestrator's SQLite pool is closed and a second pool reopened over
  the same file, and a brand-new orchestrator instance resumes and completes
  the run -- asserting correct reconstructed history, exactly-once inbox
  completion, no duplicate `run_started`, and exactly one `run_finished`.
  `TestTargetedMultiLeafResumeLeavesUntargetedLeafPaused` proves a targeted
  resume of one of two simultaneously-pending tool interrupts leaves the
  other paused untouched (no new dispatch), then resumes it too by its
  current-generation address.
  `TestReconcileCrashedRunTerminalizesUnfinishedToolCall` injects a crash at
  a tool-settlement boundary (a durably-claimed, never-settled tool call)
  recovered through `reconcileCrashedRun`'s `terminalizeUnfinishedTools`
  call in the same reconciliation that settles the dangling turn that
  requested it; checkpoint-boundary crash injection is covered by the
  phase-6 dangling-turn tests above (a crash before any checkpoint for the
  dangling turn was ever staged, and a crash after an earlier checkpoint was
  promoted but before a new one was). The residual gap phase 5 already named
  -- `loadInboxItems`' durable-state filter (inside `admitTurn`, after
  `claimItems` has already committed to a nonempty filtered set) can still
  admit a degenerate, dispatching continuation turn for a cross-process
  duplicate -- remains genuinely uncovered; item 2's fix closes only the
  in-process split (see the phase-6 bullet) and the checkpoint-restored
  case (see the phase-7 bullet). Reconciling more than one
  simultaneously-dangling turn for the same run is not exercised (ordinary
  operation never admits a second turn before the first settles, so this
  is believed unreachable, not merely untested) and multiple, back-to-back
  crashes across several resumes are not exercised beyond one. Still out of
  scope, unchanged from phase 5: stop-mode timeout escalation, recursive
  cancel, preempt (in particular "a late preempt targets only the captured
  turn"), idle exit specifically, checkpoint promotion-failure and
  terminal-delete-failure specifically (only `Set` failure is covered), a
  stale fence on resume beyond the concurrent-`ResumeRun` case already
  covered, verifying an `AgentFactory`'s returned agent's tool/model
  identity at construction time rather than post-hoc (see the
  `AgentBuildContext` bullet above), narrowing the rogue-model check's
  zero-dispatch predicate for a legitimately-non-dispatching workflow
  factory (documented as a constraint instead -- see the phase-6 bullet),
  explicit `release()`-count and goroutine-cleanup assertions on every exit
  path (no test does this -- `grep -rn 'goleak\|NumGoroutine\|releaseCount'
  runtime/` returns nothing), and the two ADK stop safe points
  (`CancelAfterChatModel`/`CancelAfterToolCalls`) exercised separately
  rather than only as the combined pair `runtime.StopPolicy{Graceful: true}`
  always requests (the public API has no per-safe-point control to test
  against, only graceful-vs-immediate, which is covered),
  and ADK's own gob-encoded payload being unsupported/malformed (this
  adapter only validates its own envelope wrapper around that opaque
  payload -- see the checkpoint envelope bullet -- ADK's own gob decode of
  `envelope.Payload` is not exercised here). "An unsafe running tool is
  never rerun on resume" is
  covered by the existing
  `TestStreamingOrchestratorResumeDoesNotReexecuteRunningTool`
  (`runtime/orchestrator_resume_test.go`): `adkTool.InvokableRun`'s
  `case record.Status == session.ToolCallRunning:` path and the legacy
  `Resume` path both terminalize through the same
  `settleInterruptedRunningTool` helper that test exercises, so no
  dedicated new-engine-specific test was added.
- Out of scope, not started: child agents (typed `AgentTool`/`DeepAgent`),
  W6/W7. The superseded classic `PartKind`s were removed in W5 phase 2 (see
  the W2 section's "superseded part kinds" note); a WIT/bindings reference
  to those kinds' string values was not found in `wit/eino-agent-extensions.wit`
  or `wasmext/gen`, so no W6 follow-up was required for this removal.
