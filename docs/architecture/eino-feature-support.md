# Eino Feature Support

This document is the maintained capability and verification matrix for the
CloudWeGo Eino dependency. It replaces the planning inventory that produced the
v0.9.19 adoption and is updated whenever a package of that adoption lands.

## Row-by-row capability matrix (`01-feature-inventory.md`)

`.agents/plans/eino-v0-9-19/08-execution-handoff.md`'s Definition of done item
1 requires: "Every capability row in `01` has concrete passing tests or the
specified direct-upstream example; no unclassified changed public file
remains. No row may silently become deferred work." The narrative sections
below (W1-W8) answer that question, but item 1 asks it row-by-row against
`01-feature-inventory.md`'s 27 data rows, and nothing before this table
answered it in that shape -- some rows conceded out of scope mid-narrative
(the W5 section, `:1732-1735`/`:1761` in an earlier revision) were invisible
to anyone checking item 1 from here. This table is that direct answer, built
from a full row-by-row audit (2026-09-13). **COVERED** means a named test (or
the specified upstream example) asserts what the row's acceptance text
claims. **PARTIAL** names the specific unmet clause. **NOT COVERED** gives the
reason and, where one exists, the tracking bead. A row is never marked
COVERED by a test that does not assert what the row claims.

| # | Capability delta | Status | Evidence / gap |
| --- | --- | --- | --- |
| 1 | AgenticMessage/AgenticModel and typed roles | COVERED | `TestPublicNativeAgenticModelGenerateStreamEquivalenceAndContinuation` (`testdata/external-consumer/agentic_fixture_test.go`) and `TestNativeAgenticProviderStateReasoningSignatureRestoresAcrossReopenWithoutLeakage` (`runtime/native_provider_state_test.go`): real native adapter I/O and SQL reopen with no flattening. |
| 2 | All 20 content block kinds and structured function results | PARTIAL | SQL boundary: all 20 kinds proven per-kind (`Makefile`'s `POSTGRES_STORE_CONTENT_KIND_SUITE`, `TestPostgresStore/contract/durable_rich_content`). Model boundary: only 6 kinds (+1 conditional) cross `runtime/adk_model.go`'s assistant-output allowlist; 5 kinds fail the turn closed via `errADKUnsupportedBlock`. Transport boundary: no per-kind matrix exists yet (bead `eino-agent-nsv`). |
| 3 | OpenAI reasoning/refusal/citations/response state, Claude citations/stop, Gemini grounding | PARTIAL | Typed public/private fixtures exist for all three providers (`session/content_test.go`, `TestContentPrivateSplitNeverLeaks`). Native continuation after reopen is proven only for Claude (`TestPublicNativeAgenticModelGenerateStreamEquivalenceAndContinuation`); OpenAI/Gemini native continuation is unverified. |
| 4 | Generic BaseModel and typed ADK agent/input/output/event/message variants | PARTIAL | Real `TypedChatModelAgent`, runner and durable IDs covered (`runtime/adk_approval_production_test.go`). Child-agent calls: not started (see W5 section). |
| 5 | Agentic tool choice, forced/allowed function/MCP/server selectors | COVERED | `TestAgenticCallOptionsExactSetAndOrder` and `TestAgenticStreamerValidationFailsBeforeDispatch` (`model/agentic_streamer_test.go`): exact call-time option set/order and duplicate-name validation failing before dispatch. `TestValidateAllowedToolListRejectsInvalidSelectors` and `TestValidateAllowedToolListAcceptsEachSelectorKind` (`model/agentic_options_test.go`, added by this fix pass): `validateAllowedToolList`'s unknown-function rejection, one-of-three rule, and MCP/server required-field rules, for both `Allowed` and `Forced` tool choice -- mutation-verified (replacing the function body with `return nil` fails all seven rejection cases). |
| 6 | Deferred tools, tool-search tool, ToolSearchResult serialization | COVERED | `TestPublicToolSearchDiscoversDeferredToolThenAliasExecutes` (`testdata/external-consumer/agentic_fixture_test.go`), corrected by this fix pass to actually assert deferral (call 1 does not offer the tool; call 2, after discovery, does) rather than only naming it, covers "discover then invoke permitted tool". The other two acceptance clauses (`01-feature-inventory.md:16`) are covered separately, just previously uncited: "checkpoint/reopen preserves definitions" by `TestOrchestratorSearchResultReferencesOnlyFrozenRegistryTools` (discovered names are scoped to the frozen plan registry) and `TestStreamingOrchestratorResumesPendingToolSearchCall` (a pending search call resumes and settles correctly after execution interruption) (`runtime/w4_acceptance_test.go:673`, `:904`); "unauthorized discovery fails" by `TestNewRunPlanDisablesToolSearchWhenRestrictionDeniesSearchName` (a restriction denying the search tool's own name disables `ToolSearch` on the plan) (`runtime/w4_acceptance_test.go:773`). |
| 7 | ToolsNode name/argument aliases; dynamic list overrides | PARTIAL | Canonicalization, collision validation and settlement identity covered (`runtime/w4_acceptance_test.go`). Upstream's per-invocation `compose.WithToolList` dynamic override has zero occurrences; only this runtime's own alias logic is tested. |
| 8 | AgenticToolsNode and new graph/workflow/chain/branch/parallel nodes | PARTIAL | Branch/parallel proven through a real `compose.Graph` (`examples/agentic-graph`: `TestBuildBranchExampleCompilesAndRoutesThroughRegisteredBranch`, `TestBuildParallelExampleCompilesAndFansOutToBothRegisteredMembers`). The `compose/workflow.go` `Workflow` variant (`AddAgenticChatTemplateNode`/`AddAgenticModelNode`/`AddAgenticToolsNode` on `compose.Workflow`) has zero occurrences anywhere in the module. |
| 9 | AgenticChatTemplate/FromAgenticMessages/placeholders | PARTIAL | Real API used (`examples/agentic-graph/graph.go`'s `chatTemplate`). No placeholder-substitution test and no "invalid placeholder fails" test. |
| 10 | Typed callbacks for model/prompt/agent/tools-node | NOT COVERED | Not implemented by this pass -- see the W7 "Not implemented by this pass" list. Bead `eino-agent-sm0`. |
| 11 | Agent cancellation modes, recursive cancel, timeout, handles | PARTIAL | Immediate/graceful covered (`runtime/acceptance_matrix_test.go`). Recursive cancel and timeout escalation remain out of scope (see W5 section). |
| 12 | TurnLoop Push/preempt/stop/idle/graceful behavior | PARTIAL | Documented out of scope in the W5 section: preempt (specifically, a late preempt targeting only the captured turn) and idle exit specifically. |
| 13 | Typed/stateful/composite interrupt and LastCheckpoint | PARTIAL | Mechanics well tested (`runtime/w5_round4_acceptance_test.go`, `runtime/orchestrator_resume_test.go`) via the classic, non-generic `compose.StatefulInterrupt` (`runtime/adk_execution.go`, `runtime/adk_approval.go`). None of `TypedInterrupt`/`TypedStatefulInterrupt`/`TypedCompositeInterrupt`/`LastCheckpoint` is used. |
| 14 | Runner checkpoint deletion | PARTIAL | Only `Set` failure is covered; checkpoint promotion-failure and terminal-delete-failure specifically are untested (see W5 section). |
| 15 | Retry context/decision/reject reason and model failover | PARTIAL | Bounded attempts and selected identity proven (`runtime/tool_call_id_publicize_test.go`: `TestDefaultShouldRetryAndShouldFailoverRefuseUnresolvedToolCallID`, `TestRunRetriesTransientToolCallIDLookupFailureAndCompletes`, `TestRunFailsClosedOnceOnDeterministicToolCallIDLookupFailure`). Partial-stream isolation and charged usage were not independently reverified in this audit. |
| 16 | Typed DeepAgent/AgentTool and prebuilt/workflow behavior changes | NOT COVERED | Out of scope, not started (see W5 section). Zero occurrences of `DeepAgent`/`Supervisor`/`PlanExecute`/`deterministic_transfer`/`NewTypedAgentTool`. Bead `eino-agent-bpj`. |
| 17 | Typed handlers, AfterAgent, tool-call context, after-tool hook | PARTIAL | Middleware handler-chain covered (`runtime/adk_middleware_e2e_test.go`). `AfterAgent`/`ToolCallsContext`/`WithAfterToolCallsHook` all have zero occurrences. |
| 18 | Message ID helpers and event sender wrappers | PARTIAL (known defect) | Durable/ADK identity mapping is stable, but "no duplicate model/tool events" is currently violated: `eino-agent-doj` causes AG-UI replay to re-emit a tool call's whole lifecycle a second time on reconnect. `testdata/external-consumer/agentic_fixture_test.go`'s AG-UI fixture asserts this known defect explicitly (exactly 2 native `TOOL_CALL_START`/`TOOL_CALL_RESULT` events for one call, dropping to 1 once `eino-agent-doj` is fixed) rather than hiding it. |
| 19 | agentsmd middleware | COVERED | `runtime/w6_round4_correlation_test.go`, `runtime/w6_round3_test.go`, `runtime/adk_middleware_e2e_test.go`, `composition/registry_handler_test.go`, `examples/agentic-middleware/composed_test.go`. |
| 20 | Typed skill middleware, agent/model hubs | PARTIAL | Skill activation/resume covered (`runtime/adk_middleware_e2e_test.go`). `TypedAgentHub`/`TypedModelHub`/`TypedSubAgentInput`/`TypedSubAgentOutput` all have zero occurrences. |
| 21 | Multimodal filesystem reader and typed filesystem middleware | PARTIAL | Media read covered (`runtime/adk_middleware_e2e_test.go`) against an in-memory fake store (`runtime/admission_store_test.go`); the "and reopen" clause against a real store is not exercised. |
| 22 | Typed plantask, patchtoolcalls | COVERED | `runtime/patchtoolcalls_settlement_test.go`, `runtime/w6_round4_correlation_test.go`, `runtime/adk_middleware_discovery_test.go`, `runtime/adk_middleware_e2e_test.go`. |
| 23 | Typed reduction and summarization, explicit Summarize/finalizers/retry/failover | COVERED (see P1-8 note) | `TestPublicSummarizationMiddlewareWritesDurableContextEpochSurvivingReopen` (`testdata/external-consumer/agentic_fixture_test.go`), `runtime/adk_middleware_e2e_test.go`, `runtime/w6_round3_test.go`, `runtime/w6_round2_group_a_test.go`. The storage primitives are also PostgreSQL-tagged; the summarization-middleware-triggered path specifically is proven only under SQLite (see this section's `POSTGRES_REQUIRED_SUITES` note below). |
| 24 | ToolInfo JSON/Gob encoding, ParamsOneOf nil/empty distinctions | PARTIAL | `ParamsOneOf`/schema definitions are exercised widely (`tools/definition.go`, `tools/einotools/einotools.go`, `runtime/tool_search.go`, `runtime/ledger.go` and their tests), but no test in the module exercises `schema.ToolInfo`'s Gob encode/decode round trip directly (zero matches for `GobEncode`/`GobDecode` in any `_test.go` file). |
| 25 | Stream WithOnEOF and copy/concat/cleanup changes | NOT COVERED | Zero occurrences of `WithOnEOF` anywhere in the module. `model/agentic_streamer.go`'s `einoschema.StreamReaderWithConvert` call is the natural integration point but does not pass it. This is real runtime behavior (the row is "Runtime", requiring implementation and integration acceptance, not just an upstream-API demonstration), so implementing it honestly needs its own implementation and review pass rather than a doc-only fix. Bead `eino-agent-5r8`. |
| 26 | Indexer WithIndex | COVERED (added by this fix pass) | `TestIndexerWithIndexOptionReachesStore` and `TestIndexerWithoutIndexOptionLeavesIndexNil` (`examples/indexer-option/indexer_option_test.go`): a small test indexer reads the real upstream `indexer.WithIndex` call option through `indexer.GetCommonOptions`; no new indexing service. |
| 27 | Graph scheduling/checkpoint/panic/stream fixes, Jinja formatting and other changed existing behavior | NOT COVERED | No section of this document addresses this row, and there are zero occurrences of "Jinja" (case-insensitive) anywhere in the module. Needs a semantic-diff re-read of `compose/graph.go`/`compose/checkpoint.go`/`compose/stream_concat.go` plus fixtures proving graph panic/cancel/checkpoint behavior, and either a Jinja-formatting regression fixture or an explicit note that this codebase has no Jinja-formatting surface to regress. Bead `eino-agent-2wi`. |

Of the 27 rows above: **7 COVERED, 16 PARTIAL, 4 NOT COVERED.** The adoption
proves its core agentic path end to end; most rows have a specific named
unmet clause rather than full coverage, and every gap is named rather than
hidden.

**Changed-public-source ledger** (`01-feature-inventory.md`'s appendix): of the
paths with no mention anywhere in this document, `components/indexer` is now
addressed by row 26 above; `schema/stream.go` by row 25's bead
(`eino-agent-5r8`); `compose/workflow.go` by row 8's partial note;
`adk/prebuilt/supervisor`, `adk/prebuilt/planexecute` and
`adk/deterministic_transfer` by row 16's bead (`eino-agent-bpj`);
`compose/stream_concat` by row 27's bead (`eino-agent-2wi`); and
`utils/callbacks/template` by row 10's bead (`eino-agent-sm0`).
`schema/serialization.go` adds no exported name (an inherited-behavior-only
change per the ledger) and is not separately tracked.

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
  identity comparison (including ADK's own tracked conversation via
  `compose.GetToolCallID` and `adkModel.prepareDispatchInput`/`begin`'s own
  per-dispatch re-projection) exactly as it was. Only the literal
  wire payload handed to the model provider needs to show the provider back
  its own id: `runtime.publicizeToolCallIDs` (`runtime/orchestrator.go`)
  rewrites `function_tool_call`/`function_tool_result`/`tool_search_result`
  block `CallID`s from the minted `ID` to `ProviderCallID` (falling back to
  `ID` when the provider supplied none or an invalid/oversized one, and,
  processing every call in request order, whenever sending `ProviderCallID`
  would be ambiguous in that same outgoing request — an earlier call
  already sends that exact string, or the string equals the durable id of
  any call in the request — see below) in a copy of the messages, applied exactly
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
    given provider id keeps it when a later call reuses that id, so history
    already sent normally keeps its wire ids as the conversation grows
    (provider-side prompt-prefix caches — llama.cpp, vLLM, Ollama — stay
    valid). One exception, from rule (b): if an earlier call's provider id
    equals the durable id of a call added to history later (possible only
    when a provider emits ids in the `IDGenerator`'s minted format), the
    earlier call sends its own durable id from that request on. Every
    request is still unique and internally consistent; only the prefix
    cache for that history is lost.
  - **Failing closed.** A tool-call block with no `tool_calls` row (only
    reachable via direct store writes or imported history, since
    `rejectNonCallerBlocks` refuses caller-authored tool blocks and context
    contributions are text-only) fails the dispatch with
    `errToolCallIDUnresolved` when the underlying store lookup itself failed
    deterministically (`session.ErrNotFound` or `session.ErrConflict`,
    wrapped with `%w` so the cause stays inspectable). A resolved row that
    belongs to a different session is a separate case, not a lookup error:
    the cross-session check fails the dispatch with the same sentinel on its
    own, carrying no store cause. Either way, a sentinel `defaultShouldRetry`/
    `defaultShouldFailover` both refuse to retry or fail over, since a
    deterministic, durable-consistency failure would only burn the run's
    retry/failover budget for nothing. Any other store error (a transient
    read failure, for example) is returned without the sentinel (still
    wrapped with `%w` for the cause) and stays fully
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
  `runtime/w5_round2_test.go` respectively. **A third, complementary
  post-hoc check (round-two W6 review, HA-S6)**: `guardRan`/`dispatches`
  alone do not prove a factory also wired `AgentBuildContext.DurableBaseline`/
  `SettlementSeal` into its agent's real handler chain -- both are
  independent `AgentBuildContext` values a factory could drop while still
  installing `Guard` and dispatching through `build.Model`, satisfying
  both existing checks while never seeding `state.Messages` from durable
  history and never settlement-verifying a result before dispatch.
  `durableBaselineHandler`/`settlementSeal` now track `hasRun()` the same
  way `durableGuard` does (`adkEngine.baselineRan()`/`sealRan()`), checked
  alongside `guardRan`/`dispatches` in the same post-hoc location.
  `TestNonCompliantFactoryWithoutBaselineOrSealFailsAsConstructionError`
  (`runtime/adk_agent_factory_compliance_test.go`) proves it.
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
  `runtime/tool_search.go` (`executeToolSearchCall`). The durable content
  block this produces DOES carry the dedicated `tool_search_result` kind
  (`session.BlockKindToolSearchResult`, built by `toolSearchResultBlock` in
  `runtime/tool_search.go`) rather than a generic `function_tool_result` --
  proven end to end through a real orchestrator run by W8's own
  `TestPublicToolSearchDiscoversDeferredToolThenAliasExecutes`
  (`testdata/external-consumer/agentic_fixture_test.go`), which asserts
  exactly a `ContentBlockTypeToolSearchResult` block and fails if none is
  found. This corrects an earlier version of this bullet, which claimed
  unconditionally that "a model that inspects the conversation for a
  `tool_search_result`-shaped block ... will not find one." That claim does
  not hold: `runtime/w4_acceptance_test.go` defines `isToolSearchResultMessage`
  (`:55`, keyed on `einoschema.ContentBlockTypeToolSearchResult`) and
  `hasAnyToolSearchResult` (`:83`), and three model scripts branch on
  `hasAnyToolSearchResult(request.Messages)` (`:544`, `:579`, `:677`) --
  precisely a model inspecting the conversation for a `tool_search_result`
  block -- and all three tests pass today, so the block is found. What remains
  unverified (not claimed fixed here): whether ADK's own generic tools-node
  round trip -- as opposed to `runtime/tool_search.go`'s direct persistence
  and same-turn model-visible construction -- ever independently represents
  a tool-search result as a plain `function_tool_result` on some other path.
- Enhanced (multi-part) tool results: `adkTool.InvokableRun` still implements
  only `tool.InvokableTool` (a `string` return), not `tool.EnhancedInvokableTool`
  (`*schema.ToolResult`) -- but `TestOrchestratorEnhancedToolResultPersistedMatchesModelVisibleAndReplay`
  passes today (`runtime/w4_acceptance_test.go:1103`; reverify with `go test
  ./runtime -run TestOrchestratorEnhancedToolResultPersistedMatchesModelVisibleAndReplay
  -v`), contradicting this bullet's earlier claim that it fails. The reason:
  the same-turn model-visible content this test checks is NOT built by
  round-tripping through `adkTool.InvokableRun`'s string return at all -- it
  is assembled directly from the settlement's `ToolResult.Parts` by the same
  mechanism `toolSearchResultBlock`'s doc comment calls "the same-turn
  outgoing model message" (`runtime/tool_search.go`, `executePreparedTools`),
  independent of what ADK's own tools node does with the string `adkTool`
  returns. So this specific property (durable persistence and same-turn
  model-visible content both preserving all parts) is proven; it is not
  evidence that ADK's own generic tools-node round trip (a *later* turn's
  request rebuilt purely from ADK's own history mechanism, if one exists) is
  multi-part-aware.
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
  W7. The superseded classic `PartKind`s were removed in W5 phase 2 (see
  the W2 section's "superseded part kinds" note); a WIT/bindings reference
  to those kinds' string values was not found in `wit/eino-agent-extensions.wit`
  or `wasmext/gen`, so no W6 follow-up was required for this removal.

## W6: typed ADK middleware, context and extensions

Status: groups A through F landed and are verified per the gate list below
(`go test ./composition ./runtime ./session/... ./examples/agentic-middleware/...`,
`go vet -tags postgres_integration ./...`, `golangci-lint` 0 issues, `go
test ./runtime ./examples/agentic-middleware -race -count=3`, `make
postgres-test`, `make postgres-race`, `EINO_AGENT_CONSUMER_POSTGRES=1
testdata/external-consumer/check.sh`). The two integration gaps an earlier
pass of this work documented (host-injected content never reaching the
model; a handler-injected tool never being callable) are both resolved --
see below for the mechanism and the tests proving each fix through a real
turn, not just construction. Group E (WASM/WIT content-block evolution) and
Group F (`examples/agentic-middleware/`) are covered in their own bullets
below.

- **Group A (composition + fingerprint)**: `composition.Registrar.Handler(HandlerRegistration{ID,
  Order, Scope, Descriptor: HandlerDescriptor{Kind, Version, Config
  json.RawMessage}, Factory runtime.HandlerFactory})` registers one typed
  ADK agent-handler factory per component. `handlerConfigHash` canonicalizes
  `Config` (decode-then-remarshal, so key order never affects it) before
  sealing it. `discoverHandlerTools` additionally probes each factory once
  at plan-compile time, in a bounded stub-backed `HandlerBuildContext`, to
  enumerate the tools it contributes; the discovered `{Name, SchemaHash}`
  set is sealed alongside the handler's own identity as
  `session.AgentHandlerPlanIdentity{ID, Kind, Version, ConfigHash, Order,
  Scope, Tools []HandlerToolIdentity}` on `session.ComponentPlan.AgentHandlers`
  -- part of `ExtensionPlanDescriptor` and therefore the run's fingerprint
  (golden updated in `session/extensions_test.go`). Neither the factory
  closure nor the discovered `Info`/schema bytes themselves are sealed, only
  the hash. `runtime.RunPlan.AgentHandlers()` exposes the sealed, ordered
  list (with live `Factory` and discovered `Tools []HandlerToolSpec`
  attached) for `adkEngine.buildAgent` to invoke fresh per admitted turn. A
  changed `Config` changes the sealed fingerprint
  (`TestAgentHandlerConfigChangeChangesFingerprintAndRefusesResume`), which
  the existing `AcquireResumePlan`/`VerifyExtensionPlanForSession`
  fingerprint-mismatch path already refuses on resume for every capability
  kind, `AgentHandlers` included by construction.
- **Group B (`runtime/adk_middleware.go`)**: `AgentBuildContext` gains
  `DurableBaseline`, `Handlers` (the per-execution snapshot of every
  plan-ordered handler factory's built instance) and `SettlementSeal`.
  `adkEngine.buildAgent` installs, in ADK's own handler order (first
  registered is outermost for `Wrap*` methods, first registered is first
  called for hooks): `durableBaselineHandler` first, host handlers next,
  then the existing `durableGuard` (integrated, not duplicated, from W5),
  then `settlementSeal` last. `durableBaselineHandler.BeforeModelRewriteState`
  rewrites `state.Messages` to a deep clone of
  `adkEngine.buildDurableBaseline`'s fresh durable projection for this
  cycle -- the durable projection is the **baseline every host handler
  transforms**, not a value discarded and re-derived afterward.
  `adkModel.prepareDispatchInput` (adk_model.go) dispatches exactly what the
  handler chain leaves `state.Messages` as, with no re-projection; it trusts
  the baseline-computed `providerState` (reasoning signatures, citations)
  unchanged when the final input is at least as long as the baseline
  (content edited in place or appended -- robust to ADK's retry/failover
  wrapper defensively cloning the message slice) and drops it, never
  misapplying it, when a handler shortens history (summarization
  compacting). The real settlement authority is
  `verifySettledToolResults`, called from `adkModel.prepareDispatchInput`
  -- the innermost dispatch point every physical attempt passes through
  regardless of a host `WrapModel` wrapper nested around the model after
  every handler's `BeforeModelRewriteState` hook has already run.
  `settlementSeal.BeforeModelRewriteState` calls the identical function and
  is kept only as an early, non-authoritative check (round-one review
  finding C2: a host `WrapModel` rewrite bypassed the hook-only seal --
  `TestSettlementSealRejectsWrapModelTamperedToolResult`). The comparison
  is per-occurrence, not a last-write-wins map (a duplicate/shadow
  `function_tool_result` block for an already-settled call ID is rejected
  even when the genuine block is also present --
  `TestSettlementSealRejectsDuplicateShadowToolResult`), and hashes the
  full canonical content of every block, media fields included, not just
  `Text` (`TestSettlementSealRejectsMediaSwapOnSettledMultimodalResult`).
  It enforces two invariants against `adkEngine.baselineMessages`' own
  reconstruction for that call ID: a settled call's result may never
  diverge from that reconstruction
  (`TestSettlementSealRejectsHostRewrittenToolResult`), and a
  `function_tool_result` with no durable settlement in the baseline is
  rejected as fabricated
  (`TestSettlementSealRejectsUnauthorizedFabricatedToolResult`) -- both
  unless the exact post-rewrite content digest was recorded as an
  authorized rewrite for that call ID, THIS cycle
  (`authorizedRewriteSet`, reset every cycle by
  `durableBaselineHandler.BeforeModelRewriteState`). `wrapAuthorizedContentRewrites`
  (diffing a wrapped middleware's own `function_tool_result` content
  before/after its `BeforeModelRewriteState`) gives patchtoolcalls'
  legitimate dangling-call patches and reduction's legitimate settled-result
  truncation/clearing that authorization, and durably records each one
  (handler ID, kind, call ID, before/after digest) as a
  `session.AuthorizedToolResultRewriteEventKind` event, drained by
  `adkModel.begin`; no other recipe gets it. `HandlerBuildContext` exposes
  no `session.Store`/`session.ExecutionStore` to any registered
  `HandlerFactory` (host-provided ones included) at all -- round-one review
  finding C1: it previously did, letting a host handler fabricate a durable
  settlement directly via `ExecutionStore.ClaimToolCall`/`SettleToolCall`
  (`TestHandlerBuildContextExposesNoDurableStoreAuthority`); the one recipe
  that legitimately needs a bounded durable write (summarization) is given
  a narrow, unexported `contextEpochCapability` instead. The ledger model
  adapter (`adkModel`) remains the sole audit authority structurally --
  `AgentBuildContext.Model` is always the mandatory adapter, and W5's
  existing dispatch-count check (`adkEngine.dispatches`/`onAgentEvents`)
  already detects a factory that substitutes its own model -- so no
  separate "auditSeal" handler was needed. A handler-injected tool belongs
  to the frozen tool universe (see Group C); `durableGuard.BeforeAgent`
  deduplicates a handler's own redundant raw copy of an already-sealed tool
  (its real `BeforeAgent` still runs and re-appends it) rather than
  rejecting it, and still fails any other non-durable or unsealed tool as a
  construction error.
- **Group C (`runtime/adk_middleware_recipes.go`,
  `adk_middleware_workspace.go`, `adk_middleware_scratch.go`,
  `adk_middleware_discovery.go`, `adk_middleware_frozen_tools.go`)**: typed
  wiring recipes for all eight upstream middlewares (`agentsmd`, `skill`,
  `filesystem`, `plantask`, `patchtoolcalls`, `reduction`, `summarization`,
  `dynamictool/toolsearch`), each calling the upstream `NewTyped`
  constructor directly and failing construction closed when a required
  backend/config is absent. `workspaceFilesystemBackend`/
  `workspaceSkillBackend` are read-only views rooted at the admitted
  canonical workspace (`internal/workspace.CanonicalRoot`), rejecting
  `..`-relative and symlink path escape
  (`TestWorkspaceFilesystemBackendRejectsPathEscape`,
  `TestWorkspaceFilesystemBackendRejectsSymlinkEscape`) and supporting
  multimodal image/PDF reads as media parts, proven end to end through a
  real tool call
  (`TestFilesystemHandlerMultiModalReadReturnsMediaPart`).
  `MultiModalRead` bounds the source file's size (`maxMultiModalReadBytes`,
  20MiB, checked via `os.Stat` before ever reading) and rejects an
  oversized file closed with `errMultiModalReadTooLarge` rather than
  reading it fully into memory: handler tools are sealed with
  `Retention{MaxInlineBytes: -1}` (unbounded at the durable-settlement
  content-budget layer, which only clamps AFTER a result already exists in
  memory), so without this cap an arbitrarily large attachment would be
  fully read and base64-encoded before any clamp could apply (round-two W6
  review, RW-S1 --
  `TestWorkspaceFilesystemBackendMultiModalReadRejectsOversizedFile`).
  `writableWorkspaceBackend` is a private, workspace-contained scratch area
  for plantask/reduction's own state, rooted per-session
  (`.eino-agent/sessions/<sha256(session id)>/{plantask,reduction}`, never
  a workspace-wide directory two sessions on the same workspace would
  otherwise share and collide task IDs/offload files in) and constructed
  lazily, only for the recipe kinds a plan actually mounts. Both this
  backend and the read-only `workspaceFilesystemBackend`
  (`GrepRaw`/`GlobInfo`, and `resolveWorkspacePath` for a not-yet-existing
  path) resolve every walked/targeted path through symlink evaluation and
  require the result contained in the canonical workspace root, including
  when the leaf doesn't exist yet (resolving the deepest existing ancestor
  instead) -- a checked-in symlink inside an untrusted workspace can
  neither leak content through grep/glob nor let the writable scratch
  backends write outside the workspace
  (`TestWorkspaceFilesystemBackendGrepRejectsSymlinkEscape`,
  `TestWorkspaceFilesystemBackendGlobRejectsSymlinkEscape`,
  `TestWritableWorkspaceBackendRejectsSymlinkedRoot`,
  `TestWritableWorkspaceBackendRejectsSymlinkedIntermediateDirectoryOnWrite`).
  A handler's tools are discovered once
  at plan-compile time (`discoverHandlerTools`, Group A) and, at real
  per-turn build time, `adkEngine.sealHandlerTools` splices a durable
  `runtime.Tool` per sealed entry into `TurnSnapshot.Tools` *before* the
  agent is built, so `prepareToolCalls`/`resolveToolCall` resolve it like
  any other frozen tool; `handlerToolExecutor` dispatches the call through
  the full durable claim/permission/execute/settle pipeline to the live
  tool instance `adkEngine.buildAgentHandlers` collected, supporting both
  `InvokableTool` and `EnhancedInvokableTool`. A write-like tool is sealed
  with a `Permissions` tag, gated exactly like any other state-changing
  tool -- disabled unless a permission grants it. `isWriteLikeToolName`
  classifies by an explicit override table first
  (`explicitWriteLikeTools`, covering plantask's `TaskCreate`/`TaskUpdate`
  -- both genuinely state-changing but matching none of the substring
  markers below), falling back to a substring heuristic
  (`write`/`edit`/`execute`/`shell`/`delete`) for every other tool name,
  including third-party ones (round-two W6 review, HA-S3/RW-S3 --
  `TestIsWriteLikeToolNameClassifiesPlantaskCorrectly`). Proven end to end:
  `TestFilesystemHandlerToolExecutesThroughDurableWrapper`,
  `TestPlanTaskHandlerCreateAndUpdateThroughDurableWrapper` (TaskCreate then
  TaskUpdate, correlating the created task's numeric ID out of its own
  human-readable result message), `TestSkillHandlerInlineActivationThroughDurableWrapper`.
  Summarization's own internal summary-generation call runs through a
  bounded `adkModel.internalDispatch` adapter, not the turn's own
  conversational adapter (round-one review finding C1/C3: it previously
  did, so the summary text got persisted onto the turn's own assistant
  message and corrupted the session -- a second turn on the same session
  failed to admit). The internal-dispatch adapter is tagged per plan entry
  with that entry's own registered `HandlerID` (e.g. `AgentPath ==
  "summarization"` for a handler registered under that ID -- never a
  shared literal like `"summarizer"`, and never shared across entries),
  is still fully ledgered (its own `ModelRequestRecord` row, usage
  charged, retried/failed over through the same audited path) but never
  claims the turn's assistant placeholder, never persists as
  conversational content, sends no tool controls, and fails closed on
  anything but plain text/reasoning
  (`TestSummarizationWritesContextEpochPreservesReplayThenNarrowsProjection`
  drives a real second `orch.Start` on the same session and asserts it
  completes, the summarizer's ledger row carries the handler's own
  `AgentPath`, and the turn's own assistant message has exactly one
  `assistant_gen_text` part).
  `summarizationFinalize` maps a completed summary into an atomic
  `session.ContextEpoch`, correlating upstream's in-memory
  `originalMessages` to durable messages via a PER-CYCLE, POINTER-KEYED
  lookup (`adkEngine.cycleMessageSourceByPointer`, rebuilt fresh every
  cycle by the mandatory durable-baseline handler from that cycle's own
  durable reload -- round-two W6 review item 8), not by positional index
  or by `history.LoadAgentic` correlated through the active epoch: a
  message with no durable id (content a host handler like agentsmd/skill
  injected this cycle) simply resolves as "no durable id" and is always
  retained verbatim. Because an earlier handler in the chain (host-authored
  or this package's own, e.g. via `cloneProtectedMessages`) can legitimately
  replace message pointers without preserving identity, `summarizationFinalize`
  also falls back to CONTENT-based correlation (`reflect.DeepEqual` against
  that cycle's own baseline) for anything the pointer lookup misses, and
  fails the turn closed -- rather than silently compacting on a partial
  view -- if any durable baseline message still cannot be accounted for
  either way (round-three W6 summarization-correctness review, Important
  #1). This correlation (`correlateDurableSubsequence`, shared with the
  re-apply path below) runs in two STRICT passes -- pointer-keyed first,
  then content fallback only over what pass one left unresolved and only
  against baseline slots pass one did not already claim -- rather than one
  interleaved pass: an ephemeral (non-durable) message that coincidentally
  matches a baseline entry's content can otherwise steal that entry's slot
  before its real, pointer-resolvable owner claims it, letting one durable
  id resolve to two different messages with no error, or masking a
  genuinely unresolved durable message so the fail-closed check above never
  fires (round-four W6 correlation-and-seal review, Important #1).
  Summarization generates a summary at most once per live turn execution
  (`summarizeAtMostOnceMiddleware`): upstream re-evaluates its own trigger
  condition on every ReAct cycle with no per-call override, so without this
  a multi-cycle turn that crosses the threshold once would re-summarize --
  and re-bill a real summary generation call -- on every later cycle of the
  same turn (round-three W6 summarization-correctness review, Important
  #2). The bound is per execution, not per turn id: the guard lives on the
  engine (`adkEngine`'s `summarized*` fields, set by `markSummarized`) and
  `resumeEngine` constructs a fresh engine, so a turn paused and resumed --
  or redriven after a crash -- while still above the threshold can generate
  and bill a second summary and commit a second `ContextEpoch` for the SAME
  turn. That is a known open defect (`eino-agent-5g3`, raised as S1 by the
  round-four W6 correlation-and-seal review and deferred with the
  coordinator's agreement), not a guarantee; closing it needs the guard
  keyed off a durable per-turn fact, such as an epoch recorded against the
  turn id, so it survives `ResumeRun`. Bounding GENERATION to once per turn does not mean the model sees
  the full, uncompacted baseline again on later cycles: doing so measured
  as per-cycle message counts regrowing from a compacted 1 back up to 10
  across five cycles of the same tool loop (round-four W6
  correlation-and-seal review, Important #2). Every cycle after the one
  that committed the epoch instead re-applies that SAME compaction
  (`reapplyDurableSummary`, using the cached `SummarizedFromID`/
  `SummarizedToID`/boundary id/summary object) without generating or
  billing a second summary, so a long tool loop stays inside the context
  window for the rest of the turn (measured as `[1 1 3 5 7]` on the same
  five-cycle shape: compacted once, then growing by exactly the new
  call/result pair each cycle adds, not regrowing toward the
  pre-compaction baseline). `summarizationFinalize`
  commits `StartContextEpoch` + the compaction boundary in ONE fenced
  transaction (`compaction.AppendBoundaryTx`) so a mid-sequence failure can
  never leave an epoch row with no `SummaryMessageID`. It never creates an
  epoch whose `SummarizedFromID`/`SummarizedToID`/`TailStartID` overlap,
  moves the retained tail's start back so a `function_tool_call` is never
  separated from its `function_tool_result` (`moveTailStartToGroupBoundary`),
  and always keeps the session's leading system-message prefix out of the
  summarized range. `applyEpoch` (`session/history/projector.go`) likewise
  always preserves a session's leading system messages regardless of the
  active epoch, and no longer treats an empty `TailStartID` (nothing
  retained verbatim) as "nothing produced after this epoch is ever included
  again" -- the projection picks back up after the boundary message for
  content later turns produce. A failed or cancelled summary-generation
  call fails/interrupts the whole turn (upstream's own
  `BeforeModelRewriteState` propagates the error before the turn's own
  main dispatch ever runs for that cycle) and never creates a new epoch;
  any previously committed epoch survives unchanged.
  `resolveTurnHistoryOptions` resolves the most recently finished
  summarization epoch fresh at every turn admission (both a brand-new run
  and a later turn on an existing run), so the provider projection actually
  narrows for later turns without a host statically configuring
  `history.Options.Epoch`
  (`TestSummarizationWritesContextEpochPreservesReplayThenNarrowsProjection`
  asserts turn 2's provider-visible message count is narrower than the
  full durable replay). This resolution happens once per turn admission,
  not once per ReAct cycle within a turn: `adkEngine.baseMessageCount` and
  every cycle's `buildDurableBaseline` reload within ONE turn must agree on
  the same epoch view for their prefix/fresh-reload-tail splice to stay
  correct, so a summarization epoch a turn's own recipe commits mid-turn
  takes effect starting the NEXT turn on that session, not later cycles of
  the same turn that created it -- a documented, bounded scoping choice.
  Cancelled/failed summary generation never calls `Finalize` (upstream's
  own contract, not this package's code), so the previous epoch stays
  active, proven by two dedicated tests that actually trigger and then fail
  or cancel the internal summary-generation call specifically
  (`TestSummarizationFailedGenerationKeepsPreviousEpoch`,
  `TestSummarizationCancelledGenerationKeepsPreviousEpoch`) rather than
  configuring the trigger so high summarization never runs at all. Keeping
  the previous epoch does NOT mean the turn itself succeeds: upstream's own
  `BeforeModelRewriteState` propagates a failed or cancelled summary call's
  error, which fails that whole cycle -- the turn's own main dispatch never
  even runs for it. A failed summary call fails the turn
  (`session.RunFailed`); a cancelled one maps to an interrupted outcome
  (`session.RunInterrupted`, `Result.Interrupted == true`) -- both proven by
  the same two tests above, not merely asserted in this paragraph.
  Reduction's `MaxLengthForTrunc` truncation and `MaxTokensForClear`
  clearing are BOTH effective, through two different mechanisms with two
  different timings (round-two W6 review item 2). Truncation applies per
  call, at EXECUTION time, before this runtime durably settles it:
  `adkEngine.applyHandlerToolResultWrappers` threads a claimed tool's raw
  result through every mounted host agent-handler middleware's own
  `WrapInvokableToolCall`/`WrapEnhancedInvokableToolCall` (the exact
  mechanism reduction's `MaxLengthForTrunc` uses) from inside
  `executeClaimedToolPipeline`, BEFORE `buildToolSettlement`/
  `persistToolSettlement` -- so the settled result, the seal, and what the
  model sees are all the truncated form, with its offload reference, not
  an after-the-fact rewrite of an already-settled original. A
  `tools.Definition`-based tool's duplicate `Structured` field is cleared
  whenever a wrapper changes `Output`, closing a leak where the
  untransformed original would otherwise survive in the durable envelope's
  `Structured` side channel even after `Output` was truncated. Clearing
  applies across the WHOLE conversation's accumulated size and stays a
  post-settlement rewrite of the durable baseline via
  `BeforeModelRewriteState`, explicitly authorized through
  `wrapAuthorizedContentRewrites`/`settlementSeal` exactly as before. Both
  are proven with `Retention{MaxInlineBytes:-1}` so the payload is
  genuinely inline and only reduction (not the runtime's own retention
  truncation) can be shortening it:
  `TestReductionTruncatesToolResultBeforeSettlement`/
  `TestReductionTruncatesSettledToolResultBeforeSettlement` (truncation,
  asserting the durable settlement row, the durable replay, and the
  model-visible dispatch all show the truncated form) and
  `TestReductionClearsOlderSettledResultAsAuthorizedRewrite`/
  `TestReductionClearsOlderRoundAsAuthorizedRewrite` (clearing, two
  rounds, `MaxLengthForTrunc` disabled so only clearing fires).
  `verifySettledToolResults`'s authorization is Kind-scoped
  (`kindMayRewriteSettledContent`): only reduction may legitimately
  rewrite a call ID the baseline already shows real settled content for;
  a patchtoolcalls-kind authorization for such a call ID is refused even
  though `wrapAuthorizedContentRewrites` itself recorded it (round-two
  review item 4/HA-S3 convergence with RW-S3 --
  `TestPatchToolCallsCannotPatchACallWithARealSettlement`).
- **Group D**: verified no-op. `runtime/extension_{context,model,tool,lifecycle}.go`
  and `wasmext/*.go` already carry only `*schema.AgenticMessage` (landed in
  W3), not classic `schema.Message`.
- **Both previously-documented gaps are resolved.** Root causes and fixes,
  each proven by an end-to-end test that used to be `t.Skip`ped and now
  passes unskipped:
  1. Host content reaching the model: durably fixed by making the durable
     projection the baseline (see Group B's `durableBaselineHandler`
     bullet) instead of a value `adkModel` discarded and rebuilt from
     scratch after every handler ran.
     `TestAgentsMDHandlerInjectsContentIntoModelRequest` now passes.
  2. Handler-injected tools: durably fixed by sealing a handler's
     discovered tools into the frozen tool universe at plan-compile time
     (see Group C). `TestFilesystemHandlerToolExecutesThroughDurableWrapper`
     now passes.
- **Known bounded limitations, not silently dropped**:
  - `discoverHandlerTools`' compile-time probe uses stub backends/a stub
    deferred tool and no real session/model identity; this is sufficient
    for all eight of this package's own recipes (their tool lists are
    static per configuration) but is a documented limitation for a
    hypothetical third-party handler Kind whose own tool list genuinely
    depends on data the bounded probe cannot supply -- such a handler would
    be sealed with fewer tools than it might produce at real execution
    time. A static `HandlerRegistration.Tools` declaration (bypassing
    probing) is the documented escape hatch; it was not added this pass.
    The probe itself now runs under a bounded deadline
    (`discoveryProbeBudget`) and through purely in-memory stub backends
    (`probeWritableBackend`, wrapping upstream's own
    `adk/filesystem.NewInMemoryBackend()`) -- no temp directory is created
    -- and fails plan compilation closed, rather than silently sealing zero
    tools, for a Kind this package knows is always tool-bearing once
    correctly configured (filesystem, plantask, skill, toolsearch); every
    other Kind keeps the documented graceful degradation
    (`TestDiscoverHandlerToolsFailsClosedForKnownToolBearingKind`,
    `TestDiscoverHandlerToolsDegradesGracefullyForOtherKinds`,
    `TestDiscoverHandlerToolsBoundsTheProbeContext`,
    `TestHandlerProbeBuildContextNeverTouchesTheFilesystem`).
  - **Resolved (round-two W6 review item 4).** patchtoolcalls now has real
    seeded-durable-history tests, not just the authorization mechanism's
    generic proof: `TestPatchToolCallsPatchesOrphanedCallWithoutFabricatingSettlement`
    seeds a genuinely orphaned `function_tool_call` directly through the
    store (no `session.ToolCall` row at all, bypassing any live turn) and
    proves the real recipe's patch reaches the model, is durably audited
    (`AuthorizedToolResultRewriteEventKind`), and creates no settlement
    row; `TestPatchToolCallsCannotPatchACallWithARealSettlement` seeds a
    real, fully settled `ToolCall` (via the same `BuildToolSettlement`/
    `SettleToolCall` path a live turn uses) and proves a
    patchtoolcalls-kind rewrite attempt against it is rejected
    (`kindMayRewriteSettledContent`), leaving the durable settlement
    untouched. The example package's construction/registration proof
    (`TestPatchToolCallsHandlerFactoryConstructs`) and "mounts and does
    nothing when nothing is dangling" proof
    (`TestPatchToolCallsMountsAndCompletesNormalTurnWithoutAlteringSettlement`)
    still stand alongside these, at the black-box level.
  - **Resolved (round-two W6 review item 1).** The toolsearch recipe's own
    sealed search tool no longer settles as an ordinary
    `function_tool_result` with only an in-memory `markDiscovered` side
    effect: `adkTool.InvokableRun` type-asserts it as a
    `toolSearchHandlerToolExecutor` and routes it through
    `adkEngine.executeAndSettleHandlerToolSearch`
    (`runtime/tool_search.go`), which settles the call as a durable
    `tool_search_result` block via `buildTerminalToolSearchEnvelope` --
    the SAME content-block shape the native tool-search path produces --
    and marks matches discovered via `markDiscovered`. Because
    `discoveredToolsFromMessages`/`discoveredToolsFromHistoryPaged` read
    that durable shape (not any live run's in-memory set) to rebuild the
    advertised deferred-tool set, discovery now replays on a fresh turn on
    the same session, after `ResumeRun`, and after a brand-new
    orchestrator instance against the same store -- proved respectively by
    `TestToolSearchDiscoveryReplaysOnNextTurn`,
    `TestToolSearchDiscoveryReplaysAfterResumeRun`, and
    `TestToolSearchDiscoveryReplaysAfterProcessRestart`, alongside the
    existing `TestToolSearchFindsDeferredTool` (the discovered tool is
    actually callable).
  - **Resolved (round-two W6 review item 3).** `ResumeRun` now re-reads,
    from the current workspace state, every skill this session has ever
    durably recorded activating (`SkillActivatedEventKind`) and compares a
    fresh content digest against the one recorded at activation time
    (`verifySkillActivationsUnchanged`, `runtime/adk_middleware_recipes.go`)
    -- read-only, before the run's fence is ever claimed, alongside
    `ResumeRun`'s other pre-claim checks. A changed or missing `SKILL.md`
    between pause and resume fails resume closed with the new
    `ErrSkillChangedSinceActivation`, leaving the run exactly as paused as
    it was found. Proved by `TestResumeAfterUnchangedSkillContentSucceeds`
    (resume proceeds normally when the file is untouched) and
    `TestResumeAfterChangedSkillContentIsRefused` (editing `SKILL.md`
    between pause and resume is rejected, and the run's durable status is
    still `RunPaused` afterward).
  - Failover to an alternate model reuses the primary model's
    `buildDurableBaseline`-computed `providerState` rather than
    recomputing it against the failover provider's own projection (the
    pre-W6 `durableProjection` recomputed per physical attempt, using
    `activeModel()`); a bounded, accepted simplification given
    `BeforeModelRewriteState` now runs once per logical cycle, before the
    internal failover/retry wrapper, not once per physical attempt.
- **Round-two W6 review, item 5 (suggestions deferred for time in round
  one, re-evaluated)**. Applied: HA-S5/int64 config-hash precision;
  RW-S2/RW-S4 (fail-closed discovery probe, reject empty summaries);
  RW-S8 (real second-turn narrowing assertion) -- all landed earlier this
  pass, see the relevant bullets above. Newly applied this item: HA-S3/
  RW-S3 (exact `isWriteLikeToolName` override table for plantask); HA-S6
  (`baselineRan`/`sealRan` post-hoc compliance checks); RW-S7
  (`ErrHandlerConfiguration` sentinel); RW-S1 (bounded `MultiModalRead`);
  RW-S5 (`HandlerFactory`'s `BeforeAgent` idempotence requirement
  documented). Still deferred, each for a stated reason rather than
  silently dropped:
  - **HA-S1** (verify a live handler tool's schema hash against the
    sealed `HandlerToolSpec.SchemaHash` at real per-turn build time):
    defense-in-depth against a live tool's schema drifting from what
    compile-time discovery probed; not exercised by any of this package's
    own eight recipes, whose tool schemas are static per configuration --
    only relevant to a hypothetical third-party handler whose schema is
    genuinely data-dependent.
  - **HA-S4** (`NewXxxHandlerFactoryFromConfig(json.RawMessage)`
    constructors binding `HandlerDescriptor.Config` to the factory
    closure by construction): a larger API/wiring redesign (new
    constructor shape for every built-in Kind, plus a `Registrar.Handler`
    change), out of scope for a suggestion-severity finding in this pass.
  - **HA-S7** (a named `errToolAdapterBypassed` for a clearer message when
    a host tool wrapper short-circuits the durable adapter): the failure
    already fails closed correctly (probe P6, round-one review); this is
    a message-quality improvement, not a correctness/security gap.
  - **HA-S8** (stop the double `BeforeAgent` probe call structurally, by
    capturing tools from the real in-context call instead): superseded by
    RW-S5's lighter alternative applied above (document the idempotence
    requirement) rather than the larger `BeforeAgent`-wrapper refactor
    HA-S8 itself proposes.
  - **RW-S6** (bound `workspaceSkillBackend` discovery to a configured
    skills subdirectory plus an entry cap, instead of scanning every
    top-level workspace directory): a hygiene/performance concern, not a
    demonstrated exploit -- a matching `SKILL.md` would have to
    deliberately or accidentally exist in e.g. `node_modules`/`.git` to
    matter; lower severity than the items applied this pass.
  - **RW-S9** (state plainly in `wit/eino-agent-extensions.wit` that the
    package version, not a per-case version, is the variant's only
    negotiable identity): APPLIED -- `wit/eino-agent-extensions.wit`'s
    `content-block` variant doc comment (around line 116) now states this
    explicitly, including the exact rejection mechanism (export-name lookup
    at compile time, not a canonical-ABI/signature check).
  - **HA-S2** (detect handler-tool name collisions at plan compile, not
    just "last one wins" at seal time): APPLIED in the round-two W6
    review's own item 18/Group H -- `validateHandlerToolNameCollisions`
    (`runtime/extension_plan.go`) rejects a handler-declared tool name that
    collides with the native `tool_search` name or any plan tool/alias at
    `NewRunPlan` compile time.
- **Group E (WASM/WIT content-block evolution)**: landed and verified.
  `wit/eino-agent-extensions.wit`'s `text-message` record is replaced by a
  `content-block` variant (`text(string)`, `media-reference`,
  `function-call`, `function-result-text`) inside a `message` record
  (`role` + `blocks: list<content-block>`), so `context-source` (and any
  future WASM context/model/tool middleware observing message content) can
  produce media references and call/result projections without flattening
  them to text; `context-source-api.load-context` now returns
  `list<message>`. The package bumped `eino-agent:extensions@0.1.0` ->
  `@0.2.0`; bindings were regenerated with `make wit`
  (`wasmext/gen/eino-agent/extensions/v0.2.0/...`) and `make wit-check`
  passes clean against the committed tree. `wasmext/engine.go`'s six
  `worldContract` values (world + export names) are pinned to the package
  version; these were still hardcoded to `@0.1.0` after the bump, which
  made every fixture -- not only `context-source` -- fail to compile until
  fixed, confirming the version pin is load-bearing, not decorative.
  `wasmext/wasmtime_worlds.go`'s new `decodeMessages`/
  `decodeContentBlocks`/`decodeContentBlock` (modeled on the existing,
  proven `decodeReplacement` variant-decoding pattern) replace the removed
  `decodeTextMessages`, bounding block count and cumulative payload bytes
  against `Limits.MaxOutputBytes`. `wasmext/wrappers.go`'s
  `loadContextMetadata` (`convertContentBlock`/`convertMediaReference`)
  maps the two production WIT cases onto the matching `schema.ContentBlock`
  variant: `text` stays plain text; `media-reference` is classified by its
  required MIME type (`image/`, `audio/`, `video/`, else treated as an
  opaque file reference -- `media-reference` carries no separate kind
  discriminant of its own) into the matching typed
  `UserInput{Image,Audio,Video,File}` block, never flattened to text, after
  its `uri` is validated (non-empty, bounded length, valid UTF-8,
  `https`-only scheme). `function-call`/`function-result-text` are
  rejected with a contract error before any message mutation -- see the
  context-source-output-restriction bullet below. All six guest fixtures
  (`examples/wasm-extensions/*/main.go`) were rebuilt via `make
  wasm-fixtures` (both `tinygo` and `wasm-tools` are present locally); the
  `context-source` guest now emits a `content-block` message.
  **Version rejection is structural, proven, and does not crash**: an old
  guest built against the superseded `@0.1.0` world exports a
  DIFFERENTLY-VERSIONED world interface name
  (`eino-agent:extensions/context-source-api@0.1.0`, not this package's
  `@0.2.0`), so it is rejected at `Compile` by a plain export-name lookup
  failure -- `wasmext/engine_wasmtime.go`'s
  `component.GetExportIndex(nil, contract.exportName)` finds no export by
  that name and fails closed with `"required world export missing"`,
  classified `ErrorContract` -- before any call, before instantiation, as
  an ordinary `*wasmext.Error`, never a panic. This is a lookup-by-name
  failure, not a canonical-ABI/signature type check: a guest that happened
  to export the SAME versioned name with a subtly incompatible function
  signature is not a case this mechanism catches at all -- signature
  compatibility within one package version relies on WIT/package-version
  discipline between host and guest, not a runtime check. This is a
  permanent regression test, not a one-off manual check:
  `TestCheckedInOldABIContextSourceRejectedCleanly` loads a checked-in
  `@0.1.0` `context-source.wasm`
  (`examples/wasm-extensions/fixtures/context-source-abi-v0.1-incompatible.wasm`,
  the pre-bump fixture preserved under a new name) against the current host
  and asserts a clean `*Error` with `Kind == ErrorContract` specifically.
  `TestCheckedInPhaseBComponentsRoundTrip` exercises the new shape end to
  end through the rebuilt fixture. No classic `PartKind` names existed in
  this WIT file to rename (the parallel removal work in another worktree
  does not intersect this package).
  - **Context-source output is restricted to text and media-reference**:
    `function-call`/`function-result-text` are OBSERVATION-only cases in
    the shared `content-block` variant (valid for a future tool-middleware
    content-observation point), never valid for `context-source`'s
    `load-context` output -- that output becomes part of a turn's own
    admission-time prefix, which the host's settlement seal
    (`runtime.verifySettledToolResults`) treats as already-trusted baseline
    content, so a guest emitting either case there would let it fabricate
    an apparently-settled tool result no durable record ever backed.
    `wasmext/wrappers.go`'s `convertContentBlock` rejects both with a
    contract error before any message mutation. A `media-reference`'s
    `uri` is validated before use: non-empty, bounded length, valid UTF-8,
    and an allowed scheme (`https` only -- `data:`/`file:`/relative are
    rejected).
  - **Bounded limitation**: `testdata/external-consumer` (a separate Go
    module resolving the published module graph, exercised by
    `make external-consumer-check` / `EINO_AGENT_CONSUMER_POSTGRES=1
    testdata/external-consumer/check.sh`) covers the store/session/
    runtime/tools surface only; it does not instantiate WASM components
    (cgo-gated, backed by local `.wasm` fixture files, not something a
    published-module consumer loads). The content-block shape and old-ABI
    rejection are instead exercised, end to end, by this repository's own
    `wasmext` test suite as described above -- the mechanism the coordinator
    asked to be documented if this exact scenario weren't literally covered
    by the external-consumer harness.
- **Group F (`examples/agentic-middleware/`)**: landed and verified. A
  runnable `Mount` wires all eight recipes through only
  `composition.Registrar.Handler`/`runtime.StreamingOrchestrator` -- never
  this package's own internals -- and a 22-test black-box suite proves the
  acceptance scenarios hold from outside the runtime package, not only
  inside its own white-box test suite. This includes (round-two W6 review
  item 14) one composed example,
  `TestComposedExampleMountsAllEightRecipesInOneRunPlan`, that mounts all
  eight recipes together in one `RunPlan` and drives a real multi-turn
  scenario exercising every one of them with concrete, per-recipe
  assertions -- not a registration-only smoke test. Individually: a
  combined agentsmd + skill +
  multimodal-filesystem-read + plantask create/update turn; missing-
  workspace-root failing every workspace-backed recipe closed; a symlink
  escaping the workspace root rejected without leaking content; reduction
  truncating a large settled result before settlement, and separately
  clearing an older round once a second, more recent round exists (ONE
  handler, two of its own rewrites in the same turn -- not two DIFFERENT
  handlers; that scenario, "ordering with two rewrites", is proven in
  `runtime`'s own test suite instead, via
  `TestTwoHandlersRewriteTwoDifferentResultsInOneTurnBothAuthorized`, and
  -- as of the composed example below -- also inside this package itself);
  summarization with a fake summary model
  writing a `session.ContextEpoch` while the full durable replay stays
  intact (`TestSummarizationWritesContextEpochPreservesReplayThenNarrowsProjection`),
  and -- round-three W6 review item 16 -- a genuinely triggered but failed
  or cancelled summary-generation call leaving a PRIOR, already-committed
  epoch unchanged and creating no new one
  (`TestSummarizationFailedGenerationKeepsPreviousEpoch`/
  `TestSummarizationCancelledGenerationKeepsPreviousEpoch`); patchtoolcalls
  completing a normal turn without altering a real settlement; toolsearch
  finding a deferred tool by name and separately refusing construction
  with zero deferred tools; a custom, unauthorized `HandlerFactory` built
  through only the public API failing closed when it tries to mutate a
  settled tool result (immutable input, via `settlementSeal`
  -- proven from outside the package this time); and interrupt/resume via
  `Orchestrator.Stop`/`ResumeRun` (the resumable, checkpointed pause API --
  `Handle.Interrupt` is documented skip-checkpoint and does not produce a
  resumable pause) resuming a blocked tool call without re-executing it,
  then a second scenario proving a resume after the mounted handler's own
  `Config` changed is refused end to end (not just via the static
  fingerprint-inequality check `TestAgentHandlerConfigChangeChangesFingerprintAndRefusesResume`
  already covered) -- catching, in the process, a real bug in this
  example's own first draft: `Mount` was sealing every handler's
  `HandlerDescriptor.Config` as `nil` regardless of the recipe's real
  configuration, which would have made every handler's sealed fingerprint
  identical regardless of its actual settings; fixed by marshaling each
  recipe's own config value into `Config` alongside the factory that
  consumes it.
  - **Resolved (round-two W6 review item 1)**: three new tests --
    `TestToolSearchDiscoveryReplaysOnNextTurn`,
    `TestToolSearchDiscoveryReplaysAfterResumeRun`,
    `TestToolSearchDiscoveryReplaysAfterProcessRestart` -- prove discovery
    now durably replays on a fresh turn on the same session, after
    `ResumeRun`, and after a brand-new orchestrator instance against the
    same store, alongside `TestToolSearchFindsDeferredTool` (the
    discovered tool is actually callable in the SAME turn it was found).
    Two new resume tests, `TestResumeAfterUnchangedSkillContentSucceeds`
    and `TestResumeAfterChangedSkillContentIsRefused`, prove item 3's
    skill-resume-verification fix end to end from outside the runtime
    package.
  - **Resolved (round-two W6 review item 14)**: the composed example
    (`TestComposedExampleMountsAllEightRecipesInOneRunPlan`,
    `examples/agentic-middleware/composed_test.go`) builds a genuine
    "dangling call, no durable settlement" fixture at the black-box level
    -- a custom public `HandlerFactory` (`danglingCallInjector`) mounted at
    Order 0 injects an unanswered `function_tool_call` that patchtoolcalls
    patches in the same cycle, asserted both at the model input and by the
    absence of a `session.ToolCall` row. Reaching this requires no seeded
    `ExecutionStore` history: the injector reaches patchtoolcalls' own
    in-memory scan through the same public extension surface every recipe
    in the example is built on. The runtime-internal, store-seeded
    versions (`TestPatchToolCallsPatchesOrphanedCallWithoutFabricatingSettlement`
    and `TestPatchToolCallsCannotPatchACallWithARealSettlement`,
    `runtime/patchtoolcalls_settlement_test.go`) still stand alongside it.
  - **Resolved (`eino-agent-0wb`, round-four W6 correlation-and-seal
    review, Important #4).** patchtoolcalls and summarization previously
    could not be mounted together once summarization fired: this runtime
    commits summarization's compaction boundary message
    (`session.RoleSystem`, `PartCompaction`) mid-turn, and a turn's own
    admission-time epoch view does not narrow the provider projection via
    `applyEpoch` for later cycles of that same turn (only the NEXT turn
    admitted on the session sees the epoch), so a later cycle's raw reload
    could place that boundary message BETWEEN an assistant
    `function_tool_call` and its `function_tool_result` -- specifically
    when the trigger fired on a turn's own first cycle, since that cycle's
    assistant placeholder row is reserved at admission (an early durable
    position) while its eventual call and that call's result both commit
    later, straddling the boundary. Upstream patchtoolcalls'
    `hasCorrespondingAgenticToolResult` requires strict adjacency (it stops
    at the first non-user message), so it judged the already-settled call
    dangling and appended a fabricated second result; `settlementSeal`
    correctly rejected that as a diverged occurrence and failed the turn --
    the right last line of defence, but a failed turn all the same. Fixed
    by `runtime.repositionMidTurnCompactionBoundary`
    (`runtime/adk_model.go`), called from `buildDurableBaseline` on every
    cycle after the one that committed a mid-turn boundary: it moves the
    boundary, in memory only, to sit immediately before the earliest
    `function_tool_call` (or toolsearch `ToolSearchFunctionToolResult`
    -- both call/result content-block shapes are handled) it would
    otherwise separate from its own result -- the same "never forward,
    only backward until safe" invariant `moveTailStartToGroupBoundary`
    already enforces for the tail's own cut point.
    `TestComposedExampleMountsAllEightRecipesInOneRunPlan` now mounts
    patchtoolcalls in both turns, and
    `TestRepositionMidTurnCompactionBoundaryMovesBeforeSplitGroup`/
    `TestRepositionMidTurnCompactionBoundaryHandlesToolSearchResult`
    (`runtime/w6_round4_correlation_test.go`) prove the fix directly,
    mutation-proved by hand against a reverted no-op.

## W7: AG-UI bridge, durable identity, and rich transport ingress (partial)

Status: this section states only what is implemented and empirically
verified below -- it is not a complete account of the W7 plan
(`.agents/plans/eino-v0-9-19/07-transport-and-observability.md`). Durable
identity, `message_committed`, and the agentic committed-projection
emission path (replay and live) are implemented and tested. Rich AG-UI
transport ingress (decode plus two new handlers) is implemented and tested
but not yet wired as the default path. Full AG-UI lifecycle mapping
(`run_paused`/`attempt_replaced`/subagent events), transient per-block live
deltas, watch bounded block state, and the observability typed-callback
adapters are **not implemented** by this pass -- see
`docs/architecture/agui-events.md`'s "W7" section for the exact boundary.
Verified: `go build ./...`; `go vet ./...` and `-tags postgres_integration`;
`gofmt`/`goimports` clean; `golangci-lint` 0 issues; `go test ./... -count=1`;
`go test ./agui ./transport ./watch ./stream ./obs ./runtime -count=1`; `go
test -race ./agui ./transport ./watch ./stream`; `make check`; `make
postgres-test` (real Docker PostgreSQL, "required suites passed; zero
skips"); `EINO_AGENT_CONSUMER_POSTGRES=1 testdata/external-consumer/check.sh`.
`make postgres-race` passes the full runtime/store contract except one
subtest (`store/postgres` `TestPostgresStore/contract/paused_runs/
claim_run_on_a_paused_run_succeeds_immediately_without_waiting_for_lease_expiry`)
that fails intermittently under the full suite's `-race` load but passes
reliably standalone (`go test -race -tags=postgres_integration
./store/postgres -run 'TestPostgresStore/contract/paused_runs'`); this test
asserts real wall-clock lease timing and this pass's diff does not touch
`ClaimRun`/`PromotePause`/lease code, so it reads as a pre-existing
environment-timing flake under heavy concurrent load, not a regression --
flagged rather than silently ignored.

- **Durable identity (`session.Message.TurnID`/`AgentPath`)**: stamped at
  most (not quite every -- see below) append sites: admission, turn
  admission, continuation dispatch, approval response, tool settlement,
  tool search settlement, and the compaction boundary message
  (`session/compaction`). `runtime.settleInterruptedTool`'s
  crash-reconciliation path (`runtime/tool_execution.go`) is a deliberate
  exception: no live `TurnSnapshot` exists on that path and `session.Store`
  exposes no by-ID message read to recover the calling message's turn, so
  it stamps an empty `TurnID`/`AgentPath` on purpose rather than paying for
  a full `ListMessages` scan on an already-degraded settlement path (bounded
  by `agui.agenticIdentity`'s synthetic-turn-id fallback -- see
  `docs/architecture/agui-events.md`'s "Durable identity" section). Both
  fields are record-JSON-only
  correlation metadata, matching the pre-existing
  `EventRecord.TurnID`/`AgentPath` and `ModelRequestRecord.TurnID`/
  `AgentPath` convention -- no column or index backs any of the four, and no
  SQL migration was made or needed (confirmed directly against
  `store/sqlite/migrations/00001_initial.sql` and
  `store/postgres/migrations/00001_initial.sql`: `messages`/`events` persist
  the full struct as an opaque blob/bytea `record` with no enum `CHECK` on
  `kind`). `session.ValidateAdmitTurn` rejects a message stamped with a
  *different* turn than the one being admitted but tolerates one left
  unset. `store/storetest/w5_durable.go`'s new `durable_identity` contract
  suite (registered in `POSTGRES_REQUIRED_SUITES`) proves the round trip
  through `AdmitTurn` and a follow-up `AppendMessage`, the tolerate-unset
  case, and the reject-mismatch case, against both SQLite and PostgreSQL.
  `ServerCallBlock`/`ServerResultBlock` (`session/content.go`) now document
  that a server tool block's `ProviderServerID` (the eino-agui projection
  term) is its existing `CallID` field -- no separate field exists or is
  needed.
- **`session.MessageCommittedEventKind`** ("message_committed",
  `session/event_kinds.go`): an ordinary (non-canonical) durable event,
  published best-effort by `runtime.StreamingOrchestrator.
  publishMessageCommitted` (`runtime/message_commit_event.go`) once after
  `persistAssistantTurn` commits an assistant message's content and once
  after each tool call settles (`persistToolSettlement`), carrying the
  message id and the session's observation-watermark revision at commit
  time. Best-effort by design: the underlying content is already durably
  committed regardless of whether this notification is observed, so its own
  failure must never be reported as though the commit itself failed.
  `runtime/message_commit_event_test.go` drives a full turn with a real
  tool call through a scripted provider and proves exactly three
  `message_committed` events (no duplicates, correct message ids, every
  committed message carries a non-empty `TurnID`).
- **Agentic committed-projection emission (`agui/bridge.go`,
  `agui/replay.go`, `agui/agentic_projection.go`)**: both the durable replay
  path and the live path now use the accepted `eino-agui` agentic bridge
  (`convert.ToAgenticProjection`, `emitter.Emitter.EmitCommittedProjection`)
  instead of a locally duplicated conversion. `emitMessageSnapshot` projects
  every durable message and emits each projection with `DeliveryModeReplay`.
  It does not emit a `MESSAGES_SNAPSHOT`: an earlier fix pass added one
  built from each projection's `NativeMessage` (user-role display text
  only, since `convert.ToAgenticProjection` populates `NativeMessage` only
  for user-role messages upstream), emitted ahead of the assistant
  projections that follow it -- reordering any transcript containing
  assistant messages and ignoring the replay cursor entirely. That snapshot
  was reverted; a native-only client (one that never parses the
  `eino.agentic.v1` custom envelope) has no representation of user-role
  history on this path today. This removes the
  `history.ErrClassicUnsupported` failure the classic `history.Load`
  projector hit on `tool_search_result`, `mcp_*`, and assistant media --
  but those specific kinds have no native AG-UI representation at all
  (`convert.nativeEventsForBlock` returns none for them) and replay as an
  `eino.agentic.v1` custom content-block supplement **only**; only
  `reasoning`, `assistant_gen_text`, `function_tool_call`, and text-only
  `function_tool_result` get a native event alongside their supplement.
  `TestReplayMessageSnapshotIncludesUserMediaBlock` proves the media content
  actually reaches the stream for `user_input_text`/`user_input_image`
  specifically (not merely that loading didn't error); it does not cover
  `tool_search_result`/`mcp_*`/assistant media. `Bridge.Emit` gains a
  `session.MessageCommittedEventKind` case that reprojects the committed
  message and emits it with one of two delivery modes depending on whether
  THIS connection has already natively streamed THIS message's TEXT/
  REASONING content (`Bridge.nativeStreamed`, a per-message record set by
  `emitMessageDelta` only -- see below for why tool-call natives are
  tracked separately, per call): `DeliveryModeCommittedOnly` (native events
  plus the custom supplement) if not -- covering a message that first
  commits during `replay()`'s own durable sweep, among other cases -- and
  `DeliveryModeLiveContinuation` (custom supplement only) if so, where
  representable native content already streamed live via the existing
  delta path before the message committed. `emitMessageDelta` also drops
  any delta naming a message already in `Bridge.projectedMessages`
  outright, as necessarily stale (a delta strictly precedes its own
  message's commit, by construction). (An earlier version of this mode
  selection keyed off a connection-phase flag, `Bridge.inReplaySweep`; the
  fourth W7 fix-pass review's P0-1 finding replaced it with the
  per-message record above after finding that a stale, buffered live delta
  for a message the durable sweep already delivered could still reach
  `emitMessageDelta` and duplicate its native content -- a phase flag
  alone could not prevent that. See `docs/architecture/agui-events.md`'s
  "Committed-projection emission" section for the full mechanism.)

  **`emitToolCallUpdated` does NOT share `emitMessageDelta`'s
  `Bridge.projectedMessages` guard** (W7 fifth fix-pass review P0-1): a
  `tool_call_updated` record's `MessageID` is the owning ASSISTANT message,
  which always commits BEFORE any of its own tool transitions publish
  (`runtime/tool_preparation.go`), so that message is already in
  `Bridge.projectedMessages` by the time the first such event reaches
  `Emit` -- a prior fix pass that added the same guard there suppressed
  EVERY live tool-call event on every tool-calling turn, the common case,
  not an edge case. Tool-call native dedup is keyed on the CALL instead:
  `Bridge.toolCallNativeSuppressed` (set when the assistant message's own
  committed-projection emission already included that call's native
  `function_tool_call` representation), `Bridge.toolCallLiveStartSent` (the
  live path's own dedup, since a durable `tool_call_updated` record repeats
  Name/Arguments at every phase), and `Bridge.toolCallResultSent` (dedup
  for the terminal `TOOL_CALL_RESULT` against the separate result
  message's own later commit). See `docs/architecture/agui-events.md`'s
  "Committed-projection emission" section for the full mechanism. A miss --
  the named message not (yet)
  present in a reload -- is a benign, non-fatal skip (`Bridge.liveErr` is
  reserved for a hard reload failure; `Bridge.BenignCommitMisses` counts
  it for host observability). `TestReplayForwardsMessageCommittedDuringReplayWindow`,
  `TestBridgeEmitLiveMessageCommittedProjectsDurableContent`, and
  `TestReconnectDoesNotDuplicateNativeContentForMessageCommittedDuringReplayWindow`
  prove both modes, and the message-level dedup, end to end against a real
  SQLite store. `TestBridgeDeliversFullStreamingTextThenToolCallTurn` and
  `TestBridgeDeliversToolCallTurnWithNoPrecedingTextDelta`
  (`agui/tool_call_p0_regression_test.go`) prove the per-call tool dedup
  above end to end, against a real SQLite store, driving `CreateToolCall`/
  `ClaimToolCall`/`SettleToolCall` and `runtime.BuildToolSettlement` for
  both DeliveryMode directions -- text streamed before the tool call, and
  no text streamed at all. `NewBridge`'s signature grew a
  required `(store session.Store, contentLimits session.ContentLimits,
  includeReasoning bool)` triple (a nil store disables the new path;
  existing classic-only tests pass nil), a breaking constructor change per
  this repository's no-compatibility-shim policy. `includeReasoning`
  defaults to `false` at the `transport.SSEConfig.IncludeReasoning` host
  boundary: a host must explicitly opt in, attesting
  `agui.GateProviderReasoningStorage` is satisfied (a host attestation this
  package trusts and does not independently verify -- the Gate constant
  itself is a documentation label no code path reads), before durable
  reasoning content blocks are included in the message snapshot, live
  commit reprojection, or the live `EventMessageDelta` path uniformly.
  `agui.Replay`/`agui.Reconnect` both gained the same `includeReasoning
  bool` parameter. `replay()` forwards every non-`LiveOnly` durable event,
  including `session.MessageCommittedEventKind`, to `bridge.Emit`
  unconditionally; `Bridge` itself tracks every message ID already emitted
  through the committed-projection path for the life of a connection and
  `emitLiveMessageCommitted` skips a notification naming one already in
  that set, rather than replay unconditionally dropping every
  `message_committed` event regardless of whether the message it names was
  actually covered by the snapshot (an earlier fix pass's version of this
  did the latter, which silently dropped a message that committed strictly
  during the replay window).
- **Rich transport ingress (`transport/rich.go`)**: `DecodeUserMessage`
  decodes a native AG-UI `types.InputContent` list into a
  `runtime.UserMessage`, mapping text/image/audio/video/document onto their
  `session.ContentBlock` (bounded: 16MiB body, 64 content fragments, every
  media block requires exactly one URL/base64 source). `EnqueueHandler` and
  `ResumeTargetedHandler` adapt application-owned routes to
  `runtime.Enqueue`/`runtime.ResumeRun`; both run the host's auth callback
  before reading the request body or resolving any session/run content --
  `TestResumeTargetedHandlerNeverInfersPermissionFromTargetPossession` and
  `TestEnqueueHandlerAuthFailureNeverReachesEnqueue` prove a well-formed,
  in-bounds target id or idempotency key never substitutes for a failed
  auth call. `ResumeTargetedHandler` bounds target count (256) and per-id
  length (512 bytes); `EnqueueHandler` requires a bounded `Idempotency-Key`
  header. Not yet wired as the default ingress path in `SSEHandler` or
  `examples/minimal-server`, which decodes its own request body inline
  (`examples/minimal-server/main.go`). The classic `transport.DecodeMessages`
  this paragraph previously described as "the default for existing callers"
  had zero callers anywhere in the module (its own declaration and doc
  comment were the only two matches) and has been removed as part of W8's
  unused-classic-public-entrypoint cleanup (Definition of done item 6).
- **Not implemented by this pass** (see `docs/architecture/agui-events.md`):
  `run_paused`/`InterruptTargetV1` construction from durable approval
  records, `run_resumed`, `attempt_replaced`, and subagent lifecycle
  mapping (the runtime does not emit any `Subagent*EventKind` anywhere yet,
  so there is nothing for a mapping to consume); transient per-block live
  deltas via `convert.TransientEventForBlock` (today's live path still uses
  the classic per-delta emitter methods); `watch/`'s bounded public block
  state and block-indexed live overlay; the observability typed-callback
  adapters and single accounting source. Each of these touches a
  deeply concurrent or correctness-sensitive existing subsystem (interrupt/
  approval identity validation, the watch service's live overlay, or
  double-counting-safe observability accounting) that this pass judged
  required its own careful grounding and test pass rather than a partial,
  unverified change.

## W8: external-consumer fixtures and publication validation

Status: landed. `testdata/external-consumer/agentic_fixture_test.go` (package
`consumer`, always copied by `check.sh`) is a fresh set of fixtures proving
the agentic adoption composes from a genuine external module boundary --
real `eino-agent`, `eino-providers`, and Eino/AG-UI constructors against fake
native HTTP/SSE transports and a real SQLite store, never hand-built
messages standing in for provider translation. `bash
testdata/external-consumer/check.sh` (local mode, no Docker) passes with
these fixtures included, via `go test -race ./...` inside the temporary
consumer module `check.sh` generates. `go test ./testdata/external-consumer/
-race` still cannot compile from the repository root --
`github.com/mattsp1290/eino-providers` is deliberately not a root
dependency (see `docs/dependency-status.md`), so resolving it fails -- but
that is a repository-root-only limitation: inside the generated consumer
module, `eino-providers` is a required dependency, so `-race` compiles and
runs there, and `check.sh`'s non-Postgres `go test` call now passes
`-race` (added by this fix pass; verified locally in ~8s). The Postgres
path (`EINO_AGENT_CONSUMER_POSTGRES=1 check.sh`, `-tags
postgres_integration`) does not yet pass `-race` and is unchanged here.
`EINO_AGENT_CONSUMER_POSTGRES=1 check.sh` and published-mode `check.sh`
(unsatisfiable at any currently published version -- see
`docs/dependency-status.md`) are gates the coordinator runs.

- **Native `AgenticModel` generate/stream equivalence and continuation**
  (`TestPublicNativeAgenticModelGenerateStreamEquivalenceAndContinuation`):
  drives `github.com/mattsp1290/eino-providers/claude.NewAgenticModel`
  against a fake `httptest` native Anthropic Messages server serving both a
  JSON response and an SSE stream with identical content (message start,
  thinking/signature/text deltas, `message_stop`), asserts `Generate` and
  concatenated `Stream` chunks are content-equivalent, round-trips the
  private reasoning signature through `SplitAgenticContinuation`/
  `RestoreAgenticContinuation` (public projection carries no signature;
  restore brings it back exactly), then drives one real durable turn through
  `runtime.StreamingOrchestrator` and a real SQLite store using
  `model.NewAgenticStreamerWithProviderState` plus
  `model.NewTypedExtensionStateCodec` to capture that same signature as
  private block state.
  - **Response-meta identity state**: a provider-declared marker is removed
    by the plain streamer and captured as bounded private state by the
    state-aware streamer. Its arbitrary native sidecar is never serialized;
    a restored request receives only the nested JSON identity map.
- **Ordered media/citations, function call, and server/MCP records survive a
  real SQLite reopen**
  (`TestPublicOrderedContentCitationsServerAndMCPRecordsSurviveReopen`):
  proves two things through two different, honestly-labeled seams.
  (1) A real model turn emits an assistant message with an
  `assistant_gen_text` block carrying a Claude web-search citation
  (`claude.TextCitation`/`CitationWebSearchResultLocation`), an
  `assistant_gen_image` block, and a real `function_tool_call` that a
  registered tool executes; ordering, citation content, and the function
  call all survive a close/reopen of the SQLite file, read back through
  `session/history.LoadAgentic`. (2) Separately,
  `server_tool_call`/`server_tool_result`/`mcp_tool_call`/`mcp_tool_result`/
  `mcp_list_tools_result` blocks are written directly through the store's
  own public contract (`AdmitRun`, `Execution`, `AppendMessage`/
  `AppendPart`, `FinalizeAssistantMessage`) and also survive reopen in
  order.
  - **Discovered, grounded gap** (not a test-writing mistake -- reproduced
    directly against `runtime/adk_model.go`): `adkModel.commit`
    (`errADKUnsupportedBlock`) fails a turn closed if the MODEL's own result
    carries a `server_tool_call`/`server_tool_result`/`mcp_tool_call`/
    `mcp_tool_result`/`mcp_list_tools_result` block -- the current typed-ADK
    adapter accepts only text/reasoning/media/function-tool-call blocks
    (plus `mcp_tool_approval_request` when an approval binding is wired) as
    assistant OUTPUT, even though `session.ContentFromAgenticMessage` (the
    content/store layer) fully supports encoding/decoding all 20 kinds.
    This is consistent with, and broader than, the W5 "known gaps" note
    that ADK's own tools node cannot represent `tool_search_result` either.
    No live model-turn path can legally produce server/MCP output content
    today; part (2) above is therefore a store-contract proof, not a
    runtime/ADK proof, and is labeled as such in the fixture itself.
- **Tool search discovers a deferred tool, then the model calls it by alias**
  (`TestPublicToolSearchDiscoversDeferredToolThenAliasExecutes`): a real
  `composition.Registrar.ToolSearch` registration plus a `Deferred: true`
  tool with an `Aliases` entry; the scripted model calls `tool_search` with
  `select:get_weather`, then calls the tool by its alias `weather`; the
  fixture asserts the durable `tool_search_result` block, the durable
  `function_tool_call` block under the model-requested alias name, and that
  the executor observes `RequestedName == "weather"`.
- **`adk.TurnLoop`: two completed turns under one run**
  (`TestPublicTurnLoopTwoCompletedTurnsUnderOneRunAgainstSQLite`): `Start`
  then an immediate `Enqueue` (before the loop idles out) against a real
  SQLite store; asserts `store.ListTurns` returns two turns with ordinals 1
  and 2, distinct assistant messages, and `store.ListModelRequests` returns
  two ledger rows with distinct `InvocationID`s.
- **MCP approval pause, checkpoint reopen (simulated process restart), and
  resume** (`TestPublicApprovalCheckpointSurvivesProcessRestartAndResumes`):
  a scripted model result carrying `MCPToolApprovalRequest` pauses the run
  at the model boundary; `handle.AwaitPause()` reports the current-generation
  `adk.InterruptCtx.ID`; the SQLite pool is closed entirely and reopened
  from scratch with a brand-new store handle, registry, and orchestrator
  (no in-process state survives); `ResumeRun(..., ResumeRequest{Targets:
  map[string]any{interruptID: "approve"}})` against the reopened store
  completes the run, commits the durable `mcp_tool_approval_response`
  block, and the public `session.Run` record carries no private material.
- **Enhanced (multi-part) streamed tool results survive reopen**
  (`TestPublicEnhancedToolResultPartsSurviveReopen`): a tool's
  `Definition.ExecuteRich` returns a `tools.RichResult` with a text part and
  an image part (`runtime.ToolResultPart`/`ToolResultMedia`); after a real
  turn and SQLite reopen, the durable `function_tool_result` block's
  `Content` carries both items in order, distinct from the classic
  single-text-part shape a scalar `Execute` result would produce. Also
  documents, by construction, that the zero-value `runtime.RetentionPolicy`
  (`MaxInlineBytes: 0`) degrades every part to an omission record --
  `Retention: runtime.RetentionPolicy{MaxInlineBytes: 4096}` is required for
  a rich result's parts to actually retain content, a real fail-closed
  default a host must configure per tool, not a fixture bug.
- **Typed ADK summarization middleware writes a durable `ContextEpoch`,
  surviving reopen**
  (`TestPublicSummarizationMiddlewareWritesDurableContextEpochSurvivingReopen`):
  mounts `examples/agentic-middleware.Mount` (a real, already-reviewed
  public example package, imported directly -- not copied -- since it lives
  outside `testdata/` and is part of the published module) with only the
  `summarization` recipe enabled and `TriggerContextMessages: 1`; two real
  turns trigger summarization on the second; `store.ListContextEpochs`
  shows a `Trigger == "summarization"` epoch with a non-empty
  `SummaryMessageID`; the epoch (same ID and `SummaryMessageID`) is still
  present after closing and reopening the SQLite file.
- **AG-UI decode of native input, plus real Bridge/Replay projection of
  committed content -- eino-agent-doj and eino-agent-6wj documented, not
  hidden** (`TestPublicAGUIDecodesNativeInputAndReplayProjectsCommittedContent`):
  `transport.DecodeUserMessage` decodes a real `types.InputContent` JSON
  body into a `runtime.UserMessage`, admitted through `orchestrator.Start`;
  the same real `agui.NewBridge`/`agui.Replay` entry points
  `transport.SSEHandler` uses in production replay every durable event for
  the session into a buffer. The fixture asserts:
  - the final assistant text reaches the replayed stream, and no
    `PRIVATE_` sentinel does;
  - **eino-agent-doj** (AG-UI replay re-emits a tool call's whole lifecycle
    a second time on reconnect, since `emitMessageSnapshot`'s replay path
    never calls `recordNativeToolDelivery`/`markToolCallResultSent` the way
    the live path does): the fixture asserts the tool name occurs **at
    least twice** in one full replay -- the actual current (buggy)
    behavior -- rather than a single-emission contract that does not hold;
  - **eino-agent-6wj** (`agui/bridge.go`'s `toolPayload` decodes
    `content`/`structured`, but the durable wire payload carries
    `output`/`error`/`metadata`): the fixture asserts the replayed tool
    result carries the synthesized `{"status":...}` stub, and explicitly
    fails itself (naming the bead) if the real tool output ever appears
    instead, so a future fix is caught rather than silently re-validated
    against a fixture written to expect the bug forever.

### Publication validation (exact evidence)

Every external pin below was verified with `GOWORK=off go mod download
-json <module>@<version>` in a fresh, empty temporary module (no
replacement, no workspace, no vendor tree, no sibling checkout):

| Module | Version | Origin commit | Module checksum |
| --- | --- | --- | --- |
| `github.com/mattsp1290/eino-providers` | `v0.0.0-20260912022125-79248358b8e6` | `79248358b8e6324bbdb1f014526629f82e6bce90` | `h1:yhGEAfP0NTXwsNBiEQGDR20iZK4Lnk2N6o2JbGDcivo=` |
| `github.com/mattsp1290/eino-agui` | `v0.1.2-0.20260910210826-ed64f77f3f16` | `ed64f77f3f16d8eb0f63f1cc34b985b870cdde88` | `h1:DlwVUYzSDmYOO9oxyEygARKzSVxfpR7M4UqCyaM8v9o=` |
| `github.com/mattsp1290/ag-ui/sdks/community/go` | `v0.0.0-20260909025854-aaa75b54d572` | `aaa75b54d572be8cd1d51c72e951273c5b893ed0` | `h1:ymOBlna6bESjwEgbyaaunIKrDNC5eytwhRcsYeBCNDI=` |

`github.com/mattsp1290/eino-providers`'s own `go.mod` declares no `replace`
directive, and its module graph resolved every transitive dependency
(including `github.com/cloudwego/eino-ext/components/model/claude` and its
own SDK dependencies) through the public proxy with no manual intervention
-- a self-contained resolvable module graph on its own. `github.com/mattsp1290/eino-agui`'s
own `go.mod` confirms, by direct inspection, that it `require`s
`github.com/ag-ui-protocol/ag-ui/sdks/community/go
v0.0.0-20260909025854-aaa75b54d572` and separately `replace`s that exact
path to the `mattsp1290` fork at the identical version -- so this repository
(and, transitively, every consumer of its AG-UI packages) is NOT a
self-contained module graph: the root replacement is mandatory, documented
precisely in `README.md`'s Module Baseline section, `docs/consumer-guide.md`'s
Installation section, and enforced mechanically by
`testdata/external-consumer/check.sh`'s own `go.mod edit -replace` calls
(exactly 2 replace directives in local mode, exactly 1 -- the AG-UI fork --
in published mode).

`github.com/mattsp1290/eino-providers` is deliberately NOT a dependency of
`eino-agent`'s own root `go.mod` -- the library stays provider-agnostic, and
Go's `testdata/` exclusion means `go mod tidy` never sees
`agentic_fixture_test.go`'s import of it anyway (confirmed empirically:
manually adding the `require` and running `go mod tidy` silently drops it
again, since nothing outside `testdata/` imports it). Instead,
`testdata/external-consumer/check.sh` pins the exact verified pseudo-version
explicitly via its own `go mod edit -require` call before `go mod tidy`, so
the external consumer module -- the one that actually combines `eino-agent`
with a concrete native provider -- resolves it deterministically rather
than whatever the module's default (untagged) branch head happens to be at
run time. `bash testdata/external-consumer/check.sh` (local mode) passed
with `eino-providers` selected at exactly that pin, unreplaced, and `go mod
verify` reporting "all modules verified".

`eino-agent-td8` stays open for the two `eino-providers` deliverables its
own response still lists as incomplete: a merged immutable release tag (no
tag exists upstream today, so every pin above remains a pseudo-version) and
the per-cell capability matrix plus native-byte fixture evidence beyond what
this fixture file itself exercises. This W8 pass does not claim either.

### Not delivered by this pass, and why

- A live, credentialed native-provider round trip (real Claude/OpenAI/Gemini
  credentials, a real tool call settled through a real provider turn after a
  process restart) is an explicit bounded opt-in per the plan; no
  credentials were available in this environment. This is reported as an
  external validation limitation, not a passed capability.
- `make postgres-test`, `make postgres-race`, and
  `EINO_AGENT_CONSUMER_POSTGRES=1 testdata/external-consumer/check.sh`
  require Docker the coordinator runs separately to avoid contention; this
  pass ran `make check` and non-Docker `go test`/`go vet`/`gofmt` only.
  `agentic_fixture_test.go` is copied unconditionally, so it also runs
  under the PostgreSQL consumer mode once the coordinator executes it.
- No new authoritative root-level (`store/postgres`/`runtime`,
  `postgres_integration`-tagged) storage/recovery test was added: the
  storage primitives underlying every capability this pass proves through a
  live durable turn already have existing PostgreSQL-tagged coverage from
  W1-W7 (for example `durable_identity`, `turn_loop_pause_resume` in
  `POSTGRES_REQUIRED_SUITES`), and the two genuinely new proofs above
  (native-provider continuation, server/MCP direct-store content) are
  exercised against SQLite, consistent with every existing
  `testdata/external-consumer/` fixture. One exception: `ContextEpoch`
  *storage* is covered under required suites
  (`TestPostgresRuntime/admission` and `/optional_references`, plus the
  stale-fence cases, all in `POSTGRES_REQUIRED_SUITES`), but an epoch
  *produced by the typed summarization middleware specifically* --
  `TestPublicSummarizationMiddlewareWritesDurableContextEpochSurvivingReopen`
  -- has its only durable proof in this pass's new SQLite fixture; no
  PostgreSQL-tagged case exercises the summarization-triggered path.
  `Makefile`'s `POSTGRES_REQUIRED_SUITES` is therefore unchanged by this
  pass; this is a deliberate scope decision, not an oversight.
- The two `eino-agent-td8` deliverables (provider-side per-cell capability
  matrix, native-byte fixture evidence beyond this file) and a merged
  immutable release tag remain open on `eino-providers`' side.
- The `errADKUnsupportedBlock` server/MCP-as-model-output gap remains a real,
  reproduced finding from this pass.
- "Composed agentic graph nodes" (the plan's phrase for
  `01-feature-inventory.md` row 8/9, classified "Upstream through
  composition") is not given a NEW `testdata/external-consumer/` fixture by
  this pass. That classification's own acceptance bar is "the real public
  Eino API in a runnable consumer example" -- already satisfied by
  `examples/agentic-graph` (a real `compose.Graph` built from
  `AddAgenticChatTemplateNode`/`AddAgenticModelNode`/`AddAgenticToolsNode`
  over `tools.WrapEnhanced`, landed and tested in W4). This is a scope
  decision (the bar is already met by an existing public example, not a
  gap this pass introduces), stated explicitly rather than silently
  assumed covered.
