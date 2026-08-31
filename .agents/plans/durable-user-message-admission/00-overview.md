# Durable user-message admission

Status: Ready

Planning is complete. Implementation has not occurred.

## Application context

```json
{
  "application_context": {
    "has_active_users": false,
    "backward_compatibility_required": false,
    "feature_flags": "not-applicable",
    "confirmation_digest": "717f18582b3c3f595feed8bd52607eb3630930ab4e32df75074dc700ec4b9169",
    "confirmed_at": "2026-08-30T23:38:20Z"
  }
}
```

The user confirmed that `eino-agent` has no active users or external
consumers and that this change does not need backward compatibility for APIs,
stored data, configuration, or workflows. Replace the ambiguous admission API
directly. Do not add aliases, migrations, feature flags, dual-write paths, or
deprecated request fields.

## Change classification

- Change type: breaking public Go API correction plus transactional runtime
  persistence, replay verification, and a new release.
- Affected areas: `runtime` request validation and admission, SQLite replay
  ordering, runtime and SQLite tests, public examples and documentation, the
  external-consumer fixture, release metadata, and the external response.
- Unchanged boundaries: `session.Store`, `session.ExecutionStore`, the SQLite
  schema, provider execution, tool execution, reasoning policy, and transport
  ownership.

## Requested outcome and success criteria

`StreamingOrchestrator.Start` must accept one current user message, make the
runtime the sole writer of that message, and commit it with the admitted run
and assistant placeholder before execution can begin.

Success requires all of the following:

1. The public request accepts one text user message rather than an arbitrary
   provider-history slice.
2. A successful admission commits one `session.Message` with `RoleUser`, one
   canonical `PartText`, one assistant placeholder, the session, run, context
   epoch, and `run_started` event in one `Store.WithinTx` transaction.
3. The runtime generates the user message and part IDs. A consumer neither
   supplies a run claim token nor writes a transcript record.
4. `session.Run.ParentMsgID` and the assistant message's `ParentID` equal the
   admitted user message ID.
5. A synchronous admission error exposes no records belonging to the failed
   admission. A failed second admission preserves the pre-existing session and
   transcript byte-for-byte.
6. After admission succeeds, completion, interruption, provider failure, and
   pre-execution failure retain the user message. Failed or interrupted runs
   retain an empty assistant placeholder unless settled assistant content was
   already committed.
7. `runtime.LoadHistory` with `history.Options{IncludeReasoning: false}` returns
   the submitted user text and settled assistant text exactly once and in that
   order.
8. A second run reuses the existing durable session, obtains the first run's
   user and assistant messages from durable history, and appends only its new
   current user message.
9. History projection and `AdmitRun` share one store transaction. A contender
   cannot admit successfully with history read before another run's committed
   transcript.
10. Tests cover an empty session, a second run, concurrent/stale admission,
   completion, interruption,
   provider failure, transaction rollback, parent IDs, exact replay content,
   and close/reopen behavior with `store/sqlite`.
11. Public documentation states message ownership, ordering, failure
   semantics, the intentional API break, and the absence of a data migration.
12. The final response identifies a remotely published `v0.2.0` tag and its
    peeled full commit SHA only after published-mode consumer verification.

## Scope

- Replace `runtime.Request.Input` and `runtime.Request.ParentID` with the
  proposed `runtime.UserMessage` value on `runtime.Request.Message`.
- Admit text-only user messages and reject blank or invalid-UTF-8 text before
  run-plan or model acquisition.
- Generate durable user-message and user-part identities through the existing
  `runtime.IDGenerator`.
- Extend the private admission record builders and return value to include the
  canonical user message and part.
- Reuse an existing session when its immutable identity matches the admission
  candidate; preserve the original session record and reject identity drift.
- Move durable-history loading into the admission transaction after
  `AdmitRun`, then freeze the provider snapshot from that fenced view.
- Use the existing store transaction and run-fenced append methods for the new
  records.
- Allocate each admitted pair strictly after the session's latest durable
  message and make SQLite's text timestamp encoding fixed-width so replay order
  remains chronological under frozen clocks and opaque IDs.
- Update repository call sites, examples, tests, documentation, release
  metadata, and the external response.

## Non-goals

- Do not add TUI models, rendering, keymaps, path policy, or terminal behavior.
- Do not edit the `eino-tui` repository or add a TUI-owned transcript store.
- Do not expose run claim tokens or make message mutation public to consumers.
- Do not admit caller-supplied history, assistant messages, system messages,
  tool messages, reasoning, attachments, or multimodal Eino message graphs.
- Do not add session branching, compaction UI, remote transport, provider
  selection, or tool-policy changes.
- Do not change settled assistant/tool persistence or `history.Options`.
- Do not add a SQLite data migration or compatibility reader for the old
  timestamp representation.

## Constraints

- Keep `session` independent of `runtime`; the proposed API lives in
  `runtime/types.go`.
- Preserve the current `Store.WithinTx` admission boundary and
  `ExecutionStore` claim-token fence.
- Validate all proposed durable IDs before opening the transaction.
- Require every generated admission ID to be pairwise distinct; an ID collision
  is invalid admission, not a transaction fault-injection mechanism.
- Preserve the submitted text byte-for-byte after rejecting whitespace-only
  or invalid UTF-8 input. Do not trim or normalize admitted content.
- Store text with the existing canonical payload shape `{"text":"..."}` and
  `Ordinal: 0`.
- Publish durable records before `EventRunStarted` reaches live sinks or
  extension notifications.
- Treat IDs as opaque. Ordering must not depend on lexical ID order.
- Keep the external response outside the repository. Resolve it as
  `$HOME/.agents/projects/eino-agent/responses/2026-08-30-durable-user-message-admission.md`
  at implementation time.

## Repository findings

### Verified facts

- `runtime.Request` currently exposes `ParentID` plus
  `Input []*schema.Message`; `StreamingOrchestrator.providerInput` prepends
  durable history before `AdmitRun` and passes the combined provider graph into
  admission.
- `runtime/admission.go:admitDurable` commits the session, run, context epoch,
  empty assistant message, and `run_started` event inside one
  `Store.WithinTx`; it never writes a user message or part.
- The assistant placeholder and `session.Run.ParentMsgID` currently use the
  caller's `Request.ParentID`, even though the runtime never creates that
  parent message.
- `session.Store` already exposes transactional reads and run admission.
  `session.ExecutionStore` already exposes fenced `AppendMessage` and
  `AppendPart`; no new store method is required.
- `store/sqlite.Store.WithinTx` rolls back every child-store write when its
  callback returns an error or panics.
- `store/sqlite.CreateSession` accepts an existing ID only when the entire JSON
  record is byte-identical. Because `admissionSession` stamps new timestamps,
  the current runtime cannot admit a normal second run against SQLite.
- `session/history.Project` already maps `RoleUser` plus canonical `PartText`
  to an Eino user message, and `runtime.LoadHistory` delegates to that path.
- `store/sqlite.ListMessages` orders and pages by `created_at, id`.
  `timeText` currently uses variable-width `time.RFC3339Nano`, which is not a
  safe lexical order when an exact-second timestamp is adjacent to a
  fractional timestamp.
- `WithClock` does not promise strict advancement, and normal runtime tests use
  a constant clock. An intra-run user/assistant offset alone can therefore
  interleave two admitted pairs.
- Because `providerInput` runs before the transactional active-run fence, a
  contender can read history, another run can admit and settle, and the
  contender can then admit with a stale provider snapshot.
- The current runtime test store already clones all admission maps for
  transactional rollback and can inject part and event failures.
- `examples/minimal-server` accepts either one `message` string or a caller
  message slice. The AG-UI sketch converts the entire client message list into
  provider input. Both patterns would preserve consumer-owned history unless
  changed.
- The focused baseline command passed before planning:
  `go test ./runtime ./session/history ./store/sqlite ./store/storetest ./examples/minimal-server ./examples/ag-ui-go-server-example`.
- The request's pinned commit and current `HEAD` have the same admission gap;
  intervening changes do not implement user-message durability.

### Assumptions

- The selected TUI milestone needs text prompts only. Attachments and other
  multimodal parts can receive a separate typed admission design later.
- `origin` remains the authoritative public source for the root module and
  release tags.

## Key decisions

1. Introduce proposed `runtime.UserMessage{Content string}` and
   `runtime.Request.Message`. This gives the runtime a single canonical current
   message and prevents callers from resubmitting provider history.
2. Delete `Request.Input` and `Request.ParentID`. The confirmed greenfield
   context makes an alias or fallback unnecessary, and retaining either field
   would keep split ownership possible.
3. Generate the user message and text-part IDs inside `Start`. Set both
   `Run.ParentMsgID` and the assistant placeholder's `ParentID` to the generated
   user message ID.
4. Reuse `Store.WithinTx` and the private fenced `ExecutionStore`. A public
   admission-specific store method would duplicate existing atomicity and
   widen the consumer mutation surface.
5. Resolve an existing session inside the admission transaction, require its
   immutable identity to match, and allocate the new pair after the latest
   durable message. This enables repeated runs without trusting clock or ID
   monotonicity.
6. Call `AdmitRun` before loading or projecting history inside the same
   transaction. A successful admission therefore freezes a history view that
   cannot predate another committed run.
7. Publish `v0.2.0` after implementation. Replacing fields on the public
   `runtime.Request` is a semver-minor-breaking change from the `v0.1.x`
   series, so a new minor line is clearer than a patch tag or an unversioned
   SHA-only support claim.

### Rejected alternatives

- Keeping `Input []*schema.Message` and documenting “one user message” leaves
  an API that still accepts duplicate history and provider-only fields that
  cannot be reconstructed from durable parts.
- Adding a second durable-input field while retaining `Input` creates two
  sources of truth and requires precedence rules.
- Letting a consumer create the user message would expose mutation authority
  or require a dual write before `Start`.
- Persisting an opaque serialized Eino message would bypass the existing
  message/part history contract and make normal projection provider-specific.
- Sorting the admitted pair by generated IDs would rely on an ordering
  property that `IDGenerator` does not require.

## Change model

```text
Before
consumer history + current prompt
  -> Request.Input []*EinoMessage
  -> runtime loads durable history and appends Request.Input
  -> transaction: session + run + epoch + empty assistant + start event
  -> provider snapshot only owns the submitted prompt

After
current prompt only
  -> Request.Message runtime.UserMessage
  -> transaction:
       get/create matching session
       AdmitRun (active-run fence)
       load/project durable history + append one derived Eino user message
       freeze provider snapshot + allocate time after durable tail
       epoch
       user message -> canonical text part
       assistant placeholder (parent = user message)
       start event
  -> provider execution
  -> normal history projects the same committed user/assistant transcript
```

## Compatibility, rollout, migration, and rollback

- Public API: this is an intentional direct break. Every repository call site
  moves from `Input`/`ParentID` to `Message: runtime.UserMessage{...}`.
- Stored data: no schema migration is required because the user confirmed that
  no durable user data needs compatibility. Existing development databases
  may be recreated; no dual timestamp decoder is added.
- Session identity: `Request.Metadata`, workspace ID/root, title, and parent
  session identity are creation-time facts. A later admission must submit the
  same values; runtime reuses the original record and its timestamps rather
  than rewriting it.
- Configuration: no keys or environment variables change.
- Workflow: consumers submit only the current text message and read transcript
  state through `runtime.LoadHistory` or existing replay adapters.
- Rollout: merge code, tests, examples, and candidate documentation; run the
  full repository gate; publish `v0.2.0`; verify the public module from a fresh
  cache; then write the external response.
- Rollback before publication: revert the candidate changes as one unit.
- Rollback after publication: never move or delete `v0.2.0`. Fix forward on
  the next unused minor or patch version, rerun published verification, and
  name only the verified replacement in the response.
- Feature flags: not applicable. There is no old behavior path to preserve.

## Risks and stop/go gates

- Stop before admission writes if the proposed `UserMessage` is blank or
  invalid UTF-8, or if any generated ID is empty or duplicates another
  admission ID. Tests must prove that no store or plan/model side effect occurs
  for invalid input.
- Stop before execution if any durable admission write fails. The outer
  transaction must roll back records belonging to that attempt while
  preserving any pre-existing session and transcript.
- Stop if SQLite returns anything other than
  `user1, assistant1, user2, assistant2` for frozen clocks or
  reverse-lexicographic IDs. Do not mask this with test-only ID ordering.
- Stop if a second provider request contains the first user prompt twice or
  depends on caller-supplied prior history.
- Stop if a contender can read history before the active-run fence and later
  admit successfully after that history becomes stale.
- Stop before merging if any runtime call site still uses `Request.Input` or
  `Request.ParentID`.
- Stop before publishing if `v0.2.0` already exists at another commit. Select
  the next unused minor version and update every planned pin consistently.
- Stop before writing a completed response until the tag, peeled SHA, public
  module resolution, focused replay tests, and `make check` all pass.

## Decisions and open questions

- Resolved: no active-user or backward-compatibility gate remains.
- Resolved: feature flags are not applicable.
- Resolved: the first contract is text-only and runtime-owned.
- Resolved: existing store transaction and fencing APIs are sufficient.
- Resolved: publish a new minor release because the request shape changes.
- Blocking open questions: none.
- Non-blocking open questions: none. A tag collision is an execution gate, not
  an unresolved design decision.

## Document map

- [01-public-admission-contract.md](01-public-admission-contract.md): replace
  caller-owned provider input with one runtime-owned user message.
- [02-atomic-persistence-and-ordering.md](02-atomic-persistence-and-ordering.md):
  commit the user/assistant relationship and harden replay ordering.
- [03-verification-and-consumer-updates.md](03-verification-and-consumer-updates.md):
  prove lifecycle behavior and update examples, docs, and compile fixtures.
- [04-release-and-response.md](04-release-and-response.md): publish and verify
  the usable module pin before responding to `eino-tui`.
- [05-execution-handoff.md](05-execution-handoff.md): execute work packages in
  dependency order with integration and completion gates.
