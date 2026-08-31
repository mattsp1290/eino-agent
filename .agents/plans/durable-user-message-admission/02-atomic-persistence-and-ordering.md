# Atomic persistence and ordering

## Goal and prerequisite state

Commit the submitted user message, canonical text part, assistant placeholder,
and existing admission records as one run-fenced transaction. Make user-before-
assistant replay independent of generated ID lexical order.

Prerequisite: the public request and private admission request from
[01-public-admission-contract.md](01-public-admission-contract.md) compile.

## Repository evidence

- `runtime/admission.go:admitter.admit` already freezes the complete provider
  snapshot before opening `Store.WithinTx`; this must move inside the
  transaction after `AdmitRun` so history and ownership cannot diverge.
- `runtime/admission.go:admissionSession` constructs fresh timestamps for every
  run, while `store/sqlite.CreateSession` requires an existing record to be
  byte-identical. The current code therefore conflicts on a normal second run.
- `runtime/admission.go:admitDurable` calls `CreateSession`, `AdmitRun`, obtains
  an `ExecutionStore` with the generated claim token, and persists epoch,
  assistant, and event records in the same transaction.
- `session.ExecutionStore.AppendMessage` and `AppendPart` are the only generic
  message mutations after admission; SQLite verifies the run fence in the same
  transaction as each write.
- `session/history.Project` already understands `RoleUser` plus `PartText` with
  `{"text":"..."}`.
- SQLite orders messages by the separate `created_at` column and uses the same
  timestamp encoding for replay cursor comparisons.

## Exact change surface

### Runtime admission records

- `runtime/admission.go` (existing): add proposed private helper
  `getOrCreateAdmissionSession` at the existing `admitDurable` boundary. Call
  `GetSession` first; create only on `ErrNotFound`.
- For an existing session, compare the candidate's ID, parent session ID,
  workspace ID, canonical directory, title, and metadata while ignoring only
  `CreatedAt` and `UpdatedAt`. Return `session.ErrConflict` on drift. Reuse the
  entire stored session record without updating it when identity matches.
- Treat `Request.Metadata` as creation-time session metadata. Update repository
  adapters so later runs submit the same metadata; do not place changing AG-UI
  run IDs in it.
- `runtime/admission.go` (existing): add proposed private builders
  `admissionUserMessage` and `admissionUserPart` at the existing admission
  record-builder section.
- `runtime/admission.go` (existing): extend proposed private `admittedRun` with
  `UserMessage session.Message` and `UserPart session.Part` so tests and later
  runtime code receive the committed canonical records rather than rebuilding
  them.
- `runtime/admission.go` (existing): change `admissionRun` so
  `Run.ParentMsgID` is the generated user message ID.
- `runtime/admission.go` (existing): change `admissionAssistantMessage` so
  `ParentID` is the generated user message ID.
- `runtime/admission.go` (existing): create the user envelope with
  `RoleUser`, the admitted session/run IDs, no assistant agent/model identity,
  and an empty `ParentID` because branching is out of scope.
- `runtime/admission.go` (existing): create one text part with generated ID,
  `Kind: PartText`, `Ordinal: 0`, and canonical JSON payload
  `{"text": request.UserMessage.Content}`.

### Durable write order

Keep one outer `Store.WithinTx` and execute this sequence:

1. Get and validate the existing session, or `CreateSession` on
   `session.ErrNotFound`.
2. `AdmitRun` with the user message ID as `ParentMsgID`. This establishes the
   active-run fence before any history used by the provider is read.
3. Call `LoadHistory` with the admission's cloned `history.Options` through the
   transactional store, append the one new Eino user message, and freeze the
   complete provider snapshot. Any projection or clone failure rolls back the
   just-admitted run.
4. Page `ListMessages` inside the same transaction and find the greatest
   durable `Message.CreatedAt` without relying on returned ID order.
5. Allocate the new user timestamp as `max(admissionNow,
   latestMessage.CreatedAt + 1ns)`; allocate the assistant at `userAt + 1ns`.
6. Acquire `store.Execution(RunFence{RunID, ClaimToken})` privately.
7. `StartContextEpoch`.
8. `AppendMessage` for the user.
9. `AppendPart` for the user's text.
10. `AppendMessage` for the assistant placeholder whose parent is the user.
11. `AppendEvent` for `run_started`, still correlated to the assistant message.
12. Return the committed records and freeze no additional caller-owned state.

Do not publish the start event or invoke `RunAdmittedPoint` until the outer
transaction returns successfully.

### Replay ordering

- `runtime/admission.go` (existing): add proposed private helper
  `latestAdmissionMessageTime` at `admitDurable`. It must consume every
  `ReplayCursor` page within the same transaction and select the maximum record
  timestamp. Empty history uses the admission clock unchanged.
- `store/sqlite/sql_helpers.go` (existing): replace variable-width
  `time.RFC3339Nano` output in `timeText` with a fixed-width UTC RFC3339 layout
  that always emits nine fractional digits. Preserve the empty string for zero
  time.
- `store/sqlite/store_test.go` (existing): add a test with an exact-second user
  timestamp, a one-nanosecond-later assistant timestamp, and IDs whose lexical
  order opposes conversation order. Assert both initial replay and cursor
  pagination return user before assistant.

The fixed-width timestamp representation is a direct greenfield correction.
Do not add a dual parser or rewrite migration for development databases.

## Intended behavior and failure semantics

| Boundary | Durable transcript result |
| --- | --- |
| Request validation, plan acquisition, or model resolution fails | No admission transaction starts; no new records exist. |
| Transactional history projection or snapshot cloning fails after `AdmitRun` | The run and any newly created session roll back; prior session/history remain unchanged. |
| Any write inside `admitDurable` fails | The transaction rolls back all records created by that attempt; any pre-existing session and transcript remain unchanged. |
| A second admission fails | The first session and transcript remain unchanged; no records owned by the failed run become visible. |
| `Start` returns a handle, then pre-execution policy fails | User and empty assistant remain; run settles failed. |
| Provider fails before assistant content settles | User and empty assistant remain; run settles failed. |
| Interrupt arrives before assistant content settles | User and empty assistant remain; run settles interrupted. |
| Assistant content settles, then later work fails | User plus already settled assistant/tool content remain; run terminal status reports the failure. |
| Run completes | User and settled assistant project once in conversation order. |

Additional invariants:

- Consumers never receive or need the admission claim token.
- No top-level `session.Store` message writer is added.
- Replaying the session after process restart uses messages and parts, not the
  model-request ledger or live event sink.
- The user part is content-bearing and therefore must not be copied into the
  `run_started` event's metadata-redacted payload.

## Tests and acceptance criteria

Update `runtime/admission_test.go` to assert:

- both committed message roles and the canonical user part;
- `Run.ParentMsgID == UserMessage.ID`;
- `AssistantMessage.ParentID == UserMessage.ID`;
- both messages and the part use the admitted session and run IDs;
- mutating the original public request after admission cannot change provider
  snapshot content or persisted content;
- an injected user-part failure rolls back every admission map;
- the existing injected event failure rolls back both messages and the part;
- provider failure does not erase the admitted user.

Add `runtime/admission_sqlite_test.go` (new under existing `runtime/`) for a
real SQLite integration suite. It must:

- start and settle two text runs with a frozen clock and reverse-lexical IDs;
- assert the original session record remains byte-identical when the second run
  reuses it;
- close and reopen the same database, call `LoadHistory` with reasoning
  excluded, and assert exact `user1, assistant1, user2, assistant2` content;
- page raw messages and assert the same order;
- use valid non-ASCII Unicode in one prompt and prove exact round-trip;
- wrap the real SQLite `session.Store` with a proposed test-only
  `failingAdmissionStore` that delegates `WithinTx` to SQLite, wraps the child
  transaction's `ExecutionStore`, and injects an error on the second
  `AppendMessage` after the new user and part have been written. Assert the
  prior session/transcript remains while the failed run, epoch, user, part,
  assistant, and start event are absent.

Focused verification:

```text
go test ./runtime -run 'Admission|Admit|History|SecondRun|Interrupt|Failure'
go test ./store/sqlite -run 'ListMessages|Transaction|Rollback'
go test -race ./runtime ./store/sqlite
```

Acceptance: there is no observable state containing only one side of a newly
admitted user/assistant pair, failed later admissions preserve earlier state,
and SQLite replay order does not depend on clock advancement or IDs.

## Dependencies, risks, and exclusions

- This package blocks lifecycle and release verification.
- `timeText` also orders events and model requests. Run the complete SQLite and
  store contract suites after changing its representation.
- The session-tail scan is linear in durable message count. Keep it inside the
  transaction for correctness, record the cost as a bounded first-contract
  tradeoff, and file a separate optimization issue only if measurements justify
  a store-level tail query.
- Derived one-nanosecond increments are ordering keys, not claims about
  wall-clock work duration.
- Do not alter `session.Message`, `session.Part`, `session.Store`,
  `session.ExecutionStore`, or `store/sqlite/schema.sql` unless implementation
  evidence proves the existing transaction cannot satisfy an acceptance
  criterion. Such evidence is a stop-and-replan condition.
- The plan relies on `Store.WithinTx` providing one consistency boundary for
  `AdmitRun`, `ListMessages`, and admission writes. If a custom backend cannot
  provide that isolation, it fails the existing store contract and must not be
  used to weaken this runtime guarantee.
