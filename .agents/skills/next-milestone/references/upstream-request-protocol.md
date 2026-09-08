# Upstream Request and Blocking Protocol

Read this reference before the startup resume gate, and reuse it after selection when a required public contract appears sibling-owned. Request records are the only external artifacts this skill may write.

## Deterministic helper

Use `../scripts/request_records.py` for inspection, complete-set creation, and status transitions. It uses only Python's standard library and requires `--projects-root <absolute-path>` on every command; it never defaults to the real home directory.

```text
python3 scripts/request_records.py inspect --projects-root <root> --consumer eino-agent
python3 scripts/request_records.py create-set --projects-root <root> --manifest <manifest.json>
python3 scripts/request_records.py transition-set --projects-root <root> --manifest <manifest.json>
```

The helper owns deterministic filesystem, metadata, digest, and cooperating-writer invariants. The agent still owns Git/module identity checks, ownership judgment, request wording, user authorization, response verification, and interpretation.

## Outbound resume gate

Run `inspect` before ordinary frontier discovery. It searches direct project children for requests whose exact scalar `Blocker consumer` is `eino-agent`, matches same-named responses, and validates status and request-set consistency.

- `open` always blocks.
- `resolved` still blocks until the response, referenced committed public code, tests, and consumable version/pin are verified.
- `withdrawn` and `superseded` are historical.
- malformed metadata, unsafe paths, inconsistent sets, and missing set members block with exact repair actions.

Report every blocker together and stop before refreshing candidates. Do not confuse these outgoing blockers with incoming consumer requests under the `eino-agent` project directory.

## Request threshold and ownership preflight

Create or reuse one request per distinct sibling-owned public contract only when all are true:

1. The selected milestone needs it for the bounded acceptance path.
2. Current target code, tests, plans, requests, responses, and releases provide no usable verified public contract/pin.
3. Local implementation would duplicate established ownership, import private code, weaken safety/durability, or create a speculative compatibility surface.
4. The verified target and smallest acceptable public behavior can be named precisely.

Do not request optional polish, hypothetical parity, roadmaps, ordinary `eino-agent` decisions, or already usable dependencies.

Before writing, resolve each target checkout and verify basename, Git root, module/repository identity, guidance, revision, worktree status, public contracts, releases, and existing equivalent records. Complete the whole dependency map, group by owner, and assign one `YYYY-MM-DD-<selected-milestone-slug>` set with a deterministic complete member list.

Reuse an equivalent open request with stable `Request identity` read-only. Reverify a resolved equivalent. Historical withdrawn/superseded records are not silently reopened. Never bundle different owners.

## Path and authorization boundary

Every destination is a direct child:

```text
<canonical-projects-root>/<verified-target-repo>/requests/YYYY-MM-DD-<short-kebab-slug>.md
```

Filenames must match `^[0-9]{4}-[0-9]{2}-[0-9]{2}-[a-z0-9]+(?:-[a-z0-9]+)*\.md$`. Reject absolute/nested slugs, traversal, symlink path components, identity disagreement, and changes between preflight and mutation.

Validate every body and destination first. Then show the user every target, exact canonical destination, selected milestone, purpose, and `new`/`reused` disposition. Selection is not write authorization. Obtain explicit authorization for those exact new destinations and record its RFC3339 time plus the SHA-256 of newline-joined canonical destinations. Reuse is read-only.

Pass only that manifest to `create-set`. Files are created exclusively in sorted order and never overwritten. The helper pins the canonical projects root, direct project child, and `requests/` child with no-follow directory descriptors before leaf mutation, and rechecks every visible inode before success. A partial set remains durable; report created and missing members and stop. Recovery reruns full preflight and may create only the recorded missing members after authorization for still-new destinations.

## Canonical request body

Place these fields in this exact order before the sections:

```markdown
# Request: <consumer-visible public contract>

- **Requested by:** `eino-agent` next-milestone selection
- **Blocker consumer:** `eino-agent`
- **Request status:** `open`
- **Date:** YYYY-MM-DD
- **Priority:** <why it blocks the selected journey>
- **Selected milestone:** <selected milestone name>
- **Request set:** <YYYY-MM-DD-selected-milestone-slug>
- **Request set members:** <complete comma-separated target/filename list>
- **Request identity:** `eino-agent/<verified-target-repo>/<contract-slug>/v1`
- **Target repo:** <module-or-repository> (<absolute-resolved-local-checkout>)
- **Pinned commit under evaluation:** <full SHA>
- **Consumer:** `eino-agent`

## Background
...

## Ask
...

## Out of scope
...

## Acceptance
...

## Response and unblock contract
...

## References
...

## Status history
- YYYY-MM-DD — `open`: created for <selected milestone>.
```

The response contract requires a same-named response, a decision or committed implementation, a consumable pin, and independent verification. Request bodies describe current evidence and the smallest acceptable public behavior, label unestablished API shapes `proposed`, keep host/sibling ownership explicit, and contain no secrets, private prompts/session content, reasoning, or sensitive payloads.

`Request identity` is the stable equivalence key: `eino-agent/<target-kebab>/<contract-kebab>/v1`. It is independent of the selected milestone. A reused record remains in its original set and is never rewritten into a new one. A non-equivalent same-day collision stops for a more specific slug and renewed authorization; never append a numeric suffix silently.

## Manifest and result contracts

All JSON objects reject unknown keys. Members are sorted by `(target_repo, filename)` with unique destinations and identities.

`eino-agent-next-milestone/request-create-set/v1` has `schema`, exact `consumer: eino-agent`, `request_set`, `authorization`, and nonempty `members`. Authorization has only strict RFC3339 `confirmed_at` and lowercase `destinations_digest`. Each member has `target_repo`, `filename`, `request_identity`, `target_checkout`, `target_module`, `target_commit`, `body`, and `mode`. Mode is `create` or `reuse`; reuse also has `expected_sha256`, while create forbids it. A reuse body must equal and validate against the existing record. Same-set records agree on the complete declaration; a linked record retained in an older set includes itself and has that acyclic original-set closure validated independently.

`eino-agent-next-milestone/request-transition-set/v1` has `schema`, exact consumer, `request_set`, authorization, exact `from_status: open`, `to_status` (`withdrawn`, `superseded`, or `resolved`), single-line `reason`, canonical `history_line`, and exact-set members only. Linked reused records retained in older sets are validated but are not transitioned with the newer set. Every transition member has `target_repo`, `filename`, `request_identity`, and `expected_sha256`. Resolved members also have same-named `response_filename`, full `verified_target_commit`, and nonempty `verified_pin`.

For create authorization, digest newline-joined sorted canonical request destinations. For transition authorization, digest newline-joined strings `<canonical-request-path> -> <to_status>`.

Every command emits one `eino-agent-next-milestone/request-result/v1` JSON object. It reports `operation`, `status` (`complete`, `blocked`, or `partial`), sorted `created`, `reused`, `transitioned`, `untouched`, and content-free errors with stable `code`, `path`, and `message`; inspect also returns normalized records. Exit codes are `0` complete, `2` blocked inspection or validation/authorization/identity failure before mutation, `3` create conflict/partial create, `4` lock/digest conflict/partial transition, and `1` unexpected internal failure. Validation precedence is schema, canonical projects root, authorization/scalars, member order/uniqueness, safe direct-child paths, request bodies/sets, target identity, action classification, locks, then deterministic mutation. To recover a partial transition, rebuild the full-set manifest from fresh digests; members already at the exact authorized target status and history are revalidated after all locks are held and reported `untouched`, while remaining open members transition under the same lock and authorization rules.

## Status transitions

Show exact files, target status, and reason, then obtain explicit authorization. `transition-set` revalidates canonical paths, identities, statuses, sets, response paths, and expected digests. It acquires adjacent exclusive locks in sorted order, makes only the status/history edit via same-directory temporary files, rechecks identity/stat/digest immediately before atomic replace, and verifies the result. It removes only its own locks.

The lock prevents lost updates only among cooperating helper processes. It cannot provide compare-and-swap against editors or other processes that ignore locks; abort on detectable drift and never claim stronger safety. Cleanup records the device/inode identity of every helper-created leaf, temporary, and lock entry and removes it only while that identity remains visible. A non-cooperating process can still replace a pathname between the final identity check and unlink because portable filesystem APIs provide no conditional unlink; this is another reason the lock protocol is cooperative rather than arbitrary-filesystem safety. Update every open member of the selected request set for withdrawal/supersession, never delete records, and never alter another consumer's request.

## Mandatory stop

After creating or reusing unresolved requests, mark the milestone `blocked upstream`, report each target and clickable path, new/reused status, why it blocks, and exact response/code/test/pin evidence that clears it. Stop before an implementation plan, fallback, fork, adapter, Beads issue, or local duplicate.
