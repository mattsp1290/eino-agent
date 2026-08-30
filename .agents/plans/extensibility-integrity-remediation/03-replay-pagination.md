# Replay Pagination

## Goal and prerequisites

Make the replay cursor truthful and keep each page query proportional to the page rather than the full session.

This work is independent of the extension lifecycle changes and can be implemented after the public API break is accepted in the overview.

## Repository evidence

- `session.ReplayCursor.AfterPartID` has no production reader or writer.
- `store/sqlite.ListMessages` selects `limit+1` messages, then selects every part in the session and filters decoded records in Go.
- The `parts_replay_idx` index already begins with `session_id` and `message_id`.
- `session/history.Project` consumes only `ReplayBatch.Next` and does not depend on a part cursor.

## Change surface

- `session/types.go`
  - Delete `ReplayCursor.AfterPartID` directly.
- `store/sqlite/messages.go`
  - Bind an `AfterMessageID` cursor lookup to the requested session.
  - Use a CTE or equivalent page subquery to select only parts whose message IDs are in the truncated `limit` page, excluding the `limit+1` lookahead row.
  - Preserve ordering by message creation time, message ID, part ordinal, and part ID.
  - Return no parts when the page contains no messages.
- `store/storetest/contract.go` and `store/sqlite/store_test.go`
  - Cover multiple pages with multiple parts per message.
  - Reject or report not found for a cursor from another session.
  - Prove each page contains only its messages' parts and no duplicates or omissions.
  - Insert malformed part JSON for an off-page message. Assert the current page succeeds and the page containing that message fails decoding.

## Invariants and acceptance criteria

- The public cursor contains only fields implemented by the store contract.
- `Next.AfterMessageID` remains stable for equal timestamps through the ID tie-breaker.
- A cursor cannot borrow ordering state from another session.
- SQLite performs part filtering before JSON decoding.
- The parts query never includes the message lookahead row.
- History projection returns the same ordered message content across page sizes 1, 2, and the default.

## Risks and exclusions

- Do not redesign event pagination in this work package.
- Do not add offset pagination.
- No migration is required because the cursor is not stored in SQLite.
