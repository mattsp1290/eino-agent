# Contained Notifications

Issue: `eino-agent-30b`

## Objective

Collapse observer failure behavior to the only mode the runtime uses: report a failure and continue notifying other observers.

## Changes

### `extension/notify.go` and registry configuration

- Change `Notify` to return nothing.
- Change the constructor to `NewNotification(contract, clone)`.
- Delete `NotificationPolicy`, `NotificationContained`, `NotificationReturnFailures`, and `Failures`.
- Delete `Notification.policy`, `registrationEntry.policy`, failure accumulation, and any registry/configuration fields that select a notification policy.
- For each observer error, call the existing failure reporter and continue iteration.
- Keep `maxReportedFailures` for cleanup error aggregation only; it does not bound notification reporting.

### Runtime callers

- Replace discarded assignments such as `_ = extension.Notify(...)` with direct calls.
- Remove policy plumbing from construction paths, tests, examples, and current docs.
- Do not add a compatibility wrapper for the old result-bearing function.

## Required behavior tests

- One observer failure is reported and does not prevent later observers from running.
- Every failed observer is reported, all later observers still execute, and no notification-reporting cap is introduced.
- Cleanup error aggregation remains bounded independently.
- A nil/absent reporter follows the package's documented safe behavior.
- Registry cleanup reporting remains unchanged.

## Verification

- Search for all deleted policy/result symbols, old three-argument `NewNotification` calls, and discarded `Notify` results.
- Run focused `extension` and `runtime` tests, then the repository quality gate.
