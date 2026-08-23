# Scope Validation and Settlement Reconciliation

## Work package 1: accept opaque session scope keys

Goal: make registration validation match the durable session identity contract.

Repository evidence:

- `session/types.go` defines `session.ID` as a string with no identifier regex.
- Runtime admission and `composition.Registry` require a non-empty session ID.
- `extension/types.go:validateTargetScope` accepts every non-empty session key, while `validateScope` uses `identifierPattern`.
- `composition.Registry.Mount` installs a synthetic lease at the component registration scope, so the extension mismatch blocks both callbacks and capabilities.

Exact change surface:

- Modify existing `extension/types.go:validateScope` so `ScopeSession` rejects only `scope.Key == ""`.
- Preserve `identifierPattern` for existing contract IDs, handler registration IDs, component instance IDs, and artifact names.
- Extend existing tests in `extension/extension_test.go` with registration and snapshot cases using opaque keys such as `user@example.com`, padded base64, and a non-empty key longer than 256 bytes.
- Extend existing `composition/registry_test.go` coverage so a component with callbacks/capabilities mounted at the same opaque session scope acquires for the matching session and remains absent for another session. Include the longer-than-256-byte key so the synthetic lease path cannot retain the identifier limit.
- Update `extension/extension_test.go:FuzzSessionScope` to skip only empty keys, so equality semantics are exercised for arbitrary opaque strings.

Acceptance criteria:

- Registration imposes no character, length, trimming, or normalization restriction beyond non-emptiness, and selection uses exact equality.
- The empty session key still returns `extension.ErrInvalidRegistration`.
- Invalid handler/component identifiers remain rejected.

Risk: whitespace-only IDs are valid opaque durable IDs under the existing non-empty contract. Do not introduce trimming in only this layer.

## Work package 2: detect either missing reserved result record

Goal: return every terminal tool call whose reserved message or reserved part is absent, while preserving idempotent repair when one record already exists.

Repository evidence:

- Existing `store/sqlite/store.go:ListUnreconciledToolSettlements` skips a call as soon as `GetMessage` succeeds.
- SQLite has private `getJSON`, `GetMessage`, and `AppendPart` helpers; no public `session.Store` expansion is needed.
- `SettleToolCall` appends both records and relies on exact-record idempotency in `AppendMessage` and `AppendPart`.
- A reconstructed message lacks fields such as the original `ModelID`, so a repair settlement must carry the exact existing message when only the part is absent.

Exact change surface:

- In existing `store/sqlite/store.go:ListUnreconciledToolSettlements`, independently load the reserved message and reserved part by their IDs.
- Treat `session.ErrNotFound` for either lookup as unreconciled; propagate other lookup errors.
- Skip only when both records exist.
- Populate `ToolSettlement.ResultMessage` with the existing message when present, otherwise reconstruct it from the terminal call as today.
- Populate `ToolSettlement.ResultPart` with the existing part when present, otherwise reconstruct it from the terminal call output as today.
- Keep lookup implementation private to `store/sqlite`; a small private part lookup helper is allowed if it clarifies the flow.
- Add a focused case in `store/sqlite/store_test.go` that creates a terminal call plus its exact reserved message but omits the part, confirms one unreconciled settlement, applies it, and confirms the part exists and a second listing is empty.

Acceptance criteria:

- Message present + part missing is returned and repaired.
- Message missing + part missing retains current repair behavior.
- Message present + part present remains reconciled.
- Repair remains idempotent and does not conflict when the existing message has non-zero metadata unavailable from the tool call.

Edge cases and exclusions:

- Calls without reserved IDs remain outside strict settlement reconciliation as today.
- This work does not weaken conflict detection for mismatched settlement payloads or claim identities.
