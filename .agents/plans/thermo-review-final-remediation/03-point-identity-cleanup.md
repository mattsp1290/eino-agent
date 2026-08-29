# Point Identity Cleanup

## Goal and prerequisite state

Delete reflection metadata that no longer participates in extension point identity. Preserve canonical point-definition authority and every registration/dispatch behavior.

## Existing evidence

- Each generic constructor calls `reflect.TypeFor` and stores the result in `pointDefinition.signature`.
- `Registry.CommitMount` rejects a second definition for the same durable key by exact `*pointDefinition` pointer inequality.
- `matchingEntries` dispatches only when the stored point pointer equals the requested point pointer.
- The signature is never compared. `validatePoint` only checks it for non-nil.

## Exact change surface

- `extension/types.go`
  - Remove the `reflect` import.
  - Remove `pointDefinition.signature`.
  - Change proposed private `newPointDefinition` to accept only `Contract` and `HandlerKind`.
  - Remove all `reflect.TypeFor` constructor arguments.
  - Make `validatePoint` reject nil definitions, empty kinds, or invalid contracts without a signature check.
- `extension/point_identity_test.go` and `extension/extension_test.go`
  - Preserve tests proving point copies share authority.
  - Preserve tests proving an independently constructed definition with the same durable contract cannot dispatch or replace the canonical definition.
  - Preserve zero-value point rejection.

## Invariants and acceptance criteria

- Copying a typed point value keeps the same immutable definition and dispatch authority.
- A separately constructed point with the same contract and kind remains a conflicting definition.
- Registrations made through one point cannot be dispatched through another definition.
- Generic registration functions continue providing compile-time callback type safety.
- `rg -n 'reflect\.TypeFor|signature reflect\.Type|\.signature' extension` returns no match.
- Run `go test -race ./extension`.

## Risks and exclusions

- Do not change durable point identity, registration ordering, callback policy, or error values.
- Do not reintroduce signature-based interoperability between separately constructed point values. Canonical definition identity is the current deliberate contract.
