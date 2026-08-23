# Atomic Settlement and Defensive Cloning

## Work package 3: make `BuildToolSettlement` store-ready

Goal: make the public builder produce the complete atomic settlement required by reserved durable tool calls.

Repository evidence:

- `tools.BuildToolSettlement` in `tools/output.go` creates terminal fields and a returned part but leaves both reserved records out of `session.ToolSettlement`.
- Its `runtime.ToolCall` input currently lacks `ResultMessageID` and `ResultPartID`, while those fields exist on `session.ToolCall`.
- `store/sqlite.Store.SettleToolCall` rejects any settlement whose result message ID, result part ID, or part-to-message link differs from the claimed call's reserved IDs.
- Runtime's orchestration paths already construct the required message and part envelope inline.

Change surface:

- Add `ResultMessageID` and `ResultPartID` to `runtime.ToolCall` in `runtime/types.go`.
- Populate the fields after initial durable reservation in `runtime/orchestrator.go` and while reconstructing resumed calls in `runtime/interrupt.go`; existing clone and protected comparison paths must carry them unchanged.
- Modify `tools.BuildToolSettlement` in `tools/output.go`.
- Extend `tools/output_test.go`.
- Add a proposed integration assertion in `store/sqlite/store_test.go` if needed to prove store acceptance rather than only field equality.

Required behavior:

- Capture one UTC completion time.
- Reject a call with either reserved ID missing before producing a partial settlement.
- Populate `ToolSettlement.ResultMessage` with `call.ResultMessageID`, session/run IDs, `ParentID=call.MessageID`, `RoleTool`, and the settlement timestamps.
- Populate `ToolSettlement.ResultPart` with `call.ResultPartID`, `MessageID=call.ResultMessageID`, session/run IDs, `PartToolResult`, the exact settlement payload, and the same timestamps.
- Return that result part to the caller instead of a part attached to the assistant message.
- Preserve claim fencing, output bounds, error redaction, status classification, and the existing function signature. The additive `runtime.ToolCall` fields are an accepted pre-1.0 compatibility break for external unkeyed struct literals.

Verification:

- Unit assertions cover all reserved IDs, linkage, role/kind, payload equality, and timestamp equality.
- Unit assertions cover rejection when either reservation is missing.
- A claimed SQLite tool call with reserved IDs accepts the builder output through `SettleToolCall` and persists both records.

## Work package 4: preserve protected tool schemas

Goal: deliver intact, independent parameter schemas to extension callbacks and include schemas in protected comparison.

Repository evidence:

- `runtime.cloneTool` and `runtime.sameProtectedToolInfo` use JSON on `ToolInfo`.
- Eino v0.8.13 stores `ParamsOneOf.params` and `.jsonschema` in unexported fields.
- `model.cloneParamsOneOf` and `tools.cloneParamsOneOfChecked` establish the local `ToJSONSchema`/JSON-copy pattern.

Change surface:

- Modify `runtime.cloneTool` and `runtime.sameProtectedToolInfo` in `runtime/extensions.go`.
- Add an unexported panic-safe runtime schema clone/serialization helper.
- Add proposed tests in `runtime/extensions_test.go`.

Required behavior:

- A cloned tool retains a non-nil, pointer-distinct `ParamsOneOf` with an equivalent JSON schema.
- Unsupported `ToolInfo` content, schema-conversion error, schema-conversion panic, or a nonnil wrapper producing a nil schema retains fail-closed behavior.
- `sameProtectedToolInfo` rejects schema removal or replacement even when every JSON-exported field is unchanged.
- Schema comparison preserves the distinction between nil and nonnil schema wrappers.
- Existing JSON type normalization behavior for `ToolInfo.Extra` remains accepted.

Verification:

- Test both callback clone preservation and protected validation against a different schema.
- Test a malformed constructor-supplied schema entry and a nonnil wrapper with no backing representation; assert extension callbacks are not entered and no panic escapes.
- Compare schemas via `ToJSONSchema`; do not inspect Eino's unexported representation.

## Work package 5: distinguish aliased slice headers

Goal: prevent `Request.Clone` from conflating different slice views that share the same first element.

Repository evidence:

- `model.cloneVisit` records only type, pointer, and kind.
- `model.cloneReflectValue` caches slices before recursively cloning elements.

Change surface:

- Extend `model.cloneVisit` and its slice construction in `model/provider.go`.
- Add a proposed regression test in `model/provider_test.go`.

Required behavior:

- Include slice length and capacity in slice visit identity.
- Exact repeated visits to the same slice header still reuse the in-progress clone for cycle safety.
- Different headers over the same backing array receive independent cloned headers with their original lengths and values.
- Mutating either cloned view does not mutate the source metadata.

Verification:

- Put a full slice and its prefix into provider message/tool metadata in both map visitation orders.
- Assert every clone preserves the correct length and values deterministically.
- Assert source slices remain unchanged after clone mutation.
