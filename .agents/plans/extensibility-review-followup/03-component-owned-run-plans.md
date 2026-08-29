# Component-Owned Run Plans

## Goal and prerequisites

Preserve component ownership from the extension snapshot through executable plan sealing and durable descriptor creation. Canonical typed handler identities from [01-extension-dispatch-contract.md](01-extension-dispatch-contract.md) are a prerequisite.

## Existing evidence

- `extension.Snapshot.Values` already returns one lease-protected value per mounted component.
- `composition.Registry.acquire` flattens selected tools, prompts, guards, and restrictions into separate runtime slices, repeating `extension.Component` on every item.
- `runtime.NewRunPlan` recreates component ownership through `byInstance`, a closure, dispatch diagnostics, and four capability loops.
- The durable `session.ExtensionPlanDescriptor` already uses the desired component-owned `[]ComponentPlan` representation.

## Proposed runtime contract

Add `runtime.PlanComponent` in `runtime/extension_plan.go`:

```text
PlanComponent
  Component
  Handlers []extension.HandlerIdentity
  Tools []PlanTool
  Prompts []PlanPrompt
  Guards []PlanGuard
  Restrictions []PlanRestriction
```

Change `RunPlanSpec` to contain `Dispatch *extension.Plan` and `Components []PlanComponent`. Delete the old flat fields rather than retaining both shapes.

Make every nested plan-input record owner-free:

- `PlanTool` contains identity, resolver, and proposed `Sequence int`.
- `PlanPrompt` contains identity, provider, and `Sequence`; it does not embed `MountedPrompt` or an instance ID.
- `PlanGuard` contains identity, guard callback, and `Sequence`; it does not embed `MountedToolGuard` or an instance ID.
- `PlanRestriction` contains identity, canonical rules, and `Sequence`.

`Sequence` is non-durable. Composition assigns a zero-based ordinal per capability kind after applying the existing global selector comparator. Runtime uses it only to reconstruct sealed executable order after component-owned validation; descriptor fingerprinting ignores it.

`extension.HandlerIdentity` is proposed and contains registration ID, contract, order, scope, and typed kind. Component/artifact ownership stays on `PlanComponent`, not each identity.

## Composition construction

Change `composition.Registry.acquire` to iterate selected mounted components as the ownership boundary:

1. Start one `runtime.PlanComponent` from each `extension.MountedValue` that owns selected behavior.
2. Obtain scope-filtered typed handler identities from `MountedValue.Handlers()`; the `extension.Plan` contains the same grouped authoritative identities through `HandlerComponents()`.
3. Apply current tool selection, prompt shadowing, guard ordering, and restriction filtering while appending the chosen executable records to their owner.
4. Omit a component only when none of its handlers or capabilities remain selected.
5. Preserve the current global selector order by assigning each selected tool, prompt, guard, and restriction its index as `Sequence` before appending it to the owning component record.
6. Sort component records and nested durable identities for deterministic validation only; never use descriptor ordering as execution ordering.
7. Pass the grouped records directly to `runtime.NewRunPlan` with the snapshot dispatch lease.

Selection helpers may return owner-indexed selections, but they must not create a second long-lived ownership model or require runtime to regroup flat slices. Callback context remains derived from the owning mounted value.

## Runtime sealing

Refactor `runtime.NewRunPlan` to validate one `PlanComponent` at a time:

- validate component identity once;
- reject duplicate component instance IDs and conflicting artifacts directly;
- compare all supplied component handler identities with the defensive `Dispatch.HandlerComponents()` authority and reject a forged owner, omitted identity, extra identity, or scope mismatch;
- validate each executable capability and append its identity to the same `session.ComponentPlan`;
- derive private sealed prompt/guard ownership from the enclosing component;
- append executable tools/prompts/guards/restrictions to temporary collections and globally stable-sort each collection by its validated `Sequence` before sealing;
- reject empty component records;
- fingerprint the resulting descriptor and retain dispatch as the sole lease owner.

Delete `byInstance`, the `componentPlan` closure, string kind conversion, repeated nested owner validation, `Plan.Diagnostics`, and `PlanEntryDiagnostic`. Validate sequence tokens as unique and contiguous within each capability kind so malformed grouped input cannot silently reorder behavior.

## Resume and mismatch behavior

Keep existing descriptor-driven instance and capability selection. A resume plan must preserve:

- exact component ownership and artifacts;
- persisted handler and capability identities;
- session-scope validation;
- prompt shadowing and tool identity selection;
- fingerprint mismatch release behavior.

No compatibility decoder or old `RunPlanSpec` fields remain.

## Tests and acceptance criteria

Update runtime and composition tests to construct grouped components. Add cases for:

- one component owning every capability kind plus multiple handlers;
- multiple components with overlapping orders;
- an interleaved `A(order 1), B(order 2), A(order 3)` selection whose tool exposure, prompt invocation, guard callback order, and first-denial behavior exactly match current global comparator order;
- handler-only and capability-only components;
- duplicate component IDs and conflicting artifacts;
- a handler identity attributed to the wrong owner;
- selected and unselected scoped handlers, plus omitted and extra handler identities;
- prompt shadowing and resume filtering preserving the correct owner;
- construction failure releasing the dispatch lease exactly once;
- descriptor fingerprint stability across equivalent input ordering.

Run:

```text
go test -race ./extension ./composition ./runtime ./session
```

Acceptance: `runtime.NewRunPlan` consumes component-owned input directly, produces the durable component-owned descriptor without regrouping, and preserves all current selection and lease behavior.

Also assert that no nested unsealed `PlanTool`, `PlanPrompt`, `PlanGuard`, or `PlanRestriction` contains `Component` or `InstanceID`; ownership exists only on `PlanComponent`.

## Risks and exclusions

- Do not move composition-private executable types into `extension`.
- Do not serialize callbacks or reflection signatures.
- Do not change descriptor schema version unless a persisted JSON representation actually changes. There are no users, but an unnecessary schema change adds noise without proving this finding.
