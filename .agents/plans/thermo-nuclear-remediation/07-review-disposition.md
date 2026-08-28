# Plan Review Disposition

Status: two independent reviews and one fresh adversarial review complete; all accepted corrections applied.

This is a synthesized decision record, not a reviewer transcript.

| Finding | Disposition | Applied correction and measurable gate |
| --- | --- | --- |
| Opaque `EventRecord.Payload` cannot safely prove tool-transition correlation or idempotency. | Accepted. | `03-atomic-tool-events.md` now requires a session-owned typed transition as the source of state and canonical event projections, exhaustive field-mutation tests, and no SQLite JSON reparsing. |
| Event ID uniqueness alone allows duplicate transition phases. | Accepted. | `03-atomic-tool-events.md` now requires greenfield event columns plus a partial unique `(tool_call_id, tool_transition)` index, generic-append rejection, and different-event-ID replay tests. |
| Claim rollback omitted the run-lease renewal; settlement rollback omitted reserved result records. | Accepted. | `03-atomic-tool-events.md` and `05-verification-and-docs.md` now make those records part of the atomic boundary and require unchanged-state assertions on failure. |
| Owner-free nested identities lose provenance for components without handlers. | Accepted. | `02-component-owned-run-plans.md` now requires explicit component-owned executable specs and tool-only/prompt-only/guard-only/restriction-only tests. |
| Root Go test pattern cannot enter the nested Wasm example module. | Accepted. | `05-verification-and-docs.md` now runs the root and nested-module commands separately. |
| Review decisions were not traceable. | Accepted. | This file records dispositions and is a pre-implementation gate in `06-execution-handoff.md`. |
| The retained Wasm run metadata cache could leak when a run exits before settlement. | Accepted. | `04-wasm-and-dead-surfaces.md` now makes the settlement notice self-contained and requires a stateless adapter plus a post-admission/pre-settlement failure test. |
| A generic transition beside `ToolCall`/`ToolSettlement` still created dual authorities. | Accepted. | `03-atomic-tool-events.md` now defines concrete create, claim, and settle request shapes, their sole state authority, and the one store-generated lease field. |
| Different-event-ID replay behavior was ambiguous. | Accepted. | `03-atomic-tool-events.md` now makes the event ID the phase idempotency key and requires exact success/conflict assertions for three replay cases. |
| Empty component plans admitted two descriptors for the same behavior and could not resume. | Accepted. | `02-component-owned-run-plans.md` now rejects empty components and requires validation/fingerprint/resume coverage. |

## Pre-implementation gate

- Two independent reviews and a fresh adversarial review have completed; all material findings were accepted and mapped above.
- Re-read every plan file and validate links, ordering, application context, and verification commands before implementation.
