# Review Disposition

## Independent architecture review

Accepted:

- move handler scope to each registration and preserve mixed-scope instances;
- validate object input after every transform, not only provider ingress;
- wrap permission-pattern callbacks in composition lifecycle leases;
- make Wasm ownership transfer two-phase and atomic;
- document destructive pre-release database rollback/recovery; and
- remove stale FollowUp guidance and test doubles repo-wide.

## Independent testability review

Accepted:

- fingerprint handler notification/interceptor kind;
- cross-check typed identities against attached behavior and reject duplicates;
- make descriptor validation mandatory inside fingerprinting;
- retain adapter-specific cleanup in loader-owned resource finalizers;
- define exact observer/upstream/downstream stream lifecycle;
- validate and preserve persisted permission patterns through resume/settlement;
- privatize loaded Wasm types whose public signatures do not require them; and
- expand SQLite race and JSON-record verification.

## Consolidation decisions

The two reviews independently found the handler-scope and object-validation
defects; those are represented once in the corrected work packages. No
substantive finding was rejected. Historical prompt artifacts remain excluded
from removed-symbol checks because they describe past requested states rather
than current contracts.

## Fresh adversarial review

Accepted:

- precompute a validated tool-name fallback pattern so prepare failures can
  still be durably settled without masking the original error;
- update Wasm registration/projection call sites that name privatized types;
- use Eino's close-propagating conversion reader and state the observer contract
  for consumer abandonment precisely;
- keep permission-sensitive pattern data out of public events and narrow the
  visibility invariant accordingly;
- deterministically normalize every accepted JSON object instead of merely
  validating its shape; and
- test every non-tool capability drift path against the mandatory resume
  fingerprint comparison and exactly-once release.

No adversarial finding was rejected. The event-payload finding was applied by
narrowing the invariant rather than expanding the public payload: permission
patterns remain available to enforcement and tool observation but are not
duplicated into model-facing/live event arguments.
