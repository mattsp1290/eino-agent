# Release and response

## Goal and prerequisite state

Publish a verified module version that `eino-tui` can pin, then write the
required external completion response with the exact tag and commit.

Prerequisites:

- All code, tests, examples, and candidate documentation from work packages 1
  through 3 are merged into the intended release commit.
- `make check` passes on that exact commit.
- The implementation Beads issue records the intended tag target.
- No response claims completion yet.

## Proposed release identity

- Root tag: `v0.2.0` (proposed).
- Tag target: the final merged commit containing the new admission contract,
  tests, examples, and versioned public documentation.
- Supported pin: `github.com/mattsp1290/eino-agent@v0.2.0` after public
  verification only.

The new minor line communicates the direct public `runtime.Request` field
replacement. At planning time, local and remote root tags stop at `v0.1.3`.

## Repository release surfaces

- `README.md` (existing): change `Supported root release` from `v0.1.3` to the
  verified tag.
- `docs/consumer-guide.md` (existing): change the installation pin and keep the
  new request example in the same release commit.
- `docs/dependency-status.md` (existing): record the verified tag, peeled full
  SHA, and published-mode consumer command without replacing the separate
  generated-bindings `wasmext/gen/v0.1.0` evidence.
- `testdata/external-consumer/consumer.go` (existing): compile the new request
  contract as described in the verification work package.
- `refs/tags/v0.2.0` (proposed annotated tag): create only on the verified
  release commit and push explicitly.

If `v0.2.0` exists when implementation reaches publication:

1. Reuse it only when its peeled commit equals the intended release commit.
2. If it points elsewhere, select the next unused minor version.
3. Update README, consumer guide, dependency status, fixture command, Beads
   notes, and response candidate to that one version before publication.
4. Never move or delete an existing public tag.

## Published verification

After the release commit and annotated tag are reachable from `origin`, run
published mode with a fresh module cache and the repository's sanitized public
Go environment profile:

```text
git ls-remote --tags origin refs/tags/v0.2.0 'refs/tags/v0.2.0^{}'
env GOPROXY=https://proxy.golang.org,direct \
  GOSUMDB=sum.golang.org \
  GOFLAGS= GOPRIVATE= GONOSUMDB= GONOPROXY= \
  GOTOOLCHAIN=go1.26.3 \
  EINO_AGENT_CONSUMER_VERSION=v0.2.0 \
  testdata/external-consumer/check.sh
```

The gate must prove:

- the tag peels to the recorded release SHA;
- the public module cache supplies `runtime`, `store/sqlite`, and the other
  fixture imports without checkout access;
- the fixture compiles the new `runtime.UserMessage` request shape;
- the root and nested modules resolve without replacements;
- `go mod tidy`, `go list -m all`, `go mod verify`, `go test ./...`, and
  `go build ./...` pass in the external module.

Proxy propagation delay is not a reason to move the tag. Retry the same
immutable tag after propagation.

## External response

Create
`$HOME/.agents/projects/eino-agent/responses/2026-08-30-durable-user-message-admission.md`
only after published verification succeeds. `$HOME` means the implementation
operator's home directory; resolve it at execution time and verify the target
directory belongs to the expected project before writing.

The proposed response file must contain:

- status `completed` and verification date;
- supported module tag and peeled full commit SHA;
- the public `runtime.UserMessage` and `runtime.Request.Message` contract;
- explicit ownership: runtime writes the user message/part and assistant
  placeholder in one admission transaction;
- parentage: run and assistant point to the generated user message;
- failure/interruption and replay semantics;
- exact focused, repository-wide, SQLite restart, and published-consumer gates
  that passed;
- a consumer decision that `eino-tui` may pin the verified version and must
  submit only its current prompt.

Do not include claim tokens, credentials, raw private prompts, or unverified
placeholders. Do not mark the source request resolved from this repository;
the consumer owns its request-status transition after verifying the pin.

## Acceptance and rollback

Acceptance:

- the tag and release commit are on `origin`;
- published-mode verification passes without a workaround;
- public docs name the same supported pin;
- the response names the exact peeled SHA and accurately describes implemented
  behavior;
- the implementation issue closes only after the response exists.

Rollback:

- Before tag publication, stop and fix the candidate commit.
- After tag publication, leave the tag immutable, fix forward, publish a new
  version, rerun every gate, and update the response to name only the usable
  version.
