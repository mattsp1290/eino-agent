# Action Items

## Critical

- [ ] None.

## Important

- [ ] In `tools/einotools/einotools_windows_test.go:25`, replace `_, err := MountStandard(...)` with assignment to the existing variable (`_, err = MountStandard(...)`), then verify the package compiles for `GOOS=windows GOARCH=amd64 CGO_ENABLED=0`.

## Suggestions

- [ ] Add a compile-only Windows gate for the build-tagged einotools integration test so native Unix checks cannot silently miss Windows syntax/type errors.
- [ ] Replace the one-second atomic polling loop in `TestToolTransitionTransportPanicIsPostCommitBestEffort` with a channel signal fired on the third delivery attempt.
