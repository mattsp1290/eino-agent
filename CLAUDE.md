# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on this project.

## Beans issue tracker (`bn`)

This project tracks work with `bn`; no other tracker is authorized, and initializing a second tracker in this repository is prohibited.

Issues live in the hub at `~/.beans/hub/projects/eino-agent/`; `bn` commits and pushes the hub itself on every mutating command, so never commit hub files by hand.

### Quick reference

```bash
bn prime
bn ready
bn show <id>
bn update <id> --claim
bn create "title" -d "why and what" -p 2 -t task -l impl
bn close <id> -r "reason"
bn dep add <child> <parent>
bn remember "insight"
bn memories <keyword>
bn status
```

### Rules

- Use `bn` for all task tracking; do not use TodoWrite, TaskCreate, or markdown TODO lists.
- Use `bn remember` for persistent knowledge; do not use `MEMORY.md` files.
- Run `bn` commands serially.
- Create an issue before writing code and claim it when starting.
- IDs are `eino-agent-<hash>` and never change; legacy IDs such as `eino-agent-d64.8` remain valid.

## Session completion

Work is NOT complete until `git push` succeeds.

1. File issues for remaining work with `bn create`.
2. Run quality gates when code changed (`make check` or the Makefile targets that apply).
3. Update issue status with `bn close` or `bn update`.
4. Push this repository:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
5. Verify the hub is pushed: `bn status` must show `ahead: 0`.
6. Hand off context in the issue log with `bn note <id> ...`.


## Build & Test

_Add your build and test commands here_

```bash
# Example:
# npm install
# npm test
```

## Architecture Overview

_Add a brief overview of your project architecture_

## Conventions & Patterns

_Add your project-specific conventions here_
