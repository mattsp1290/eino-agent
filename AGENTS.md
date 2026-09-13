# Agent Instructions

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

## Local Agent Artifacts

`.agents/plans/`, `.agents/reviews/`, and `reviews/` are intentionally
gitignored local agent artifacts. Never stage or commit files in these
directories, including with `git add -f`. Preserve their contents unless the
user explicitly requests local deletion. Treat a request to remove these
artifacts from Git or a pull request as index-only removal; verify the local
files still exist afterward. Before committing, verify that
`git ls-files -- .agents/plans .agents/reviews reviews` returns no paths.

## Non-Interactive Shell Commands

**ALWAYS use non-interactive flags** with file operations to avoid hanging on confirmation prompts.

Shell commands like `cp`, `mv`, and `rm` may be aliased to include `-i` (interactive) mode on some systems, causing the agent to hang indefinitely waiting for y/n input.

**Use these forms instead:**
```bash
# Force overwrite without prompting
cp -f source dest           # NOT: cp source dest
mv -f source dest           # NOT: mv source dest
rm -f file                  # NOT: rm file

# For recursive operations
rm -rf directory            # NOT: rm -r directory
cp -rf source dest          # NOT: cp -r source dest
```

**Other commands that may prompt:**
- `scp` - use `-o BatchMode=yes` for non-interactive
- `ssh` - use `-o BatchMode=yes` to fail instead of prompting
- `apt-get` - use `-y` flag
- `brew` - use `HOMEBREW_NO_AUTO_UPDATE=1` env var
