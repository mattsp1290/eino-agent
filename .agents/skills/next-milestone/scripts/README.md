# Request-record helper

`request_records.py` is the standard-library CLI entrypoint. It parses arguments
and emits the versioned JSON result; the `request_record` package owns the
implementation:

- `contract.py`: wire schemas, scalar validation and Markdown record parsing.
- `filesystem.py`: pinned directories, inode-safe cleanup and the canonical
  nonblocking, no-follow snapshot read.
- `inspection.py`: record loading and linked request-set consistency.
- `creation.py`: complete preflight followed by deterministic application of a
  typed creation plan.
- `transition.py`: complete preflight followed by scoped lock acquisition and
  application of a typed transition plan. Pins outlive locks, and cleanup runs
  on every exit. Progress is recorded immediately after each replacement so
  later verification failures still report the actual partial mutation.

Raw manifest dictionaries remain at validation boundaries. Apply functions use
prepared records; filesystem fault-injection tests patch the filesystem owner.
The CLI, schemas, ordering, recovery rules and cooperating-writer guarantees
are defined in `../references/upstream-request-protocol.md`.

From the repository root, run all format/CLI, graph, creation, transition and
filesystem tests with:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s .agents/skills/next-milestone/scripts -p 'test_*.py'
```

`request_test_support.py` contains shared fixtures without collected tests.
