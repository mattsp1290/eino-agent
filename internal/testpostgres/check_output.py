"""Validate go test -json output while preserving test diagnostics."""
import json
import sys

required = set(sys.argv[1:])
if not required:
    sys.exit("postgres integration: required suite names are missing")
ran, passed, problems = set(), set(), []
for line in sys.stdin:
    try:
        event = json.loads(line)
    except (ValueError, TypeError):
        problems.append("invalid test JSON")
        continue
    package, test, action = (event.get(key, "") for key in ("Package", "Test", "Action"))
    identity = package + ":" + test
    if event.get("Output"):
        sys.stdout.write(event["Output"])
    if action == "run":
        ran.add(identity)
    if action == "pass":
        passed.add(identity)
    if action == "fail":
        problems.append("failed: " + identity)
    if action == "skip" and (
        package.endswith("/store/postgres") or test.startswith("TestPostgres")
        or any(identity == suite or identity.startswith(suite + "/") for suite in required)
    ):
        problems.append("skipped: " + identity)
for identity in sorted(required - (ran & passed)):
    problems.append("required suite did not run and pass: " + identity)
if problems:
    sys.exit("postgres integration: " + "; ".join(problems))
print("postgres integration: required suites passed; zero skips")
