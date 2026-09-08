from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from request_record import contract, creation

SCRIPT = Path(__file__).with_name("request_records.py")

DATE = "2026-09-07"
COMMIT = "a" * 40
CONFIRMED_AT = "2026-09-07T12:00:00-04:00"


def digest(lines: list[str]) -> str:
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


class RequestFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.projects = self.base / "projects"
        self.projects.mkdir()
        self.checkouts = self.base / "checkouts"
        self.checkouts.mkdir()
        self.targets: dict[str, Path] = {}
        for target in ("eino-tools", "eino-providers"):
            checkout = self.checkouts / target
            checkout.mkdir()
            self.targets[target] = checkout

    def tearDown(self) -> None:
        self.temp.cleanup()

    def destination(self, target: str, filename: str) -> Path:
        return self.projects.resolve() / target / "requests" / filename

    def body(
        self,
        target: str,
        filename: str,
        request_set: str,
        members: list[str],
        contract: str = "safe-runner",
        status: str = "open",
    ) -> str:
        identity = f"eino-agent/{target}/{contract}/v1"
        checkout = self.targets[target]
        module = f"github.com/example/{target}"
        return f"""# Request: Safe public runner

- **Requested by:** `eino-agent` next-milestone selection
- **Blocker consumer:** `eino-agent`
- **Request status:** `{status}`
- **Date:** {filename[:10]}
- **Priority:** Blocks the selected public journey
- **Selected milestone:** Durable tool execution
- **Request set:** {request_set}
- **Request set members:** {", ".join(members)}
- **Request identity:** `{identity}`
- **Target repo:** {module} ({checkout})
- **Pinned commit under evaluation:** {COMMIT}
- **Consumer:** `eino-agent`

## Background
Synthetic fixture evidence.

## Ask
Provide the smallest public runner contract.

## Out of scope
Host presentation and policy.

## Acceptance
Public API, tests, documentation, and a consumable pin.

## Response and unblock contract
Write the same-named response and identify the verified pin.

## References
Synthetic public paths only.

## Status history
- {DATE} — `open`: created for Durable tool execution.
"""

    def member(
        self,
        target: str,
        filename: str,
        body: str,
        mode: str = "create",
        contract: str = "safe-runner",
        expected_sha256: str | None = None,
    ) -> dict[str, str]:
        value = {
            "target_repo": target,
            "filename": filename,
            "request_identity": f"eino-agent/{target}/{contract}/v1",
            "target_checkout": str(self.targets[target]),
            "target_module": f"github.com/example/{target}",
            "target_commit": COMMIT,
            "body": body,
            "mode": mode,
        }
        if expected_sha256 is not None:
            value["expected_sha256"] = expected_sha256
        return value

    def create_manifest(
        self, request_set: str, members: list[dict[str, str]]
    ) -> dict[str, object]:
        paths = [
            str(self.destination(item["target_repo"], item["filename"]))
            for item in members
        ]
        return {
            "schema": contract.CREATE_SCHEMA,
            "consumer": "eino-agent",
            "request_set": request_set,
            "authorization": {
                "confirmed_at": CONFIRMED_AT,
                "destinations_digest": digest(paths),
            },
            "members": members,
        }

    def transition_manifest(
        self,
        request_set: str,
        members: list[dict[str, str]],
        to_status: str = "withdrawn",
        reason: str = "Fixture decision",
    ) -> dict[str, object]:
        paths = [
            f"{self.destination(item['target_repo'], item['filename'])} -> {to_status}"
            for item in members
        ]
        manifest_members: list[dict[str, str]] = []
        for item in members:
            value = {
                "target_repo": item["target_repo"],
                "filename": item["filename"],
                "request_identity": item["request_identity"],
                "expected_sha256": item["expected_sha256"],
            }
            if to_status == "resolved":
                value.update(
                    response_filename=item["filename"],
                    verified_target_commit="b" * 40,
                    verified_pin="v1.2.3",
                )
            manifest_members.append(value)
        return {
            "schema": contract.TRANSITION_SCHEMA,
            "consumer": "eino-agent",
            "request_set": request_set,
            "authorization": {
                "confirmed_at": CONFIRMED_AT,
                "destinations_digest": digest(paths),
            },
            "from_status": "open",
            "to_status": to_status,
            "reason": reason,
            "history_line": f"- {DATE} — `{to_status}`: {reason}",
            "members": manifest_members,
        }

    def write_manifest(
        self, value: dict[str, object], name: str = "manifest.json"
    ) -> Path:
        path = self.base / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def run_cli(self, *args: str) -> tuple[int, dict[str, object], str]:
        completed = subprocess.run(
            [sys.executable, str(SCRIPT), *args],
            text=True,
            capture_output=True,
            check=False,
            timeout=5,
        )
        return completed.returncode, json.loads(completed.stdout), completed.stderr

    def create_one(self) -> tuple[str, str, dict[str, str], Path]:
        request_set = f"{DATE}-durable-tools"
        filename = f"{DATE}-safe-runner.md"
        members = [f"eino-tools/{filename}"]
        body = self.body("eino-tools", filename, request_set, members)
        member = self.member("eino-tools", filename, body)
        manifest = self.create_manifest(request_set, [member])
        result, code = creation.create_set(self.projects, manifest, "fixture")
        self.assertEqual((code, result["status"]), (0, "complete"))
        return request_set, filename, member, self.destination("eino-tools", filename)
