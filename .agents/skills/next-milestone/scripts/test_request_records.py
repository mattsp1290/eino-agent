from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).with_name("request_records.py")
SPEC = importlib.util.spec_from_file_location("request_records", SCRIPT)
assert SPEC and SPEC.loader
rr = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = rr
SPEC.loader.exec_module(rr)


DATE = "2026-09-07"
COMMIT = "a" * 40
CONFIRMED_AT = "2026-09-07T12:00:00-04:00"


def digest(lines: list[str]) -> str:
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


class RequestRecordsTests(unittest.TestCase):
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
            "schema": rr.CREATE_SCHEMA,
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
            "schema": rr.TRANSITION_SCHEMA,
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
        )
        return completed.returncode, json.loads(completed.stdout), completed.stderr

    def create_one(self) -> tuple[str, str, dict[str, str], Path]:
        request_set = f"{DATE}-durable-tools"
        filename = f"{DATE}-safe-runner.md"
        members = [f"eino-tools/{filename}"]
        body = self.body("eino-tools", filename, request_set, members)
        member = self.member("eino-tools", filename, body)
        manifest = self.create_manifest(request_set, [member])
        result, code = rr.create_set(self.projects, manifest, "fixture")
        self.assertEqual((code, result["status"]), (0, "complete"))
        return request_set, filename, member, self.destination("eino-tools", filename)

    def test_create_inspect_and_resolved_transition(self) -> None:
        request_set, filename, member, destination = self.create_one()
        code, inspected, stderr = self.run_cli(
            "inspect", "--projects-root", str(self.projects), "--consumer", "eino-agent"
        )
        self.assertEqual((code, stderr), (2, ""))
        self.assertEqual(inspected["schema"], rr.RESULT_SCHEMA)
        self.assertEqual(inspected["status"], "blocked")
        self.assertEqual(len(inspected["records"]), 1)
        self.assertIsNone(inspected["records"][0]["response_path"])

        response_dir = self.projects / "eino-tools" / "responses"
        response_dir.mkdir()
        (response_dir / filename).write_text(
            "# Response\n\nImplemented and pinned.\n", encoding="utf-8"
        )
        transition_member = dict(member)
        transition_member["expected_sha256"] = hashlib.sha256(
            destination.read_bytes()
        ).hexdigest()
        manifest = self.transition_manifest(
            request_set, [transition_member], to_status="resolved"
        )
        result, transition_code = rr.transition_set(self.projects, manifest, "fixture")
        self.assertEqual((transition_code, result["status"]), (0, "complete"))
        self.assertIn(
            "- **Request status:** `resolved`", destination.read_text(encoding="utf-8")
        )
        code, inspected, _ = self.run_cli(
            "inspect", "--projects-root", str(self.projects), "--consumer", "eino-agent"
        )
        self.assertEqual(code, 2)
        self.assertEqual(
            inspected["records"][0]["response_path"],
            str((response_dir / filename).resolve()),
        )
        self.assertTrue(inspected["records"][0]["requires_resolution_verification"])

    def test_equivalent_reuse_is_read_only(self) -> None:
        _, _, member, destination = self.create_one()
        before = destination.read_bytes()
        reuse = dict(member)
        reuse["mode"] = "reuse"
        reuse["expected_sha256"] = hashlib.sha256(before).hexdigest()
        manifest = self.create_manifest(f"{DATE}-later-milestone", [reuse])
        result, code = rr.create_set(self.projects, manifest, "fixture")
        self.assertEqual((code, result["status"]), (0, "complete"))
        self.assertEqual(result["reused"], [str(destination)])
        self.assertEqual(destination.read_bytes(), before)

    def test_non_equivalent_same_day_collision_never_overwrites(self) -> None:
        request_set, filename, _, destination = self.create_one()
        before = destination.read_bytes()
        body = self.body(
            "eino-tools",
            filename,
            request_set,
            [f"eino-tools/{filename}"],
            contract="different-contract",
        )
        collision = self.member(
            "eino-tools", filename, body, contract="different-contract"
        )
        result, code = rr.create_set(
            self.projects, self.create_manifest(request_set, [collision]), "fixture"
        )
        self.assertEqual(code, 3)
        self.assertEqual(result["errors"][0]["code"], "create_conflict")
        self.assertEqual(destination.read_bytes(), before)

    def test_manifest_schema_order_identity_and_authorization_fail_closed(self) -> None:
        request_set = f"{DATE}-two-targets"
        filenames = {
            "eino-providers": f"{DATE}-provider-contract.md",
            "eino-tools": f"{DATE}-tool-contract.md",
        }
        names = [f"{target}/{filenames[target]}" for target in sorted(filenames)]
        members = []
        for target in sorted(filenames):
            contract = (
                "provider-contract" if target == "eino-providers" else "tool-contract"
            )
            body = self.body(
                target, filenames[target], request_set, names, contract=contract
            )
            members.append(
                self.member(target, filenames[target], body, contract=contract)
            )
        base = self.create_manifest(request_set, members)
        cases: list[tuple[str, dict[str, object], str]] = []
        unknown = json.loads(json.dumps(base))
        unknown["extra"] = True
        cases.append(("unknown", unknown, "invalid_manifest"))
        version = json.loads(json.dumps(base))
        version["schema"] = "eino-agent-next-milestone/request-create-set/v2"
        cases.append(("version", version, "invalid_manifest"))
        reverse = json.loads(json.dumps(base))
        reverse["members"].reverse()
        reverse["authorization"]["destinations_digest"] = digest(
            [
                str(self.destination(item["target_repo"], item["filename"]))
                for item in reverse["members"]
            ]
        )
        cases.append(("order", reverse, "member_order"))
        identity = json.loads(json.dumps(base))
        identity["members"][0]["request_identity"] = "eino-agent/eino-tools/wrong/v1"
        cases.append(("identity", identity, "invalid_identity"))
        authorization = json.loads(json.dumps(base))
        authorization["authorization"]["destinations_digest"] = "0" * 64
        cases.append(("authorization", authorization, "authorization_mismatch"))
        timestamp = json.loads(json.dumps(base))
        timestamp["authorization"]["confirmed_at"] = "2026-09-07 12:00:00-04:00"
        cases.append(("timestamp", timestamp, "invalid_authorization"))
        for name, manifest, expected in cases:
            with self.subTest(name=name):
                result, code = rr.create_set(self.projects, manifest, "fixture")
                self.assertEqual(code, 2)
                self.assertEqual(result["errors"][0]["code"], expected)
        self.assertFalse(any(self.projects.iterdir()))

    def test_target_identity_mismatch_is_rejected(self) -> None:
        request_set = f"{DATE}-identity"
        filename = f"{DATE}-safe-runner.md"
        body = self.body(
            "eino-tools", filename, request_set, [f"eino-tools/{filename}"]
        )
        member = self.member("eino-tools", filename, body)
        member["target_checkout"] = str(self.targets["eino-providers"])
        result, code = rr.create_set(
            self.projects, self.create_manifest(request_set, [member]), "fixture"
        )
        self.assertEqual(code, 2)
        self.assertEqual(result["errors"][0]["code"], "target_identity_mismatch")

    def test_traversal_nested_absolute_and_symlink_paths_are_rejected(self) -> None:
        request_set = f"{DATE}-paths"
        for bad in ("../escape.md", "nested/escape.md", "/tmp/escape.md"):
            member = {
                "target_repo": "eino-tools",
                "filename": bad,
                "request_identity": "eino-agent/eino-tools/safe-runner/v1",
                "target_checkout": str(self.targets["eino-tools"]),
                "target_module": "github.com/example/eino-tools",
                "target_commit": COMMIT,
                "body": "invalid",
                "mode": "create",
            }
            manifest = {
                "schema": rr.CREATE_SCHEMA,
                "consumer": "eino-agent",
                "request_set": request_set,
                "authorization": {
                    "confirmed_at": CONFIRMED_AT,
                    "destinations_digest": "0" * 64,
                },
                "members": [member],
            }
            with self.subTest(filename=bad):
                result, code = rr.create_set(self.projects, manifest, "fixture")
                self.assertEqual(code, 2)
                self.assertEqual(result["errors"][0]["code"], "invalid_filename")

        outside = self.base / "outside"
        outside.mkdir()
        (self.projects / "eino-tools").symlink_to(outside, target_is_directory=True)
        filename = f"{DATE}-safe-runner.md"
        body = self.body(
            "eino-tools", filename, request_set, [f"eino-tools/{filename}"]
        )
        member = self.member("eino-tools", filename, body)
        result, code = rr.create_set(
            self.projects, self.create_manifest(request_set, [member]), "fixture"
        )
        self.assertEqual(code, 2)
        self.assertEqual(result["errors"][0]["code"], "unsafe_path")

    def test_inspect_rejects_malformed_duplicate_status_unknown_status_and_missing_member(
        self,
    ) -> None:
        target_dir = self.projects / "eino-tools" / "requests"
        target_dir.mkdir(parents=True)
        request_set = f"{DATE}-broken"
        filename = f"{DATE}-broken.md"
        complete = self.body(
            "eino-tools",
            filename,
            request_set,
            [f"eino-tools/{filename}", f"eino-tools/{DATE}-missing.md"],
        )
        variants = {
            "missing": complete.replace("- **Request status:** `open`\n", ""),
            "duplicate": complete.replace(
                "- **Request status:** `open`\n",
                "- **Request status:** `open`\n- **Request status:** `open`\n",
            ),
            "unknown": complete.replace(
                "- **Request status:** `open`", "- **Request status:** `waiting`"
            ),
            "incomplete": complete,
        }
        for name, text in variants.items():
            with self.subTest(name=name):
                for existing in target_dir.glob("*.md"):
                    existing.unlink()
                (target_dir / filename).write_text(text, encoding="utf-8")
                code, result, _ = self.run_cli(
                    "inspect",
                    "--projects-root",
                    str(self.projects),
                    "--consumer",
                    "eino-agent",
                )
                self.assertEqual(code, 2)
                self.assertEqual(result["status"], "blocked")
                self.assertTrue(result["errors"])

    def test_inspect_matches_same_name_response(self) -> None:
        _, filename, _, _ = self.create_one()
        (self.projects / "legacyProject" / "requests").mkdir(parents=True)
        (self.projects / "legacyProject" / "requests" / "legacy.md").write_text(
            "legacy noncanonical project data", encoding="utf-8"
        )
        response_dir = self.projects / "eino-tools" / "responses"
        response_dir.mkdir()
        response = response_dir / filename
        response.write_text("# Response\n", encoding="utf-8")
        result, code = rr.inspect_records(self.projects, "eino-agent")
        self.assertEqual(code, 2)
        self.assertEqual(result["records"][0]["response_path"], str(response.resolve()))

    def test_inspect_reports_relevant_record_in_noncanonical_project(self) -> None:
        requests = self.projects / "legacyProject" / "requests"
        requests.mkdir(parents=True)
        filename = f"{DATE}-legacy.md"
        body = self.body(
            "eino-tools",
            filename,
            f"{DATE}-legacy",
            [f"eino-tools/{filename}"],
            contract="legacy",
        )
        (requests / filename).write_text(body, encoding="utf-8")
        result, code = rr.inspect_records(self.projects, "eino-agent")
        self.assertEqual((code, result["status"]), (2, "blocked"))
        self.assertEqual(result["errors"][0]["code"], "identity_mismatch")

    def test_concurrent_create_has_one_winner_and_no_overwrite(self) -> None:
        request_set = f"{DATE}-race"
        filename = f"{DATE}-race.md"
        body = self.body(
            "eino-tools", filename, request_set, [f"eino-tools/{filename}"]
        )
        member = self.member("eino-tools", filename, body)
        manifest_path = self.write_manifest(self.create_manifest(request_set, [member]))
        command = [
            sys.executable,
            str(SCRIPT),
            "create-set",
            "--projects-root",
            str(self.projects),
            "--manifest",
            str(manifest_path),
        ]
        first = subprocess.Popen(
            command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        second = subprocess.Popen(
            command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out1, _ = first.communicate(timeout=10)
        out2, _ = second.communicate(timeout=10)
        self.assertEqual(sorted([first.returncode, second.returncode]), [0, 3])
        self.assertEqual(
            self.destination("eino-tools", filename).read_text(encoding="utf-8"), body
        )
        self.assertEqual(
            {json.loads(out1)["schema"], json.loads(out2)["schema"]}, {rr.RESULT_SCHEMA}
        )

    def test_create_parent_symlink_swap_cannot_escape_pinned_directory(self) -> None:
        request_set = f"{DATE}-parent-race"
        filename = f"{DATE}-parent-race.md"
        body = self.body(
            "eino-tools", filename, request_set, [f"eino-tools/{filename}"]
        )
        member = self.member("eino-tools", filename, body)
        requests = self.projects / "eino-tools" / "requests"
        requests.mkdir(parents=True)
        moved = self.base / "moved-requests"
        outside = self.base / "outside-requests"
        outside.mkdir()
        real_open = rr.os.open
        swapped = False

        def swap_parent(
            path: os.PathLike[str] | str,
            flags: int,
            mode: int = 0o777,
            **kwargs: object,
        ) -> int:
            nonlocal swapped
            if (
                path == filename
                and kwargs.get("dir_fd") is not None
                and flags & os.O_EXCL
            ):
                requests.rename(moved)
                requests.symlink_to(outside, target_is_directory=True)
                swapped = True
            return real_open(path, flags, mode, **kwargs)

        with mock.patch.object(rr.os, "open", side_effect=swap_parent):
            result, code = rr.create_set(
                self.projects, self.create_manifest(request_set, [member]), "fixture"
            )
        self.assertTrue(swapped)
        self.assertEqual((code, result["status"]), (3, "blocked"))
        self.assertEqual(result["errors"][0]["code"], "unsafe_path")
        self.assertFalse((outside / filename).exists())
        self.assertFalse((moved / filename).exists())

    def test_create_ancestor_symlink_swap_cannot_escape_pinned_chain(self) -> None:
        request_set = f"{DATE}-ancestor-race"
        filename = f"{DATE}-ancestor-race.md"
        body = self.body(
            "eino-tools", filename, request_set, [f"eino-tools/{filename}"]
        )
        member = self.member("eino-tools", filename, body)
        project = self.projects / "eino-tools"
        (project / "requests").mkdir(parents=True)
        moved_project = self.base / "moved-project"
        outside_project = self.base / "outside-project"
        (outside_project / "requests").mkdir(parents=True)
        real_open = rr.os.open
        swapped = False

        def swap_ancestor(
            path: os.PathLike[str] | str,
            flags: int,
            mode: int = 0o777,
            **kwargs: object,
        ) -> int:
            nonlocal swapped
            if (
                path == "requests"
                and kwargs.get("dir_fd") is not None
                and flags & getattr(os, "O_DIRECTORY", 0)
            ):
                project.rename(moved_project)
                project.symlink_to(outside_project, target_is_directory=True)
                swapped = True
            return real_open(path, flags, mode, **kwargs)

        with mock.patch.object(rr.os, "open", side_effect=swap_ancestor):
            result, code = rr.create_set(
                self.projects, self.create_manifest(request_set, [member]), "fixture"
            )
        self.assertTrue(swapped)
        self.assertEqual((code, result["status"]), (3, "blocked"))
        self.assertEqual(result["errors"][0]["code"], "unsafe_path")
        self.assertFalse((outside_project / "requests" / filename).exists())
        self.assertFalse((moved_project / "requests" / filename).exists())

    def test_partial_create_reports_and_recovers_missing_member(self) -> None:
        request_set = f"{DATE}-partial"
        filenames = {
            "eino-providers": f"{DATE}-provider-contract.md",
            "eino-tools": f"{DATE}-tool-contract.md",
        }
        names = [f"{target}/{filenames[target]}" for target in sorted(filenames)]
        members = []
        for target in sorted(filenames):
            contract = (
                "provider-contract" if target == "eino-providers" else "tool-contract"
            )
            members.append(
                self.member(
                    target,
                    filenames[target],
                    self.body(
                        target, filenames[target], request_set, names, contract=contract
                    ),
                    contract=contract,
                )
            )
        real_open = rr.os.open
        request_opens = 0

        def racing_open(
            path: os.PathLike[str] | str,
            flags: int,
            mode: int = 0o777,
            **kwargs: object,
        ) -> int:
            nonlocal request_opens
            if str(path).endswith(".md") and flags & os.O_EXCL:
                request_opens += 1
                if request_opens == 2:
                    raise FileExistsError(str(path))
            return real_open(path, flags, mode, **kwargs)

        with mock.patch.object(rr.os, "open", side_effect=racing_open):
            result, code = rr.create_set(
                self.projects, self.create_manifest(request_set, members), "fixture"
            )
        self.assertEqual((code, result["status"]), (3, "partial"))
        self.assertEqual(len(result["created"]), 1)
        first = members[0]
        first_path = self.destination(first["target_repo"], first["filename"])
        first["mode"] = "reuse"
        first["expected_sha256"] = hashlib.sha256(first_path.read_bytes()).hexdigest()
        recovered, recovered_code = rr.create_set(
            self.projects, self.create_manifest(request_set, members), "fixture"
        )
        self.assertEqual((recovered_code, recovered["status"]), (0, "complete"))
        self.assertEqual(len(recovered["reused"]), 1)
        self.assertEqual(len(recovered["created"]), 1)

    def test_create_write_failure_removes_incomplete_leaf(self) -> None:
        request_set = f"{DATE}-write-failure"
        filename = f"{DATE}-write-failure.md"
        body = self.body(
            "eino-tools", filename, request_set, [f"eino-tools/{filename}"]
        )
        member = self.member("eino-tools", filename, body)
        with mock.patch.object(
            rr.os, "fdopen", side_effect=OSError("fixture write failure")
        ):
            result, code = rr.create_set(
                self.projects, self.create_manifest(request_set, [member]), "fixture"
            )
        self.assertEqual((code, result["status"]), (3, "blocked"))
        self.assertEqual(result["errors"][0]["code"], "write_failure")
        self.assertFalse(self.destination("eino-tools", filename).exists())

    def test_digest_drift_blocks_transition_without_mutation(self) -> None:
        request_set, _, member, destination = self.create_one()
        original = destination.read_bytes()
        transition_member = dict(member)
        transition_member["expected_sha256"] = hashlib.sha256(original).hexdigest()
        destination.write_bytes(original + b"\n")
        changed = destination.read_bytes()
        result, code = rr.transition_set(
            self.projects,
            self.transition_manifest(request_set, [transition_member]),
            "fixture",
        )
        self.assertEqual(code, 4)
        self.assertEqual(result["errors"][0]["code"], "digest_mismatch")
        self.assertEqual(destination.read_bytes(), changed)

    def test_two_helper_lock_contention_does_not_overwrite_winner(self) -> None:
        request_set, _, member, destination = self.create_one()
        transition_member = dict(member)
        transition_member["expected_sha256"] = hashlib.sha256(
            destination.read_bytes()
        ).hexdigest()
        winner_manifest = self.transition_manifest(
            request_set, [transition_member], reason="Winner"
        )
        loser_manifest = self.transition_manifest(
            request_set, [transition_member], reason="Loser"
        )
        original_snapshot = rr.snapshot_at
        lock_held = threading.Event()
        release = threading.Event()
        lock = destination.with_name(destination.name + ".lock")
        winner_paused = False

        def controlled_snapshot(directory_fd: int, filename: str, path: Path):
            nonlocal winner_paused
            if (
                threading.current_thread().name == "winner"
                and lock.exists()
                and not winner_paused
            ):
                winner_paused = True
                lock_held.set()
                self.assertTrue(release.wait(timeout=5))
            return original_snapshot(directory_fd, filename, path)

        outcome: dict[str, tuple[dict[str, object], int]] = {}

        def run_winner() -> None:
            outcome["winner"] = rr.transition_set(
                self.projects, winner_manifest, "winner"
            )

        with mock.patch.object(rr, "snapshot_at", side_effect=controlled_snapshot):
            thread = threading.Thread(target=run_winner, name="winner")
            thread.start()
            self.assertTrue(lock_held.wait(timeout=5))
            loser_result, loser_code = rr.transition_set(
                self.projects, loser_manifest, "loser"
            )
            release.set()
            thread.join(timeout=5)
        self.assertEqual(loser_code, 4)
        self.assertEqual(loser_result["errors"][0]["code"], "lock_conflict")
        self.assertEqual(outcome["winner"][1], 0)
        text = destination.read_text(encoding="utf-8")
        self.assertIn("Winner", text)
        self.assertNotIn("Loser", text)

    def test_partial_transition_is_reported(self) -> None:
        request_set = f"{DATE}-transition-partial"
        filenames = {
            "eino-providers": f"{DATE}-provider-contract.md",
            "eino-tools": f"{DATE}-tool-contract.md",
        }
        names = [f"{target}/{filenames[target]}" for target in sorted(filenames)]
        members = []
        for target in sorted(filenames):
            contract = (
                "provider-contract" if target == "eino-providers" else "tool-contract"
            )
            body = self.body(
                target, filenames[target], request_set, names, contract=contract
            )
            members.append(
                self.member(target, filenames[target], body, contract=contract)
            )
        created, code = rr.create_set(
            self.projects, self.create_manifest(request_set, members), "fixture"
        )
        self.assertEqual(code, 0, created)
        for member in members:
            path = self.destination(member["target_repo"], member["filename"])
            member["expected_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest = self.transition_manifest(request_set, members)
        second_path = self.destination(
            members[1]["target_repo"], members[1]["filename"]
        )
        original_snapshot = rr.snapshot_at
        counts: dict[Path, int] = {}

        def drift_second(directory_fd: int, filename: str, path: Path):
            counts[path] = counts.get(path, 0) + 1
            if path == second_path and counts[path] == 2:
                path.write_text(
                    path.read_text(encoding="utf-8") + "\n", encoding="utf-8"
                )
            return original_snapshot(directory_fd, filename, path)

        with mock.patch.object(rr, "snapshot_at", side_effect=drift_second):
            result, transition_code = rr.transition_set(
                self.projects, manifest, "fixture"
            )
        self.assertEqual((transition_code, result["status"]), (4, "partial"))
        self.assertEqual(len(result["transitioned"]), 1)
        self.assertIn(
            "`withdrawn`", Path(result["transitioned"][0]).read_text(encoding="utf-8")
        )
        self.assertIn("`open`", second_path.read_text(encoding="utf-8"))

        for member in members:
            path = self.destination(member["target_repo"], member["filename"])
            member["expected_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        recovered, recovered_code = rr.transition_set(
            self.projects, self.transition_manifest(request_set, members), "recovery"
        )
        self.assertEqual((recovered_code, recovered["status"]), (0, "complete"))
        self.assertEqual(len(recovered["transitioned"]), 1)
        self.assertEqual(len(recovered["untouched"]), 1)
        for member in members:
            path = self.destination(member["target_repo"], member["filename"])
            self.assertIn("`withdrawn`", path.read_text(encoding="utf-8"))

    def test_transition_ancestor_swap_does_not_follow_replacement_chain(self) -> None:
        request_set, filename, member, destination = self.create_one()
        transition_member = dict(member)
        transition_member["expected_sha256"] = hashlib.sha256(
            destination.read_bytes()
        ).hexdigest()
        manifest = self.transition_manifest(request_set, [transition_member])
        project = self.projects / "eino-tools"
        moved_project = self.base / "transition-moved-project"
        outside_project = self.base / "transition-outside-project"
        outside_requests = outside_project / "requests"
        outside_requests.mkdir(parents=True)
        os.link(destination, outside_requests / filename)
        real_open = rr.os.open
        swapped = False

        def swap_ancestor(
            path: os.PathLike[str] | str,
            flags: int,
            mode: int = 0o777,
            **kwargs: object,
        ) -> int:
            nonlocal swapped
            if path == filename + ".lock" and flags & os.O_EXCL:
                project.rename(moved_project)
                project.symlink_to(outside_project, target_is_directory=True)
                swapped = True
            return real_open(path, flags, mode, **kwargs)

        with mock.patch.object(rr.os, "open", side_effect=swap_ancestor):
            result, code = rr.transition_set(self.projects, manifest, "fixture")
        self.assertTrue(swapped)
        self.assertEqual((code, result["status"]), (4, "blocked"))
        self.assertEqual(result["errors"][0]["code"], "unsafe_path")
        self.assertIn(
            "`open`", (outside_requests / filename).read_text(encoding="utf-8")
        )
        self.assertIn(
            "`open`",
            (moved_project / "requests" / filename).read_text(encoding="utf-8"),
        )

    def test_post_replace_edit_is_detected_as_partial(self) -> None:
        request_set, _, member, destination = self.create_one()
        member["expected_sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
        manifest = self.transition_manifest(request_set, [member])
        original_snapshot = rr.snapshot_at
        edited = False

        def edit_after_replace(directory_fd: int, filename: str, path: Path):
            nonlocal edited
            if not edited and "`withdrawn`" in path.read_text(encoding="utf-8"):
                edited = True
                path.write_text(
                    path.read_text(encoding="utf-8") + "\n",
                    encoding="utf-8",
                )
            return original_snapshot(directory_fd, filename, path)

        with mock.patch.object(rr, "snapshot_at", side_effect=edit_after_replace):
            result, code = rr.transition_set(self.projects, manifest, "fixture")
        self.assertEqual((code, result["status"]), (4, "partial"))
        self.assertEqual(result["errors"][0]["code"], "post_write_mismatch")
        self.assertEqual(result["transitioned"], [str(destination)])

    def test_mode_drift_during_lock_is_detected_before_transition(self) -> None:
        request_set, _, member, destination = self.create_one()
        member["expected_sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
        manifest = self.transition_manifest(request_set, [member])
        original_snapshot = rr.snapshot_at
        changed = False
        lock = destination.with_name(destination.name + ".lock")

        def chmod_after_lock(directory_fd: int, filename: str, path: Path):
            nonlocal changed
            if lock.exists() and not changed:
                changed = True
                path.chmod(0o644)
            return original_snapshot(directory_fd, filename, path)

        with mock.patch.object(rr, "snapshot_at", side_effect=chmod_after_lock):
            result, code = rr.transition_set(self.projects, manifest, "fixture")
        self.assertEqual((code, result["status"]), (4, "blocked"))
        self.assertEqual(result["errors"][0]["code"], "digest_mismatch")
        self.assertEqual(destination.stat().st_mode & 0o777, 0o644)
        self.assertIn("`open`", destination.read_text(encoding="utf-8"))

    def test_structured_metadata_and_noncanonical_target_are_rejected(self) -> None:
        request_set = f"{DATE}-metadata"
        filename = f"{DATE}-metadata.md"
        canonical = self.body(
            "eino-tools", filename, request_set, [f"eino-tools/{filename}"]
        )
        variants = {
            "status-continuation": canonical.replace(
                "- **Request status:** `open`\n",
                "- **Request status:** `open`\n  extra-structured-value\n",
            ),
            "path-continuation": canonical.replace(
                "- **Target repo:** ", "- **Target repo:** extra\n  "
            ),
            "target-prefix": canonical.replace(
                "- **Target repo:** github.com/example/eino-tools",
                "- **Target repo:** junk github.com/example/eino-tools",
            ),
        }
        for name, body in variants.items():
            with self.subTest(name=name):
                member = self.member("eino-tools", filename, body)
                result, code = rr.create_set(
                    self.projects,
                    self.create_manifest(request_set, [member]),
                    "fixture",
                )
                self.assertEqual(code, 2)
                self.assertFalse(self.destination("eino-tools", filename).exists())
                self.assertIn(
                    result["errors"][0]["code"],
                    {"invalid_metadata", "target_identity_mismatch"},
                )

    def test_reuse_rejects_self_omitting_cross_set_declaration(self) -> None:
        requests = self.projects / "eino-tools" / "requests"
        requests.mkdir(parents=True)
        a_name = f"{DATE}-set-a.md"
        b_name = f"{DATE}-set-b.md"
        b_body = self.body(
            "eino-tools",
            b_name,
            f"{DATE}-set-b",
            [f"eino-tools/{b_name}"],
            contract="set-b",
        )
        (requests / b_name).write_text(b_body, encoding="utf-8")
        a_body = self.body(
            "eino-tools",
            a_name,
            f"{DATE}-set-a",
            [f"eino-tools/{b_name}"],
            contract="set-a",
        )
        destination = requests / a_name
        destination.write_text(a_body, encoding="utf-8")
        member = self.member(
            "eino-tools",
            a_name,
            a_body,
            mode="reuse",
            contract="set-a",
            expected_sha256=hashlib.sha256(a_body.encode()).hexdigest(),
        )
        result, code = rr.create_set(
            self.projects,
            self.create_manifest(f"{DATE}-later", [member]),
            "fixture",
        )
        self.assertEqual(code, 2)
        self.assertEqual(result["errors"][0]["code"], "inconsistent_request_set")

    def test_mixed_new_and_linked_reuse_set_lifecycle(self) -> None:
        old_set, old_filename, old_member, old_path = self.create_one()
        self.assertNotEqual(old_set, f"{DATE}-mixed")
        old_member["mode"] = "reuse"
        old_member["expected_sha256"] = hashlib.sha256(
            old_path.read_bytes()
        ).hexdigest()

        new_set = f"{DATE}-mixed"
        new_filename = f"{DATE}-provider-link.md"
        names = [
            f"eino-providers/{new_filename}",
            f"eino-tools/{old_filename}",
        ]
        new_body = self.body(
            "eino-providers",
            new_filename,
            new_set,
            names,
            contract="provider-link",
        )
        new_member = self.member(
            "eino-providers",
            new_filename,
            new_body,
            contract="provider-link",
        )
        mixed_manifest = self.create_manifest(new_set, [new_member, old_member])
        created, created_code = rr.create_set(
            self.projects, mixed_manifest, "mixed-create"
        )
        self.assertEqual((created_code, created["status"]), (0, "complete"))
        self.assertEqual(len(created["created"]), 1)
        self.assertEqual(len(created["reused"]), 1)

        inspected, inspect_code = rr.inspect_records(self.projects, "eino-agent")
        self.assertEqual((inspect_code, inspected["status"]), (2, "blocked"))
        self.assertEqual(inspected["errors"], [])

        new_path = self.destination("eino-providers", new_filename)
        later_reuse = dict(new_member)
        later_reuse["mode"] = "reuse"
        later_reuse["expected_sha256"] = hashlib.sha256(
            new_path.read_bytes()
        ).hexdigest()
        reused, reused_code = rr.create_set(
            self.projects,
            self.create_manifest(f"{DATE}-later", [later_reuse]),
            "later-reuse",
        )
        self.assertEqual((reused_code, reused["status"]), (0, "complete"))

        transition_member = dict(new_member)
        transition_member["expected_sha256"] = hashlib.sha256(
            new_path.read_bytes()
        ).hexdigest()
        transitioned, transition_code = rr.transition_set(
            self.projects,
            self.transition_manifest(new_set, [transition_member]),
            "mixed-transition",
        )
        self.assertEqual((transition_code, transitioned["status"]), (0, "complete"))
        self.assertIn("`withdrawn`", new_path.read_text(encoding="utf-8"))
        self.assertIn("`open`", old_path.read_text(encoding="utf-8"))

    def test_cross_set_cycle_is_rejected_by_inspect_and_reuse(self) -> None:
        requests = self.projects / "eino-tools" / "requests"
        requests.mkdir(parents=True)
        a_name = f"{DATE}-cycle-a.md"
        b_name = f"{DATE}-cycle-b.md"
        members = [f"eino-tools/{a_name}", f"eino-tools/{b_name}"]
        a_body = self.body(
            "eino-tools",
            a_name,
            f"{DATE}-cycle-a",
            members,
            contract="cycle-a",
        )
        b_body = self.body(
            "eino-tools",
            b_name,
            f"{DATE}-cycle-b",
            members,
            contract="cycle-b",
        )
        (requests / a_name).write_text(a_body, encoding="utf-8")
        (requests / b_name).write_text(b_body, encoding="utf-8")

        inspected, inspect_code = rr.inspect_records(self.projects, "eino-agent")
        self.assertEqual((inspect_code, inspected["status"]), (2, "blocked"))
        self.assertTrue(
            any(
                item["message"] == "linked request sets contain a cycle"
                for item in inspected["errors"]
            )
        )

        reuse = self.member(
            "eino-tools",
            a_name,
            a_body,
            mode="reuse",
            contract="cycle-a",
            expected_sha256=hashlib.sha256(a_body.encode()).hexdigest(),
        )
        result, code = rr.create_set(
            self.projects,
            self.create_manifest(f"{DATE}-later", [reuse]),
            "cycle-reuse",
        )
        self.assertEqual(code, 2)
        self.assertEqual(result["errors"][0]["code"], "inconsistent_request_set")

    def test_create_failure_does_not_delete_replacement_leaf(self) -> None:
        request_set = f"{DATE}-replacement"
        filename = f"{DATE}-replacement.md"
        body = self.body(
            "eino-tools", filename, request_set, [f"eino-tools/{filename}"]
        )
        destination = self.destination("eino-tools", filename)
        member = self.member("eino-tools", filename, body)

        def replace_then_fail(descriptor: int, mode: str):
            del mode
            os.close(descriptor)
            destination.unlink()
            destination.write_text("replacement", encoding="utf-8")
            raise OSError("fixture write failure")

        with mock.patch.object(rr.os, "fdopen", side_effect=replace_then_fail):
            result, code = rr.create_set(
                self.projects, self.create_manifest(request_set, [member]), "fixture"
            )
        self.assertEqual((code, result["status"]), (3, "blocked"))
        self.assertEqual(destination.read_text(encoding="utf-8"), "replacement")

    def test_lock_cleanup_does_not_delete_replacement_lock(self) -> None:
        request_set, filename, member, destination = self.create_one()
        member["expected_sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
        manifest = self.transition_manifest(request_set, [member])
        original_snapshot = rr.snapshot_at
        replaced = False
        lock = destination.with_name(filename + ".lock")

        def replace_lock_then_fail(directory_fd: int, leaf: str, path: Path):
            nonlocal replaced
            if lock.exists() and not replaced:
                replaced = True
                lock.unlink()
                lock.write_text("replacement lock", encoding="utf-8")
                raise rr.ContractError("digest_mismatch", str(path), "fixture drift")
            return original_snapshot(directory_fd, leaf, path)

        with mock.patch.object(rr, "snapshot_at", side_effect=replace_lock_then_fail):
            result, code = rr.transition_set(self.projects, manifest, "fixture")
        self.assertEqual((code, result["status"]), (4, "blocked"))
        self.assertEqual(lock.read_text(encoding="utf-8"), "replacement lock")

    def test_terminal_recovery_member_is_rechecked_after_locking(self) -> None:
        request_set, filename, member, destination = self.create_one()
        member["expected_sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
        first, first_code = rr.transition_set(
            self.projects, self.transition_manifest(request_set, [member]), "first"
        )
        self.assertEqual((first_code, first["status"]), (0, "complete"))
        member["expected_sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
        manifest = self.transition_manifest(request_set, [member])
        real_open = rr.os.open
        changed = False

        def edit_after_lock(
            path: os.PathLike[str] | str,
            flags: int,
            mode: int = 0o777,
            **kwargs: object,
        ) -> int:
            nonlocal changed
            descriptor = real_open(path, flags, mode, **kwargs)
            if path == filename + ".lock" and flags & os.O_EXCL:
                destination.write_text(
                    destination.read_text(encoding="utf-8") + "\n",
                    encoding="utf-8",
                )
                changed = True
            return descriptor

        with mock.patch.object(rr.os, "open", side_effect=edit_after_lock):
            result, code = rr.transition_set(self.projects, manifest, "recovery")
        self.assertTrue(changed)
        self.assertEqual((code, result["status"]), (4, "blocked"))
        self.assertEqual(result["errors"][0]["code"], "digest_mismatch")

    def test_invalid_unicode_and_fixed_validation_precedence(self) -> None:
        request_set = f"{DATE}-unicode"
        filename = f"{DATE}-unicode.md"
        body = self.body(
            "eino-tools", filename, request_set, [f"eino-tools/{filename}"]
        )
        member = self.member("eino-tools", filename, body + "\ud800")
        result, code = rr.create_set(
            self.projects, self.create_manifest(request_set, [member]), "fixture"
        )
        self.assertEqual(code, 2)
        self.assertEqual(result["errors"][0]["code"], "invalid_request")

        invalid = self.create_manifest(
            request_set, [self.member("eino-tools", filename, body)]
        )
        invalid["schema"] = "wrong"
        manifest_path = self.write_manifest(invalid, "invalid-schema.json")
        cli_code, cli_result, _ = self.run_cli(
            "create-set",
            "--projects-root",
            str(self.base / "missing-root"),
            "--manifest",
            str(manifest_path),
        )
        self.assertEqual(cli_code, 2)
        self.assertEqual(cli_result["errors"][0]["code"], "invalid_manifest")

    def test_lowercase_rfc3339_and_leap_second_authorization_are_accepted(
        self,
    ) -> None:
        request_set = f"{DATE}-rfc3339"
        filename = f"{DATE}-rfc3339.md"
        body = self.body(
            "eino-tools", filename, request_set, [f"eino-tools/{filename}"]
        )
        member = self.member("eino-tools", filename, body)
        manifest = self.create_manifest(request_set, [member])
        manifest["authorization"]["confirmed_at"] = "2026-09-07t12:00:00z"
        result, code = rr.create_set(self.projects, manifest, "lowercase-rfc3339")
        self.assertEqual((code, result["status"]), (0, "complete"))
        self.assertEqual(
            rr.parse_rfc3339("2016-12-31T23:59:60Z", "fixture"),
            "2016-12-31T23:59:60Z",
        )

    def test_result_shape_parser_errors_and_transition_schema_fail_closed(self) -> None:
        code, result, _ = self.run_cli("inspect")
        self.assertEqual(code, 2)
        self.assertEqual(
            set(result),
            {
                "schema",
                "operation",
                "status",
                "created",
                "reused",
                "transitioned",
                "untouched",
                "errors",
            },
        )
        self.assertEqual(result["operation"], "inspect")
        self.assertEqual(
            result["errors"],
            sorted(
                result["errors"],
                key=lambda item: (item["path"], item["code"], item["message"]),
            ),
        )

        request_set, _, member, destination = self.create_one()
        member["expected_sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
        base = self.transition_manifest(request_set, [member])
        cases: list[tuple[dict[str, object], str]] = []
        wrong_version = json.loads(json.dumps(base))
        wrong_version["schema"] = rr.TRANSITION_SCHEMA.replace("/v1", "/v2")
        cases.append((wrong_version, "invalid_manifest"))
        bad_member = json.loads(json.dumps(base))
        bad_member["members"][0]["unknown"] = True
        cases.append((bad_member, "invalid_manifest"))
        reverse = self.transition_manifest(
            request_set,
            [
                member,
                {
                    "target_repo": "eino-providers",
                    "filename": f"{DATE}-provider.md",
                    "request_identity": "eino-agent/eino-providers/provider/v1",
                    "expected_sha256": "0" * 64,
                },
            ],
        )
        cases.append((reverse, "member_order"))
        for manifest, expected_error in cases:
            with self.subTest(manifest=manifest["schema"]):
                outcome, outcome_code = rr.transition_set(
                    self.projects, manifest, "fixture"
                )
                self.assertEqual(outcome_code, 2)
                self.assertEqual(outcome["errors"][0]["code"], expected_error)
                self.assertIn("`open`", destination.read_text(encoding="utf-8"))

    def test_transition_validation_and_result_exit_contracts(self) -> None:
        request_set, _, member, destination = self.create_one()
        transition_member = dict(member)
        transition_member["expected_sha256"] = hashlib.sha256(
            destination.read_bytes()
        ).hexdigest()
        invalid = self.transition_manifest(request_set, [transition_member])
        invalid["extra"] = True
        result, code = rr.transition_set(self.projects, invalid, "fixture")
        self.assertEqual(code, 2)
        self.assertEqual(result["schema"], rr.RESULT_SCHEMA)
        self.assertEqual(result["errors"][0]["code"], "invalid_manifest")

        no_response = self.transition_manifest(
            request_set, [transition_member], to_status="resolved"
        )
        result, code = rr.transition_set(self.projects, no_response, "fixture")
        self.assertEqual(code, 2)
        self.assertEqual(result["errors"][0]["code"], "unsafe_path")

        buffer = io.StringIO()
        with (
            mock.patch.object(
                rr, "canonical_projects_root", side_effect=RuntimeError("boom")
            ),
            contextlib.redirect_stdout(buffer),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            internal_code = rr.main(
                [
                    "inspect",
                    "--projects-root",
                    str(self.projects),
                    "--consumer",
                    "eino-agent",
                ]
            )
        self.assertEqual(internal_code, 1)
        self.assertEqual(
            json.loads(buffer.getvalue())["errors"][0]["code"], "internal_error"
        )


if __name__ == "__main__":
    unittest.main()
