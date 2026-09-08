from __future__ import annotations

import os
import subprocess
import sys
import unittest

from request_record import contract, creation
from request_test_support import COMMIT, CONFIRMED_AT, DATE, SCRIPT, RequestFixture


class FilesystemTests(RequestFixture):
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
                "schema": contract.CREATE_SCHEMA,
                "consumer": "eino-agent",
                "request_set": request_set,
                "authorization": {
                    "confirmed_at": CONFIRMED_AT,
                    "destinations_digest": "0" * 64,
                },
                "members": [member],
            }
            with self.subTest(filename=bad):
                result, code = creation.create_set(self.projects, manifest, "fixture")
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
        result, code = creation.create_set(
            self.projects, self.create_manifest(request_set, [member]), "fixture"
        )
        self.assertEqual(code, 2)
        self.assertEqual(result["errors"][0]["code"], "unsafe_path")

    @unittest.skipUnless(hasattr(os, "mkfifo"), "requires FIFO support")
    def test_inspect_rejects_unwritten_fifo_without_blocking(self) -> None:
        destination = self.destination("eino-tools", f"{DATE}-fifo.md")
        destination.parent.mkdir(parents=True)
        os.mkfifo(destination)
        code, result, stderr = self.run_cli(
            "inspect", "--projects-root", str(self.projects), "--consumer", "eino-agent"
        )
        self.assertEqual((code, result["status"]), (2, "blocked"))
        self.assertIn("unsafe_path", {e["code"] for e in result["errors"]})
        self.assertEqual(stderr, "")
        self.assertEqual(list(destination.parent.iterdir()), [destination])

    @unittest.skipUnless(hasattr(os, "mkfifo"), "requires FIFO support")
    def test_read_rejects_validated_file_replaced_by_fifo(self) -> None:
        destination = self.destination("eino-tools", f"{DATE}-swapped.md")
        destination.parent.mkdir(parents=True)
        destination.write_text("PRIVATE CONTENT MUST NOT APPEAR")
        probe = """
import os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from request_record import contract, filesystem as fs
path = Path(sys.argv[2])
fs.require_real_file(path)
path.unlink()
os.mkfifo(path)
fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
try:
    try:
        fs.snapshot_at(fd, path.name, path)
    except contract.ContractError as exc:
        print(exc.code)
    else:
        raise AssertionError("accepted FIFO")
finally:
    os.close(fd)
"""
        result = subprocess.run(
            [sys.executable, "-c", probe, str(SCRIPT.parent), str(destination)],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        self.assertEqual(result.stdout.strip(), "unsafe_path")
        self.assertEqual(result.stderr, "")
        self.assertEqual(list(destination.parent.iterdir()), [destination])
