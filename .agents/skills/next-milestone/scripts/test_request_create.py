from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from unittest import mock

from request_record import contract, creation
from request_test_support import DATE, SCRIPT, RequestFixture


class CreateTests(RequestFixture):
    def test_equivalent_reuse_is_read_only(self) -> None:
        _, _, member, destination = self.create_one()
        before = destination.read_bytes()
        reuse = dict(member)
        reuse["mode"] = "reuse"
        reuse["expected_sha256"] = hashlib.sha256(before).hexdigest()
        manifest = self.create_manifest(f"{DATE}-later-milestone", [reuse])
        result, code = creation.create_set(self.projects, manifest, "fixture")
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
        result, code = creation.create_set(
            self.projects, self.create_manifest(request_set, [collision]), "fixture"
        )
        self.assertEqual(code, 3)
        self.assertEqual(result["errors"][0]["code"], "create_conflict")
        self.assertEqual(destination.read_bytes(), before)

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
            {json.loads(out1)["schema"], json.loads(out2)["schema"]},
            {contract.RESULT_SCHEMA},
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
        real_open = os.open
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

        with mock.patch.object(os, "open", side_effect=swap_parent):
            result, code = creation.create_set(
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
        real_open = os.open
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

        with mock.patch.object(os, "open", side_effect=swap_ancestor):
            result, code = creation.create_set(
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
        real_open = os.open
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

        with mock.patch.object(os, "open", side_effect=racing_open):
            result, code = creation.create_set(
                self.projects, self.create_manifest(request_set, members), "fixture"
            )
        self.assertEqual((code, result["status"]), (3, "partial"))
        self.assertEqual(len(result["created"]), 1)
        first = members[0]
        first_path = self.destination(first["target_repo"], first["filename"])
        first["mode"] = "reuse"
        first["expected_sha256"] = hashlib.sha256(first_path.read_bytes()).hexdigest()
        recovered, recovered_code = creation.create_set(
            self.projects, self.create_manifest(request_set, members), "fixture"
        )
        self.assertEqual((recovered_code, recovered["status"]), (0, "complete"))
        self.assertEqual(len(recovered["reused"]), 1)
        self.assertEqual(len(recovered["created"]), 1)

    def test_same_set_reuse_rejects_mismatched_milestones_before_writing(self) -> None:
        for second_mode in ("create", "reuse"):
            with self.subTest(second_mode=second_mode):
                request_set = f"{DATE}-mismatch-{second_mode}"
                filename = f"{DATE}-mismatch-{second_mode}.md"
                names = [f"{target}/{filename}" for target in sorted(self.targets)]
                members = []
                before = {}
                for index, target in enumerate(sorted(self.targets)):
                    body = self.body(target, filename, request_set, names)
                    if index == 1:
                        body = body.replace(
                            "**Selected milestone:** Durable tool execution",
                            "**Selected milestone:** Different milestone",
                        )
                    mode = "reuse" if index == 0 else second_mode
                    destination = self.destination(target, filename)
                    if mode == "reuse":
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        destination.write_text(body)
                        before[destination] = destination.read_bytes()
                    members.append(
                        self.member(
                            target,
                            filename,
                            body,
                            mode=mode,
                            expected_sha256=hashlib.sha256(body.encode()).hexdigest()
                            if mode == "reuse"
                            else None,
                        )
                    )
                result, code = creation.create_set(
                    self.projects,
                    self.create_manifest(request_set, members),
                    "mismatch",
                )
                self.assertEqual((code, result["status"]), (2, "blocked"))
                self.assertIn(
                    "inconsistent_request_set", {e["code"] for e in result["errors"]}
                )
                self.assertEqual(result["created"], [])
                for destination, content in before.items():
                    self.assertEqual(destination.read_bytes(), content)
                if second_mode == "create":
                    self.assertFalse(self.destination("eino-tools", filename).exists())

    def test_create_write_failure_removes_incomplete_leaf(self) -> None:
        request_set = f"{DATE}-write-failure"
        filename = f"{DATE}-write-failure.md"
        body = self.body(
            "eino-tools", filename, request_set, [f"eino-tools/{filename}"]
        )
        member = self.member("eino-tools", filename, body)
        with mock.patch.object(
            os, "fdopen", side_effect=OSError("fixture write failure")
        ):
            result, code = creation.create_set(
                self.projects, self.create_manifest(request_set, [member]), "fixture"
            )
        self.assertEqual((code, result["status"]), (3, "blocked"))
        self.assertEqual(result["errors"][0]["code"], "write_failure")
        self.assertFalse(self.destination("eino-tools", filename).exists())

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

        with mock.patch.object(os, "fdopen", side_effect=replace_then_fail):
            result, code = creation.create_set(
                self.projects, self.create_manifest(request_set, [member]), "fixture"
            )
        self.assertEqual((code, result["status"]), (3, "blocked"))
        self.assertEqual(destination.read_text(encoding="utf-8"), "replacement")
