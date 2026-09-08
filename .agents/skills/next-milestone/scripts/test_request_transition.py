from __future__ import annotations

import hashlib
import os
import threading
from pathlib import Path
from unittest import mock

from request_record import contract, creation, transition
from request_record import filesystem as fs
from request_test_support import DATE, RequestFixture


class TransitionTests(RequestFixture):
    def test_digest_drift_blocks_transition_without_mutation(self) -> None:
        request_set, _, member, destination = self.create_one()
        original = destination.read_bytes()
        transition_member = dict(member)
        transition_member["expected_sha256"] = hashlib.sha256(original).hexdigest()
        destination.write_bytes(original + b"\n")
        changed = destination.read_bytes()
        result, code = transition.transition_set(
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
        original_snapshot = fs.snapshot_at
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
            outcome["winner"] = transition.transition_set(
                self.projects, winner_manifest, "winner"
            )

        with mock.patch.object(fs, "snapshot_at", side_effect=controlled_snapshot):
            thread = threading.Thread(target=run_winner, name="winner")
            thread.start()
            self.assertTrue(lock_held.wait(timeout=5))
            loser_result, loser_code = transition.transition_set(
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
        created, code = creation.create_set(
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
        original_snapshot = fs.snapshot_at
        counts: dict[Path, int] = {}

        def drift_second(directory_fd: int, filename: str, path: Path):
            counts[path] = counts.get(path, 0) + 1
            if path == second_path and counts[path] == 2:
                path.write_text(
                    path.read_text(encoding="utf-8") + "\n", encoding="utf-8"
                )
            return original_snapshot(directory_fd, filename, path)

        with mock.patch.object(fs, "snapshot_at", side_effect=drift_second):
            result, transition_code = transition.transition_set(
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
        recovered, recovered_code = transition.transition_set(
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
        real_open = os.open
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

        with mock.patch.object(os, "open", side_effect=swap_ancestor):
            result, code = transition.transition_set(self.projects, manifest, "fixture")
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
        original_snapshot = fs.snapshot_at
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

        with mock.patch.object(fs, "snapshot_at", side_effect=edit_after_replace):
            result, code = transition.transition_set(self.projects, manifest, "fixture")
        self.assertEqual((code, result["status"]), (4, "partial"))
        self.assertEqual(result["errors"][0]["code"], "post_write_mismatch")
        self.assertEqual(result["transitioned"], [str(destination)])

    def test_mode_drift_during_lock_is_detected_before_transition(self) -> None:
        request_set, _, member, destination = self.create_one()
        member["expected_sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
        manifest = self.transition_manifest(request_set, [member])
        original_snapshot = fs.snapshot_at
        changed = False
        lock = destination.with_name(destination.name + ".lock")

        def chmod_after_lock(directory_fd: int, filename: str, path: Path):
            nonlocal changed
            if lock.exists() and not changed:
                changed = True
                path.chmod(0o644)
            return original_snapshot(directory_fd, filename, path)

        with mock.patch.object(fs, "snapshot_at", side_effect=chmod_after_lock):
            result, code = transition.transition_set(self.projects, manifest, "fixture")
        self.assertEqual((code, result["status"]), (4, "blocked"))
        self.assertEqual(result["errors"][0]["code"], "digest_mismatch")
        self.assertEqual(destination.stat().st_mode & 0o777, 0o644)
        self.assertIn("`open`", destination.read_text(encoding="utf-8"))

    def test_lock_cleanup_does_not_delete_replacement_lock(self) -> None:
        request_set, filename, member, destination = self.create_one()
        member["expected_sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
        manifest = self.transition_manifest(request_set, [member])
        original_snapshot = fs.snapshot_at
        replaced = False
        lock = destination.with_name(filename + ".lock")

        def replace_lock_then_fail(directory_fd: int, leaf: str, path: Path):
            nonlocal replaced
            if lock.exists() and not replaced:
                replaced = True
                lock.unlink()
                lock.write_text("replacement lock", encoding="utf-8")
                raise contract.ContractError(
                    "digest_mismatch", str(path), "fixture drift"
                )
            return original_snapshot(directory_fd, leaf, path)

        with mock.patch.object(fs, "snapshot_at", side_effect=replace_lock_then_fail):
            result, code = transition.transition_set(self.projects, manifest, "fixture")
        self.assertEqual((code, result["status"]), (4, "blocked"))
        self.assertEqual(lock.read_text(encoding="utf-8"), "replacement lock")

    def test_terminal_recovery_member_is_rechecked_after_locking(self) -> None:
        request_set, filename, member, destination = self.create_one()
        member["expected_sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
        first, first_code = transition.transition_set(
            self.projects, self.transition_manifest(request_set, [member]), "first"
        )
        self.assertEqual((first_code, first["status"]), (0, "complete"))
        member["expected_sha256"] = hashlib.sha256(destination.read_bytes()).hexdigest()
        manifest = self.transition_manifest(request_set, [member])
        real_open = os.open
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

        with mock.patch.object(os, "open", side_effect=edit_after_lock):
            result, code = transition.transition_set(
                self.projects, manifest, "recovery"
            )
        self.assertTrue(changed)
        self.assertEqual((code, result["status"]), (4, "blocked"))
        self.assertEqual(result["errors"][0]["code"], "digest_mismatch")
