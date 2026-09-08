"""Preflight and cooperative atomic request transitions."""

from __future__ import annotations

import hashlib
import os
import re
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import contract, inspection
from . import filesystem as fs


def transition_text(text: str, to_status: str, history_line: str, path: Path) -> str:
    status_line = re.compile(
        r"^- \*\*Request status:\*\*[ \t]+(?:`open`|open)[ \t]*$", re.MULTILINE
    )
    matches = list(status_line.finditer(text))
    if len(matches) != 1:
        raise contract.ContractError(
            "status_conflict",
            str(path),
            "open status line does not match the canonical form",
        )
    updated = status_line.sub(f"- **Request status:** `{to_status}`", text, count=1)
    if history_line in updated.splitlines():
        raise contract.ContractError(
            "history_conflict", str(path), "history transition already exists"
        )
    if not updated.endswith("\n"):
        updated += "\n"
    return updated + history_line + "\n"


@dataclass(frozen=True)
class PreparedTransition:
    path: Path
    record: contract.Record
    data: bytes
    stat_key: fs.StatKey
    pin: fs.PinnedDirectory
    already_transitioned: bool


@dataclass(frozen=True)
class TransitionPlan:
    to_status: str
    history_line: str
    members: tuple[PreparedTransition, ...]


def validate_members(
    members: list[dict[str, Any]], to_status: str, manifest_path: str
) -> tuple[str, ...]:
    keys_seen: list[tuple[str, str]] = []
    identities: set[str] = set()
    expected_member_names: list[str] = []
    for index, raw in enumerate(members):
        member_path = f"{manifest_path}#members/{index}"
        base_keys = {
            "target_repo",
            "filename",
            "request_identity",
            "expected_sha256",
        }
        required_keys = base_keys | (
            {"response_filename", "verified_target_commit", "verified_pin"}
            if to_status == "resolved"
            else set()
        )
        contract.exact_object(raw, required_keys, member_path)
        target = contract.validate_kebab(raw["target_repo"], member_path, "target_repo")
        filename = contract.validate_filename(raw["filename"], member_path)
        identity = contract.validate_identity(
            raw["request_identity"], target, member_path
        )
        expected_sha = raw["expected_sha256"]
        if not isinstance(expected_sha, str) or not contract.SHA_RE.fullmatch(
            expected_sha
        ):
            raise contract.ContractError(
                "invalid_digest", member_path, "expected_sha256 is invalid"
            )
        if identity in identities:
            raise contract.ContractError(
                "duplicate_member", member_path, "request identities must be unique"
            )
        identities.add(identity)
        keys_seen.append((target, filename))
        expected_member_names.append(f"{target}/{filename}")
        if to_status == "resolved":
            if raw["response_filename"] != filename:
                raise contract.ContractError(
                    "response_mismatch",
                    member_path,
                    "resolved response must have the same filename",
                )
            if not contract.COMMIT_RE.fullmatch(raw["verified_target_commit"]):
                raise contract.ContractError(
                    "invalid_commit",
                    member_path,
                    "verified target commit must be full hexadecimal text",
                )
            if (
                not isinstance(raw["verified_pin"], str)
                or not raw["verified_pin"]
                or contract.has_control(raw["verified_pin"])
            ):
                raise contract.ContractError(
                    "invalid_pin",
                    member_path,
                    "verified pin must be nonempty single-line text",
                )
    if keys_seen != sorted(keys_seen) or len(set(keys_seen)) != len(keys_seen):
        raise contract.ContractError(
            "member_order", manifest_path, "members must be unique and sorted"
        )
    return tuple(expected_member_names)


def prepare_transition(
    root: Path, manifest: dict[str, Any], manifest_path: str, resources: ExitStack
) -> TransitionPlan:
    prepared: list[PreparedTransition] = []
    contract.exact_object(
        manifest,
        {
            "schema",
            "consumer",
            "request_set",
            "authorization",
            "from_status",
            "to_status",
            "reason",
            "history_line",
            "members",
        },
        manifest_path,
    )
    if (
        manifest["schema"] != contract.TRANSITION_SCHEMA
        or manifest["consumer"] != contract.CONSUMER
    ):
        raise contract.ContractError(
            "invalid_manifest",
            manifest_path,
            "transition manifest schema or consumer is invalid",
        )
    root = fs.canonical_projects_root(str(root))
    request_set = contract.validate_set_id(manifest["request_set"], manifest_path)
    authorization_digest = contract.validate_authorization_shape(
        manifest["authorization"], manifest_path
    )
    if (
        manifest["from_status"] != "open"
        or manifest["to_status"] not in contract.TRANSITION_STATUSES
    ):
        raise contract.ContractError(
            "invalid_transition",
            manifest_path,
            "transition must move open to a recognized terminal status",
        )
    reason = manifest["reason"]
    if not isinstance(reason, str) or not reason or contract.has_control(reason):
        raise contract.ContractError(
            "invalid_transition",
            manifest_path,
            "reason must be nonempty single-line text",
        )
    history = manifest["history_line"]
    match = contract.HISTORY_RE.fullmatch(history) if isinstance(history, str) else None
    if (
        not match
        or match.group("status") != manifest["to_status"]
        or match.group("reason") != reason
    ):
        raise contract.ContractError(
            "invalid_transition",
            manifest_path,
            "history_line must canonically match status and reason",
        )
    members = manifest["members"]
    if not isinstance(members, list) or not members:
        raise contract.ContractError(
            "invalid_manifest", manifest_path, "members must be a nonempty array"
        )
    expected_members = validate_members(members, manifest["to_status"], manifest_path)
    destinations: list[Path] = []
    for raw in members:
        destinations.append(fs.request_path(root, raw["target_repo"], raw["filename"]))
    digest_lines = [f"{path} -> {manifest['to_status']}" for path in destinations]
    contract.validate_authorization(
        authorization_digest,
        contract.sha256_bytes("\n".join(digest_lines).encode("utf-8")),
        manifest_path,
    )
    for raw, destination in zip(members, destinations):
        target = raw["target_repo"]
        identity = raw["request_identity"]
        expected_sha = raw["expected_sha256"]
        pin = fs.pin_project_directory(root, target, "requests")
        resources.callback(pin.close)
        directory_fd = pin.leaf_fd
        data, stat_key = fs.snapshot_at(directory_fd, destination.name, destination)
        if not pin.matches_visible_chain():
            raise contract.ContractError(
                "unsafe_path",
                str(destination.parent),
                "requests directory identity changed during transition preflight",
            )
        record = contract.record_from_bytes(destination, data)
        if record.digest != expected_sha:
            raise contract.ContractError(
                "digest_mismatch",
                str(destination),
                "request differs from the authorized snapshot",
            )
        if (
            record.metadata["Request identity"] != identity
            or record.metadata["Request set"] != request_set
            or record.metadata["Request status"] not in {"open", manifest["to_status"]}
        ):
            raise contract.ContractError(
                "identity_mismatch",
                str(destination),
                "request identity, set, or source status does not match",
            )
        already_transitioned = (
            record.metadata["Request status"] == manifest["to_status"]
        )
        if (
            already_transitioned
            and manifest["history_line"] not in data.decode("utf-8").splitlines()
        ):
            raise contract.ContractError(
                "history_conflict",
                str(destination),
                "terminal member does not contain the authorized transition history",
            )
        if manifest["to_status"] == "resolved":
            response = fs.response_path(root, target, raw["response_filename"])
            response_pin = fs.pin_project_directory(root, target, "responses")
            try:
                fs.snapshot_at(response_pin.leaf_fd, response.name, response)
                if not response_pin.matches_visible_chain():
                    raise contract.ContractError(
                        "unsafe_path",
                        str(response.parent),
                        "responses directory identity changed during verification",
                    )
            finally:
                response_pin.close()
        prepared.append(
            PreparedTransition(
                destination, record, data, stat_key, pin, already_transitioned
            )
        )
    selected_milestones = {
        item.record.metadata["Selected milestone"] for item in prepared
    }
    if len(selected_milestones) != 1:
        raise contract.ContractError(
            "inconsistent_request_set",
            manifest_path,
            "transition members name different selected milestones",
        )
    for item in prepared:
        if inspection.exact_set_member_names(root, item.record) != expected_members:
            raise contract.ContractError(
                "inconsistent_request_set",
                str(item.path),
                "exact-set members do not match transition manifest",
            )
    return TransitionPlan(
        manifest["to_status"], manifest["history_line"], tuple(prepared)
    )


def acquire_lock(item: PreparedTransition, resources: ExitStack) -> None:
    if not item.pin.matches_visible_chain():
        raise contract.ContractError(
            "unsafe_path",
            str(item.path.parent),
            "requests directory identity changed before lock acquisition",
        )
    lock_name = item.path.name + ".lock"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_name, flags, 0o600, dir_fd=item.pin.leaf_fd)
    except FileExistsError as exc:
        raise contract.ContractError(
            "lock_conflict",
            str(item.path.with_name(lock_name)),
            "another cooperating writer holds the transition lock",
        ) from exc
    lock_info = os.fstat(descriptor)
    os.close(descriptor)
    resources.callback(
        fs.unlink_owned_at,
        item.pin.leaf_fd,
        lock_name,
        (lock_info.st_dev, lock_info.st_ino),
    )
    if not item.pin.matches_visible_chain():
        raise contract.ContractError(
            "unsafe_path",
            str(item.path.parent),
            "requests directory identity changed during lock acquisition",
        )


def apply_member(
    item: PreparedTransition,
    plan: TransitionPlan,
    transitioned: list[str],
    already_untouched: list[str],
) -> None:
    current_data, current_stat = fs.snapshot_at(
        item.pin.leaf_fd, item.path.name, item.path
    )
    if (
        current_data != item.data
        or current_stat != item.stat_key
        or contract.sha256_bytes(current_data) != item.record.digest
    ):
        raise contract.ContractError(
            "digest_mismatch",
            str(item.path),
            "request changed after transition preflight",
        )
    if item.already_transitioned:
        already_untouched.append(str(item.path))
        return
    updated = transition_text(
        current_data.decode("utf-8"),
        plan.to_status,
        plan.history_line,
        item.path,
    )
    updated_bytes = updated.encode("utf-8")
    temp_name = (
        f".{item.path.name}.{os.getpid()}."
        f"{hashlib.sha256(updated_bytes).hexdigest()[:16]}.tmp"
    )
    temp_created = False
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(
            temp_name,
            flags,
            item.stat_key.mode,
            dir_fd=item.pin.leaf_fd,
        )
        temp_created = True
        temp_info = os.fstat(descriptor)
        temp_identity = (temp_info.st_dev, temp_info.st_ino)
        os.fchmod(descriptor, item.stat_key.mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(updated_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        final_data, final_stat = fs.snapshot_at(
            item.pin.leaf_fd, item.path.name, item.path
        )
        if final_data != item.data or final_stat != item.stat_key:
            raise contract.ContractError(
                "digest_mismatch",
                str(item.path),
                "request changed before atomic replacement",
            )
        if not item.pin.matches_visible_chain():
            raise contract.ContractError(
                "unsafe_path",
                str(item.path.parent),
                "requests directory identity changed before replacement",
            )
        os.replace(
            temp_name,
            item.path.name,
            src_dir_fd=item.pin.leaf_fd,
            dst_dir_fd=item.pin.leaf_fd,
        )
        temp_created = False
        transitioned.append(str(item.path))
    except OSError as exc:
        raise contract.ContractError(
            "write_failure",
            str(item.path),
            "atomic request transition did not complete",
        ) from exc
    finally:
        if temp_created:
            fs.unlink_owned_at(item.pin.leaf_fd, temp_name, temp_identity)
    verified_data, _ = fs.snapshot_at(item.pin.leaf_fd, item.path.name, item.path)
    if verified_data != updated_bytes:
        raise contract.ContractError(
            "post_write_mismatch",
            str(item.path),
            "transition output changed before exact post-write verification",
        )
    verified = contract.record_from_bytes(item.path, verified_data)
    if (
        verified.metadata["Request status"] != plan.to_status
        or plan.history_line not in verified_data.decode("utf-8").splitlines()
    ):
        raise contract.ContractError(
            "post_write_mismatch",
            str(item.path),
            "transition failed post-write verification",
        )
    if not item.pin.matches_visible_chain():
        raise contract.ContractError(
            "unsafe_path",
            str(item.path.parent),
            "requests directory identity changed after replacement",
        )
    os.fsync(item.pin.leaf_fd)


def apply_transition(plan: TransitionPlan) -> tuple[dict[str, Any], int]:
    result = contract.result_object("transition-set")
    transitioned: list[str] = []
    already_untouched: list[str] = []
    try:
        with ExitStack() as resources:
            for item in plan.members:
                acquire_lock(item, resources)
            for item in plan.members:
                apply_member(item, plan, transitioned, already_untouched)
    except contract.ContractError as exc:
        result["transitioned"] = sorted(transitioned)
        result["untouched"] = sorted(
            set(already_untouched)
            | {
                str(item.path)
                for item in plan.members
                if str(item.path) not in transitioned
            }
        )
        result["status"] = "partial" if transitioned else "blocked"
        contract.add_errors(result, [exc])
        return result, 4
    result["transitioned"] = sorted(transitioned)
    result["untouched"] = sorted(already_untouched)
    return result, 0


def transition_set(
    root: Path, manifest: dict[str, Any], manifest_path: str
) -> tuple[dict[str, Any], int]:
    with ExitStack() as resources:
        try:
            plan = prepare_transition(root, manifest, manifest_path, resources)
        except contract.ContractError as exc:
            result = contract.result_object("transition-set")
            result["status"] = "blocked"
            contract.add_errors(result, [exc])
            code = (
                4
                if exc.code in {"digest_mismatch", "status_conflict", "lock_conflict"}
                else 2
            )
            return result, code
        return apply_transition(plan)
