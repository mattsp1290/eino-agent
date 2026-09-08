"""Preflight and deterministic exclusive request creation."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import contract, inspection
from . import filesystem as fs


def validate_target_path(member: dict[str, Any], path: str) -> None:
    checkout = Path(member["target_checkout"])
    fs.require_real_directory(checkout)
    if checkout.resolve(strict=True).name != member["target_repo"]:
        raise contract.ContractError(
            "target_identity_mismatch",
            path,
            "target checkout does not match target_repo",
        )


@dataclass(frozen=True)
class PreparedCreate:
    target: str
    path: Path
    body: bytes
    reuse: bool


@dataclass(frozen=True)
class CreationPlan:
    root: Path
    members: tuple[PreparedCreate, ...]


def validate_members(
    members: list[dict[str, Any]], manifest_path: str
) -> tuple[str, ...]:
    sort_keys: list[tuple[str, str]] = []
    identities: set[str] = set()
    member_names: list[str] = []
    for index, raw in enumerate(members):
        member_path = f"{manifest_path}#members/{index}"
        if not isinstance(raw, dict):
            raise contract.ContractError(
                "invalid_manifest", member_path, "member must be an object"
            )
        mode = raw.get("mode")
        keys = {
            "target_repo",
            "filename",
            "request_identity",
            "target_checkout",
            "target_module",
            "target_commit",
            "body",
            "mode",
        }
        if mode == "reuse":
            keys.add("expected_sha256")
        elif mode != "create":
            raise contract.ContractError(
                "invalid_manifest", member_path, "mode must be create or reuse"
            )
        contract.exact_object(raw, keys, member_path)
        target = contract.validate_kebab(raw["target_repo"], member_path, "target_repo")
        filename = contract.validate_filename(raw["filename"], member_path)
        identity = contract.validate_identity(
            raw["request_identity"], target, member_path
        )
        if identity in identities:
            raise contract.ContractError(
                "duplicate_member", member_path, "request identities must be unique"
            )
        identities.add(identity)
        contract.validate_target_scalars(raw, member_path)
        if mode == "reuse" and (
            not isinstance(raw["expected_sha256"], str)
            or not contract.SHA_RE.fullmatch(raw["expected_sha256"])
        ):
            raise contract.ContractError(
                "invalid_digest", member_path, "expected_sha256 is invalid"
            )
        if not isinstance(raw["body"], str) or "\x00" in raw["body"]:
            raise contract.ContractError(
                "invalid_request", member_path, "body must be UTF-8-compatible text"
            )
        try:
            raw["body"].encode("utf-8")
        except UnicodeEncodeError as exc:
            raise contract.ContractError(
                "invalid_request", member_path, "body must be UTF-8-compatible text"
            ) from exc
        sort_keys.append((target, filename))
        member_names.append(f"{target}/{filename}")
    if sort_keys != sorted(sort_keys) or len(set(sort_keys)) != len(sort_keys):
        raise contract.ContractError(
            "member_order",
            manifest_path,
            "members must be unique and sorted by target_repo and filename",
        )
    return tuple(member_names)


def prepare_creation(
    root: Path, manifest: dict[str, Any], manifest_path: str
) -> CreationPlan:
    prepared: list[PreparedCreate] = []
    contract.exact_object(
        manifest,
        {"schema", "consumer", "request_set", "authorization", "members"},
        manifest_path,
    )
    if (
        manifest["schema"] != contract.CREATE_SCHEMA
        or manifest["consumer"] != contract.CONSUMER
    ):
        raise contract.ContractError(
            "invalid_manifest",
            manifest_path,
            "create manifest schema or consumer is invalid",
        )
    root = fs.canonical_projects_root(str(root))
    request_set = contract.validate_set_id(manifest["request_set"], manifest_path)
    authorization_digest = contract.validate_authorization_shape(
        manifest["authorization"], manifest_path
    )
    members = manifest["members"]
    if not isinstance(members, list) or not members:
        raise contract.ContractError(
            "invalid_manifest", manifest_path, "members must be a nonempty array"
        )
    member_names = validate_members(members, manifest_path)
    destinations: list[Path] = []
    for index, raw in enumerate(members):
        destinations.append(
            fs.request_path(root, raw["target_repo"], raw["filename"], create_dirs=True)
        )
    expected_members = tuple(sorted(member_names))
    digest = contract.sha256_bytes(
        "\n".join(str(path) for path in destinations).encode("utf-8")
    )
    contract.validate_authorization(authorization_digest, digest, manifest_path)
    selected_milestones: set[str] = set()
    for index, (raw, destination) in enumerate(zip(members, destinations)):
        member_path = f"{manifest_path}#members/{index}"
        body_bytes = raw["body"].encode("utf-8")
        title, metadata, body_members = contract.parse_request_text(
            raw["body"], destination
        )
        del title
        validate_target_path(raw, member_path)
        if (
            metadata["Request status"] != "open"
            or metadata["Request identity"] != raw["request_identity"]
        ):
            raise contract.ContractError(
                "identity_mismatch",
                str(destination),
                "request body identity or status is invalid",
            )
        expected_target = f"{raw['target_module']} ({raw['target_checkout']})"
        if metadata["Target repo"] != expected_target:
            raise contract.ContractError(
                "target_identity_mismatch",
                str(destination),
                "request body target does not match preflight",
            )
        if (
            metadata["Pinned commit under evaluation"].lower()
            != raw["target_commit"].lower()
        ):
            raise contract.ContractError(
                "target_identity_mismatch",
                str(destination),
                "request body commit does not match preflight",
            )
        if raw["mode"] == "create":
            selected_milestones.add(metadata["Selected milestone"])
            if (
                metadata["Request set"] != request_set
                or body_members != expected_members
            ):
                raise contract.ContractError(
                    "inconsistent_request_set",
                    str(destination),
                    "new request body does not contain the complete manifest set",
                )
            if metadata["Date"] != request_set[:10]:
                raise contract.ContractError(
                    "inconsistent_request_set",
                    str(destination),
                    "request date and request-set date do not match",
                )
            if destination.exists() or destination.is_symlink():
                raise contract.ContractError(
                    "create_conflict",
                    str(destination),
                    "destination already exists; use verified reuse or a new authorized slug",
                )
        else:
            expected_sha = raw["expected_sha256"]
            existing_record = inspection.read_project_record(
                root, raw["target_repo"], raw["filename"]
            )
            if contract.sha256_bytes(body_bytes) != expected_sha:
                raise contract.ContractError(
                    "digest_mismatch",
                    str(destination),
                    "reused record does not match its expected snapshot",
                )
            if existing_record.digest != expected_sha:
                raise contract.ContractError(
                    "digest_mismatch",
                    str(destination),
                    "reused record does not match its expected snapshot",
                )
            if existing_record.metadata["Request identity"] != raw["request_identity"]:
                raise contract.ContractError(
                    "identity_mismatch",
                    str(destination),
                    "reused record identity does not match",
                )
            if existing_record.metadata["Request set"] == request_set:
                selected_milestones.add(existing_record.metadata["Selected milestone"])
                if existing_record.members != expected_members:
                    raise contract.ContractError(
                        "inconsistent_request_set",
                        str(destination),
                        "reused partial-set member does not match the manifest set",
                    )
            else:
                set_errors = inspection.validate_complete_record_set(
                    root, existing_record
                )
                if set_errors:
                    raise set_errors[0]
        prepared.append(
            PreparedCreate(
                raw["target_repo"], destination, body_bytes, raw["mode"] == "reuse"
            )
        )
    if len(selected_milestones) > 1:
        raise contract.ContractError(
            "inconsistent_request_set",
            manifest_path,
            "request-set members name different selected milestones",
        )
    return CreationPlan(root, tuple(prepared))


def create_member(root: Path, item: PreparedCreate) -> None:
    destination, body = item.path, item.body
    requests = destination.parent
    pinned = fs.pin_project_directory(root, item.target, "requests", create=True)
    directory_fd = pinned.leaf_fd
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(destination.name, flags, 0o600, dir_fd=directory_fd)
        except FileExistsError as exc:
            raise contract.ContractError(
                "create_conflict",
                str(destination),
                "destination appeared during exclusive creation",
            ) from exc
        leaf_info = os.fstat(descriptor)
        leaf_identity = (leaf_info.st_dev, leaf_info.st_ino)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            if not pinned.matches_visible_chain():
                raise contract.ContractError(
                    "unsafe_path",
                    str(requests),
                    "requests directory identity changed during creation",
                )
            if fs.snapshot_at(directory_fd, destination.name, destination)[0] != body:
                raise contract.ContractError(
                    "post_write_mismatch",
                    str(destination),
                    "created record failed post-write verification",
                )
            os.fsync(directory_fd)
        except (contract.ContractError, OSError) as exc:
            fs.unlink_owned_at(directory_fd, destination.name, leaf_identity)
            if isinstance(exc, contract.ContractError):
                raise
            raise contract.ContractError(
                "write_failure",
                str(destination),
                "exclusive request creation did not complete",
            ) from exc
    finally:
        pinned.close()


def apply_creation(plan: CreationPlan) -> tuple[dict[str, Any], int]:
    result = contract.result_object("create-set")
    created: list[str] = []
    reused: list[str] = []
    try:
        for item in plan.members:
            if item.reuse:
                reused.append(str(item.path))
                continue
            create_member(plan.root, item)
            created.append(str(item.path))
    except contract.ContractError as exc:
        result["created"] = sorted(created)
        result["reused"] = sorted(reused)
        result["untouched"] = sorted(
            str(item.path)
            for item in plan.members
            if str(item.path) not in created and str(item.path) not in reused
        )
        result["status"] = "partial" if created else "blocked"
        contract.add_errors(result, [exc])
        return result, 3
    result["created"] = sorted(created)
    result["reused"] = sorted(reused)
    return result, 0


def create_set(
    root: Path, manifest: dict[str, Any], manifest_path: str
) -> tuple[dict[str, Any], int]:
    try:
        plan = prepare_creation(root, manifest, manifest_path)
    except contract.ContractError as exc:
        result = contract.result_object("create-set")
        result["status"] = "blocked"
        contract.add_errors(result, [exc])
        return result, 3 if exc.code == "create_conflict" else 2
    return apply_creation(plan)
