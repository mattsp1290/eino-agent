#!/usr/bin/env python3
"""Safely inspect and mutate next-milestone cross-repository request records."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import stat
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CREATE_SCHEMA = "eino-agent-next-milestone/request-create-set/v1"
TRANSITION_SCHEMA = "eino-agent-next-milestone/request-transition-set/v1"
RESULT_SCHEMA = "eino-agent-next-milestone/request-result/v1"
CONSUMER = "eino-agent"
VALID_STATUSES = {"open", "withdrawn", "superseded", "resolved"}
TRANSITION_STATUSES = {"withdrawn", "superseded", "resolved"}
KEBAB_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
FILENAME_RE = re.compile(
    r"^(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})-[a-z0-9]+(?:-[a-z0-9]+)*\.md$"
)
SET_RE = re.compile(r"^(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})-[a-z0-9]+(?:-[a-z0-9]+)*$")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")
RFC3339_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?(?:[Zz]|[+-][0-9]{2}:[0-9]{2})$"
)
IDENTITY_RE = re.compile(
    r"^eino-agent/(?P<target>[a-z0-9]+(?:-[a-z0-9]+)*)/"
    r"(?P<contract>[a-z0-9]+(?:-[a-z0-9]+)*)/v1$"
)
TARGET_FIELD_RE = re.compile(r"^(?P<module>\S+) \((?P<checkout>/[^()\r\n]+)\)$")
FIELD_RE = re.compile(r"^- \*\*(?P<label>[^*\r\n]+):\*\*[ \t]*(?P<value>[^\r\n]*)$")
HISTORY_RE = re.compile(
    r"^- (?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2}) — `(?P<status>withdrawn|superseded|resolved)`: (?P<reason>[^\r\n]+)$"
)
FIELDS = (
    "Requested by",
    "Blocker consumer",
    "Request status",
    "Date",
    "Priority",
    "Selected milestone",
    "Request set",
    "Request set members",
    "Request identity",
    "Target repo",
    "Pinned commit under evaluation",
    "Consumer",
)
SECTIONS = (
    "Background",
    "Ask",
    "Out of scope",
    "Acceptance",
    "Response and unblock contract",
    "References",
    "Status history",
)


@dataclass(frozen=True)
class Record:
    path: Path
    title: str
    metadata: dict[str, str]
    members: tuple[str, ...]
    digest: str
    response_path: Path | None


@dataclass
class PinnedDirectory:
    root_path: Path
    project_path: Path
    leaf_path: Path
    root_fd: int
    project_fd: int
    leaf_fd: int

    def matches_visible_chain(self) -> bool:
        for path, descriptor in (
            (self.root_path, self.root_fd),
            (self.project_path, self.project_fd),
            (self.leaf_path, self.leaf_fd),
        ):
            try:
                opened = os.fstat(descriptor)
                visible = path.lstat()
            except (FileNotFoundError, OSError):
                return False
            if (
                not stat.S_ISDIR(opened.st_mode)
                or stat.S_ISLNK(visible.st_mode)
                or (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino)
            ):
                return False
        return True

    def close(self) -> None:
        os.close(self.leaf_fd)
        os.close(self.project_fd)
        os.close(self.root_fd)


class ContractError(Exception):
    def __init__(self, code: str, path: str, message: str):
        super().__init__(message)
        self.code = code
        self.path = path
        self.message = message


class ContractArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ContractError("invalid_arguments", "arguments", message)


def error_item(error: ContractError) -> dict[str, str]:
    return {"code": error.code, "path": error.path, "message": error.message}


def result_object(operation: str) -> dict[str, Any]:
    return {
        "schema": RESULT_SCHEMA,
        "operation": operation,
        "status": "complete",
        "created": [],
        "reused": [],
        "transitioned": [],
        "untouched": [],
        "errors": [],
    }


def add_errors(result: dict[str, Any], errors: Iterable[ContractError]) -> None:
    existing = {
        (item["code"], item["path"], item["message"]) for item in result["errors"]
    }
    for error in errors:
        item = error_item(error)
        key = (item["code"], item["path"], item["message"])
        if key not in existing:
            result["errors"].append(item)
            existing.add(key)
    result["errors"].sort(
        key=lambda item: (item["path"], item["code"], item["message"])
    )


def normalized_scalar(value: str) -> str:
    value = value.strip(" \t")
    if len(value) >= 2 and value[0] == "`" and value[-1] == "`":
        value = value[1:-1]
    return value.strip(" \t")


def has_control(value: str) -> bool:
    return any(ord(char) < 32 or ord(char) == 127 for char in value)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parse_rfc3339(value: Any, path: str) -> str:
    if (
        not isinstance(value, str)
        or not RFC3339_RE.fullmatch(value)
        or has_control(value)
    ):
        raise ContractError(
            "invalid_authorization", path, "confirmed_at must be RFC3339 text"
        )
    candidate = value[:-1] + "+00:00" if value[-1] in "Zz" else value
    candidate = candidate[:10] + "T" + candidate[11:]
    if candidate[17:19] == "60":
        candidate = candidate[:17] + "59" + candidate[19:]
    try:
        parsed = dt.datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ContractError(
            "invalid_authorization", path, "confirmed_at must be RFC3339 text"
        ) from exc
    if parsed.tzinfo is None:
        raise ContractError(
            "invalid_authorization", path, "confirmed_at must include a timezone"
        )
    return value


def exact_object(
    value: Any, keys: set[str], path: str, code: str = "invalid_manifest"
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(code, path, "value must be an object")
    actual = set(value)
    if actual != keys:
        raise ContractError(code, path, "object keys do not match the versioned schema")
    return value


def validate_kebab(value: Any, path: str, label: str = "value") -> str:
    if not isinstance(value, str) or not KEBAB_RE.fullmatch(value):
        raise ContractError(
            "invalid_scalar", path, f"{label} must be one lowercase kebab segment"
        )
    return value


def validate_filename(value: Any, path: str) -> str:
    if not isinstance(value, str) or not FILENAME_RE.fullmatch(value):
        raise ContractError(
            "invalid_filename",
            path,
            "filename must be a canonical dated Markdown basename",
        )
    if Path(value).name != value or "/" in value or "\\" in value:
        raise ContractError(
            "invalid_filename", path, "filename must be a direct-child basename"
        )
    return value


def validate_set_id(value: Any, path: str) -> str:
    if not isinstance(value, str) or not SET_RE.fullmatch(value):
        raise ContractError(
            "invalid_request_set",
            path,
            "request_set must be a canonical dated kebab identifier",
        )
    return value


def validate_identity(value: Any, target_repo: str, path: str) -> str:
    if not isinstance(value, str):
        raise ContractError("invalid_identity", path, "request_identity must be text")
    match = IDENTITY_RE.fullmatch(value)
    if not match or match.group("target") != target_repo:
        raise ContractError(
            "invalid_identity",
            path,
            "request_identity does not match the verified target",
        )
    return value


def extract_field_values(text: str, label: str) -> list[str]:
    values: list[str] = []
    for line in text.splitlines():
        match = FIELD_RE.fullmatch(line)
        if match and match.group("label") == label:
            values.append(normalized_scalar(match.group("value")))
    return values


def parse_members(value: str, path: str) -> tuple[str, ...]:
    raw_members = [part.strip(" \t") for part in value.split(",")]
    if not raw_members or any(not item for item in raw_members):
        raise ContractError(
            "invalid_set_members",
            path,
            "request set members must be a nonempty comma-separated list",
        )
    normalized: list[str] = []
    for item in raw_members:
        parts = item.split("/")
        if len(parts) != 2:
            raise ContractError(
                "invalid_set_members",
                path,
                "each request set member must be target/filename",
            )
        validate_kebab(parts[0], path, "request set target")
        validate_filename(parts[1], path)
        normalized.append(item)
    if normalized != sorted(normalized) or len(set(normalized)) != len(normalized):
        raise ContractError(
            "invalid_set_members", path, "request set members must be unique and sorted"
        )
    return tuple(normalized)


def parse_request_text(
    text: str, path: Path
) -> tuple[str, dict[str, str], tuple[str, ...]]:
    lines = text.splitlines()
    if not lines or not lines[0].startswith("# Request: ") or not lines[0][11:].strip():
        raise ContractError(
            "invalid_request", str(path), "request title is missing or malformed"
        )
    title = lines[0][11:].strip()
    first_section = next(
        (index for index, line in enumerate(lines) if line.startswith("## ")),
        len(lines),
    )
    found: list[tuple[str, str]] = []
    for line in lines[1:first_section]:
        if not line.strip():
            continue
        match = FIELD_RE.fullmatch(line)
        if not match:
            raise ContractError(
                "invalid_metadata",
                str(path),
                "metadata must use the exact bold scalar form",
            )
        found.append((match.group("label"), normalized_scalar(match.group("value"))))
    labels = [label for label, _ in found]
    if tuple(labels) != FIELDS:
        raise ContractError(
            "invalid_metadata",
            str(path),
            "metadata fields are missing, duplicated, unknown, or out of order",
        )
    metadata = dict(found)
    for label, value in metadata.items():
        if not value or has_control(value):
            raise ContractError(
                "invalid_metadata",
                str(path),
                f"{label} must be nonempty single-line text",
            )
    if metadata["Requested by"] != "`eino-agent` next-milestone selection":
        raise ContractError(
            "identity_mismatch", str(path), "Requested by does not match this skill"
        )
    if metadata["Blocker consumer"] != CONSUMER or metadata["Consumer"] != CONSUMER:
        raise ContractError(
            "identity_mismatch",
            str(path),
            "consumer metadata does not match eino-agent",
        )
    if metadata["Request status"] not in VALID_STATUSES:
        raise ContractError(
            "invalid_status", str(path), "Request status is not recognized"
        )
    if not SET_RE.fullmatch(metadata["Request set"]):
        raise ContractError(
            "invalid_request_set", str(path), "Request set is malformed"
        )
    if metadata["Request set"][:10] != metadata["Date"]:
        raise ContractError(
            "inconsistent_request_set",
            str(path),
            "Request set date does not match request metadata",
        )
    identity_match = IDENTITY_RE.fullmatch(metadata["Request identity"])
    if not identity_match:
        raise ContractError(
            "invalid_identity", str(path), "Request identity is malformed"
        )
    target_match = TARGET_FIELD_RE.fullmatch(metadata["Target repo"])
    stored_target = path.parent.parent.name
    if (
        not target_match
        or target_match.group("module").rstrip("/").removesuffix(".git").split("/")[-1]
        != stored_target
        or Path(target_match.group("checkout")).name != stored_target
    ):
        raise ContractError(
            "target_identity_mismatch",
            str(path),
            "Target repo must canonically identify the storage project",
        )
    members = parse_members(metadata["Request set members"], str(path))
    filename_match = FILENAME_RE.fullmatch(path.name)
    if not filename_match or metadata["Date"] != filename_match.group("date"):
        raise ContractError(
            "identity_mismatch", str(path), "Date does not match the request filename"
        )
    if not COMMIT_RE.fullmatch(metadata["Pinned commit under evaluation"]):
        raise ContractError(
            "invalid_commit",
            str(path),
            "Pinned commit must be a full hexadecimal commit",
        )
    section_names = [line[3:].strip() for line in lines if line.startswith("## ")]
    if tuple(section_names) != SECTIONS:
        raise ContractError(
            "invalid_sections",
            str(path),
            "required sections are missing, duplicated, unknown, or out of order",
        )
    section_indexes = [
        index for index, line in enumerate(lines) if line.startswith("## ")
    ]
    for position, index in enumerate(section_indexes):
        end = (
            section_indexes[position + 1]
            if position + 1 < len(section_indexes)
            else len(lines)
        )
        if not any(line.strip() for line in lines[index + 1 : end]):
            raise ContractError(
                "invalid_sections", str(path), "required sections must contain content"
            )
    return title, metadata, members


def canonical_projects_root(value: str) -> Path:
    supplied = Path(value)
    if not supplied.is_absolute():
        raise ContractError("unsafe_path", value, "projects root must be absolute")
    try:
        info = supplied.lstat()
    except FileNotFoundError as exc:
        raise ContractError(
            "unsafe_path", value, "projects root does not exist"
        ) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ContractError(
            "unsafe_path", value, "projects root must be a real directory"
        )
    return supplied.resolve(strict=True)


def require_real_directory(path: Path, allow_missing: bool = False) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        if allow_missing:
            return
        raise ContractError(
            "unsafe_path", str(path), "required directory does not exist"
        )
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ContractError(
            "unsafe_path", str(path), "path component must be a real directory"
        )


def require_real_file(path: Path) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise ContractError(
            "missing_record", str(path), "required file does not exist"
        ) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ContractError(
            "unsafe_path", str(path), "path must be a regular non-symlink file"
        )


def open_directory_fd(path: Path) -> int:
    """Open and pin a real directory, rejecting a path-to-inode race."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ContractError(
            "unsafe_path",
            str(path),
            "directory could not be opened without symlink traversal",
        ) from exc
    try:
        opened = os.fstat(descriptor)
        current = path.lstat()
        if (
            not stat.S_ISDIR(opened.st_mode)
            or stat.S_ISLNK(current.st_mode)
            or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
        ):
            raise ContractError(
                "unsafe_path", str(path), "directory identity changed during validation"
            )
    except (ContractError, OSError):
        os.close(descriptor)
        raise
    return descriptor


def open_child_directory_fd(
    parent_fd: int, name: str, display_path: Path, create: bool
) -> int:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        return os.open(name, flags, dir_fd=parent_fd)
    except FileNotFoundError:
        if not create:
            raise ContractError(
                "unsafe_path", str(display_path), "required directory does not exist"
            )
        try:
            os.mkdir(name, 0o700, dir_fd=parent_fd)
        except FileExistsError:
            pass
        try:
            return os.open(name, flags, dir_fd=parent_fd)
        except OSError as exc:
            raise ContractError(
                "unsafe_path",
                str(display_path),
                "created directory could not be pinned without symlink traversal",
            ) from exc
    except OSError as exc:
        raise ContractError(
            "unsafe_path",
            str(display_path),
            "directory component could not be opened without symlink traversal",
        ) from exc


def pin_named_directory(
    root: Path, project_name: str, leaf_name: str, create: bool = False
) -> PinnedDirectory:
    if (
        Path(project_name).name != project_name
        or "/" in project_name
        or "\\" in project_name
        or Path(leaf_name).name != leaf_name
    ):
        raise ContractError(
            "unsafe_path", project_name, "directory names must be direct children"
        )
    root_fd = open_directory_fd(root)
    project_path = root / project_name
    leaf_path = project_path / leaf_name
    try:
        project_fd = open_child_directory_fd(
            root_fd, project_name, project_path, create=create
        )
    except (ContractError, OSError):
        os.close(root_fd)
        raise
    try:
        leaf_fd = open_child_directory_fd(
            project_fd, leaf_name, leaf_path, create=create
        )
    except (ContractError, OSError):
        os.close(project_fd)
        os.close(root_fd)
        raise
    pinned = PinnedDirectory(
        root_path=root,
        project_path=project_path,
        leaf_path=leaf_path,
        root_fd=root_fd,
        project_fd=project_fd,
        leaf_fd=leaf_fd,
    )
    if not pinned.matches_visible_chain():
        pinned.close()
        raise ContractError(
            "unsafe_path",
            str(leaf_path),
            "project directory chain changed while it was being pinned",
        )
    return pinned


def pin_project_directory(
    root: Path, target_repo: str, leaf_name: str, create: bool = False
) -> PinnedDirectory:
    validate_kebab(target_repo, target_repo, "target_repo")
    return pin_named_directory(root, target_repo, leaf_name, create)


def read_bytes_at(directory_fd: int, filename: str, display_path: Path) -> bytes:
    flags = os.O_RDONLY | os.O_NONBLOCK
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(filename, flags, dir_fd=directory_fd)
    except OSError as exc:
        raise ContractError(
            "unsafe_path",
            str(display_path),
            "file could not be opened relative to its validated directory",
        ) from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ContractError(
                "unsafe_path", str(display_path), "opened request is not a regular file"
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
    finally:
        os.close(descriptor)


def entry_identity_at(directory_fd: int, filename: str) -> tuple[int, int] | None:
    try:
        info = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    return info.st_dev, info.st_ino


def unlink_owned_at(
    directory_fd: int, filename: str, owned_identity: tuple[int, int]
) -> bool:
    """Remove a helper-created entry only while its visible inode is still ours."""
    if entry_identity_at(directory_fd, filename) != owned_identity:
        return False
    try:
        os.unlink(filename, dir_fd=directory_fd)
    except FileNotFoundError:
        return False
    return True


def request_path(
    root: Path, target_repo: str, filename: str, create_dirs: bool = False
) -> Path:
    validate_kebab(target_repo, target_repo, "target_repo")
    validate_filename(filename, filename)
    project = root / target_repo
    requests = project / "requests"
    if project.parent != root or requests.parent != project:
        raise ContractError(
            "unsafe_path", str(project), "project path is not a direct child"
        )
    require_real_directory(project, allow_missing=create_dirs)
    require_real_directory(requests, allow_missing=create_dirs)
    destination = requests / filename
    if destination.parent != requests:
        raise ContractError(
            "unsafe_path", str(destination), "request destination is not a direct child"
        )
    if destination.exists() or destination.is_symlink():
        require_real_file(destination)
    return destination


def response_path(root: Path, target_repo: str, filename: str) -> Path:
    validate_kebab(target_repo, target_repo, "target_repo")
    validate_filename(filename, filename)
    project = root / target_repo
    responses = project / "responses"
    require_real_directory(project)
    require_real_directory(responses)
    destination = responses / filename
    require_real_file(destination)
    return destination


def record_json(record: Record) -> dict[str, Any]:
    return {
        "path": str(record.path),
        "response_path": str(record.response_path) if record.response_path else None,
        "sha256": record.digest,
        "title": record.title,
        "metadata": record.metadata,
        "request_set_members": list(record.members),
        "requires_resolution_verification": record.metadata["Request status"]
        == "resolved",
    }


def validate_set_consistency(root: Path, records: list[Record]) -> list[ContractError]:
    errors: list[ContractError] = []
    by_path = {
        f"{record.path.parent.parent.name}/{record.path.name}": record
        for record in records
    }
    by_set: dict[str, list[Record]] = {}
    for record in records:
        by_set.setdefault(record.metadata["Request set"], []).append(record)
    for request_set, set_records in by_set.items():
        declared = {record.members for record in set_records}
        if len(declared) != 1:
            for record in set_records:
                errors.append(
                    ContractError(
                        "inconsistent_request_set",
                        str(record.path),
                        "request set members disagree",
                    )
                )
            continue
        members = next(iter(declared))
        own_paths = {
            f"{record.path.parent.parent.name}/{record.path.name}"
            for record in set_records
        }
        selected_milestones = {
            record.metadata["Selected milestone"] for record in set_records
        }
        identities = [record.metadata["Request identity"] for record in set_records]
        if len(selected_milestones) != 1 or len(identities) != len(set(identities)):
            for record in set_records:
                errors.append(
                    ContractError(
                        "inconsistent_request_set",
                        str(record.path),
                        "request set identity metadata is inconsistent",
                    )
                )
        if not own_paths.issubset(set(members)):
            for record in set_records:
                errors.append(
                    ContractError(
                        "inconsistent_request_set",
                        str(record.path),
                        "same-set record is absent from its declared member list",
                    )
                )
        for member in members:
            member_record = by_path.get(member)
            if member_record is None:
                target, filename = member.split("/", 1)
                candidate = root / target / "requests" / filename
                if candidate.exists() and not candidate.is_symlink():
                    try:
                        member_record = read_project_record(root, target, filename)
                    except ContractError as exc:
                        errors.append(exc)
                        continue
                else:
                    errors.append(
                        ContractError(
                            "missing_set_member",
                            str(candidate),
                            "declared request set member is missing",
                        )
                    )
                    continue
            if member_record.metadata["Blocker consumer"] != CONSUMER:
                errors.append(
                    ContractError(
                        "inconsistent_request_set",
                        str(member_record.path),
                        "linked member has a different consumer",
                    )
                )
            if (
                member_record.metadata["Request set"] == request_set
                and member not in own_paths
            ):
                errors.append(
                    ContractError(
                        "inconsistent_request_set",
                        str(member_record.path),
                        "same-set member was not grouped consistently",
                    )
                )
    return errors


def validate_complete_record_set(
    root: Path,
    record: Record,
    validated: set[Path] | None = None,
    active: set[Path] | None = None,
) -> list[ContractError]:
    """Validate one existing set, loading every declared member from storage."""
    validated = set() if validated is None else validated
    active = set() if active is None else active
    if record.path in active:
        return [
            ContractError(
                "inconsistent_request_set",
                str(record.path),
                "linked request sets contain a cycle",
            )
        ]
    if record.path in validated:
        return []
    active.add(record.path)
    errors: list[ContractError] = []
    own_member = f"{record.path.parent.parent.name}/{record.path.name}"
    if own_member not in record.members:
        errors.append(
            ContractError(
                "inconsistent_request_set",
                str(record.path),
                "reused record is absent from its declared member list",
            )
        )
    loaded: list[Record] = []
    for member in record.members:
        target, filename = member.split("/", 1)
        try:
            loaded.append(read_project_record(root, target, filename))
        except ContractError as exc:
            errors.append(exc)
    if errors:
        active.remove(record.path)
        return errors
    if len({item.metadata["Request identity"] for item in loaded}) != len(loaded):
        errors.append(
            ContractError(
                "inconsistent_request_set",
                str(record.path),
                "request set contains duplicate identities",
            )
        )
    for item in loaded:
        if item.metadata["Blocker consumer"] != CONSUMER:
            errors.append(
                ContractError(
                    "inconsistent_request_set",
                    str(item.path),
                    "linked member has a different consumer",
                )
            )
        canonical_member = f"{item.path.parent.parent.name}/{item.path.name}"
        if canonical_member not in item.members:
            errors.append(
                ContractError(
                    "inconsistent_request_set",
                    str(item.path),
                    "request record is absent from its own declared member list",
                )
            )
        if item.metadata["Request set"] == record.metadata["Request set"]:
            if (
                item.members != record.members
                or item.metadata["Selected milestone"]
                != record.metadata["Selected milestone"]
            ):
                errors.append(
                    ContractError(
                        "inconsistent_request_set",
                        str(item.path),
                        "same-set request members or milestone disagree",
                    )
                )
        else:
            errors.extend(validate_complete_record_set(root, item, validated, active))
    active.remove(record.path)
    if not errors:
        validated.add(record.path)
    return errors


def exact_set_member_names(root: Path, record: Record) -> tuple[str, ...]:
    errors = validate_complete_record_set(root, record)
    if errors:
        raise errors[0]
    exact_members: list[str] = []
    for member in record.members:
        target, filename = member.split("/", 1)
        linked = read_project_record(root, target, filename)
        if linked.metadata["Request set"] == record.metadata["Request set"]:
            exact_members.append(member)
    return tuple(exact_members)


def inspect_records(root: Path, consumer: str) -> tuple[dict[str, Any], int]:
    root = canonical_projects_root(str(root))
    result = result_object("inspect")
    result["records"] = []
    errors: list[ContractError] = []
    records: list[Record] = []
    if consumer != CONSUMER:
        errors.append(
            ContractError(
                "invalid_consumer", consumer, "inspect supports only eino-agent"
            )
        )
    else:
        for project in sorted(root.iterdir(), key=lambda item: item.name):
            if project.name.startswith("."):
                continue
            requests = project / "requests"
            if not requests.exists() and not requests.is_symlink():
                continue
            try:
                scan_pin = pin_named_directory(root, project.name, "requests")
            except ContractError as exc:
                errors.append(exc)
                continue
            try:
                for filename in sorted(os.listdir(scan_pin.leaf_fd)):
                    candidate = requests / filename
                    if candidate.suffix != ".md":
                        continue
                    try:
                        data, _ = snapshot_at(scan_pin.leaf_fd, filename, candidate)
                        text = data.decode("utf-8")
                    except ContractError as exc:
                        errors.append(exc)
                        continue
                    except UnicodeDecodeError:
                        errors.append(
                            ContractError(
                                "invalid_utf8", str(candidate), "request must be UTF-8"
                            )
                        )
                        continue
                    blockers = extract_field_values(text, "Blocker consumer")
                    if consumer not in blockers:
                        continue
                    if blockers != [consumer]:
                        errors.append(
                            ContractError(
                                "invalid_metadata",
                                str(candidate),
                                "Blocker consumer must appear exactly once",
                            )
                        )
                        continue
                    if not KEBAB_RE.fullmatch(project.name):
                        errors.append(
                            ContractError(
                                "identity_mismatch",
                                str(candidate),
                                "relevant request is stored under a noncanonical project",
                            )
                        )
                        continue
                    try:
                        matched_response = find_matching_response(
                            root, project.name, filename
                        )
                        record = record_from_bytes(candidate, data, matched_response)
                        if record.path.parent.parent.name != IDENTITY_RE.fullmatch(
                            record.metadata["Request identity"]
                        ).group("target"):  # type: ignore[union-attr]
                            raise ContractError(
                                "identity_mismatch",
                                str(candidate),
                                "request identity target does not match storage project",
                            )
                        records.append(record)
                    except ContractError as exc:
                        errors.append(exc)
                if not scan_pin.matches_visible_chain():
                    errors.append(
                        ContractError(
                            "unsafe_path",
                            str(requests),
                            "request directory chain changed during inspection",
                        )
                    )
            finally:
                scan_pin.close()
        errors.extend(validate_set_consistency(root, records))
        validated_sets: set[Path] = set()
        for record in records:
            errors.extend(
                validate_complete_record_set(root, record, validated_sets, set())
            )
    result["records"] = [
        record_json(record)
        for record in sorted(records, key=lambda item: str(item.path))
    ]
    result["untouched"] = sorted(str(record.path) for record in records)
    add_errors(result, errors)
    logical_blocker = any(
        record.metadata["Request status"] in {"open", "resolved"} for record in records
    )
    if errors or logical_blocker:
        result["status"] = "blocked"
    return result, 2 if errors or logical_blocker else 0


def validate_authorization_shape(value: Any, path: str) -> str:
    auth = exact_object(
        value, {"confirmed_at", "destinations_digest"}, path, "invalid_authorization"
    )
    parse_rfc3339(auth["confirmed_at"], path)
    digest = auth["destinations_digest"]
    if not isinstance(digest, str) or not SHA_RE.fullmatch(digest):
        raise ContractError(
            "invalid_authorization",
            path,
            "authorization digest must be lowercase SHA-256 text",
        )
    return digest


def validate_authorization(
    supplied_digest: str, expected_digest: str, path: str
) -> None:
    if supplied_digest != expected_digest:
        raise ContractError(
            "authorization_mismatch",
            path,
            "authorization digest does not match canonical destinations",
        )


def load_manifest(path: Path) -> dict[str, Any]:
    require_real_file(path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractError(
            "invalid_manifest", str(path), "manifest must be one UTF-8 JSON object"
        ) from exc
    if not isinstance(value, dict):
        raise ContractError(
            "invalid_manifest", str(path), "manifest must be one JSON object"
        )
    return value


def validate_manifest_header(
    manifest: dict[str, Any], command: str, manifest_path: str
) -> None:
    if command == "create-set":
        keys = {"schema", "consumer", "request_set", "authorization", "members"}
        schema = CREATE_SCHEMA
    else:
        keys = {
            "schema",
            "consumer",
            "request_set",
            "authorization",
            "from_status",
            "to_status",
            "reason",
            "history_line",
            "members",
        }
        schema = TRANSITION_SCHEMA
    exact_object(manifest, keys, manifest_path)
    if manifest["schema"] != schema or manifest["consumer"] != CONSUMER:
        raise ContractError(
            "invalid_manifest",
            manifest_path,
            f"{command} manifest schema or consumer is invalid",
        )


def validate_target_scalars(member: dict[str, Any], path: str) -> None:
    checkout_value = member["target_checkout"]
    module = member["target_module"]
    commit = member["target_commit"]
    if (
        not isinstance(checkout_value, str)
        or has_control(checkout_value)
        or not Path(checkout_value).is_absolute()
    ):
        raise ContractError(
            "target_identity_mismatch", path, "target_checkout must be absolute"
        )
    checkout = Path(checkout_value)
    if checkout.name != member["target_repo"]:
        raise ContractError(
            "target_identity_mismatch",
            path,
            "target checkout does not match target_repo",
        )
    if (
        not isinstance(module, str)
        or has_control(module)
        or module.rstrip("/").removesuffix(".git").split("/")[-1]
        != member["target_repo"]
    ):
        raise ContractError(
            "target_identity_mismatch", path, "target module does not match target_repo"
        )
    if not isinstance(commit, str) or not COMMIT_RE.fullmatch(commit):
        raise ContractError(
            "target_identity_mismatch",
            path,
            "target commit must be a full hexadecimal commit",
        )


def validate_target_path(member: dict[str, Any], path: str) -> None:
    checkout = Path(member["target_checkout"])
    require_real_directory(checkout)
    if checkout.resolve(strict=True).name != member["target_repo"]:
        raise ContractError(
            "target_identity_mismatch",
            path,
            "target checkout does not match target_repo",
        )


def create_set(
    root: Path, manifest: dict[str, Any], manifest_path: str
) -> tuple[dict[str, Any], int]:
    result = result_object("create-set")
    prepared: list[tuple[dict[str, Any], Path, bytes]] = []
    try:
        exact_object(
            manifest,
            {"schema", "consumer", "request_set", "authorization", "members"},
            manifest_path,
        )
        if manifest["schema"] != CREATE_SCHEMA or manifest["consumer"] != CONSUMER:
            raise ContractError(
                "invalid_manifest",
                manifest_path,
                "create manifest schema or consumer is invalid",
            )
        root = canonical_projects_root(str(root))
        request_set = validate_set_id(manifest["request_set"], manifest_path)
        authorization_digest = validate_authorization_shape(
            manifest["authorization"], manifest_path
        )
        members = manifest["members"]
        if not isinstance(members, list) or not members:
            raise ContractError(
                "invalid_manifest", manifest_path, "members must be a nonempty array"
            )
        sort_keys: list[tuple[str, str]] = []
        identities: set[str] = set()
        destinations: list[Path] = []
        member_names: list[str] = []
        for index, raw in enumerate(members):
            member_path = f"{manifest_path}#members/{index}"
            if not isinstance(raw, dict):
                raise ContractError(
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
                raise ContractError(
                    "invalid_manifest", member_path, "mode must be create or reuse"
                )
            exact_object(raw, keys, member_path)
            target = validate_kebab(raw["target_repo"], member_path, "target_repo")
            filename = validate_filename(raw["filename"], member_path)
            identity = validate_identity(raw["request_identity"], target, member_path)
            if identity in identities:
                raise ContractError(
                    "duplicate_member", member_path, "request identities must be unique"
                )
            identities.add(identity)
            validate_target_scalars(raw, member_path)
            if mode == "reuse" and (
                not isinstance(raw["expected_sha256"], str)
                or not SHA_RE.fullmatch(raw["expected_sha256"])
            ):
                raise ContractError(
                    "invalid_digest", member_path, "expected_sha256 is invalid"
                )
            if not isinstance(raw["body"], str) or "\x00" in raw["body"]:
                raise ContractError(
                    "invalid_request", member_path, "body must be UTF-8-compatible text"
                )
            try:
                raw["body"].encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ContractError(
                    "invalid_request", member_path, "body must be UTF-8-compatible text"
                ) from exc
            sort_keys.append((target, filename))
            member_names.append(f"{target}/{filename}")
        if sort_keys != sorted(sort_keys) or len(set(sort_keys)) != len(sort_keys):
            raise ContractError(
                "member_order",
                manifest_path,
                "members must be unique and sorted by target_repo and filename",
            )
        for index, raw in enumerate(members):
            destinations.append(
                request_path(
                    root, raw["target_repo"], raw["filename"], create_dirs=True
                )
            )
        expected_members = tuple(sorted(member_names))
        digest = sha256_bytes(
            "\n".join(str(path) for path in destinations).encode("utf-8")
        )
        validate_authorization(authorization_digest, digest, manifest_path)
        selected_milestones: set[str] = set()
        for index, (raw, destination) in enumerate(zip(members, destinations)):
            member_path = f"{manifest_path}#members/{index}"
            body_bytes = raw["body"].encode("utf-8")
            title, metadata, body_members = parse_request_text(raw["body"], destination)
            del title
            validate_target_path(raw, member_path)
            if (
                metadata["Request status"] != "open"
                or metadata["Request identity"] != raw["request_identity"]
            ):
                raise ContractError(
                    "identity_mismatch",
                    str(destination),
                    "request body identity or status is invalid",
                )
            expected_target = f"{raw['target_module']} ({raw['target_checkout']})"
            if metadata["Target repo"] != expected_target:
                raise ContractError(
                    "target_identity_mismatch",
                    str(destination),
                    "request body target does not match preflight",
                )
            if (
                metadata["Pinned commit under evaluation"].lower()
                != raw["target_commit"].lower()
            ):
                raise ContractError(
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
                    raise ContractError(
                        "inconsistent_request_set",
                        str(destination),
                        "new request body does not contain the complete manifest set",
                    )
                if metadata["Date"] != request_set[:10]:
                    raise ContractError(
                        "inconsistent_request_set",
                        str(destination),
                        "request date and request-set date do not match",
                    )
                if destination.exists() or destination.is_symlink():
                    raise ContractError(
                        "create_conflict",
                        str(destination),
                        "destination already exists; use verified reuse or a new authorized slug",
                    )
            else:
                expected_sha = raw["expected_sha256"]
                existing_record = read_project_record(
                    root, raw["target_repo"], raw["filename"]
                )
                if sha256_bytes(body_bytes) != expected_sha:
                    raise ContractError(
                        "digest_mismatch",
                        str(destination),
                        "reused record does not match its expected snapshot",
                    )
                if existing_record.digest != expected_sha:
                    raise ContractError(
                        "digest_mismatch",
                        str(destination),
                        "reused record does not match its expected snapshot",
                    )
                if (
                    existing_record.metadata["Request identity"]
                    != raw["request_identity"]
                ):
                    raise ContractError(
                        "identity_mismatch",
                        str(destination),
                        "reused record identity does not match",
                    )
                if existing_record.metadata["Request set"] == request_set:
                    selected_milestones.add(existing_record.metadata["Selected milestone"])
                    if existing_record.members != expected_members:
                        raise ContractError(
                            "inconsistent_request_set",
                            str(destination),
                            "reused partial-set member does not match the manifest set",
                        )
                else:
                    set_errors = validate_complete_record_set(root, existing_record)
                    if set_errors:
                        raise set_errors[0]
            prepared.append((raw, destination, body_bytes))
        if len(selected_milestones) > 1:
            raise ContractError(
                "inconsistent_request_set",
                manifest_path,
                "request-set members name different selected milestones",
            )
    except ContractError as exc:
        result["status"] = "blocked"
        add_errors(result, [exc])
        code = 3 if exc.code == "create_conflict" else 2
        return result, code

    created: list[str] = []
    reused: list[str] = []
    try:
        for raw, destination, body in prepared:
            if raw["mode"] == "reuse":
                reused.append(str(destination))
                continue
            requests = destination.parent
            pinned = pin_project_directory(
                root, raw["target_repo"], "requests", create=True
            )
            directory_fd = pinned.leaf_fd
            try:
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
                if hasattr(os, "O_NOFOLLOW"):
                    flags |= os.O_NOFOLLOW
                try:
                    descriptor = os.open(
                        destination.name, flags, 0o600, dir_fd=directory_fd
                    )
                except FileExistsError as exc:
                    raise ContractError(
                        "create_conflict",
                        str(destination),
                        "destination appeared during exclusive creation",
                    ) from exc
                leaf_info = os.fstat(descriptor)
                leaf_identity = (leaf_info.st_dev, leaf_info.st_ino)
                leaf_created = True
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(body)
                        handle.flush()
                        os.fsync(handle.fileno())
                    if not pinned.matches_visible_chain():
                        raise ContractError(
                            "unsafe_path",
                            str(requests),
                            "requests directory identity changed during creation",
                        )
                    if (
                        read_bytes_at(directory_fd, destination.name, destination)
                        != body
                    ):
                        raise ContractError(
                            "post_write_mismatch",
                            str(destination),
                            "created record failed post-write verification",
                        )
                    os.fsync(directory_fd)
                except (ContractError, OSError) as exc:
                    unlink_owned_at(directory_fd, destination.name, leaf_identity)
                    leaf_created = False
                    if isinstance(exc, ContractError):
                        raise
                    raise ContractError(
                        "write_failure",
                        str(destination),
                        "exclusive request creation did not complete",
                    ) from exc
                if not leaf_created:
                    raise ContractError(
                        "write_failure",
                        str(destination),
                        "exclusive request creation did not complete",
                    )
            finally:
                pinned.close()
            created.append(str(destination))
    except ContractError as exc:
        result["created"] = sorted(created)
        result["reused"] = sorted(reused)
        result["untouched"] = sorted(
            str(path)
            for _, path, _ in prepared
            if str(path) not in created and str(path) not in reused
        )
        result["status"] = "partial" if created else "blocked"
        add_errors(result, [exc])
        return result, 3
    result["created"] = sorted(created)
    result["reused"] = sorted(reused)
    return result, 0


def snapshot_at(
    directory_fd: int, filename: str, display_path: Path
) -> tuple[bytes, tuple[int, int, int, int, int, int]]:
    flags = os.O_RDONLY | os.O_NONBLOCK
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(filename, flags, dir_fd=directory_fd)
    except OSError as exc:
        raise ContractError(
            "unsafe_path",
            str(display_path),
            "request could not be opened relative to its validated directory",
        ) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ContractError(
                "unsafe_path", str(display_path), "opened request is not a regular file"
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
        before_key = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
            stat.S_IMODE(before.st_mode),
        )
        after_key = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
            stat.S_IMODE(after.st_mode),
        )
        if before_key != after_key:
            raise ContractError(
                "digest_mismatch", str(display_path), "request changed while being read"
            )
        return b"".join(chunks), after_key
    finally:
        os.close(descriptor)


def record_from_bytes(path: Path, data: bytes, response: Path | None = None) -> Record:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ContractError("invalid_utf8", str(path), "request must be UTF-8") from exc
    title, metadata, members = parse_request_text(text, path)
    return Record(path, title, metadata, members, sha256_bytes(data), response)


def read_project_record(
    root: Path, target_repo: str, filename: str, match_response: bool = False
) -> Record:
    validate_kebab(target_repo, target_repo, "target_repo")
    validate_filename(filename, filename)
    destination = root / target_repo / "requests" / filename
    pin = pin_project_directory(root, target_repo, "requests")
    try:
        data, _ = snapshot_at(pin.leaf_fd, filename, destination)
        if not pin.matches_visible_chain():
            raise ContractError(
                "unsafe_path",
                str(destination.parent),
                "request directory chain changed while reading a record",
            )
    finally:
        pin.close()
    matched_response = (
        find_matching_response(root, target_repo, filename) if match_response else None
    )
    return record_from_bytes(destination, data, matched_response)


def find_matching_response(root: Path, target_repo: str, filename: str) -> Path | None:
    response = root / target_repo / "responses" / filename
    responses = response.parent
    if not responses.exists() and not responses.is_symlink():
        return None
    response_pin = pin_project_directory(root, target_repo, "responses")
    try:
        try:
            info = os.stat(filename, dir_fd=response_pin.leaf_fd, follow_symlinks=False)
        except FileNotFoundError:
            info = None
        if info is not None:
            if not stat.S_ISREG(info.st_mode):
                raise ContractError(
                    "unsafe_path",
                    str(response),
                    "matching response must be a regular non-symlink file",
                )
            read_bytes_at(response_pin.leaf_fd, filename, response)
        if not response_pin.matches_visible_chain():
            raise ContractError(
                "unsafe_path",
                str(responses),
                "response directory chain changed while reading a record",
            )
        return response if info is not None else None
    finally:
        response_pin.close()


def transition_text(text: str, to_status: str, history_line: str, path: Path) -> str:
    status_line = re.compile(
        r"^- \*\*Request status:\*\*[ \t]+(?:`open`|open)[ \t]*$", re.MULTILINE
    )
    matches = list(status_line.finditer(text))
    if len(matches) != 1:
        raise ContractError(
            "status_conflict",
            str(path),
            "open status line does not match the canonical form",
        )
    updated = status_line.sub(f"- **Request status:** `{to_status}`", text, count=1)
    if history_line in updated.splitlines():
        raise ContractError(
            "history_conflict", str(path), "history transition already exists"
        )
    if not updated.endswith("\n"):
        updated += "\n"
    return updated + history_line + "\n"


def transition_set(
    root: Path, manifest: dict[str, Any], manifest_path: str
) -> tuple[dict[str, Any], int]:
    result = result_object("transition-set")
    prepared: list[dict[str, Any]] = []
    locked: list[dict[str, Any]] = []
    pins: list[PinnedDirectory] = []
    try:
        exact_object(
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
        if manifest["schema"] != TRANSITION_SCHEMA or manifest["consumer"] != CONSUMER:
            raise ContractError(
                "invalid_manifest",
                manifest_path,
                "transition manifest schema or consumer is invalid",
            )
        root = canonical_projects_root(str(root))
        request_set = validate_set_id(manifest["request_set"], manifest_path)
        authorization_digest = validate_authorization_shape(
            manifest["authorization"], manifest_path
        )
        if (
            manifest["from_status"] != "open"
            or manifest["to_status"] not in TRANSITION_STATUSES
        ):
            raise ContractError(
                "invalid_transition",
                manifest_path,
                "transition must move open to a recognized terminal status",
            )
        reason = manifest["reason"]
        if not isinstance(reason, str) or not reason or has_control(reason):
            raise ContractError(
                "invalid_transition",
                manifest_path,
                "reason must be nonempty single-line text",
            )
        history = manifest["history_line"]
        match = HISTORY_RE.fullmatch(history) if isinstance(history, str) else None
        if (
            not match
            or match.group("status") != manifest["to_status"]
            or match.group("reason") != reason
        ):
            raise ContractError(
                "invalid_transition",
                manifest_path,
                "history_line must canonically match status and reason",
            )
        members = manifest["members"]
        if not isinstance(members, list) or not members:
            raise ContractError(
                "invalid_manifest", manifest_path, "members must be a nonempty array"
            )
        destinations: list[Path] = []
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
                if manifest["to_status"] == "resolved"
                else set()
            )
            exact_object(raw, required_keys, member_path)
            target = validate_kebab(raw["target_repo"], member_path, "target_repo")
            filename = validate_filename(raw["filename"], member_path)
            identity = validate_identity(raw["request_identity"], target, member_path)
            expected_sha = raw["expected_sha256"]
            if not isinstance(expected_sha, str) or not SHA_RE.fullmatch(expected_sha):
                raise ContractError(
                    "invalid_digest", member_path, "expected_sha256 is invalid"
                )
            if identity in identities:
                raise ContractError(
                    "duplicate_member", member_path, "request identities must be unique"
                )
            identities.add(identity)
            keys_seen.append((target, filename))
            expected_member_names.append(f"{target}/{filename}")
            if manifest["to_status"] == "resolved":
                if raw["response_filename"] != filename:
                    raise ContractError(
                        "response_mismatch",
                        member_path,
                        "resolved response must have the same filename",
                    )
                if not COMMIT_RE.fullmatch(raw["verified_target_commit"]):
                    raise ContractError(
                        "invalid_commit",
                        member_path,
                        "verified target commit must be full hexadecimal text",
                    )
                if (
                    not isinstance(raw["verified_pin"], str)
                    or not raw["verified_pin"]
                    or has_control(raw["verified_pin"])
                ):
                    raise ContractError(
                        "invalid_pin",
                        member_path,
                        "verified pin must be nonempty single-line text",
                    )
        if keys_seen != sorted(keys_seen) or len(set(keys_seen)) != len(keys_seen):
            raise ContractError(
                "member_order", manifest_path, "members must be unique and sorted"
            )
        for raw in members:
            destinations.append(request_path(root, raw["target_repo"], raw["filename"]))
        digest_lines = [f"{path} -> {manifest['to_status']}" for path in destinations]
        validate_authorization(
            authorization_digest,
            sha256_bytes("\n".join(digest_lines).encode("utf-8")),
            manifest_path,
        )
        for raw, destination in zip(members, destinations):
            target = raw["target_repo"]
            filename = raw["filename"]
            identity = raw["request_identity"]
            expected_sha = raw["expected_sha256"]
            pin = pin_project_directory(root, target, "requests")
            pins.append(pin)
            directory_fd = pin.leaf_fd
            data, stat_key = snapshot_at(directory_fd, destination.name, destination)
            if not pin.matches_visible_chain():
                raise ContractError(
                    "unsafe_path",
                    str(destination.parent),
                    "requests directory identity changed during transition preflight",
                )
            record = record_from_bytes(destination, data)
            if record.digest != expected_sha:
                raise ContractError(
                    "digest_mismatch",
                    str(destination),
                    "request differs from the authorized snapshot",
                )
            if (
                record.metadata["Request identity"] != identity
                or record.metadata["Request set"] != request_set
                or record.metadata["Request status"]
                not in {"open", manifest["to_status"]}
            ):
                raise ContractError(
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
                raise ContractError(
                    "history_conflict",
                    str(destination),
                    "terminal member does not contain the authorized transition history",
                )
            if manifest["to_status"] == "resolved":
                response = response_path(root, target, raw["response_filename"])
                response_pin = pin_project_directory(root, target, "responses")
                try:
                    read_bytes_at(response_pin.leaf_fd, response.name, response)
                    if not response_pin.matches_visible_chain():
                        raise ContractError(
                            "unsafe_path",
                            str(response.parent),
                            "responses directory identity changed during verification",
                        )
                finally:
                    response_pin.close()
            else:
                response = None
            prepared.append(
                {
                    "raw": raw,
                    "path": destination,
                    "record": record,
                    "data": data,
                    "stat": stat_key,
                    "response": response,
                    "directory_fd": directory_fd,
                    "pin": pin,
                    "already_transitioned": already_transitioned,
                }
            )
        expected_members = tuple(expected_member_names)
        selected_milestones = {
            item["record"].metadata["Selected milestone"] for item in prepared
        }
        if len(selected_milestones) != 1:
            raise ContractError(
                "inconsistent_request_set",
                manifest_path,
                "transition members name different selected milestones",
            )
        for item in prepared:
            if exact_set_member_names(root, item["record"]) != expected_members:
                raise ContractError(
                    "inconsistent_request_set",
                    str(item["path"]),
                    "exact-set members do not match transition manifest",
                )
    except ContractError as exc:
        for pin in pins:
            pin.close()
        result["status"] = "blocked"
        add_errors(result, [exc])
        code = (
            4
            if exc.code in {"digest_mismatch", "status_conflict", "lock_conflict"}
            else 2
        )
        return result, code

    transitioned: list[str] = []
    already_untouched: list[str] = []
    try:
        for item in prepared:
            if not item["pin"].matches_visible_chain():
                raise ContractError(
                    "unsafe_path",
                    str(item["path"].parent),
                    "requests directory identity changed before lock acquisition",
                )
            lock_name = item["path"].name + ".lock"
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            try:
                descriptor = os.open(
                    lock_name, flags, 0o600, dir_fd=item["directory_fd"]
                )
            except FileExistsError as exc:
                raise ContractError(
                    "lock_conflict",
                    str(item["path"].with_name(lock_name)),
                    "another cooperating writer holds the transition lock",
                ) from exc
            lock_info = os.fstat(descriptor)
            os.close(descriptor)
            item["lock_name"] = lock_name
            item["lock_identity"] = (lock_info.st_dev, lock_info.st_ino)
            locked.append(item)
            if not item["pin"].matches_visible_chain():
                raise ContractError(
                    "unsafe_path",
                    str(item["path"].parent),
                    "requests directory identity changed during lock acquisition",
                )
        for item in prepared:
            current_data, current_stat = snapshot_at(
                item["directory_fd"], item["path"].name, item["path"]
            )
            if (
                current_data != item["data"]
                or current_stat != item["stat"]
                or sha256_bytes(current_data) != item["raw"]["expected_sha256"]
            ):
                raise ContractError(
                    "digest_mismatch",
                    str(item["path"]),
                    "request changed after transition preflight",
                )
            if item["already_transitioned"]:
                already_untouched.append(str(item["path"]))
                continue
            updated = transition_text(
                current_data.decode("utf-8"),
                manifest["to_status"],
                manifest["history_line"],
                item["path"],
            )
            updated_bytes = updated.encode("utf-8")
            temp_name = (
                f".{item['path'].name}.{os.getpid()}."
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
                    item["stat"][5],
                    dir_fd=item["directory_fd"],
                )
                temp_created = True
                temp_info = os.fstat(descriptor)
                temp_identity = (temp_info.st_dev, temp_info.st_ino)
                os.fchmod(descriptor, item["stat"][5])
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(updated_bytes)
                    handle.flush()
                    os.fsync(handle.fileno())
                final_data, final_stat = snapshot_at(
                    item["directory_fd"], item["path"].name, item["path"]
                )
                if final_data != item["data"] or final_stat != item["stat"]:
                    raise ContractError(
                        "digest_mismatch",
                        str(item["path"]),
                        "request changed before atomic replacement",
                    )
                if not item["pin"].matches_visible_chain():
                    raise ContractError(
                        "unsafe_path",
                        str(item["path"].parent),
                        "requests directory identity changed before replacement",
                    )
                os.replace(
                    temp_name,
                    item["path"].name,
                    src_dir_fd=item["directory_fd"],
                    dst_dir_fd=item["directory_fd"],
                )
                temp_created = False
                transitioned.append(str(item["path"]))
            except OSError as exc:
                raise ContractError(
                    "write_failure",
                    str(item["path"]),
                    "atomic request transition did not complete",
                ) from exc
            finally:
                if temp_created:
                    unlink_owned_at(item["directory_fd"], temp_name, temp_identity)
            verified_data, _ = snapshot_at(
                item["directory_fd"], item["path"].name, item["path"]
            )
            if verified_data != updated_bytes:
                raise ContractError(
                    "post_write_mismatch",
                    str(item["path"]),
                    "transition output changed before exact post-write verification",
                )
            verified = record_from_bytes(item["path"], verified_data)
            if (
                verified.metadata["Request status"] != manifest["to_status"]
                or manifest["history_line"]
                not in verified_data.decode("utf-8").splitlines()
            ):
                raise ContractError(
                    "post_write_mismatch",
                    str(item["path"]),
                    "transition failed post-write verification",
                )
            if not item["pin"].matches_visible_chain():
                raise ContractError(
                    "unsafe_path",
                    str(item["path"].parent),
                    "requests directory identity changed after replacement",
                )
            os.fsync(item["directory_fd"])
    except ContractError as exc:
        result["transitioned"] = sorted(transitioned)
        result["untouched"] = sorted(
            set(already_untouched)
            | {
                str(item["path"])
                for item in prepared
                if str(item["path"]) not in transitioned
            }
        )
        result["status"] = "partial" if transitioned else "blocked"
        add_errors(result, [exc])
        return result, 4
    finally:
        for item in reversed(locked):
            unlink_owned_at(
                item["directory_fd"], item["lock_name"], item["lock_identity"]
            )
        for pin in pins:
            pin.close()
    result["transitioned"] = sorted(transitioned)
    result["untouched"] = sorted(already_untouched)
    return result, 0


def build_parser() -> argparse.ArgumentParser:
    parser = ContractArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    inspect_parser = commands.add_parser("inspect")
    inspect_parser.add_argument("--projects-root", required=True)
    inspect_parser.add_argument("--consumer", required=True)
    for name in ("create-set", "transition-set"):
        command = commands.add_parser(name)
        command.add_argument("--projects-root", required=True)
        command.add_argument("--manifest", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    raw_argv = sys.argv[1:] if argv is None else argv
    operation = raw_argv[0] if raw_argv else "unknown"
    try:
        args = build_parser().parse_args(raw_argv)
        operation = args.command
        if args.command == "inspect":
            root = canonical_projects_root(args.projects_root)
            result, code = inspect_records(root, args.consumer)
        else:
            manifest_path = Path(args.manifest)
            if not manifest_path.is_absolute():
                raise ContractError(
                    "unsafe_path", str(manifest_path), "manifest path must be absolute"
                )
            manifest = load_manifest(manifest_path)
            validate_manifest_header(manifest, args.command, str(manifest_path))
            root = canonical_projects_root(args.projects_root)
            if args.command == "create-set":
                result, code = create_set(root, manifest, str(manifest_path))
            else:
                result, code = transition_set(root, manifest, str(manifest_path))
    except ContractError as exc:
        result = result_object(operation)
        result["status"] = "blocked"
        add_errors(result, [exc])
        code = 2
    except Exception:  # noqa: BLE001 - the versioned result contract maps unexpected failures to exit 1
        print("request_records.py: unexpected internal failure", file=sys.stderr)
        result = result_object(operation)
        result["status"] = "blocked"
        add_errors(
            result, [ContractError("internal_error", "", "unexpected internal failure")]
        )
        code = 1
    json.dump(result, sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
