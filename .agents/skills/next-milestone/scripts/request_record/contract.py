"""Wire schemas, record parsing and pure contract validation."""

from __future__ import annotations

import datetime as dt
import hashlib
import re
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


class ContractError(Exception):
    def __init__(self, code: str, path: str, message: str):
        super().__init__(message)
        self.code = code
        self.path = path
        self.message = message


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


def record_from_bytes(path: Path, data: bytes, response: Path | None = None) -> Record:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ContractError("invalid_utf8", str(path), "request must be UTF-8") from exc
    title, metadata, members = parse_request_text(text, path)
    return Record(path, title, metadata, members, sha256_bytes(data), response)
