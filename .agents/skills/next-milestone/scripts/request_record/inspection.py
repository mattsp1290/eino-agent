"""Read-only request inspection and linked-set consistency."""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Any

from . import contract
from . import filesystem as fs


def record_json(record: contract.Record) -> dict[str, Any]:
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


def validate_set_consistency(
    root: Path, records: list[contract.Record]
) -> list[contract.ContractError]:
    errors: list[contract.ContractError] = []
    by_path = {
        f"{record.path.parent.parent.name}/{record.path.name}": record
        for record in records
    }
    by_set: dict[str, list[contract.Record]] = {}
    for record in records:
        by_set.setdefault(record.metadata["Request set"], []).append(record)
    for request_set, set_records in by_set.items():
        declared = {record.members for record in set_records}
        if len(declared) != 1:
            for record in set_records:
                errors.append(
                    contract.ContractError(
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
                    contract.ContractError(
                        "inconsistent_request_set",
                        str(record.path),
                        "request set identity metadata is inconsistent",
                    )
                )
        if not own_paths.issubset(set(members)):
            for record in set_records:
                errors.append(
                    contract.ContractError(
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
                    except contract.ContractError as exc:
                        errors.append(exc)
                        continue
                else:
                    errors.append(
                        contract.ContractError(
                            "missing_set_member",
                            str(candidate),
                            "declared request set member is missing",
                        )
                    )
                    continue
            if member_record.metadata["Blocker consumer"] != contract.CONSUMER:
                errors.append(
                    contract.ContractError(
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
                    contract.ContractError(
                        "inconsistent_request_set",
                        str(member_record.path),
                        "same-set member was not grouped consistently",
                    )
                )
    return errors


def validate_complete_record_set(
    root: Path,
    record: contract.Record,
    validated: set[Path] | None = None,
    active: set[Path] | None = None,
) -> list[contract.ContractError]:
    """Validate one existing set, loading every declared member from storage."""
    validated = set() if validated is None else validated
    active = set() if active is None else active
    if record.path in active:
        return [
            contract.ContractError(
                "inconsistent_request_set",
                str(record.path),
                "linked request sets contain a cycle",
            )
        ]
    if record.path in validated:
        return []
    active.add(record.path)
    errors: list[contract.ContractError] = []
    own_member = f"{record.path.parent.parent.name}/{record.path.name}"
    if own_member not in record.members:
        errors.append(
            contract.ContractError(
                "inconsistent_request_set",
                str(record.path),
                "reused record is absent from its declared member list",
            )
        )
    loaded: list[contract.Record] = []
    for member in record.members:
        target, filename = member.split("/", 1)
        try:
            loaded.append(read_project_record(root, target, filename))
        except contract.ContractError as exc:
            errors.append(exc)
    if errors:
        active.remove(record.path)
        return errors
    if len({item.metadata["Request identity"] for item in loaded}) != len(loaded):
        errors.append(
            contract.ContractError(
                "inconsistent_request_set",
                str(record.path),
                "request set contains duplicate identities",
            )
        )
    for item in loaded:
        if item.metadata["Blocker consumer"] != contract.CONSUMER:
            errors.append(
                contract.ContractError(
                    "inconsistent_request_set",
                    str(item.path),
                    "linked member has a different consumer",
                )
            )
        canonical_member = f"{item.path.parent.parent.name}/{item.path.name}"
        if canonical_member not in item.members:
            errors.append(
                contract.ContractError(
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
                    contract.ContractError(
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


def exact_set_member_names(root: Path, record: contract.Record) -> tuple[str, ...]:
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
    root = fs.canonical_projects_root(str(root))
    result = contract.result_object("inspect")
    result["records"] = []
    errors: list[contract.ContractError] = []
    records: list[contract.Record] = []
    if consumer != contract.CONSUMER:
        errors.append(
            contract.ContractError(
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
                scan_pin = fs.pin_named_directory(root, project.name, "requests")
            except contract.ContractError as exc:
                errors.append(exc)
                continue
            try:
                for filename in sorted(os.listdir(scan_pin.leaf_fd)):
                    candidate = requests / filename
                    if candidate.suffix != ".md":
                        continue
                    try:
                        data, _ = fs.snapshot_at(scan_pin.leaf_fd, filename, candidate)
                        text = data.decode("utf-8")
                    except contract.ContractError as exc:
                        errors.append(exc)
                        continue
                    except UnicodeDecodeError:
                        errors.append(
                            contract.ContractError(
                                "invalid_utf8", str(candidate), "request must be UTF-8"
                            )
                        )
                        continue
                    blockers = contract.extract_field_values(text, "Blocker consumer")
                    if consumer not in blockers:
                        continue
                    if blockers != [consumer]:
                        errors.append(
                            contract.ContractError(
                                "invalid_metadata",
                                str(candidate),
                                "Blocker consumer must appear exactly once",
                            )
                        )
                        continue
                    if not contract.KEBAB_RE.fullmatch(project.name):
                        errors.append(
                            contract.ContractError(
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
                        record = contract.record_from_bytes(
                            candidate, data, matched_response
                        )
                        if (
                            record.path.parent.parent.name
                            != contract.IDENTITY_RE.fullmatch(
                                record.metadata["Request identity"]
                            ).group("target")
                        ):  # type: ignore[union-attr]
                            raise contract.ContractError(
                                "identity_mismatch",
                                str(candidate),
                                "request identity target does not match storage project",
                            )
                        records.append(record)
                    except contract.ContractError as exc:
                        errors.append(exc)
                if not scan_pin.matches_visible_chain():
                    errors.append(
                        contract.ContractError(
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
    contract.add_errors(result, errors)
    logical_blocker = any(
        record.metadata["Request status"] in {"open", "resolved"} for record in records
    )
    if errors or logical_blocker:
        result["status"] = "blocked"
    return result, 2 if errors or logical_blocker else 0


def read_project_record(
    root: Path, target_repo: str, filename: str, match_response: bool = False
) -> contract.Record:
    contract.validate_kebab(target_repo, target_repo, "target_repo")
    contract.validate_filename(filename, filename)
    destination = root / target_repo / "requests" / filename
    pin = fs.pin_project_directory(root, target_repo, "requests")
    try:
        data, _ = fs.snapshot_at(pin.leaf_fd, filename, destination)
        if not pin.matches_visible_chain():
            raise contract.ContractError(
                "unsafe_path",
                str(destination.parent),
                "request directory chain changed while reading a record",
            )
    finally:
        pin.close()
    matched_response = (
        find_matching_response(root, target_repo, filename) if match_response else None
    )
    return contract.record_from_bytes(destination, data, matched_response)


def find_matching_response(root: Path, target_repo: str, filename: str) -> Path | None:
    response = root / target_repo / "responses" / filename
    responses = response.parent
    if not responses.exists() and not responses.is_symlink():
        return None
    response_pin = fs.pin_project_directory(root, target_repo, "responses")
    try:
        try:
            info = os.stat(filename, dir_fd=response_pin.leaf_fd, follow_symlinks=False)
        except FileNotFoundError:
            info = None
        if info is not None:
            if not stat.S_ISREG(info.st_mode):
                raise contract.ContractError(
                    "unsafe_path",
                    str(response),
                    "matching response must be a regular non-symlink file",
                )
            fs.snapshot_at(response_pin.leaf_fd, filename, response)
        if not response_pin.matches_visible_chain():
            raise contract.ContractError(
                "unsafe_path",
                str(responses),
                "response directory chain changed while reading a record",
            )
        return response if info is not None else None
    finally:
        response_pin.close()
