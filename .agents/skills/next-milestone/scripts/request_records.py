#!/usr/bin/env python3
"""Safely inspect and mutate next-milestone cross-repository request records."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from request_record import contract, creation, inspection, transition
from request_record import filesystem as fs


class ContractArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise contract.ContractError("invalid_arguments", "arguments", message)


def load_manifest(path: Path) -> dict[str, Any]:
    fs.require_real_file(path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise contract.ContractError(
            "invalid_manifest", str(path), "manifest must be one UTF-8 JSON object"
        ) from exc
    if not isinstance(value, dict):
        raise contract.ContractError(
            "invalid_manifest", str(path), "manifest must be one JSON object"
        )
    return value


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
            root = fs.canonical_projects_root(args.projects_root)
            result, code = inspection.inspect_records(root, args.consumer)
        else:
            manifest_path = Path(args.manifest)
            if not manifest_path.is_absolute():
                raise contract.ContractError(
                    "unsafe_path", str(manifest_path), "manifest path must be absolute"
                )
            manifest = load_manifest(manifest_path)
            contract.validate_manifest_header(
                manifest, args.command, str(manifest_path)
            )
            root = fs.canonical_projects_root(args.projects_root)
            if args.command == "create-set":
                result, code = creation.create_set(root, manifest, str(manifest_path))
            else:
                result, code = transition.transition_set(
                    root, manifest, str(manifest_path)
                )
    except contract.ContractError as exc:
        result = contract.result_object(operation)
        result["status"] = "blocked"
        contract.add_errors(result, [exc])
        code = 2
    except Exception:  # noqa: BLE001 - the versioned result contract maps unexpected failures to exit 1
        print("request_records.py: unexpected internal failure", file=sys.stderr)
        result = contract.result_object(operation)
        result["status"] = "blocked"
        contract.add_errors(
            result,
            [
                contract.ContractError(
                    "internal_error", "", "unexpected internal failure"
                )
            ],
        )
        code = 1
    json.dump(result, sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
