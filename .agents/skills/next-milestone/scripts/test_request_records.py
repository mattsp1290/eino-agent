from __future__ import annotations

import contextlib
import hashlib
import io
import json
from unittest import mock

import request_records as cli
from request_record import contract, creation, transition
from request_record import filesystem as fs
from request_test_support import DATE, RequestFixture, digest


class FormatTests(RequestFixture):
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
                result, code = creation.create_set(self.projects, manifest, "fixture")
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
        result, code = creation.create_set(
            self.projects, self.create_manifest(request_set, [member]), "fixture"
        )
        self.assertEqual(code, 2)
        self.assertEqual(result["errors"][0]["code"], "target_identity_mismatch")

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
                result, code = creation.create_set(
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

    def test_invalid_unicode_and_fixed_validation_precedence(self) -> None:
        request_set = f"{DATE}-unicode"
        filename = f"{DATE}-unicode.md"
        body = self.body(
            "eino-tools", filename, request_set, [f"eino-tools/{filename}"]
        )
        member = self.member("eino-tools", filename, body + "\ud800")
        result, code = creation.create_set(
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
        result, code = creation.create_set(self.projects, manifest, "lowercase-rfc3339")
        self.assertEqual((code, result["status"]), (0, "complete"))
        self.assertEqual(
            contract.parse_rfc3339("2016-12-31T23:59:60Z", "fixture"),
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
        wrong_version["schema"] = contract.TRANSITION_SCHEMA.replace("/v1", "/v2")
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
                outcome, outcome_code = transition.transition_set(
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
        result, code = transition.transition_set(self.projects, invalid, "fixture")
        self.assertEqual(code, 2)
        self.assertEqual(result["schema"], contract.RESULT_SCHEMA)
        self.assertEqual(result["errors"][0]["code"], "invalid_manifest")

        no_response = self.transition_manifest(
            request_set, [transition_member], to_status="resolved"
        )
        result, code = transition.transition_set(self.projects, no_response, "fixture")
        self.assertEqual(code, 2)
        self.assertEqual(result["errors"][0]["code"], "unsafe_path")

        buffer = io.StringIO()
        with (
            mock.patch.object(
                fs, "canonical_projects_root", side_effect=RuntimeError("boom")
            ),
            contextlib.redirect_stdout(buffer),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            internal_code = cli.main(
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

    def test_create_inspect_and_resolved_transition(self) -> None:
        request_set, filename, member, destination = self.create_one()
        code, inspected, stderr = self.run_cli(
            "inspect", "--projects-root", str(self.projects), "--consumer", "eino-agent"
        )
        self.assertEqual((code, stderr), (2, ""))
        self.assertEqual(inspected["schema"], contract.RESULT_SCHEMA)
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
        result, transition_code = transition.transition_set(
            self.projects, manifest, "fixture"
        )
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
