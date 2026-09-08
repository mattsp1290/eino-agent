from __future__ import annotations

import hashlib

from request_record import creation, inspection, transition
from request_test_support import DATE, RequestFixture


class GraphTests(RequestFixture):
    def test_inspect_rejects_malformed_duplicate_status_unknown_status_and_missing_member(
        self,
    ) -> None:
        target_dir = self.projects / "eino-tools" / "requests"
        target_dir.mkdir(parents=True)
        request_set = f"{DATE}-broken"
        filename = f"{DATE}-broken.md"
        complete = self.body(
            "eino-tools",
            filename,
            request_set,
            [f"eino-tools/{filename}", f"eino-tools/{DATE}-missing.md"],
        )
        variants = {
            "missing": complete.replace("- **Request status:** `open`\n", ""),
            "duplicate": complete.replace(
                "- **Request status:** `open`\n",
                "- **Request status:** `open`\n- **Request status:** `open`\n",
            ),
            "unknown": complete.replace(
                "- **Request status:** `open`", "- **Request status:** `waiting`"
            ),
            "incomplete": complete,
        }
        for name, text in variants.items():
            with self.subTest(name=name):
                for existing in target_dir.glob("*.md"):
                    existing.unlink()
                (target_dir / filename).write_text(text, encoding="utf-8")
                code, result, _ = self.run_cli(
                    "inspect",
                    "--projects-root",
                    str(self.projects),
                    "--consumer",
                    "eino-agent",
                )
                self.assertEqual(code, 2)
                self.assertEqual(result["status"], "blocked")
                self.assertTrue(result["errors"])

    def test_inspect_matches_same_name_response(self) -> None:
        _, filename, _, _ = self.create_one()
        (self.projects / "legacyProject" / "requests").mkdir(parents=True)
        (self.projects / "legacyProject" / "requests" / "legacy.md").write_text(
            "legacy noncanonical project data", encoding="utf-8"
        )
        response_dir = self.projects / "eino-tools" / "responses"
        response_dir.mkdir()
        response = response_dir / filename
        response.write_text("# Response\n", encoding="utf-8")
        result, code = inspection.inspect_records(self.projects, "eino-agent")
        self.assertEqual(code, 2)
        self.assertEqual(result["records"][0]["response_path"], str(response.resolve()))

    def test_inspect_reports_relevant_record_in_noncanonical_project(self) -> None:
        requests = self.projects / "legacyProject" / "requests"
        requests.mkdir(parents=True)
        filename = f"{DATE}-legacy.md"
        body = self.body(
            "eino-tools",
            filename,
            f"{DATE}-legacy",
            [f"eino-tools/{filename}"],
            contract="legacy",
        )
        (requests / filename).write_text(body, encoding="utf-8")
        result, code = inspection.inspect_records(self.projects, "eino-agent")
        self.assertEqual((code, result["status"]), (2, "blocked"))
        self.assertEqual(result["errors"][0]["code"], "identity_mismatch")

    def test_reuse_rejects_self_omitting_cross_set_declaration(self) -> None:
        requests = self.projects / "eino-tools" / "requests"
        requests.mkdir(parents=True)
        a_name = f"{DATE}-set-a.md"
        b_name = f"{DATE}-set-b.md"
        b_body = self.body(
            "eino-tools",
            b_name,
            f"{DATE}-set-b",
            [f"eino-tools/{b_name}"],
            contract="set-b",
        )
        (requests / b_name).write_text(b_body, encoding="utf-8")
        a_body = self.body(
            "eino-tools",
            a_name,
            f"{DATE}-set-a",
            [f"eino-tools/{b_name}"],
            contract="set-a",
        )
        destination = requests / a_name
        destination.write_text(a_body, encoding="utf-8")
        member = self.member(
            "eino-tools",
            a_name,
            a_body,
            mode="reuse",
            contract="set-a",
            expected_sha256=hashlib.sha256(a_body.encode()).hexdigest(),
        )
        result, code = creation.create_set(
            self.projects,
            self.create_manifest(f"{DATE}-later", [member]),
            "fixture",
        )
        self.assertEqual(code, 2)
        self.assertEqual(result["errors"][0]["code"], "inconsistent_request_set")

    def test_mixed_new_and_linked_reuse_set_lifecycle(self) -> None:
        old_set, old_filename, old_member, old_path = self.create_one()
        self.assertNotEqual(old_set, f"{DATE}-mixed")
        old_member["mode"] = "reuse"
        old_member["expected_sha256"] = hashlib.sha256(
            old_path.read_bytes()
        ).hexdigest()

        new_set = f"{DATE}-mixed"
        new_filename = f"{DATE}-provider-link.md"
        names = [
            f"eino-providers/{new_filename}",
            f"eino-tools/{old_filename}",
        ]
        new_body = self.body(
            "eino-providers",
            new_filename,
            new_set,
            names,
            contract="provider-link",
        )
        new_member = self.member(
            "eino-providers",
            new_filename,
            new_body,
            contract="provider-link",
        )
        mixed_manifest = self.create_manifest(new_set, [new_member, old_member])
        created, created_code = creation.create_set(
            self.projects, mixed_manifest, "mixed-create"
        )
        self.assertEqual((created_code, created["status"]), (0, "complete"))
        self.assertEqual(len(created["created"]), 1)
        self.assertEqual(len(created["reused"]), 1)

        inspected, inspect_code = inspection.inspect_records(
            self.projects, "eino-agent"
        )
        self.assertEqual((inspect_code, inspected["status"]), (2, "blocked"))
        self.assertEqual(inspected["errors"], [])

        new_path = self.destination("eino-providers", new_filename)
        later_reuse = dict(new_member)
        later_reuse["mode"] = "reuse"
        later_reuse["expected_sha256"] = hashlib.sha256(
            new_path.read_bytes()
        ).hexdigest()
        reused, reused_code = creation.create_set(
            self.projects,
            self.create_manifest(f"{DATE}-later", [later_reuse]),
            "later-reuse",
        )
        self.assertEqual((reused_code, reused["status"]), (0, "complete"))

        transition_member = dict(new_member)
        transition_member["expected_sha256"] = hashlib.sha256(
            new_path.read_bytes()
        ).hexdigest()
        transitioned, transition_code = transition.transition_set(
            self.projects,
            self.transition_manifest(new_set, [transition_member]),
            "mixed-transition",
        )
        self.assertEqual((transition_code, transitioned["status"]), (0, "complete"))
        self.assertIn("`withdrawn`", new_path.read_text(encoding="utf-8"))
        self.assertIn("`open`", old_path.read_text(encoding="utf-8"))

    def test_cross_set_cycle_is_rejected_by_inspect_and_reuse(self) -> None:
        requests = self.projects / "eino-tools" / "requests"
        requests.mkdir(parents=True)
        a_name = f"{DATE}-cycle-a.md"
        b_name = f"{DATE}-cycle-b.md"
        members = [f"eino-tools/{a_name}", f"eino-tools/{b_name}"]
        a_body = self.body(
            "eino-tools",
            a_name,
            f"{DATE}-cycle-a",
            members,
            contract="cycle-a",
        )
        b_body = self.body(
            "eino-tools",
            b_name,
            f"{DATE}-cycle-b",
            members,
            contract="cycle-b",
        )
        (requests / a_name).write_text(a_body, encoding="utf-8")
        (requests / b_name).write_text(b_body, encoding="utf-8")

        inspected, inspect_code = inspection.inspect_records(
            self.projects, "eino-agent"
        )
        self.assertEqual((inspect_code, inspected["status"]), (2, "blocked"))
        self.assertTrue(
            any(
                item["message"] == "linked request sets contain a cycle"
                for item in inspected["errors"]
            )
        )

        reuse = self.member(
            "eino-tools",
            a_name,
            a_body,
            mode="reuse",
            contract="cycle-a",
            expected_sha256=hashlib.sha256(a_body.encode()).hexdigest(),
        )
        result, code = creation.create_set(
            self.projects,
            self.create_manifest(f"{DATE}-later", [reuse]),
            "cycle-reuse",
        )
        self.assertEqual(code, 2)
        self.assertEqual(result["errors"][0]["code"], "inconsistent_request_set")
