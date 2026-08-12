from __future__ import annotations

import os
import tempfile

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + os.path.join(tempfile.gettempdir(), "contract_google_doc.db"),
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET", "contract-google-doc-secret"
)

import unittest
from unittest import mock

from sales_support_agent.integrations.building_google_docs import (
    BuildingContractDocsClient,
    PLACEHOLDER_RE,
)


class ContractDocsClientTests(unittest.TestCase):
    """The client must fail loudly about configuration and never send."""

    def test_readiness_names_the_missing_piece(self) -> None:
        self.assertIn(
            "service-account",
            BuildingContractDocsClient(
                service_account_json="", template_document_id="t", drive_folder_id="f"
            ).readiness_error,
        )
        self.assertIn(
            "template",
            BuildingContractDocsClient(
                service_account_json="{}", template_document_id="", drive_folder_id="f"
            ).readiness_error,
        )
        self.assertIn(
            "Drive folder",
            BuildingContractDocsClient(
                service_account_json="{}", template_document_id="t", drive_folder_id=""
            ).readiness_error,
        )
        self.assertTrue(
            BuildingContractDocsClient(
                service_account_json="{}", template_document_id="t", drive_folder_id="f"
            ).configured
        )

    def test_placeholder_pattern_tolerates_spacing(self) -> None:
        found = PLACEHOLDER_RE.findall(
            "Between Anata and {{customer_name}} for {{ event_space }}."
        )
        self.assertEqual(found, ["customer_name", "event_space"])

    def test_draft_copies_then_replaces_and_never_sends(self) -> None:
        client = BuildingContractDocsClient(
            service_account_json="{}",
            template_document_id="template-1",
            drive_folder_id="folder-1",
        )
        calls: list[tuple[str, dict]] = []

        class _Resp:
            status_code = 200
            content = b"{}"

            def __init__(self, payload: dict) -> None:
                self._payload = payload

            def json(self) -> dict:
                return self._payload

        class _Session:
            def post(self, url: str, **kwargs):
                calls.append((url, kwargs))
                if "/copy" in url:
                    return _Resp({"id": "copy-1"})
                return _Resp({})

        with mock.patch.object(client, "_authorized_session", return_value=_Session()):
            created = client.create_contract_draft(
                title="Acme contract",
                values={"customer_name": "Acme", "event_space": "Arena"},
            )

        self.assertEqual(created["document_id"], "copy-1")
        self.assertIn("copy-1", created["document_url"])

        copy_call, fill_call = calls
        self.assertIn("/copy", copy_call[0])
        self.assertEqual(copy_call[1]["json"]["parents"], ["folder-1"])
        replacements = fill_call[1]["json"]["requests"]
        replaced = {
            r["replaceAllText"]["containsText"]["text"]: r["replaceAllText"]["replaceText"]
            for r in replacements
        }
        self.assertEqual(replaced["{{customer_name}}"], "Acme")
        self.assertEqual(replaced["{{event_space}}"], "Arena")

        # Nothing in the flow may email, share, or request a signature.
        for url, kwargs in calls:
            self.assertNotIn("permissions", url)
            self.assertNotIn("send", url.lower())
            self.assertNotIn("emailMessage", str(kwargs))


if __name__ == "__main__":
    unittest.main()


class ContractDocsPreflightTests(unittest.TestCase):
    """Configuration being set is not the same as access being granted."""

    def _client(self) -> BuildingContractDocsClient:
        return BuildingContractDocsClient(
            service_account_json='{"client_email": "agent@proj.iam.gserviceaccount.com"}',
            template_document_id="tpl", drive_folder_id="folder",
        )

    def test_it_names_the_account_to_share_with(self) -> None:
        self.assertEqual(
            self._client().service_account_email,
            "agent@proj.iam.gserviceaccount.com",
        )

    def test_an_unshared_template_is_reported_with_the_address(self) -> None:
        class _Resp:
            def __init__(self, code): self.status_code = code
            def json(self): return {}
        class _Session:
            def get(self, url, **kwargs): return _Resp(404)
        client = self._client()
        with mock.patch.object(client, "_authorized_session", return_value=_Session()):
            report = client.preflight()
        self.assertFalse(report["template_readable"])
        self.assertFalse(report["folder_writable"])
        joined = " ".join(report["problems"])
        self.assertIn("agent@proj.iam.gserviceaccount.com", joined)
        self.assertIn("template", joined.lower())

    def test_a_readable_folder_without_write_access_is_distinguished(self) -> None:
        class _Resp:
            def __init__(self, payload): self.status_code = 200; self._p = payload
            def json(self): return self._p
        class _Session:
            def get(self, url, **kwargs):
                if "tpl" in url:
                    return _Resp({"id": "tpl"})
                return _Resp({
                    "id": "folder",
                    "driveId": "drive-1",
                    "capabilities": {"canAddChildren": False},
                })
        client = self._client()
        with mock.patch.object(client, "_authorized_session", return_value=_Session()):
            report = client.preflight()
        self.assertTrue(report["template_readable"])
        self.assertFalse(report["folder_writable"])
        self.assertIn("Editor", " ".join(report["problems"]))

    def test_everything_shared_reports_no_problems(self) -> None:
        class _Resp:
            def __init__(self, payload): self.status_code = 200; self._p = payload
            def json(self): return self._p
        class _Session:
            def get(self, url, **kwargs):
                if "tpl" in url:
                    return _Resp({"id": "tpl"})
                return _Resp({
                    "id": "folder",
                    "driveId": "drive-1",
                    "capabilities": {"canAddChildren": True},
                })
        client = self._client()
        with mock.patch.object(client, "_authorized_session", return_value=_Session()):
            report = client.preflight()
        self.assertTrue(report["template_readable"])
        self.assertTrue(report["folder_writable"])
        self.assertTrue(report["folder_in_shared_drive"])
        self.assertEqual(report["problems"], [])

    def test_my_drive_folder_is_not_reported_as_production_ready(self) -> None:
        class _Resp:
            def __init__(self, payload): self.status_code = 200; self._p = payload
            def json(self): return self._p
        class _Session:
            def get(self, url, **kwargs):
                if "tpl" in url:
                    return _Resp({"id": "tpl"})
                return _Resp({"id": "folder", "capabilities": {"canAddChildren": True}})
        client = self._client()
        with mock.patch.object(client, "_authorized_session", return_value=_Session()):
            report = client.preflight()
        self.assertFalse(report["folder_in_shared_drive"])
        self.assertFalse(report["folder_writable"])
        self.assertIn("My Drive", " ".join(report["problems"]))
