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
