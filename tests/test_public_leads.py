from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
import uuid
from unittest import mock

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/public_leads_boot.db")

try:
    from fastapi.testclient import TestClient
    from sales_support_agent.main import app
    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class PublicLeadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from sales_support_agent.models.database import create_session_factory, init_database

        path = os.path.join(tempfile.gettempdir(), f"public_leads_{uuid.uuid4().hex}.db")
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(app.state.settings, marketing_site_intake_key="test-key")
        cls.client = TestClient(app)

    def test_requires_shared_key(self) -> None:
        response = self.client.post("/api/public/leads/contact", json={})
        self.assertEqual(response.status_code, 401)

    @mock.patch("sales_support_agent.api.leads_router._notify", return_value=False)
    @mock.patch("sales_support_agent.api.leads_router._record_hubspot", return_value=False)
    def test_persists_valid_note_before_optional_handoffs(self, _hubspot, _notify) -> None:
        response = self.client.post(
            "/api/public/leads/contact",
            headers={"X-Internal-Api-Key": "test-key", "X-Marketing-Client-Key": "a" * 64},
            json={
                "kind": "contact",
                "name": "Pat Operator",
                "email": "PAT@example.com",
                "message": "Please call me about fulfillment.",
                "source": "visitor email should not be source@example.com",
                "ignored": "must not persist",
            },
        )
        self.assertEqual(response.status_code, 201, response.text)
        self.assertTrue(response.json()["ok"])
        from sales_support_agent.models.database import session_scope
        from sales_support_agent.models.entities import AutomationRun

        with session_scope(app.state.session_factory) as session:
            run = session.get(AutomationRun, response.json()["lead_id"])
            self.assertEqual(run.status, "success")
            self.assertEqual(run.metadata_json["email"], "pat@example.com")
            self.assertEqual(run.metadata_json["source"], "anatainc.com")
            self.assertNotIn("ignored", run.metadata_json)
            self.assertTrue(run.summary_json["accepted"])

    def test_rejects_invalid_fields(self) -> None:
        response = self.client.post(
            "/api/public/leads/contact",
            headers={"X-Internal-Api-Key": "test-key", "X-Marketing-Client-Key": "b" * 64},
            json={"kind": "careers", "name": "Pat", "email": "bad", "message": "Hi"},
        )
        self.assertEqual(response.status_code, 400)


if __name__ == "__main__":
    unittest.main()
