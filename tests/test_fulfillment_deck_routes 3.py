"""Route tests for Fulfillment > Sales Deck: tool gating, generate flow,
public token-gated view, heartbeat persistence. Real backend app + temp SQLite
(same harness as test_access_rbac)."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/fulfillment_deck_routes_test.db")

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.main import app
from sales_support_agent.models.database import get_engine
from sales_support_agent.models.entities import DeckVisitSession
from sales_support_agent.services.access import store
from sales_support_agent.services.admin_auth import create_user_session_token
from sales_support_agent.services.fulfillment_deck import storage

_NOTES = "Brand: TabCo\nWidget — 6 x 5 x 3 in, 1.5 lb, ~500 units/mo"


def _cookie_for(email: str, name: str = "User", role: str = "member"):
    s = app.state.agent_settings
    return s.admin_cookie_name, create_user_session_token(s, email=email, name=name, role=role)


class FulfillmentDeckRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cookie_name, token = _cookie_for("david@anatainc.com", "David")  # seeded superadmin
        cls.client.cookies.set(cookie_name, token)

    def setUp(self) -> None:
        patcher = mock.patch.dict(
            os.environ, {"ANTHROPIC_API_KEY": "", "ANATA_WMS_BASE_URL": ""}, clear=False
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _generate(self) -> dict:
        response = self.client.post(
            "/admin/fulfillment/sales/generate",
            data={"notes": _NOTES, "origin_zip": "84043"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        runs = storage.list_runs()
        self.assertTrue(runs)
        return runs[0]

    def test_landing_renders_for_superadmin(self) -> None:
        response = self.client.get("/admin/fulfillment/sales")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Rate", response.text)
        self.assertIn("Generate rate sheet", response.text)
        self.assertIn("84043", response.text)

    def test_tool_gate_blocks_unauthorized_user(self) -> None:
        existing = store.get_role_by_name("FinanceOnlyRateSheet")
        rid = existing["id"] if existing else store.create_role("FinanceOnlyRateSheet", ["finance"], description="")
        store.upsert_user("fin_rs@anatainc.com", "Fin", role_id=rid)
        blocked = TestClient(app)
        cookie_name, token = _cookie_for("fin_rs@anatainc.com", "Fin")
        blocked.cookies.set(cookie_name, token)
        response = blocked.get("/admin/fulfillment/sales", follow_redirects=False)
        self.assertEqual(response.status_code, 403)

    def test_generate_requires_some_input(self) -> None:
        response = self.client.post(
            "/admin/fulfillment/sales/generate", data={"notes": ""}, follow_redirects=False
        )
        self.assertEqual(response.status_code, 303)
        self.assertIn("kind=warn", response.headers["location"])

    def test_generate_then_public_view_and_token_gate(self) -> None:
        run = self._generate()
        view_path = run["view_path"]
        self.assertTrue(view_path.startswith("/rate-sheets/"))

        public = TestClient(app)  # logged-out client
        response = public.get(view_path)
        self.assertEqual(response.status_code, 200)
        self.assertIn("TabCo", response.text)
        self.assertIn("window.print()", response.text)

        bad = view_path.rsplit("/", 1)[0] + "/" + "0" * 32
        self.assertEqual(public.get(bad).status_code, 404)

    def test_heartbeat_creates_visit_session(self) -> None:
        run = self._generate()
        public = TestClient(app)
        response = public.post(
            run["view_path"] + "/heartbeat",
            json={
                "visitor_token": "11111111-2222-4333-8444-555555555555",
                "is_internal": False,
                "total_seconds": 42,
                "max_scroll_pct": 60,
                "sections": {"sec-01": 30, "sec-03": 12},
            },
        )
        self.assertEqual(response.status_code, 200)
        with Session(get_engine()) as s:
            row = s.execute(
                select(DeckVisitSession).where(
                    DeckVisitSession.run_id == run["id"],
                    DeckVisitSession.visitor_token == "11111111-2222-4333-8444-555555555555",
                )
            ).scalar_one_or_none()
        self.assertIsNotNone(row)
        self.assertEqual(row.total_seconds, 42)
        self.assertEqual(row.max_scroll_pct, 60)

        engagement = storage.engagement_for([run["id"]])
        self.assertEqual(engagement[run["id"]]["external_sessions"], 1)

        response = public.post(run["view_path"] + "/heartbeat", json={})
        self.assertEqual(response.status_code, 400)

    def test_delete_removes_run_and_engagement(self) -> None:
        run = self._generate()
        public = TestClient(app)
        public.post(
            run["view_path"] + "/heartbeat",
            json={"visitor_token": "delete-test-token", "total_seconds": 5},
        )
        response = self.client.post(
            f"/admin/fulfillment/sales/runs/{run['id']}/delete", follow_redirects=False
        )
        self.assertEqual(response.status_code, 303)
        self.assertIsNone(storage.get_run(run["id"]))
        self.assertEqual(TestClient(app).get(run["view_path"]).status_code, 404)
        with Session(get_engine()) as s:
            leftover = s.execute(
                select(DeckVisitSession).where(DeckVisitSession.run_id == run["id"])
            ).scalars().all()
        self.assertEqual(leftover, [])


if __name__ == "__main__":
    unittest.main()
