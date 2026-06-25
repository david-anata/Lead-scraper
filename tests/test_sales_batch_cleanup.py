"""Tests for the batch cleanup page (GET /admin/sales/deals/cleanup and POST)."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import PropertyMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/sales_batch_cleanup_test.db",
)
os.environ.setdefault("HUBSPOT_PORTAL_ID", "999")

from fastapi.testclient import TestClient  # noqa: E402

from sales_support_agent.integrations.hubspot import HubSpotClient  # noqa: E402
from sales_support_agent.main import app  # noqa: E402
from sales_support_agent.models.database import session_scope  # noqa: E402
from sales_support_agent.models.entities import (  # noqa: E402
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
    HubSpotLineItem,
)
from sales_support_agent.services.admin_auth import create_user_session_token  # noqa: E402
from sales_support_agent.services.sales.deal_batch import build_batch_cleanup  # noqa: E402


def _cookie_for(email: str) -> tuple[str, str]:
    s = app.state.agent_settings
    return s.admin_cookie_name, create_user_session_token(s, email=email, name="D", role="member")


class TestBuildBatchCleanup(unittest.TestCase):
    """Unit tests for build_batch_cleanup — DB only, no HTTP."""

    @classmethod
    def setUpClass(cls) -> None:
        with session_scope(app.state.session_factory) as s:
            for r in s.query(HubSpotDealContact).all():
                s.delete(r)
            for r in s.query(HubSpotLineItem).all():
                s.delete(r)
            for r in s.query(HubSpotDeal).all():
                s.delete(r)
            for r in s.query(HubSpotContact).all():
                s.delete(r)
            # Deal with overdue close date + zero amount but line items → 2 mid actions
            s.add(HubSpotDeal(
                hubspot_deal_id="bc_d1",
                deal_name="Batch Deal A",
                deal_stage="appointmentscheduled",
                amount_cents=0,
                is_closed=False,
                close_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            ))
            s.add(HubSpotLineItem(
                hubspot_line_item_id="bc_li1",
                hubspot_deal_id="bc_d1",
                name="Service",
                quantity=1,
                unit_price_cents=500_000,
                amount_cents=500_000,
            ))
            # Closed deal — should produce zero actions
            s.add(HubSpotDeal(
                hubspot_deal_id="bc_d2",
                deal_name="Closed Deal",
                deal_stage="closedwon",
                amount_cents=100_000,
                is_closed=True,
            ))
            # Open deal, all clean — no close date missing (has future one), no line items mismatch
            s.add(HubSpotDeal(
                hubspot_deal_id="bc_d3",
                deal_name="Clean Deal",
                deal_stage="appointmentscheduled",
                amount_cents=200_000,
                is_closed=False,
                close_date=datetime(2027, 6, 1, tzinfo=timezone.utc),
            ))

    def _get_rows(self):
        with session_scope(app.state.session_factory) as s:
            return build_batch_cleanup(s, portal_id="999")

    def test_returns_mid_confidence_actions_only(self):
        rows = self._get_rows()
        self.assertTrue(len(rows) > 0)
        for r in rows:
            self.assertEqual(r.action.confidence, "mid")
            self.assertNotEqual(r.action.action_type, "note")

    def test_closed_deal_excluded(self):
        rows = self._get_rows()
        deal_ids = [r.deal_id for r in rows]
        self.assertNotIn("bc_d2", deal_ids)

    def test_deal_a_has_push_close_date_and_sync_amount(self):
        rows = self._get_rows()
        d1_rows = [r for r in rows if r.deal_id == "bc_d1"]
        action_ids = [r.action.action_id for r in d1_rows]
        self.assertIn("bc_d1:push_close_date", action_ids)
        self.assertIn("bc_d1:sync_amount", action_ids)

    def test_clean_deal_has_no_mid_actions(self):
        rows = self._get_rows()
        d3_rows = [r for r in rows if r.deal_id == "bc_d3"]
        self.assertEqual(d3_rows, [])

    def test_rows_have_required_fields(self):
        rows = self._get_rows()
        for r in rows:
            self.assertTrue(r.deal_id)
            self.assertTrue(r.deal_name)
            self.assertTrue(r.action.action_id)
            self.assertTrue(r.action.properties)


class TestBatchCleanupRoutes(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cookie_name, token = _cookie_for("david@anatainc.com")
        cls.client.cookies.set(cookie_name, token)

    def test_get_cleanup_page_renders(self):
        resp = self.client.get("/admin/sales/deals/cleanup")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Cleanup", resp.text)

    def test_get_cleanup_page_shows_pending_actions(self):
        resp = self.client.get("/admin/sales/deals/cleanup")
        self.assertEqual(resp.status_code, 200)
        # bc_d1 has mid-confidence actions from setUpClass seed
        self.assertIn("Batch Deal A", resp.text)

    def test_get_with_applied_shows_flash(self):
        resp = self.client.get("/admin/sales/deals/cleanup?applied=3&failed=0")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("3 action", resp.text)

    def test_get_with_failed_shows_warn_flash(self):
        resp = self.client.get("/admin/sales/deals/cleanup?applied=1&failed=2")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("2 failed", resp.text)

    def test_post_with_no_hubspot_token_redirects_with_error(self):
        with patch.object(
            app.state.agent_settings.__class__,
            "hubspot_api_token",
            new_callable=lambda: property(lambda self: ""),
        ):
            resp = self.client.post(
                "/admin/sales/deals/cleanup",
                data={},
                follow_redirects=False,
            )
        self.assertIn(resp.status_code, (302, 303))
        self.assertIn("error", resp.headers.get("location", ""))

    def test_post_with_empty_selection_redirects_cleanly(self):
        resp = self.client.post(
            "/admin/sales/deals/cleanup",
            data={},
            follow_redirects=False,
        )
        self.assertIn(resp.status_code, (302, 303))
        loc = resp.headers.get("location", "")
        self.assertIn("cleanup", loc)

    def test_post_applies_selected_actions_and_redirects_with_applied_count(self):
        mock_update = {"id": "bc_d1", "properties": {}}
        with patch.object(HubSpotClient, "is_configured", new_callable=PropertyMock, return_value=True), \
             patch.object(HubSpotClient, "update_deal", return_value=mock_update):
            resp = self.client.post(
                "/admin/sales/deals/cleanup",
                data={"action_ids": ["bc_d1:sync_amount"]},
                follow_redirects=False,
            )
        self.assertIn(resp.status_code, (302, 303))
        loc = resp.headers.get("location", "")
        self.assertIn("applied=1", loc)

    def test_post_hubspot_failure_counts_as_failed(self):
        with patch.object(HubSpotClient, "is_configured", new_callable=PropertyMock, return_value=True), \
             patch.object(HubSpotClient, "update_deal", side_effect=RuntimeError("HubSpot 503")):
            resp = self.client.post(
                "/admin/sales/deals/cleanup",
                data={"action_ids": ["bc_d1:sync_amount"]},
                follow_redirects=False,
            )
        self.assertIn(resp.status_code, (302, 303))
        loc = resp.headers.get("location", "")
        self.assertIn("failed=1", loc)

    def test_deal_board_has_cleanup_link(self):
        resp = self.client.get("/admin/sales/deals")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("cleanup", resp.text)


if __name__ == "__main__":
    unittest.main()
