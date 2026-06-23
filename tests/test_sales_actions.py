"""Tests for the confidence-tiered action system:
compute_pending_actions logic and the /actions/approve route."""

from __future__ import annotations

import os
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import PropertyMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/sales_actions_test.db",
)
os.environ.setdefault("HUBSPOT_PORTAL_ID", "999")

from fastapi.testclient import TestClient  # noqa: E402

from sales_support_agent.integrations.hubspot import HubSpotClient  # noqa: E402
from sales_support_agent.main import app  # noqa: E402
from sales_support_agent.models.database import session_scope  # noqa: E402
from sales_support_agent.models.entities import HubSpotDeal, HubSpotLineItem  # noqa: E402
from sales_support_agent.services.admin_auth import create_user_session_token  # noqa: E402
from sales_support_agent.services.sales.actions import compute_pending_actions  # noqa: E402


def _cookie_for(email: str, name: str = "User", role: str = "member"):
    s = app.state.agent_settings
    return s.admin_cookie_name, create_user_session_token(s, email=email, name=name, role=role)


class TestComputePendingActions(unittest.TestCase):
    """Unit tests for compute_pending_actions — no HTTP, no DB.

    Uses SimpleNamespace instead of the ORM class to avoid SA instrumentation.
    """

    def _deal(self, **kwargs) -> object:
        defaults = dict(
            hubspot_deal_id="d1",
            deal_name="Test",
            deal_stage="appointmentscheduled",
            amount_cents=0,
            is_closed=False,
            is_won=False,
            close_date=None,
        )
        defaults.update(kwargs)
        return types.SimpleNamespace(**defaults)

    def test_overdue_deal_returns_push_close_date(self):
        deal = self._deal(
            close_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        actions = compute_pending_actions(
            deal, [], as_of=datetime(2026, 6, 22, tzinfo=timezone.utc)
        )
        ids = [a.action_id for a in actions]
        self.assertIn("d1:push_close_date", ids)
        action = next(a for a in actions if a.action_id == "d1:push_close_date")
        self.assertEqual(action.confidence, "mid")
        self.assertIn("closedate", action.properties)

    def test_future_close_date_no_push_action(self):
        deal = self._deal(close_date=datetime(2027, 1, 1, tzinfo=timezone.utc))
        actions = compute_pending_actions(
            deal, [], as_of=datetime(2026, 6, 22, tzinfo=timezone.utc)
        )
        self.assertFalse(any("push_close_date" in a.action_id for a in actions))

    def test_closed_deal_returns_no_actions(self):
        deal = self._deal(is_closed=True)
        actions = compute_pending_actions(deal, [])
        self.assertEqual([], actions)

    def test_zero_amount_with_line_item_total_returns_sync_amount(self):
        deal = self._deal(amount_cents=0)
        actions = compute_pending_actions(deal, [], line_item_total_cents=500_000)
        ids = [a.action_id for a in actions]
        self.assertIn("d1:sync_amount", ids)
        action = next(a for a in actions if a.action_id == "d1:sync_amount")
        self.assertEqual(action.confidence, "mid")
        self.assertIn("amount", action.properties)
        self.assertEqual(action.properties["amount"], "5000.0")

    def test_nonzero_amount_no_sync_action(self):
        deal = self._deal(amount_cents=100_000)
        actions = compute_pending_actions(deal, [], line_item_total_cents=200_000)
        self.assertFalse(any("sync_amount" in a.action_id for a in actions))

    def test_no_line_item_total_no_sync_action(self):
        deal = self._deal(amount_cents=0)
        actions = compute_pending_actions(deal, [], line_item_total_cents=0)
        self.assertFalse(any("sync_amount" in a.action_id for a in actions))


class TestApproveActionRoute(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cookie_name, token = _cookie_for("david@anatainc.com", "David")
        cls.client.cookies.set(cookie_name, token)
        cls._seed()

    @classmethod
    def _seed(cls) -> None:
        with session_scope(app.state.session_factory) as s:
            for row in s.query(HubSpotDeal).all():
                s.delete(row)
            for row in s.query(HubSpotLineItem).all():
                s.delete(row)
            s.add(HubSpotDeal(
                hubspot_deal_id="act1",
                deal_name="Action Test Deal",
                deal_stage="appointmentscheduled",
                amount_cents=0,
                is_closed=False,
                close_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            ))
            s.add(HubSpotLineItem(
                hubspot_line_item_id="li_act1",
                hubspot_deal_id="act1",
                name="Service",
                quantity=1,
                unit_price_cents=300_000,
                amount_cents=300_000,
            ))

    def test_approve_with_hubspot_unconfigured_redirects_with_error(self):
        with patch.object(
            app.state.agent_settings.__class__,
            "hubspot_api_token",
            new_callable=lambda: property(lambda self: ""),
        ):
            resp = self.client.post(
                "/admin/sales/deals/act1/actions/approve",
                data={"action_id": "act1:sync_amount"},
                follow_redirects=False,
            )
        self.assertIn(resp.status_code, (302, 303))

    def test_approve_unknown_action_id_is_no_op(self):
        resp = self.client.post(
            "/admin/sales/deals/act1/actions/approve",
            data={"action_id": "act1:nonexistent_action"},
            follow_redirects=False,
        )
        self.assertIn(resp.status_code, (302, 303))

    def test_approve_pushes_update_and_redirects_with_actioned(self):
        mock_result = {"id": "act1", "properties": {"amount": "3000.0"}}
        with patch.object(HubSpotClient, "is_configured", new_callable=PropertyMock, return_value=True), \
             patch.object(HubSpotClient, "update_deal", return_value=mock_result):
            resp = self.client.post(
                "/admin/sales/deals/act1/actions/approve",
                data={"action_id": "act1:sync_amount"},
                follow_redirects=False,
            )
        self.assertIn(resp.status_code, (302, 303))
        self.assertIn("actioned=1", resp.headers.get("location", ""))

    def test_deal_detail_shows_pending_action_card(self):
        body = self.client.get("/admin/sales/deals/act1").text
        # Overdue close date + zero amount with line items → two mid-confidence actions
        self.assertIn("Pending actions", body)
        self.assertIn("Approve", body)


if __name__ == "__main__":
    unittest.main()
