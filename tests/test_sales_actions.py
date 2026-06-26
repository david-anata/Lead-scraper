"""Tests for the confidence-tiered action system:
compute_pending_actions logic and the /actions/approve route."""

from __future__ import annotations

import os
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import PropertyMock, patch, MagicMock

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
from sales_support_agent.services.sales.actions import ContactInfo, compute_pending_actions  # noqa: E402


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
            deal_stage_label="",
            pipeline="",
            amount_cents=0,
            is_closed=False,
            is_won=False,
            close_date=None,
            # Managed mirror fields used by staleness rules and note builder
            last_meaningful_touch_at=None,
            last_inbound_at=None,
            last_outbound_at=None,
            created_at=None,
            owner_email="",
            hubspot_company_id="",
            description="",
        )
        defaults.update(kwargs)
        return types.SimpleNamespace(**defaults)

    # ------------------------------------------------------------------
    # close_date rules
    # ------------------------------------------------------------------

    def test_overdue_deal_returns_push_close_date(self):
        deal = self._deal(
            close_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            amount_cents=5_000_00,  # non-zero to avoid missing_amount flag
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
        deal = self._deal(close_date=datetime(2027, 1, 1, tzinfo=timezone.utc), amount_cents=100_000)
        actions = compute_pending_actions(
            deal, [], as_of=datetime(2026, 6, 22, tzinfo=timezone.utc)
        )
        self.assertFalse(any("push_close_date" in a.action_id for a in actions))

    def test_closed_deal_returns_no_actions(self):
        deal = self._deal(is_closed=True)
        actions = compute_pending_actions(deal, [])
        self.assertEqual([], actions)

    def test_missing_close_date_returns_set_close_date(self):
        deal = self._deal(close_date=None, amount_cents=100_000)
        actions = compute_pending_actions(
            deal, [], as_of=datetime(2026, 6, 24, tzinfo=timezone.utc)
        )
        ids = [a.action_id for a in actions]
        self.assertIn("d1:set_close_date", ids)
        action = next(a for a in actions if a.action_id == "d1:set_close_date")
        self.assertEqual(action.confidence, "mid")
        self.assertIn("closedate", action.properties)

    def test_existing_close_date_no_set_action(self):
        deal = self._deal(close_date=datetime(2026, 9, 1, tzinfo=timezone.utc), amount_cents=100_000)
        actions = compute_pending_actions(
            deal, [], as_of=datetime(2026, 6, 24, tzinfo=timezone.utc)
        )
        self.assertFalse(any("set_close_date" in a.action_id for a in actions))

    # ------------------------------------------------------------------
    # amount rules
    # ------------------------------------------------------------------

    def test_zero_amount_with_line_item_total_returns_sync_amount(self):
        deal = self._deal(amount_cents=0)
        actions = compute_pending_actions(deal, [], line_item_total_cents=500_000)
        ids = [a.action_id for a in actions]
        self.assertIn("d1:sync_amount", ids)
        action = next(a for a in actions if a.action_id == "d1:sync_amount")
        self.assertEqual(action.confidence, "mid")
        self.assertIn("amount", action.properties)
        self.assertEqual(action.properties["amount"], "5000.00")

    def test_nonzero_amount_no_sync_action(self):
        deal = self._deal(amount_cents=100_000)
        actions = compute_pending_actions(deal, [], line_item_total_cents=200_000)
        self.assertFalse(any("sync_amount" in a.action_id for a in actions))

    def test_no_line_item_total_no_sync_action(self):
        deal = self._deal(amount_cents=0)
        actions = compute_pending_actions(deal, [], line_item_total_cents=0)
        self.assertFalse(any("sync_amount" in a.action_id for a in actions))

    def test_zero_amount_no_line_items_returns_missing_amount_flag(self):
        deal = self._deal(amount_cents=0)
        actions = compute_pending_actions(deal, [], line_item_total_cents=0)
        ids = [a.action_id for a in actions]
        self.assertIn("d1:missing_amount", ids)
        action = next(a for a in actions if a.action_id == "d1:missing_amount")
        self.assertEqual(action.confidence, "mid")
        self.assertEqual(action.action_type, "flag")
        self.assertEqual(action.severity, "critical")

    # ------------------------------------------------------------------
    # staleness rules
    # ------------------------------------------------------------------

    def test_never_touched_old_deal_returns_create_note(self):
        as_of = datetime(2026, 6, 24, tzinfo=timezone.utc)
        deal = self._deal(
            amount_cents=100_000,
            created_at=as_of - timedelta(days=10),  # 10 days old — old_enough
        )
        actions = compute_pending_actions(deal, [], as_of=as_of)
        ids = [a.action_id for a in actions]
        self.assertIn("d1:never_touched", ids)
        action = next(a for a in actions if a.action_id == "d1:never_touched")
        self.assertEqual(action.confidence, "mid")
        self.assertEqual(action.action_type, "create_note")
        self.assertEqual(action.severity, "critical")
        self.assertTrue(action.note_body)

    def test_new_deal_not_old_enough_no_never_touched(self):
        as_of = datetime(2026, 6, 24, tzinfo=timezone.utc)
        deal = self._deal(
            amount_cents=100_000,
            created_at=as_of - timedelta(days=3),  # only 3 days — not old enough
        )
        actions = compute_pending_actions(deal, [], as_of=as_of)
        self.assertFalse(any("never_touched" in a.action_id for a in actions))

    def test_stale_30d_returns_create_note_critical(self):
        as_of = datetime(2026, 6, 24, tzinfo=timezone.utc)
        deal = self._deal(
            amount_cents=100_000,
            last_meaningful_touch_at=as_of - timedelta(days=35),
        )
        actions = compute_pending_actions(deal, [], as_of=as_of)
        ids = [a.action_id for a in actions]
        self.assertIn("d1:stale_30d", ids)
        action = next(a for a in actions if a.action_id == "d1:stale_30d")
        self.assertEqual(action.confidence, "mid")
        self.assertEqual(action.action_type, "create_note")
        self.assertEqual(action.severity, "critical")
        self.assertTrue(action.note_body)

    def test_stale_14d_returns_create_note_warning(self):
        as_of = datetime(2026, 6, 24, tzinfo=timezone.utc)
        deal = self._deal(
            amount_cents=100_000,
            last_meaningful_touch_at=as_of - timedelta(days=20),
        )
        actions = compute_pending_actions(deal, [], as_of=as_of)
        ids = [a.action_id for a in actions]
        self.assertIn("d1:stale_14d", ids)
        action = next(a for a in actions if a.action_id == "d1:stale_14d")
        self.assertEqual(action.confidence, "mid")
        self.assertEqual(action.action_type, "create_note")
        self.assertEqual(action.severity, "warning")

    def test_recent_touch_no_staleness_action(self):
        as_of = datetime(2026, 6, 24, tzinfo=timezone.utc)
        deal = self._deal(
            amount_cents=100_000,
            last_meaningful_touch_at=as_of - timedelta(days=5),
        )
        actions = compute_pending_actions(deal, [], as_of=as_of)
        self.assertFalse(any(a.category == "staleness" for a in actions))

    # ------------------------------------------------------------------
    # review_note rule
    # ------------------------------------------------------------------

    def test_review_note_generated_when_mid_issues_no_staleness_note(self):
        as_of = datetime(2026, 6, 24, tzinfo=timezone.utc)
        # Overdue close date -> push_close_date (mid) + issues_for_note populated.
        # No staleness note. So review_note should be created.
        deal = self._deal(
            amount_cents=100_000,
            close_date=as_of - timedelta(days=10),
        )
        actions = compute_pending_actions(deal, [], as_of=as_of)
        ids = [a.action_id for a in actions]
        self.assertIn("d1:review_note", ids)
        review = next(a for a in actions if a.action_id == "d1:review_note")
        self.assertEqual(review.action_type, "create_note")
        self.assertEqual(review.category, "review")
        self.assertTrue(review.note_body)

    def test_no_review_note_when_staleness_note_already_written(self):
        as_of = datetime(2026, 6, 24, tzinfo=timezone.utc)
        # stale_30d creates a note -> review_note should NOT also be created
        deal = self._deal(
            amount_cents=100_000,
            last_meaningful_touch_at=as_of - timedelta(days=35),
        )
        actions = compute_pending_actions(deal, [], as_of=as_of)
        self.assertFalse(any("review_note" in a.action_id for a in actions))

    # ------------------------------------------------------------------
    # SalesAction fields — severity, category, action_type contract
    # ------------------------------------------------------------------

    def test_push_close_date_has_severity_and_category(self):
        deal = self._deal(
            close_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            amount_cents=100_000,
        )
        actions = compute_pending_actions(
            deal, [], as_of=datetime(2026, 6, 22, tzinfo=timezone.utc)
        )
        action = next(a for a in actions if "push_close_date" in a.action_id)
        self.assertIn(action.severity, ("critical", "warning", "hygiene"))
        self.assertTrue(action.category)

    def test_no_action_type_equals_note(self):
        # Legacy "note" type was replaced by "create_note"; must never appear.
        deal = self._deal(
            close_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        as_of = datetime(2026, 6, 24, tzinfo=timezone.utc)
        actions = compute_pending_actions(deal, [], as_of=as_of)
        for a in actions:
            self.assertNotEqual(a.action_type, "note",
                                f"action {a.action_id} still uses old 'note' type")

    # ------------------------------------------------------------------
    # Phase 3 — stage_move
    # ------------------------------------------------------------------

    def test_stage_move_mid_confidence_when_pipeline_data_available(self):
        deal = self._deal(
            deal_stage="stage_1",
            pipeline="pipeline_a",
            amount_cents=100_000,
        )
        from sales_support_agent.models.entities import MailboxSignal
        signal = MagicMock(spec=MailboxSignal)
        signal.received_at = datetime(2026, 6, 23, tzinfo=timezone.utc)
        signal.subject = "Re: your proposal"
        signal.signal_type = ""

        with patch(
            "sales_support_agent.services.sales.actions._try_get_next_stage",
            return_value=("stage_2", "Presentation Scheduled"),
        ):
            actions = compute_pending_actions(
                deal,
                [signal],
                as_of=datetime(2026, 6, 24, tzinfo=timezone.utc),
            )

        ids = [a.action_id for a in actions]
        self.assertIn("d1:stage_move", ids)
        action = next(a for a in actions if a.action_id == "d1:stage_move")
        self.assertEqual(action.confidence, "mid")
        self.assertEqual(action.action_type, "update_deal")
        self.assertEqual(action.properties["dealstage"], "stage_2")
        self.assertIn("Presentation Scheduled", action.label)
        self.assertFalse(any(a.action_id == "d1:replied_note" for a in actions))

    def test_replied_note_fallback_when_no_pipeline_data(self):
        deal = self._deal(deal_stage="stage_1", pipeline="pipeline_a", amount_cents=100_000)
        from sales_support_agent.models.entities import MailboxSignal
        signal = MagicMock(spec=MailboxSignal)
        signal.received_at = datetime(2026, 6, 23, tzinfo=timezone.utc)
        signal.subject = "Interested"
        signal.signal_type = ""

        with patch(
            "sales_support_agent.services.sales.actions._try_get_next_stage",
            return_value=None,
        ):
            actions = compute_pending_actions(
                deal,
                [signal],
                as_of=datetime(2026, 6, 24, tzinfo=timezone.utc),
            )

        ids = [a.action_id for a in actions]
        self.assertIn("d1:replied_note", ids)
        self.assertFalse(any(a.action_id == "d1:stage_move" for a in actions))
        replied = next(a for a in actions if a.action_id == "d1:replied_note")
        self.assertEqual(replied.confidence, "low")

    def test_late_stage_deal_no_stage_move(self):
        deal = self._deal(deal_stage="contractsent", pipeline="pipeline_a", amount_cents=100_000)
        from sales_support_agent.models.entities import MailboxSignal
        signal = MagicMock(spec=MailboxSignal)
        signal.received_at = datetime(2026, 6, 23, tzinfo=timezone.utc)
        signal.subject = "Signed"
        signal.signal_type = ""

        with patch(
            "sales_support_agent.services.sales.actions._try_get_next_stage",
            return_value=("closedwon", "Closed Won"),
        ):
            actions = compute_pending_actions(
                deal,
                [signal],
                as_of=datetime(2026, 6, 24, tzinfo=timezone.utc),
            )

        self.assertFalse(any("stage_move" in a.action_id for a in actions))
        self.assertFalse(any("replied_note" in a.action_id for a in actions))


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

    def test_approve_create_note_action_calls_create_note(self):
        from sales_support_agent.services.sales.actions import SalesAction
        fake_note_action = SalesAction(
            action_id="act1:stale_30d",
            action_type="create_note",
            confidence="mid",
            severity="critical",
            category="staleness",
            label="Stale",
            description="desc",
            hubspot_object_type="deals",
            hubspot_object_id="act1",
            note_body="Test note body",
        )
        mock_note = {"id": "note_1"}
        # Patch at the router module where the name is already bound.
        with patch.object(HubSpotClient, "is_configured", new_callable=PropertyMock, return_value=True), \
             patch.object(HubSpotClient, "create_note", return_value=mock_note) as mock_cn, \
             patch.object(HubSpotClient, "update_deal", return_value={}) as mock_ud, \
             patch("sales_support_agent.api.sales_router.compute_pending_actions",
                   return_value=[fake_note_action]):
            resp = self.client.post(
                "/admin/sales/deals/act1/actions/approve",
                data={"action_id": "act1:stale_30d"},
                follow_redirects=False,
            )
        self.assertIn(resp.status_code, (302, 303))
        mock_cn.assert_called_once()
        mock_ud.assert_not_called()

    def test_deal_detail_shows_pending_action_card(self):
        body = self.client.get("/admin/sales/deals/act1").text
        # Overdue close date + zero amount with line items -> two mid-confidence actions
        self.assertIn("Pending actions", body)
        self.assertIn("Approve", body)


if __name__ == "__main__":
    unittest.main()
