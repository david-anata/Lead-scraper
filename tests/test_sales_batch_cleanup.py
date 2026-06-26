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
from sales_support_agent.services.sales.deal_batch import (  # noqa: E402
    NOTE_COOLDOWN_DAYS,
    build_batch_cleanup,
    note_applied_key,
    record_note_applied,
)


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
            # Deal with overdue close date + zero amount but line items -> 2 mid actions
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
            # Open deal, amount set, future close date — no mid actions expected
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

    def test_rows_have_actions_list(self):
        rows = self._get_rows()
        self.assertTrue(len(rows) > 0)
        for r in rows:
            self.assertIsInstance(r.actions, list)
            self.assertTrue(len(r.actions) > 0)

    def test_no_action_type_is_note(self):
        # Legacy "note" type must never appear — it was renamed to "create_note".
        rows = self._get_rows()
        for r in rows:
            for a in r.actions:
                self.assertNotEqual(a.action_type, "note",
                                    f"action {a.action_id} uses old 'note' type")

    def test_closed_deal_excluded(self):
        rows = self._get_rows()
        deal_ids = [r.deal_id for r in rows]
        self.assertNotIn("bc_d2", deal_ids)

    def test_deal_a_has_push_close_date_and_sync_amount(self):
        rows = self._get_rows()
        d1_rows = [r for r in rows if r.deal_id == "bc_d1"]
        self.assertEqual(len(d1_rows), 1)
        action_ids = [a.action_id for a in d1_rows[0].actions]
        self.assertIn("bc_d1:push_close_date", action_ids)
        self.assertIn("bc_d1:sync_amount", action_ids)

    def test_clean_deal_has_no_mid_actions(self):
        # bc_d3 has amount + future close date — no mid-confidence actions.
        # It may still have low-confidence hygiene flags (no contacts, no company),
        # so we check for absence of mid actions rather than absence from rows.
        rows = self._get_rows()
        d3_rows = [r for r in rows if r.deal_id == "bc_d3"]
        for r in d3_rows:
            mid_actions = [a for a in r.actions if a.confidence == "mid"]
            self.assertEqual(mid_actions, [],
                             f"bc_d3 should have no mid-confidence actions, got {[a.action_id for a in mid_actions]}")

    def test_rows_have_required_fields(self):
        rows = self._get_rows()
        for r in rows:
            self.assertTrue(r.deal_id)
            self.assertTrue(r.deal_name)
            self.assertIsInstance(r.actions, list)
            # Each action must have core fields
            for a in r.actions:
                self.assertTrue(a.action_id)
                self.assertIn(a.confidence, ("mid", "low"))
                self.assertIn(a.severity, ("critical", "warning", "hygiene"))
                self.assertTrue(a.category)

    def test_deal_row_has_stage_label_not_raw_id(self):
        # Stage badge must be the human-readable label (or "Unknown stage"),
        # never a raw portal numeric ID.
        rows = self._get_rows()
        d1_rows = [r for r in rows if r.deal_id == "bc_d1"]
        self.assertEqual(len(d1_rows), 1)
        stage = d1_rows[0].deal_stage_label
        # Should not look like a pure numeric HubSpot portal ID
        self.assertFalse(stage.isdigit(),
                         f"deal_stage_label looks like a raw ID: {stage!r}")

    def test_deal_row_has_context_fields(self):
        rows = self._get_rows()
        d1_rows = [r for r in rows if r.deal_id == "bc_d1"]
        self.assertEqual(len(d1_rows), 1)
        r = d1_rows[0]
        self.assertIsNotNone(r.amount_cents)
        self.assertIsInstance(r.contact_count, int)

    # ------------------------------------------------------------------
    # Error isolation
    # ------------------------------------------------------------------

    def test_bad_deal_skipped_good_deal_still_appears(self):
        # If compute_pending_actions raises for one deal, the rest still render.
        from sales_support_agent.services.sales import deal_batch as _db_mod

        call_count = [0]
        real_compute = _db_mod.compute_pending_actions

        def patched_compute(deal, signals, **kwargs):
            call_count[0] += 1
            if deal.hubspot_deal_id == "bc_d1" and call_count[0] == 1:
                raise RuntimeError("simulated corrupt deal field")
            return real_compute(deal, signals, **kwargs)

        with patch.object(_db_mod, "compute_pending_actions", side_effect=patched_compute):
            with session_scope(app.state.session_factory) as s:
                rows = build_batch_cleanup(s, portal_id="999")

        deal_ids = [r.deal_id for r in rows]
        # bc_d1 errored and was skipped; bc_d2 is closed (no rows); bc_d3 may appear.
        self.assertNotIn("bc_d1", deal_ids)
        # No exception propagated to the caller — the method returned cleanly.

    # ------------------------------------------------------------------
    # Note cooldown
    # ------------------------------------------------------------------

    def _clear_cooldown(self, deal_id: str) -> None:
        from sales_support_agent.models.database import kv_set
        # Overwrite the KV entry with a timestamp far in the past (>7 days)
        import json
        from datetime import timedelta
        old_time = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
        kv_set(note_applied_key(deal_id), json.dumps({"applied_at": old_time}))

    def test_note_actions_present_before_cooldown(self):
        # Ensure any cooldown record is expired before this check.
        self._clear_cooldown("bc_d1")
        rows = self._get_rows()
        d1_rows = [r for r in rows if r.deal_id == "bc_d1"]
        if not d1_rows:
            self.skipTest("bc_d1 not in rows (no actions)")
        r = d1_rows[0]
        # With an expired record, active_cooldown_days should be None (not suppressing).
        self.assertIsNone(r.last_note_days_ago)
        # bc_d1 has overdue close date + zero amount with line items → review_note expected.
        note_actions = [a for a in r.actions if a.action_type == "create_note"]
        self.assertTrue(len(note_actions) > 0,
                        f"Expected create_note actions after cooldown cleared; got: "
                        f"{[a.action_id for a in r.actions]}")

    def test_note_actions_suppressed_after_record_note_applied(self):
        record_note_applied("bc_d1")
        try:
            rows = self._get_rows()
            d1_rows = [r for r in rows if r.deal_id == "bc_d1"]
            if not d1_rows:
                return  # deal may have been fully suppressed (only had note actions)
            r = d1_rows[0]
            note_actions = [a for a in r.actions if a.action_type == "create_note"]
            self.assertEqual(note_actions, [],
                             "create_note actions must be suppressed within cooldown")
            # last_note_days_ago should be 0 (applied just now)
            self.assertIsNotNone(r.last_note_days_ago)
            self.assertLess(r.last_note_days_ago, NOTE_COOLDOWN_DAYS)
        finally:
            self._clear_cooldown("bc_d1")

    def test_cooldown_indicator_appears_in_page_when_suppressed(self):
        from fastapi.testclient import TestClient
        client = TestClient(app)
        cookie_name, token = _cookie_for("david@anatainc.com")
        client.cookies.set(cookie_name, token)

        record_note_applied("bc_d1")
        try:
            resp = client.get("/admin/sales/deals/cleanup")
            self.assertEqual(resp.status_code, 200)
            self.assertIn("suppressed", resp.text)
        finally:
            self._clear_cooldown("bc_d1")


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

    def test_get_cleanup_page_shows_summary_bar(self):
        resp = self.client.get("/admin/sales/deals/cleanup")
        self.assertEqual(resp.status_code, 200)
        # Summary bar must appear with severity counts
        self.assertIn("critical", resp.text)

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

    def test_post_applies_selected_update_deal_action(self):
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

    def test_post_applies_create_note_action(self):
        from sales_support_agent.services.sales.actions import SalesAction
        from sales_support_agent.services.sales.deal_batch import BatchCleanupRow
        mock_note = {"id": "note_1"}
        fake_row = BatchCleanupRow(
            deal_id="bc_d1",
            deal_name="Batch Deal A",
            deal_stage_label="Appointment",
            amount_cents=0,
            owner_email="",
            last_touch_at=None,
            contact_count=0,
            actions=[SalesAction(
                action_id="bc_d1:stale_30d",
                action_type="create_note",
                confidence="mid",
                severity="critical",
                category="staleness",
                label="Stale",
                description="desc",
                hubspot_object_type="deals",
                hubspot_object_id="bc_d1",
                note_body="Test note body",
            )],
        )
        # Patch at the router module where the name is already bound.
        with patch.object(HubSpotClient, "is_configured", new_callable=PropertyMock, return_value=True), \
             patch.object(HubSpotClient, "create_note", return_value=mock_note) as mock_cn, \
             patch.object(HubSpotClient, "update_deal", return_value={}) as mock_ud, \
             patch("sales_support_agent.api.sales_router.build_batch_cleanup",
                   return_value=[fake_row]):
            resp = self.client.post(
                "/admin/sales/deals/cleanup",
                data={"action_ids": ["bc_d1:stale_30d"]},
                follow_redirects=False,
            )
        self.assertIn(resp.status_code, (302, 303))
        mock_cn.assert_called_once()
        mock_ud.assert_not_called()
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

    def test_flag_action_type_not_applied(self):
        # Flag actions (missing_amount, no_contacts, etc.) should be skipped
        # even when their action_id is submitted — no HubSpot write must occur.
        from sales_support_agent.services.sales.actions import SalesAction
        from sales_support_agent.services.sales.deal_batch import BatchCleanupRow
        fake_flag = BatchCleanupRow(
            deal_id="bc_d1",
            deal_name="Batch Deal A",
            deal_stage_label="Appointment",
            amount_cents=0,
            owner_email="",
            last_touch_at=None,
            contact_count=0,
            actions=[SalesAction(
                action_id="bc_d1:missing_amount",
                action_type="flag",
                confidence="mid",
                severity="critical",
                category="amount",
                label="No amount",
                description="desc",
                hubspot_object_type="deals",
                hubspot_object_id="bc_d1",
            )],
        )
        with patch.object(HubSpotClient, "is_configured", new_callable=PropertyMock, return_value=True), \
             patch.object(HubSpotClient, "update_deal", return_value={}) as mock_ud, \
             patch.object(HubSpotClient, "create_note", return_value={}) as mock_cn, \
             patch("sales_support_agent.api.sales_router.build_batch_cleanup",
                   return_value=[fake_flag]):
            resp = self.client.post(
                "/admin/sales/deals/cleanup",
                data={"action_ids": ["bc_d1:missing_amount"]},
                follow_redirects=False,
            )
        mock_ud.assert_not_called()
        mock_cn.assert_not_called()
        # applied=0 since flag was skipped
        loc = resp.headers.get("location", "")
        self.assertIn("applied=0", loc)

    def test_deal_board_has_cleanup_link(self):
        resp = self.client.get("/admin/sales/deals")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("cleanup", resp.text)


if __name__ == "__main__":
    unittest.main()
