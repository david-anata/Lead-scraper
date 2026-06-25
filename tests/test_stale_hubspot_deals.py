"""Tests for Phase 6b: StaleHubSpotDealsJob, deal board stale badge, last_inbound_at wiring."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/stale_hs_deals_test.db",
)
os.environ.setdefault("HUBSPOT_PORTAL_ID", "999")

from sales_support_agent.main import app  # noqa: E402
from sales_support_agent.models.database import session_scope  # noqa: E402
from sales_support_agent.models.entities import HubSpotDeal  # noqa: E402
from sales_support_agent.integrations.slack import SlackClient  # noqa: E402
from sales_support_agent.jobs.stale_hubspot_deals import StaleHubSpotDealsJob  # noqa: E402
from sales_support_agent.services.sales.deal_board import build_deal_board  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sales_support_agent.services.admin_auth import create_user_session_token  # noqa: E402


_NOW = datetime(2026, 6, 25, 12, 0, 0, tzinfo=timezone.utc)
_14_DAYS_AGO = _NOW - timedelta(days=14, seconds=1)
_RECENT = _NOW - timedelta(days=3)


def _seed_deals(session_factory):
    with session_scope(session_factory) as s:
        for r in s.query(HubSpotDeal).all():
            s.delete(r)
        # Stale: no last_inbound_at
        s.add(HubSpotDeal(
            hubspot_deal_id="stale_d1",
            deal_name="No-touch Deal",
            deal_stage="appointmentscheduled",
            amount_cents=100_000,
            is_closed=False,
            last_inbound_at=None,
        ))
        # Stale: last_inbound_at > 14 days ago
        s.add(HubSpotDeal(
            hubspot_deal_id="stale_d2",
            deal_name="Old Touch Deal",
            deal_stage="appointmentscheduled",
            amount_cents=200_000,
            is_closed=False,
            last_inbound_at=_14_DAYS_AGO,
        ))
        # Fresh: inbound 3 days ago
        s.add(HubSpotDeal(
            hubspot_deal_id="fresh_d3",
            deal_name="Recent Deal",
            deal_stage="appointmentscheduled",
            amount_cents=300_000,
            is_closed=False,
            last_inbound_at=_RECENT,
        ))
        # Closed: excluded from stale check
        s.add(HubSpotDeal(
            hubspot_deal_id="closed_d4",
            deal_name="Closed Deal",
            deal_stage="closedwon",
            amount_cents=0,
            is_closed=True,
            last_inbound_at=None,
        ))


class TestStaleHubSpotDealsJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _seed_deals(app.state.session_factory)

    def _run(self, *, dry_run=True, slack_configured=False, digest_enabled=False):
        slack = MagicMock(spec=SlackClient)
        slack.is_configured = slack_configured
        settings = app.state.agent_settings
        # Override stale_deal_slack_digest_enabled via object.__setattr__ (frozen dataclass)
        object.__setattr__(settings, "stale_deal_slack_digest_enabled", digest_enabled)
        object.__setattr__(settings, "stale_deal_days", 14)
        with session_scope(app.state.session_factory) as s:
            job = StaleHubSpotDealsJob(settings, slack, s)
            return job.run(dry_run=dry_run, as_of=_NOW), slack

    def test_identifies_stale_deals_correctly(self):
        result, _ = self._run()
        self.assertEqual(result.stale_count, 2)   # stale_d1 and stale_d2
        self.assertEqual(result.total_open, 3)     # excludes closed_d4

    def test_fresh_deal_not_counted_stale(self):
        result, _ = self._run()
        self.assertEqual(result.total_open - result.stale_count, 1)  # fresh_d3

    def test_dry_run_does_not_post_slack(self):
        result, slack = self._run(dry_run=True, slack_configured=True, digest_enabled=True)
        self.assertFalse(result.digest_posted)
        slack.post_message.assert_not_called()

    def test_digest_not_posted_when_disabled(self):
        result, slack = self._run(dry_run=False, slack_configured=True, digest_enabled=False)
        self.assertFalse(result.digest_posted)
        slack.post_message.assert_not_called()

    def test_digest_posted_when_enabled_and_configured(self):
        result, slack = self._run(dry_run=False, slack_configured=True, digest_enabled=True)
        self.assertTrue(result.digest_posted)
        slack.post_message.assert_called_once()
        call_kwargs = slack.post_message.call_args.kwargs
        self.assertIn("stale", call_kwargs.get("text", "").lower())

    def test_no_stale_deals_skips_slack(self):
        # Temporarily update all deals to have a recent touch.
        with session_scope(app.state.session_factory) as s:
            for d in s.query(HubSpotDeal).filter(HubSpotDeal.is_closed.is_(False)).all():
                d.last_inbound_at = _RECENT
        slack = MagicMock(spec=SlackClient)
        slack.is_configured = True
        settings = app.state.agent_settings
        object.__setattr__(settings, "stale_deal_slack_digest_enabled", True)
        object.__setattr__(settings, "stale_deal_days", 14)
        with session_scope(app.state.session_factory) as s:
            job = StaleHubSpotDealsJob(settings, slack, s)
            result = job.run(dry_run=False, as_of=_NOW)
        self.assertEqual(result.stale_count, 0)
        slack.post_message.assert_not_called()
        # Re-seed for subsequent tests
        _seed_deals(app.state.session_factory)


class TestDealBoardStaleBadge(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _seed_deals(app.state.session_factory)

    def test_stale_deals_flagged_in_board_rows(self):
        with session_scope(app.state.session_factory) as s:
            board = build_deal_board(s, as_of=_NOW, stale_days=14)
        stale_rows = [r for r in board.rows if r.is_stale]
        fresh_rows = [r for r in board.rows if not r.is_stale]
        self.assertEqual(len(stale_rows), 2)
        self.assertEqual(len(fresh_rows), 1)

    def test_fresh_deal_not_stale(self):
        with session_scope(app.state.session_factory) as s:
            board = build_deal_board(s, as_of=_NOW, stale_days=14)
        fresh = next(r for r in board.rows if r.deal_id == "fresh_d3")
        self.assertFalse(fresh.is_stale)

    def test_deal_with_no_inbound_is_stale(self):
        with session_scope(app.state.session_factory) as s:
            board = build_deal_board(s, as_of=_NOW, stale_days=14)
        no_touch = next(r for r in board.rows if r.deal_id == "stale_d1")
        self.assertTrue(no_touch.is_stale)

    def test_stale_badge_appears_in_board_html(self):
        client = TestClient(app)
        s = app.state.agent_settings
        cookie_name, token = s.admin_cookie_name, create_user_session_token(
            s, email="david@anatainc.com", name="D", role="member"
        )
        client.cookies.set(cookie_name, token)
        resp = client.get("/admin/sales/deals")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("stale-badge", resp.text)


class TestDisableClickupSalesSync(unittest.TestCase):
    def test_disable_clickup_sales_sync_skips_clickup(self):
        """When disable_clickup_sales_sync=True, ClickUpSyncService is not called."""
        from sales_support_agent.jobs.stale_leads import StaleLeadJob
        from sales_support_agent.integrations.clickup import ClickUpClient
        settings = app.state.agent_settings
        object.__setattr__(settings, "disable_clickup_sales_sync", True)
        try:
            clickup_client = MagicMock(spec=ClickUpClient)
            slack_client = MagicMock(spec=SlackClient)
            slack_client.is_configured = False
            with session_scope(app.state.session_factory) as s:
                job = StaleLeadJob(settings, clickup_client, slack_client, s)
                # Run with 0 tasks to make it fast
                job.run(dry_run=True, max_tasks=0)
            # ClickUp methods should not have been called for sync
            clickup_client.get_task.assert_not_called()
        finally:
            object.__setattr__(settings, "disable_clickup_sales_sync", False)


if __name__ == "__main__":
    unittest.main()
