"""Route tests for Sales Priorities > Deal Board: tool gating, close-date
ordering (soonest first), and the completeness read. Real backend app + temp
SQLite, same harness as test_fulfillment_deck_routes."""

from __future__ import annotations

import os
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/sales_deal_board_test.db"
)

from fastapi.testclient import TestClient  # noqa: E402

from sales_support_agent.main import app  # noqa: E402
from sales_support_agent.models.database import session_scope  # noqa: E402
from sales_support_agent.models.entities import (  # noqa: E402
    HubSpotDeal,
    HubSpotDealContact,
    HubSpotLineItem,
)
from sales_support_agent.services.admin_auth import create_user_session_token  # noqa: E402
from sales_support_agent.services.sales.deal_board import (  # noqa: E402
    DealBoard,
    _bucket,
    DealRow,
    render_deal_board_page,
)


def _cookie_for(email: str, name: str = "User", role: str = "member"):
    s = app.state.agent_settings
    return s.admin_cookie_name, create_user_session_token(s, email=email, name=name, role=role)


class SalesDealBoardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cookie_name, token = _cookie_for("david@anatainc.com", "David")  # seeded superadmin
        cls.client.cookies.set(cookie_name, token)
        cls._seed()

    @classmethod
    def _seed(cls) -> None:
        with session_scope(app.state.session_factory) as s:
            # FK-safe delete order: join tables first, then parents.
            for row in s.query(HubSpotDealContact).all():
                s.delete(row)
            for row in s.query(HubSpotLineItem).all():
                s.delete(row)
            for row in s.query(HubSpotDeal).all():
                s.delete(row)
            s.flush()
            # A complete, soon-closing deal.
            s.add(HubSpotDeal(
                hubspot_deal_id="d_soon", deal_name="Acme Soon", deal_stage="qualifiedtobuy",
                deal_stage_label="Qualified To Buy",
                amount_cents=500000, close_date=datetime(2026, 6, 25, tzinfo=timezone.utc),
                owner_email="rep@anatainc.com", is_closed=False,
            ))
            s.add(HubSpotLineItem(hubspot_line_item_id="li1", hubspot_deal_id="d_soon",
                                  name="Service", quantity=1, unit_price_cents=500000, amount_cents=500000))
            s.add(HubSpotDealContact(hubspot_deal_id="d_soon", hubspot_contact_id="c1"))
            # A later-closing, incomplete deal (no amount/line items/contacts).
            s.add(HubSpotDeal(
                hubspot_deal_id="d_late", deal_name="Globex Later", deal_stage="appointmentscheduled",
                amount_cents=0, close_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
                owner_email="rep@anatainc.com", is_closed=False,
            ))
            # A deal with no close date — sinks to the bottom.
            s.add(HubSpotDeal(
                hubspot_deal_id="d_none", deal_name="NoDate Deal", deal_stage="appointmentscheduled",
                amount_cents=100000, close_date=None, is_closed=False,
            ))
            # A closed deal — must NOT appear on the open board.
            s.add(HubSpotDeal(
                hubspot_deal_id="d_won", deal_name="Won Deal", deal_stage="closedwon",
                amount_cents=900000, close_date=datetime(2026, 5, 1, tzinfo=timezone.utc),
                is_closed=True, is_won=True,
            ))
            # An overdue deal — past close date, clearly in "Past close date" bucket.
            s.add(HubSpotDeal(
                hubspot_deal_id="d_overdue", deal_name="OverdueCorp Deal",
                deal_stage="appointmentscheduled", deal_stage_label="Appointment Scheduled",
                amount_cents=200000, close_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
                owner_email="rep@anatainc.com", is_closed=False,
            ))
            # A deal owned by the logged-in superadmin — used for "My deals" filter tests.
            s.add(HubSpotDeal(
                hubspot_deal_id="d_david", deal_name="DavidCorp Deal",
                deal_stage="appointmentscheduled", deal_stage_label="Appointment Scheduled",
                amount_cents=300000, close_date=datetime(2026, 7, 15, tzinfo=timezone.utc),
                owner_email="david@anatainc.com", is_closed=False,
            ))

    def test_tool_gating_denies_member_without_tool(self) -> None:
        client = TestClient(app)
        cookie_name, token = _cookie_for("nobody@anatainc.com", "Nobody", role="member")
        client.cookies.set(cookie_name, token)
        resp = client.get("/admin/sales/deals", follow_redirects=False)
        self.assertNotEqual(resp.status_code, 200)

    def test_board_renders_for_superadmin(self) -> None:
        resp = self.client.get("/admin/sales/deals")
        self.assertEqual(resp.status_code, 200)
        body = resp.text
        self.assertIn("Acme Soon", body)
        self.assertIn("Globex Later", body)

    def test_closed_deal_excluded(self) -> None:
        body = self.client.get("/admin/sales/deals").text
        self.assertNotIn("Won Deal", body)

    def test_close_date_ordering_soonest_first(self) -> None:
        body = self.client.get("/admin/sales/deals").text
        i_soon = body.index("Acme Soon")
        i_late = body.index("Globex Later")
        i_none = body.index("NoDate Deal")
        self.assertLess(i_soon, i_late)        # soonest close date first
        self.assertLess(i_late, i_none)        # no-close-date sinks to bottom

    def test_completeness_flags(self) -> None:
        body = self.client.get("/admin/sales/deals").text
        # The complete deal reads as ready; the incomplete one flags missing bits.
        self.assertIn("✓ ready", body)
        self.assertIn("line items", body)
        self.assertIn("contacts", body)

    def test_friendly_stage_label_shown_not_raw_key(self) -> None:
        body = self.client.get("/admin/sales/deals").text
        # P0 fix: the human label renders; the raw HubSpot stage id does not.
        self.assertIn("Qualified To Buy", body)
        self.assertNotIn("qualifiedtobuy", body)

    def test_deal_name_links_to_detail(self) -> None:
        body = self.client.get("/admin/sales/deals").text
        # Deal detail now exists, so names link to it (no longer a dead link).
        self.assertIn('href="/admin/sales/deals/d_soon"', body)
        self.assertIn("Acme Soon", body)


class TestBucketAssignment(unittest.TestCase):
    """Unit tests for _bucket() — no DB, no HTTP."""

    def _row(self, close_date=None) -> DealRow:
        return DealRow(
            deal_id="x", name="X", stage="open", stage_label="Open",
            amount_cents=0, close_date=close_date, owner_email="rep@x.com",
            company_name="", line_item_count=0, contact_count=0,
        )

    def _as_of(self) -> datetime:
        return datetime(2026, 6, 23, 12, 0, 0, tzinfo=timezone.utc)

    def test_no_close_date_is_no_date_bucket(self) -> None:
        self.assertEqual(_bucket(self._row(), as_of=self._as_of()), "no_date")

    def test_past_close_date_is_overdue(self) -> None:
        row = self._row(close_date=datetime(2025, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(_bucket(row, as_of=self._as_of()), "overdue")

    def test_three_days_out_is_this_week(self) -> None:
        row = self._row(close_date=self._as_of() + timedelta(days=3))
        self.assertEqual(_bucket(row, as_of=self._as_of()), "this_week")

    def test_fifteen_days_out_is_this_month(self) -> None:
        row = self._row(close_date=self._as_of() + timedelta(days=15))
        self.assertEqual(_bucket(row, as_of=self._as_of()), "this_month")

    def test_sixty_days_out_is_later(self) -> None:
        row = self._row(close_date=self._as_of() + timedelta(days=60))
        self.assertEqual(_bucket(row, as_of=self._as_of()), "later")

    def test_exactly_seven_days_out_is_this_week(self) -> None:
        row = self._row(close_date=self._as_of() + timedelta(days=7))
        self.assertEqual(_bucket(row, as_of=self._as_of()), "this_week")

    def test_exactly_thirty_days_out_is_this_month(self) -> None:
        row = self._row(close_date=self._as_of() + timedelta(days=30))
        self.assertEqual(_bucket(row, as_of=self._as_of()), "this_month")

    def test_naive_close_date_treated_as_utc(self) -> None:
        # Naive datetimes should not raise; they're treated as UTC.
        row = self._row(close_date=datetime(2025, 3, 1))  # naive, clearly past
        self.assertEqual(_bucket(row, as_of=self._as_of()), "overdue")


class TestDealBoardP1Routes(unittest.TestCase):
    """Route-level tests for P1 board features (bucketing, owner filter, flag links)."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cookie_name, token = _cookie_for("david@anatainc.com", "David")
        cls.client.cookies.set(cookie_name, token)
        cls._seed()

    @classmethod
    def _seed(cls) -> None:
        with session_scope(app.state.session_factory) as s:
            # FK-safe delete: join tables before parents.
            for row in s.query(HubSpotDealContact).all():
                s.delete(row)
            for row in s.query(HubSpotLineItem).all():
                s.delete(row)
            for row in s.query(HubSpotDeal).all():
                s.delete(row)
            s.flush()
            # Overdue deal (owned by rep) — for "Past close date" bucket header.
            s.add(HubSpotDeal(
                hubspot_deal_id="p1_overdue", deal_name="OverdueCorp P1",
                deal_stage="appointmentscheduled", deal_stage_label="Appointment Scheduled",
                amount_cents=100_000,
                close_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
                owner_email="rep@anatainc.com", is_closed=False,
            ))
            # David's deal — filtered by ?my=1.
            s.add(HubSpotDeal(
                hubspot_deal_id="p1_david", deal_name="DavidCorp Deal",
                deal_stage="appointmentscheduled", deal_stage_label="Appointment Scheduled",
                amount_cents=300_000,
                close_date=datetime(2026, 7, 15, tzinfo=timezone.utc),
                owner_email="david@anatainc.com", is_closed=False,
            ))
            # Incomplete deal owned by rep — for flag-chip test.
            s.add(HubSpotDeal(
                hubspot_deal_id="p1_incomplete", deal_name="Incomplete Corp",
                deal_stage="appointmentscheduled",
                amount_cents=0, close_date=None,
                owner_email="rep@anatainc.com", is_closed=False,
            ))

    def test_overdue_bucket_header_shown(self) -> None:
        body = self.client.get("/admin/sales/deals").text
        self.assertIn("Past close date", body)
        self.assertIn("OverdueCorp P1", body)

    def test_bucket_headers_present(self) -> None:
        body = self.client.get("/admin/sales/deals").text
        bucket_labels = ["Past close date", "Closing this week", "Closing this month", "Later", "No close date"]
        self.assertTrue(any(label in body for label in bucket_labels))

    def test_my_deals_filter_shows_only_own_deals(self) -> None:
        body = self.client.get("/admin/sales/deals?my=1").text
        self.assertIn("DavidCorp Deal", body)
        self.assertNotIn("OverdueCorp P1", body)
        self.assertNotIn("Incomplete Corp", body)

    def test_my_deals_tab_active_when_filtered(self) -> None:
        body = self.client.get("/admin/sales/deals?my=1").text
        self.assertIn('href="/admin/sales/deals?my=1" class="tab tab--active"', body)

    def test_all_deals_tab_active_by_default(self) -> None:
        body = self.client.get("/admin/sales/deals").text
        self.assertIn('href="/admin/sales/deals" class="tab tab--active"', body)

    def test_flag_chips_are_anchor_tags(self) -> None:
        body = self.client.get("/admin/sales/deals").text
        self.assertIn('<a class="flag"', body)

    def test_my_deals_empty_state_render(self) -> None:
        # Unit-level: render_deal_board_page with empty board + show_my renders the right message.
        html = render_deal_board_page(DealBoard(), show_my=True)
        self.assertIn("No deals assigned to you yet", html)

    def test_all_deals_empty_state_render(self) -> None:
        html = render_deal_board_page(DealBoard(), show_my=False)
        self.assertIn("Sync now", html)


if __name__ == "__main__":
    unittest.main()
