"""Route tests for Sales Priorities > Deal Detail (HubSpot companion page):
tool gating, 404 for unknown deals, the accountability layer (completeness +
next action), read-only records with HubSpot deep links, and the three
closing-tool CTAs. Real backend app + temp SQLite."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/sales_deal_detail_test.db"
)
from fastapi.testclient import TestClient  # noqa: E402

from sales_support_agent.main import app  # noqa: E402
from sales_support_agent.models.database import session_scope  # noqa: E402
from sales_support_agent.models.entities import (  # noqa: E402
    AutomationRun,
    DeckVisitSession,
    HubSpotCompany,
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
    HubSpotLineItem,
    SalesDealAsset,
)
from sales_support_agent.services.admin_auth import create_user_session_token  # noqa: E402


def _cookie_for(email: str, name: str = "User", role: str = "member"):
    s = app.state.agent_settings
    return s.admin_cookie_name, create_user_session_token(s, email=email, name=name, role=role)


class SalesDealDetailTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cookie_name, token = _cookie_for("david@anatainc.com", "David")  # seeded superadmin
        cls.client.cookies.set(cookie_name, token)
        # Settings is frozen; bypass with object.__setattr__ so the portal id
        # is present regardless of which test file initializes the app first.
        object.__setattr__(app.state.settings, "hubspot_portal_id", "999")
        cls._seed()

    @classmethod
    def _seed(cls) -> None:
        with session_scope(app.state.session_factory) as s:
            # FK-safe delete order: join/child tables before parents.
            for model in (HubSpotDealContact, SalesDealAsset, HubSpotLineItem, DeckVisitSession,
                          HubSpotContact, HubSpotDeal, HubSpotCompany):
                for row in s.query(model).all():
                    s.delete(row)
            for row in s.query(AutomationRun).all():
                s.delete(row)
            s.flush()
            # A fully-populated deal.
            s.add(HubSpotCompany(hubspot_company_id="co1", name="Acme Inc", domain="acme.com"))
            s.add(HubSpotDeal(
                hubspot_deal_id="full", deal_name="Acme — Fulfillment",
                deal_stage="contractsent", deal_stage_label="Contract Sent",
                amount_cents=4250000, close_date=datetime(2026, 7, 1, tzinfo=timezone.utc),
                owner_email="maya@anatainc.com", hubspot_company_id="co1", is_closed=False,
            ))
            s.add(HubSpotContact(hubspot_contact_id="p1", first_name="Sam", last_name="Lee",
                                 email="sam@acme.com", job_title="COO"))
            s.add(HubSpotDealContact(hubspot_deal_id="full", hubspot_contact_id="p1"))
            s.add(HubSpotLineItem(hubspot_line_item_id="li1", hubspot_deal_id="full",
                                  name="3PL Pick & Pack", quantity=2,
                                  unit_price_cents=100000, amount_cents=200000))
            s.add(AutomationRun(
                id=42,
                run_type="fulfillment_rate_sheet",
                status="completed",
                trigger="test",
                started_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
                completed_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
                summary_json={
                    "view_path": "/rate-sheets/acme/42/tok",
                    "export_token": "cost-token",
                    "hubspot_deal_id": "full",
                    "hubspot_quote_url": "https://app.hubspot.com/quotes/abc",
                    "published_at": "2026-06-01T00:00:00+00:00",
                    "fulfillment_cost_submissions": [{
                        "at": "2026-06-01T12:00:00+00:00",
                        "name": "Kyle Paulson",
                        "email": "kyle@anatainc.com",
                        "costs": {"pick_pack_per_order": 0.8},
                    }],
                    "sales_pricing": {
                        "reviewed": True,
                        "margin_pct": 42.5,
                        "fee_rows": [],
                    },
                },
            ))
            s.add(DeckVisitSession(run_id=42, visitor_token="visitor-1", is_internal=False))
            s.add(SalesDealAsset(hubspot_deal_id="full", asset_type="rate_sheet",
                                 run_id="42", url="/rate-sheets/acme/42/tok", label="Rate Sheet"))
            # An empty deal — missing everything (drives the accountability nudge).
            s.add(HubSpotDeal(
                hubspot_deal_id="empty", deal_name="Globex — New", deal_stage="appointmentscheduled",
                amount_cents=0, close_date=None, owner_email="", is_closed=False,
            ))

    def test_tool_gating_denies_member(self) -> None:
        client = TestClient(app)
        cookie_name, token = _cookie_for("nobody@anatainc.com", "Nobody", role="member")
        client.cookies.set(cookie_name, token)
        resp = client.get("/admin/sales/deals/full", follow_redirects=False)
        self.assertNotEqual(resp.status_code, 200)

    def test_unknown_deal_404(self) -> None:
        resp = self.client.get("/admin/sales/deals/does-not-exist")
        self.assertEqual(resp.status_code, 404)

    def test_full_deal_renders_records_and_hubspot_links(self) -> None:
        body = self.client.get("/admin/sales/deals/full").text
        self.assertIn("Acme — Fulfillment", body)
        self.assertIn("Contract Sent", body)          # friendly stage
        self.assertIn("3PL Pick &amp; Pack", body)    # line item (escaped)
        self.assertIn("Sam Lee", body)                # contact
        self.assertIn("Acme Inc", body)               # company
        # "Open in HubSpot" deep link built from portal id + deal id.
        self.assertIn("app.hubspot.com/contacts/999/record/0-3/full", body)

    def test_closing_tool_cta_present(self) -> None:
        body = self.client.get("/admin/sales/deals/full").text
        self.assertIn("/rate-sheets/acme/42/tok", body)
        # The other two closing tools show as "not linked yet" placeholders.
        self.assertIn("Sales Deck", body)
        self.assertIn("Ads Audit", body)
        self.assertIn("/admin/sales-decks?hubspot_deal_id=full", body)

    def test_command_center_shows_fulfillment_quote_and_cost_status(self) -> None:
        body = self.client.get("/admin/sales/deals/full").text
        self.assertIn("Sales command center", body)
        self.assertIn("Fulfillment-to-sales workflow", body)
        self.assertIn("Source of truth", body)
        self.assertIn("Signed", body)
        self.assertIn("Kyle Paulson", body)
        self.assertIn("/fulfillment-costs/42/cost-token", body)
        self.assertIn("Quote ready", body)
        self.assertIn("https://app.hubspot.com/quotes/abc", body)
        self.assertIn("1 prospect view", body)

    def test_empty_deal_shows_accountability_nudge(self) -> None:
        body = self.client.get("/admin/sales/deals/empty").text
        # The companion's value: a concrete next action + completeness gaps.
        self.assertIn("Next action", body)
        self.assertIn("add the buyer in HubSpot", body)
        self.assertIn("Create fulfillment deck", body)


if __name__ == "__main__":
    unittest.main()
