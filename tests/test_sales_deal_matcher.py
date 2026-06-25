"""Tests for match_deal_by_email — email → open HubSpot deal lookup."""

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
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/sales_deal_matcher_test.db",
)

from sales_support_agent.main import app  # noqa: E402
from sales_support_agent.models.database import session_scope  # noqa: E402
from sales_support_agent.models.entities import (  # noqa: E402
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
)
from sales_support_agent.services.sales.deal_matcher import match_deal_by_email  # noqa: E402


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _seed(session, *, contact_email: str, deal_id: str, is_closed: bool = False, close_date=None):
    """Insert a minimal contact+deal+link set and flush."""
    contact = HubSpotContact(
        hubspot_contact_id=f"c_{deal_id}",
        first_name="Test",
        last_name="User",
        email=contact_email,
    )
    deal = HubSpotDeal(
        hubspot_deal_id=deal_id,
        deal_name=f"Deal {deal_id}",
        deal_stage="appointmentscheduled",
        amount_cents=10_000,
        is_closed=is_closed,
        close_date=close_date,
    )
    link = HubSpotDealContact(
        hubspot_deal_id=deal_id,
        hubspot_contact_id=f"c_{deal_id}",
    )
    session.add(contact)
    session.add(deal)
    session.flush()
    session.add(link)
    session.flush()


class TestMatchDealByEmail(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.factory = app.state.session_factory

    def _clean(self, session):
        for row in session.query(HubSpotDealContact).all():
            session.delete(row)
        for row in session.query(HubSpotDeal).all():
            session.delete(row)
        for row in session.query(HubSpotContact).all():
            session.delete(row)
        session.flush()

    def test_match_by_exact_email_returns_deal_id(self):
        with session_scope(self.factory) as s:
            self._clean(s)
            _seed(s, contact_email="buyer@acme.com", deal_id="dm_d1")
            result = match_deal_by_email(s, "buyer@acme.com")
        self.assertEqual(result, "dm_d1")

    def test_case_insensitive_match(self):
        with session_scope(self.factory) as s:
            self._clean(s)
            _seed(s, contact_email="Buyer@ACME.COM", deal_id="dm_d2")
            result = match_deal_by_email(s, "buyer@acme.com")
        self.assertEqual(result, "dm_d2")

    def test_no_match_returns_none(self):
        with session_scope(self.factory) as s:
            self._clean(s)
            _seed(s, contact_email="other@corp.com", deal_id="dm_d3")
            result = match_deal_by_email(s, "nobody@example.com")
        self.assertIsNone(result)

    def test_closed_deal_not_returned(self):
        with session_scope(self.factory) as s:
            self._clean(s)
            _seed(s, contact_email="closed@co.com", deal_id="dm_d4", is_closed=True)
            result = match_deal_by_email(s, "closed@co.com")
        self.assertIsNone(result)

    def test_empty_email_returns_none(self):
        with session_scope(self.factory) as s:
            self._clean(s)
            result = match_deal_by_email(s, "")
        self.assertIsNone(result)

    def test_multiple_open_deals_returns_soonest_close_date(self):
        with session_scope(self.factory) as s:
            self._clean(s)
            # Two open deals for the same contact — contact shared via two link rows.
            contact = HubSpotContact(
                hubspot_contact_id="c_shared",
                first_name="Shared",
                last_name="Contact",
                email="shared@corp.com",
            )
            d_soon = HubSpotDeal(
                hubspot_deal_id="dm_soon",
                deal_name="Soon",
                deal_stage="open",
                amount_cents=0,
                is_closed=False,
                close_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
            )
            d_late = HubSpotDeal(
                hubspot_deal_id="dm_late",
                deal_name="Late",
                deal_stage="open",
                amount_cents=0,
                is_closed=False,
                close_date=datetime(2026, 12, 1, tzinfo=timezone.utc),
            )
            s.add(contact)
            s.add(d_soon)
            s.add(d_late)
            s.flush()
            s.add(HubSpotDealContact(hubspot_deal_id="dm_soon", hubspot_contact_id="c_shared"))
            s.add(HubSpotDealContact(hubspot_deal_id="dm_late", hubspot_contact_id="c_shared"))
            s.flush()
            result = match_deal_by_email(s, "shared@corp.com")
        self.assertEqual(result, "dm_soon")


if __name__ == "__main__":
    unittest.main()
