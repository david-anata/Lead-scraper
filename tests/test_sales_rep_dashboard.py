"""Tests for sales/rep_dashboard.py."""

from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone

from sales_support_agent.models.database import create_session_factory, init_database, session_scope
from sales_support_agent.models.entities import HubSpotDeal
from sales_support_agent.services.sales.rep_dashboard import (
    RepDashboard,
    RepMetrics,
    build_rep_dashboard,
    render_rep_dashboard_page,
)


def _deal(did, email, amount_cents, close_date_iso=None, is_closed=False, touch_dt=None):
    props = {
        "hubspot_deal_id": did,
        "deal_name": f"Deal {did}",
        "amount_cents": amount_cents,
        "deal_stage": "qualifiedtobuy",
        "deal_stage_label": "Qualified",
        "pipeline": "default",
        "owner_email": email,
        "is_closed": is_closed,
        "is_won": False,
    }
    if close_date_iso:
        props["close_date"] = datetime.fromisoformat(close_date_iso.replace("Z", "+00:00"))
    if touch_dt:
        props["last_meaningful_touch_at"] = touch_dt
    return props


class TestBuildRepDashboard(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sf = create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        init_database(cls.sf)

    def setUp(self):
        with session_scope(self.sf) as s:
            for row in s.query(HubSpotDeal).all():
                s.delete(row)
            s.flush()

    def _add_deal(self, did, email, amount_cents, close_date_iso=None, is_closed=False, touch_dt=None):
        with session_scope(self.sf) as s:
            d = HubSpotDeal(
                hubspot_deal_id=did,
                deal_name=f"Deal {did}",
                amount_cents=amount_cents,
                deal_stage="qualifiedtobuy",
                deal_stage_label="Qualified",
                pipeline="default",
                owner_email=email,
                is_closed=is_closed,
                is_won=False,
            )
            if close_date_iso:
                d.close_date = datetime.fromisoformat(close_date_iso.replace("Z", "+00:00"))
            if touch_dt:
                d.last_meaningful_touch_at = touch_dt
            s.add(d)

    def test_empty_dashboard(self):
        with session_scope(self.sf) as s:
            db = build_rep_dashboard(s)
        self.assertEqual(db.reps, [])
        self.assertEqual(db.total_open, 0)

    def test_groups_by_rep(self):
        self._add_deal("r1", "alice@ex.com", 10000)
        self._add_deal("r2", "alice@ex.com", 20000)
        self._add_deal("r3", "bob@ex.com", 15000)
        with session_scope(self.sf) as s:
            db = build_rep_dashboard(s)
        self.assertEqual(len(db.reps), 2)
        alice = next(r for r in db.reps if r.owner_email == "alice@ex.com")
        self.assertEqual(alice.open_deal_count, 2)
        self.assertEqual(alice.pipeline_cents, 30000)

    def test_overdue_count(self):
        now = datetime.now(timezone.utc)
        self._add_deal("od1", "carol@ex.com", 5000, close_date_iso="2020-01-01T00:00:00Z")
        self._add_deal("od2", "carol@ex.com", 5000, close_date_iso="2099-01-01T00:00:00Z")
        with session_scope(self.sf) as s:
            db = build_rep_dashboard(s, as_of=now)
        carol = next(r for r in db.reps if r.owner_email == "carol@ex.com")
        self.assertEqual(carol.overdue_count, 1)

    def test_stale_count(self):
        now = datetime.now(timezone.utc)
        fresh = now - timedelta(days=3)
        stale = now - timedelta(days=30)
        self._add_deal("st1", "dave@ex.com", 5000, touch_dt=fresh)
        self._add_deal("st2", "dave@ex.com", 5000, touch_dt=stale)
        self._add_deal("st3", "dave@ex.com", 5000)  # no touch at all = stale
        with session_scope(self.sf) as s:
            db = build_rep_dashboard(s, as_of=now, stale_days=14)
        dave = next(r for r in db.reps if r.owner_email == "dave@ex.com")
        self.assertEqual(dave.stale_count, 2)

    def test_no_amount_count(self):
        self._add_deal("na1", "eve@ex.com", 0)
        self._add_deal("na2", "eve@ex.com", 10000)
        with session_scope(self.sf) as s:
            db = build_rep_dashboard(s)
        eve = next(r for r in db.reps if r.owner_email == "eve@ex.com")
        self.assertEqual(eve.no_amount_count, 1)

    def test_closed_deals_excluded(self):
        self._add_deal("cl1", "frank@ex.com", 5000, is_closed=True)
        self._add_deal("op1", "frank@ex.com", 5000)
        with session_scope(self.sf) as s:
            db = build_rep_dashboard(s)
        frank = next(r for r in db.reps if r.owner_email == "frank@ex.com")
        self.assertEqual(frank.open_deal_count, 1)

    def test_health_score_perfect(self):
        now = datetime.now(timezone.utc)
        self._add_deal(
            "hs1", "gina@ex.com", 10000,
            close_date_iso="2099-01-01T00:00:00Z",
            touch_dt=now - timedelta(days=1),
        )
        with session_scope(self.sf) as s:
            db = build_rep_dashboard(s, as_of=now)
        gina = next(r for r in db.reps if r.owner_email == "gina@ex.com")
        self.assertEqual(gina.health_score, 100)

    def test_render_produces_html(self):
        self._add_deal("p1", "henry@ex.com", 50000)
        with session_scope(self.sf) as s:
            db = build_rep_dashboard(s)
        page = render_rep_dashboard_page(db)
        self.assertIn("Rep Accountability", page)
        self.assertIn("henry", page.lower())


if __name__ == "__main__":
    unittest.main()
