"""The Amazon surfaces on the outbound pages.

These cover the three things a green unit suite cannot: that the new page
actually mounts, that its numbers reconcile on screen, and that simply reading
the report never spends money at the data provider.
"""

from __future__ import annotations

import os
import tempfile
import unittest

os.environ.setdefault(
    "SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/outbound_router_amazon_test.db"
)

try:
    from fastapi.testclient import TestClient
    from sales_support_agent.main import app
    from sales_support_agent.services.admin_auth import create_user_session_token
    from sales_support_agent.api import outbound_router as orr
    DEPS = True
except ModuleNotFoundError as exc:  # pragma: no cover
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


def _lead(**over):
    base = {
        "domain": "rho.com",
        "brand": "Rho Nutrition",
        "reason": "There are a handful of other sellers sitting on your NAD+ listing. Are those all authorized?",
        "amazon_checked_at": "2026-07-26T12:00:00+00:00",
        "amazon_marketplace": "amazon.com",
        "amazon_confidence": "high",
        "amazon_absent": False,
        "amazon_sellers_unknown": 18,
        "amazon_skipped_reason": "",
    }
    base.update(over)
    return base


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class ScanSummaryTests(unittest.TestCase):
    """The panel exists to make a silently dropped brand impossible to miss."""

    def test_every_scanned_brand_lands_in_exactly_one_bucket(self):
        leads = [
            _lead(domain="a.com"),
            _lead(domain="b.com", amazon_absent=True, amazon_sellers_unknown=0),
            _lead(domain="c.com", amazon_skipped_reason="no confident match", reason=""),
        ]
        s = orr._amazon_scan_summary(leads)
        self.assertEqual(s["scanned"], 3)
        self.assertEqual(s["findings"] + s["skipped"] + s["absent"], s["scanned"])

    def test_a_brand_never_checked_is_not_counted_as_scanned(self):
        """Otherwise the panel claims coverage the scan never had."""
        s = orr._amazon_scan_summary([_lead(amazon_checked_at="")])
        self.assertEqual(s["scanned"], 0)

    def test_marketplace_split_is_reported(self):
        s = orr._amazon_scan_summary([_lead(domain="a.com"), _lead(domain="b.com")])
        self.assertEqual(s["markets"].get("amazon.com"), 2)


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class ErosionTests(unittest.TestCase):
    def test_measured_against_the_brands_own_price_not_the_list_price(self):
        """The real Rho numbers. Against the struck-through 55.95 this reads as
        14 percent, which overstates the problem three times over and is checkable
        by the recipient in seconds."""
        pct = orr._erosion_pct(50.18, 48.00)
        self.assertIsNotNone(pct)
        self.assertAlmostEqual(pct, 4.34, places=1)
        self.assertLess(pct, 6.0)

    def test_no_erosion_when_nobody_undercuts(self):
        self.assertIsNone(orr._erosion_pct(50.18, 50.18))
        self.assertIsNone(orr._erosion_pct(50.18, 55.00))

    def test_junk_prices_do_not_produce_a_number(self):
        self.assertIsNone(orr._erosion_pct(None, 48.0))
        self.assertIsNone(orr._erosion_pct("50.18", 48.0))
        self.assertIsNone(orr._erosion_pct(True, 48.0))


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class OpeningLineCellTests(unittest.TestCase):
    def test_a_skipped_brand_keeps_its_row_and_says_why(self):
        """A brand that disappears from the list looks like a brand we covered."""
        cell = orr._opening_line_cell(_lead(amazon_skipped_reason="no confident match", reason=""))
        self.assertIn("Skipped", cell)
        self.assertIn("no confident match", cell)

    def test_unchecked_brand_says_so_rather_than_looking_empty(self):
        self.assertIn("Not checked", orr._opening_line_cell(_lead(amazon_checked_at="")))

    def test_the_line_is_escaped(self):
        cell = orr._opening_line_cell(_lead(reason="<script>alert(1)</script>"))
        self.assertNotIn("<script>", cell)


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class LeakReportRouteTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def _as_admin(self, path):
        s = app.state.agent_settings
        token = create_user_session_token(s, email="david@anatainc.com", name="David", role="admin")
        self.client.cookies.set(s.admin_cookie_name, token)
        try:
            return self.client.get(path, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def test_report_requires_login(self):
        r = self.client.get("/admin/outbound/leak-report/rho.com", follow_redirects=False)
        self.assertIn(r.status_code, (302, 303, 307))

    def test_unknown_brand_renders_an_empty_state_not_an_error(self):
        """A typo in the domain must not 500 a page we may show a prospect."""
        r = self._as_admin("/admin/outbound/leak-report/definitely-not-a-lead.com")
        self.assertEqual(r.status_code, 200)
        self.assertIn("no record", r.text.lower())

    def test_reading_the_report_does_not_call_amazon(self):
        """Every page view would otherwise bill the data provider. The live
        re-check is opt in via refresh."""
        import outbound_amazon as oa
        calls: list[int] = []
        original = oa.brand_control
        oa.brand_control = lambda *a, **k: calls.append(1) or {}
        try:
            r = self._as_admin("/admin/outbound/leak-report/rho.com")
            self.assertEqual(r.status_code, 200)
            self.assertEqual(calls, [], "the report re-checked Amazon without being asked")
        finally:
            oa.brand_control = original


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class OutboundPagesStillRenderTests(unittest.TestCase):
    """The Amazon work touched shared pages. They must still come up."""

    def setUp(self):
        self.client = TestClient(app)

    def _as_admin(self, path):
        s = app.state.agent_settings
        token = create_user_session_token(s, email="david@anatainc.com", name="David", role="admin")
        self.client.cookies.set(s.admin_cookie_name, token)
        try:
            return self.client.get(path, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def test_lead_ops_renders(self):
        r = self._as_admin("/admin/outbound/lead-ops")
        self.assertEqual(r.status_code, 307)
        self.assertEqual(r.headers["location"], "/admin/outbound/daily")

    def test_recipes_page_renders(self):
        r = self._as_admin("/admin/outbound/recipes")
        self.assertEqual(r.status_code, 200)
        self.assertIn("Recipes &amp; ICP", r.text)
        self.assertIn("Daily pull list", r.text)
        self.assertIn("method='post' action='/admin/api/outbound/brands.csv?recipe=new_growth_app'", r.text)

    def test_brands_page_renders(self):
        r = self._as_admin("/admin/outbound/brands")
        self.assertEqual(r.status_code, 307)
        self.assertEqual(r.headers["location"], "/admin/outbound/daily")

    def test_leads_page_renders(self):
        r = self._as_admin("/admin/outbound/leads")
        self.assertEqual(r.status_code, 200)

    def test_daily_leads_page_renders_with_clear_process(self):
        r = self._as_admin("/admin/outbound/daily")
        self.assertEqual(r.status_code, 200)
        self.assertIn("Daily Leads", r.text)
        self.assertIn("Weekdays · 7:00 AM Denver", r.text)
        self.assertIn("download one combined CSV", r.text)


if __name__ == "__main__":
    unittest.main()
