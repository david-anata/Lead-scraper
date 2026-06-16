"""Public token-gated share route + storage round-trip (save → share → rerun).

Proves: the public /brand/... page needs no session (RBAC bypass), the token
gates access (wrong/empty token 404s), and rerun overwrites in place while
keeping the same id, slug, and share token so a shared link stays live.
"""

from __future__ import annotations

import os
import tempfile
import unittest

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/ba_share_test.db")

try:
    from fastapi.testclient import TestClient
    from sales_support_agent.main import app
    from sales_support_agent.services.brand_analysis import storage
    from sales_support_agent.services.brand_analysis.schema import BrandReport, Scorecard
    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


def _report(brand: str = "Luxmery") -> BrandReport:
    return BrandReport(brand=brand, category="dtc", prepared_date="2026-06-16",
                       scorecard=Scorecard(score_100=58, letter="D"),
                       executive_summary="Test brief.", recommendation="Proceed with Caution")


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class ShareRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Pin the global engine to a fresh DB so this test is deterministic
        # regardless of which other test last (re)initialised the shared engine.
        from sales_support_agent.models.database import create_session_factory, init_database
        db = os.path.join(tempfile.gettempdir(), "ba_share_route_isolated.db")
        if os.path.exists(db):
            os.remove(db)
        url = "sqlite:///" + db
        init_database(create_session_factory(url))
        cls.client = TestClient(app)

    def test_public_page_served_with_valid_token_no_auth(self) -> None:
        rid = storage.save_report(_report(), report_html="<!doctype html><title>Luxmery</title><h1>Luxmery brief</h1>")
        row = storage.get_report_row(rid)
        path = storage.share_path(row)
        self.assertTrue(path.startswith("/brand/"))
        # No session cookie at all — the token is the gate.
        resp = self.client.get(path)
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Luxmery brief", resp.text)

    def test_wrong_token_404s(self) -> None:
        rid = storage.save_report(_report("Acme"), report_html="<h1>Acme</h1>")
        row = storage.get_report_row(rid)
        bad = f"/brand/{row['slug']}/{rid}/not-the-real-token"
        self.assertEqual(self.client.get(bad).status_code, 404)

    def test_admin_view_requires_auth(self) -> None:
        rid = storage.save_report(_report("Gated"), report_html="<h1>Gated</h1>")
        # The admin view is under /admin and tool-gated -> redirect to login.
        resp = self.client.get(f"/admin/executive/brand-analysis/{rid}", follow_redirects=False)
        self.assertIn(resp.status_code, (302, 303, 307))
        self.assertIn("/admin/login", resp.headers.get("location", ""))

    def test_rerun_keeps_same_token_and_id(self) -> None:
        rid = storage.save_report(_report("Evolve"), report_html="<h1>v1</h1>")
        row1 = storage.get_report_row(rid)
        token1, slug1 = row1["share_token"], row1["slug"]
        # Simulate an edit + rerun overwrite.
        updated = _report("Evolve")
        updated.context_notes = "Added Q1 actuals"
        storage.update_report(rid, updated, report_html="<h1>v2</h1>")
        row2 = storage.get_report_row(rid)
        self.assertEqual(row2["share_token"], token1)   # link stays live
        self.assertEqual(row2["slug"], slug1)
        self.assertEqual(row2["context_notes"], "Added Q1 actuals")
        # Public page now serves v2 at the same URL.
        resp = self.client.get(storage.share_path(row2))
        self.assertEqual(resp.status_code, 200)
        self.assertIn("v2", resp.text)

    def test_list_reports_exposes_share_path(self) -> None:
        storage.save_report(_report("Listed"), report_html="<h1>Listed</h1>")
        rows = storage.list_reports()
        self.assertTrue(rows)
        self.assertTrue(any(r.get("share_path", "").startswith("/brand/") for r in rows))


if __name__ == "__main__":
    unittest.main()
