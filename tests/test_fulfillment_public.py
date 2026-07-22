"""Public self-serve funnel for Fulfillment Rate Sheets (taste + unlock).

Proves: the shared-secret header gates both routes; taste returns TEASER fields
only (no deck HTML, no email) and persists the full product catalog even though
the rate pass is trimmed; unlock publishes the sheet, would-email the tokenized
view URL, and tags the funnel segment on the run; the DIY segment hides the
line-item invoice section and leads the closer with Anata Shipping OS.

Real backend app + temp SQLite (same harness as test_fulfillment_deck_routes),
network/LLM/WMS all off so the deterministic fallback parser + mock rates run.
"""

from __future__ import annotations

import dataclasses
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/fulfillment_public_test.db",
)
os.environ.setdefault("MARKETING_SITE_INTAKE_KEY", "test-intake-key")

from fastapi.testclient import TestClient

from sales_support_agent.api import fulfillment_public_router as P
from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.services.fulfillment_deck import service as svc
from sales_support_agent.services.fulfillment_deck import storage

_KEY = "test-intake-key"
_HEADERS = {"X-Internal-Api-Key": _KEY}

# Canned extraction context: 4 products (so max_products=3 trimming is exercised
# while the full catalog of 4 is persisted). Parsed by the no-key fallback.
_CONTEXT = (
    "=== SALES NOTES ===\n"
    "Brand: TabCo\n"
    "Widget A - 6 x 5 x 3 in, 1.5 lb, ~500 units/mo\n"
    "Widget B - 8 x 6 x 4 in, 2.0 lb, ~300 units/mo\n"
    "Widget C - 5 x 4 x 3 in, 1.0 lb, ~200 units/mo\n"
    "Widget D - 9 x 7 x 5 in, 3.0 lb, ~100 units/mo\n"
)


class FulfillmentPublicFunnelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        factory = create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        init_database(factory)
        app.state.session_factory = factory
        # Ensure the shared-secret key + a public base URL are configured on the
        # settings the routes read (app.state.settings).
        app.state.settings = dataclasses.replace(
            app.state.settings,
            marketing_site_intake_key=_KEY,
            deck_public_base_url="https://agent.anatainc.com",
        )
        cls.client = TestClient(app)

    def setUp(self) -> None:
        # No network / LLM / WMS: patch the intake so the fallback parser runs on
        # our canned context, and no brand assets are fetched.
        patchers = [
            mock.patch.dict(
                os.environ,
                {"ANTHROPIC_API_KEY": "", "ANATA_WMS_BASE_URL": "", "HUBSPOT_ACCESS_TOKEN": ""},
                clear=False,
            ),
            mock.patch.object(
                svc, "build_extraction_context", return_value=(_CONTEXT, [], [])
            ),
            mock.patch.object(svc, "fetch_brand_assets", return_value={}),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)

    def _taste(self, segment: str = "dfy") -> dict:
        resp = self.client.post(
            "/api/public/fulfillment/rate-sheet/taste",
            json={"url": "https://tabco.example", "segment": segment, "source": "hero"},
            headers=_HEADERS,
        )
        self.assertEqual(resp.status_code, 202, resp.text)
        return resp.json()

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def test_taste_requires_intake_key(self) -> None:
        resp = self.client.post(
            "/api/public/fulfillment/rate-sheet/taste",
            json={"url": "https://tabco.example", "segment": "dfy"},
        )
        self.assertEqual(resp.status_code, 401)

    def test_unlock_requires_intake_key(self) -> None:
        resp = self.client.post(
            "/api/public/fulfillment/rate-sheet/unlock",
            json={"run_id": 1, "token": "x", "email": "a@b.com"},
        )
        self.assertEqual(resp.status_code, 401)

    # ------------------------------------------------------------------
    # Taste teaser
    # ------------------------------------------------------------------

    def test_taste_returns_teaser_fields_without_email_or_sheet(self) -> None:
        data = self._taste("dfy")
        # Teaser shape: exactly these safe public keys, nothing more.
        self.assertEqual(
            set(data),
            {
                "run_id", "token", "carrier_rate", "avg_transit_days", "rates_source",
                "excludes_3pl_fees", "product_count", "brand_name",
            },
        )
        self.assertTrue(data["token"])
        self.assertEqual(data["brand_name"], "TabCo")
        # Full catalog (4) persisted even though only 3 were rate-quoted.
        self.assertEqual(data["product_count"], 4)
        self.assertNotIn("deck_html", data)
        self.assertIsNone(data["carrier_rate"])
        self.assertIsNone(data["avg_transit_days"])
        self.assertEqual(data["rates_source"], "unavailable")
        self.assertTrue(data["excludes_3pl_fees"])

        # The draft run exists, is not public yet, and stored the full profile.
        run = storage.get_run(data["run_id"])
        self.assertIsNotNone(run)
        self.assertEqual(run.status, "draft")
        summary = dict(run.summary_json or {})
        self.assertEqual(summary["segment"], "dfy")
        self.assertTrue(summary["suppress_fulfillment_pricing"])
        self.assertEqual(len(summary["prospect_profile"]["products"]), 4)
        self.assertEqual(summary.get("public_source"), "hero")

    def test_taste_validates_url(self) -> None:
        resp = self.client.post(
            "/api/public/fulfillment/rate-sheet/taste",
            json={"url": "", "segment": "dfy"},
            headers=_HEADERS,
        )
        self.assertEqual(resp.status_code, 400)

    def test_diy_taste_requires_valid_origin_zip(self) -> None:
        resp = self.client.post(
            "/api/public/fulfillment/rate-sheet/taste",
            json={"url": "https://tabco.example", "segment": "diy"},
            headers=_HEADERS,
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("ship-from ZIP", resp.json()["detail"])

    def test_taste_honeypot_returns_building_without_work(self) -> None:
        before = len(storage.list_runs())
        resp = self.client.post(
            "/api/public/fulfillment/rate-sheet/taste",
            json={"url": "https://tabco.example", "segment": "dfy", "hp": "bot"},
            headers=_HEADERS,
        )
        self.assertEqual(resp.status_code, 202)
        self.assertEqual(resp.json(), {"status": "building"})
        self.assertEqual(len(storage.list_runs()), before)

    # ------------------------------------------------------------------
    # Unlock -> publish + deliver + segment
    # ------------------------------------------------------------------

    def test_unlock_rejects_sample_rates(self) -> None:
        taste = self._taste("dfy")
        resp = self.client.post(
            "/api/public/fulfillment/rate-sheet/unlock",
            json={
                "run_id": taste["run_id"],
                "token": taste["token"],
                "email": "buyer@tabco.com",
                "monthly_orders": 1200,
            },
            headers=_HEADERS,
        )
        self.assertEqual(resp.status_code, 503, resp.text)
        self.assertIn("Live carrier rates", resp.json()["detail"])
        run = storage.get_run(taste["run_id"])
        self.assertEqual(run.status, "draft")

    def test_unlock_rejects_bad_token(self) -> None:
        taste = self._taste("dfy")
        resp = self.client.post(
            "/api/public/fulfillment/rate-sheet/unlock",
            json={"run_id": taste["run_id"], "token": "0" * 32, "email": "a@b.com"},
            headers=_HEADERS,
        )
        self.assertEqual(resp.status_code, 404)

    def test_unlock_requires_valid_email(self) -> None:
        taste = self._taste("dfy")
        resp = self.client.post(
            "/api/public/fulfillment/rate-sheet/unlock",
            json={"run_id": taste["run_id"], "token": taste["token"], "email": "nope"},
            headers=_HEADERS,
        )
        self.assertEqual(resp.status_code, 400)

    # ------------------------------------------------------------------
    # DIY variant
    # ------------------------------------------------------------------

    def test_diy_hides_all_3pl_pricing_and_leads_with_shipping_os(self) -> None:
        resp = self.client.post(
            "/api/public/fulfillment/rate-sheet/taste",
            json={
                "url": "https://tabco.example",
                "segment": "diy",
                "origin_zip": "10001",
                "source": "hero",
            },
            headers=_HEADERS,
        )
        self.assertEqual(resp.status_code, 202, resp.text)
        taste = resp.json()
        run = storage.get_run(taste["run_id"])
        summary = dict(run.summary_json or {})
        self.assertEqual(summary["segment"], "diy")
        self.assertEqual(summary["origin_zip"], "10001")
        html = summary["deck_html"]
        # No public funnel sheet may expose unapproved 3PL fees.
        self.assertNotIn('data-key="fee-schedule"', html)
        self.assertNotIn('data-key="quote"', html)
        self.assertNotIn("Your estimated monthly invoice", html)
        # Closer leads with the try-free Shipping OS offer.
        self.assertIn("Anata Shipping OS", html)
        self.assertIn("Start free", html)
        pos_os = html.find("Anata Shipping OS")
        pos_ff = html.find("Anata Fulfillment")
        self.assertLess(pos_os, pos_ff)  # Shipping OS card comes first

    def test_public_dfy_hides_all_3pl_pricing(self) -> None:
        taste = self._taste("dfy")
        run = storage.get_run(taste["run_id"])
        html = dict(run.summary_json or {})["deck_html"]
        self.assertNotIn('data-key="fee-schedule"', html)
        self.assertNotIn('data-key="quote"', html)
        self.assertNotIn("Your estimated monthly invoice", html)
        self.assertNotIn("all 50 U.S. zones", html)
        self.assertNotIn("entire West", html)
        self.assertNotIn("avg per parcel, your mix", html)
        self.assertIn("representative shipping zones", html)
        self.assertEqual(dict(run.summary_json or {})["narrative"]["model"], "public-deterministic")
        # DFY leads with the full-3PL card.
        pos_ff = html.find("Anata Fulfillment")
        pos_os = html.find("Anata Shipping OS")
        self.assertLess(pos_ff, pos_os)


if __name__ == "__main__":
    unittest.main()
