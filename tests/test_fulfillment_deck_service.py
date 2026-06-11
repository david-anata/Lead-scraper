"""End-to-end rate-sheet generation: fallback extraction -> mock rates ->
rendered HTML -> persisted AutomationRun. No LLM key, no network, temp SQLite."""

from __future__ import annotations

import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_DB_PATH = tempfile.gettempdir() + "/fulfillment_deck_service_test.db"
os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + _DB_PATH)

from sales_support_agent.config import load_settings
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.services.fulfillment_deck import storage
from sales_support_agent.services.fulfillment_deck.service import (
    generate_rate_sheet,
    rate_sheet_slug,
)

_NOTES = """
Company: GlowCo Labs
Brand: GlowCo
Website: https://glowco.example
Spoke with Sarah. Monthly orders: 3000
Destinations: mostly West Coast
Current costs: paying about $9.80/parcel with UPS
Products:
Super Serum — 4 x 4 x 6 in, 1.2 lb, ~2000 units/mo
Glow Kit — 10 x 8 x 4 in, 2.5 lb
"""


class RateSheetServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        init_database(create_session_factory(os.environ["SALES_AGENT_DB_URL"]))

    def setUp(self) -> None:
        # Force fallback extraction + mock WMS regardless of the dev machine env.
        patcher = mock.patch.dict(
            os.environ, {"ANTHROPIC_API_KEY": "", "ANATA_WMS_BASE_URL": ""}, clear=False
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _generate(self, **overrides):
        kwargs = dict(
            settings=load_settings(),
            notes=_NOTES,
            files=[],
            website_url="",
            origin_zip="84043",
        )
        kwargs.update(overrides)
        return generate_rate_sheet(**kwargs)

    def test_full_pipeline_persists_completed_run(self) -> None:
        result = self._generate()
        run_id = result["run_id"]
        self.assertGreater(run_id, 0)

        run = storage.get_run(run_id)
        self.assertIsNotNone(run)
        self.assertEqual(run.status, "completed")
        summary = dict(run.summary_json or {})

        self.assertEqual(summary["rates_source"], "mock")
        self.assertEqual(summary["origin_zip"], "84043")
        self.assertRegex(summary["deck_slug"], r"^glowco-x-anata-rate-sheet-\d{4}-\d{2}-\d{2}-\d{4}$")
        self.assertEqual(len(summary["export_token"]), 32)
        self.assertEqual(
            summary["view_path"],
            f"/rate-sheets/{summary['deck_slug']}/{run_id}/{summary['export_token']}",
        )
        self.assertIn("rate_matrix", summary)
        self.assertIn("zone_map", summary["sections_included"])
        self.assertIn("volume_economics", summary["sections_included"])
        self.assertIn("cost_comparison", summary["sections_included"])

    def test_rendered_html_has_tabs_zone_map_print_and_sample_badge(self) -> None:
        result = self._generate()
        html = result["deck_html"]
        # Both products as labeled tabs.
        self.assertIn("Super Serum", html)
        self.assertIn("Glow Kit", html)
        self.assertIn('data-off="prod-0"', html)
        self.assertIn('data-off="prod-1"', html)
        # Zone map SVG + legend, print rail, sample badge, heartbeat JS.
        self.assertIn("US shipping zones from ZIP 84043", html)
        self.assertIn("window.print()", html)
        self.assertIn("Sample rates", html)
        self.assertIn("/heartbeat", html)
        # Print rule expands tab panes.
        self.assertIn(".off-pane.rate-pane[hidden]", html)

    def test_brand_override_and_bad_origin_fall_back(self) -> None:
        result = self._generate(brand_override="Acme Co", origin_zip="abc")
        self.assertEqual(result["prospect"], "Acme Co")
        self.assertEqual(result["origin_zip"], "84043")
        self.assertTrue(any("Origin ZIP" in w for w in result["warnings"]))

    def test_no_products_drops_rate_sections(self) -> None:
        result = self._generate(notes="Company: MysteryCo\nNo product info yet.")
        self.assertNotIn("rate_matrix", result["sections_included"])
        self.assertNotIn("zone_map", result["sections_included"])
        self.assertIn("cover", result["sections_included"])
        self.assertIn("about_anata", result["sections_included"])

    def test_history_listing_and_delete(self) -> None:
        result = self._generate()
        run_id = result["run_id"]
        rows = storage.list_runs()
        self.assertTrue(any(r["id"] == run_id for r in rows))
        listed = next(r for r in rows if r["id"] == run_id)
        self.assertEqual(listed["prospect"], "GlowCo")
        self.assertEqual(listed["rates_source"], "mock")
        self.assertTrue(storage.delete_run(run_id))
        self.assertFalse(any(r["id"] == run_id for r in storage.list_runs()))
        self.assertFalse(storage.delete_run(run_id))

    def test_slug_helper_handles_blank_brand(self) -> None:
        self.assertTrue(rate_sheet_slug("").startswith("prospect-x-anata-rate-sheet-"))


if __name__ == "__main__":
    unittest.main()
