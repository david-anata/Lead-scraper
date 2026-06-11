"""End-to-end rate-sheet generation: fallback extraction -> mock rates ->
narrative + savings -> rendered HTML -> persisted DRAFT AutomationRun, then
the review/edit/publish lifecycle. No LLM key, no network, temp SQLite."""

from __future__ import annotations

import os
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
from sales_support_agent.services.fulfillment_deck import service as service_module
from sales_support_agent.services.fulfillment_deck import storage
from sales_support_agent.services.fulfillment_deck.service import (
    apply_profile_edits,
    generate_rate_sheet,
    rate_sheet_slug,
    rerender_rate_sheet,
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

# High enough that the blended mock rate is always below it, so the savings
# section reliably renders in tests.
_CURRENT_COST = 18.50


def _blended_from_matrix(matrix: dict) -> float:
    """Mean of the cheapest quote per zone across products (mirrors service)."""
    cheapest = []
    for product in matrix.get("products") or []:
        for zone in product.get("zones") or []:
            rates = [q["rate_usd"] for q in (zone.get("quotes") or [])]
            if rates:
                cheapest.append(min(rates))
    return sum(cheapest) / len(cheapest)


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

    def _generate_with_current_cost(self, current_cost: float = _CURRENT_COST, **overrides):
        """Generate with the prospect's $/parcel forced to a known value —
        deterministic regardless of how llm.py's fallback parser evolves."""
        real_extract = service_module.extract_prospect_profile

        def _wrapped(context, *_args, **_kwargs):
            # Drop attachments/kwargs — works against both the legacy and the
            # new extract_prospect_profile signatures.
            profile, meta = real_extract(context)
            profile = profile.__class__.from_dict(
                {
                    **profile.to_dict(),
                    "current_cost_per_parcel_usd": current_cost,
                    "monthly_order_volume": 3000,
                }
            )
            return profile, meta

        with mock.patch.object(service_module, "extract_prospect_profile", _wrapped):
            return self._generate(**overrides)

    # ------------------------------------------------------------------
    # Draft lifecycle
    # ------------------------------------------------------------------

    def test_full_pipeline_persists_draft_run(self) -> None:
        result = self._generate()
        run_id = result["run_id"]
        self.assertGreater(run_id, 0)

        run = storage.get_run(run_id)
        self.assertIsNotNone(run)
        self.assertEqual(run.status, "draft")
        summary = dict(run.summary_json or {})

        self.assertEqual(summary["rates_source"], "mock")
        self.assertEqual(summary["origin_zip"], "84043")
        self.assertRegex(summary["deck_slug"], r"^glowco-x-anata-rate-sheet-\d{4}-\d{2}-\d{2}-\d{4}$")
        self.assertEqual(len(summary["export_token"]), 32)
        self.assertEqual(
            summary["view_path"],
            f"/rate-sheets/{summary['deck_slug']}/{run_id}/{summary['export_token']}",
        )
        self.assertEqual(
            summary["review_path"], f"/admin/fulfillment/sales/runs/{run_id}/review"
        )
        self.assertIn("status_note", summary)
        self.assertIn("rate_matrix", summary)
        self.assertIn("zone_map", summary["sections_included"])
        self.assertIn("volume_economics", summary["sections_included"])
        self.assertIn("cost_comparison", summary["sections_included"])

    def test_publish_flips_draft_to_completed(self) -> None:
        result = self._generate()
        run_id = result["run_id"]
        self.assertTrue(storage.publish_run(run_id))
        run = storage.get_run(run_id)
        self.assertEqual(run.status, "completed")
        self.assertTrue(dict(run.summary_json)["published_at"])
        # Idempotent on already-published runs.
        self.assertTrue(storage.publish_run(run_id))
        # But not on failed/missing runs.
        self.assertFalse(storage.publish_run(999999))

    def test_list_runs_published_flag(self) -> None:
        result = self._generate()
        run_id = result["run_id"]
        row = next(r for r in storage.list_runs() if r["id"] == run_id)
        self.assertEqual(row["status"], "draft")
        self.assertFalse(row["published"])
        storage.publish_run(run_id)
        row = next(r for r in storage.list_runs() if r["id"] == run_id)
        self.assertTrue(row["published"])

    # ------------------------------------------------------------------
    # Narrative + savings
    # ------------------------------------------------------------------

    def test_narrative_present_even_without_api_key(self) -> None:
        result = self._generate()
        narrative = result["narrative"]
        self.assertTrue(narrative["executive_summary"].strip())
        self.assertTrue(narrative["bullets"])
        self.assertIn("Executive summary", result["deck_html"])
        self.assertIn("GlowCo", narrative["executive_summary"])

    def test_savings_math_when_cost_volume_and_dims_known(self) -> None:
        result = self._generate_with_current_cost()
        savings = result["savings"]
        self.assertIsNotNone(savings)
        blended = _blended_from_matrix(result["rate_matrix"])
        self.assertAlmostEqual(savings["anata_blended_per_parcel"], blended, places=2)
        self.assertEqual(savings["current_per_parcel"], _CURRENT_COST)
        self.assertEqual(savings["monthly_orders"], 3000)
        self.assertAlmostEqual(
            savings["monthly_savings"], (_CURRENT_COST - blended) * 3000, delta=0.05
        )
        self.assertAlmostEqual(savings["annual_savings"], savings["monthly_savings"] * 12, places=2)
        html = result["deck_html"]
        self.assertIn("Projected savings", html)
        self.assertIn("anata-sage", html)

    def test_savings_omitted_when_blended_not_below_current(self) -> None:
        result = self._generate_with_current_cost(current_cost=0.05)
        self.assertIsNone(result["savings"])
        self.assertTrue(any("savings section omitted" in w for w in result["warnings"]))
        self.assertNotIn("Projected savings", result["deck_html"])

    def test_savings_omitted_without_current_cost_or_volume(self) -> None:
        result = self._generate(notes="Brand: NoCostCo\nWidget — 6 x 5 x 3 in, 1.5 lb")
        self.assertIsNone(result["savings"])

    # ------------------------------------------------------------------
    # Edits + re-render
    # ------------------------------------------------------------------

    def test_apply_profile_edits_changes_dims_and_clears_estimated(self) -> None:
        result = self._generate()
        run_id = result["run_id"]
        # Mark the first stored product estimated, as if the LLM had guessed.
        summary = dict(storage.get_run(run_id).summary_json)
        profile = dict(summary["prospect_profile"])
        products = [dict(p) for p in profile["products"]]
        products[0]["dims_estimated"] = True
        profile["products"] = products
        storage.update_summary(run_id, {"prospect_profile": profile})
        rerendered = rerender_rate_sheet(run_id, settings=load_settings())
        self.assertIn("estimated — to be confirmed", rerendered["deck_html"])

        edited_products = [dict(p) for p in products]
        edited_products[0].update(
            {"length_in": 7.0, "width_in": 5.0, "height_in": 3.0, "dims_estimated": False}
        )
        updated = apply_profile_edits(
            run_id,
            {
                "brand": "GlowCo Updated",
                "products": edited_products,
                "monthly_order_volume": 4000,
            },
            settings=load_settings(),
        )
        self.assertEqual(updated["prospect"], "GlowCo Updated")
        self.assertIn("7 × 5 × 3 in", updated["deck_html"])
        self.assertNotIn("estimated — to be confirmed", updated["deck_html"])
        stored = dict(storage.get_run(run_id).summary_json)
        self.assertEqual(stored["prospect_profile"]["monthly_order_volume"], 4000)
        self.assertEqual(stored["prospect_profile"]["products"][0]["length_in"], 7.0)
        # Run is still a draft after edits.
        self.assertEqual(storage.get_run(run_id).status, "draft")

    def test_apply_profile_edits_deletes_absent_products(self) -> None:
        result = self._generate()
        run_id = result["run_id"]
        profile = dict(storage.get_run(run_id).summary_json)["prospect_profile"]
        kept = [dict(profile["products"][0])]
        updated = apply_profile_edits(run_id, {"products": kept}, settings=load_settings())
        self.assertEqual(len(updated["prospect_profile"]["products"]), 1)
        self.assertNotIn("Glow Kit", updated["deck_html"])

    def test_rerender_preserves_link_identity(self) -> None:
        result = self._generate()
        run_id = result["run_id"]
        rerendered = rerender_rate_sheet(run_id, settings=load_settings())
        self.assertEqual(rerendered["view_path"], result["view_path"])
        self.assertEqual(rerendered["export_token"], result["export_token"])
        self.assertEqual(rerendered["deck_slug"], result["deck_slug"])

    # ------------------------------------------------------------------
    # Existing behaviour kept
    # ------------------------------------------------------------------

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
