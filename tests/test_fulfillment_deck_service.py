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
from sales_support_agent.services.fulfillment_deck.schema import (
    ProductRates,
    ProductSpec,
    ProspectProfile,
    RateMatrix,
    RateQuote,
    ZoneRates,
)
from sales_support_agent.services.fulfillment_deck.service import (
    BLEND_METHOD_FLAT,
    BLEND_METHOD_WEIGHTED,
    _blended_rate,
    apply_profile_edits,
    apply_viewer_requote,
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
    """Units-weighted blend of per-product flat zone means (mirrors the
    service's _blended_rate for profiles with no parseable state mix)."""
    blends: list[tuple[float, int]] = []
    for product in matrix.get("products") or []:
        cheapest = []
        for zone in product.get("zones") or []:
            rates = [q["rate_usd"] for q in (zone.get("quotes") or [])]
            if rates:
                cheapest.append(min(rates))
        if cheapest:
            units = (product.get("product") or {}).get("monthly_units") or 0
            blends.append((sum(cheapest) / len(cheapest), units))
    if any(units for _b, units in blends):
        total = sum(units for _b, units in blends)
        return sum(blend * units for blend, units in blends) / total
    return sum(blend for blend, _u in blends) / len(blends)


def _quoted_zone(zone: int, rate: float, carrier: str = "USPS") -> ZoneRates:
    return ZoneRates(
        zone=zone,
        dest_zip="00000",
        dest_label="Testville, TS",
        quotes=(RateQuote(carrier=carrier, service="Ground", rate_usd=rate, zone=zone),),
    )


def _spec(name: str, units=None) -> ProductSpec:
    return ProductSpec(
        name=name, length_in=4.0, width_in=4.0, height_in=4.0, weight_lb=1.0,
        monthly_units=units,
    )


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
        html = result["deck_html"]
        # The narrative leads the hero as left-aligned body copy, with the
        # bullets rendered exactly once as a compact list.
        self.assertIn('class="hero-narrative"', html)
        self.assertIn('class="stat-strip"', html)
        self.assertEqual(html.count('class="hero-bullets"'), 1)
        self.assertIn("GlowCo", narrative["executive_summary"])
        # Old standalone sections are gone.
        self.assertNotIn("Why this works", html)
        self.assertNotIn("Executive summary", html)
        self.assertNotIn("Your context", html)
        self.assertNotIn("Why Anata", html)
        # The hero context line replaces the "Your context" tiles.
        self.assertIn("Today: paying about $9.80/parcel with UPS", html)
        self.assertIn("Destinations: mostly West Coast", html)

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
        # "mostly West Coast" parses no state codes -> flat zone average.
        self.assertEqual(savings["blend_method"], BLEND_METHOD_FLAT)
        self.assertEqual(result["blend_method"], BLEND_METHOD_FLAT)
        html = result["deck_html"]
        # Volume + savings merged into the single monthly-math section.
        self.assertIn("What this means monthly", html)
        self.assertIn('data-key="monthly-math"', html)
        self.assertNotIn("Projected savings", html)
        self.assertNotIn("Volume economics", html)
        self.assertIn("anata-sage", html)
        self.assertIn(f"Blended best-rate average, {BLEND_METHOD_FLAT}", html)

    def test_savings_omitted_when_blended_not_below_current(self) -> None:
        result = self._generate_with_current_cost(current_cost=0.05)
        self.assertIsNone(result["savings"])
        self.assertTrue(any("savings section omitted" in w for w in result["warnings"]))
        self.assertNotIn("Projected savings", result["deck_html"])
        self.assertNotIn("annual savings", result["deck_html"])
        # The monthly-math section still renders the volume economics.
        self.assertIn("What this means monthly", result["deck_html"])

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

    def test_apply_viewer_requote_persists_without_llm_call(self) -> None:
        result = self._generate()
        run_id = result["run_id"]
        # Mark the first stored product estimated, as if the LLM had guessed.
        summary = dict(storage.get_run(run_id).summary_json)
        profile = dict(summary["prospect_profile"])
        products = [dict(p) for p in profile["products"]]
        products[0]["dims_estimated"] = True
        profile["products"] = products
        storage.update_summary(run_id, {"prospect_profile": profile})

        posted = [
            ProductSpec.from_dict(
                {"name": "Super Serum", "length_in": 10, "width_in": 8,
                 "height_in": 6, "weight_lb": 4.0}
            )
        ]
        # Viewer edits must never trigger an LLM narrative call.
        with mock.patch.object(
            service_module.llm_module, "generate_narrative",
            side_effect=AssertionError("LLM called on viewer requote"),
        ):
            patch = apply_viewer_requote(
                run_id, posted, "84043", settings=load_settings()
            )

        self.assertEqual(patch["run_id"], run_id)
        stored = dict(storage.get_run(run_id).summary_json)
        serum = next(
            p for p in stored["prospect_profile"]["products"] if p["name"] == "Super Serum"
        )
        self.assertEqual(serum["length_in"], 10.0)
        self.assertEqual(serum["weight_lb"], 4.0)
        self.assertFalse(serum["dims_estimated"])
        # Product not posted keeps its stored spec.
        kit = next(
            p for p in stored["prospect_profile"]["products"] if p["name"] == "Glow Kit"
        )
        self.assertEqual(kit["length_in"], 10.0)
        self.assertEqual(kit["weight_lb"], 2.5)
        # Re-rendered + persisted HTML reflects the edit at the same link.
        self.assertIn("10 × 8 × 6 in", stored["deck_html"])
        self.assertEqual(stored["view_path"], result["view_path"])
        self.assertIn(f"{result['view_path']}/requote", stored["deck_html"])
        self.assertTrue(stored["narrative"]["executive_summary"].strip())
        self.assertIn(stored["narrative"]["model"], ("none", "fallback"))

    def test_apply_viewer_requote_ignores_unknown_products(self) -> None:
        result = self._generate()
        run_id = result["run_id"]
        posted = [
            ProductSpec.from_dict(
                {"name": "Injected Product", "length_in": 5, "width_in": 5,
                 "height_in": 5, "weight_lb": 1.0}
            )
        ]
        apply_viewer_requote(run_id, posted, "84043", settings=load_settings())
        stored = dict(storage.get_run(run_id).summary_json)
        names = [p["name"] for p in stored["prospect_profile"]["products"]]
        self.assertNotIn("Injected Product", names)
        self.assertEqual(names, ["Super Serum", "Glow Kit"])

    def test_data_keys_and_carrier_grouped_table(self) -> None:
        result = self._generate_with_current_cost()
        html = result["deck_html"]
        for key in ("hero", "rate-map", "carrier-rates", "monthly-math", "quote", "partner"):
            self.assertIn(f'data-key="{key}"', html)
        for gone in ("volume-economics", "savings"):
            self.assertNotIn(f'data-key="{gone}"', html)
        # v4: the estimated invoice sits immediately before the partner closer.
        self.assertLess(html.index('data-key="quote"'), html.index('data-key="partner"'))
        self.assertLess(html.index('data-key="monthly-math"'), html.index('data-key="quote"'))
        # Carrier-grouped columns: the header is a brand-colored logo chip
        # tagged with data-carrier for the viewer-side filter; the cheaper
        # service per carrier wins, so mock USPS Priority Mail is out.
        for carrier in ("USPS", "UPS", "FedEx"):
            self.assertIn(f'<th data-carrier="{carrier}">', html)
        self.assertIn("#004B87", html)  # USPS brand blue chip
        self.assertNotIn("Priority Mail", html)
        self.assertIn("Ground Advantage", html)
        # Price / transit badge / muted service line cell anatomy.
        self.assertIn('class="rc-price"', html)
        self.assertIn('class="rc-transit"', html)
        self.assertIn('class="rc-service"', html)
        # Explicit request button replaces the auto-debounce flow.
        self.assertIn("Request rates", html)
        self.assertIn("rm-overlay", html)
        self.assertNotIn("debounceTimer", html)

    def test_carrier_filter_chips_render_once_per_carrier(self) -> None:
        result = self._generate()
        html = result["deck_html"]
        self.assertIn('id="carrier-filter"', html)
        for carrier in ("USPS", "UPS", "FedEx"):
            chip = f'class="cf-chip" data-carrier="{carrier}"'
            self.assertEqual(html.count(chip), 1, chip)
        # All enabled by default; filtering is viewer-local JS only.
        self.assertIn('aria-pressed="true"', html)
        self.assertNotIn('class="cf-chip cf-off"', html)
        # Map payload ships per-carrier zone rates for the filter to re-min.
        self.assertIn('"zoneRates": {"1": {"', html)
        self.assertIn("applyCarrierFilter", html)
        # v4: the chips live INSIDE the map section's controls (the map
        # renders before the table), not in the carrier-rates section.
        chips_at = html.index('id="carrier-filter"')
        self.assertGreater(chips_at, html.index('data-key="rate-map"'))
        self.assertLess(chips_at, html.index('data-key="carrier-rates"'))
        self.assertIn('id="rm-controls"', html[:chips_at])

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
        # Interactive ZIP-level rate map: ~900 zip3 cells with distance data,
        # mileage rings, hover tooltip, live requote wired to this sheet's
        # public token URL.
        self.assertIn('data-p="841"', html)   # Salt Lake City prefix
        self.assertIn('data-p="100"', html)   # Manhattan prefix
        self.assertIn('data-p="995"', html)   # Anchorage (AK inset)
        self.assertGreater(html.count('class="rm-cell"'), 850)
        self.assertIn('class="rm-ring"', html)
        self.assertIn("1800 mi", html)
        self.assertIn("rm-tooltip", html)
        self.assertIn(f"{result['view_path']}/requote", html)
        # Map comes BEFORE the carrier-rate tables in document order (v3:
        # hero -> rate map -> carrier rates).
        self.assertLess(html.index("What shipping costs, anywhere in the US"),
                        html.index("Your rates, by product and zone"))
        self.assertLess(html.index('data-key="hero"'), html.index('data-key="rate-map"'))
        self.assertIn("window.print()", html)
        self.assertIn("Sample rates", html)
        self.assertIn("/heartbeat", html)
        # Print rules: tab panes expand, map edit controls hide.
        self.assertIn(".off-pane.rate-pane[hidden]", html)
        self.assertIn(".rm-controls { display: none !important; }", html)

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

    # ------------------------------------------------------------------
    # Blended-rate math (v3)
    # ------------------------------------------------------------------

    def test_blended_rate_weights_zones_by_destination_states(self) -> None:
        # From 84043 (Lehi): CA -> zone 4, TX -> zone 5, NY -> zone 8.
        # Quoted zones: 2 ($4), 4 ($5), 5 ($7), 8 ($9).
        matrix = RateMatrix(
            origin_zip="84043",
            products=(
                ProductRates(
                    product=_spec("Widget"),
                    zones=(
                        _quoted_zone(2, 4.0),
                        _quoted_zone(4, 5.0),
                        _quoted_zone(5, 7.0),
                        _quoted_zone(8, 9.0),
                    ),
                ),
            ),
        )
        weighted_profile = ProspectProfile.from_dict(
            {"brand": "T", "destinations_note": "CA, TX, NY"}
        )
        flat_profile = ProspectProfile.from_dict({"brand": "T"})

        blended, method = _blended_rate(weighted_profile, matrix)
        # Hand-computed: zone 2 has zero state weight and is excluded;
        # (5 + 7 + 9) / 3 = 7.00.
        self.assertAlmostEqual(blended, 7.0, places=6)
        self.assertEqual(method, BLEND_METHOD_WEIGHTED)

        flat_blended, flat_method = _blended_rate(flat_profile, matrix)
        # Flat: (4 + 5 + 7 + 9) / 4 = 6.25 — weighting must change the number.
        self.assertAlmostEqual(flat_blended, 6.25, places=6)
        self.assertEqual(flat_method, BLEND_METHOD_FLAT)
        self.assertNotEqual(blended, flat_blended)

    def test_blended_rate_single_state_falls_back_to_flat(self) -> None:
        matrix = RateMatrix(
            origin_zip="84043",
            products=(
                ProductRates(
                    product=_spec("Widget"),
                    zones=(_quoted_zone(4, 5.0), _quoted_zone(8, 9.0)),
                ),
            ),
        )
        profile = ProspectProfile.from_dict({"brand": "T", "destinations_note": "CA only"})
        blended, method = _blended_rate(profile, matrix)
        self.assertEqual(method, BLEND_METHOD_FLAT)
        self.assertAlmostEqual(blended, 7.0, places=6)

    def test_blended_rate_weights_products_by_monthly_units(self) -> None:
        # A: 100 units, zone blend (4 + 6) / 2 = 5; B: 300 units, (8 + 10) / 2 = 9.
        # Units-weighted: (100 * 5 + 300 * 9) / 400 = 8.00.
        matrix = RateMatrix(
            origin_zip="84043",
            products=(
                ProductRates(
                    product=_spec("A", units=100),
                    zones=(_quoted_zone(1, 4.0), _quoted_zone(2, 6.0)),
                ),
                ProductRates(
                    product=ProductSpec(
                        name="B", length_in=8.0, width_in=8.0, height_in=8.0,
                        weight_lb=2.0, monthly_units=300,
                    ),
                    zones=(_quoted_zone(1, 8.0), _quoted_zone(2, 10.0)),
                ),
            ),
        )
        profile = ProspectProfile.from_dict({"brand": "T"})
        blended, method = _blended_rate(profile, matrix)
        self.assertAlmostEqual(blended, 8.0, places=6)
        self.assertEqual(method, BLEND_METHOD_FLAT)
        # A product with no units gets zero weight when any product has units.
        matrix_mixed = RateMatrix(
            origin_zip="84043",
            products=(
                matrix.products[0],
                ProductRates(
                    product=ProductSpec(
                        name="C", length_in=9.0, width_in=9.0, height_in=9.0,
                        weight_lb=3.0,
                    ),
                    zones=(_quoted_zone(1, 20.0),),
                ),
            ),
        )
        blended_mixed, _ = _blended_rate(profile, matrix_mixed)
        self.assertAlmostEqual(blended_mixed, 5.0, places=6)

    def test_blended_rate_empty_matrix(self) -> None:
        blended, method = _blended_rate(
            ProspectProfile.from_dict({"brand": "T"}), RateMatrix(products=())
        )
        self.assertEqual(blended, 0.0)
        self.assertEqual(method, BLEND_METHOD_FLAT)

    def test_destination_weighted_savings_end_to_end(self) -> None:
        notes = _NOTES.replace("Destinations: mostly West Coast",
                               "Destinations: shipping mostly to CA, TX, NY")
        result = self._generate_with_current_cost(notes=notes)
        self.assertEqual(result["blend_method"], BLEND_METHOD_WEIGHTED)
        self.assertEqual(result["savings"]["blend_method"], BLEND_METHOD_WEIGHTED)
        self.assertIn(
            f"Blended best-rate average, {BLEND_METHOD_WEIGHTED}", result["deck_html"]
        )

    def test_wholesale_product_gets_parcel_rate_caveat(self) -> None:
        notes = (
            "Brand: BulkCo\n"
            "Widget — 6 x 5 x 3 in, 1.5 lb, ~500 units/mo\n"
            "Wholesale Case Pack — 12 x 12 x 12 in, 10 lb, ~50 units/mo\n"
        )
        result = self._generate(notes=notes)
        html = result["deck_html"]
        self.assertIn("Wholesale Case Pack is quoted at parcel rates", html)
        self.assertIn("we'll quote that separately", html)
        # Non-wholesale generation carries no caveat.
        clean = self._generate()
        self.assertNotIn("quoted at parcel rates", clean["deck_html"])

    # ------------------------------------------------------------------
    # v3 layout details
    # ------------------------------------------------------------------

    def test_tab_labels_truncate_with_full_name_in_title(self) -> None:
        notes = (
            "Brand: LongCo\n"
            "Super Premium Extra Large Gift Bundle — 8 x 6 x 4 in, 2 lb\n"
            "Mini Kit — 4 x 4 x 2 in, 0.5 lb\n"
        )
        result = self._generate(notes=notes)
        html = result["deck_html"]
        self.assertIn('title="Super Premium Extra Large Gift Bundle"', html)
        self.assertIn(">Super Premium Extra Large…<", html)
        # Short names stay untouched.
        self.assertIn(">Mini Kit<", html)

    def test_partner_section_sells_both_offers_once(self) -> None:
        result = self._generate()
        html = result["deck_html"]
        self.assertIn('data-key="partner"', html)
        self.assertEqual(html.count("Anata Shipping OS"), 1)
        self.assertEqual(html.count("Anata Fulfillment</h4>"), 1)
        self.assertIn("Coming soon: additional Anata fulfillment locations", html)
        self.assertIn("Lock these rates in", html)
        self.assertIn("https://anatainc.com/contact", html)
        # Generic capability claims live only in the partner section.
        self.assertEqual(html.count("2pm MT"), 1)
        self.assertEqual(html.count("Named account manager"), 1)

    def test_visual_sanity_written_sheet_parses(self) -> None:
        from html.parser import HTMLParser

        result = self._generate_with_current_cost()
        path = Path(tempfile.gettempdir()) / "rate_sheet_v4_visual_sanity.html"
        path.write_text(result["deck_html"], encoding="utf-8")

        class _Collector(HTMLParser):
            def __init__(self) -> None:
                super().__init__()
                self.section_keys: list[str] = []
                self.open_sections = 0
                self.closed_sections = 0

            def handle_starttag(self, tag, attrs):
                if tag == "section":
                    self.open_sections += 1
                    self.section_keys.append(dict(attrs).get("data-key", ""))

            def handle_endtag(self, tag):
                if tag == "section":
                    self.closed_sections += 1

        collector = _Collector()
        collector.feed(path.read_text(encoding="utf-8"))
        self.assertEqual(collector.open_sections, collector.closed_sections)
        self.assertEqual(
            collector.section_keys,
            ["hero", "rate-map", "carrier-rates", "monthly-math", "quote", "partner"],
        )

    def test_inline_scripts_pass_node_check(self) -> None:
        """Every inline <script> in the rendered sheet is valid JS."""
        import re as _re
        import shutil
        import subprocess

        node = shutil.which("node")
        if not node:
            self.skipTest("node not available")
        result = self._generate_with_current_cost()
        scripts = _re.findall(r"<script>(.*?)</script>", result["deck_html"], _re.S)
        self.assertGreaterEqual(len(scripts), 4)  # tabs, polish, engagement, map
        for index, script in enumerate(scripts):
            path = Path(tempfile.gettempdir()) / f"rate_sheet_v4_script_{index}.js"
            path.write_text(script, encoding="utf-8")
            proc = subprocess.run(
                [node, "--check", str(path)], capture_output=True, text=True
            )
            self.assertEqual(proc.returncode, 0, f"script {index}: {proc.stderr[:400]}")

    # ------------------------------------------------------------------
    # v4: state-outline map background + affine fit
    # ------------------------------------------------------------------

    def test_map_renders_state_outlines_behind_dots(self) -> None:
        result = self._generate()
        html = result["deck_html"]
        svg_start = html.index('id="rm-svg"')
        svg_end = html.index("</svg>", svg_start)
        map_svg = html[svg_start:svg_end]
        # Lower-48 (+DC, +faded AK/HI) outline paths render in the map svg…
        self.assertGreater(map_svg.count("<path"), 40)
        self.assertEqual(map_svg.count('class="rm-state'), 51)
        self.assertEqual(map_svg.count("rm-state-faded"), 2)  # AK/HI at 50% opacity
        # …BEHIND the dots: states group opens before the cells group.
        self.assertLess(map_svg.index('id="rm-states"'), map_svg.index('id="rm-cells"'))
        # Wikimedia space viewBox.
        self.assertIn('viewBox="0 0 959 593"', html)
        # Dots, rings, insets and tooltips survive the re-projection.
        self.assertGreater(html.count('class="rm-cell"'), 850)
        self.assertIn('class="rm-ring"', html)
        self.assertIn("1800 mi", html)

    def test_affine_fit_sanity(self) -> None:
        from sales_support_agent.services.fulfillment_deck import us_map
        from sales_support_agent.services.fulfillment_deck.zip3_centroids import (
            ZIP3_CENTROIDS,
        )

        # Measured 28.1px max residual over kept anchors (worst: TX).
        self.assertLess(us_map.AFFINE_MAX_RESIDUAL_PX, 35.0)
        # UT bbox center sanity in the Wikimedia 959x593 space.
        min_x, min_y, max_x, max_y = us_map.path_bbox("UT")
        center = ((min_x + max_x) / 2, (min_y + max_y) / 2)
        self.assertLess(abs(center[0] - 216), 60)
        self.assertLess(abs(center[1] - 249), 60)
        # The Salt Lake City (841) dot lands inside the UT path bbox (+12px).
        lat, lon = ZIP3_CENTROIDS["841"]
        x, y = us_map.albers_point_px(lat, lon)
        self.assertGreater(x, min_x - 12)
        self.assertLess(x, max_x + 12)
        self.assertGreater(y, min_y - 12)
        self.assertLess(y, max_y + 12)

    def test_map_mode_toggle_markup(self) -> None:
        html = self._generate()["deck_html"]
        self.assertIn('id="rm-mode"', html)
        self.assertIn('data-mode="cost"', html)
        self.assertIn('data-mode="transit"', html)
        self.assertIn("bestTransitForZone", html)
        self.assertIn("best transit time by ZIP area", html)

    # ------------------------------------------------------------------
    # v4: volume vetting
    # ------------------------------------------------------------------

    def test_hero_shows_volume_basis_sublabel(self) -> None:
        result = self._generate()
        run_id = result["run_id"]
        summary = dict(storage.get_run(run_id).summary_json)
        profile = dict(summary["prospect_profile"])
        profile["volume_basis"] = "74 DTC Shopify + 64 B2B wholesale"
        profile["monthly_order_volume"] = 138
        storage.update_summary(run_id, {"prospect_profile": profile})
        rerendered = rerender_rate_sheet(run_id, settings=load_settings())
        html = rerendered["deck_html"]
        self.assertIn("orders / month · 74 DTC Shopify + 64 B2B wholesale", html)
        # Without a basis the plain sublabel renders.
        clean = self._generate()
        self.assertIn(">orders / month<", clean["deck_html"])
        self.assertNotIn("orders / month ·", clean["deck_html"])

    # ------------------------------------------------------------------
    # v4: fulfillment quote engine
    # ------------------------------------------------------------------

    def test_quote_hand_checked_line_math(self) -> None:
        """3,000 orders, 2,000+1,000 units, all beauty (x1.15), blended $7.50."""
        from sales_support_agent.services.fulfillment_deck.quote import (
            build_fulfillment_quote,
        )

        profile = ProspectProfile.from_dict({
            "brand": "GlowCo",
            "monthly_order_volume": 3000,
            "products": [
                {"name": "Super Serum", "length_in": 4, "width_in": 4,
                 "height_in": 6, "weight_lb": 1.2, "monthly_units": 2000,
                 "product_category": "beauty"},
                {"name": "Glow Kit", "length_in": 10, "width_in": 8,
                 "height_in": 4, "weight_lb": 2.5, "monthly_units": 1000,
                 "product_category": "beauty"},
            ],
        })
        quote = build_fulfillment_quote(profile, RateMatrix(products=()), 7.50)
        self.assertEqual(quote["orders"], 3000)
        self.assertEqual(quote["units_total"], 3000)
        self.assertEqual(quote["multiplier"], 1.15)
        # Pallet math: cube 48*40*60*0.65 = 74,880 in³.
        # Serum 96 in³ -> 780/pallet; Kit 320 in³ -> 234/pallet; avg 507.
        self.assertEqual(quote["units_per_pallet"], 507)
        # ceil(3000 / 507) = 6 pallets.
        self.assertEqual(quote["pallets_per_month"], 6)
        by_key = {line["key"]: line for line in quote["lines"]}
        # Receiving: 6 x $20 x 1.15 = $138.00
        self.assertEqual(by_key["receiving"]["monthly"], 138.00)
        # Storage: 6 x $35 x 1.15 = $241.50
        self.assertEqual(by_key["storage"]["monthly"], 241.50)
        # Pick & pack: avg items = 3000/3000 = 1 -> no additional-item fee;
        # 3000 x $1.60 x 1.15 = $5,520.00
        self.assertEqual(quote["avg_items_per_order"], 1.0)
        self.assertEqual(by_key["pick_pack"]["monthly"], 5520.00)
        # Tech: $75 flat, NO multiplier.
        self.assertEqual(by_key["tech"]["monthly"], 75.00)
        self.assertEqual(by_key["tech"]["multiplier"], 1.0)
        # Shipping: 3000 x $7.50 = $22,500.00, NO multiplier.
        self.assertEqual(by_key["shipping"]["monthly"], 22500.00)
        self.assertEqual(by_key["shipping"]["multiplier"], 1.0)
        self.assertEqual(by_key["shipping"]["note"], "at the carrier rates above")
        # No wholesale-smelling product -> no wholesale line.
        self.assertNotIn("wholesale", by_key)
        # Total 138 + 241.50 + 5520 + 75 + 22500 = 28,474.50; per order 9.49.
        self.assertEqual(quote["monthly_total"], 28474.50)
        self.assertEqual(quote["effective_per_order"], 9.49)
        # Order-driven vs flat split for the scenario slider.
        self.assertEqual(quote["variable_monthly"], 28020.00)
        self.assertEqual(quote["fixed_monthly"], 454.50)

    def test_quote_multiplier_rules(self) -> None:
        from sales_support_agent.services.fulfillment_deck.quote import (
            build_fulfillment_quote,
            quote_multiplier,
        )

        fragile_beauty = ProspectProfile.from_dict({
            "brand": "T", "monthly_order_volume": 100,
            "products": [{"name": "Vase", "product_category": "beauty",
                          "fragile": True, "monthly_units": 100}],
        })
        # beauty 1.15 + fragile 0.05 = 1.20 (under the 1.25 cap).
        self.assertEqual(quote_multiplier(fragile_beauty), 1.20)
        # Hard cap: food 1.15 + fragile would be 1.20; force cap via override.
        self.assertEqual(quote_multiplier(fragile_beauty, margin_override=40), 1.25)
        # Flat override replaces the table: 12 -> x1.12.
        self.assertEqual(quote_multiplier(fragile_beauty, margin_override=12), 1.12)
        quote = build_fulfillment_quote(
            fragile_beauty, RateMatrix(products=()), 5.0, margin_override=12
        )
        self.assertEqual(quote["multiplier"], 1.12)
        self.assertEqual(quote["margin_override_pct"], 12)
        # Unknown category falls back to "other" (1.10).
        other = ProspectProfile.from_dict({
            "brand": "T", "monthly_order_volume": 100,
            "products": [{"name": "X", "product_category": "weird stuff",
                          "monthly_units": 100}],
        })
        self.assertEqual(quote_multiplier(other), 1.10)

    def test_quote_none_without_orders_and_wholesale_line(self) -> None:
        from sales_support_agent.services.fulfillment_deck.quote import (
            build_fulfillment_quote,
        )

        empty = ProspectProfile.from_dict({"brand": "T"})
        self.assertIsNone(build_fulfillment_quote(empty, RateMatrix(products=()), 5.0))

        wholesale = ProspectProfile.from_dict({
            "brand": "BulkCo", "monthly_order_volume": 1000,
            "products": [
                {"name": "Widget", "monthly_units": 800, "product_category": "other"},
                {"name": "Wholesale Case Pack", "monthly_units": 200,
                 "product_category": "other"},
            ],
        })
        quote = build_fulfillment_quote(wholesale, RateMatrix(products=()), None)
        by_key = {line["key"]: line for line in quote["lines"]}
        # 200 wholesale units x $0.15 x 1.10 = $33.00; no shipping line
        # without a blended rate.
        self.assertEqual(by_key["wholesale"]["monthly"], 33.00)
        self.assertNotIn("shipping", by_key)

    def test_quote_section_renders_invoice_without_internal_margins(self) -> None:
        result = self._generate_with_current_cost()
        html = result["deck_html"]
        quote = result["fulfillment_quote"]
        self.assertIsNotNone(quote)
        self.assertIn("Your estimated monthly invoice", html)
        self.assertIn("Estimated monthly total", html)
        self.assertIn("all-in monthly", html)
        self.assertIn("effective per order", html)
        # current_cost_per_parcel_usd is set -> the "vs. today" chip renders.
        self.assertIn("per order vs. today", html)
        self.assertIn("we finalize after a 30-minute scoping call", html)
        # Quoted rates only — the baseline floors and multiplier never render.
        section = html[html.index('data-key="quote"'):html.index('data-key="partner"')]
        self.assertNotIn("1.15", section)
        self.assertNotIn("baseline", section.lower())
        self.assertNotIn("multiplier", section.lower())
        # …but the stored quote keeps the margin audit trail for History.
        self.assertIn("multiplier", quote)
        # Scenario hooks: the total row splits fixed vs order-driven money.
        self.assertIn('data-scn="total"', section)
        self.assertIn('data-scn="per-order"', section)

    def test_margin_override_flows_through_edits(self) -> None:
        result = self._generate()
        run_id = result["run_id"]
        self.assertIsNone(
            dict(storage.get_run(run_id).summary_json).get("quote_margin_override")
        )
        updated = apply_profile_edits(
            run_id, {"quote_margin_override": 12.0}, settings=load_settings()
        )
        stored = dict(storage.get_run(run_id).summary_json)
        self.assertEqual(stored["quote_margin_override"], 12.0)
        self.assertEqual(updated["fulfillment_quote"]["multiplier"], 1.12)
        self.assertEqual(stored["fulfillment_quote"]["multiplier"], 1.12)
        # Clearing the override returns to automatic category margins.
        apply_profile_edits(
            run_id, {"quote_margin_override": None}, settings=load_settings()
        )
        stored = dict(storage.get_run(run_id).summary_json)
        self.assertIsNone(stored["quote_margin_override"])
        self.assertNotEqual(stored["fulfillment_quote"]["multiplier"], 1.12)

    # ------------------------------------------------------------------
    # v4: partner CTA, trust stamp, polish hooks
    # ------------------------------------------------------------------

    def test_shipping_os_card_icon_and_register_cta(self) -> None:
        html = self._generate()["deck_html"]
        self.assertIn(
            '<a class="os-cta" href="https://app.anatainc.com/register" '
            'target="_blank" rel="noreferrer">Try for free →</a>',
            html,
        )
        # The icon asset embeds as an inline <img> inside the sized wrapper,
        # directly above the Shipping OS card heading.
        icon_at = html.index('<div class="offer-icon">')
        heading_at = html.index("<h4>Anata Shipping OS</h4>")
        self.assertLess(icon_at, heading_at)
        self.assertIn("<img src='data:image/png;base64,", html[icon_at:heading_at])

    def test_trust_stamp_only_for_live_wms_rates(self) -> None:
        from sales_support_agent.services.fulfillment_deck.rendering import (
            render_rate_sheet_html,
        )
        from sales_support_agent.services.fulfillment_deck.schema import SectionFlags

        def _render(source: str) -> str:
            matrix = RateMatrix(
                origin_zip="84043",
                products=(
                    ProductRates(
                        product=_spec("Widget", units=100),
                        zones=(
                            ZoneRates(
                                zone=4, dest_zip="30303", dest_label="Atlanta, GA",
                                quotes=(RateQuote(carrier="USPS", service="GA",
                                                  rate_usd=7.0, transit_days=3,
                                                  zone=4, source=source),),
                            ),
                        ),
                    ),
                ),
            )
            return render_rate_sheet_html(
                profile=ProspectProfile.from_dict({"brand": "T"}),
                matrix=matrix,
                flags=SectionFlags(rate_matrix=True, zone_map=True),
                origin_label="ZIP 84043",
                generated_on="June 11, 2026",
                settings=load_settings(),
                blended_rate=7.0,
            )

        live = _render("wms")
        self.assertIn(
            "Rates pulled live from Anata&#x27;s carrier accounts · June 11, 2026", live
        )
        self.assertNotIn("Sample rates — illustrative", live)
        mock_html = _render("mock")
        self.assertNotIn("Rates pulled live", mock_html)
        self.assertIn("Sample rates — illustrative", mock_html)

    def test_polish_markup_hooks(self) -> None:
        result = self._generate_with_current_cost()
        html = result["deck_html"]
        # Count-up: final values in markup, animated once on view.
        self.assertIn("data-countup", html)
        self.assertIn("prefers-reduced-motion", html)
        # Scenario slider inside the monthly-math fragment, 50–200% step 5.
        mm = html[html.index('data-key="monthly-math"'):html.index('data-key="quote"')]
        self.assertIn('id="mm-scenario-range"', mm)
        self.assertIn('min="50" max="200" step="5" value="100"', mm)
        self.assertIn('data-scn="orders"', mm)
        self.assertIn('data-scn="linear"', mm)
        # Section entrance: hidden state only under the JS-added class.
        self.assertIn("html.js-anim .slide", html)
        self.assertIn("js-anim", html)
        self.assertIn("in-view", html)


if __name__ == "__main__":
    unittest.main()
