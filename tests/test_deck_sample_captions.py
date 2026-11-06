"""Captions for sample-based strategy decks.

Xray totals are a listing sample, ranks outside the top 50 are not "Unranked",
the Cerebro card counts the target plus competitor ASINs, the growth model
says the figures are directional estimates, and a custom Agency fee does not
grow a free-trial card.
"""

from __future__ import annotations

import json
import unittest
from datetime import date
from pathlib import Path
import sys
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services.deck.agency_sections import render_offer_section
from sales_support_agent.services.deck.dataset import (
    _asin_comparison_phrase,
    _build_cerebro_metric_cards,
    _compared_asin_count,
)
from sales_support_agent.services.deck.formatting import (
    offers_include_trial,
    offer_payload_selects_custom_cards,
)
from sales_support_agent.services.deck.growth_plan import (
    GrowthPlanInputs,
    build_growth_plan,
    render_growth_plan_section,
)
from sales_support_agent.services.deck.rendering import _render_cerebro_rank_summary, _render_metric_card
from sales_support_agent.services.deck.service import (
    market_is_xray_sample,
    market_sample_note,
    market_units_caption,
)
from sales_support_agent.services.helium10 import CerebroKeywordInsight, Helium10CerebroReport


def _keyword(phrase: str, rank: int | None, volume: int = 100) -> CerebroKeywordInsight:
    return CerebroKeywordInsight(
        phrase=phrase,
        search_volume=volume,
        keyword_sales=None,
        search_volume_trend="",
        target_rank=rank,
        target_impression_proxy=0,
        competitor_ranks={},
    )


def _cerebro(asins: list[str], ranks: list[int | None]) -> Helium10CerebroReport:
    return Helium10CerebroReport(
        keywords=[_keyword(f"term {index}", rank) for index, rank in enumerate(ranks)],
        competitor_asins=asins,
        top_20_ranked_keywords=0,
        impression_proxy=0,
        warnings=[],
    )


class SampleCaptionTests(unittest.TestCase):
    def test_units_caption_names_the_xray_sample(self) -> None:
        self.assertEqual(
            market_units_caption(listing_count=17, xray_sample=True),
            "17-listing Xray sample",
        )
        self.assertEqual(
            market_units_caption(listing_count=17, xray_sample=False),
            "monthly · category-wide",
        )
        note = market_sample_note(listing_count=17, xray_sample=True)
        self.assertIn("17-listing Xray sample", note)
        self.assertNotIn("category-wide", note)

    def test_xray_sample_follows_the_export_not_a_discovery_query(self) -> None:
        report = SimpleNamespace(products=[object()], discovery_query="", search_results_count=17)
        self.assertTrue(market_is_xray_sample({"market_figures_source": "xray_sample"}, report))
        self.assertFalse(market_is_xray_sample({"market_figures_source": "rainforest"}, report))
        self.assertTrue(market_is_xray_sample({}, report))
        discovered = SimpleNamespace(products=[object()], discovery_query="ski socks", search_results_count=17)
        self.assertFalse(market_is_xray_sample({}, discovered))

    def test_ranks_outside_the_top_50_are_not_called_unranked(self) -> None:
        report = _cerebro([], [2, 0, 75, None])
        html = _render_cerebro_rank_summary(report)
        self.assertIn("Not ranked in top 50", html)
        self.assertNotIn("Unranked", html)
        self.assertIn("Rank 1–5", html)
        # Position 0, rank 75, and a missing rank share the outside-top-50 bucket.
        self.assertRegex(
            html,
            r"Not ranked in top 50</span>.*?<span class='rank-count'>3 keywords</span>",
        )

    def test_comparison_card_counts_target_plus_competitors(self) -> None:
        competitors = [f"B0COMP{index:04d}" for index in range(10)]
        self.assertEqual(_compared_asin_count(competitors, "B0TARGET01"), 11)
        self.assertEqual(_asin_comparison_phrase(11), "11 ASINs compared")
        # The target column is already in the export, so it is not added twice.
        self.assertEqual(_compared_asin_count([*competitors, "B0TARGET01"], "b0target01"), 11)
        self.assertEqual(_asin_comparison_phrase(1), "1 ASIN compared")

        cards = _build_cerebro_metric_cards(_cerebro(competitors, [2]), None, target_asin="B0TARGET01")
        comparison = next(card for card in cards if "compared" in card["label"])
        self.assertEqual(comparison["label"], "11 ASINs compared")
        self.assertEqual(comparison["value"], "11")
        self.assertEqual(comparison["meta"], "ASIN columns compared against the target listing")
        rendered = _render_metric_card(comparison)
        self.assertIn("11 ASINs compared", rendered)
        self.assertNotIn("Competitor overlays", rendered)
        self.assertIn("ASIN columns compared against the target listing", rendered)

    def test_growth_slide_discloses_directional_estimates(self) -> None:
        plan = build_growth_plan(
            inputs=GrowthPlanInputs(
                conversion_rate_pct=15.0,
                goal_monthly_sessions=12_167,
                cogs_per_unit=4.0,
            ),
            target_units=3,
        )
        html = render_growth_plan_section(plan, target_brand="Le Bent", target_aov=29.95)
        note_at = html.lower().find("directional estimates")
        details_at = html.find("<details")
        self.assertGreater(note_at, 0)
        self.assertLess(note_at, details_at)
        self.assertIn('data-model-note="directional"', html)
        self.assertIn("not a forecast or a guarantee", html)

    def test_custom_agency_offer_omits_the_static_trial(self) -> None:
        agency = {
            "title": "Agency: Amazon growth partnership",
            "description": "Anata manages the agreed Amazon growth work.",
            "price": "$3,000/mo",
            "commission": "5% of growth",
            "baseline": "To be agreed from verified Amazon sales data",
        }
        payload = json.dumps([agency])
        self.assertTrue(offer_payload_selects_custom_cards(payload))
        self.assertFalse(offers_include_trial([agency], custom_payload=True))
        self.assertTrue(offers_include_trial([agency], custom_payload=False))
        trial_card = {**agency, "title": "Anata Intelligence", "description": "Free 14-day trial, then $99/mo."}
        self.assertTrue(offers_include_trial([agency, trial_card], custom_payload=True))
        flagged = {**agency, "includes_trial": True}
        self.assertTrue(offers_include_trial([flagged], custom_payload=True))

        urls = {"trial": "https://dashboard.anatainc.com/signup", "booking": "https://example.com/book"}
        agency_html = render_offer_section(
            offer_cards_html="<article class='offer-card'><h3>Agency</h3></article>",
            urls=urls,
            generated=date(2026, 11, 6),
            include_trial=False,
        )
        self.assertIn("Agency", agency_html)
        self.assertIn("Selected engagement", agency_html)
        self.assertNotIn("Free 14-day trial", agency_html)
        self.assertNotIn("14-day", agency_html.lower())
        self.assertNotIn("free trial", agency_html.lower())
        self.assertNotIn(urls["trial"], agency_html)
        self.assertIn("Book a call", agency_html)

        trial_html = render_offer_section(
            offer_cards_html="<article class='offer-card'><h3>Agency</h3></article>",
            urls=urls,
            generated=date(2026, 11, 6),
            include_trial=True,
        )
        self.assertIn("Free 14-day trial", trial_html)
        self.assertIn("Start the free 14-day trial", trial_html)
        self.assertIn(urls["trial"], trial_html)


try:
    from sqlalchemy import select
    from sales_support_agent.models.database import create_session_factory, init_database, session_scope
    from sales_support_agent.models.entities import AutomationRun
    from sales_support_agent.services.deck_generator import DeckGenerationService
    from tests.test_deck_generator import _FakeAmazonClient, _FakeProductResearch, _build_settings, _cerebro_csv, _xray_csv

    _INTEGRATION = True
except ModuleNotFoundError:
    _INTEGRATION = False


@unittest.skipUnless(_INTEGRATION, "sqlalchemy is required for deck generation")
class SampleCaptionDeckTests(unittest.TestCase):
    def _generate(self, **kwargs: object) -> str:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        with session_scope(session_factory) as session:
            service = DeckGenerationService(
                _build_settings(),
                session,
                amazon_client=_FakeAmazonClient(),
            )
            service.product_research = _FakeProductResearch()
            service.generate_deck(
                competitor_xray_csv_bytes=_xray_csv(),
                competitor_xray_filename="xray.csv",
                cerebro_csv_bytes=_cerebro_csv(),
                cerebro_filename="cerebro.csv",
                target_product_input="B0TARGET01",
                template_version="agency_v1",
                growth_plan_inputs={"growth_goal_sessions": "1000", "growth_cogs_per_unit": "4"},
                **kwargs,
            )
        with session_scope(session_factory) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.run_type == "deck_generation")
            ).scalar_one()
            return str(dict(run.summary_json or {}).get("deck_html") or "")

    def test_xray_deck_uses_sample_captions_and_keeps_the_default_trial(self) -> None:
        html = self._generate()
        self.assertIn("3-listing Xray sample", html)
        self.assertNotIn("monthly · category-wide", html)
        self.assertIn("Not ranked in top 50", html)
        self.assertNotIn("Unranked", html)
        self.assertIn("3 ASINs compared", html)
        self.assertNotIn("Competitor overlays", html)
        self.assertIn("ASIN columns compared against the target listing", html)
        self.assertIn("Directional estimates", html)
        self.assertIn('data-model-note="directional"', html)
        self.assertIn("Free 14-day trial", html)

    def test_custom_agency_deck_does_not_add_a_trial(self) -> None:
        html = self._generate(
            offer_payload_json=json.dumps(
                [
                    {
                        "enabled": True,
                        "title": "Agency: Amazon growth partnership",
                        "description": "Anata manages the agreed Amazon growth work.",
                        "price": "$3,000/mo",
                        "price_label": "Monthly retainer",
                        "commission": "5% of growth",
                        "commission_label": "Variable fee",
                        "baseline": "To be agreed from verified Amazon sales data",
                        "baseline_label": "Growth baseline",
                    }
                ]
            )
        )
        self.assertIn("Agency: Amazon growth partnership", html)
        self.assertIn("$3,000/mo", html)
        self.assertNotIn("Free 14-day trial", html)
        self.assertNotIn("Start the free 14-day trial", html)
        self.assertNotIn("Start your free 14-day trial", html)
        self.assertIn("3-listing Xray sample", html)


if __name__ == "__main__":
    unittest.main()
