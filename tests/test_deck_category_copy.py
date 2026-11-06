"""Category-grounded deck copy.

A non-supplement brand must not inherit the supplement CVR citation, a flat
15% referral fee presented as Amazon's rate, or trial wording on a case study
when the selected offer has no trial.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services.deck.agency_sections import render_case_studies_section
from sales_support_agent.services.deck.dataset import (
    _build_margin_snapshot_html,
    resolve_amazon_referral_fee,
)
from sales_support_agent.services.deck.growth_plan import (
    GrowthPlanInputs,
    build_growth_plan,
    conversion_rate_methodology,
    cpc_methodology_lines,
)
from sales_support_agent.services.deck.intelligence_v2 import case_study_detail, load_case_studies


class CaseStudyTrialCopyTests(unittest.TestCase):
    def test_zantrex_detail_is_trial_neutral_when_no_trial_is_offered(self) -> None:
        zantrex = next(study for study in load_case_studies() if study["key"] == "zantrex")
        with_trial = case_study_detail(zantrex, include_trial=True)
        without_trial = case_study_detail(zantrex, include_trial=False)
        self.assertIn("the same platform trial users get", with_trial)
        self.assertNotIn("trial", without_trial.lower())
        self.assertIn("Connected to Anata Intelligence", without_trial)
        self.assertIn("first 30 days", without_trial)

        html = render_case_studies_section(include_trial=False)
        self.assertNotIn("trial users", html.lower())
        self.assertNotIn("free trial", html.lower())
        self.assertIn("40% revenue growth within 30 days", html)

        with_trial_html = render_case_studies_section(include_trial=True)
        self.assertIn("the same platform trial users get", with_trial_html)


class ReferralFeeTests(unittest.TestCase):
    def test_apparel_over_20_uses_clothing_rate(self) -> None:
        rate, name, caption = resolve_amazon_referral_fee(
            29.95,
            category="Merino ski and snow socks",
        )
        self.assertEqual(name, "Clothing & Accessories")
        self.assertEqual(rate, 0.17)
        self.assertIn("17%", caption)
        self.assertIn("Clothing & Accessories", caption)
        self.assertIn("over $20", caption)

        block = _build_margin_snapshot_html("$29.95", category="Clothing & Accessories")
        self.assertIn("5.09", block)
        self.assertIn("24.86", block)
        self.assertIn("17%", block)
        self.assertIn("Clothing &amp; Accessories", block)
        self.assertNotIn("assumed rate", block)
        self.assertNotIn("—", block)

    def test_clothing_price_bands(self) -> None:
        low, _, low_caption = resolve_amazon_referral_fee(14.00, title="wool socks")
        mid, _, mid_caption = resolve_amazon_referral_fee(18.00, title="wool socks")
        self.assertEqual(low, 0.05)
        self.assertIn("at or under $15", low_caption)
        self.assertEqual(mid, 0.10)
        self.assertIn("through $20", mid_caption)

    def test_unmatched_category_labels_the_assumed_rate(self) -> None:
        rate, name, caption = resolve_amazon_referral_fee(29.95, category="Industrial valves")
        self.assertEqual(rate, 0.15)
        self.assertEqual(name, "")
        self.assertIn("assumed rate", caption)
        self.assertIn("Industrial valves", caption)
        self.assertNotIn("Amazon's published rate", caption)
        block = _build_margin_snapshot_html("$29.95", category="Industrial valves")
        self.assertIn("4.49", block)
        self.assertIn("assumed rate", block)
        self.assertIn("Industrial valves", block)

    def test_missing_price_renders_nothing(self) -> None:
        self.assertEqual(_build_margin_snapshot_html(""), "")
        self.assertEqual(_build_margin_snapshot_html("Unavailable", category="socks"), "")


class ConversionRateCitationTests(unittest.TestCase):
    def test_non_supplement_category_uses_assumed_conversion_rate(self) -> None:
        line = conversion_rate_methodology(15.0, category="Merino ski and snow socks")
        self.assertIn("assumed conversion rate", line)
        self.assertIn("Merino ski and snow socks", line)
        self.assertNotIn("supplements typically", line)

        plan = build_growth_plan(
            inputs=GrowthPlanInputs(conversion_rate_pct=15.0, goal_monthly_sessions=1000, cogs_per_unit=4.0),
            target_units=3,
            category="Merino ski and snow socks",
            listing_title="The Fit Zero Cushion Snow Sock",
        )
        joined = " ".join(plan.methodology_lines)
        self.assertIn("assumed conversion rate", joined)
        self.assertNotIn("supplements typically 7", joined)

    def test_supplement_category_keeps_the_supplement_range(self) -> None:
        line = conversion_rate_methodology(15.0, category="spirulina supplements")
        self.assertIn("supplements typically 7", line)
        self.assertNotIn("assumed conversion rate", line)

    def test_title_is_used_only_when_category_is_blank(self) -> None:
        from_title = conversion_rate_methodology(15.0, listing_title="blue spirulina capsules")
        self.assertIn("supplements typically 7", from_title)
        # A clothing category wins even if the title mentions a supplement.
        named = conversion_rate_methodology(
            15.0,
            category="ski socks",
            listing_title="spirulina capsules",
        )
        self.assertIn("assumed conversion rate", named)
        self.assertNotIn("supplements typically", named)


class CpcCitationTests(unittest.TestCase):
    def test_non_supplement_category_does_not_cite_supplement_cpc(self) -> None:
        on_channel, off_channel = cpc_methodology_lines(category="Merino ski socks, Clothing & Accessories")
        self.assertIn("planning assumption", on_channel)
        self.assertIn("planning assumption", off_channel)
        for line in (on_channel, off_channel):
            self.assertNotIn("Pacvue", line)
            self.assertNotIn("WordStream", line)
            self.assertNotIn("Digital Applied", line)
            self.assertNotIn("Health & Household", line)
            self.assertNotIn("supplements", line.lower())
            self.assertNotRegex(line, r"\$\d")

        plan = build_growth_plan(
            inputs=GrowthPlanInputs(conversion_rate_pct=15.0, goal_monthly_sessions=1000, cogs_per_unit=4.0),
            target_units=3,
            category="Clothing & Accessories",
            listing_title="spirulina capsules",
        )
        joined = " ".join(plan.methodology_lines)
        self.assertNotIn("Pacvue", joined)
        self.assertNotIn("WordStream", joined)
        self.assertNotIn("Digital Applied", joined)
        self.assertNotIn("$2.17", joined)
        self.assertNotIn("$1.20", joined)
        self.assertIn("planning assumption", joined)
        on_channel_card = next(channel for channel in plan.channels if channel.key == "on_channel_paid")
        self.assertNotIn("Pacvue", on_channel_card.source_label)
        self.assertIn("Planning assumption", on_channel_card.source_label)

    def test_supplement_and_health_categories_keep_published_cpc_sources(self) -> None:
        for category in ("spirulina supplements", "Health & Household", "Beauty, Health, and Personal Care"):
            on_channel, off_channel = cpc_methodology_lines(category=category)
            self.assertIn("Pacvue Q1 2026 Health & Household", on_channel)
            self.assertIn("supplements $2.50", on_channel)
            self.assertIn("WordStream 2025", off_channel)
            self.assertIn("Digital Applied 2026", off_channel)

    def test_blank_category_uses_the_title_then_stays_neutral(self) -> None:
        on_channel, _off_channel = cpc_methodology_lines(listing_title="blue spirulina capsules")
        self.assertIn("Pacvue", on_channel)
        neutral_on, neutral_off = cpc_methodology_lines()
        self.assertIn("planning assumption", neutral_on)
        self.assertIn("planning assumption", neutral_off)
        self.assertNotIn("Pacvue", neutral_on + neutral_off)


try:
    from sqlalchemy import select
    from sales_support_agent.models.database import create_session_factory, init_database, session_scope
    from sales_support_agent.models.entities import AutomationRun
    from sales_support_agent.services.deck_generator import DeckGenerationService
    from tests.test_deck_generator import _FakeAmazonClient, _FakeProductResearch, _build_settings, _xray_csv

    _INTEGRATION = True
except ModuleNotFoundError:
    _INTEGRATION = False


@unittest.skipUnless(_INTEGRATION, "sqlalchemy is required for deck generation")
class CategoryCopyDeckTests(unittest.TestCase):
    def test_generated_apparel_deck_without_a_trial_uses_grounded_copy(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        with session_scope(session_factory) as session:
            service = DeckGenerationService(_build_settings(), session, amazon_client=_FakeAmazonClient())
            service.product_research = _FakeProductResearch()
            service.generate_deck(
                template_version="agency_v1",
                competitor_xray_csv_bytes=_xray_csv(),
                competitor_xray_filename="xray.csv",
                target_product_input="B0TARGET01",
                category_label="Merino ski socks",
                growth_plan_inputs={"growth_goal_sessions": "1000", "growth_cogs_per_unit": "4"},
                offer_payload_json=json.dumps(
                    [
                        {
                            "enabled": True,
                            "title": "Agency: Amazon growth partnership",
                            "description": "Anata manages the agreed Amazon growth work.",
                            "price": "$3,000/mo",
                        }
                    ]
                ),
            )
        with session_scope(session_factory) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.run_type == "deck_generation")
            ).scalar_one()
            html = str(dict(run.summary_json or {}).get("deck_html") or "")
        self.assertIn("Clothing &amp; Accessories", html)
        self.assertIn("17%", html)
        self.assertIn("for items over $20", html)
        self.assertNotIn("trial users get", html)
        self.assertIn("assumed conversion rate", html)
        self.assertNotIn("supplements typically 7", html)
        self.assertNotIn("Pacvue", html)
        self.assertNotIn("WordStream", html)
        self.assertNotIn("Digital Applied", html)
        self.assertIn("planning assumption", html)


if __name__ == "__main__":
    unittest.main()
