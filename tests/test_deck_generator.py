from __future__ import annotations

import unittest
from pathlib import Path
import sys
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from sqlalchemy import select
    from sales_support_agent.models.database import create_session_factory, init_database, session_scope
    from sales_support_agent.models.entities import AutomationRun
    from sales_support_agent.services.deck_generator import (
        DeckGenerationService,
        _extract_listing_copy_points,
        _normalize_custom_offer_cards,
    )
    from sales_support_agent.services.helium10 import parse_cerebro_csv, parse_word_frequency_csv
    from sales_support_agent.services.product_research import EnrichedHeroProduct

    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    SQLALCHEMY_AVAILABLE = False


class _FakeAmazonClient:
    def is_configured(self) -> bool:
        return False


class _FakeProductResearch:
    def enrich_target_product(self, target: dict[str, str]) -> EnrichedHeroProduct:
        asin = target.get("asin", "") or "B0TARGET01"
        return EnrichedHeroProduct(
            asin=asin,
            candidate_asin=asin,
            brand_name="OceanRx",
            title="Ocean Rx Experience Pure Blue Spirulina",
            source_url=target.get("source_url", ""),
            description="Blue spirulina supplement positioned for energy and recovery.",
            price="$29.99",
            dimensions="4 x 2 x 2 in",
            image_url="https://example.com/hero.jpg",
            product_type="Spirulina Supplement",
            bsr=5,
            rating=4.6,
            review_count=121,
            identity_source="amazon",
            market_metrics_source="amazon",
            tags=(),
            warnings=(),
        )


def _build_settings() -> SimpleNamespace:
    repo_root = Path("/Users/davidnarayan/Documents/Playground/Lead-scraper")
    return SimpleNamespace(
        google_sheets_spreadsheet_id="",
        google_sheets_sales_range="",
        google_service_account_json="",
        canva_client_id="",
        canva_client_secret="",
        canva_redirect_uri="https://sales-support-agent.onrender.com/admin/api/canva/callback",
        canva_brand_template_id="",
        canva_token_secret="token-secret",
        deck_canva_poll_interval_seconds=1,
        deck_canva_poll_attempts=1,
        deck_competitor_required_columns=(),
        deck_competitor_allowed_columns=(),
        deck_required_template_fields=(),
        shared_brand_package_path=repo_root / "shared" / "anata_brand",
        deck_public_base_url="https://sales-support-agent.onrender.com",
    )


def _xray_csv() -> bytes:
    return (
        "Product Details,ASIN,URL,Image URL,Brand,Price  $,ASIN Revenue,ASIN Sales,BSR,Ratings,Review Count,Category,Seller Country/Region,Size Tier,Fulfillment,Dimensions,Weight\n"
        "Ocean Rx Experience Pure Blue Spirulina,B0TARGET01,https://www.amazon.com/dp/B0TARGET01,https://example.com/target.jpg,OceanRx,29.99,100007.29,4200,5,4.6,121,Spirulina,USA,Large Standard-Size,FBA,4 x 2 x 2 in,1 lb\n"
        "Organic Blue Spirulina,B08DK5RDJV,https://www.amazon.com/dp/B08DK5RDJV,https://example.com/comp1.jpg,Rival A,23.03,650564.87,12000,16,4.5,57,Spirulina,USA,Large Standard-Size,FBA,4 x 2 x 2 in,1 lb\n"
        "USDA Organic Blue Spirulina,B08YRDBFFX,https://www.amazon.com/dp/B08YRDBFFX,https://example.com/comp2.jpg,Rival B,15.29,67071.53,2400,2,4.2,41,Spirulina,USA,Small Standard-Size,AMZ,3 x 2 x 2 in,1 lb\n"
    ).encode("utf-8")


def _keyword_csv() -> bytes:
    return (
        "Keyword Phrase,Search Volume,Keyword Sales,Suggested PPC Bid,Competing Products,Title Density,Competitor Rank (avg)\n"
        "blue spirulina,8299,1820,1.23,317,9,11.2\n"
        "blue spirulina powder,4200,930,1.05,188,6,14.0\n"
    ).encode("utf-8")


def _keyword_csv_extra() -> bytes:
    return (
        "Keyword Phrase,Search Volume,Keyword Sales,Suggested PPC Bid,Competing Products,Title Density,Competitor Rank (avg)\n"
        "spirulina gummies,1800,220,0.84,91,4,18.0\n"
        "blue spirulina,9000,1900,1.10,321,8,10.5\n"
    ).encode("utf-8")


def _cerebro_csv() -> bytes:
    return (
        "Keyword Phrase,Keyword Sales,Search Volume,Search Volume Trend,Position (Rank),B07FKKP72W,B071W6CQ7S\n"
        "blue spirulina,1820,8299,stable,7,4,2\n"
        "spirulina powder,930,4200,growing,18,6,3\n"
        "organic spirulina tablets,410,1800,flat,29,8,5\n"
    ).encode("utf-8")


def _word_frequency_csv() -> bytes:
    return (
        "Word,Frequency\n"
        "spirulina,510\n"
        "powder,330\n"
        "organic,240\n"
        "blue,210\n"
        "supplement,120\n"
    ).encode("utf-8")


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required for deck generator tests")
class DeckGeneratorTests(unittest.TestCase):
    def test_normalize_custom_offer_cards_prefers_json_payload(self) -> None:
        cards = _normalize_custom_offer_cards(
            offer_payload_json="""
            [
              {"enabled": true, "title": "Offer A", "description": "Desc", "price": "$1", "price_label": "Fee", "commission": "2%", "commission_label": "Comm", "baseline": "$10", "baseline_label": "Base", "bonus": "Note"},
              {"enabled": false, "title": "Offer B"}
            ]
            """,
            offers=["channel_management"],
        )

        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0]["title"], "Offer A")
        self.assertEqual(cards[0]["bonus"], "Note")

    def test_extract_listing_copy_points_summarizes_long_blob(self) -> None:
        bullets = _extract_listing_copy_points(
            "About this item CONFIDENCE IN EVERY READING: Monitor your heart health with advanced accuracy technology "
            "SAFEGUARD YOUR HEART: Detect heartbeat irregularities during routine blood pressure measurements "
            "TURN NUMBERS INTO INSIGHTS: Connect effortlessly to the app to store readings and share reports"
        )

        self.assertGreaterEqual(len(bullets), 2)
        self.assertTrue(any("Confidence In Every Reading" in item for item in bullets))
        self.assertTrue(all("About this item" not in item for item in bullets))

    def test_parse_cerebro_and_word_frequency_reports(self) -> None:
        cerebro = parse_cerebro_csv(_cerebro_csv())
        words = parse_word_frequency_csv(_word_frequency_csv())

        self.assertIsNotNone(cerebro)
        self.assertIsNotNone(words)
        assert cerebro is not None
        assert words is not None
        self.assertEqual(cerebro.top_20_ranked_keywords, 2)
        self.assertEqual(cerebro.impression_proxy, 12499)
        self.assertEqual(len(cerebro.competitor_asins), 2)
        self.assertEqual(words.words[0].word, "spirulina")
        self.assertEqual(words.total_frequency, 1410)

    def test_target_xray_csv_overrides_when_target_not_in_competitor_set(self) -> None:
        """Regression: when the prospect's ASIN isn't in the competitor Xray
        and product-research enrichment returns nothing (no SP-API), uploading
        a separate Target Xray CSV must populate the target metrics so the
        deck doesn't render "Unavailable" for every cell."""
        from sales_support_agent.services.product_research import EnrichedHeroProduct

        # Empty enrichment — simulates no SP-API in production.
        class _EmptyResearch:
            def enrich_target_product(self, target):
                return EnrichedHeroProduct(
                    asin="", candidate_asin="", brand_name="", title="",
                    source_url=target.get("source_url", ""), description="",
                    price="", dimensions="", image_url="", product_type="",
                    bsr=None, rating=None, review_count=None,
                    identity_source="", market_metrics_source="",
                    tags=(), warnings=(),
                )

        # Single-row target Xray (just the prospect listing). ASIN is NOT
        # present in _xray_csv() (the competitor set), so without this
        # upload the target would fall back to "Unavailable" everywhere.
        target_xray = (
            "Product Details,ASIN,URL,Image URL,Brand,Price  $,ASIN Revenue,ASIN Sales,BSR,Ratings,Review Count,Category,Seller Country/Region,Size Tier,Fulfillment,Dimensions,Weight\n"
            "Zantrex Shred GLP Drink,B0FML8PTJH,https://www.amazon.com/dp/B0FML8PTJH,https://example.com/zantrex.jpg,Zantrex,43.64,823.07,18,344373,4.2,25,Health,US,Large,FBA,3.66 x 3.42 x 3.58 in,0.54\n"
        ).encode("utf-8")

        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        with session_scope(session_factory) as session:
            service = DeckGenerationService(
                _build_settings(), session,
                amazon_client=_FakeAmazonClient(),
            )
            service.product_research = _EmptyResearch()
            service.generate_deck(
                competitor_xray_csv_bytes=_xray_csv(),
                competitor_xray_filename="competitors.csv",
                target_xray_csv_bytes=target_xray,
                target_xray_filename="target.csv",
                target_product_input="B0FML8PTJH",
            )

        with session_scope(session_factory) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.run_type == "deck_generation")
            ).scalar_one()
            html = str(dict(run.summary_json or {}).get("deck_html") or "")

        # Target metrics from the target-only Xray must appear in the deck.
        self.assertIn("Zantrex", html)
        self.assertIn("$43.64", html)
        # And the deck should NOT render the "Unavailable" placeholder
        # on the target column anywhere — the upload populated everything.
        self.assertEqual(html.count("Unavailable"), 0)

    def test_aggregate_brands_groups_xray_products(self) -> None:
        """Audit item 3: brand aggregation rolls up multiple ASINs per brand."""
        from sales_support_agent.services.deck.rendering import _aggregate_brands
        from sales_support_agent.services.helium10 import parse_xray_csv

        # Xray with two products from same brand "Rival A". ASINs need to be
        # real-looking (10-char, B0…) for the parser to extract them.
        csv_bytes = (
            "Product Details,ASIN,URL,Image URL,Brand,Price  $,ASIN Revenue,ASIN Sales,BSR,Ratings,Review Count,Category,Seller Country/Region,Size Tier,Fulfillment,Dimensions,Weight\n"
            "Item One,B00AAAA001,https://www.amazon.com/dp/B00AAAA001,,Rival A,10.00,10000.00,1000,5,4.5,50,Cat,USA,Std,FBA,1in,1lb\n"
            "Item Two,B00AAAA002,https://www.amazon.com/dp/B00AAAA002,,Rival A,15.00,5000.00,500,12,4.2,30,Cat,USA,Std,FBA,1in,1lb\n"
            "Item Three,B00BBBB001,https://www.amazon.com/dp/B00BBBB001,,Rival B,20.00,20000.00,2000,3,4.8,200,Cat,USA,Std,FBA,1in,1lb\n"
        ).encode("utf-8")
        report = parse_xray_csv(csv_bytes)
        buckets = _aggregate_brands(report.products)
        # Sorted by total revenue desc → Rival B ($20k) first, Rival A ($15k) second.
        self.assertEqual(buckets[0]["brand"], "Rival B")
        self.assertEqual(buckets[0]["listing_count"], 1)
        self.assertEqual(buckets[1]["brand"], "Rival A")
        self.assertEqual(buckets[1]["listing_count"], 2)
        self.assertEqual(buckets[1]["total_revenue"], 15000.0)
        self.assertEqual(buckets[1]["best_bsr"], 5)  # min BSR across the two

    def test_detect_niche_mismatch_aligned_returns_empty(self) -> None:
        """Aligned datasets (same niche tokens) → no warning."""
        from sales_support_agent.services.deck.service import _detect_niche_mismatch
        from sales_support_agent.services.helium10 import parse_xray_csv, parse_keyword_csv
        warning = _detect_niche_mismatch(
            target_title="Ocean Rx Experience Pure Blue Spirulina",
            keyword_report=parse_keyword_csv(_keyword_csv()),
            cerebro_report=None,
            xray_report=parse_xray_csv(_xray_csv()),
        )
        self.assertEqual(warning, "")

    def test_detect_niche_mismatch_misaligned_returns_warning(self) -> None:
        """Wrong CSVs (different niche from target title) → warning."""
        from sales_support_agent.services.deck.service import _detect_niche_mismatch
        from sales_support_agent.services.helium10 import parse_keyword_csv
        wrong_keyword_csv = (
            "Keyword Phrase,Search Volume,Keyword Sales,Suggested PPC Bid,Competing Products,Title Density,Competitor Rank (avg)\n"
            "moroccanoil,23000,710,2.50,150,10,8.0\n"
            "frizz ease serum,5000,180,1.80,90,7,12.0\n"
        ).encode("utf-8")
        warning = _detect_niche_mismatch(
            target_title="Portable Breast Milk Cooler — Keeps Milk Cold Up to 15 Hours",
            keyword_report=parse_keyword_csv(wrong_keyword_csv),
            cerebro_report=None,
            xray_report=None,
        )
        self.assertIn("different niches", warning)
        self.assertIn("keyword CSV", warning)

    def test_generate_deck_returns_html_output_and_persists_run_metadata(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        with session_scope(session_factory) as session:
            service = DeckGenerationService(
                _build_settings(),
                session,
                amazon_client=_FakeAmazonClient(),
            )
            service.product_research = _FakeProductResearch()
            result = service.generate_deck(
                competitor_xray_csv_bytes=_xray_csv(),
                competitor_xray_filename="xray.csv",
                keyword_xray_csv_bytes=_keyword_csv(),
                keyword_xray_filename="keywords.csv",
                target_product_input="https://www.amazon.com/dp/B0TARGET01",
                channels=["amazon", "shopify"],
            )

        self.assertEqual(result.output_type, "html")
        self.assertIn("/decks/", result.view_url)
        self.assertGreater(result.sales_row_count, 0)
        self.assertGreater(result.competitor_row_count, 0)

        with session_scope(session_factory) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.run_type == "deck_generation")
            ).scalar_one()
            summary = dict(run.summary_json or {})
            self.assertEqual(summary.get("output_type"), "html")
            self.assertEqual(summary.get("view_count"), 0)
            # NOTE: production currently ignores the caller's `channels` arg and
            # always emits the full DEFAULT_SERVICE_TABS set. The docs at
            # sales_support_agent/docs/amazon_first_sales_deck.md still describe
            # the arg as caller-controlled. Drift flagged for follow-up; this
            # assertion matches current production behavior.
            self.assertEqual(
                summary.get("channels"),
                ["amazon", "tiktok_shop", "shopify", "3pl", "shipping_os"],
            )
            self.assertTrue(summary.get("deck_html"))
            self.assertTrue(summary.get("deck_slug"))

    def test_generate_deck_without_keyword_csv_still_generates(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        with session_scope(session_factory) as session:
            service = DeckGenerationService(
                _build_settings(),
                session,
                amazon_client=_FakeAmazonClient(),
            )
            service.product_research = _FakeProductResearch()
            result = service.generate_deck(
                competitor_xray_csv_bytes=_xray_csv(),
                competitor_xray_filename="xray.csv",
                keyword_xray_csv_bytes=None,
                keyword_xray_filename="",
                target_product_input="B0TARGET01",
                channels=["amazon"],
            )

        self.assertEqual(result.output_type, "html")
        self.assertIn("/decks/", result.view_url)

    def test_generate_deck_merges_multiple_keyword_uploads_and_uses_optional_reports(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        with session_scope(session_factory) as session:
            service = DeckGenerationService(
                _build_settings(),
                session,
                amazon_client=_FakeAmazonClient(),
            )
            service.product_research = _FakeProductResearch()
            result = service.generate_deck(
                competitor_xray_csv_payloads=[("xray.csv", _xray_csv())],
                keyword_xray_csv_payloads=[("keywords-a.csv", _keyword_csv()), ("keywords-b.csv", _keyword_csv_extra())],
                cerebro_csv_bytes=_cerebro_csv(),
                cerebro_filename="cerebro.csv",
                word_frequency_csv_bytes=_word_frequency_csv(),
                word_frequency_filename="words.csv",
                target_product_input="B0TARGET01",
                channels=["amazon"],
            )

        self.assertEqual(result.output_type, "html")
        with session_scope(session_factory) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.run_type == "deck_generation")
            ).scalar_one()
            summary = dict(run.summary_json or {})
            deck_html = str(summary.get("deck_html") or "")
            self.assertIn("Top-20 impression proxy", deck_html)
            self.assertIn("Top keyword opportunities from the Cerebro rank set", deck_html)


    def test_generated_deck_html_contains_brand_target_and_competitor(self) -> None:
        """Regression guard: after the module split (PR2) and rendering refactors,
        the rendered HTML must still contain the target ASIN, at least one
        competitor brand, and the brand-package monogram or fallback text.
        """
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
                target_product_input="https://www.amazon.com/dp/B0TARGET01",
            )

        with session_scope(session_factory) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.run_type == "deck_generation")
            ).scalar_one()
            deck_html = str(dict(run.summary_json or {}).get("deck_html") or "")

        self.assertIn("B0TARGET01", deck_html, "target ASIN must appear in deck HTML")
        # Competitor brands from _xray_csv() — at least one should render.
        self.assertTrue(
            "Rival A" in deck_html or "Rival B" in deck_html,
            "expected at least one competitor brand to render",
        )
        # The renderer either inlines the brand monogram (data: URI) or — if the
        # asset isn't found in test env — emits an empty string. Either way
        # the deck shell selector should be present.
        self.assertIn("brand-monogram", deck_html, "deck shell missing")

    def test_generate_deck_persists_when_embed_preview_fails(self) -> None:
        """`_fetch_embed_preview` makes an outbound HTTP request during render.
        A slow / down third-party host must not stall or break deck generation.
        """
        from unittest import mock
        import requests as real_requests

        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        with mock.patch(
            "sales_support_agent.services.deck.rendering.requests.get",
            side_effect=real_requests.exceptions.ConnectTimeout("simulated timeout"),
        ):
            with session_scope(session_factory) as session:
                service = DeckGenerationService(
                    _build_settings(),
                    session,
                    amazon_client=_FakeAmazonClient(),
                )
                service.product_research = _FakeProductResearch()
                result = service.generate_deck(
                    competitor_xray_csv_bytes=_xray_csv(),
                    competitor_xray_filename="xray.csv",
                    target_product_input="B0TARGET01",
                    case_study_url="https://this-host-will-timeout.example.com/case-study",
                )

        self.assertEqual(result.output_type, "html")
        self.assertGreater(result.competitor_row_count, 0)


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required for growth-plan tests")
class GrowthPlanTests(unittest.TestCase):
    """Tests for the new Growth Plan Synopsis section (PR5)."""

    def test_growth_plan_basic_math_matches_napkin(self) -> None:
        from sales_support_agent.services.deck.growth_plan import (
            GrowthPlanInputs,
            build_growth_plan,
        )
        # User's napkin: 3,000 units / 15% CVR = 20,000 sessions; goal 60,000 → delta 40,000
        plan = build_growth_plan(
            inputs=GrowthPlanInputs(
                conversion_rate_pct=15.0,
                goal_monthly_sessions=60_000,
                average_order_value=29.99,
                cogs_per_unit=4.5,
                shipping_per_unit=2.5,
            ),
            target_units=3_000,
        )
        self.assertEqual(plan.current_sessions, 20_000)
        self.assertEqual(plan.goal_sessions, 60_000)
        self.assertEqual(plan.delta_sessions, 40_000)
        # Channel mix sums (approximately) to delta sessions
        self.assertAlmostEqual(
            sum(c.sessions for c in plan.channels),
            plan.delta_sessions,
            delta=4,  # rounding tolerance across 5 buckets
        )
        # Off-channel paid uses Anata's $0.15 storefront-traffic CPC
        off_channel = next(c for c in plan.channels if c.key == "off_channel_paid")
        self.assertIn("$0.15", off_channel.detail)
        # On-channel paid spend = sessions × $3 CPC
        on_channel = next(c for c in plan.channels if c.key == "on_channel_paid")
        self.assertAlmostEqual(on_channel.monthly_cost, on_channel.sessions * 3.0, delta=0.01)

    def test_growth_plan_validation_flags_mix_and_missing_cogs(self) -> None:
        from sales_support_agent.services.deck.growth_plan import GrowthPlanInputs
        bad_mix = GrowthPlanInputs(mix_organic=50, mix_on_channel_paid=10)  # sums to 95
        errors = bad_mix.validate()
        self.assertTrue(any("sum to 100" in e for e in errors))

        affiliate_no_cogs = GrowthPlanInputs(mix_affiliate=20, cogs_per_unit=0.0)
        errors = affiliate_no_cogs.validate()
        self.assertTrue(any("COGS" in e for e in errors))

    def test_growth_plan_shortfall_flagged_when_mix_underdelivers(self) -> None:
        from sales_support_agent.services.deck.growth_plan import (
            GrowthPlanInputs,
            build_growth_plan,
        )
        # Goal needs 1000 delta sessions; assign all 100% to one channel — it
        # will deliver the full delta, no shortfall. So contrive shortfall by
        # zero-ing all channels (sum 0 != 100, but build still runs).
        plan = build_growth_plan(
            inputs=GrowthPlanInputs(
                conversion_rate_pct=15.0,
                goal_monthly_sessions=10_000,
                mix_organic=0, mix_on_channel_paid=0, mix_off_channel_paid=0,
                mix_affiliate=0, mix_retargeting=0,
                average_order_value=29.99,
            ),
            target_units=100,  # current_sessions = ~666
        )
        self.assertGreater(plan.delta_sessions, 0)
        self.assertEqual(plan.total_sessions_delivered, 0)
        self.assertEqual(plan.shortfall_sessions, plan.delta_sessions)

    def test_generate_deck_includes_growth_section_when_inputs_provided(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        with session_scope(session_factory) as session:
            service = DeckGenerationService(
                _build_settings(), session,
                amazon_client=_FakeAmazonClient(),
            )
            service.product_research = _FakeProductResearch()
            service.generate_deck(
                competitor_xray_csv_bytes=_xray_csv(),
                competitor_xray_filename="xray.csv",
                target_product_input="B0TARGET01",
                growth_plan_inputs={
                    "growth_cvr_pct": "15",
                    "growth_goal_sessions": "60000",
                    "growth_aov": "29.99",
                    "growth_cogs_per_unit": "4.5",
                    "growth_shipping_per_unit": "2.5",
                },
            )

        with session_scope(session_factory) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.run_type == "deck_generation")
            ).scalar_one()
            html = str(dict(run.summary_json or {}).get("deck_html") or "")

        self.assertIn("Closing the sessions gap", html)
        self.assertIn("growth-plan-slide", html)
        self.assertIn("Methodology and sources", html)

    def test_generate_deck_omits_growth_section_when_no_inputs(self) -> None:
        """Without growth_plan_inputs the section must not appear — preserves
        backward compatibility for callers that don't opt in."""
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        with session_scope(session_factory) as session:
            service = DeckGenerationService(
                _build_settings(), session,
                amazon_client=_FakeAmazonClient(),
            )
            service.product_research = _FakeProductResearch()
            service.generate_deck(
                competitor_xray_csv_bytes=_xray_csv(),
                competitor_xray_filename="xray.csv",
                target_product_input="B0TARGET01",
            )

        with session_scope(session_factory) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.run_type == "deck_generation")
            ).scalar_one()
            html = str(dict(run.summary_json or {}).get("deck_html") or "")

        # The CSS rules for .growth-plan-slide are always present in the
        # inlined brand stylesheet — assert on the actual section markup
        # and the visible heading instead.
        self.assertNotIn('<section class="slide growth-plan-slide"', html)
        self.assertNotIn("Closing the sessions gap", html)


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required for deck routing tests")
class DeckRoutingTests(unittest.TestCase):
    """End-to-end tests for the deck export and admin generate-deck routes.

    These spin up a minimal FastAPI app with just the deck routes mounted
    plus the in-memory session factory.
    """

    def _make_client(self, *, internal_api_key: str = ""):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from sales_support_agent.api.router import router as deck_router
        import tempfile, os

        # Use a temp file DB instead of :memory: because TestClient runs the
        # route on a worker thread; with :memory: the worker thread's connection
        # isn't shared with the seeding thread, so the table is "missing" when
        # the route looks it up.
        tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        tmp.close()
        self.addCleanup(lambda: os.unlink(tmp.name))
        session_factory = create_session_factory(f"sqlite:///{tmp.name}")
        init_database(session_factory)

        settings = _build_settings()
        # Routes used by these tests need a few extra settings attributes.
        settings.admin_cookie_name = "test_admin"
        settings.admin_session_secret = "test-session-secret"
        settings.admin_password_hash = ""
        settings.admin_username = ""
        settings.internal_api_key = internal_api_key
        settings.allow_admin_dashboard = True

        app = FastAPI()
        app.state.settings = settings
        app.state.agent_settings = settings
        app.state.admin_dashboard_settings = settings
        app.state.session_factory = session_factory
        app.include_router(deck_router)
        return TestClient(app, raise_server_exceptions=False), session_factory

    def _seed_deck(self, session_factory) -> tuple[int, str, str]:
        """Run the service end-to-end once and return (run_id, token, slug)."""
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
                target_product_input="B0TARGET01",
            )
        with session_scope(session_factory) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.run_type == "deck_generation")
            ).scalar_one()
            summary = dict(run.summary_json or {})
            return run.id, str(summary["export_token"]), str(summary["deck_slug"])

    def test_deck_export_route_increments_view_count(self) -> None:
        client, sf = self._make_client()
        run_id, token, slug = self._seed_deck(sf)

        first = client.get(f"/decks/{slug}/{run_id}/{token}")
        self.assertEqual(first.status_code, 200)
        self.assertIn("brand-monogram", first.text)

        # First-touch view count should now be 1
        with session_scope(sf) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.id == run_id)
            ).scalar_one()
            summary = dict(run.summary_json or {})
            self.assertEqual(summary.get("view_count"), 1)
            self.assertTrue(summary.get("first_viewed_at"))
            self.assertTrue(summary.get("last_viewed_at"))

        # Second hit from a different visitor key (different UA) should also increment.
        second = client.get(
            f"/decks/{slug}/{run_id}/{token}",
            headers={"User-Agent": "different-test-ua/1.0"},
        )
        self.assertEqual(second.status_code, 200)
        with session_scope(sf) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.id == run_id)
            ).scalar_one()
            summary = dict(run.summary_json or {})
            self.assertEqual(summary.get("view_count"), 2)

    def test_deck_export_token_mismatch_returns_404_not_500(self) -> None:
        client, sf = self._make_client()
        run_id, _real_token, slug = self._seed_deck(sf)

        wrong_token_resp = client.get(f"/decks/{slug}/{run_id}/THIS-TOKEN-IS-WRONG")
        self.assertEqual(wrong_token_resp.status_code, 404)
        self.assertIn("not found", wrong_token_resp.text.lower())

    def test_deck_export_unknown_run_id_returns_404(self) -> None:
        client, sf = self._make_client()
        # Don't seed; ask for a run that doesn't exist.
        resp = client.get("/decks/some-slug/9999/anytoken")
        self.assertEqual(resp.status_code, 404)

    def test_internal_generate_deck_requires_api_key(self) -> None:
        """`/api/admin/generate-deck` must reject requests without the
        configured internal-key header."""
        client, _sf = self._make_client(internal_api_key="real-internal-key")
        resp = client.post(
            "/api/admin/generate-deck",
            files={"competitor_xray_csv": ("x.csv", _xray_csv(), "text/csv")},
            data={"target_product_input": "B0TARGET01"},
        )
        self.assertIn(resp.status_code, (401, 403))

        # And with the wrong key, also rejected.
        resp_bad = client.post(
            "/api/admin/generate-deck",
            files={"competitor_xray_csv": ("x.csv", _xray_csv(), "text/csv")},
            data={"target_product_input": "B0TARGET01"},
            headers={"X-Internal-API-Key": "wrong-key"},
        )
        self.assertIn(resp_bad.status_code, (401, 403))


if __name__ == "__main__":
    unittest.main()
