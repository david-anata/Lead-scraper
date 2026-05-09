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
        # Amazon SP-API fields read by AmazonSpApiClient. Empty strings
        # mean "not configured" — the client falls back to public scraping
        # paths. Required so service-level instantiation doesn't blow up.
        amazon_sp_api_base_url="",
        amazon_sp_api_region="",
        amazon_sp_api_marketplace_id="",
        amazon_sp_api_lwa_client_id="",
        amazon_sp_api_lwa_client_secret="",
        amazon_sp_api_refresh_token="",
        amazon_sp_api_aws_access_key_id="",
        amazon_sp_api_aws_secret_access_key="",
        amazon_sp_api_aws_session_token="",
        # Misc deck-generation fields
        shopify_request_timeout_seconds=15,
        shopify_user_agent="",
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
        # PR32: deck shell is now app + rail (left nav). Old `.brand-monogram`
        # was replaced by the rail logo + brand name in the sidebar.
        self.assertIn('class="rail"', deck_html, "deck shell missing")
        self.assertIn("rail-brand-name", deck_html, "rail brand block missing")

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

    def test_growth_ramp_phase1_below_full_organic_and_paid(self) -> None:
        """PR29: Phase 1 cumulative must reflect ramp curves (organic 10%,
        on-channel paid 50%) — NOT the at-goal step function. Without this,
        the deck claimed Phase 1 delivers organic.sessions + on_channel_paid.sessions
        instantly, which a sharp prospect would call out as false data."""
        from sales_support_agent.services.deck.growth_plan import (
            GrowthPlanInputs,
            build_growth_plan,
            cumulative_sessions_at_phase,
        )
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
        organic = next(c for c in plan.channels if c.key == "organic")
        on_paid = next(c for c in plan.channels if c.key == "on_channel_paid")
        # Step-function would have given us organic.sessions + on_paid.sessions
        # (= 20,000 since each is 25% of 40,000 delta). With the ramp, P1
        # should deliver ~ organic*0.10 + on_paid*0.50 = 1,000 + 5,000 = 6,000.
        step_function_total = organic.sessions + on_paid.sessions
        ramp_p1_total = cumulative_sessions_at_phase(plan.channels, 1)
        self.assertLess(
            ramp_p1_total,
            step_function_total,
            "Phase 1 must reflect ramp, not deliver full at-goal allocation immediately",
        )
        expected_p1 = int(round(organic.sessions * 0.10 + on_paid.sessions * 0.50))
        self.assertEqual(ramp_p1_total, expected_p1)

    def test_growth_ramp_phase4_equals_steady_state(self) -> None:
        """PR29: by the end of Phase 4 every channel runs at 100% of its
        sessions allocation, so cumulative ramp == total_sessions_delivered."""
        from sales_support_agent.services.deck.growth_plan import (
            GrowthPlanInputs,
            build_growth_plan,
            cumulative_sessions_at_phase,
        )
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
        ramp_p4_total = cumulative_sessions_at_phase(plan.channels, 4)
        # Allow rounding tolerance — cumulative_sessions_at_phase rounds at
        # the end, total_sessions_delivered sums int channel.sessions directly.
        self.assertAlmostEqual(ramp_p4_total, plan.total_sessions_delivered, delta=2)

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

        self.assertIn("Closing the gap", html)
        self.assertIn("growth-plan-slide", html)
        # PR32: methodology label became "Sources & methodology" per the design.
        self.assertIn("Sources", html)
        self.assertIn("methodology", html)
        # PR28: per-phase ramp visualization is rendered.
        # PR32: ramp class names match the design (`.ramp` / `.ramp-step`).
        self.assertIn("class='ramp'", html)
        self.assertIn("Growth path", html)
        # All four phase tiles plus the "Today" tile are present.
        self.assertIn("Today", html)
        self.assertIn("Phase 1", html)
        self.assertIn("Phase 4", html)
        # Steady-state funnel panel is marked default for print.
        self.assertIn('data-default="1"', html)
        # PR29: ramp tiles use new "this phase" parenthetical, not the
        # misleading "from delta" that just restated the cumulative number.
        self.assertIn("this phase", html)
        self.assertNotIn("from delta", html)
        # PR32: ramp now puts the steady-state framing in the section
        # subtitle (`.gp-section-h .desc`) rather than a paragraph caption.
        self.assertIn("End-of-phase steady state", html)
        # PR30: heavy slides get semantic classes so the print stylesheet
        # can force page-breaks on them while letting small slides flow.
        self.assertIn("slide slide-conversion", html)
        self.assertIn("slide slide-offers", html)
        # PR32: cover slide replaced by full executive summary (`.exec`
        # element with `id="summary"`) — no more `.slide-cover`.
        self.assertIn('class="exec"', html)
        self.assertIn('id="summary"', html)
        # PR32: 3-card findings strip below the exec summary.
        self.assertIn('class="findings"', html)
        # PR32: sticky left-rail nav.
        self.assertIn('class="rail"', html)
        # PR32: section dividers between slides.
        self.assertIn('class="section-divider"', html)
        # PR33: rail footer has all 3 utility links (Open one-pager, Print
        # PDF, Get started). The first two were dropped in PR32 by mistake.
        self.assertIn('rail-open-story', html)
        self.assertIn('rail-print', html)
        # PR33: service offerings emit the design's flat class names so
        # `deck.css` styles apply (the old `.offering-*` classes aren't
        # in the redesigned stylesheet).
        self.assertIn('class="off-tabs"', html)
        self.assertIn('class="off-pane"', html)
        self.assertIn("class='off-block'", html)
        # PR33: floating .deck-toolbar removed (not in the design).
        self.assertNotIn('class="deck-toolbar"', html)
        # PR31: deck <head> now includes the Anata favicon (same source
        # as the admin dashboard) so browser tabs aren't a blank globe.
        self.assertIn('rel="icon"', html)
        self.assertIn("data:image/png;base64,", html)
        self.assertIn('rel="apple-touch-icon"', html)

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
        self.assertNotIn("Closing the gap", html)
        # PR33: when the growth section is absent the rail item still
        # renders, but as `.is-disabled` (30% opacity, click no-op) so the
        # rail's wayfinding stays the same shape across decks.
        self.assertIn("is-disabled", html)
        self.assertIn('aria-disabled="true"', html)

    def test_estimate_target_units_falls_back_to_bsr_then_zero(self) -> None:
        """PR39: when target_row.units_sold is None, derive monthly units
        from BSR (75,000 / rank rule, capped at 50,000). When BSR is also
        missing, return 0 cleanly without crashing the growth-plan math."""
        from sales_support_agent.services.deck.service import _estimate_target_units
        from sales_support_agent.services.helium10 import XrayProduct
        from sales_support_agent.services.product_research import EnrichedHeroProduct

        def _row(units, bsr):
            return XrayProduct(
                display_order=1, title="X", asin="B0X", url="", image_url="",
                brand="X", price=10.0, price_label="$10", revenue=None,
                revenue_label="", units_sold=units, units_label="",
                bsr=bsr, bsr_label="", rating=None, rating_label="",
                review_count=None, category="", seller_country="US",
                size_tier="LARGE STANDARD-SIZE", fulfillment="FBA",
                dimensions="", weight="",
            )

        def _hero(bsr):
            return EnrichedHeroProduct(
                asin="", candidate_asin="", brand_name="", title="",
                source_url="", description="", price="", dimensions="",
                image_url="", product_type="", bsr=bsr, rating=None,
                review_count=None, identity_source="", market_metrics_source="",
                tags=(), warnings=(),
            )

        # Direct units_sold wins.
        self.assertEqual(_estimate_target_units(target_row=_row(2500, 1000), hero_product=None), 2500)

        # No units → fall back to BSR-based estimate (75000/8903 ≈ 8 units).
        est = _estimate_target_units(target_row=_row(None, 8903), hero_product=None)
        self.assertGreater(est, 0)
        self.assertLess(est, 100)  # 75k/8903 ≈ 8

        # No row, just hero with BSR.
        est_hero = _estimate_target_units(target_row=None, hero_product=_hero(50.0))
        # 75000/50 = 1500
        self.assertEqual(est_hero, 1500)

        # Cap at 50,000 for absurdly low BSRs.
        est_top = _estimate_target_units(target_row=None, hero_product=_hero(0.5))
        self.assertEqual(est_top, 50_000)

        # No data at all → 0 (deck still renders).
        self.assertEqual(_estimate_target_units(target_row=_row(None, None), hero_product=_hero(None)), 0)
        self.assertEqual(_estimate_target_units(target_row=None, hero_product=None), 0)

    def test_shopify_product_data_extracts_title_vendor_price(self) -> None:
        """PR38: Shopify storefronts expose `/products/<handle>.json` with the
        full product record. The scraper extracts title, vendor (brand_name),
        price, image, description, and category. Detects Shopify by URL path
        shape and gracefully returns None for non-Shopify URLs so the caller
        falls back to the generic OG/JSON-LD scraper."""
        from unittest import mock
        from sales_support_agent.services.product_research import _fetch_shopify_product_data

        # Non-product paths return None immediately without a network call.
        self.assertIsNone(_fetch_shopify_product_data(""))
        self.assertIsNone(_fetch_shopify_product_data("https://example.com/"))
        self.assertIsNone(_fetch_shopify_product_data("https://example.com/collections/all"))

        # Stub a realistic Shopify product.json response.
        canned = {
            "product": {
                "title": "Grip Stick - Hydrating Lip Treatment",
                "vendor": "Tilt Beauty",
                "product_type": "Lip Balm",
                "body_html": "<p>Instant and lasting hydration.</p>",
                "handle": "new-grip-stick-hydrating-lip-treatment",
                "variants": [
                    {"id": 51562214031681, "price": "26.00", "featured_image": None},
                    {"id": 51562214031682, "price": "28.00", "featured_image": None},
                ],
                "images": [
                    {"src": "https://cdn.shopify.com/s/files/1/0879/1991/9425/files/main.jpg"},
                ],
            }
        }

        class _StubResponse:
            status_code = 200

            def json(self):
                return canned

        with mock.patch(
            "sales_support_agent.services.product_research.requests.get",
            return_value=_StubResponse(),
        ):
            result = _fetch_shopify_product_data(
                "https://tiltbeauty.com/products/new-grip-stick-hydrating-lip-treatment?variant=51562214031681"
            )

        self.assertIsNotNone(result)
        self.assertEqual(result["title"], "Grip Stick - Hydrating Lip Treatment")
        self.assertEqual(result["brand_name"], "Tilt Beauty")
        self.assertEqual(result["category"], "Lip Balm")
        # Selected variant from `?variant=` query param drives the price.
        self.assertEqual(result["price"], "$26.00")
        self.assertIn("hydration", result["description"])
        self.assertTrue(result["image_url"].startswith("https://cdn.shopify.com/"))

    def test_dtc_target_url_does_not_require_competitor_xray(self) -> None:
        """PR38: when the target is a website URL (Shopify/DTC), the deck
        should still generate WITHOUT a competitor Xray CSV. Synthesizes
        an empty Xray report so the niche/competitor sections render
        empty rather than blowing up."""
        from unittest import mock
        from sales_support_agent.services.product_research import EnrichedHeroProduct

        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        # Stub the product-research enrichment so the test doesn't hit
        # the live tiltbeauty.com endpoint (we just need the type contract).
        fake_hero = EnrichedHeroProduct(
            asin="",
            candidate_asin="",
            brand_name="Tilt Beauty",
            title="Grip Stick - Hydrating Lip Treatment",
            source_url="https://tiltbeauty.com/products/new-grip-stick-hydrating-lip-treatment",
            description="Instant and lasting hydration.",
            price="$26.00",
            dimensions="",
            image_url="https://cdn.shopify.com/.../main.jpg",
            product_type="Lip Balm",
            bsr=None,
            rating=None,
            review_count=None,
            identity_source="website",
            market_metrics_source="",
            tags=(),
            warnings=(),
        )

        with session_scope(session_factory) as session:
            service = DeckGenerationService(
                _build_settings(), session,
                amazon_client=_FakeAmazonClient(),
            )
            with mock.patch.object(
                service.product_research,
                "enrich_target_product",
                return_value=fake_hero,
            ):
                result = service.generate_deck(
                    target_product_input="https://tiltbeauty.com/products/new-grip-stick-hydrating-lip-treatment",
                    # NOTE: no competitor_xray_csv_bytes — DTC-only.
                )

        self.assertEqual(result.status, "success")
        # The DTC-mode warning is surfaced via the warnings list so the
        # AE knows the deck was generated without market-level data.
        warnings_blob = " ".join(result.warnings or [])
        self.assertIn("DTC mode", warnings_blob)
        # Sanity-check the rendered deck still has core slides.
        with session_scope(session_factory) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.run_type == "deck_generation")
            ).scalar_one()
            html = str(dict(run.summary_json or {}).get("deck_html") or "")
        self.assertIn("Tilt Beauty", html)
        self.assertIn("Grip Stick", html)


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
        # PR32: deck shell uses left rail nav, not the old brand-monogram bar.
        self.assertIn('class="rail"', first.text)

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

    def test_target_xray_detection_handles_parent_with_multiple_children(self) -> None:
        """PR43: a target Xray = an ASIN list where ALL rows belong to the
        SAME LISTING (parent + variants). Pre-PR43 detector required exactly
        1 product row, which misclassified parent+variants exports as
        competitor_xray. New rule: 1 distinct Brand → target_xray."""
        from sales_support_agent.services.helium10 import detect_csv_kind

        # 3 rows, all same brand (parent + 2 variant children)
        parent_with_variants = (
            b'Product Details,ASIN,URL,Image URL,Brand,Price  $,'
            b'Parent Level Sales,ASIN Sales,Parent Level Revenue,'
            b'ASIN Revenue,BSR,Ratings,Review Count\n'
            b'"Zantrex Berry",B0CC6QQGF3,https://amzn.com/dp/B0CC6QQGF3,,'
            b'Zantrex,24.99,5886,700,191533,29045,8903,4.2,2950\n'
            b'"Zantrex Tropical",B0CC6QQGF4,https://amzn.com/dp/B0CC6QQGF4,,'
            b'Zantrex,24.99,5886,1163,191533,46720,8903,4.2,2950\n'
            b'"Zantrex Citrus",B0CC6QQGF5,https://amzn.com/dp/B0CC6QQGF5,,'
            b'Zantrex,24.99,5886,4023,191533,115768,8903,4.2,2950\n'
        )
        # All same brand → target_xray (was misclassified as competitor pre-PR43)
        self.assertEqual(detect_csv_kind(parent_with_variants), "target_xray")

        # Multi-brand → competitor_xray
        multi_brand = (
            b'Product Details,ASIN,URL,Image URL,Brand,Price  $,'
            b'Parent Level Sales,ASIN Sales,Parent Level Revenue,'
            b'ASIN Revenue,BSR,Ratings,Review Count\n'
            b'"Brand A Product",B000000001,https://amzn.com/dp/B000000001,,'
            b'BrandA,9.99,1000,500,9990,4995,1000,4.0,100\n'
            b'"Brand B Product",B000000002,https://amzn.com/dp/B000000002,,'
            b'BrandB,19.99,2000,1500,39980,29985,500,4.5,200\n'
        )
        self.assertEqual(detect_csv_kind(multi_brand), "competitor_xray")

    def test_xray_parser_prefers_parent_level_sales(self) -> None:
        """PR43: parser reads Parent Level Sales/Revenue when present, falls
        back to ASIN-level for older Xray exports that don't expose parent
        columns. The 8x undercount on multi-variant brands (5,886 brand-wide
        units read as 700 single-variant units) was the root cause of the
        '53 sessions today' bug on the Zantrex deck."""
        from sales_support_agent.services.helium10 import parse_xray_csv

        # New-format Xray with both Parent Level and ASIN Sales columns.
        new_format = (
            b'Product Details,ASIN,URL,Image URL,Brand,Price  $,'
            b'Parent Level Sales,ASIN Sales,Parent Level Revenue,'
            b'ASIN Revenue,BSR,Ratings,Review Count\n'
            b'"Zantrex Berry",B0CC6QQGF3,https://amzn.com/dp/B0CC6QQGF3,,'
            b'Zantrex,24.99,5886,700,191533,29045,8903,4.2,2950\n'
        )
        report = parse_xray_csv(new_format)
        self.assertEqual(report.products[0].units_sold, 5886.0)
        self.assertEqual(report.products[0].revenue, 191533.0)

        # Legacy Xray with no Parent Level columns → falls back to ASIN.
        legacy_format = (
            b'Product Details,ASIN,URL,Image URL,Brand,Price  $,'
            b'ASIN Revenue,ASIN Sales,BSR,Ratings,Review Count\n'
            b'"Old Listing",B0LEGACY01,https://amzn.com/dp/B0LEGACY01,,'
            b'Brand,9.99,4995,500,1000,4.0,100\n'
        )
        report = parse_xray_csv(legacy_format)
        self.assertEqual(report.products[0].units_sold, 500.0)
        self.assertEqual(report.products[0].revenue, 4995.0)

    def test_competitor_table_dedupes_by_parent_brand_and_units(self) -> None:
        """PR43: competitor Xrays often include the same parent SKU multiple
        times (one row per variant child). After the parser switch to Parent
        Level Sales, those rows carry IDENTICAL parent-level numbers — listing
        them all makes the brand look 6x bigger than it is.

        Dedupe by (brand, units_sold). Same brand + same parent units = same
        parent SKU → keep first occurrence."""
        from sales_support_agent.services.helium10 import XrayProduct
        from sales_support_agent.services.deck.rendering import _dedupe_by_parent

        def _row(brand, units, revenue, asin):
            return XrayProduct(
                display_order=1, title=f"{brand} variant", asin=asin, url="",
                image_url="", brand=brand, price=10.0, price_label="$10",
                revenue=revenue, revenue_label=f"${revenue}", units_sold=units,
                units_label=str(units), bsr=1000.0, bsr_label="1000",
                rating=4.0, rating_label="4.0", review_count=100,
                category="", seller_country="US", size_tier="LARGE STANDARD-SIZE",
                fulfillment="FBA", dimensions="", weight="",
            )

        products = [
            _row("Ultima",  213227, 4874180, "B0DWVHD39N"),
            _row("Ultima",  213227, 4874180, "B0CY7WSJ86"),  # same parent, different variant
            _row("Zipfizz",  94252, 2109054, "B00KAWSJYC"),
            _row("CELSIUS", 112877, 1209184, "B002RSRURY"),
        ]
        deduped = _dedupe_by_parent(products)
        # Ultima collapses 2 → 1
        self.assertEqual(len(deduped), 3)
        self.assertEqual([p.brand for p in deduped], ["Ultima", "Zipfizz", "CELSIUS"])
        # First Ultima ASIN survives (most-prominent row in sales-desc order)
        self.assertEqual(deduped[0].asin, "B0DWVHD39N")

    def test_xray_totals_dedupe_parent_listings(self) -> None:
        """PR45: total_units_sold and total_revenue must dedupe parent
        listings before summing. After PR43 the parser reads Parent Level
        Sales/Revenue, but those values repeat IDENTICALLY on every child
        row of a multi-variant listing. Without dedupe a 6-flavor brand
        with 5,886 parent units gets summed as 35,316 (6×) — and the
        Units Sold tile inflates accordingly.

        Also asserts distinct_brand_count is the brand count, not the row
        count — the "top N brands" label should reflect actual brands."""
        from sales_support_agent.services.helium10 import parse_xray_csv

        # Brand A has 3 variants of one parent (5,886 / $147,150 each).
        # Brand B has 1 listing (1,000 / $25,000).
        # Brand C has 2 variants of one parent (2,000 / $40,000 each).
        # Correct totals: 5,886 + 1,000 + 2,000 = 8,886 units across 3 brands.
        # Buggy totals would be: 17,658 + 1,000 + 4,000 = 22,658 units across "6 brands".
        csv_data = (
            b'Product Details,ASIN,URL,Image URL,Brand,Price  $,'
            b'Parent Level Sales,ASIN Sales,Parent Level Revenue,'
            b'ASIN Revenue,BSR,Ratings,Review Count\n'
            b'"BrandA flavor 1",B00AAA0001,https://amzn.com/dp/B00AAA0001,,'
            b'BrandA,25,5886,2000,147150,50000,1000,4.2,100\n'
            b'"BrandA flavor 2",B00AAA0002,https://amzn.com/dp/B00AAA0002,,'
            b'BrandA,25,5886,2000,147150,50000,1000,4.2,100\n'
            b'"BrandA flavor 3",B00AAA0003,https://amzn.com/dp/B00AAA0003,,'
            b'BrandA,25,5886,1886,147150,47150,1000,4.2,100\n'
            b'"BrandB only",B00BBB0001,https://amzn.com/dp/B00BBB0001,,'
            b'BrandB,25,1000,1000,25000,25000,2000,4.0,50\n'
            b'"BrandC flavor 1",B00CCC0001,https://amzn.com/dp/B00CCC0001,,'
            b'BrandC,20,2000,1200,40000,24000,3000,4.1,75\n'
            b'"BrandC flavor 2",B00CCC0002,https://amzn.com/dp/B00CCC0002,,'
            b'BrandC,20,2000,800,40000,16000,3000,4.1,75\n'
        )
        report = parse_xray_csv(csv_data)
        # 6 raw rows survive parsing.
        self.assertEqual(len(report.products), 6)
        # But totals dedupe to per-parent-listing.
        self.assertEqual(report.total_units_sold, 8886.0)
        self.assertEqual(report.total_revenue, 212150.0)
        # And brand count is distinct brands, not row count.
        self.assertEqual(report.distinct_brand_count, 3)

    def test_csv_kind_detection_routes_files_to_correct_slot(self) -> None:
        """PR40: header-based detection lets the admin form drop ALL CSVs
        into one input. Each file kind is uniquely identified by its
        header signature (target vs competitor xray decided by row count)."""
        from sales_support_agent.services.helium10 import (
            detect_csv_kind,
            extract_target_asin_from_xray,
        )

        # Single-row Xray = target.
        target_xray = (
            b'Product Details,ASIN,URL,Image URL,Brand,Price  $,'
            b'ASIN Revenue,ASIN Sales,BSR,Ratings,Review Count\n'
            b'"My Target",B0XYZ123AB,https://amzn.com/dp/B0XYZ123AB,,Brand,29.99,$5000,200,1234,4.5,100\n'
        )
        self.assertEqual(detect_csv_kind(target_xray), "target_xray")
        self.assertEqual(extract_target_asin_from_xray(target_xray), "B0XYZ123AB")

        # Multi-row Xray = competitors. ASIN extraction returns "" for
        # competitor xray so the optional-ASIN derivation only fires on
        # the single-row target file.
        competitor_xray = target_xray + (
            b'"Comp 1",B0AAA111AA,https://amzn.com,,B,9.99,$1000,50,5000,4.2,30\n'
            b'"Comp 2",B0BBB222BB,https://amzn.com,,B,19.99,$2000,100,2000,4.3,60\n'
        )
        self.assertEqual(detect_csv_kind(competitor_xray), "competitor_xray")
        self.assertEqual(extract_target_asin_from_xray(competitor_xray), "")

        # Magnet/Keyword (no rank column).
        keyword_csv = (
            b'Keyword Phrase,Search Volume,Competing Products\n'
            b'fat burner,12000,500\n'
        )
        self.assertEqual(detect_csv_kind(keyword_csv), "keyword")

        # Cerebro (rank column distinguishes from keyword).
        cerebro_csv = (
            b'Keyword Phrase,Search Volume,Position (Rank),Keyword Sales\n'
            b'fat burner,12000,5,250\n'
        )
        self.assertEqual(detect_csv_kind(cerebro_csv), "cerebro")

        # Word frequency.
        word_freq = b'Word,Frequency\nweight,499\nloss,417\n'
        self.assertEqual(detect_csv_kind(word_freq), "word_frequency")

        # Unknown / empty input.
        self.assertEqual(detect_csv_kind(b""), "unknown")
        self.assertEqual(detect_csv_kind(None), "unknown")
        self.assertEqual(detect_csv_kind(b"Random,Stuff\nfoo,bar\n"), "unknown")

    def test_unified_upload_partitions_files_and_derives_asin(self) -> None:
        """PR40 end-to-end: drop a target Xray + a competitor Xray into the
        unified `csv_files` field, leave target_product_input blank, and the
        deck still generates because the server (a) auto-routes each file
        and (b) derives the target ASIN from the single-row Xray."""
        client, sf = self._make_client(internal_api_key="real-internal-key")
        target_xray = (
            b'Product Details,ASIN,URL,Image URL,Brand,Price  $,'
            b'ASIN Revenue,ASIN Sales,BSR,Ratings,Review Count\n'
            b'"My Target",B0TARGET01,https://amzn.com/dp/B0TARGET01,,MyBrand,29.99,$5000,200,1234,4.5,100\n'
        )
        # Reuse the existing _xray_csv() fixture as the competitor file.
        competitor_xray = _xray_csv()
        resp = client.post(
            "/api/admin/generate-deck",
            files=[
                ("csv_files", ("target.csv", target_xray, "text/csv")),
                ("csv_files", ("competitors.csv", competitor_xray, "text/csv")),
            ],
            data={
                # NOTE: target_product_input intentionally blank — server
                # should derive the ASIN from the target Xray.
                "target_product_input": "",
                "include_recommended_plan": "false",
                "include_growth_plan": "false",
            },
            headers={"X-Internal-API-Key": "real-internal-key"},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        # The auto-detect log is appended to warnings so the AE sees what
        # was routed to which slot.
        warnings = " ".join(body["details"].get("warnings", []) or [])
        self.assertIn("Auto-detected uploads", warnings)
        self.assertIn("target.csv", warnings)
        self.assertIn("competitors.csv", warnings)
        self.assertIn("target_xray", warnings)
        self.assertIn("competitor_xray", warnings)

    def test_target_product_input_required_when_no_target_xray(self) -> None:
        """Sanity check: blank target_product_input AND no target Xray
        should still 400 — we have no way to identify the prospect."""
        client, _sf = self._make_client(internal_api_key="real-internal-key")
        resp = client.post(
            "/api/admin/generate-deck",
            files=[
                ("csv_files", ("competitors.csv", _xray_csv(), "text/csv")),
            ],
            data={"target_product_input": ""},
            headers={"X-Internal-API-Key": "real-internal-key"},
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Target product is required", resp.text)

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


    def test_story_markdown_is_persisted_and_served_by_routes(self) -> None:
        """Generating a deck should persist `story_markdown` in summary_json
        and the two new routes should serve it as HTML and as a .md
        attachment."""
        client, sf = self._make_client()
        run_id, token, slug = self._seed_deck(sf)

        # 1. Persisted on the run row.
        with session_scope(sf) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.id == run_id)
            ).scalar_one()
            summary = dict(run.summary_json or {})
            story_md = str(summary.get("story_markdown") or "")
            self.assertTrue(story_md, "story_markdown should be saved on creation")
            # Required section headers from the canonical structure.
            self.assertIn("Executive summary", story_md)
            self.assertIn("Market & competitive landscape", story_md)
            self.assertIn("Search behavior & keyword opportunities", story_md)
            self.assertIn("Conversion & PDP", story_md)
            self.assertIn("Proposed offers & next step", story_md)

        # 2. HTML viewer route renders markdown as HTML.
        story_resp = client.get(f"/decks/{slug}/{run_id}/{token}/story")
        self.assertEqual(story_resp.status_code, 200)
        self.assertIn("text/html", story_resp.headers.get("content-type", ""))
        # Content from the markdown should appear in the rendered body.
        self.assertIn("Executive summary", story_resp.text)
        self.assertIn("/story.md", story_resp.text)  # download link present

        # 3. .md route returns the raw markdown with attachment headers.
        md_resp = client.get(f"/decks/{slug}/{run_id}/{token}/story.md")
        self.assertEqual(md_resp.status_code, 200)
        self.assertIn("text/markdown", md_resp.headers.get("content-type", ""))
        self.assertIn("attachment", md_resp.headers.get("content-disposition", "").lower())
        self.assertIn(f"{slug}-story.md", md_resp.headers.get("content-disposition", ""))
        self.assertIn("Executive summary", md_resp.text)

        # 4. Wrong token → 404 on both routes (defense-in-depth).
        bad_html = client.get(f"/decks/{slug}/{run_id}/WRONG-TOKEN/story")
        self.assertEqual(bad_html.status_code, 404)
        bad_md = client.get(f"/decks/{slug}/{run_id}/WRONG-TOKEN/story.md")
        self.assertEqual(bad_md.status_code, 404)


    def test_story_routes_serve_fallback_for_old_decks_without_story(self) -> None:
        """Older decks generated before PR27 don't have story_markdown saved.
        The Story routes must NOT 404 — they should render a fallback that
        explains the deck pre-dates the feature and links to re-generation."""
        client, sf = self._make_client()
        run_id, token, slug = self._seed_deck(sf)

        # Simulate a pre-PR27 deck by clearing the story_markdown field.
        with session_scope(sf) as session:
            run = session.execute(
                select(AutomationRun).where(AutomationRun.id == run_id)
            ).scalar_one()
            summary = dict(run.summary_json or {})
            summary.pop("story_markdown", None)
            run.summary_json = summary
            session.add(run)

        # HTML viewer returns a 200 with the fallback messaging.
        story_resp = client.get(f"/decks/{slug}/{run_id}/{token}/story")
        self.assertEqual(story_resp.status_code, 200)
        self.assertIn("Story not yet generated", story_resp.text)
        self.assertIn("Re-generate", story_resp.text)

        # The .md route also serves the fallback markdown (200, not 404).
        md_resp = client.get(f"/decks/{slug}/{run_id}/{token}/story.md")
        self.assertEqual(md_resp.status_code, 200)
        self.assertIn("Story not yet generated", md_resp.text)


if __name__ == "__main__":
    unittest.main()
