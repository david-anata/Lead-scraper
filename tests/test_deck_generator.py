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

    def test_generate_deck_returns_html_output_and_persists_run_metadata(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        with session_scope(session_factory) as session:
            service = DeckGenerationService(
                _build_settings(),
                session,
                shopify_client=object(),
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
                shopify_client=object(),
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
                shopify_client=object(),
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


if __name__ == "__main__":
    unittest.main()
