"""Deck value upgrades (SPEC C2): growth plan on for prospects, margin block,
per-listing reasoned recommendations, advisement CTA, and store-mode decks.

Network is never touched: product research is faked and no Anthropic key is set
in the test environment, so the reasoned-recommendation path deterministically
falls back to the fixed strings (proving the fallback is safe).
"""

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
    from sales_support_agent.services.deck.service import DeckGenerationService
    from sales_support_agent.services.deck.dataset import _build_margin_snapshot_html
    from sales_support_agent.services.product_research import EnrichedHeroProduct

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    DEPS = False


class _FakeAmazonClient:
    def is_configured(self) -> bool:
        return False


class _FakeProductResearch:
    """Returns a target with a real price + BSR so the margin block and the
    BSR-derived growth-plan math have data to render from."""

    def __init__(self, *, source_type_website: bool = False):
        self._website = source_type_website

    def enrich_target_product(self, target: dict) -> "EnrichedHeroProduct":
        asin = target.get("asin", "") or ("" if self._website else "B0TARGET01")
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


def _build_settings(*, booking_url: str = "") -> SimpleNamespace:
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
        amazon_sp_api_base_url="",
        amazon_sp_api_region="",
        amazon_sp_api_marketplace_id="",
        amazon_sp_api_lwa_client_id="",
        amazon_sp_api_lwa_client_secret="",
        amazon_sp_api_refresh_token="",
        amazon_sp_api_aws_access_key_id="",
        amazon_sp_api_aws_secret_access_key="",
        amazon_sp_api_aws_session_token="",
        shopify_request_timeout_seconds=15,
        shopify_user_agent="",
        marketing_booking_url=booking_url,
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


def _deck_html(session_factory) -> str:
    with session_scope(session_factory) as session:
        run = session.execute(
            select(AutomationRun).where(AutomationRun.run_type == "deck_generation")
        ).scalars().first()
        return str(dict(run.summary_json or {}).get("deck_html") or "")


@unittest.skipUnless(DEPS, "sqlalchemy is required for deck upgrade tests")
class DeckUpgradeTests(unittest.TestCase):
    # -- Task 1: growth plan renders for a prospect deck ------------------
    def test_growth_plan_renders_for_prospect_deck(self) -> None:
        """The marketing prospect flow now passes growth_plan_inputs={} (an
        empty dict, not None), so the 4-phase Growth Plan section must render."""
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        with session_scope(session_factory) as session:
            service = DeckGenerationService(_build_settings(), session, amazon_client=_FakeAmazonClient())
            service.product_research = _FakeProductResearch()
            service.generate_deck(
                competitor_xray_csv_bytes=_xray_csv(),
                competitor_xray_filename="xray.csv",
                keyword_xray_csv_bytes=_keyword_csv(),
                keyword_xray_filename="keywords.csv",
                target_product_input="B0TARGET01",
                growth_plan_inputs={},  # mirrors marketing_router prospect flow
            )
        html = _deck_html(session_factory)
        self.assertIn("growth-plan-slide", html)
        self.assertIn("Closing the gap", html)
        self.assertIn("Phase 1", html)
        self.assertIn("Phase 4", html)

    def test_growth_plan_absent_when_inputs_none(self) -> None:
        """Guard: with growth_plan_inputs=None (the old prospect behavior) the
        section does NOT render, proving the empty-dict switch is what turns it
        on."""
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        with session_scope(session_factory) as session:
            service = DeckGenerationService(_build_settings(), session, amazon_client=_FakeAmazonClient())
            service.product_research = _FakeProductResearch()
            service.generate_deck(
                competitor_xray_csv_bytes=_xray_csv(),
                competitor_xray_filename="xray.csv",
                target_product_input="B0TARGET01",
                growth_plan_inputs=None,
            )
        html = _deck_html(session_factory)
        self.assertNotIn("growth-plan-slide", html)

    # -- Task 2: margin block with estimates labeled ---------------------
    def test_margin_block_renders_with_estimates_labeled(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        with session_scope(session_factory) as session:
            service = DeckGenerationService(_build_settings(), session, amazon_client=_FakeAmazonClient())
            service.product_research = _FakeProductResearch()
            service.generate_deck(
                competitor_xray_csv_bytes=_xray_csv(),
                competitor_xray_filename="xray.csv",
                target_product_input="B0TARGET01",
                growth_plan_inputs={},
            )
        html = _deck_html(session_factory)
        self.assertIn("Margin snapshot", html)
        # 15% referral on $29.99 = $4.50; margin before fulfillment = $25.49.
        self.assertIn("4.50", html)
        self.assertIn("25.49", html)
        # Every derived figure must be visibly labeled estimated.
        self.assertIn("estimated", html.lower())
        self.assertIn("15%", html)

    def test_margin_snapshot_empty_when_no_price(self) -> None:
        self.assertEqual(_build_margin_snapshot_html(""), "")
        self.assertEqual(_build_margin_snapshot_html("Unavailable"), "")
        block = _build_margin_snapshot_html("$29.99")
        self.assertIn("Margin snapshot", block)
        self.assertIn("estimated", block.lower())
        # No em-dashes in shipped copy (CLAUDE.md voice rule).
        self.assertNotIn("—", block)

    # -- Task 4: advisement CTA on the deck page -------------------------
    def test_advisement_cta_present_when_booking_url_set(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        booking = "https://meetings.hubspot.com/anata/advisement"
        with session_scope(session_factory) as session:
            service = DeckGenerationService(
                _build_settings(booking_url=booking), session, amazon_client=_FakeAmazonClient()
            )
            service.product_research = _FakeProductResearch()
            service.generate_deck(
                competitor_xray_csv_bytes=_xray_csv(),
                competitor_xray_filename="xray.csv",
                target_product_input="B0TARGET01",
                growth_plan_inputs={},
            )
        html = _deck_html(session_factory)
        self.assertIn("Schedule a free advisement call", html)
        self.assertIn(booking, html)

    def test_advisement_cta_absent_when_booking_url_unset(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        with session_scope(session_factory) as session:
            service = DeckGenerationService(_build_settings(), session, amazon_client=_FakeAmazonClient())
            service.product_research = _FakeProductResearch()
            service.generate_deck(
                competitor_xray_csv_bytes=_xray_csv(),
                competitor_xray_filename="xray.csv",
                target_product_input="B0TARGET01",
                growth_plan_inputs={},
            )
        html = _deck_html(session_factory)
        self.assertNotIn("Schedule a free advisement call", html)

    # -- Task 5: store-mode deck (Phase 3D) ------------------------------
    def test_store_url_produces_deck_with_growth_plan(self) -> None:
        """A store URL (kind=store) goes through the deck pipeline's website
        (DTC) mode: it produces a real deck with no competitor Xray, populating
        the growth plan and recommendations while omitting Amazon-only pulls.
        This is exactly what _deliver_store_unlock now calls."""
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        with session_scope(session_factory) as session:
            service = DeckGenerationService(_build_settings(), session, amazon_client=_FakeAmazonClient())
            service.product_research = _FakeProductResearch(source_type_website=True)
            result = service.generate_deck(
                target_product_input="https://oceanrx.com",
                competitor_xray_csv_payloads=[],
                keyword_xray_csv_payloads=[],
                growth_plan_inputs={},
            )
        self.assertEqual(result.output_type, "html")
        html = _deck_html(session_factory)
        # A real deck rendered (not an ack), with the growth plan populated.
        self.assertIn("growth-plan-slide", html)
        self.assertIn("Margin snapshot", html)


if __name__ == "__main__":
    unittest.main()
