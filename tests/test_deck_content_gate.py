"""Content gate, storefront titles, and empty DTC market data."""

from __future__ import annotations

import re
import unittest
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services.deck.formatting import strip_site_title_noise
from sales_support_agent.services.deck.quality import (
    assess_deck_content,
    is_zero_dollar,
    public_deck_block_reason,
    scrub_dash_copy,
)

try:
    from sqlalchemy import select

    from sales_support_agent.models.database import create_session_factory, init_database, session_scope
    from sales_support_agent.models.entities import AutomationRun
    from sales_support_agent.services.deck_generator import DeckGenerationService
    from sales_support_agent.services.product_research import EnrichedHeroProduct
    from tests.test_deck_generator import _build_settings

    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "tests"}:
        raise
    SQLALCHEMY_AVAILABLE = False


class TitleNoiseTests(unittest.TestCase):
    def test_home_separator_and_tagline_drop_out(self) -> None:
        self.assertEqual(
            strip_site_title_noise("Home - Babaco Alarm Systems", brand="Babaco"),
            "Babaco Alarm Systems",
        )
        self.assertEqual(
            strip_site_title_noise("Shop | Official Site | Sonrei"),
            "Sonrei",
        )
        self.assertEqual(
            strip_site_title_noise(
                "Babaco Alarm Systems | The Most Trusted Name In Cargo Security Since 1930"
            ),
            "Babaco Alarm Systems",
        )

    def test_product_name_hyphen_stays(self) -> None:
        title = "Grip Stick - Hydrating Lip Treatment"
        self.assertEqual(strip_site_title_noise(title), title)


class ContentGateTests(unittest.TestCase):
    def test_flags_the_babaco_failure_modes(self) -> None:
        html = (
            "<h1>A $0 monthly category benchmark for this category.</h1>"
            "<p>Top 0 brands</p><p>Units sold</p><p>0</p>"
            "<p>0 listings tracked</p><p>Zantrex</p>"
        )
        findings = assess_deck_content(html)
        for item in ("$0", "this category", "Top 0", "zero metric", "Zantrex"):
            self.assertIn(item, findings)

    def test_allows_a_nonzero_cpc_and_a_free_trial(self) -> None:
        html = "<p>Free for 14 days. Planning CPC $0.15. Revenue $33K.</p>"
        self.assertEqual(assess_deck_content(html), [])
        self.assertTrue(is_zero_dollar("$0"))
        self.assertTrue(is_zero_dollar("$0.00"))
        self.assertFalse(is_zero_dollar("$0.15"))

    def test_scrub_replaces_dashes_outside_style(self) -> None:
        html = "<style>.a{content:'—'}</style><p>Months 1–3 — foundation</p>"
        scrubbed = scrub_dash_copy(html)
        self.assertIn("content:'—'", scrubbed)
        self.assertIn("Months 1 to 3, foundation", scrubbed)
        self.assertNotIn("\u2014", scrubbed.split("</style>", 1)[1])

    def test_flags_empty_coverage_category_mismatch_and_unsupported_claims(self) -> None:
        html = (
            "<meta name=\"anata-store-category\" content=\"Cargo security\">"
            "<span class='meta'>0 covered · 0 missing</span>"
            "<p class='val'>Unknown</p>"
            "<p>product details (materials, size and fit)</p>"
            "<p>The product already has a usable primary image.</p>"
            "<p>Price on the site: $42</p>"
        )
        findings = assess_deck_content(html)
        for item in ("coverage zero", "zero metric", "category mismatch", "unsupported image", "unsupported price"):
            self.assertIn(item, findings)

    def test_allows_matching_category_advice_and_supported_claims(self) -> None:
        html = (
            "<meta name=\"anata-store-category\" content=\"Merino socks\">"
            "<meta name=\"anata-scraped-support\" content=\"image price\">"
            "<span class='meta'>4 covered · 2 missing</span>"
            "<p class='val'>$24.00</p>"
            "<p>product details (materials, size and fit)</p>"
            "<p>The product already has a usable primary image.</p>"
            "<p>Price on the site: $24</p>"
        )
        self.assertEqual(assess_deck_content(html), [])

    def test_public_page_predicate_serves_dtc_and_holds_amazon_gaps(self) -> None:
        dtc = {
            "market_mode": "dtc",
            "content_gate_passed": True,
            "content_gate_findings": [],
            "evidence_status": "missing",
            "market_evidence_sufficient": False,
            "market_evidence_reason": "No competitor market export was supplied.",
        }
        self.assertEqual(public_deck_block_reason(dtc), "")
        amazon = dict(dtc, market_mode="amazon")
        self.assertIn("No competitor market export", public_deck_block_reason(amazon))
        failed = dict(dtc, content_gate_passed=False, content_gate_findings=["coverage zero"])
        self.assertIn("coverage zero", public_deck_block_reason(failed))


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required")
class EmptyMarketDeckTests(unittest.TestCase):
    def test_dtc_without_competitors_omits_zero_market_and_passes_the_gate(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        hero = EnrichedHeroProduct(
            asin="",
            candidate_asin="",
            brand_name="Babaco Alarm Systems",
            title="Home - Babaco Alarm Systems",
            source_url="https://babaco.com",
            description="Babaco builds cargo locks, alarms, and GPS tracking for commercial vehicles.",
            price="",
            dimensions="",
            image_url="https://example.com/babaco.jpg",
            product_type="Cargo security",
            bsr=None,
            rating=None,
            review_count=None,
            identity_source="website",
            market_metrics_source="",
            tags=(),
            warnings=(),
        )
        with session_scope(session_factory) as session:
            service = DeckGenerationService(_build_settings(), session)
            service.product_research.enrich_target_product = lambda _target: hero  # type: ignore[method-assign]
            result = service.generate_deck(
                target_product_input="https://babaco.com",
                include_recommended_plan=True,
                growth_plan_inputs={},
            )
        self.assertEqual(result.evidence_status, "missing")
        self.assertFalse(result.market_evidence_sufficient)
        self.assertTrue(result.content_gate_passed, result.content_gate_findings)
        self.assertEqual(result.design_title, "Babaco Alarm Systems x anata strategy deck")
        with session_scope(session_factory) as session:
            run = session.execute(select(AutomationRun)).scalar_one()
            html = str((run.summary_json or {}).get("deck_html") or "")
        self.assertIn("cargo locks", html)
        self.assertNotIn("Home", result.design_title)
        visible = re.sub(r"<style\b[^>]*>.*?</style>", " ", html, flags=re.I | re.S)
        visible = re.sub(r"<script\b[^>]*>.*?</script>", " ", visible, flags=re.I | re.S)
        self.assertNotIn("this category", visible.lower())
        self.assertNotIn("Top 0", visible)
        self.assertNotIn("Zantrex", visible)
        self.assertNotIn("$0", visible)
        self.assertNotIn("\u2014", visible)
        self.assertNotIn("\u2013", visible)
        self.assertEqual(result.market_mode, "dtc")
        self.assertNotIn("0 covered", visible)
        self.assertNotIn("0 missing", visible)
        self.assertNotIn("materials, size and fit", visible)
        self.assertNotIn("usable primary image", visible)
        self.assertNotIn("niche leaders", visible)
        self.assertNotIn("n/a ★", visible)
        self.assertNotIn("n/a &#9733;", visible.lower())
        self.assertNotIn("Price on the site", visible)
        self.assertIn("Amazon launch plan", visible)
        self.assertIn("Agency", visible)
        self.assertNotIn("Portfolio", visible)
        self.assertNotIn("Zantrex", visible)

    def test_public_page_serves_dtc_and_blocks_a_missing_amazon_export(self) -> None:
        from sales_support_agent.models.entities import AutomationRun
        from tests.test_deck_generator import DeckRoutingTests

        client, session_factory = DeckRoutingTests._make_client(self)
        hero = EnrichedHeroProduct(
            asin="",
            candidate_asin="",
            brand_name="Babaco Alarm Systems",
            title="Home - Babaco Alarm Systems",
            source_url="https://babaco.com",
            description="Babaco builds cargo locks, alarms, and GPS tracking for commercial vehicles.",
            price="",
            dimensions="",
            image_url="https://example.com/babaco.jpg",
            product_type="Cargo security",
            bsr=None,
            rating=None,
            review_count=None,
            identity_source="website",
            market_metrics_source="",
            tags=(),
            warnings=(),
        )
        with session_scope(session_factory) as session:
            service = DeckGenerationService(_build_settings(), session)
            service.product_research.enrich_target_product = lambda _target: hero  # type: ignore[method-assign]
            result = service.generate_deck(
                target_product_input="https://babaco.com",
                include_recommended_plan=True,
                growth_plan_inputs={},
            )
        self.assertEqual(result.market_mode, "dtc")
        self.assertTrue(result.content_gate_passed, result.content_gate_findings)
        with session_scope(session_factory) as session:
            run = session.execute(select(AutomationRun).where(AutomationRun.id == result.run_id)).scalar_one()
            summary = dict(run.summary_json or {})
        page = client.get(
            f"/decks/{summary['deck_slug']}/{result.run_id}/{summary['export_token']}"
        )
        self.assertEqual(page.status_code, 200, page.text[:800])
        self.assertIn("Amazon launch plan", page.text)
        self.assertNotIn("0 covered", page.text)
        self.assertNotIn("materials, size and fit", page.text)
        self.assertNotIn("usable primary image", page.text)

        with session_scope(session_factory) as session:
            blocked = AutomationRun(
                run_type="deck_generation",
                status="success",
                trigger="test",
                summary_json={
                    "export_token": "amazon-gap",
                    "deck_slug": "amazon-gap",
                    "deck_html": "<p>Amazon deck with a missing export.</p>",
                    "market_mode": "amazon",
                    "evidence_status": "missing",
                    "market_evidence_sufficient": False,
                    "market_evidence_reason": "The market export was expected and is missing.",
                    "content_gate_passed": True,
                    "content_gate_findings": [],
                },
            )
            session.add(blocked)
            session.flush()
            blocked_id = blocked.id
        blocked_page = client.get(f"/decks/amazon-gap/{blocked_id}/amazon-gap")
        self.assertEqual(blocked_page.status_code, 409, blocked_page.text)
        self.assertIn("still under review", blocked_page.text.lower())
        self.assertIn("market export was expected", blocked_page.text.lower())
