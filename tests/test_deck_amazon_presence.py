"""Amazon presence, deck titles, follow-up dates, and the content gate."""

from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services.deck.amazon_presence import (
    ABSENT,
    PRESENT,
    UNKNOWN,
    AmazonPresence,
    _search_terms,
    asserts_amazon_absence,
    listing_plan_text,
    lookup_brand_on_amazon,
)
from sales_support_agent.services.deck.calendar import next_follow_up_date, us_federal_holidays
from sales_support_agent.services.deck.formatting import (
    clean_strategy_brand,
    scrub_scraped_page_chrome,
    strategy_deck_title,
)
from sales_support_agent.services.deck.quality import assess_deck_content

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


class _SearchClient:
    def __init__(self, payload, product=None):
        self.payload = payload
        self.product = product or {}
        self.searches: list[str] = []

    def search(self, term, page=1, amazon_domain=None):
        del page, amazon_domain
        self.searches.append(term)
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload

    def get_product(self, asin, amazon_domain=None):
        del asin, amazon_domain
        return self.product


def _presence(status: str, listings=()) -> AmazonPresence:
    return AmazonPresence(status=status, source="test", listings=tuple(listings))


class TitleCleanupTests(unittest.TestCase):
    def test_title_is_brand_without_duplicate_product_or_region(self) -> None:
        self.assertEqual(strategy_deck_title("Xvive Audio", "Xvive"), "Xvive Audio x anata strategy deck")
        self.assertEqual(strategy_deck_title("Elvy Charger"), "Elvy x anata strategy deck")
        self.assertEqual(strategy_deck_title("Earth Moon Magick Tarot"), "Earth Moon Magick x anata strategy deck")
        self.assertEqual(strategy_deck_title("fwee US"), "fwee x anata strategy deck")
        self.assertEqual(strategy_deck_title("Northstar", "Black"), "Northstar Black x anata strategy deck")
        self.assertEqual(
            clean_strategy_brand("Home - Babaco Alarm Systems"),
            "Babaco Alarm Systems",
        )

    def test_body_drops_home_page_and_contact_us_nav(self) -> None:
        text = "<p>Xvive Audio - Home Page</p><p>Contact Us Shop About FAQ We build wireless systems.</p>"
        cleaned = scrub_scraped_page_chrome(text)
        self.assertNotIn("Home Page", cleaned)
        self.assertNotIn("Contact Us", cleaned)
        self.assertIn("wireless systems", cleaned)
        self.assertIn("content:'—'", scrub_scraped_page_chrome("<style>.a{content:'—'}</style><p>Hi - Home Page</p>"))


class FollowUpDateTests(unittest.TestCase):
    def test_thanksgiving_2026_moves_to_friday(self) -> None:
        thanksgiving = date(2026, 11, 26)
        self.assertIn(thanksgiving, us_federal_holidays(2026))
        self.assertEqual(next_follow_up_date(date(2026, 11, 23)), date(2026, 11, 27))

    def test_weekend_and_observed_independence_day_are_skipped(self) -> None:
        self.assertEqual(next_follow_up_date(date(2026, 11, 19)), date(2026, 11, 23))
        self.assertIn(date(2026, 7, 3), us_federal_holidays(2026))
        self.assertEqual(next_follow_up_date(date(2026, 6, 30)), date(2026, 7, 6))

    def test_offer_section_does_not_name_thanksgiving(self) -> None:
        from sales_support_agent.services.deck.agency_sections import render_offer_section

        html = render_offer_section(
            offer_cards_html="",
            urls={"trial": "https://example.com/trial", "booking": "https://example.com/book"},
            generated=date(2026, 11, 23),
        )
        self.assertIn("November 27", html)
        self.assertNotIn("November 26", html)


class AmazonLookupTests(unittest.TestCase):
    def test_matching_brand_is_present_with_listing_facts(self) -> None:
        client = _SearchClient(
            {
                "request_info": {"success": True},
                "search_results": [
                    {
                        "asin": "B0DY5F6862",
                        "title": "IChoose Daily",
                        "brand": "IChoose",
                        "rating": 4.2,
                        "ratings_total": 10,
                        "price": {"raw": "$18.00"},
                        "image": "https://example.com/search.jpg",
                    }
                ],
            },
            product={
                "product": {
                    "title": "IChoose Daily Vitamin",
                    "brand": "IChoose Global",
                    "rating": 4.6,
                    "ratings_total": 812,
                    "buybox_winner": {"price": {"raw": "$24.00"}},
                    "images": [{"link": f"https://example.com/{i}.jpg"} for i in range(7)],
                }
            },
        )
        found = lookup_brand_on_amazon("IChoose Global", domain="ichooseglobal.com", client=client)
        self.assertEqual(found.status, PRESENT)
        self.assertEqual(found.listings[0]["asin"], "B0DY5F6862")
        self.assertEqual(found.listings[0]["title"], "IChoose Daily Vitamin")
        self.assertEqual(found.listings[0]["rating"], 4.6)
        self.assertEqual(found.listings[0]["review_count"], 812)
        self.assertEqual(found.listings[0]["price"], "$24.00")
        self.assertEqual(found.listings[0]["image_count"], 7)
        plan = listing_plan_text("IChoose Global", found.listings)
        for fact in ("B0DY5F6862", "IChoose Daily Vitamin", "4.6 stars", "812 reviews", "$24.00", "7 images"):
            self.assertIn(fact, plan)

    def test_empty_search_is_absent_and_a_miss_or_error_is_unknown(self) -> None:
        empty = lookup_brand_on_amazon(
            "Babaco",
            client=_SearchClient({"request_info": {"success": True}, "search_results": []}),
        )
        self.assertEqual(empty.status, ABSENT)
        unmatched = lookup_brand_on_amazon(
            "Babaco",
            client=_SearchClient({
                "request_info": {"success": True},
                "search_results": [{"asin": "B0OTHER0001", "title": "Other Lock", "brand": "Other Co"}],
            }),
        )
        self.assertEqual(unmatched.status, UNKNOWN)
        failed = lookup_brand_on_amazon("Babaco", client=_SearchClient(RuntimeError("down")))
        self.assertEqual(failed.status, UNKNOWN)
        self.assertFalse(asserts_amazon_absence("Babaco already sells on Amazon"))
        self.assertTrue(asserts_amazon_absence("Babaco is not an Amazon seller"))

    def test_brand_store_confirms_presence_and_fwee_does_not_search_benow(self) -> None:
        client = _SearchClient({
            "request_info": {"success": True},
            "search_results": [],
            "brand_store": {"title": "fwee", "link": "https://www.amazon.com/stores/fwee/page/ABC"},
        })
        found = lookup_brand_on_amazon("fwee", domain="https://benow.co.kr", client=client)
        self.assertEqual(found.status, PRESENT)
        self.assertEqual(found.source, "brand_store")
        self.assertEqual(client.searches, ["fwee"])
        self.assertEqual(_search_terms("fwee", "benow.co.kr"), ["fwee"])

    def test_xvive_brand_field_matches_the_longer_prospect_name(self) -> None:
        client = _SearchClient({
            "request_info": {"success": True},
            "search_results": [{
                "asin": "B07XVIVEU2",
                "title": "Xvive U2 Guitar Wireless System",
                "brand": "Xvive",
                "rating": 4.4,
                "ratings_total": 2200,
                "price": {"value": 149},
            }],
        })
        found = lookup_brand_on_amazon("Xvive Audio", domain="xviveaudio.com", client=client)
        self.assertEqual(found.status, PRESENT)
        self.assertEqual(found.listings[0]["asin"], "B07XVIVEU2")
        self.assertIn("xvive audio", client.searches[0])

    def test_a_failed_query_blocks_an_absence_claim(self) -> None:
        class _ByTerm:
            def search(self, term, page=1, amazon_domain=None):
                del page, amazon_domain
                if term == "xvive audio":
                    raise RuntimeError("timeout")
                return {"request_info": {"success": True}, "search_results": []}

        found = lookup_brand_on_amazon("Xvive Audio", domain="xviveaudio.com", client=_ByTerm())
        self.assertEqual(found.status, UNKNOWN)
        self.assertEqual(found.source, "incomplete")

    def test_a_shared_first_word_is_not_the_brand(self) -> None:
        for brand in ("Earth", None):
            item = {
                "asin": "B0EARTHBAG1",
                "title": "Earth Rated Dog Poop Bags",
            }
            if brand:
                item["brand"] = brand
            found = lookup_brand_on_amazon(
                "Earth Moon Magick",
                domain="earthmoonmagick.com",
                client=_SearchClient({"request_info": {"success": True}, "search_results": [item]}),
            )
            self.assertEqual(found.status, UNKNOWN, brand)
        real = lookup_brand_on_amazon(
            "Earth Moon Magick",
            domain="earthmoonmagick.com",
            client=_SearchClient({
                "request_info": {"success": True},
                "search_results": [{
                    "asin": "B0MAGICK01",
                    "title": "Earth Moon Magick Tarot Deck",
                    "brand": "Earth Moon Magick",
                }],
            }),
        )
        self.assertEqual(real.status, PRESENT)
        self.assertEqual(real.listings[0]["asin"], "B0MAGICK01")


class ContentGateAmazonTests(unittest.TestCase):
    def test_absence_claim_fails_unless_the_check_was_negative(self) -> None:
        html = "<p>Xvive Audio is not an Amazon seller. This brand does not sell on Amazon yet.</p>"
        self.assertIn("unconfirmed Amazon absence", assess_deck_content(html))
        self.assertIn("unconfirmed Amazon absence", assess_deck_content(html, amazon_presence="unknown"))
        self.assertIn("unconfirmed Amazon absence", assess_deck_content(html, amazon_presence="present"))
        self.assertNotIn("unconfirmed Amazon absence", assess_deck_content(html, amazon_presence="absent"))
        self.assertEqual(assess_deck_content("<p>Xvive Audio already sells on Amazon.</p>", amazon_presence="present"), [])


def _hero(brand: str, title: str, description: str) -> EnrichedHeroProduct:
    return EnrichedHeroProduct(
        asin="",
        candidate_asin="",
        brand_name=brand,
        title=title,
        source_url="https://xviveaudio.com",
        description=description,
        price="",
        dimensions="",
        image_url="https://example.com/xvive.jpg",
        product_type="Wireless audio",
        bsr=None,
        rating=None,
        review_count=None,
        identity_source="website",
        market_metrics_source="",
        tags=(),
        warnings=(),
    )


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required")
class DeckAmazonStatusTests(unittest.TestCase):
    def _generate(self, presence: AmazonPresence, *, brand: str = "Xvive Audio Xvive", title: str | None = None):
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        hero = _hero(
            brand,
            title or "Xvive Audio Xvive - Home Page",
            "Contact Us Shop About FAQ We build wireless guitar systems.",
        )
        with session_scope(session_factory) as session:
            service = DeckGenerationService(_build_settings(), session)
            service.product_research.enrich_target_product = lambda _target: hero  # type: ignore[method-assign]
            service.amazon_lookup = lambda brand_name, domain="": presence  # type: ignore[method-assign]
            result = service.generate_deck(
                target_product_input="https://xviveaudio.com",
                include_recommended_plan=True,
                growth_plan_inputs={},
            )
        with session_scope(session_factory) as session:
            run = session.execute(select(AutomationRun)).scalar_one()
            html = str((run.summary_json or {}).get("deck_html") or "")
        return result, html

    def test_confirmed_seller_gets_an_existing_seller_plan(self) -> None:
        presence = _presence(PRESENT, listings=({
            "asin": "B07XVIVEU2",
            "title": "Xvive U2 Guitar Wireless System",
            "rating": 4.4,
            "review_count": 2200,
            "price": "$149.00",
            "image_count": 6,
        },))
        result, html = self._generate(presence)
        self.assertEqual(result.design_title, "Xvive Audio x anata strategy deck")
        self.assertEqual(result.amazon_presence, PRESENT)
        self.assertTrue(result.content_gate_passed, result.content_gate_findings)
        self.assertIn("already sells on Amazon", html)
        for fact in ("B07XVIVEU2", "Xvive U2 Guitar Wireless System", "4.4 stars", "2,200 reviews", "$149.00", "6 images"):
            self.assertIn(fact, html)
        self.assertNotIn("not an Amazon seller", html)
        self.assertNotIn("does not sell on Amazon", html)
        self.assertNotIn("Home Page", html)
        self.assertNotIn("Contact Us", html)

    def test_confirmed_absence_may_say_so_and_passes_the_gate(self) -> None:
        result, html = self._generate(
            _presence(ABSENT),
            brand="Babaco Alarm Systems",
            title="Home - Babaco Alarm Systems",
        )
        self.assertEqual(result.design_title, "Babaco Alarm Systems x anata strategy deck")
        self.assertIn("not an Amazon seller", html)
        self.assertTrue(result.content_gate_passed, result.content_gate_findings)

    def test_unknown_presence_makes_no_amazon_claim(self) -> None:
        result, html = self._generate(_presence(UNKNOWN))
        self.assertNotIn("not an Amazon seller", html)
        self.assertNotIn("does not sell on Amazon", html)
        self.assertNotIn("no Amazon listing", html.lower())
        self.assertNotIn("already sells on Amazon", html)
        self.assertTrue(result.content_gate_passed, result.content_gate_findings)
        self.assertEqual(result.amazon_presence, UNKNOWN)


if __name__ == "__main__":
    unittest.main()
