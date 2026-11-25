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
    def test_thanksgiving_2026_skips_black_friday(self) -> None:
        thanksgiving = date(2026, 11, 26)
        self.assertIn(thanksgiving, us_federal_holidays(2026))
        self.assertEqual(next_follow_up_date(date(2026, 11, 23)), date(2026, 11, 30))
        self.assertNotEqual(next_follow_up_date(date(2026, 11, 23)), date(2026, 11, 27))

    def test_year_end_break_is_skipped(self) -> None:
        self.assertEqual(next_follow_up_date(date(2026, 12, 21)), date(2027, 1, 4))
        self.assertEqual(next_follow_up_date(date(2026, 12, 28)), date(2027, 1, 4))
        self.assertEqual(next_follow_up_date(date(2026, 12, 1)), date(2026, 12, 4))

    def test_weekend_and_observed_independence_day_are_skipped(self) -> None:
        self.assertEqual(next_follow_up_date(date(2026, 11, 19)), date(2026, 11, 23))
        self.assertIn(date(2026, 7, 3), us_federal_holidays(2026))
        self.assertEqual(next_follow_up_date(date(2026, 6, 30)), date(2026, 7, 6))

    def test_offer_section_does_not_name_thanksgiving_or_black_friday(self) -> None:
        from sales_support_agent.services.deck.agency_sections import render_offer_section

        html = render_offer_section(
            offer_cards_html="",
            urls={"trial": "https://example.com/trial", "booking": "https://example.com/book"},
            generated=date(2026, 11, 23),
        )
        self.assertIn("November 30", html)
        self.assertNotIn("November 26", html)
        self.assertNotIn("November 27", html)


class _IChooseClient:
    """IChoose listings come back for the short brand, not the legal name or the domain stem."""

    def __init__(self, *, fail_short: bool = False, only_domain: bool = False):
        self.searches: list[str] = []
        self.fail_short = fail_short
        self.only_domain = only_domain

    def _hits(self) -> dict:
        return {
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
                },
                {
                    "asin": "B0GJVKZJH4",
                    "title": "IChoose Weekly Pack",
                    "brand": {"name": "ICHOOSE"},
                    "rating": 4.4,
                    "ratings_total": 40,
                    "price": {"value": 28},
                },
                {
                    "asin": "B0DYBLDF1F",
                    "title": "IChoose Travel Kit",
                    "brand": "I Choose",
                    "rating": 4.1,
                    "ratings_total": 12,
                    "price": {"raw": "$16.00"},
                },
            ],
        }

    def search(self, term, page=1, amazon_domain=None):
        del page, amazon_domain
        self.searches.append(term)
        key = term.lower()
        if self.only_domain:
            return self._hits() if key == "ichooseglobal" else {"request_info": {"success": True}, "search_results": []}
        if key in {"ichoose", "i choose"}:
            if self.fail_short:
                raise TimeoutError("amazon search timed out")
            return self._hits()
        if key == "ichoose global":
            return {
                "request_info": {"success": True},
                "search_results": [{
                    "asin": "B0OTHERGLOB",
                    "title": "Global Choice Planner",
                    "brand": "Global Choice",
                }],
            }
        return {"request_info": {"success": True}, "search_results": []}

    def get_product(self, asin, amazon_domain=None):
        del amazon_domain
        if asin != "B0DY5F6862":
            return {}
        return {
            "product": {
                "title": "IChoose Daily Vitamin",
                "brand": "IChoose",
                "rating": 4.6,
                "ratings_total": 812,
                "buybox_winner": {"price": {"raw": "$24.00"}},
                "images": [{"link": f"https://example.com/{i}.jpg"} for i in range(7)],
            }
        }


class AmazonLookupTests(unittest.TestCase):
    def test_ichoose_global_matches_the_short_brand_field(self) -> None:
        client = _IChooseClient()
        found = lookup_brand_on_amazon("IChoose Global", domain="ichooseglobal.com", client=client)
        self.assertEqual(found.status, PRESENT, found.detail)
        self.assertIn("ichoose", client.searches)
        self.assertEqual(
            [item["asin"] for item in found.listings],
            ["B0DY5F6862", "B0GJVKZJH4", "B0DYBLDF1F"],
        )
        self.assertEqual(found.listings[0]["title"], "IChoose Daily Vitamin")
        self.assertEqual(found.listings[0]["rating"], 4.6)
        self.assertEqual(found.listings[0]["review_count"], 812)
        self.assertEqual(found.listings[0]["price"], "$24.00")
        self.assertEqual(found.listings[0]["image_count"], 7)
        plan = listing_plan_text("IChoose Global", found.listings)
        for fact in ("B0DY5F6862", "IChoose Daily Vitamin", "4.6 stars", "812 reviews", "$24.00", "7 images"):
            self.assertIn(fact, plan)

    def test_concatenated_domain_still_matches_ichoose_brand_field(self) -> None:
        client = _IChooseClient(only_domain=True)
        found = lookup_brand_on_amazon("IChoose Global", domain="ichooseglobal.com", client=client)
        self.assertEqual(found.status, PRESENT, found.detail)
        self.assertIn("B0DY5F6862", [item["asin"] for item in found.listings])

    def test_ichoose_timeout_on_the_short_query_stays_unknown(self) -> None:
        found = lookup_brand_on_amazon(
            "IChoose Global",
            domain="ichooseglobal.com",
            client=_IChooseClient(fail_short=True),
        )
        self.assertEqual(found.status, UNKNOWN)
        self.assertEqual(found.source, "incomplete")

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
            client = _SearchClient({"request_info": {"success": True}, "search_results": [item]})
            found = lookup_brand_on_amazon(
                "Earth Moon Magick",
                domain="earthmoonmagick.com",
                client=client,
            )
            self.assertEqual(found.status, UNKNOWN, brand)
            self.assertNotIn("earth", client.searches)
            self.assertTrue(any(term.startswith("earth moon") for term in client.searches))
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

    def test_present_deck_fails_launch_wording_duplicates_and_assumed_fees(self) -> None:
        launch = "<p>Xvive Audio already sells on Amazon.</p><h2>Where Xvive Audio can enter</h2><p>Amazon launch plan. What to build before launch.</p>"
        self.assertIn("launch wording on an Amazon seller", assess_deck_content(launch, amazon_presence="present"))
        self.assertNotIn(
            "launch wording on an Amazon seller",
            assess_deck_content(launch, amazon_presence="absent"),
        )
        doubled = "<p>Xvive Audio already sells on Amazon. Xvive Audio already sells on Amazon.</p>"
        self.assertIn("duplicated deck copy", assess_deck_content(doubled, amazon_presence="present"))
        repeated = (
            "<p>Xvive U2 Guitar Wireless System, ASIN B07XVIVEU2, 4.4 stars, 2,200 reviews, $149.00, 6 images.</p>"
            "<p>Xvive U2 Guitar Wireless System, ASIN B07XVIVEU2, 4.4 stars, 2,200 reviews, $149.00, 6 images.</p>"
        )
        self.assertIn("duplicated deck copy", assess_deck_content(repeated))
        fee = "<p>Amazon referral fee $30.00, estimated at 15%.</p>"
        self.assertIn("assumed referral fee", assess_deck_content(fee))
        self.assertIn("assumed referral fee", assess_deck_content(
            "<p class='lab'>Amazon referral fee</p><p class='val'>$30.00</p><p class='sub'>estimated at 15%, an assumed rate for category audio</p>"
        ))


def _hero(
    brand: str,
    title: str,
    description: str,
    *,
    source_url: str = "https://xviveaudio.com",
    product_type: str = "Wireless audio",
    asin: str = "",
) -> EnrichedHeroProduct:
    return EnrichedHeroProduct(
        asin=asin,
        candidate_asin="",
        brand_name=brand,
        title=title,
        source_url=source_url,
        description=description,
        price="",
        dimensions="",
        image_url="https://example.com/product.jpg",
        product_type=product_type,
        bsr=None,
        rating=None,
        review_count=None,
        identity_source="website",
        market_metrics_source="",
        tags=(),
        warnings=(),
    )


_LAUNCH_PHRASES = (
    "sells on its own site",
    "amazon launch plan",
    "launch page",
    "before launch",
    "market entry",
    "can enter",
    "would enter",
    "entry point",
    "amazon entry",
    "first launch",
    "listing is live",
    "walk through the launch",
    "what to build before",
)


_SELLER_FIXTURES = (
    {
        "brand": "Xvive Audio",
        "source": "https://xviveaudio.com",
        "title": "Xvive Audio - Home Page",
        "description": "We build wireless guitar systems for working players.",
        "product_type": "Wireless audio",
        "listings": ({
            "asin": "B07XVIVEU2",
            "title": "Xvive U2 Guitar Wireless System",
            "rating": 4.4,
            "review_count": 2200,
            "price": "$149.00",
            "image_count": 6,
        },),
    },
    {
        "brand": "elvy.ai",
        "source": "https://elvy.ai",
        "title": "elvy.ai",
        "description": "elvy builds software teams use every day.",
        "product_type": "Software",
        "listings": ({
            "asin": "B0ELVYAI001",
            "title": "elvy team workspace",
            "rating": 4.3,
            "review_count": 86,
            "price": "$49.00",
            "image_count": 3,
        },),
    },
    {
        "brand": "Earth Moon Magick",
        "source": "https://earthmoonmagick.com",
        "title": "Earth Moon Magick",
        "description": "Earth Moon Magick makes tarot and ritual goods.",
        "product_type": "Tarot",
        "listings": ({
            "asin": "B0MAGICK01",
            "title": "Earth Moon Magick Tarot Deck",
            "rating": 4.8,
            "review_count": 140,
            "price": "$32.00",
            "image_count": 5,
        },),
    },
    {
        "brand": "fwee",
        "source": "https://benow.co.kr",
        "title": "fwee",
        "description": "fwee makes color cosmetics.",
        "product_type": "Cosmetics",
        "listings": ({
            "asin": "B0FWEE0001",
            "title": "fwee lip tint",
            "rating": 4.5,
            "review_count": 960,
            "price": "$18.00",
            "image_count": 8,
        },),
    },
)


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required")
class DeckAmazonStatusTests(unittest.TestCase):
    def _generate(
        self,
        presence: AmazonPresence,
        *,
        brand: str = "Xvive Audio",
        title: str | None = None,
        description: str = "We build wireless guitar systems for working players.",
        source_url: str = "https://xviveaudio.com",
        product_type: str = "Wireless audio",
        asin: str = "",
    ):
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)
        hero = _hero(
            brand,
            title or brand,
            description,
            source_url=source_url,
            product_type=product_type,
            asin=asin,
        )
        with session_scope(session_factory) as session:
            service = DeckGenerationService(_build_settings(), session)
            service.product_research.enrich_target_product = lambda _target: hero  # type: ignore[method-assign]
            service.amazon_lookup = lambda brand_name, domain="": presence  # type: ignore[method-assign]
            result = service.generate_deck(
                target_product_input=source_url,
                include_recommended_plan=True,
                growth_plan_inputs={},
            )
        with session_scope(session_factory) as session:
            run = session.execute(select(AutomationRun)).scalar_one()
            html = str((run.summary_json or {}).get("deck_html") or "")
        return result, html

    def test_confirmed_seller_gets_an_existing_seller_plan(self) -> None:
        for fixture in _SELLER_FIXTURES:
            with self.subTest(brand=fixture["brand"]):
                listings = fixture["listings"]
                presence = _presence(PRESENT, listings=listings)
                result, html = self._generate(
                    presence,
                    brand=fixture["brand"],
                    title=fixture["title"],
                    description=fixture["description"],
                    source_url=fixture["source"],
                    product_type=fixture["product_type"],
                )
                visible = html.lower()
                self.assertEqual(result.amazon_presence, PRESENT)
                self.assertTrue(result.content_gate_passed, result.content_gate_findings)
                self.assertEqual(visible.count("already sells on amazon"), 1)
                line = (
                    f"{listings[0]['title']}, ASIN {listings[0]['asin']}, "
                    f"{listings[0]['rating']:.1f} stars, {listings[0]['review_count']:,} reviews, "
                    f"{listings[0]['price']}, {listings[0]['image_count']} images"
                )
                self.assertEqual(html.count(line), 1, line)
                self.assertIn("Which ASINs to focus on", html)
                self.assertIn("Ads structure for the live catalog", html)
                self.assertIn("tracks conversion", html.lower())
                self.assertIn("images", html.lower())
                for phrase in _LAUNCH_PHRASES:
                    self.assertNotIn(phrase, visible, phrase)
                self.assertNotIn("Amazon referral fee", html)
                self.assertNotIn("assumed rate", html.lower())
        xvive = next(item for item in _SELLER_FIXTURES if item["brand"] == "Xvive Audio")
        result, html = self._generate(
            _presence(PRESENT, listings=xvive["listings"]),
            brand="Xvive Audio Xvive",
            title="Xvive Audio Xvive - Home Page",
            description="Contact Us Shop About FAQ We build wireless guitar systems.",
        )
        self.assertEqual(result.design_title, "Xvive Audio x anata strategy deck")
        self.assertNotIn("Home Page", html)
        self.assertNotIn("Contact Us", html)

    def test_attached_asin_on_a_store_url_uses_the_existing_seller_plan(self) -> None:
        result, html = self._generate(
            _presence(UNKNOWN),
            brand="IChoose Global",
            title="IChoose Global",
            description="IChoose Global sells daily vitamins on its site.",
            source_url="https://ichooseglobal.com",
            product_type="Vitamins",
            asin="B0DY5F6862",
        )
        # The generator replaces the injected lookup when the hero carries an ASIN.
        self.assertEqual(result.amazon_presence, PRESENT)
        self.assertTrue(result.content_gate_passed, result.content_gate_findings)
        self.assertIn("B0DY5F6862", html)
        self.assertNotIn("amazon launch plan", html.lower())
        self.assertNotIn("listing is live", html.lower())
        self.assertNotIn("before launch", html.lower())

    def test_present_seller_with_a_market_export_does_not_deny_the_export(self) -> None:
        from types import SimpleNamespace

        from sales_support_agent.services.deck.dataset import _build_expected_impact_summary

        supplied = _build_expected_impact_summary(
            SimpleNamespace(market_evidence_sufficient=True, under_75_reviews_count=4),
            market_mode="amazon",
            amazon_status="present",
        )
        self.assertNotIn("no market export", supplied.lower())
        self.assertIn("under 75 reviews", supplied)
        self.assertIn("live listings", supplied)
        withheld = _build_expected_impact_summary(
            SimpleNamespace(market_evidence_sufficient=False, under_75_reviews_count=0),
            market_mode="dtc",
            amazon_status="present",
        )
        self.assertIn("no market export was supplied", withheld.lower())

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
