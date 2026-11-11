"""Catalog product facts, and the gate that rejects keyword-stuffed openers."""

from outbound_product_facts import (
    apply_product_facts,
    collect_product_facts,
    description_is_product,
    fetch_shopify_catalog,
    fetch_storeleads_record,
    domain_brand_mismatch,
    natural_product_phrase,
    parse_shopify_products,
    short_product_phrase,
    pick_representative,
    stuffed_copy_reason,
)
import outbound_openers as openers


class _Response:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def _product(title, product_type, *, available=True, tags=None, body=""):
    return {
        "title": title,
        "product_type": product_type,
        "tags": tags or [],
        "body_html": body,
        "vendor": "ArtisRaw",
        "variants": [{"available": available}],
    }


def test_shopify_parse_skips_non_products_and_keeps_stock_and_body():
    payload = {"products": [
        _product("Olive Wood Serving Board", "Boards", body="<p>Hand carved olive wood.</p>"),
        _product("Gift Card", "Gift Cards"),
        _product("Free Sample", "Samples"),
        _product("Shipping Protection", "Protection"),
        _product("Extended Warranty", "Warranty"),
        _product("Test Product", "Test"),
        _product("Order Fees", "Fees"),
        _product("Walnut Board", "Boards", available=False),
        _product("Small Board", "Boards"),
    ]}
    parsed = parse_shopify_products(payload)
    titles = [item["title"] for item in parsed]
    assert titles == ["Olive Wood Serving Board", "Walnut Board", "Small Board"]
    assert parsed[0]["in_stock"] is True
    assert parsed[1]["in_stock"] is False
    assert parsed[0]["body"] == "Hand carved olive wood."
    assert parsed[0]["product_type"] == "Boards"
    chosen = pick_representative(parsed)
    assert len(chosen) == 2
    assert all(item["in_stock"] for item in chosen)
    assert chosen[0]["product_type"] == "Boards"


def test_short_phrase_uses_the_core_noun_and_skips_the_wrong_product():
    assert short_product_phrase("Cherry Cola (12 Cans)", "Soda", "Perfy") == "the cherry cola"
    assert short_product_phrase("Ultra Slick Shave Cream", "", "RSL") == "the shave cream"
    assert short_product_phrase("Adai Dosa Mix", "Dosa Mix", "Adukale") == "the dosa mix"
    assert short_product_phrase("Cashmere Crew", "Cashmere Crews", "Colorush") == "the cashmere crews"
    assert short_product_phrase("Bicycle Unicorn Playing Cards", "Cards", "Jetset Gear") == ""
    assert short_product_phrase("Protective Bandages", "Skincare", "Miracle") == ""
    assert domain_brand_mismatch("popfunk.com", "Jay's Juniors", "Pop Funk")
    assert not domain_brand_mismatch("pedrogarcia.com", "Pedro García", "Pedro Garcia")
    assert not domain_brand_mismatch("rainshadowlabs.com", "RSL", "Rain Shadow Labs")


def test_best_selling_beats_the_first_catalog_item():
    calls = []

    def get(url, timeout, headers, allow_redirects):
        calls.append(url)
        if "sort_by=best-selling" in url:
            return _Response(200, {"products": [
                _product("Gift Card", "Gift Cards"),
                _product("Cherry Cola (12 Cans)", "Soda"),
            ]})
        if url.rstrip("/").endswith("drinkperfy.com"):
            return _Response(200, {})
        return _Response(404, {})

    facts = collect_product_facts(
        "drinkperfy.com", brand="Perfy", allow_storeleads=False,
        get=lambda url, **kwargs: get(url, **{k: kwargs[k] for k in ("timeout", "headers", "allow_redirects") if k in kwargs} if False else kwargs),
        sleep=lambda _s: None, guard=lambda url: url,
    )
    # The lambda above is awkward. Call get directly through a wrapper.
    del facts

    def getter(url, timeout=6, headers=None, allow_redirects=True):
        return get(url, timeout, headers or {}, allow_redirects)

    facts = collect_product_facts(
        "drinkperfy.com", brand="Perfy", allow_storeleads=False,
        get=getter, sleep=lambda _s: None, guard=lambda url: url, homepage_html="<html lang='en'></html>",
    )
    assert facts["pick"] == "best-selling"
    assert facts["phrase"] == "the cherry cola"
    assert any("best-selling" in url for url in calls)


def test_prompt_receives_only_the_short_phrase_and_tics_are_rejected():
    import outbound_openers as op
    import outbound_personalization as personalization

    sheet = personalization.fact_sheet({
        "brand": "Perfy",
        "recipe": "trial_offer",
        "campaign_id": "e5f5046b-07e4-41ad-a395-79cf8766ccd0",
        "store_profile": {
            "product_phrase": "the cherry cola",
            "most_recently_published_product": {"title": "Blood Orange Yuzu"},
            "description": "ADOPT A FRAILEJON",
        },
    })
    prompt = personalization._prompt(sheet)
    assert sheet.product == "the cherry cola"
    assert sheet.facts == ["the cherry cola"]
    assert "the cherry cola" in prompt
    assert "Blood Orange" not in prompt
    assert "recent product" not in prompt.lower()
    examples = prompt.split("Examples of the voice:", 1)[-1]
    assert "small set" not in examples.lower()
    assert "easy to picture" not in examples.lower()
    bad = "Blood Orange Yuzu is the recent Perfy can I noticed"
    assert personalization.style_violation(bad, recipe="trial_offer") == "repeated opener tic"
    assert not op.validate("A small set would give new shoppers a way to try it.")[0]
    assert op.variety_clash(
        "Cherry cola is the can a shopper would reach for first.",
        ["Cherry cola is the drink they already know."],
    ) == "repeats an opening"
    assert short_product_phrase("ADOPT A FRAILEJON", "Donation", "Paramo") == ""
    assert short_product_phrase("First Baptist Church", "Doormat", "Mats") == ""
    assert short_product_phrase("Somedays It Rocks Me", "Doormat", "Mats") == ""


def test_catalog_fetch_is_two_calls_with_no_retry():
    calls = []

    def get(url, timeout=5, headers=None, allow_redirects=True):
        calls.append((url, timeout))
        return _Response(404, {})

    collect_product_facts(
        "slow.example", brand="Slow", allow_storeleads=False,
        get=get, sleep=lambda _s: None, guard=lambda url: url,
    )
    assert len(calls) == 2
    assert "sort_by=best-selling" in calls[0][0]
    assert calls[0][0].endswith("/products.json?limit=50") or "products.json?limit=50" in calls[1][0]
    assert all(timeout <= 5 for _url, timeout in calls)


def test_natural_phrase_is_lowercase_with_an_article():
    assert natural_product_phrase("Olive Wood Serving Boards", "ArtisRaw") == "the olive wood serving boards"
    assert natural_product_phrase("Olive Wood Board - Large") == "the olive wood board"
    assert natural_product_phrase("Travel Lifestyle Store") == ""
    assert natural_product_phrase("Amplifying hip-hop influence") == ""
    assert natural_product_phrase("Natural Marine Extract Eric Lewis") == ""
    assert natural_product_phrase("Robot Vacuums, Smart Home") == ""
    assert natural_product_phrase("Protective Bandages MPB") == "the protective bandages"
    assert natural_product_phrase("Aardwolf Replacement Rubber Strips rrs") == ""
    assert natural_product_phrase("Pack Chocolate Chunk Cookie Mix") == "the chocolate chunk cookie mix"
    assert natural_product_phrase("Sirculit comprimidos naturales para piernas") == ""
    assert natural_product_phrase("Certified Organic CBDA Extract") == ""
    assert natural_product_phrase("Acci Anticelulítica Global") == ""
    assert natural_product_phrase("Order Fees") == ""


def test_homepage_keyword_strings_are_not_product_descriptions():
    assert not description_is_product("Travel Lifestyle Store | Official Site")
    assert not description_is_product("Health Conscious Body Care")
    assert not description_is_product("Robot Vacuums, Smart Home")
    assert not description_is_product("Amplifying hip-hop influence across generations.")
    assert description_is_product("Handmade olive wood serving boards for the table.")


def test_keyword_stuffed_lines_are_rejected():
    assert stuffed_copy_reason(
        "travel lifestyle store is the clearest offer from Jetset Gear"
    ) == "store word used as the product"
    assert stuffed_copy_reason(
        "Miracle Of The Sea makes natural marine extract eric lewis, which is an easy first product for a new shopper."
    ) == "person name used as a product"
    assert stuffed_copy_reason(
        "amplifying hip-hop influence is the clearest offer from Rock The Bells, and it belongs in the first image."
    ) == "slogan"
    assert stuffed_copy_reason(
        "Inova Professional makes keratin treatments salon hair care, which is an easy first product."
    ) == "noun pile"
    assert stuffed_copy_reason("Shop the line | official store") == "keyword string"
    good = "A new shopper should meet ArtisRaw through the olive wood serving boards."
    assert stuffed_copy_reason(good) == ""
    assert openers.validate(good, allow_amazon=False)[0]
    assert openers.opener_hold_reason(
        "travel lifestyle store is the clearest offer from Jetset Gear",
        {"recipe": "trial_offer"},
    )


def test_shopify_404_does_not_retry_and_500_retries_once():
    calls = {"n": 0}

    def get_404(url, timeout, headers, allow_redirects):
        calls["n"] += 1
        assert "products.json" in url
        assert "Chrome" in headers["User-Agent"]
        return _Response(404, {})

    assert fetch_shopify_catalog("shop.example", get=get_404, sleep=lambda _s: None, guard=lambda url: url) is None
    assert calls["n"] == 1

    def get_500(url, timeout, headers, allow_redirects):
        calls["n"] += 1
        if calls["n"] == 2:
            return _Response(500, {})
        return _Response(200, {"products": [_product("Olive Wood Board", "Boards"), _product("Walnut Board", "Boards")]})

    calls["n"] = 1
    found = fetch_shopify_catalog("shop.example", get=get_500, sleep=lambda _s: None, guard=lambda url: url)
    assert calls["n"] == 3
    assert found and found[0]["title"] == "Olive Wood Board"


def test_storeleads_key_stays_in_the_header():
    seen = {}

    def get(url, headers, timeout):
        seen["url"] = url
        seen["headers"] = dict(headers)
        return _Response(200, {"domain": {
            "name": "artisraw.com",
            "description": "Handmade olive wood serving boards for the table.",
            "most_recently_published_product": {"title": "Olive Wood Serving Board"},
            "api_key": "secret-key-should-not-leak",
        }})

    record = fetch_storeleads_record("artisraw.com", api_key="secret-key-should-not-leak", get=get)
    assert seen["headers"]["Authorization"] == "Bearer secret-key-should-not-leak"
    assert "secret-key-should-not-leak" not in str(record)
    assert "api_key" not in record
    facts = collect_product_facts(
        "artisraw.com",
        allow_storeleads=True,
        api_key="secret-key-should-not-leak",
        get=lambda url, **kwargs: _Response(404, {}) if "products.json" in url else get(url, **kwargs),
        sleep=lambda _s: None,
        guard=lambda url: url,
    )
    assert facts["source"] == "storeleads"
    assert facts["phrase"] == "the olive wood serving board"
    assert "secret-key" not in str(facts)


def test_catalog_product_beats_an_seo_title():
    lead = {
        "brand": "ArtisRaw",
        "domain": "artisraw.com",
        "recipe": "replatform",
        "store_profile": apply_product_facts(
            {"description": "travel lifestyle store", "title": "Amplifying hip-hop influence"},
            {
                "source": "products.json",
                "products": [{
                    "title": "Olive Wood Serving Board",
                    "product_type": "Boards",
                    "tags": ["olive"],
                    "body": "",
                    "in_stock": True,
                    "phrase": "the olive wood serving board",
                }],
                "phrase": "the olive wood serving board",
                "description": "",
            },
        ),
    }
    opener, angle, reason = openers.idea_decision(lead)
    assert reason == ""
    assert "olive wood serving board" in opener.lower()
    assert "lifestyle" not in opener.lower()
    assert "amplifying" not in opener.lower()
    assert "olive" not in (angle or "").lower()
    assert "amazon" not in opener.lower()
