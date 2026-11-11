"""Catalog product facts, and the gate that rejects keyword-stuffed openers."""

from outbound_product_facts import (
    apply_product_facts,
    blocked_category_reason,
    collect_product_facts,
    description_is_product,
    fetch_shopify_catalog,
    fetch_storeleads_record,
    domain_brand_mismatch,
    natural_product_phrase,
    hemp_is_product,
    phrase_is_grounded,
    phrase_is_natural,
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
        self.text = payload if isinstance(payload, str) else ""

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
        if "/collections/best-sellers/" in url:
            return _Response(200, {"products": [
                _product("Best Sellers (12 Cans)", "Bundles"),
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
    assert facts["products"][0]["title"] == "Cherry Cola (12 Cans)"
    assert any("/collections/best-sellers/" in url for url in calls)


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
    ) == ""
    assert op.variety_clash(
        "Cherry cola is the can a shopper would reach for first.",
        [
            "Cherry cola is the drink they already know.",
            "Cherry cola is the one I would buy again.",
        ],
    ) == "repeats an opening"
    assert op.variety_clash(
        "The soap smells like cedar.",
        ["The balm is strong.", "The cream is plain."],
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
    assert [url for url, _timeout in calls][:3] == [
        calls[0][0],
        calls[1][0],
        calls[2][0],
    ]
    assert calls[0][0].rstrip("/").endswith("slow.example")
    assert "/collections/best-sellers/" in calls[1][0]
    assert "/collections/bestsellers/" in calls[2][0]
    assert all(timeout <= 5 for _url, timeout in calls)
    assert not any("products.json?limit=50" in url for url, _timeout in calls)


def test_blocked_categories_skip_and_a_copper_peptide_serum_passes():
    import outbound_personalization as personalization

    assert blocked_category_reason("CBD oil", "Hemp") == "blocked_category"
    assert blocked_category_reason("Microdose gummies") == "blocked_category"
    assert blocked_category_reason("Psilocybin chocolate") == "blocked_category"
    assert blocked_category_reason("Functional mushroom capsules for mood") == "blocked_category"
    assert blocked_category_reason("BPC-157 research use only") == "blocked_category"
    assert blocked_category_reason("Semaglutide") == "blocked_category"
    assert blocked_category_reason("Tirzepatide") == "blocked_category"
    assert blocked_category_reason("Kratom powder") == "blocked_category"
    assert blocked_category_reason("Nicotine vape") == "blocked_category"
    assert blocked_category_reason("This formula can cure cancer") == "blocked_category"
    assert blocked_category_reason("Lion's mane mushroom mocktail") == "blocked_category"
    assert blocked_category_reason("Chaga reishi elixir") == "blocked_category"
    assert blocked_category_reason("Mushroom mocktails") == "blocked_category"
    assert blocked_category_reason("Porcini mushroom seasoning") == ""
    assert blocked_category_reason("Calming chews", "full spectrum CBD oil for dogs") == "blocked_category"
    assert blocked_category_reason("Cotton candy", "microdose", "THC gummies") == "blocked_category"
    assert blocked_category_reason("Shroom soda", "lion's mane and chaga") == "blocked_category"
    assert blocked_category_reason("superCerum", "copper peptide serum", "Skincare") == ""
    assert not phrase_is_grounded(
        "blood orange yuzu bright",
        [{"title": "Blood Orange Yuzu", "product_type": "Soda", "body": "Bright. Citrusy."}],
    )
    assert openers.missing_possessive("Paramo Snacks baked organic beet chips.", "Paramo Snacks")
    assert not openers.missing_possessive("Paramo Snacks' baked beet chips.", "Paramo Snacks")
    assert openers.brand_named_twice(
        "Rita Fine Jewelry has an infinity bracelet.",
        "The bracelet gives Rita Fine Jewelry a listing image.",
        "Rita Fine Jewelry",
    )
    assert personalization.style_violation(
        "New shoppers can spot the can.", recipe="trial_offer",
    ) == "guesses what shoppers will do"
    assert personalization.style_violation(
        "This brand has a smart bottle in a beverages lineup.",
    ) == "generic brand or category label"
    assert personalization.style_violation(
        "Your Amazon listing should lead with the serum.",
        role="angle", recipe="trial_offer", trial_offer=True,
    ) == "claims the brand already sells on Amazon"
    assert personalization.style_violation(
        "Saw the chips at Whole Foods while walking past.",
    ) == "made-up sighting"
    assert personalization.style_violation(
        "Saw the chips at Whole Foods while walking past.",
        retail=True,
    ) == ""
    assert personalization.style_violation(
        "Marine extract capsules could support total body health over time.",
    ) == "health claim"
    assert personalization.style_violation(
        "The chips seem like a plain snack made from the fruit.",
    ) == "put-down"
    assert personalization.style_violation(
        "The chess set is enough of a product to build the ad around.",
    ) == "put-down"
    assert personalization.style_violation(
        "The chess set has that single piece of wood look today.",
    ) == "garbled phrase"
    assert personalization.style_violation(
        "Blood Orange Yuzu looks like a nice change from the usual cola.",
        product_phrase="Blood Orange Yuzu",
    ) == "bare product title"
    assert personalization.style_violation(
        "The pillow looks like it would cool a hot neck fast.",
        catalog_text="Cooling pillow for side sleepers",
    ) == "guessed benefit"
    assert personalization.style_violation(
        "The bracelet should lead an Amazon listing for the brand.",
        role="angle", recipe="replatform",
    ) == "claims the brand already sells on Amazon"
    assert personalization.style_violation(
        "Launching the bracelet on Amazon gives the photo a home.",
        role="angle", recipe="amz_off",
    ) == ""
    assert openers.variety_clash(
        "", [],
        angle="The capsules would work as the main image in a listing.",
        angles=[
            "The cookie mix would work as the main image on an ad.",
            "The serum would work as the main image for the brand.",
        ],
    ) == "repeats an angle"
    assert blocked_category_reason("Copper peptide serum", "Skincare") == ""
    assert phrase_is_natural("24 hour gold keratin treatment")
    assert phrase_is_natural("the Amara necklace")
    assert phrase_is_natural("olive wood mug set of two")
    assert phrase_is_natural("welcome mat")
    assert phrase_is_grounded(
        "24 hour gold",
        [{"title": "24 Hour Gold Keratin Treatment", "product_type": "Treatment", "body": ""}],
    )
    assert phrase_is_natural("copper peptide serum")
    assert not phrase_is_natural("the spectrum nutrition with")
    assert not phrase_is_natural("the options hidden product")
    assert not phrase_is_natural("the google shopping")
    assert not phrase_is_natural("the head gel iws")
    products = [{"title": "Adai Dosa Mix", "product_type": "Mixes", "body": "A South Indian dosa mix"}]
    assert phrase_is_grounded("adai dosa mix", products)
    assert not phrase_is_grounded("blood orange yuzu", products)
    import outbound_personalization as personalization
    assert "Your adai dosa mix caught my eye." in personalization._EXAMPLES
    assert "The chocolate chunk cookie mix is the one I would come back for." in personalization._EXAMPLES
    assert "Love that Perfy does flavors like blood orange yuzu" in personalization._EXAMPLES


def test_handles_join_real_products_and_ignore_link_text():
    html = (" " * 180_000) + '<a href="/collections/all/products/cookie-mix">shop now</a>'
    catalog = {"products": [{
        "title": "Chocolate Chunk Cookie Mix",
        "handle": "cookie-mix",
        "product_type": "Mixes",
        "tags": [],
        "body_html": "<p>Just add water and oil.</p>",
        "variants": [{"available": True}],
    }]}

    def get(url, timeout=5, headers=None, allow_redirects=True):
        if "sort_by=best-selling" in url:
            return _Response(200, html)
        if "/products.json" in url:
            return _Response(200, catalog)
        if "/collections/best-sellers/" in url or "/collections/bestsellers/" in url:
            return _Response(200, {"products": []})
        return _Response(200, "<html lang='en'></html>")

    facts = collect_product_facts(
        "autumn.example", brand="Autumn Sons", allow_storeleads=False,
        get=get, sleep=lambda _s: None, guard=lambda url: url, homepage_html="<html></html>",
    )
    assert facts["products"]
    assert facts["products"][0]["title"] == "Chocolate Chunk Cookie Mix"
    assert "shop now" not in facts["products"][0]["title"].lower()
    assert facts["blocked"] == ""


def test_full_catalog_blocks_cbd_even_when_the_title_is_plain():
    html = '<a href="/products/calm-chews">Quick view</a>'
    catalog = {"products": [{
        "title": "Calming Chews",
        "handle": "calm-chews",
        "product_type": "Chews",
        "tags": ["dogs"],
        "body_html": "<p>Full spectrum CBD oil for anxious dogs.</p>",
        "variants": [{"available": True}],
    }]}

    def get(url, timeout=5, headers=None, allow_redirects=True):
        if "sort_by=best-selling" in url:
            return _Response(200, html)
        if "/products.json" in url:
            return _Response(200, catalog)
        if "best-sellers" in url or "bestsellers" in url:
            return _Response(200, {"products": []})
        return _Response(200, "<html></html>")

    facts = collect_product_facts(
        "chill.example", brand="Chill Paws", allow_storeleads=False,
        get=get, sleep=lambda _s: None, guard=lambda url: url, homepage_html="<html></html>",
    )
    assert facts["blocked"] == "blocked_category"
    assert facts["products"] == []


class _StreamedPage:
    """A response whose body can be read once, the way requests streams it."""

    def __init__(self, html: str):
        self.status_code = 200
        self._html = html
        self.text_reads = 0

    def iter_content(self, _size: int = 65536):
        yield self._html

    @property
    def text(self) -> str:
        self.text_reads += 1
        raise RuntimeError("The content for this response was already consumed")


def test_a_short_streamed_page_is_kept_and_the_us_check_still_runs():
    import outbound_pipeline as pipeline
    from outbound_product_facts import _html_text

    page = (
        '<html lang="en">Shopify.currency = {"active":"EUR","rate":"1.0"} '
        '<a href="/products/shoe">Bera</a></html>'
    )
    streamed = _StreamedPage(page)
    assert _html_text(streamed, max_bytes=3_000_000, stop_after_handles=15) == page
    assert streamed.text_reads == 0

    def get(url, timeout=5, headers=None, allow_redirects=True, stream=False):
        if url.rstrip("/").endswith("glo.example"):
            return _StreamedPage(page)
        return _Response(404, {})

    facts = collect_product_facts(
        "glo.example", brand="GLO", allow_storeleads=False,
        get=get, sleep=lambda _s: None, guard=lambda url: url,
    )
    assert facts["currency_code"] == "EUR"
    assert pipeline.copy_country_reason({
        "name": "glo.example",
        "merchant_name": "GLO",
        "country_code": "US",
        "currency_code": facts["currency_code"],
        "language_code": facts["language_code"],
    }) == "non_us"


def test_hemp_color_is_kept_and_a_hemp_product_is_blocked():
    assert hemp_is_product("Comfort Colors 1717", "Shirts", "Colors: Hemp, Ivory, Pepper") is False
    assert hemp_is_product("Hemp cord bracelet", "Jewelry", "") is False
    assert hemp_is_product("Organic Hemp Seed Oil", "Oils", "Cold pressed hemp oil") is True
    assert hemp_is_product("Nesa's Hemp") is True

    html = "<html><button>Hemp</button><button>Ivory</button></html>"
    catalog = {"products": [{
        "title": "Comfort Colors Heavyweight Tee",
        "handle": "cc-1717",
        "product_type": "Shirts",
        "tags": ["Hemp"],
        "body_html": "<p>A heavyweight cotton tee.</p>",
        "options": [{"name": "Color", "values": ["Hemp", "Ivory"]}],
        "variants": [{"available": True, "option1": "Hemp"}],
    }]}

    def get(url, timeout=5, headers=None, allow_redirects=True):
        if "sort_by=best-selling" in url:
            return _Response(200, '<a href="/products/cc-1717">Quick view</a>')
        if "/products.json" in url:
            return _Response(200, catalog)
        if "best-sellers" in url or "bestsellers" in url:
            return _Response(200, {"products": []})
        return _Response(200, html)

    facts = collect_product_facts(
        "htv.example", brand="American HTV", allow_storeleads=False,
        get=get, sleep=lambda _s: None, guard=lambda url: url, homepage_html=html,
    )
    assert facts["blocked"] == ""
    assert facts["products"]

    hemp = {"products": [{
        "title": "CBDA Extract",
        "handle": "cbda",
        "product_type": "Extract",
        "tags": [],
        "body_html": "<p>Organic hemp extract.</p>",
        "variants": [{"available": True}],
    }]}

    def hemp_get(url, timeout=5, headers=None, allow_redirects=True):
        if "/products.json" in url:
            return _Response(200, hemp)
        if "best-sellers" in url or "bestsellers" in url or "sort_by" in url:
            return _Response(200, {"products": []} if "json" in url else "")
        return _Response(200, "<html></html>")

    blocked = collect_product_facts(
        "nesa.example", brand="Nesa's Hemp", allow_storeleads=False,
        get=hemp_get, sleep=lambda _s: None, guard=lambda url: url, homepage_html="<html></html>",
    )
    assert blocked["blocked"] == "blocked_category"


def test_upsell_partner_and_duplicate_titles_are_dropped():
    catalog = {"products": [
        {"title": "Lens Only - Hip Care", "handle": "lens", "product_type": "Upsell",
         "tags": [], "body_html": "", "variants": [{"available": True}]},
        {"title": "Lewis", "handle": "lewis-1", "product_type": "Frames",
         "tags": [], "body_html": "<p>An acetate frame.</p>", "variants": [{"available": True}]},
        {"title": "Lewis", "handle": "lewis-2", "product_type": "Frames",
         "tags": [], "body_html": "<p>An acetate frame.</p>", "variants": [{"available": True}]},
        {"title": "Smart Bottle - Benefithub", "handle": "bottle", "product_type": "Bottle",
         "tags": [], "body_html": "", "variants": [{"available": True}]},
        {"title": "Day Pass - Abenity", "handle": "pass", "product_type": "Pass",
         "tags": [], "body_html": "", "variants": [{"available": True}]},
    ]}

    def get(url, timeout=5, headers=None, allow_redirects=True):
        if "/collections/best-sellers/" in url or "/collections/bestsellers/" in url:
            return _Response(200, catalog)
        return _Response(404, {})

    facts = collect_product_facts(
        "hip.example", brand="Hip Optical", allow_storeleads=False,
        get=get, sleep=lambda _s: None, guard=lambda url: url, homepage_html="<html></html>",
    )
    titles = [item["title"] for item in facts["products"]]
    assert titles == ["Lewis"]


def test_homepage_currency_is_read_before_copy():
    import outbound_pipeline as pipeline

    html = '<html lang="es">Shopify.currency = {"active":"EUR","rate":"1.0"}</html>'

    def get(url, timeout=5, headers=None, allow_redirects=True):
        if url.rstrip("/").endswith("pedrogarcia.com"):
            return _Response(200, html)
        if "best-selling" in url:
            return _Response(200, {"products": [_product("Bera Nappa", "Shoes")]})
        return _Response(404, {})

    facts = collect_product_facts(
        "pedrogarcia.com", brand="Pedro García", allow_storeleads=False,
        get=get, sleep=lambda _s: None, guard=lambda url: url,
    )
    assert facts["currency_code"] == "EUR"
    assert facts["language_code"] == "es"
    assert pipeline.copy_country_reason({
        "name": "pedrogarcia.com",
        "merchant_name": "Pedro García",
        "country_code": "US",
        "currency_code": facts["currency_code"],
        "language_code": facts["language_code"],
    }) == "non_us"


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
        get=lambda url, **kwargs: get(url, kwargs.get("headers"), kwargs.get("timeout")) if "storeleads" in url else _Response(404, {}),
        sleep=lambda _s: None,
        guard=lambda url: url,
    )
    assert facts["source"] == "storeleads"
    assert any("Olive Wood" in str(item.get("title") or "") for item in facts["products"])
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
