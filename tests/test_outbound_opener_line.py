"""The rebuilt opener: one product fact, one line, one grade."""

import json

from outbound_opener_line import (
    FAST_MODEL,
    LineFacts,
    compose_line,
    extract_detail,
    hard_reason,
    pick_line_facts,
)
from outbound_product_facts import blocked_category_reason, hemp_is_product


def test_a_reseller_catalog_is_skipped():
    products = [
        {"title": "Kitchen flange with stopper", "vendor": "Waterstone", "body": "A brass flange with stopper."},
        {"title": "Air gap", "vendor": "Waterstone", "body": "A brass air gap."},
        {"title": "Soap dispenser", "vendor": "ASPECT", "body": "A brass dispenser."},
    ]
    facts = pick_line_facts(products, brand="Plumbing Overstock", recipe="plan_upgrade")
    assert facts.skip == "reseller"


def test_hemp_in_a_color_list_is_not_a_hemp_product():
    colors = "Available Colors: Berry, Black, Granite, Hemp, Ivory, Navy, Pepper"
    assert hemp_is_product("Comfort Colors Blank T-Shirt", "Shirts", colors) is False
    assert hemp_is_product("Organic Hemp Seed Oil", "Oils", "Cold pressed hemp oil") is True


def test_ghk_copper_peptide_is_allowed_and_named():
    from outbound_opener_line import shopper_name

    title = "superPeptide GHK Copper Peptide Serum"
    assert blocked_category_reason(title, "Skincare") == ""
    assert "GHK" in shopper_name(title)
    product, detail, _kind = extract_detail(title, "A serum.")
    assert product and detail == ""
    product, detail, kind = extract_detail(title, "Light and made with hyaluronic acid.")
    assert "GHK" in product
    assert "hyaluronic" in detail
    assert kind == "detail"
    facts = LineFacts(brand="Pure & Cimple", product=product, detail=detail, recipe="amz_on")
    assert hard_reason(
        "The GHK copper peptide serum feels light on skin.",
        facts,
    ) == ""


def test_an_ananda_style_mushroom_drink_is_blocked():
    assert blocked_category_reason("Lion's mane mushroom mocktail", "Drinks") == "blocked_category"
    assert blocked_category_reason("Chaga reishi elixir") == "blocked_category"


def test_a_line_that_misses_the_product_is_skipped_after_one_rewrite():
    facts = LineFacts(brand="Inova", product="keratin treatment", detail="keratin", detail_kind="material", recipe="trial_offer")
    calls = {"writer": 0}

    def writer(_prompt):
        calls["writer"] += 1
        return "This is a nice specific piece for the bathroom shelf."

    def grader(_prompt):
        return json.dumps({"pass": True, "reason": "specific"})

    result = compose_line(facts, writer=writer, grader=grader)
    assert calls["writer"] == 2
    assert result.source == "skipped:grounding"
    assert result.line == ""


def test_amz_off_may_mention_launching_and_not_an_existing_store():
    facts = LineFacts(brand="Perfy", product="cherry cola", detail="cherry", detail_kind="flavor", recipe="amz_off")
    assert hard_reason("Launching the cherry cola on Amazon would give the flavor a first photo.", facts) == ""
    assert hard_reason("The cherry cola is already on Amazon for new buyers.", facts) == "amazon presence"
    trial = LineFacts(brand="Perfy", product="cherry cola", detail="cherry", detail_kind="flavor", recipe="trial_offer")
    assert hard_reason("The cherry cola belongs on an Amazon listing.", trial) == "amazon presence"


def test_grader_fail_gets_one_rewrite_then_skips():
    facts = LineFacts(
        brand="Biggs", product="coffee bar soap", detail="coffee", detail_kind="flavor", recipe="trial_offer",
    )
    calls = {"writer": 0, "grader": 0}

    def writer(_prompt):
        calls["writer"] += 1
        return "The coffee bar soap is worth a closer look."

    def grader(_prompt):
        calls["grader"] += 1
        return json.dumps({"pass": False, "reason": "filler"})

    result = compose_line(facts, writer=writer, grader=grader)
    assert calls["writer"] == 2
    assert calls["grader"] == 2
    assert result.source == "skipped:grader"
    assert result.grader_reason == "filler"
    assert result.line == ""


def test_a_grader_timeout_is_retried_once():
    from outbound_personalization import ModelTimeout

    facts = LineFacts(
        brand="Biggs", product="Coffee Bar", detail="vetiver and cedarwood",
        detail_kind="detail", recipe="trial_offer",
    )
    grades = {"n": 0}

    def writer(_prompt):
        return "Vetiver and cedarwood coffee bar from Biggs has real coffee brewed in."

    def grader(_prompt):
        grades["n"] += 1
        if grades["n"] == 1:
            raise ModelTimeout("grader slow")
        return json.dumps({"pass": True, "reason": "specific"})

    result = compose_line(facts, writer=writer, grader=grader)
    assert grades["n"] == 2
    assert result.source == "ai"
    assert "brewed" in result.line


def test_option_values_sizes_and_slogans_are_not_details():
    product, detail, _kind = extract_detail(
        "Variety Pack",
        "",
        [{"name": "Title", "values": ["Default Title"]}],
    )
    assert product == "Variety Pack" and detail == ""
    _product, detail, _kind = extract_detail(
        "Olive Wood Chess Set",
        "This handmade set measures 7 Inches (17 Cm).",
    )
    assert detail == ""
    _product, detail, _kind = extract_detail(
        "Infinity Bracelet",
        "Embrace the beauty of elegance with the infinity.",
    )
    assert detail == ""
    product, detail, _kind = extract_detail(
        "Coffee Bar",
        "🟦 Vetiver and cedarwood. 🟦 Deep wrinkle.",
    )
    assert "vetiver" in detail.lower() and "🟦" not in detail and "," not in detail


def test_pixel_in_the_product_name_is_not_backend_tech():
    facts = LineFacts(
        brand="Pix Perfect", product="Pixel Art Kit", detail="insert the pegs", recipe="amz_on",
    )
    assert hard_reason(
        "I like how the Pixel Art Kit lets you simply insert the pegs into the panels.",
        facts,
    ) == ""
    cola = LineFacts(brand="Perfy", product="Cherry Cola", detail="turmeric", recipe="trial_offer")
    assert hard_reason("They added a facebook pixel to the cherry cola shop.", cola) == "backend"


def test_a_long_added_to_cart_sentence_is_an_invented_action():
    facts = LineFacts(
        brand="Inova",
        product="Keratin Treatment",
        detail="one step",
        recipe="trial_offer",
    )
    assert hard_reason(
        "I just added the Inova Professional Keratin Treatment One-Step Kera-Quik to my cart.",
        facts,
    ) == "invented action"
    assert hard_reason("The keratin treatment is already in my cart.", facts) == "invented action"


def test_one_health_product_does_not_block_the_pillow_shop():
    from outbound_product_facts import catalog_blocked_reason, product_health_claim

    supplement = {
        "title": "Sleep Strips",
        "product_type": "Supplement",
        "body": "These strips support health with valerian and chamomile.",
    }
    pillow = {
        "title": "Snowtouch Pillow",
        "product_type": "Pillow",
        "body": "A gel pillow for side sleepers.",
    }
    assert product_health_claim(supplement)
    assert not product_health_claim(pillow)
    assert catalog_blocked_reason([pillow, pillow, supplement], brand="I Wanna Sleep") == ""
    majority = [supplement, supplement, pillow]
    assert catalog_blocked_reason(majority, brand="Miracle of the Sea") == "blocked_category"


def test_best_selling_handles_are_fetched_directly_and_junk_handles_are_skipped():
    from outbound_product_facts import collect_product_facts

    class Response:
        def __init__(self, status, payload):
            self.status_code = status
            self._payload = payload
            self.text = payload if isinstance(payload, str) else ""

        def json(self):
            return self._payload

    calls = []
    html = (
        '<a href="/products/front">Home</a>'
        '<a href="/products/lens-replacements">Lenses</a>'
        '<a href="/products/higgins">Higgins</a>'
    )
    frame = {
        "title": "Higgins",
        "handle": "higgins",
        "product_type": "Frames",
        "tags": [],
        "body_html": "<p>An acetate frame with a keyhole bridge.</p>",
        "variants": [{"available": True}],
        "vendor": "Hip Optical",
    }

    def get(url, timeout=5, headers=None, allow_redirects=True):
        calls.append(url)
        if "sort_by=best-selling" in url:
            return Response(200, html)
        if url.endswith("/products/higgins.json"):
            return Response(200, {"product": frame})
        if "best-sellers" in url or "bestsellers" in url:
            return Response(200, {"products": []})
        if "/products.json" in url:
            return Response(200, {"products": []})
        return Response(404, {})

    facts = collect_product_facts(
        "hip.example", brand="Hip Optical", allow_storeleads=False,
        get=get, sleep=lambda _s: None, guard=lambda url: url, homepage_html="<html></html>",
    )
    assert any(url.endswith("/products/higgins.json") for url in calls)
    assert not any(url.endswith("/products/front.json") for url in calls)
    assert not any(url.endswith("/products/lens-replacements.json") for url in calls)
    assert facts["products"] and facts["products"][0]["title"] == "Higgins"
    assert facts["pick"] == "best-selling"


def test_reseller_and_custom_labels_survive_without_a_best_seller_order():
    from outbound_product_facts import collect_product_facts

    class Response:
        def __init__(self, status, payload):
            self.status_code = status
            self._payload = payload
            self.text = payload if isinstance(payload, str) else ""

        def json(self):
            return self._payload

    waterstone = {"products": [
        {"title": "Air gap", "handle": "air-gap", "product_type": "Parts", "tags": [],
         "body_html": "A brass air gap.", "variants": [{"available": True}], "vendor": "Waterstone"},
        {"title": "Flange", "handle": "flange", "product_type": "Parts", "tags": [],
         "body_html": "A brass flange.", "variants": [{"available": True}], "vendor": "Waterstone"},
    ]}

    def plumbing(url, timeout=5, headers=None, allow_redirects=True):
        if "best-sellers" in url or "bestsellers" in url:
            return Response(200, {"products": []})
        if "/products.json" in url:
            return Response(200, waterstone)
        return Response(200, "<html></html>")

    facts = collect_product_facts(
        "plumbing.example", brand="Plumbing Overstock", allow_storeleads=False,
        get=plumbing, sleep=lambda _s: None, guard=lambda url: url, homepage_html="<html></html>",
    )
    assert facts["catalog_skip"] == "reseller"
    assert facts["products"] == []

    mats = {"products": [
        {"title": "TNT Pizza", "handle": "tnt", "product_type": "Doormat", "tags": [],
         "body_html": "A logo mat.", "variants": [{"available": True}], "vendor": "Logomats"},
        {"title": "Gem Pawn", "handle": "gem", "product_type": "Doormat", "tags": [],
         "body_html": "A logo mat.", "variants": [{"available": True}], "vendor": "Logomats"},
    ]}

    def doormats(url, timeout=5, headers=None, allow_redirects=True):
        if "best-sellers" in url or "bestsellers" in url:
            return Response(200, {"products": []})
        if "/products.json" in url:
            return Response(200, mats)
        return Response(200, "<html></html>")

    labeled = collect_product_facts(
        "mats.example", brand="Personalized Doormats", allow_storeleads=False,
        get=doormats, sleep=lambda _s: None, guard=lambda url: url, homepage_html="<html></html>",
    )
    assert labeled["catalog_skip"] == "custom"

    uploads = {"products": [
        {"title": "Gang Sheet", "handle": "gang", "product_type": "DTF", "tags": [],
         "body_html": "A print sheet.", "variants": [{"available": True}], "vendor": "Upload:"},
        {"title": "Blank Tee", "handle": "tee", "product_type": "Shirts", "tags": [],
         "body_html": "A cotton tee.", "variants": [{"available": True}], "vendor": "Builder:"},
    ]}

    def htv(url, timeout=5, headers=None, allow_redirects=True):
        if "best-sellers" in url or "bestsellers" in url:
            return Response(200, {"products": []})
        if "/products.json" in url:
            return Response(200, uploads)
        return Response(200, "<html></html>")

    plain = collect_product_facts(
        "htv2.example", brand="American HTV", allow_storeleads=False,
        get=htv, sleep=lambda _s: None, guard=lambda url: url, homepage_html="<html></html>",
    )
    assert plain["catalog_skip"] == ""


def test_the_default_model_is_the_fast_non_reasoning_grok():
    from outbound_opener_line import GRADER_MODEL

    assert FAST_MODEL == "grok-4.20-0309-non-reasoning"
    assert GRADER_MODEL == "grok-4.3"


def test_a_detail_comes_from_the_body_and_not_the_product_name():
    from outbound_opener_line import shopper_name

    assert shopper_name("Perfy Cherry Cola (12 Cans)") == "Perfy Cherry Cola"
    assert shopper_name("Coffee Bar® Single 3.5 oz Packaged Bar") == "Coffee Bar"
    assert shopper_name("Organic Baked Beet and Yacon Chips") == "Organic Baked Beet and Yacon Chips"
    assert shopper_name("Inova Professional Keratin Treatment One-Step Kera-Quik. 0% formaldehyde") == (
        "Inova Professional Keratin Treatment One-Step Kera-Quik"
    )
    assert shopper_name("2018 Edition - Pixel Art Kit by Pix Perfect") == "Pixel Art Kit"

    product, detail, _kind = extract_detail(
        "Coffee Bar® Single 3.5 oz Packaged Bar",
        "Vetiver & Cedarwood, real coffee brewed in.",
    )
    assert product == "Coffee Bar"
    assert "vetiver" in detail.lower() and "cedarwood" in detail.lower()

    product, detail, _kind = extract_detail("Cherry Cola (12 Cans)", "A cherry soda.")
    assert detail == ""

    product, detail, _kind = extract_detail(
        "Live Edge Olive Wood Chess Set",
        "Carved from one piece. Hidden storage drawer.",
    )
    assert "carved" in detail.lower()
    assert "," not in detail
    assert "drawer" not in detail.lower()

    product, detail, _kind = extract_detail(
        "2018 Edition - Pixel Art Kit by Pix Perfect",
        "Comes with this unique and fun craft experience.",
    )
    assert detail == ""

    product, detail, _kind = extract_detail(
        "Infinity Diamond Bracelet",
        "Set with 14K gold, diamonds, and blue topaz.",
    )
    assert "topaz" in detail.lower()


def test_grounding_uses_the_product_nouns_not_every_title_word():
    facts = LineFacts(
        brand="Inova",
        product="Keratin Treatment",
        detail="no formaldehyde",
        recipe="trial_offer",
    )
    assert hard_reason("The keratin treatment is the one I would mention to a friend.", facts) == ""


def test_invented_actions_hype_and_soap_flavor_are_rejected():
    soap = LineFacts(brand="Biggs", product="Coffee Bar", detail="vetiver and cedarwood", recipe="trial_offer")
    assert hard_reason("The coffee bar comes in a coffee flavor.", soap) == "flavor"
    chips = LineFacts(brand="Paramo", product="Beet Chips", detail="yacon", recipe="amz_on")
    assert hard_reason("I just bought these beet chips on Amazon.", chips) == "invented action"
    assert hard_reason("I added the beet chips to my cart.", chips) == "invented action"
    assert hard_reason("The beet chips look perfect on the table.", chips) == "hype"
    cola = LineFacts(brand="Perfy", product="Cherry Cola", detail="turmeric", recipe="amz_off")
    assert hard_reason("Launching the cherry cola on Amazon would suit the turmeric.", cola) == ""


def test_category_vendors_are_not_resellers_and_custom_goods_are_labeled():
    frames = [
        {"title": "Higgins", "vendor": "Women Tinted", "body": "An acetate frame with a keyhole bridge."},
        {"title": "Jax", "vendor": "Men Polarized", "body": "A titanium frame with spring hinges."},
    ]
    assert pick_line_facts(frames, brand="Hip Optical").skip != "reseller"
    mats = [
        {"title": "TNT Pizza", "vendor": "Logomats", "body": "A logo mat for the shop."},
        {"title": "Gem Pawnbrokers", "vendor": "Logomats", "body": "A logo mat for the shop."},
    ]
    assert pick_line_facts(mats, brand="Personalized Doormats").skip == "custom"


def test_design_names_are_not_drug_claims_and_health_copy_is_blocked():
    from outbound_product_facts import catalog_blocked_reason

    designs = [
        {"title": "Tackle Breast Cancer Football", "product_type": "DTF Transfers", "body": ""},
        {"title": "Peace Love Cure Sunflower Skull", "product_type": "DTF Transfers", "body": ""},
        {"title": "Comfort Colors Tee", "product_type": "Shirts", "body": "Available Colors: Berry, Black, Hemp, Ivory."},
    ]
    assert catalog_blocked_reason(designs, brand="American HTV") == ""
    supplement = [{
        "title": "Marine Extract Capsules",
        "product_type": "Capsules",
        "body": "Marine extracts improve total body health and the copy mentions lip cancer.",
    }]
    assert catalog_blocked_reason(supplement, brand="Miracle of the Sea") == "blocked_category"
    serum = [{"title": "GHK Copper Peptide Serum", "product_type": "Skincare", "body": "A cosmetic serum."}]
    assert catalog_blocked_reason(serum, brand="Pure & Cimple") == ""


def test_without_a_best_selling_order_the_catalog_is_not_used():
    from outbound_product_facts import collect_product_facts

    class Response:
        def __init__(self, status, payload):
            self.status_code = status
            self._payload = payload
            self.text = payload if isinstance(payload, str) else ""

        def json(self):
            return self._payload

    catalog = {"products": [
        {"title": "Zebra Tile", "handle": "zebra", "product_type": "Tile", "tags": [],
         "body_html": "A glossy glass tile.", "variants": [{"available": True}], "vendor": "LYSM"},
        {"title": "Alpha Tile", "handle": "alpha", "product_type": "Tile", "tags": [],
         "body_html": "A matte stone tile.", "variants": [{"available": True}], "vendor": "LYSM"},
    ]}

    def get(url, timeout=5, headers=None, allow_redirects=True):
        if "best-sellers" in url or "bestsellers" in url:
            return Response(200, {"products": []})
        if "/products.json" in url:
            return Response(200, catalog)
        return Response(200, "<html lang='en'></html>")

    facts = collect_product_facts(
        "tile.example", brand="American Tile Depot", allow_storeleads=False,
        get=get, sleep=lambda _s: None, guard=lambda url: url, homepage_html="<html lang='en'></html>",
    )
    assert facts["products"] == []
    assert facts["pick"] != "catalog"
