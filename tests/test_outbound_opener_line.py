"""The rebuilt opener: one product fact, one line, one grade."""

import json

from outbound_opener_line import (
    FAST_MODEL,
    LineFacts,
    compose_line,
    extract_detail,
    grader_prompt,
    hard_reason,
    pick_line_facts,
    writer_prompt,
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
    assert result.grader_reason == "identical"
    assert result.line == ""


def test_a_grader_timeout_is_retried_once():
    from outbound_personalization import ModelTimeout

    facts = LineFacts(
        brand="Biggs", product="Coffee Bar", detail="vetiver and cedarwood",
        detail_kind="detail", recipe="trial_offer",
    )
    grades = {"n": 0}

    def writer(_prompt):
        return "Vetiver and cedarwood in this coffee bar from Biggs is brewed for the cup."

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


def test_a_full_sentence_is_the_detail_and_a_pillow_line_is_kept():
    body = (
        "The cover zips off. "
        "Add or remove fill until your hips, shoulders, and spine finally line up."
    )
    product, detail, _kind = extract_detail("SnowTouch Full-Body Pillow", body, brand="I Wanna Sleep")
    assert product == "SnowTouch Full-Body Pillow"
    assert detail.lower().startswith("add or remove fill")
    assert detail.endswith("line up")
    assert "sna" not in detail


def test_name_plurals_and_the_brand_are_not_a_new_detail():
    product, detail, _kind = extract_detail(
        "Chocolate Chunk Cookie Mixes",
        "A Chocolate Chunk Cookie Mix.",
        brand="Autumn Sons",
    )
    assert detail == ""


def test_hype_words_inside_the_brand_are_allowed():
    facts = LineFacts(
        brand="Pix Perfect", product="Pixel Art Kit", detail="insert the pegs into the panels",
        recipe="amz_on",
    )
    assert hard_reason(
        "The Pixel Art Kit from Pix Perfect lets you insert the pegs into the panels.",
        facts,
    ) == ""
    cola = LineFacts(brand="Perfy", product="Cherry Cola", detail="turmeric", recipe="trial_offer")
    assert hard_reason("The cherry cola looks perfect.", cola) == "hype"


def test_an_identical_retry_is_a_fail():
    facts = LineFacts(
        brand="Perfy", product="Cherry Cola", detail="blood orange yuzu and turmeric",
        detail_kind="flavor", recipe="trial_offer",
    )

    def writer(prompt):
        assert "different sentence" in prompt or "Rejected line" not in prompt
        return "The cherry cola has blood orange yuzu and turmeric."

    def grader(_prompt):
        assert "Source:" in _prompt
        return json.dumps({"pass": False, "reason": "filler"})

    result = compose_line(facts, writer=writer, grader=grader)
    assert result.source == "skipped:grader"
    assert result.grader_reason == "identical"


def test_a_drug_claim_on_the_subject_blocks_the_brand_and_pain_does_not():
    gel = {
        "title": "Aloe Vera Gel",
        "body": "Useful for wound healing, inflammation, infection, and acne scars.",
    }
    pillow = {
        "title": "SnowTouch Pillow",
        "body": "Add or remove fill until your hips, shoulders, and spine finally line up.",
    }
    sore = {"title": "Neck Pillow", "body": "The perfect cooling pillow for neck and back pain."}
    assert pick_line_facts([gel], brand="Miracle of the Sea").skip == "blocked_category"
    chosen = pick_line_facts([sore, pillow], brand="I Wanna Sleep")
    assert chosen.skip == ""
    assert "SnowTouch" in chosen.product
    assert "pain" not in chosen.detail.lower()


def test_marks_are_stripped_and_spare_parts_are_not_the_subject():
    facts = LineFacts(
        brand="Miracle", product="Aloe Vera Gel", detail="almond oil", recipe="replatform",
    )

    def writer(_prompt):
        return "I like how Miracle of the Sea™ blends the gel."

    def grader(_prompt):
        return json.dumps({"pass": True, "reason": "specific"})

    result = compose_line(facts, writer=writer, grader=grader)
    assert "™" not in result.line and "®" not in result.line

    vacuum = {"title": "Home Vacuum", "body": "The vacuum maps the room before it starts."}
    brush = {"title": "W10 Side Brushes 2-pack", "product_type": "Replacement", "body": "A side brush."}
    picked = pick_line_facts([brush, vacuum], brand="Dreame")
    assert "Vacuum" in picked.product
    assert pick_line_facts([brush], brand="Dreame").skip == "no_product_detail"


def test_addon_vendors_are_ignored_and_one_custom_product_is_not_the_shop():
    frames = [
        {"title": "Higgins", "vendor": "Lens Replacement Page"},
        {"title": "Jax", "vendor": "Lens Coating"},
        {"title": "Melvin", "vendor": "Buy 1"},
        {"title": "Orlo", "vendor": "Hip Optical"},
    ]
    assert pick_line_facts(frames, brand="Hip Optical").skip != "reseller"
    chess = [
        {"title": "Olive Wood Chess Set", "vendor": "ArtisRaw"},
        {"title": "Olive Wood Mug", "vendor": "ArtisRaw"},
        {"title": "Personalized Olive Wood Board", "vendor": "ArtisRaw"},
        {"title": "Olive Wood Bowl", "vendor": "ArtisRaw"},
    ]
    assert pick_line_facts(chess, brand="ArtisRaw").skip != "custom"


def test_water_io_is_not_renamed_this_brand():
    import outbound_personalization as personalization

    sheet = personalization.fact_sheet({
        "brand": "Water.io",
        "display_brand": "Water.io",
        "domain": "water.io",
    })
    assert sheet.brand == "Water.io"
    assert sheet.brand != "This brand"


def test_the_grader_treats_the_brand_as_known_and_the_line_stays_short():
    facts = LineFacts(
        brand="Autumn Sons Baking",
        product="Chocolate Chunk Cookie Mix",
        detail="The only ingredients you need are a small amount of water and oil.",
        recipe="amz_on",
    )
    prompt = grader_prompt(facts, "This Chocolate Chunk Cookie Mix from Autumn Sons Baking only needs a small amount of water and oil.")
    assert "Brand: Autumn Sons Baking" in prompt
    assert "known" in prompt.lower()
    assert "paraphrase" in prompt.lower()
    assert hard_reason(
        "This shampoo-style keratin treatment helps reduce frizz control volume improve manageability and enhance shine while allowing same-day rinse and same-day color service today.",
        LineFacts(brand="Inova", product="Keratin Treatment", detail="reduces frizz", recipe="trial_offer"),
    ) == "too long"


def test_personal_use_is_rejected_and_an_opinion_is_kept():
    facts = LineFacts(
        brand="I Wanna Sleep", product="SnowTouch Pillow",
        detail="Add or remove fill until your hips and shoulders line up.",
        recipe="trial_offer",
    )
    assert hard_reason("The SnowTouch pillow lets me add or remove fill.", facts) == "invented action"
    assert hard_reason("I picked up the SnowTouch pillow yesterday.", facts) == "invented action"
    assert hard_reason("Love that the SnowTouch pillow lets you add or remove fill.", facts) == ""
    cola = LineFacts(brand="Perfy", product="Cherry Cola", detail="turmeric", recipe="trial_offer")
    assert hard_reason("I like the cherry cola.", cola) == ""


def test_a_near_duplicate_retry_moves_to_the_next_detail():
    first = LineFacts(brand="Perfy", product="Cherry Cola", detail="blood orange yuzu", recipe="trial_offer")
    second = LineFacts(brand="Perfy", product="Variety Pack", detail="fruit punch and tropical citrus", recipe="trial_offer")
    seen = {"n": 0}

    def writer(prompt):
        seen["n"] += 1
        if "Variety Pack" in prompt:
            return "The variety pack includes fruit punch and tropical citrus."
        return "The cherry cola includes blood orange yuzu."

    def grader(prompt):
        if "Variety Pack" in prompt or "fruit punch" in prompt:
            return json.dumps({"pass": True, "reason": "specific"})
        return json.dumps({"pass": False, "reason": "catalog copy"})

    result = compose_line(first, writer=writer, grader=grader, alternatives=[second])
    assert result.source == "ai"
    assert "variety pack" in result.line.lower()
    assert seen["n"] >= 3


def test_domains_stay_whole_bullets_split_and_benefits_lose():
    _product, detail, _kind = extract_detail(
        "Daily+",
        "Water.io is a smart bottle.",
        brand="Water.io",
    )
    assert "water.io" in detail.lower()
    assert not detail.lower().startswith("io")
    _product, detail, _kind = extract_detail(
        "superCerum",
        "Skin firming and elasticity. Made with GHK and collagen.",
        brand="Pure & Cimple",
    )
    assert "ghk" in detail.lower() or "collagen" in detail.lower()
    assert "firming" not in detail.lower()
    _product, detail, _kind = extract_detail(
        "Pixel Art Kit",
        "Insert the pegs into the panel. (You will have plenty left over.)",
        brand="Pix Perfect",
    )
    assert "pegs" in detail.lower()
    assert "plenty" not in detail.lower()


def test_brush_covers_are_spares_and_an_all_accessory_list_is_labeled():
    from outbound_product_facts import collect_product_facts, spare_part

    assert spare_part({"title": "D10 Plus Main Brush Cover", "product_type": "Parts"})
    vacuum = {"title": "L10s Ultra Robot Vacuum", "body": "The robot maps the room before it starts."}
    cover = {"title": "D10 Plus Main Brush Cover", "product_type": "Parts", "body": "A cover for the brush."}
    picked = pick_line_facts([cover, vacuum], brand="Dreame")
    assert picked.product == "L10s Ultra"

    class Response:
        def __init__(self, status, payload):
            self.status_code = status
            self._payload = payload
            self.text = payload if isinstance(payload, str) else ""

        def json(self):
            return self._payload

    plates = {"products": [
        {"title": "Carrara Switch Plate", "handle": "carrara", "product_type": "Accessories",
         "tags": ["Accessories"], "body_html": "A marble plate.", "variants": [{"available": True}],
         "vendor": "American Tile Depot"},
        {"title": "Sample Plate", "handle": "sample-plate", "product_type": "Accessories",
         "tags": ["Accessories"], "body_html": "A sample plate.", "variants": [{"available": True}],
         "vendor": "American Tile Depot"},
    ]}

    def get(url, timeout=5, headers=None, allow_redirects=True):
        if "best-sellers" in url or "bestsellers" in url:
            return Response(200, plates)
        return Response(404, {})

    facts = collect_product_facts(
        "tile.example", brand="American Tile Depot", allow_storeleads=False,
        get=get, sleep=lambda _s: None, guard=lambda url: url, homepage_html="<html></html>",
    )
    assert facts["catalog_skip"] == "no_bestseller_product"
    assert facts["products"] == []


def test_voice_grader_hype_words_and_an_unsupported_blend():
    facts = LineFacts(
        brand="Autumn Sons Baking", product="Chocolate Chunk Cookie Mix",
        detail="The only ingredients you need are a small amount of water and oil.",
        recipe="amz_on",
    )
    prompt = grader_prompt(facts, "Chocolate Chunk Cookie Mixes from Autumn Sons Baking need just water and oil.")
    assert "two checks" in prompt.lower()
    assert "mesmerizing cascade" in prompt
    assert "60 days" in prompt
    assert "water and oil" in prompt
    climber = LineFacts(brand="Rita", product="Hana Ear Climber", detail="a cascade of shimmer", recipe="replatform")
    assert hard_reason("The Hana ear climber creates a mesmerizing cascade.", climber) == "hype"
    pack = LineFacts(brand="Perfy", product="Variety Pack", detail="Fruit Punch and Blood Orange Yuzu.", recipe="trial_offer")
    assert hard_reason("The variety pack blends fruit punch and blood orange yuzu.", pack) == "unsupported verb"
    assert hard_reason("superPeptide serum calms redness.", LineFacts(product="SuperPeptide Serum", brand="Pure")) == "lowercase opening"


def test_yacon_outranks_a_dietary_bullet_and_a_benefit_only_body_is_skipped():
    _name, detail, _kind = extract_detail(
        "Baked Organic Beet and Yacon Chips",
        "Oil-free and gluten-free. Yacon is the Andean root in every chip.",
        brand="Paramo Snacks",
    )
    assert "yacon" in detail.lower()
    assert "gluten" not in detail.lower()
    serum = {"title": "GHK Copper Peptide Serum", "body": "Calming redness and soothing sensitive skin. Deep wrinkle reduction."}
    chips = {"title": "Beet Chips", "body": "Yacon is the root in the chip."}
    picked = pick_line_facts([serum, chips], brand="Pure & Cimple")
    assert "Beet" in picked.product or "yacon" in picked.detail.lower()


def test_a_capitalized_bullet_is_its_own_sentence_and_material_wins():
    _name, detail, _kind = extract_detail(
        "Higgins",
        "Crafted from premium acetate and stainless steel Ultra-light high quality prescription lenses",
        brand="Hip Optical",
    )
    assert "acetate" in detail.lower()
    assert "ultra-light" not in detail.lower()


def test_accessory_tagged_switch_plates_stay_when_they_are_the_shop():
    from outbound_product_facts import collect_product_facts

    class Response:
        def __init__(self, status, payload):
            self.status_code = status
            self._payload = payload
            self.text = payload if isinstance(payload, str) else ""

        def json(self):
            return self._payload

    plates = {"products": [
        {"title": f"Plate {index}", "handle": f"plate-{index}", "product_type": "Accessories",
         "tags": ["Accessories"], "body_html": "Cut from marble.", "variants": [{"available": True}],
         "vendor": "American Tile Depot"}
        for index in range(3)
    ]}

    def get(url, timeout=5, headers=None, allow_redirects=True):
        if "best-sellers" in url or "bestsellers" in url:
            return Response(200, plates)
        return Response(404, {})

    facts = collect_product_facts(
        "plates.example", brand="American Tile Depot", allow_storeleads=False,
        get=get, sleep=lambda _s: None, guard=lambda url: url, homepage_html="<html></html>",
    )
    assert facts["catalog_skip"] != "no_bestseller_product"
    assert facts["products"]


def test_handle_fetches_stop_when_the_lookup_budget_is_spent():
    from outbound_product_facts import products_for_handles

    calls = []

    def get(url, timeout=5, headers=None, allow_redirects=True):
        calls.append(url)
        return type("Response", (), {"status_code": 404, "text": "", "json": lambda self: {}})()

    assert products_for_handles("slow.example", ["higgins", "jax"], get=get, sleep=lambda _s: None, budget=0) == []
    assert calls == []


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
        "One-Step Kera-Quik"
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
    assert hard_reason("The coffee bar comes in a citrus flavor.", soap) == "flavor"
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


def test_grounding_accepts_the_noun_plus_one_distinguishing_word():
    chess = LineFacts(brand="ArtisRaw", product="Olive Wood Chess Set", detail="A hidden storage drawer.", recipe="amz_off")
    assert hard_reason("Nice that the olive wood chess set has a storage drawer.", chess) == ""
    assert hard_reason("Nice that the chess set has a storage drawer.", chess) == ""
    assert hard_reason("Nice that the storage drawer holds all 32 hand-turned olive wood pieces.", chess) == "grounding"
    pillow = LineFacts(
        brand="I Wanna Sleep", product="SnowTouch Pillow",
        detail="Add or remove fill until your hips, shoulders, and spine finally line up.",
        recipe="trial_offer",
    )
    assert hard_reason("Nice that the SnowTouch pillow lets you add or remove fill.", pillow) == ""
    assert hard_reason("Nice that you can add or remove fill until your hips line up.", pillow) == "grounding"
    vacuum = LineFacts(brand="Dreame", product="L10s Ultra", detail="It cleans and dries its own mops.", recipe="amz_on")
    assert hard_reason("Nice that the L10s Ultra cleans and dries its own mops.", vacuum) == ""
    assert hard_reason("Nice that it automatically cleans and dries its own mops.", vacuum) == "grounding"


def test_short_names_drop_filler_and_keep_the_model():
    from outbound_opener_line import shopper_name

    assert shopper_name("Dreame L10s Ultra Robot Vacuum") == "L10s Ultra"
    assert shopper_name("SnowTouch Adjustable Full-Body Pillow") == "SnowTouch Pillow"
    assert shopper_name("Handmade Olive Wood Chess Set") == "Olive Wood Chess Set"


def test_the_writer_may_only_use_the_source_and_drops_marketing_words():
    facts = LineFacts(
        brand="Hip Optical", product="Higgins",
        detail="Crafted from premium acetate and stainless steel.", recipe="replatform",
    )
    prompt = writer_prompt(facts)
    assert "only facts" in prompt.lower()
    assert "Higgins" in prompt
    for word in ("elegantly", "gracefully", "effortlessly", "premium"):
        assert word in prompt.lower()
    climber = LineFacts(brand="Rita", product="Hana Ear Climber", detail="ascends along the ear", recipe="replatform")
    assert hard_reason("Noticed how the Hana Ear Climber elegantly climbs the ear.", climber) == "hype"
    assert hard_reason("Noticed how the Hana Ear Climber gracefully climbs the ear.", climber) == "hype"
    assert hard_reason("Noticed how the Hana Ear Climber effortlessly climbs the ear.", climber) == "hype"


def test_a_cause_line_names_the_brand():
    chips = LineFacts(
        brand="Paramo", product="Beet Chips",
        detail="2% to Save Páramos & Water For a Good Cause", recipe="amz_on",
    )
    prompt = writer_prompt(chips)
    assert "Paramo" in prompt
    assert "not the product" in prompt
    assert hard_reason("Nice that Paramo gives 2% to help save the páramos.", chips) == ""
    assert hard_reason("Nice that these beet chips donate 2% to save the páramos.", chips) == "cause actor"


def test_a_three_word_fragment_is_dropped_and_scent_outranks_skin_feel():
    assert extract_detail("superCerum", "Skin", brand="Pure & Cimple")[1] == ""
    serum = {
        "title": "superCerum",
        "body": "Skin. Calming redness. Skin firming and elasticity.",
    }
    chips = {"title": "Beet Chips", "body": "Yacon is the root in the chip."}
    picked = pick_line_facts([serum, chips], brand="Pure & Cimple")
    assert "Beet" in picked.product
    assert pick_line_facts([serum], brand="Pure & Cimple").skip == "no_product_detail"
    _name, detail, _kind = extract_detail(
        "Coffee Bar",
        "Leaves skin feeling fresh and extra clean. "
        "Fold in ground beans for a heavy-duty scrub. "
        "Smoky vetiver and warm cedarwood.",
        brand="Biggs",
    )
    assert "feeling fresh" not in detail.lower()
    assert "vetiver" in detail.lower() or "cedarwood" in detail.lower() or "ground" in detail.lower()


def test_the_grader_allows_paraphrase_and_sensory_lines():
    facts = LineFacts(
        brand="Autumn Sons Baking", product="Chocolate Chunk Cookie Mix",
        detail="The only ingredients you need are a small amount of water and oil.",
        recipe="amz_on",
    )
    prompt = grader_prompt(facts, "Noticed these mixes need a splash of water and oil.")
    assert "splash" in prompt
    assert "leaves skin feeling fresh" in prompt
    assert "medical" in prompt.lower()
    assert "gentle" in prompt.lower()


def test_accessories_and_no_sample_tags_survive_parse():
    from outbound_product_facts import _skip_product, collect_product_facts, parse_shopify_products

    assert _skip_product("Carrara Switch Plate", "Accessories", ["Accessories", "No Sample"]) is False
    assert _skip_product("Sample Plate", "Accessories", ["Accessories"]) is True
    payload = {"products": [{
        "title": "Italian Carrara White Marble Switch Plate",
        "product_type": "Marble Wall/Switch Plate",
        "tags": ["Accessories", "No Sample", "MATERIAL_Marble"],
        "body_html": "Cut from Carrara marble.",
        "variants": [{"available": True}],
        "vendor": "OTS",
    }]}
    assert len(parse_shopify_products(payload)) == 1
    plates = [
        {
            "title": f"{name} Switch Plate",
            "vendor": code,
            "product_type": "Accessories",
            "tags": ["Accessories", "No Sample"],
            "body": "Cut from Carrara marble.",
        }
        for name, code in (("Carrara", "OTS"), ("Calacatta", "ATD-DT"), ("Travertine", "LYSM"))
    ]
    assert pick_line_facts(plates, brand="American Tile Depot").skip != "reseller"

    class Response:
        def __init__(self, status, body):
            self.status_code = status
            self._body = body
            self.text = body if isinstance(body, str) else ""

        def json(self):
            return self._body

    catalog = {"products": [
        {
            "title": f"{name} Switch Plate",
            "handle": name.lower(),
            "product_type": "Marble Wall/Switch Plate",
            "tags": ["Accessories", "No Sample"],
            "body_html": "Cut from Carrara marble.",
            "variants": [{"available": True}],
            "vendor": "OTS",
        }
        for name in ("Carrara", "Calacatta", "Travertine")
    ]}

    def get(url, timeout=5, headers=None, allow_redirects=True):
        if "best-sellers" in url or "bestsellers" in url:
            return Response(200, catalog)
        return Response(404, {})

    facts = collect_product_facts(
        "tile.example", brand="American Tile Depot", allow_storeleads=False,
        get=get, sleep=lambda _s: None, guard=lambda url: url, homepage_html="<html></html>",
    )
    assert facts["catalog_skip"] != "no_bestseller_product"
    assert facts["catalog_skip"] != "reseller"
    assert facts["products"]


def test_a_passing_rewrite_is_kept_when_it_resembles_the_rejected_line():
    facts = LineFacts(
        brand="Perfy", product="Variety Pack",
        detail="Fruit Punch, Tropical Citrus, and Blood Orange Yuzu",
        recipe="trial_offer",
    )
    calls = {"n": 0}

    def writer(_prompt):
        calls["n"] += 1
        if calls["n"] == 1:
            return "Nice that the Variety Pack from Perfy includes Fruit Punch, Tropical Citrus, and Blood Orange Yuzu today."
        return "Nice that the Variety Pack from Perfy includes Fruit Punch, Tropical Citrus, and Blood Orange Yuzu."

    def grader(prompt):
        if "yuzu today" in prompt.lower():
            return json.dumps({"pass": False, "reason": "extra word"})
        return json.dumps({"pass": True, "reason": "matches facts and pass example voice"})

    result = compose_line(facts, writer=writer, grader=grader)
    assert result.source == "ai"
    assert "tropical citrus" in result.line.lower()


def test_automatically_is_not_backend_tech():
    import outbound_openers as op

    facts = LineFacts(
        brand="Dreame", product="L10s Ultra",
        detail="cleans and dries its own mops", recipe="amz_on",
    )
    line = "Noticed the L10s Ultra automatically cleans and dries its mops."
    assert hard_reason(line, facts) == ""
    assert not op.BACKEND_LANGUAGE.search(line)
    assert not op.BACKEND_LANGUAGE.search("The automatic mop cycle is short.")
    assert op.BACKEND_LANGUAGE.search("The automation runs overnight.")
    assert op.BACKEND_LANGUAGE.search("The workflow sends the emails.")


def test_grader_json_in_a_code_fence_still_parses():
    from outbound_opener_line import parse_grade

    passed, reason = parse_grade(
        'Here is the grade:\n```json\n{"pass": true, "reason": "one plain fact"}\n```'
    )
    assert passed is True
    assert reason == "one plain fact"


def test_pass_examples_are_human_openings_and_the_writer_cannot_copy_them():
    facts = LineFacts(
        brand="ArtisRaw", product="Olive Wood Chess Set",
        detail="all 32 pieces stored inside its drawer", recipe="amz_off",
    )
    prompt = grader_prompt(facts, "Noticed the Olive Wood Chess Set has all 32 pieces stored inside its drawer.")
    assert "Noticed the Olive Wood Chess Set has all 32 pieces stored inside its drawer." in prompt
    assert "Noticed Autumn Sons Baking Chocolate Chunk Cookie Mixes only need a small amount of water and oil." in prompt
    assert "do not fail that shape as a flat catalog statement" in prompt.lower()
    written = writer_prompt(facts)
    assert "looks sturdy enough" not in written.lower()
    assert "do not reuse their phrases" in written.lower()
    assert "one fact only" in written.lower()
    assert "linen apron" in written.lower()
    cause_facts = LineFacts(
        brand="Paramo", product="Beet Chips",
        detail="2% to Save Páramos", recipe="amz_on", domain="paramo.example",
    )
    cause = writer_prompt(cause_facts)
    from outbound_opener_line import opening_for
    assert f"{opening_for('paramo.example')} Paramo gives X to help Y." in cause


def test_a_finish_after_a_dash_becomes_the_product_noun():
    from outbound_opener_line import shopper_name

    title = (
        "Italian Carrara White Marble Single Rocker Switch Wall Plate / "
        "Switch Plate / Cover - Honed"
    )
    assert shopper_name(title) == "Carrara Marble Switch Plate"
    assert shopper_name("Cedar Board - Honed") == "Cedar Board"
    plate = LineFacts(
        brand="American Tile Depot", product="Carrara Marble Switch Plate",
        detail="Cut from Carrara marble.", recipe="plan_upgrade",
    )
    assert hard_reason(
        "Nice that the Carrara marble switch plate is made from authentic Italian marble with a honed honed finish.",
        plate,
    ) == "hype"
    assert hard_reason(
        "Nice that the honed wall plate is honed from Carrara marble.",
        plate,
    ) == "repeated word"


def test_a_grader_timeout_uses_the_remaining_budget_and_does_not_fall_back():
    import importlib.util
    import time

    import outbound_personalization as personalization

    spec = importlib.util.spec_from_file_location("dry_run_openers", "scripts/dry_run_openers.py")
    dry = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dry)
    source = open("scripts/dry_run_openers.py", encoding="utf-8").read()
    assert "temperature: float = 0.2" in source
    assert "temperature=0.2" in source
    seen = []

    def llm(prompt, timeout_s=30):
        seen.append(("write", prompt.startswith("Grade"), timeout_s))
        return "Nice that the cherry cola is brewed with turmeric."

    def slow(_prompt, timeout_s=30):
        seen.append(("grade", False, timeout_s))
        raise personalization.ModelTimeout("grader")

    row = dry.render_row(
        {"domain": "perfy.com", "brand": "Perfy", "first_name": "Ann", "recipe": "trial_offer"},
        facts={"source": "products.json", "products": [{"title": "Cherry Cola", "product_type": "Soda", "body": "Brewed with turmeric."}]},
        llm=llm,
        grader=slow,
        deadline=time.monotonic() + 35,
    )
    grades = [timeout for kind, _graded, timeout in seen if kind == "grade"]
    assert grades and min(grades) > 20
    assert not any(graded for _kind, graded, _timeout in seen if _kind == "write")
    assert row["hold"] == "skipped:model_timeout"
    assert row["opener"] == ""


def test_amazon_wording_and_openings_do_not_confuse_the_writer():
    from outbound_opener_line import opening_for

    facts = LineFacts(
        brand="Pix Perfect", product="Pixel Art Kit",
        detail="insert the pegs into the panel", recipe="amz_on", domain="pix.example",
    )
    prompt = writer_prompt(facts)
    assert "You may mention that they sell on Amazon" in prompt
    assert "The line may say so" not in prompt
    assert "Do not repeat a word" not in prompt
    assert f"Open with '{opening_for('pix.example')}'" in prompt
    seen = {opening_for(f"shop-{index}.example") for index in range(40)}
    assert seen == {"Noticed", "Love that", "Nice that"}
    assert opening_for("pix.example") == opening_for("PIX.example")


def test_hashtags_lists_spec_claims_and_odd_hooks():
    _name, detail, _kind = extract_detail(
        "Beet Chips",
        "2% to help #SaveTheParamos and the watershed.",
        brand="Paramo",
    )
    assert "save the páramos" in detail.lower()
    assert "savetheparamos" not in detail.lower()
    _name, detail, _kind = extract_detail(
        "Beet Chips",
        "Gives 2% to help SaveTheParamos.",
        brand="Paramo",
    )
    assert "save the páramos" in detail.lower()

    pillow = LineFacts(
        brand="I Wanna Sleep", product="SnowTouch Pillow",
        detail="Add or remove fill until your hips, shoulders, and spine finally line up.",
        recipe="amz_on",
    )
    bare = "Noticed the SnowTouch Pillow lets you add or remove fill until your hips shoulders and spine line up."
    assert hard_reason(bare, pillow) == "missing commas"
    kept = "Noticed the SnowTouch Pillow lets you add or remove fill until your hips, shoulders, and spine line up."
    assert hard_reason(kept, pillow) == ""
    assert hard_reason("Noticed the SnowTouch Pillow fits hips shoulders and spine.", pillow) == "missing commas"

    vacuum = LineFacts(
        brand="Dreame", product="L10s Ultra",
        detail="cleans and dries its own mops", recipe="amz_on",
    )
    assert hard_reason("Noticed the L10s Ultra delivers up to 60 days of hands-free cleaning.", vacuum) == "spec claim"
    assert hard_reason("Noticed the L10s Ultra cleans and dries its own mops.", vacuum) == ""
    daily = LineFacts(brand="Water.io", product="Daily+", detail="personalized hydration plans", recipe="amz_off")
    assert hard_reason("Noticed the Daily+ gives daily personalized hydration plans.", daily) == "repeated word"
    assert hard_reason("Noticed the Daily+ sets a personalized hydration plan.", daily) == ""

    _name, picked, _kind = extract_detail(
        "SuperSupple Moisturizer",
        "Delivers up to 60 days of moisture. Ceramide NP plus Cholesterol. Jojoba esters soften the cream.",
        brand="Pure & Cimple",
    )
    assert "jojoba" in picked.lower() or "ceramide" in picked.lower()
    assert "cholesterol" not in picked.lower()
    assert "delivers up to" not in picked.lower()
