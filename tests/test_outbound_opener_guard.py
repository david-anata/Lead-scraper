"""Openers stay a plain shopper-visible fact, or the lead is skipped."""

import json

from sqlalchemy import create_engine, text

import outbound_enrich as oe
import outbound_instantly_push as push
import outbound_openers as op
import outbound_personalization as personalization
import outbound_pipeline as pipeline


def setup_function():
    op._OPENERS_BY_DOMAIN.clear()
    op._ANGLES_BY_DOMAIN.clear()


CONTACT = {"email": "ada@brand.com", "first_name": "Ada", "title": "Founder"}
GENTS = (
    "Gents' apparel brand is positioned to scale across premium consumer "
    "channels with strategic automation tools."
)


def test_retail_page_is_not_a_standalone_opener_and_a_named_store_is():
    locator = {
        "brand": "CMY Cubes",
        "domain": "cmycubes.com",
        "categories": ["food_beverage"],
        "store_profile": {"store_locator_page": "https://cmycubes.com/pages/stores"},
    }
    assert "is sold in retail stores" not in op.store_fact_phrases(locator)
    line = op.opener_line(op.collect_facts(locator))
    assert "retail stores" not in line.lower()
    named = {
        "brand": "Huppy",
        "domain": "huppy.com",
        "store_profile": {
            "retailers": ["REI"],
            "retailer_page": "https://www.rei.com/b/huppy",
        },
    }
    phrases = op.store_fact_phrases(named)
    assert "is sold at REI" in phrases
    assert "retail stores" not in " ".join(phrases).lower()
    sentence, source = op.write_opener(named, CONTACT, llm=lambda _prompt: GENTS)
    assert source == "fallback"
    assert "REI" in sentence and "retail stores" not in sentence.lower()
    assert "positioned" not in sentence.lower()


def test_category_fallback_is_not_an_opener():
    assert op.plain_category("food_beverage") == "food and beverage"
    assert op.fallback_line("CMY Cubes", "food_beverage") == ""
    assert op.fallback_line("Dandylion", "food_beverage") == ""
    assert op.opener_line(op.LeadFacts(brand="CMY Cubes", category="food and beverage")) == ""
    line, source = op.write_opener(
        {"brand": "Dandylion", "domain": "dandylion.com", "niche": "food_beverage",
         "categories": ["food_beverage"]},
        CONTACT,
        llm=lambda _prompt: "Dandylion sells food and beverage.",
    )
    assert line == "" and source == "none"
    assert "category filler" in op.opener_hold_reason(
        "CMY Cubes sells food and beverage.",
        {"brand": "CMY Cubes", "categories": "food_beverage"},
    )


def test_saw_category_lines_are_removed():
    obs = op.rich_observations({
        "brand": "Dandylion",
        "domain": "dandylion.com",
        "categories": "food_beverage",
    })
    text, used = op.human_opener(obs)
    assert text == "" and used == []
    assert "shoppers already compare" not in text.lower()

    called = []

    def bad_model(prompt):
        called.append(prompt)
        return json.dumps({
            "opener": "Saw Dandylion in food and beverage, a category where shoppers already compare options.",
            "angle": "Shoppers already compare food and beverage.",
        })

    category_only = {
        "brand": "Dandylion",
        "domain": "dandylion.com",
        "categories": "food_beverage",
        "store_profile": {"sales_channels": ["Amazon"]},
    }
    opener, angle, source = personalization.generate(category_only, llm=bad_model)
    assert called == []
    assert opener == "" and angle == "" and source == "none"

    product_lead = {
        **category_only,
        "domain": "dandylion-soda.com",
        "description": "Hibiscus rose soda",
    }
    opener, angle, source = personalization.generate(product_lead, llm=bad_model)
    assert source == "none" and opener == "" and angle == ""
    assert called
    plain, plain_source = op.write_opener(product_lead, CONTACT, llm=bad_model)
    assert plain_source == "fallback"
    assert "hibiscus rose soda" in plain.lower()
    assert "shoppers already compare" not in plain.lower()
    assert "category where" not in plain.lower()


def test_validator_rejects_strategy_copy_and_skips_without_a_fact():
    ok, why = op.validate(GENTS)
    assert not ok and why == "generic business phrasing"
    assert personalization.style_violation(GENTS) == "generic business phrasing"
    assert "generic business phrasing" in op.opener_hold_reason(GENTS, {"brand": "Gents"})

    skipped, source = op.write_opener(
        {"brand": "Gents", "domain": "gents.com", "categories": ["apparel"]},
        CONTACT,
        llm=lambda _prompt: GENTS,
    )
    assert skipped == "" and source == "none"

    kept, kept_source = op.write_opener(
        {"brand": "Gents", "domain": "gents-soap.com", "categories": ["apparel"],
         "site_product": "cedar soap"},
        CONTACT,
        llm=lambda _prompt: GENTS,
    )
    assert kept_source == "fallback"
    assert "cedar soap" in kept.lower()
    assert "positioned" not in kept.lower() and "automation" not in kept.lower()
    assert op.validate("Gents sells hair growth serum.")[0]


def test_page_boilerplate_is_not_treated_as_a_product():
    assert op._shopper_sentence("Acme", "Official Website") == ""
    assert op._shopper_sentence("Acme", "Shop Now") == ""
    assert op._shopper_sentence("Acme", "Home") == ""
    assert op._shopper_sentence("Acme", "recent product: Home") == ""
    assert "page boilerplate" in op.opener_hold_reason(
        "Acme sells official Website.", {"brand": "Acme"},
    )
    thin = {
        "brand": "Acme",
        "domain": "acme.com",
        "categories": ["apparel"],
        "store_profile": {"title": "Official Website"},
    }
    assert personalization.fact_sheet(thin).facts == []
    line, source = op.write_opener(thin, CONTACT, llm=lambda _prompt: "Acme sells official Website.")
    assert line == "" and source == "none"
    assert "cedar soap" in op._shopper_sentence("Acme", "recent product: Cedar Soap").lower()
    assert "hibiscus rose soda" in op._shopper_sentence("Acme", "Hibiscus rose soda").lower()


def test_company_labels_drop_suffixes_and_prefer_the_store_title():
    assert pipeline.clean_brand_name("Young Living US Prod") == "Young Living"
    assert pipeline.clean_brand_name("Ziwi Us") == "Ziwi"
    assert pipeline.clean_brand_name("masarratmakeup-USA") == "Masarratmakeup"
    assert pipeline.clean_brand_name("REV Gum") == "REV Gum"
    assert pipeline.clean_brand_name("MyBevi") == "MyBevi"
    assert pipeline.company_label(
        "Zollicandy", title="ZolliCandy", domain="zollicandy.com",
    ) == "ZolliCandy"
    assert pipeline.company_label(
        "masarratmakeup-USA", title="Masarrat Makeup", domain="masarratmakeup.com",
    ) == "Masarrat Makeup"
    assert pipeline.company_label(
        "Sherwin-Williams", title="York Wallcoverings", domain="yorkwall.com",
    ) == "York Wallcoverings"
    assert pipeline.company_label("Young Living US Prod", domain="youngliving.com") == "Young Living"

    stored = pipeline.to_clay_lead({
        "name": "yorkwall.com",
        "merchant_name": "Sherwin-Williams",
        "title": "York Wallcoverings | Wallpaper",
        "platform": "shopify",
        "country_code": "US",
        "categories": "Home",
        "estimated_sales_yearly": 5_000_000_00,
        "contact_info": [{"type": "email", "value": "hi@yorkwall.com"}],
    })
    assert stored["brand"] == "York Wallcoverings"
    pushed = push.build_lead(
        {"email": "ada@yorkwall.com", "first_name": "Ada"},
        {
            "brand": "Sherwin-Williams",
            "domain": "yorkwall.com",
            "store_profile": {"title": "York Wallcoverings"},
        },
        "York Wallcoverings sells grasscloth wallpaper.",
    )
    assert pushed["company_name"] == "York Wallcoverings"


def test_skipped_lead_stays_unpushed_with_a_reason():
    engine = create_engine("sqlite://")

    def find(domain, *, api_key, max_contacts=2, **_kwargs):
        import outbound_contacts as oc

        return oc.FindResult(
            domain=domain,
            contacts=[oc.Contact(
                domain=domain, first_name="Ada", last_name="Lee",
                email="ada@cmycubes.com", title="Founder",
            )],
            credits_used=1,
            reason="",
        )

    food = {
        "domain": "cmycubes.com",
        "brand": "CMY Cubes",
        "niche": "food_beverage",
        "categories": ["food_beverage"],
        "recipe": "amz_on",
        "tier": "A",
        "store_profile": {"store_locator_page": "https://cmycubes.com/pages/stores"},
    }
    sent = []

    def capture(payload, _cid, dry_run=False, **_kwargs):
        sent.append(payload)
        return push.PushOutcome(attempted=len(payload), created=len(payload), dry_run=False)

    result = oe.run_enrichment(
        engine, [food], api_key="k", find=find, dry_run=False, push=capture,
    )
    assert result.pushed == 1 and sent
    assert oe.unpushed(engine) == []
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT opener, opener_source, skip_reason FROM outbound_enriched_contacts "
            "WHERE email = 'ada@cmycubes.com'"
        )).one()
    assert row[0]
    assert str(row[1]).startswith("general:")
    assert row[2] in ("", None)
    assert sent[0][0]["custom_variables"]["opener_type"] == "general"


REFUSAL = (
    "I have no concrete product, flavor, story, store name, or review theme "
    "listed for Bella Rosa Coffee Company, so I'm writing nothing."
)
YOUTUBE_BIO = "Share your videos with friends, family, and the world."
YOUTUBE_LINE = (
    "Belagio Enterprises youtube bio: Share your videos with friends, family, and the world."
)
PROMO_TITLE = "Black Friday Sale Special - Studio Full Retail Kit"
PROMO_LINE = "sells Black Friday Sale Special - Studio Full Retail Kit"


def test_model_refusal_is_held_and_never_sent():
    ok, why = op.validate(REFUSAL)
    assert not ok and why == "model refusal text"
    assert personalization.style_violation(REFUSAL) == "model refusal text"
    thin = {
        "brand": "Bella Rosa Coffee Company",
        "domain": "bellarosacoffeecompany.com",
        "recipe": "replatform",
    }
    reason = op.opener_hold_reason(REFUSAL, thin)
    assert reason.startswith("opener is model refusal text")
    assert f"{op.OPENER_HOLD_PREFIX} {reason}".startswith("held:opener:")
    line, source = op.write_opener(thin, CONTACT, llm=lambda _prompt: REFUSAL)
    assert line == "" and source == "none"
    assert "empty string" in op._SYSTEM
    assert "write nothing" not in op._SYSTEM.lower()

    coffee = {
        **thin,
        "description": "Single origin coffee",
        "store_profile": {"description": "Single origin coffee"},
    }
    kept, kept_source = op.write_opener(coffee, CONTACT, llm=lambda _prompt: REFUSAL)
    assert kept_source == "fallback"
    assert "single origin coffee" in kept.lower()
    assert "writing nothing" not in kept.lower()
    assert "i have no" not in kept.lower()
    for echo in ("as an AI", "I cannot", "I have no", "no concrete product", "writing nothing"):
        assert op.copy_block_reason(echo) == "model refusal text"


def test_platform_bios_are_not_facts_and_a_foreign_domain_is_held():
    for line in (
        YOUTUBE_LINE,
        f"Healing Noni youtube bio: {YOUTUBE_BIO}",
        f"Arcos USA youtube bio: {YOUTUBE_BIO}",
        f"Healing Noni instagram bio: Create an account or log in to Instagram",
        "This shop will be powered by Shopify",
        "This website was built on Wix. Create yours today.",
        "Build a website. Sell your stuff. Powered by Squarespace.",
        "TikTok - Make Your Day",
        "Linktree: the only link in bio tool",
    ):
        assert op.copy_block_reason(line) == "platform boilerplate"
        assert "platform boilerplate" in op.opener_hold_reason(line, {"brand": "Healing Noni"})

    belagio = {
        "brand": "Belagio Enterprises",
        "domain": "belagioenterprises.com",
        "recipe": "plan_upgrade",
        "email": "fmelamed@rjsinger.com",
        "description": YOUTUBE_BIO,
        "store_profile": {
            "description": YOUTUBE_BIO,
            "contact_info": [{
                "type": "youtube",
                "description": YOUTUBE_BIO,
                "url": "https://www.youtube.com/@rjsinger",
                "followers": 12,
            }],
        },
    }
    sheet = personalization.fact_sheet(belagio)
    assert sheet.facts == []
    assert all("bio:" not in fact.lower() for fact in sheet.facts)
    assert op.shopper_lines(belagio) == ("", "")
    line, source = op.write_opener(
        belagio, {"email": "fmelamed@rjsinger.com"}, llm=lambda _prompt: YOUTUBE_LINE,
    )
    assert line == "" and source == "none"
    assert "youtube bio" not in line.lower()
    assert op.opener_hold_reason(YOUTUBE_LINE, belagio).startswith("opener is platform boilerplate")

    owned = {
        "brand": "Belagio Enterprises",
        "domain": "belagioenterprises.com",
        "email": "fmelamed@rjsinger.com",
        "description": "Espresso beans",
        "store_profile": {
            "description": "Espresso beans",
            "most_recently_published_product": {
                "title": "Espresso beans",
                "url": "https://rjsinger.com/products/espresso-beans",
            },
        },
    }
    assert op.fact_domain_reason(owned) == "fact source domain does not match the store"
    assert op.opener_hold_reason("Belagio Enterprises sells espresso beans.", owned) == (
        "fact source domain does not match the store"
    )
    owned_facts = " ".join(personalization.fact_sheet(owned).facts).lower()
    assert "recent product" not in owned_facts
    healing = {
        "brand": "Healing Noni",
        "domain": "healingnoni.com",
        "recipe": "replatform",
        "email": "mharvey@healingnoni.com",
        "store_profile": {"description": YOUTUBE_BIO},
    }
    assert personalization.fact_sheet(healing).facts == []
    assert op.shopper_lines(healing) == ("", "")
    assert op.source_matches_store("https://brand.myshopify.com/products/soap", "brand.com")
    assert not op.source_matches_store("https://www.youtube.com/@brand", "brand.com")
    assert not op.source_matches_store("https://other.myshopify.com/products/soap", "brand.com")
    assert not op.source_matches_store("https://myshopify.com", "brand.com")


def test_promo_titles_and_program_suffixes_are_not_a_company_or_a_product():
    assert pipeline.clean_brand_name("Tattoo Lovers Care Loyalty Program TLC") == "Tattoo Lovers Care"
    assert pipeline.clean_brand_name("Tattoo Lovers Care Rewards Program") == "Tattoo Lovers Care"
    assert pipeline.company_label(
        "Tattoo Lovers Care Loyalty Program TLC", domain="tattooloverscare.com",
    ) == "Tattoo Lovers Care"
    assert pipeline.clean_brand_name("REV Gum") == "REV Gum"
    assert op.is_promo_title(PROMO_TITLE)
    assert op.is_promo_title("Holiday Bundle Deal")
    assert op.is_promo_title("Gift Card")
    assert op.is_promo_title("20% off")
    assert not op.is_promo_title("Studio aftercare balm")

    promo = {
        "brand": "Tattoo Lovers Care Loyalty Program TLC",
        "domain": "tattooloverscare.com",
        "store_profile": {
            "title": "Tattoo Lovers Care",
            "most_recently_published_product": {
                "title": PROMO_TITLE,
                "url": "https://tattooloverscare.com/products/black-friday-kit",
            },
        },
    }
    assert personalization.fact_sheet(promo).facts == []
    line, source = op.write_opener(promo, CONTACT, llm=lambda _prompt: f"Tattoo Lovers Care {PROMO_LINE}.")
    assert line == "" and source == "none"
    assert "promotion" in op.opener_hold_reason(PROMO_LINE, promo)
    assert "promotion" in op.opener_hold_reason(
        "Tattoo Lovers Care Loyalty Program TLC sells Black Friday Sale Special - Studio Full Retail Kit.",
        promo,
    )
    pushed = push.build_lead(
        {"email": "ada@tattooloverscare.com", "first_name": "Ada"},
        {"brand": "Tattoo Lovers Care Loyalty Program TLC", "domain": "tattooloverscare.com"},
        "Tattoo Lovers Care sells aftercare balm.",
    )
    assert pushed["company_name"] == "Tattoo Lovers Care"


def test_ungrounded_opener_is_held_even_when_the_sentence_looks_specific():
    assert "store-verified" in op.opener_hold_reason(
        "Bella Rosa Coffee Company sells a house roast.",
        {"brand": "Bella Rosa Coffee Company", "domain": "bellarosacoffeecompany.com"},
    )
    grounded = {
        "brand": "Bella Rosa Coffee Company",
        "domain": "bellarosacoffeecompany.com",
        "email": "ray@bellarosacoffeecompany.com",
        "site_product": "house roast",
    }
    assert op.opener_hold_reason(
        "Bella Rosa Coffee Company sells house roast.", grounded,
    ) == ""
