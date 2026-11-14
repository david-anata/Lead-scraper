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
    assert result.pushed == 0 and sent == []
    assert oe.unpushed(engine) == []
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT opener, skip_reason FROM outbound_enriched_contacts "
            "WHERE email = 'ada@cmycubes.com'"
        )).one()
    assert row[0] in ("", None)
    assert str(row[1]).startswith("held:opener:")
    assert "shopper-visible" in str(row[1])
