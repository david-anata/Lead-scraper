"""The five opener rules from the Nov 10 Trial Offer audit."""

import outbound_instantly_push as push
import outbound_openers as op
import outbound_personalization as personalization
import outbound_pipeline as pipeline
from outbound_contacts import acceptable_first_name, split_person_name


def setup_function():
    op._OPENERS_BY_DOMAIN.clear()
    op._ANGLES_BY_DOMAIN.clear()


def _lead(recipe, **extra):
    lead = {
        "brand": "Performance Kitchen",
        "domain": f"{recipe}.example",
        "recipe": recipe,
        "categories": ["food_beverage"],
        "niche": "food_beverage",
        "store_profile": {
            "description": "Frozen meals built around whole vegetables",
            "plan": "Shopify Plus",
            "apps": ["Klaviyo"],
            "technologies": ["Facebook Pixel"],
            "last_platform": "woocommerce",
            "last_platform_change_at": "2026-09-06T00:00:00Z",
            "sales_channels": ["Amazon"],
        },
    }
    lead.update(extra)
    return lead


def _banned(text):
    low = (text or "").lower()
    assert "stood out" not in low
    assert "food_beverage" not in low
    assert "apparel_accessories" not in low
    assert "?" not in (text or "")
    assert "—" not in (text or "") and "–" not in (text or "") and " - " not in (text or "")
    assert "http" not in low and "www." not in low
    assert not op.BACKEND_LANGUAGE.search(text or "")
    for word in (
        "klaviyo", "pixel", "shopify", "woocommerce", "automation", "bid",
        "direct-to-consumer", "direct to consumer", "upgraded", "app install",
    ):
        assert word not in low


def test_trial_offer_never_says_the_brand_is_on_amazon_and_amz_on_may():
    trial = _lead("trial_offer", campaign_id=op.TRIAL_OFFER_CAMPAIGN_ID)
    opener, angle = op.idea_lines(trial)
    assert opener and "amazon" not in opener.lower()
    assert "on amazon" not in (angle or "").lower()
    _banned(opener)
    _banned(angle.replace("Amazon", "") if angle else "")
    assert op.opener_hold_reason("Your catalog on Amazon has room for a better offer.", trial)
    assert op.opener_hold_reason("Your products are live on Amazon today.", trial)
    assert not op.validate(
        "Performance Kitchen's meals are already on Amazon and ready for a small set.",
        trial_offer=True,
    )[0]
    assert op.validate(
        "A vegetable meal set could give new Amazon shoppers an easy way to try Performance Kitchen.",
        trial_offer=True, role="angle", max_words=op.ANGLE_MAX_WORDS,
    )[0]

    other = _lead("plan_upgrade")
    plan_opener, plan_angle = op.idea_lines(other)
    assert "amazon" not in plan_opener.lower()
    assert "amazon" not in (plan_angle or "").lower()
    assert op.opener_hold_reason("The catalog on Amazon needs a small set.", other)

    amz = _lead(
        "amz_on",
        amz_product="vegetable meals",
        amz_confidence="high",
    )
    amz_opener, _amz_angle = op.idea_lines(amz)
    assert "on Amazon" in amz_opener
    assert op.opener_hold_reason(amz_opener, amz) == ""


def test_backend_and_consultant_language_never_reaches_the_copy():
    for recipe in ("plan_upgrade", "replatform", "amz_off", "amz_on", "trial_offer"):
        opener, angle = op.idea_lines(_lead(recipe))
        _banned(opener)
        _banned(angle or "")
    sheet = personalization.fact_sheet(_lead("plan_upgrade"))
    blob = " ".join(sheet.facts).lower()
    assert "shopify" not in blob and "klaviyo" not in blob and "pixel" not in blob
    assert "woocommerce" not in blob and "september" not in blob
    assert personalization.style_violation(
        "The recent app install suggests a direct-to-consumer move with bid adjustments.",
    ) == "tech or backend change"
    assert op.validate(
        "Most brands miss the automation opportunities that come with a plan upgrade.",
    )[1] == "backend language"


def test_stood_out_template_raw_codes_and_questions_are_rejected():
    template = (
        "Performance Kitchen stood out while I was looking at food_beverage brands. "
        "Is Amazon a real growth channel for you yet?"
    )
    assert op.validate(template)[1] in {"stood out template", "question", "slug"}
    assert "stood out" in op.opener_hold_reason(template, _lead("trial_offer"))
    opener, _angle = op.idea_lines(_lead("trial_offer"))
    assert "stood out" not in opener.lower()
    assert "_" not in opener
    assert not opener.endswith("?")
    assert personalization.style_violation(
        "Rebel Raw stood out while I was looking at food_beverage brands today."
    ) == "stood out template"
    words = op.plain_category(["food_beverage", "/Apparel/Clothing"])
    assert "food_beverage" not in words and "_" not in words
    assert "food and beverage" in words
    assert "['" not in personalization.as_text(["food_beverage"])


def test_company_and_first_names_are_cleaned_before_instantly():
    assert pipeline.clean_brand_name("www.osgoodemarley.com") == "Osgoodemarley"
    assert pipeline.clean_brand_name("https://osgoodemarley.com/shop") == "Osgoodemarley"
    assert pipeline.clean_brand_name("Acme, LLC") == "Acme"
    assert pipeline.clean_brand_name("Acme Co.") == "Acme"
    assert pipeline.clean_brand_name("JANGEORGe Interior Design") == "Jangeorge Interior Design"
    assert pipeline.clean_brand_name("caliray") == "Caliray"
    assert pipeline.clean_brand_name("REV Gum") == "REV Gum"
    assert pipeline.clean_brand_name("MyBevi") == "MyBevi"
    assert split_person_name("Julie Mitchell") == ("Julie", "Mitchell")
    assert split_person_name("JULIE", "MITCHELL") == ("Julie", "Mitchell")
    assert split_person_name("Ann", "Lee") == ("Ann", "Lee")
    assert not acceptable_first_name("Julie Mitchell")
    assert acceptable_first_name("Julie")
    lead = push.build_lead(
        {"email": "Julie.Mitchell@example.com", "first_name": "Julie Mitchell", "last_name": ""},
        {"brand": "www.osgoodemarley.com", "domain": "osgoodemarley.com", "niche": "apparel_accessories"},
        "Osgoodemarley is known for tailored shirts a new shopper can picture.",
    )
    assert lead["first_name"] == "Julie"
    assert lead["last_name"] == "Mitchell"
    assert lead["company_name"] == "Osgoodemarley"
    assert "_" not in lead["custom_variables"]["category"]


def test_openers_are_short_specific_ideas_without_dashes_or_urls():
    lead = _lead("amz_off", brand="Humble Grape", domain="humble.example", categories=["wine"])
    assert op.idea_lines(lead) == ("", "")
    opener, angle = op.idea_lines(_lead("replatform"))
    assert 6 <= len(opener.split()) <= 25
    assert opener.endswith(".")
    assert "vegetable" in opener.lower()
    assert angle and 6 <= len(angle.split()) <= 20
    assert angle != opener
    _banned(opener)
    _banned(angle)
    assert op.validate("Nice work - really good for this shopper today.")[1] == "em dash"
    assert op.validate("A smart bottle — with coaching built in for you.")[1] == "em dash"
    assert op.validate("See https://brand.com/pages for the new shopper offer.")[1] == "contains a link or address"
