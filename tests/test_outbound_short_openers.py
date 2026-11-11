"""Short opener and brand-angle lines, and the Trial Offer Amazon ban."""

import json

import outbound_instantly_push as push
import outbound_openers as op
import outbound_personalization as personalization


def setup_function():
    op._OPENERS_BY_DOMAIN.clear()
    op._ANGLES_BY_DOMAIN.clear()


CONTACT = {"email": "ann@huppy.com", "first_name": "Ann", "title": "Founder"}
LEAD = {
    "brand": "Huppy",
    "domain": "huppy.com",
    "niche": "oral care",
    "recipe": "amz_on",
    "categories": ["oral care"],
}


def _words(count: int, *head: str) -> str:
    words = list(head) + ["today"] * (count - len(head))
    return " ".join(words[:count]) + "."


def test_prompts_state_the_word_caps_and_one_sentence():
    assert "about 20 words" in op._SYSTEM
    assert "never more than 25" in op._SYSTEM
    assert "about 15 words" in op._ANGLE_SYSTEM
    assert "never more than 20" in op._ANGLE_SYSTEM
    assert "Trial Offer" in op._SYSTEM and "Trial Offer" in op._ANGLE_SYSTEM
    sheet = personalization.fact_sheet({
        **LEAD,
        "description": "Toothpaste tabs for daily brushing",
    })
    prompt = personalization._prompt(sheet)
    assert "one sentence of about 20 words, never more than 25" in prompt
    assert "one sentence of about 15 words, never more than 20" in prompt
    assert "amz_on" in prompt and "may say it is on Amazon" in prompt


def test_validator_enforces_the_hard_max_and_keeps_the_old_bans():
    assert op.validate(_words(25, "Huppy", "sells", "oral", "care"))[0]
    ok, why = op.validate(_words(26, "Huppy", "sells", "oral", "care"))
    assert not ok and why == "too long"
    assert op.validate(_words(20, "Anata", "helps", "Huppy", "grow"), max_words=op.ANGLE_MAX_WORDS)[0]
    ok, why = op.validate(_words(21, "Anata", "helps", "Huppy", "grow"), max_words=op.ANGLE_MAX_WORDS)
    assert not ok and why == "too long"
    ok, why = op.validate("Huppy sells oral care. Shoppers like it.")
    assert not ok and why == "more than one sentence"
    assert not op.validate("Huppy grew 4 percent.")[0]
    assert not op.validate("See https://huppy.com today.")[0]
    assert not op.validate("Nice work — really")[0]
    assert personalization.style_violation(_words(25, "Huppy", "sells", "oral", "care")) == ""
    assert personalization.style_violation(_words(26, "Huppy", "sells", "oral", "care")) == "too long"
    assert personalization.style_violation(
        _words(21, "Shoppers", "compare", "oral", "care"), role="angle",
    ) == "too long"
    assert personalization.style_violation(
        "Huppy sells oral care. Shoppers like it. So does everyone else."
    ) == "more than two sentences"
    assert personalization.style_violation(
        "Your adai dosa mix caught my eye. It's not something I see from many brands."
    ) == ""


def test_over_long_opener_is_regenerated_once_then_held():
    calls = []

    def llm(prompt):
        calls.append(prompt)
        if len(calls) == 1:
            return _words(30, "Huppy", "sells", "oral", "care")
        return (
            "Huppy's oral care line gives a new shopper one product to remember."
        )

    line, source = op.write_opener({**LEAD, "domain": "retry.com"}, CONTACT, llm=llm)
    assert source == "ai" and "oral care" in line and len(line.split()) <= 25
    assert len(calls) == 2
    assert "too long" in calls[1]
    assert "never more than 25" in calls[0]

    calls.clear()

    def still_long(_prompt):
        calls.append(1)
        return _words(30, "Huppy", "sells", "oral", "care")

    line, source = op.write_opener({**LEAD, "domain": "held.com"}, CONTACT, llm=still_long)
    assert line == "" and source == "none"
    assert len(calls) == 2
    assert "25 words" in op.opener_hold_reason(
        _words(26, "Huppy", "sells", "toothpaste", "tabs"),
        {"brand": "Huppy", "categories": "Food"},
    )
    assert op.opener_hold_reason(
        _words(25, "Huppy", "sells", "toothpaste", "tabs"),
        {"brand": "Huppy", "categories": "Food"},
    ) == ""


def test_over_long_angle_is_regenerated_once_then_held():
    calls = []

    def llm(prompt):
        calls.append(prompt)
        if len(calls) == 1:
            return _words(24, "Anata", "helps", "Huppy", "grow", "oral", "care")
        return "A small oral care set would give a new shopper an easy way to try Huppy."

    angle, source = op.write_brand_angle({**LEAD, "domain": "angle-retry.com"}, CONTACT, llm=llm)
    assert source == "ai" and "oral care" in angle and len(angle.split()) <= 20
    assert len(calls) == 2
    assert "too long" in calls[1]
    assert "never more than 20" in calls[0]

    calls.clear()

    def still_long(_prompt):
        calls.append(1)
        return _words(24, "Anata", "helps", "Huppy", "grow", "oral", "care")

    angle, source = op.write_brand_angle({**LEAD, "domain": "angle-held.com"}, CONTACT, llm=still_long)
    assert angle == "" and source == "none"
    assert len(calls) == 2
    assert "20 words" in op.angle_hold_reason(_words(21, "Shoppers", "compare", "oral", "care"))
    assert op.angle_hold_reason(_words(20, "Shoppers", "compare", "oral", "care")) == ""


def test_rich_pair_over_the_max_is_regenerated_once_then_left_empty():
    lead = {**LEAD, "domain": "rich.com", "description": "Toothpaste tabs for daily brushing"}
    calls = {"n": 0}
    long_opener = _words(30, "Huppy", "sells", "toothpaste", "tabs")
    short = {
        "opener": "Huppy's toothpaste tabs give a new shopper one product to remember.",
        "angle": "Listing photos of the toothpaste tabs give a new shopper a reason to look.",
    }

    def llm(_prompt):
        calls["n"] += 1
        if calls["n"] == 1:
            return json.dumps({"opener": long_opener, "angle": short["angle"]})
        return json.dumps(short)

    opener, angle, source = personalization.generate(lead, llm=llm)
    assert calls["n"] == 2 and source == "ai"
    assert opener == short["opener"] and angle == short["angle"]

    calls["n"] = 0

    def still_long(_prompt):
        calls["n"] += 1
        return json.dumps({"opener": long_opener, "angle": short["angle"]})

    opener, angle, source = personalization.generate(
        {**lead, "domain": "rich-held.com", "template_fallback": True}, llm=still_long,
    )
    assert calls["n"] == 2 and source == "fallback"
    assert opener and "toothpaste" in opener.lower()
    assert len(opener.split()) <= 25
    assert not angle or len(angle.split()) <= 20


def test_trial_offer_never_claims_or_assumes_amazon():
    assert op.TRIAL_OFFER_CAMPAIGN_ID == push.LEGACY_TRIAL_CAMPAIGN_ID
    assert op.trial_offer_lead({"campaign_id": op.TRIAL_OFFER_CAMPAIGN_ID})
    assert op.trial_offer_lead({"campaign_name": "Anata // Trial Offer // Oct 2026"})
    assert op.trial_offer_lead({"recipe": "trial_offer"})
    assert not op.trial_offer_lead({"recipe": "amz_on"})
    assert not op.trial_offer_lead({"recipe": "new_growth_app"})

    trial = {
        **LEAD,
        "domain": "trial.example",
        "recipe": "trial_offer",
        "campaign_id": op.TRIAL_OFFER_CAMPAIGN_ID,
        "amz_product": "toothpaste tabs",
        "amz_confidence": "high",
        "description": "Toothpaste tabs for daily brushing",
        "store_profile": {
            "sales_channels": ["Amazon"],
            "description": "Toothpaste tabs for daily brushing",
        },
    }
    sheet = personalization.fact_sheet(trial)
    assert sheet.trial_offer
    assert all("amazon" not in fact.lower() for fact in sheet.facts)
    prompt = personalization._prompt(sheet)
    assert "Trial Offer" in prompt
    assert "already sells on Amazon" in prompt
    assert "new Amazon shoppers" not in prompt
    assert "new shoppers" in prompt
    assert "your Amazon store" in prompt and "your listings" in prompt
    assert "already on Amazon" in prompt
    assert "new Amazon shoppers" not in op._ANGLE_SYSTEM
    assert "new shoppers" in op._ANGLE_SYSTEM
    opportunity = (
        "A milk and nut butter bundle could give new shoppers an easy way to try both."
    )
    assert op.validate(
        opportunity, max_words=op.ANGLE_MAX_WORDS, trial_offer=True, role="angle",
    )[0]
    assert not op.validate(
        "A bundle could give new Amazon shoppers an easy way to try both.",
        max_words=op.ANGLE_MAX_WORDS, trial_offer=True, role="angle",
    )[0]
    assert personalization.style_violation(opportunity, role="angle", recipe="trial_offer") == ""
    assert op.angle_hold_reason(opportunity, trial) == ""

    calls = []

    def claim_amazon(prompt):
        calls.append(prompt)
        return "Huppy lists toothpaste tabs on Amazon."

    line, source = op.write_opener({**trial, "template_fallback": True}, CONTACT, llm=claim_amazon)
    assert "amazon" not in line.lower()
    assert source == "fallback"
    assert len(calls) == 2
    assert "Trial Offer" in calls[0]
    assert "already sells on Amazon" in calls[0]
    assert "Amazon product" not in calls[0]
    assert "already sells on Amazon" in op.opener_hold_reason(
        "Huppy sells on Amazon.", trial,
    )
    assert op.opener_hold_reason(
        "Huppy's toothpaste tabs give a new shopper one product to remember.",
        trial,
    ) == ""
    assert op.opener_hold_reason(
        "A small set would give new shoppers a way to try it today.",
        trial,
    ) == "repeated opener tic"
    for claim in (
        "Shoppers reorder on Amazon.",
        "Your Amazon store is ready.",
        "Your listings need a bundle.",
        "Huppy is already on Amazon.",
    ):
        assert "already sells on Amazon" in op.angle_hold_reason(claim, trial)
        assert personalization.style_violation(claim, role="angle", recipe="trial_offer") == (
            "claims the brand already sells on Amazon"
        )
    angle, angle_source = op.write_brand_angle(
        {**trial, "domain": "trial-angle.example"},
        CONTACT,
        llm=lambda _prompt: "A bundle could give new shoppers an easy way to try oral care.",
    )
    assert angle_source == "ai" and "amazon" not in angle.lower() and "shoppers" in angle.lower()

    confirmed, confirmed_source = op.write_opener(
        {
            **LEAD,
            "domain": "confirmed.example",
            "amz_product": "toothpaste tabs",
            "amz_confidence": "high",
        },
        CONTACT,
        llm=lambda _prompt: "Huppy lists toothpaste tabs on Amazon.",
    )
    assert confirmed_source == "ai" and "Amazon" in confirmed
    assert "backend" in op.opener_hold_reason(
        "Huppy upgraded their store plan.", {"brand": "Huppy"},
    )


def test_a_short_budget_does_not_retry_or_relabel_a_rejected_line():
    calls = {"n": 0}

    def llm(_prompt):
        calls["n"] += 1
        return json.dumps({
            "product_phrase": "tooth tabs",
            "opener": "Huppy tooth tabs are a plain product I would mention to a friend.",
            "angle": "Huppy tooth tabs are what I would put in the ad.",
        })

    lead = {**LEAD, "catalog_products": [{"title": "Tooth Tabs", "product_type": "Tabs"}]}
    _opener, _angle, source = personalization.generate(lead, llm=llm, allow_retry=lambda: False)
    assert calls["n"] == 1
    assert source == "skipped:no_product"


def test_a_model_timeout_stays_a_timeout():
    def llm(_prompt):
        raise personalization.ModelTimeout("slow")

    lead = {**LEAD, "catalog_products": [{"title": "Tooth Tabs", "product_type": "Tabs"}]}
    assert personalization.generate(lead, llm=llm)[2] == "skipped:model_timeout"


def test_the_first_dry_run_call_is_capped_and_a_timeout_is_not_a_missing_product():
    import importlib.util
    import time

    spec = importlib.util.spec_from_file_location(
        "dry_run_openers", "scripts/dry_run_openers.py",
    )
    dry = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dry)
    import os
    os.environ.pop("OUTBOUND_OPENER_LEAD_BUDGET", None)
    os.environ.pop("OUTBOUND_OPENER_FIRST_CALL", None)
    os.environ.pop("OUTBOUND_OPENER_TIMEOUT", None)
    seen = []

    def llm(_prompt, timeout_s=30):
        seen.append(timeout_s)
        raise personalization.ModelTimeout("slow")

    row = dry.render_row(
        {"domain": "huppy.com", "brand": "Huppy", "first_name": "Ann", "recipe": "amz_on"},
        facts={"source": "products.json", "products": [{"title": "Tooth Tabs", "product_type": "Tabs"}]},
        llm=llm,
        deadline=time.monotonic() + 35,
    )
    assert seen and seen[0] == dry.first_call_seconds()
    assert row["hold"] == "skipped:model_timeout"
    assert dry.lead_budget_seconds() == 35


def test_a_rejected_line_is_retried_when_twelve_seconds_remain():
    calls = {"n": 0}

    def llm(_prompt):
        calls["n"] += 1
        if calls["n"] == 1:
            return json.dumps({
                "product_phrase": "keratin treatment",
                "opener": "The keratin treatment is the one I would tell a friend about today.",
                "angle": "New shoppers can picture the treatment in one photo.",
            })
        return json.dumps({
            "product_phrase": "keratin treatment",
            "opener": "The keratin treatment is the one I would tell a friend about today.",
            "angle": "I would use one photo of the treatment and leave the rest alone.",
        })

    lead = {
        **LEAD,
        "brand": "Inova",
        "recipe": "trial_offer",
        "catalog_products": [{"title": "Kera Quik Keratin Treatment", "product_type": "Treatment", "body": ""}],
    }
    opener, angle, source = personalization.generate(lead, llm=llm, allow_retry=lambda: True)
    assert calls["n"] == 2
    assert source == "ai"
    assert "picture" not in angle.lower()
    assert opener
