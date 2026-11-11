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
    title = "superPeptide GHK Copper Peptide Serum"
    assert blocked_category_reason(title, "Skincare") == ""
    product, detail, kind = extract_detail(title, "A cosmetic serum.")
    assert "GHK" in product
    assert detail == "copper peptide"
    assert kind == "material"
    facts = LineFacts(brand="Pure & Cimple", product=product, detail=detail, recipe="amz_on")
    assert hard_reason(
        "The superPeptide GHK copper peptide serum is a cosmetic serum.",
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


def test_the_default_model_is_the_fast_non_reasoning_grok():
    assert FAST_MODEL == "grok-4.20-0309-non-reasoning"
