"""Regression checks for standard engagement terms and intake parity."""
import json
from sales_support_agent.services.deck.formatting import _default_offer_cards, _normalize_custom_offer_cards
from sales_support_agent.services.deck.rendering import _render_offer_card
from sales_support_agent.services.deck.story import _section_offers


def test_renamed_platform_offer_keeps_terms_through_normalization():
    cards = _default_offer_cards()
    assert [card["title"] for card in cards] == ["DFY", "DYI"]
    assert (cards[0]["price"], cards[0]["commission"], cards[0]["baseline"]) == ("$3,000", "5%", "$10,000")
    cards[0]["enabled"] = False
    cards[1]["title"] = "My platform plan"
    cards[1]["price"] = "$150"
    result = _normalize_custom_offer_cards(offer_payload_json=json.dumps(cards), offers=[])
    assert len(result) == 1
    output = _render_offer_card(result[0])
    for text in ("My platform plan", "$150", "Onboarding fee", "Your team", "Discuss platform signup"):
        assert text in output
    assert "3 months minimum" not in output
    assert "10%" not in output
    assert "Commission" not in output


def test_legacy_custom_offer_preserves_existing_defaults():
    result = _normalize_custom_offer_cards(offer_payload_json=json.dumps([{"title": "Legacy", "price": "$900"}]), offers=[])
    assert "term" not in result[0]
    assert "3 months minimum" in _render_offer_card(result[0])


def test_story_closer_does_not_promise_agency_execution_for_platform():
    output = _section_offers({"offer_cards": _default_offer_cards(), "text_fields": {"why_anata_summary": "Campaigns live in week 2"}})
    assert "Campaigns live in week 2" not in output
    assert "your team takes over execution" in output
