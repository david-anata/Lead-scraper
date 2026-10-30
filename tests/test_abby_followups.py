"""Approved Abby follow-up templates load, stay free of em dashes, and sign off."""

from sales_support_agent.abby_followups import (
    APPROVED_FOLLOWUPS,
    DEFAULT_BOOKING_LINK,
    PERSONALIZATION_RULE,
    SIGNATURE,
    load_approved_followups,
)

EM_DASH = "\u2014"


def test_loader_returns_the_approved_followups():
    loaded = load_approved_followups()
    assert list(loaded) == ["out_of_office", "interested_not_booked", "wrong_person"]
    assert loaded == {item.key: item for item in APPROVED_FOLLOWUPS}
    for item in loaded.values():
        assert item.status == "approved"
        assert item.approved_by == "David"
        assert item.approved_on == "2026-10-30"
        assert item.signature == "Abby\nAccount Specialist, Anata"
        assert item.signature == SIGNATURE
        assert item.signed_copies()


def test_personalization_rule_and_booking_link_are_loadable():
    assert "never guesses or invents details" in PERSONALIZATION_RULE
    assert "Hope all is well" in PERSONALIZATION_RULE
    assert DEFAULT_BOOKING_LINK == "https://calendar.app.google/5TwjbbpmKaGAQuBk8"
    assert EM_DASH not in PERSONALIZATION_RULE


def test_templates_have_no_em_dashes_and_carry_the_signature():
    loaded = load_approved_followups()
    for item in loaded.values():
        for part in item.copy_parts():
            assert EM_DASH not in part
        for copy in item.signed_copies():
            assert EM_DASH not in copy
            assert copy.endswith(SIGNATURE)
            assert copy.count(SIGNATURE) == 1


def test_out_of_office_personal_lines_and_timing():
    step = load_approved_followups()["out_of_office"].steps[0]
    assert step.send_when.startswith("the day after the return date")
    lines = dict(step.personal_lines)
    assert lines["vacation"] == "Hope you had a great vacation."
    assert lines["conference_or_trade_show"] == "Hope the show was worth the trip."
    assert lines["no_reason"] == "Hope your time away went well."
    assert "{personal_line}" in step.body and "{booking_link}" in step.body
    assert "{brand}'s Amazon and Shopify growth" in step.body


def test_interested_not_booked_steps():
    steps = load_approved_followups()["interested_not_booked"].steps
    assert steps[0].send_when == "2 days after their reply"
    assert "free 14-day trial" in steps[0].body
    assert "{booking_link}" in steps[0].body
    assert steps[1].send_when == "4 days after step 1"
    assert steps[1].body == (
        "Hi {first_name}, should I hold a trial spot for {brand}, "
        "or is the timing off right now?"
    )
    assert "{timing_hint}" in steps[1].alternate_body
    assert "after our launch" in steps[1].alternate_when


def test_wrong_person_ask_and_referral():
    ask, referral = load_approved_followups()["wrong_person"].steps
    assert ask.send_when == "immediately"
    assert ask.example_personal_line == "makes sense since you're on the design side."
    assert ask.body == (
        "Thanks {first_name}, {personal_line} Who handles Amazon or "
        "marketing growth for {brand}?"
    )
    assert referral.optional_detail_example == ", especially with your recent launch of {product}"
    assert "{storeleads_detail}" in referral.body
    assert "free 14-day trial" in referral.body
