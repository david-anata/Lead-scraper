"""The junk guard must catch scripted submissions and never a real lead."""

from sales_support_agent.services.marketing_junk_guard import (
    is_automated_submission,
    junk_signals,
    normalize_email_identity,
)


# The exact submission David received on 2026-07-31.
OBSERVED_BOT = {
    "email": "oroqe.n.u.z.94.5@gmail.com",
    "qualification": {
        "name": "jRYUmRZKmbAAdcTVxYKORSZX",
        "company": "Pmynutqga LLC",
        "phone": "6490216433",
    },
}


def test_observed_bot_submission_is_suppressed():
    assert is_automated_submission(**OBSERVED_BOT) is True


def test_observed_bot_trips_several_independent_signals():
    signals = junk_signals(**OBSERVED_BOT)
    assert "unpronounceable_name" in signals
    assert "implausible_phone" in signals  # exchange code 021 is never issued
    assert "aliased_email" in signals
    assert len(signals) >= 2


def test_ordinary_lead_trips_nothing():
    assert (
        junk_signals(
            email="sarah.chen@brightleaf.com",
            qualification={
                "name": "Sarah Chen",
                "company": "Brightleaf Supplements",
                "phone": "(801) 555-0142",
            },
        )
        == []
    )


def test_ordinary_compound_words_survive():
    """English compounds reach five consonants in a row. That is not a signal."""
    for company in ("Northshore Goods", "Strengths Collective", "Lightship Brands"):
        assert junk_signals(qualification={"company": company}) == [], company


def test_long_real_names_survive():
    """Length alone must never suppress. These are all real, pronounceable names."""
    for name in (
        "Konstantinos Papadopoulos",
        "Bartholomew Fotherington",
        "Nguyen Thi Minh Khai",
        "Christoph Schmidt-Wellenburg",
        "Siobhan O'Callaghan",
        "McDonald MacGregor",
    ):
        assert junk_signals(qualification={"name": name}) == [], name


def test_gmail_plus_tag_is_not_a_signal():
    """Real people use plus tags. Only dot padding is a tell."""
    assert junk_signals(email="sarah+anata@gmail.com") == []


def test_single_signal_is_never_enough():
    """A phone typo on an otherwise real lead must still reach the list."""
    submission = {
        "email": "mark@northshoregoods.com",
        "qualification": {
            "name": "Mark Alvarez",
            "company": "Northshore Goods",
            "phone": "0015551234",
        },
    }
    assert junk_signals(**submission) == ["implausible_phone"]
    assert is_automated_submission(**submission) is False


def test_empty_submission_is_not_suppressed():
    """Missing optional fields are not evidence of anything."""
    assert is_automated_submission(email="real@company.com", qualification={}) is False
    assert is_automated_submission() is False


def test_international_phone_is_not_judged():
    """We cannot validate non-NANP numbers, so they carry no signal."""
    assert junk_signals(qualification={"phone": "+44 20 7946 0958"}) == []


def test_gmail_dots_and_tags_collapse_to_one_identity():
    assert normalize_email_identity("oroqe.n.u.z.94.5@gmail.com") == "oroqenuz945@gmail.com"
    assert normalize_email_identity("O.R.O.Q.E.NUZ945+x@googlemail.com") == "oroqenuz945@gmail.com"


def test_dots_are_significant_outside_gmail():
    """Other providers treat dots as real. Collapsing them would merge strangers."""
    assert normalize_email_identity("sarah.chen@brightleaf.com") == "sarah.chen@brightleaf.com"


def test_normalize_is_stable_on_junk_input():
    assert normalize_email_identity("") == ""
    assert normalize_email_identity("not-an-email") == "not-an-email"
