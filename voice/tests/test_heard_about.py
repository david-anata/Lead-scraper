"""The receptionist asks how they heard about us early, then stores the answer."""

from voice.attribution import (
    HOW_HEARD_QUESTION,
    HOW_HEARD_QUESTION_ES,
    apply_heard_about,
    how_heard_nudge,
    normalize_heard_about,
    should_ask_how_heard,
)
from voice.leads import build_call_row, build_inquiry_row
from voice.lines import LineContext
from voice.notify import call_summary_text
from voice.prompts import build_instructions
from voice.session import CallSession
from voice.tools import execute_tool


def _session(**overrides: object) -> CallSession:
    session = CallSession(
        call_sid="CA123",
        caller_number="+14155550100",
        dialed_number="+13853361778",
        line=LineContext(id="track_building_events", label="Anata events", source="events-page"),
    )
    for key, value in overrides.items():
        setattr(session, key, value)
    return session


def test_the_question_sits_in_discovery_before_pricing_rules() -> None:
    prompt = build_instructions(_session())
    discovery = prompt.index("# Discovery")
    asked = prompt.index(HOW_HEARD_QUESTION)
    contracts = prompt.index("# Contracts")
    assert discovery < asked < contracts
    assert HOW_HEARD_QUESTION_ES in prompt
    assert "ask once, in one short sentence" in prompt
    assert "lockout, flood, no heat" in prompt
    assert "known tenant" in prompt


def test_indexed_callers_are_told_not_to_be_asked() -> None:
    for category in ("tenant", "client", "landlord_legal", "vip"):
        session = _session(known_caller_category=category, known_caller_name="Mina", reason="Checking in")
        prompt = build_instructions(session)
        assert "Do not ask how they heard about us." in prompt
        assert should_ask_how_heard(session) is False


def test_a_new_caller_is_asked_right_after_the_reason_and_not_before() -> None:
    waiting = _session()
    assert should_ask_how_heard(waiting) is False
    session = _session(reason="A Saturday birthday in the Arena")
    nudge = how_heard_nudge(session)
    assert nudge["ask_how_heard"] is True
    assert nudge["say_next"] == HOW_HEARD_QUESTION
    saved = execute_tool(
        "save_caller_details",
        {"reason": "A Saturday birthday in the Arena", "event_or_interest_type": "birthday"},
        session=session,
        settings=None,  # type: ignore[arg-type]
        calendar=None,  # type: ignore[arg-type]
        known_numbers=frozenset(),
    )
    assert saved["ask_how_heard"] is True
    assert saved["say_next"] == HOW_HEARD_QUESTION


def test_urgent_calls_and_a_shrug_do_not_hold_the_conversation() -> None:
    locked = _session(reason="We're locked out of the suite")
    assert should_ask_how_heard(locked) is False
    flooded = _session(notes="There is a flood in the warehouse")
    assert should_ask_how_heard(flooded) is False
    cold = _session(reason="There is no heat in the office")
    assert should_ask_how_heard(cold) is False
    security = _session(reason="Security problem at the front door")
    assert should_ask_how_heard(security) is False
    spanish = _session(reason="Quiero reservar la Arena", notes="Hola, necesito una fiesta")
    assert how_heard_nudge(spanish)["say_next"] == HOW_HEARD_QUESTION_ES


def test_categories_follow_the_callers_words() -> None:
    samples = {
        "I found you on Google": "google_search",
        "Google": "google_search",
        "Google Maps": "google_maps",
        "your website": "website",
        "Peerspace": "peerspace",
        "Eventective": "eventective",
        "The Knot": "other_event_site",
        "a friend told me": "referral",
        "Instagram": "social",
        "we rented before": "returning_customer",
        "I drove by and saw the sign": "drive_by",
        "the radio": "other",
        "I don't know": "unknown",
        "": "unknown",
        "not sure, maybe Google": "google_search",
    }
    for raw, category in samples.items():
        assert normalize_heard_about(raw) == category, raw


def test_simulated_transcript_saves_the_answer_next_to_the_dialed_line() -> None:
    session = _session(summary="Wants the Arena for a birthday.", category="lead", caller_name="Jamie")
    session.add_turn("Caller", "I want the Arena for a birthday party.")
    session.add_turn("Anata", "By the way, how'd you hear about us?")
    session.add_turn("Caller", "I found you guys on Google.")
    session.add_turn("Anata", "What date are you looking at?")
    session.heard_about_raw = "Peerspace"
    row = build_call_row(session, inquiry_id="", sms_notified=False)
    assert row["heard_about_raw"] == "I found you guys on Google."
    assert row["heard_about_source"] == "google_search"
    assert row["dialed_line_source"] == "events-page"
    assert row["line_id"] == "track_building_events"
    assert row["lead_fields"]["source"] == "events-page"
    assert row["lead_fields"]["heard_about_source"] == "google_search"
    assert row["transcript_summary"].endswith("Heard about us: I found you guys on Google.")
    text = call_summary_text(session)
    assert "google_search" not in text
    assert "events-page" not in text
    assert "Heard about us" not in text
    inquiry = build_inquiry_row(session, owner="building-operator")
    assert inquiry["payload_json"]["heardAbout"] == "I found you guys on Google."
    assert inquiry["payload_json"]["heardAboutSource"] == "google_search"
    assert inquiry["payload_json"]["dialedLine"] == "events-page"
    assert inquiry["source"] == "voice"


def test_a_skipped_answer_is_unknown_and_a_tool_value_is_only_a_fallback() -> None:
    skipped = _session(summary="Asked about a tour.")
    skipped.add_turn("Anata", "By the way, how'd you hear about us?")
    skipped.add_turn("Caller", "I don't know.")
    apply_heard_about(skipped)
    assert skipped.heard_about_raw == "I don't know."
    assert skipped.heard_about_source == "unknown"
    row = build_call_row(skipped, inquiry_id="", sms_notified=False)
    assert row["heard_about_source"] == "unknown"
    assert "Heard about us" not in row["transcript_summary"]

    fallback = _session()
    fallback.heard_about_raw = "We saw you on Eventective"
    apply_heard_about(fallback)
    assert fallback.heard_about_source == "eventective"

    volunteered = _session()
    volunteered.add_turn("Caller", "Hi, I found you on Google and want the Arena.")
    apply_heard_about(volunteered)
    assert volunteered.heard_about_source == "google_search"
