"""Published facts stay aligned, and texts do not make promises."""

import re
from datetime import date

from voice.audio import mulaw_frames
from voice.calendar_availability import CalendarAvailability
from voice.knowledge import (
    ANATA_INTELLIGENCE_PLANS,
    FRONT_DESK_PATH,
    INC_KNOWLEDGE_PATH,
    KNOWLEDGE_PATH,
    PUBLISHED_PHONE,
    RENTAL_RATES,
    anata_inc_facts,
    hours_guidance,
    load_knowledge_text,
    published_addons,
    published_rates,
)
from voice.prompts import build_instructions
from voice.leads import build_inquiry_row, inquiry_kind, should_create_inquiry
from voice.lines import LineContext
from voice.notify import should_text_about_call
from voice.session import CallSession
from voice.sms_reply import reply_to_sms
from voice.tools import session_update_event
from voice.settings import Settings


def _session(**overrides: object) -> CallSession:
    session = CallSession(
        call_sid="CA123",
        caller_number="+14155550100",
        dialed_number="+15550001111",
        line=LineContext(id="anata", label="Anata"),
    )
    for key, value in overrides.items():
        setattr(session, key, value)
    return session


def _dollars(text: str) -> set[int]:
    return {int(match.replace(",", "")) for match in re.findall(r"\$(\d[\d,]*)", text)}


def test_knowledge_file_contains_the_published_rental_rates_and_no_extras() -> None:
    text = KNOWLEDGE_PATH.read_text(encoding="utf-8")
    assert KNOWLEDGE_PATH.is_file()
    for amount in (1500, 2000, 3000, 3500, 1000, 250, 400, 550, 75, 125, 150):
        assert f"${amount:,}" in text
    assert _dollars(text) == {1500, 2000, 1000, 3000, 3500, 250, 400, 550, 75, 125, 150, 2, 4}
    assert "1657 N. State Street" in text
    assert "5:00 p.m. to 11:00 p.m." not in text
    assert "8:00 a.m. to midnight" in text
    assert "8:00 a.m. to 10:00 p.m." in text
    assert "9:00 a.m. to 4:00 p.m." in text
    assert "$2 to $4 per square foot per month" in text
    assert "do not take trash out" in text
    assert "BNI" in text
    assert "$175" not in text
    assert "(385) 336-3562" in text
    assert "386-8104" not in text
    assert "You set them up and reset them" in text
    assert "event support" not in text.casefold()
    assert "2:00 a.m." not in text
    assert "Teardown ends" not in text
    assert "require customer-paid security" not in text.casefold()
    assert "never required" in text.casefold()
    assert "autoservicio" in text
    assert "security_preference" in text
    rates = published_rates()
    assert rates["half_day_up_to_6_hours_usd"] == RENTAL_RATES["half_day_up_to_6_hours_usd"] == 1500
    assert "not reserved" in str(rates["confirmation"]).lower() or "confirmed only" in str(rates["confirmation"]).lower()


def test_anata_inc_knowledge_has_only_published_prices_and_says_agency() -> None:
    text = INC_KNOWLEDGE_PATH.read_text(encoding="utf-8")
    assert _dollars(text) == {1, 20, 99, 199, 399, 999}
    assert "Agency plan" in text
    assert "portfolio plan" not in text.replace("Never say Portfolio plan", "").casefold()
    assert "Do not quote any fulfillment fee" in text
    assert "trial requires a card" in text
    assert "Shipping OS is free" in text
    assert "customizable wallet" in text
    facts = anata_inc_facts()
    assert facts["software"]["agency_from_usd_per_month"] == ANATA_INTELLIGENCE_PLANS["agency_from_usd_per_month"] == 999


def test_front_desk_rules_cover_every_line_and_the_closed_topics() -> None:
    text = FRONT_DESK_PATH.read_text(encoding="utf-8")
    assert _dollars(text) == set()
    for line_id in ("main:", "front_desk:", "anata_services:", "gabe_line:", "val_line:", "test:"):
        assert line_id in text
    for topic in ("Fondell", "forbearance", "billing disputes", "Never give out a personal or cell number"):
        assert topic in text
    assert "track_inc_*" in text
    assert "track_eventective" in text
    assert "Anata Building and Arena flow" in text
    assert "anata Inc. flow" in text
    combined = load_knowledge_text()
    assert combined.index("# Front desk rules") < combined.index("# Anata Building knowledge") < combined.index("# anata Inc. knowledge")


def test_tracking_source_is_in_the_summary_and_the_lead_fields() -> None:
    from voice.notify import call_summary_text

    session = _session()
    session.line = LineContext(id="track_inc_fulfillment", label="Anata fulfillment", source="inc-fulfillment")
    fields = session.lead_fields()
    assert fields["source"] == "inc-fulfillment"
    summary = call_summary_text(session)
    assert "Source: inc-fulfillment" in summary
    assert _session().lead_fields().get("source", "") == ""


def test_prompt_names_both_businesses_and_the_line_id() -> None:
    prompt = build_instructions(_session())
    assert "anata Inc." in prompt
    assert "Line id: anata" in prompt
    assert "Never negotiate" in prompt
    assert "get_anata_inc_facts" in prompt
    assert "continue in Spanish" in prompt
    assert "security_preference" in prompt
    assert "Security is never required" in prompt
    assert "in mind for security" not in prompt
    assert "Great question" in prompt
    assert "one question at a time" in prompt
    assert "rental agreement" in prompt.casefold()
    assert "rental agreement" in KNOWLEDGE_PATH.read_text(encoding="utf-8").casefold()
    assert PUBLISHED_PHONE == "(385) 336-3562"
    assert "(385) 336-3562" in anata_inc_facts()["contact"]
    assert "(385) 336-3562" in FRONT_DESK_PATH.read_text(encoding="utf-8")
    assert "(385) 336-3562" in INC_KNOWLEDGE_PATH.read_text(encoding="utf-8")
    saturday = hours_guidance(date(2026, 12, 5), "party")
    assert "2:00 a.m." not in saturday
    assert "midnight" in saturday
    assert "never required" in saturday.casefold()
    included = str(published_addons()["included"])
    assert "Included: tables, chairs" in included
    assert "You set them up and reset them" in included


def test_agency_lead_is_not_a_building_inquiry() -> None:
    agency = _session(category="lead", business_line="agency", company="Northwind Supplements")
    assert should_create_inquiry(agency) is False
    event = _session(category="lead", business_line="event", event_date="2026-12-01")
    assert should_create_inquiry(event) is True


def test_sms_replies_quote_published_rates_and_do_not_confirm_a_booking() -> None:
    rates = reply_to_sms("how much is a Saturday?")
    hours = reply_to_sms("what are your hours")
    booked = reply_to_sms("is June 4 available")
    for reply in (rates, hours, booked, reply_to_sms("hello")):
        assert "you're booked" not in reply.lower()
        assert "reserved for you" not in reply.lower()
    assert "$1,500" in rates
    assert "$2,000" in rates
    assert "does not reserve" in rates
    assert "priced by the team" in rates
    assert "can't confirm a date" in booked
    assert "8:00 a.m. to midnight" in hours
    assert "9:00 a.m. to 4:00 p.m." in hours
    assert "5:00 p.m. to 11:00 p.m." not in hours
    assert "no free teardown" in hours
    assert "You set up and reset the room" in rates
    security = reply_to_sms("do we need a security guard?")
    assert "never required" in security
    assert "20%" in security
    assert reply_to_sms("is there a security deposit and how much").startswith("Weekend")


def test_spam_calls_are_not_texted_and_sms_summaries_always_can_be() -> None:
    assert should_text_about_call("spam") is False
    assert should_text_about_call("lead") is True
    assert should_text_about_call("vendor") is True


def test_calendar_without_credentials_does_not_invent_availability() -> None:
    calendar = CalendarAvailability(calendar_id="cal", service_account_json="")
    result = calendar.check("2026-12-01", "party")
    assert result["status"] == "unavailable"
    assert "will confirm" in str(result["speak"])
    assert "open" not in str(result["speak"]).lower() or "do not say the date is open" in str(result["speak"]).lower()


def test_calendar_hides_event_titles() -> None:
    def fetch(start: str, end: str) -> list[dict[str, object]]:
        assert start
        assert end
        return [
            {"status": "confirmed", "summary": "Private client name"},
            {"status": "cancelled", "summary": "Ignore me"},
            {"transparency": "transparent", "summary": "Free hold"},
        ]

    calendar = CalendarAvailability(
        calendar_id="cal",
        service_account_json="{}",
        fetch_events=fetch,
    )
    result = calendar.check("2026-12-01", "party")
    assert result["status"] == "busy"
    assert result["conflict_count"] == 1
    rendered = str(result)
    assert "Private client" not in rendered


def test_clear_calendar_day_is_not_a_booking() -> None:
    calendar = CalendarAvailability(
        calendar_id="cal",
        service_account_json="{}",
        fetch_events=lambda start, end: [],
    )
    result = calendar.check("2026-12-01", "business")
    assert result["status"] == "clear"
    assert "not reserved" in str(result["speak"])


def test_lead_row_matches_the_admin_inquiry_shape() -> None:
    session = _session(
        category="lead",
        caller_name="Jamie Lee",
        email="jamie@example.com",
        company="Northwind",
        event_date="2026-12-01",
        guest_count="80",
        event_or_interest_type="wedding",
        notes="Evening party",
    )
    assert should_create_inquiry(session) is True
    assert inquiry_kind(session) == "event"
    row = build_inquiry_row(session, owner="building-operator")
    assert row["source"] == "voice"
    assert row["kind"] == "event"
    assert row["email"] == "jamie@example.com"
    assert row["preferred_date"] == "2026-12-01"
    assert row["consent_to_marketing"] is False
    assert row["payload_json"]["eventType"] == "wedding"
    assert row["payload_json"]["groupSize"] == "80"
    assert row["payload_json"]["company"] == "Northwind"
    assert row["payload_json"]["_lifecycle"]["stage"] == "new"
    assert row["idempotency_key"] == "voice-call:CA123"
    assert row["source"] == "voice"

    party = _session(
        category="lead",
        caller_name="Jamie Lee",
        event_date="2026-12-05",
        guest_count="120",
        event_or_interest_type="birthday party",
        security_preference="no",
    )
    party_row = build_inquiry_row(party, owner="building-operator")
    assert party_row["source"] == "voice"
    assert party_row["payload_json"]["security_preference"] == "no"
    assert party_row["payload_json"]["_event_interview"]["security_preference"] == "no"

    spam = _session(category="spam")
    assert should_create_inquiry(spam) is False
    vendor = _session(
        category="vendor",
        event_or_interest_type="event marketing",
        notes="We run events for venues",
    )
    assert should_create_inquiry(vendor) is False


def test_realtime_session_uses_native_mulaw() -> None:
    settings = Settings(
        xai_api_key="k",
        xai_model="grok-voice-latest",
        xai_voice="eve",
        xai_realtime_url="wss://api.x.ai/v1/realtime",
        twilio_account_sid="",
        twilio_auth_token="",
        twilio_messaging_service_sid="",
        forward_to_number="",
        notify_sms_number="",
        public_base_url="",
        render_external_url="",
        supabase_url="",
        supabase_service_key="",
        calendar_id="",
        google_service_account_json="",
        google_delegated_subject="",
        line_contexts_json="",
        known_caller_numbers="",
        lead_owner="building-operator",
        sms_autoreply=False,
        recording_enabled=False,
        validate_signatures=True,
        max_call_seconds=720,
    )
    event = session_update_event(settings, "Be brief.")
    audio = event["session"]["audio"]
    assert audio["input"]["format"]["type"] == "audio/pcmu"
    assert audio["output"]["format"]["type"] == "audio/pcmu"
    assert "language_hint" not in audio["input"]["transcription"]
    assert audio["output"]["speed"] == 0.95
    assert "rate" not in audio["input"]["format"]
    names = [tool["name"] for tool in event["session"]["tools"]]
    assert "request_transfer" in names
    assert "check_event_date" in names


def test_outbound_audio_is_framed_for_twilio() -> None:
    import base64

    raw = b"\xff" * 320
    frames = mulaw_frames(base64.b64encode(raw).decode("ascii"))
    assert len(frames) == 2
    assert base64.b64decode(frames[0]) == b"\xff" * 160
