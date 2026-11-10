"""Published facts stay aligned, and texts do not make promises."""

import re

from voice.audio import mulaw_frames
from voice.calendar_availability import CalendarAvailability
from voice.knowledge import KNOWLEDGE_PATH, RENTAL_RATES, load_knowledge_text, published_rates
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


def test_knowledge_file_contains_the_published_rental_rates_and_no_extras() -> None:
    text = load_knowledge_text()
    assert KNOWLEDGE_PATH.is_file()
    for amount in (1500, 2000, 3000, 3500, 250, 400, 550, 75, 125):
        assert f"${amount:,}" in text
    amounts = {int(match.replace(",", "")) for match in re.findall(r"\$(\d[\d,]*)", text)}
    assert amounts == {1500, 2000, 3000, 3500, 250, 400, 550, 75, 125}
    assert "1657 N. State Street" in text
    assert "5:00 p.m. to 11:00 p.m." in text
    assert "8:00 a.m. to midnight" in text
    assert "8:00 a.m. to 10:00 p.m." in text
    assert "do not take trash out" in text
    rates = published_rates()
    assert rates["half_day_up_to_6_hours_usd"] == RENTAL_RATES["half_day_up_to_6_hours_usd"] == 1500
    assert "not reserved" in str(rates["confirmation"]).lower() or "confirmed only" in str(rates["confirmation"]).lower()


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
    assert "can't confirm a date" in booked
    assert "5:00 p.m." in hours


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
