"""Dialed-number greeting context."""

from voice.lines import resolve_line
from voice.twiml import RECORDING_NOTICE, inbound_twiml
from voice.settings import Settings


LINE_MAP = """
{
  "default": {"id": "anata", "label": "Anata"},
  "+15550001111": {"id": "front_desk", "label": "Anata Building front desk"}
}
"""


def test_known_number_selects_its_line() -> None:
    line = resolve_line(LINE_MAP, "555-000-1111")
    assert line.id == "front_desk"
    assert line.spoken_greeting() == "Thanks for calling the Anata Building front desk. How can I help you today?"


def test_unknown_number_uses_the_default_line() -> None:
    line = resolve_line(LINE_MAP, "+15559990000")
    assert line.id == "anata"
    assert line.spoken_greeting().startswith("Thanks for calling Anata.")


def test_bad_json_does_not_raise() -> None:
    line = resolve_line("{", "+15550001111")
    assert line.id == "anata"


def _settings(**overrides: object) -> Settings:
    base = dict(
        xai_api_key="test-key",
        xai_model="grok-voice-latest",
        xai_voice="eve",
        xai_realtime_url="wss://api.x.ai/v1/realtime",
        twilio_account_sid="AC123",
        twilio_auth_token="12345",
        twilio_messaging_service_sid="",
        forward_to_number="+14155550199",
        notify_sms_number="+14155550198",
        public_base_url="https://voice.example.com",
        render_external_url="",
        supabase_url="",
        supabase_service_key="",
        calendar_id="calendar@group.calendar.google.com",
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
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def test_recording_notice_is_off_unless_enabled() -> None:
    quiet = inbound_twiml(_settings(), http_base="https://voice.example.com", call_sid="CA1")
    loud = inbound_twiml(
        _settings(recording_enabled=True),
        http_base="https://voice.example.com",
        call_sid="CA1",
    )
    assert RECORDING_NOTICE not in quiet
    assert "<Recording" not in quiet
    assert RECORDING_NOTICE in loud
    assert 'url="wss://voice.example.com/voice/media?token=' in quiet
    assert "audio/pcmu" not in quiet


def test_resume_does_not_repeat_the_recording_notice() -> None:
    xml = inbound_twiml(
        _settings(recording_enabled=True),
        http_base="https://voice.example.com",
        call_sid="CA1",
        resume="message",
    )
    assert RECORDING_NOTICE not in xml
    assert "/voice/media?token=" in xml
