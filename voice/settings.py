"""Environment configuration for the voice receptionist.

Secrets stay in the environment. Nothing in this module has a default that
is a credential, a staff phone number, or customer data.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().casefold() in {"1", "true", "yes", "on"}


def _text(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


@dataclass(frozen=True)
class Settings:
    """Runtime settings. Construct with `from_env` so tests can pass a copy."""

    xai_api_key: str
    xai_model: str
    xai_voice: str
    xai_realtime_url: str
    twilio_account_sid: str
    twilio_auth_token: str
    twilio_messaging_service_sid: str
    forward_to_number: str
    notify_sms_number: str
    public_base_url: str
    render_external_url: str
    supabase_url: str
    supabase_service_key: str
    calendar_id: str
    google_service_account_json: str
    google_delegated_subject: str
    line_contexts_json: str
    known_caller_numbers: str
    lead_owner: str
    sms_autoreply: bool
    recording_enabled: bool
    validate_signatures: bool
    max_call_seconds: int
    voice_database_url: str = ""

    @classmethod
    def from_env(cls) -> "Settings":
        calendar_id = _text(
            "VOICE_EVENTS_CALENDAR_ID",
            _text("BUILDING_GOOGLE_CALENDAR_ID"),
        ) or (
            "c_0e16476c25a1a45fe0ff1b15619e1d6fee639fdbc4682c26fb19ad84b4a78862"
            "@group.calendar.google.com"
        )
        service_account = _text("GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON") or _text(
            "BUILDING_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON"
        )
        supabase_key = _text("SUPABASE_SERVICE_ROLE_KEY") or _text("SUPABASE_SERVICE_KEY")
        try:
            max_call = int(_text("VOICE_MAX_CALL_SECONDS", "720") or "720")
        except ValueError:
            max_call = 720
        return cls(
            xai_api_key=_text("XAI_API_KEY"),
            xai_model=_text("XAI_VOICE_MODEL", "grok-voice-latest"),
            xai_voice=_text("XAI_VOICE", "ara"),
            xai_realtime_url=_text(
                "XAI_REALTIME_URL", "wss://api.x.ai/v1/realtime"
            ),
            twilio_account_sid=_text("TWILIO_ACCOUNT_SID"),
            twilio_auth_token=_text("TWILIO_AUTH_TOKEN"),
            twilio_messaging_service_sid=_text("TWILIO_MESSAGING_SERVICE_SID"),
            forward_to_number=_text("FORWARD_TO_NUMBER"),
            notify_sms_number=_text("NOTIFY_SMS_NUMBER"),
            public_base_url=_text("PUBLIC_BASE_URL").rstrip("/"),
            render_external_url=_text("RENDER_EXTERNAL_URL").rstrip("/"),
            supabase_url=_text("SUPABASE_URL").rstrip("/"),
            supabase_service_key=supabase_key,
            voice_database_url=_text("VOICE_DATABASE_URL"),
            calendar_id=calendar_id,
            google_service_account_json=service_account,
            google_delegated_subject=_text("BUILDING_GOOGLE_CALENDAR_DELEGATED_SUBJECT"),
            line_contexts_json=_text("LINE_CONTEXTS_JSON"),
            known_caller_numbers=_text("KNOWN_CALLER_NUMBERS"),
            lead_owner=_text("BUILDING_DEFAULT_LEAD_OWNER", "building-operator"),
            sms_autoreply=_flag("SMS_AUTOREPLY", False),
            recording_enabled=_flag("RECORDING_ENABLED", False),
            validate_signatures=_flag("VOICE_SIGNATURE_VALIDATION", True),
            max_call_seconds=max(60, min(max_call, 3600)),
        )

    def http_base(self, request_base: str = "") -> str:
        """Public https origin used in TwiML and signature checks."""

        if self.public_base_url:
            return self.public_base_url
        if self.render_external_url:
            return self.render_external_url
        if request_base and not self.validate_signatures:
            return request_base.rstrip("/")
        return ""

    def missing(self) -> list[str]:
        """Names of settings that are empty. Values are never returned."""

        required = {
            "XAI_API_KEY": self.xai_api_key,
            "TWILIO_ACCOUNT_SID": self.twilio_account_sid,
            "TWILIO_AUTH_TOKEN": self.twilio_auth_token,
            "FORWARD_TO_NUMBER": self.forward_to_number,
            "NOTIFY_SMS_NUMBER": self.notify_sms_number,
            "PUBLIC_BASE_URL": self.http_base(),
        }
        missing = [name for name, value in required.items() if not value]
        if not self.logging_ready:
            missing.append("VOICE_DATABASE_URL")
        return missing

    @property
    def calls_ready(self) -> bool:
        return bool(
            self.xai_api_key
            and self.twilio_account_sid
            and self.twilio_auth_token
            and self.http_base()
        )

    @property
    def logging_ready(self) -> bool:
        if self.voice_database_url:
            return True
        return bool(self.supabase_url and self.supabase_service_key)

    @property
    def calendar_ready(self) -> bool:
        return bool(self.calendar_id and self.google_service_account_json)
